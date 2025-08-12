#!/usr/bin/env python3
"""
Pre-simulation Status Change Calculator (MP3)

Рассчитывает status_change в таблице heli_pandas на дату D (текущая дата симуляции)
по правилам разметки RTC (ops_check + balance) на базе MP1/MP3/MP4/MP5, без начисления
sne/ppr и без финальных переходов. Поддерживает dry-run (SQL печать) по умолчанию.

Правила (кратко):
- Фильтры по group_by (1=МИ-8Т, 2=МИ-17)
- rtc_ops_check (LL/OH/BR с daily_today/daily_next): выставляет status_change in (4,6)
- host trigger: trigger_pr_final_{grp} = target_ops(D) - current_ops(D)
- rtc_balance: 
  - trigger<0: из OPS→3 (top |trigger| по ppr DESC, sne DESC, mfg_date ASC)
  - trigger>0: 5→2, затем 3→2, затем 1→2 при (D - version_date) >= repair_time(partno_comp)

Дата: 2025-08-10
"""

import argparse
import sys
from typing import List
from datetime import datetime
from pathlib import Path

sys.path.append(str(__file__).rsplit('/code/', 1)[0] + '/code/utils')
from config_loader import get_clickhouse_client


def _current_version_subq() -> str:
    return (
        "SELECT version_date, version_id FROM heli_pandas "
        "ORDER BY version_date DESC, version_id DESC LIMIT 1"
    )


def build_ops_check_sql(group_by_value: int) -> str:
    return f"""
-- rtc_ops_check для group_by={group_by_value}
WITH 
  D AS (
    SELECT min(dates) FROM flight_program_fl 
    WHERE (version_date, version_id) = (
      SELECT version_date, version_id FROM heli_pandas ORDER BY version_date DESC, version_id DESC LIMIT 1
    )
  ),
  daily_today AS (
    SELECT aircraft_number, daily_hours 
    FROM flight_program_fl WHERE dates = (SELECT D) 
  ),
  daily_next AS (
    SELECT aircraft_number, daily_hours 
    FROM flight_program_fl WHERE dates = addDays((SELECT D), 1)
  ),
  mp1 AS (
    SELECT partno_comp, br, repair_time FROM md_components
  )
ALTER TABLE heli_pandas
UPDATE status_change = multiIf(
    -- LL: хватит на сегодня, не хватит на завтра → 6
    (ll - sne) >= dt.daily_hours AND (ll - sne) < (dt.daily_hours + coalesce(dn.daily_hours, 0)), 6,
    -- OH+BR: хватит на сегодня, не хватит на завтра и неремонтопригоден → 6
    (oh - ppr) >= dt.daily_hours AND (oh - ppr) < (dt.daily_hours + coalesce(dn.daily_hours, 0)) AND (sne + dt.daily_hours) >= coalesce(m1.br, 4294967295), 6,
    -- OH: хватит на сегодня, не хватит на завтра и ремонтопригоден → 4
    (oh - ppr) >= dt.daily_hours AND (oh - ppr) < (dt.daily_hours + coalesce(dn.daily_hours, 0)) AND (sne + dt.daily_hours) < coalesce(m1.br, 4294967295), 4,
    0
)
WHERE status_id = 2 AND status_change = 0 AND group_by = {group_by_value}
  AND aircraft_number IN (SELECT aircraft_number FROM flight_program_fl)
  SETTINGS allow_experimental_alter_update = 1
AS hp
JOIN daily_today dt ON hp.aircraft_number = dt.aircraft_number
LEFT JOIN daily_next dn ON hp.aircraft_number = dn.aircraft_number
LEFT JOIN mp1 m1 ON m1.partno_comp = hp.partseqno_i
""".strip()


def build_host_diag_sql(group_by_value: int) -> str:
    field = 'ops_counter_mi8' if group_by_value == 1 else 'ops_counter_mi17'
    return f"""
-- Диагностика host-триггера для group_by={group_by_value}
WITH 
  D AS (
    SELECT min(dates) FROM flight_program_fl 
    WHERE (version_date, version_id) = (
      SELECT version_date, version_id FROM heli_pandas ORDER BY version_date DESC, version_id DESC LIMIT 1
    )
  )
SELECT
  (SELECT count() FROM heli_pandas WHERE status_id=2 AND status_change=0 AND group_by={group_by_value}) AS current_ops,
  (SELECT sum({field}) FROM flight_program_ac WHERE dates=(SELECT D)) AS target_ops,
  (target_ops - current_ops) AS trigger_pr_final;
""".strip()


def build_balance_templates_sql(group_by_value: int) -> str:
    return f"""
-- Шаблон UPDATE для rtc_balance, trigger<0 (сокращение):
-- ALTER TABLE heli_pandas UPDATE status_change = 3
-- WHERE status_id=2 AND status_change=0 AND group_by={group_by_value}
-- ORDER BY ppr DESC, sne DESC, mfg_date ASC
-- LIMIT abs(:trigger_pr_final)
-- SETTINGS allow_experimental_alter_update = 1;

-- Шаблон UPDATE для rtc_balance, trigger>0 (дефицит):
-- Этап1: 5→2; Этап2: 3→2; Этап3: 1→2 (при (D-version_date) >= repair_time)
-- ALTER TABLE heli_pandas UPDATE status_change = 2 WHERE status_id=5 AND status_change=0 AND group_by={group_by_value};
-- ALTER TABLE heli_pandas UPDATE status_change = 2 WHERE status_id=3 AND status_change=0 AND group_by={group_by_value};
-- ALTER TABLE heli_pandas UPDATE status_change = 2 
-- WHERE status_id=1 AND status_change=0 AND group_by={group_by_value} AND dateDiff('day', version_date, (SELECT min(dates) FROM flight_program_fl)) >= (
--   SELECT coalesce(repair_time, 0) FROM md_components WHERE partno_comp = heli_pandas.partseqno_i
-- );


""".strip()


def write_plan_files(plan_lines: List[str], balance_tpl_lines: List[str]) -> None:
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_dir = Path('temp_data')
    out_dir.mkdir(exist_ok=True)
    plan_file = out_dir / f'status_change_plan_{timestamp}.sql'
    with plan_file.open('w', encoding='utf-8') as f:
        f.write("\n\n".join(plan_lines) + "\n")
    balance_file = out_dir / f'status_change_balance_templates_{timestamp}.sql'
    with balance_file.open('w', encoding='utf-8') as f:
        f.write("\n\n".join(balance_tpl_lines) + "\n")
    print(f"📝 Сохранены файлы плана: {plan_file}, {balance_file}")


def main():
    parser = argparse.ArgumentParser(description='Расчет MP3.status_change на D по правилам RTC (ops_check + balance)')
    parser.add_argument('--apply', action='store_true', help='Выполнить SQL (по умолчанию только печать)')
    parser.add_argument('--group', choices=['all', '1', '2'], default='all', help='Ограничить расчёт конкретной группой (1=МИ‑8Т, 2=МИ‑17)')
    args = parser.parse_args()

    groups = [1, 2] if args.group == 'all' else [int(args.group)]

    plan_sqls: List[str] = []
    balance_tpl_sqls: List[str] = []

    # 0) Безопасно добавить колонку status_change и очистить предыдущую разметку
    plan_sqls.append("""
ALTER TABLE heli_pandas
ADD COLUMN IF NOT EXISTS status_change UInt8 DEFAULT 0
""".strip())
    plan_sqls.append("""
ALTER TABLE heli_pandas
UPDATE status_change = 0
WHERE status_change != 0
SETTINGS allow_experimental_alter_update = 1
""".strip())

    # 1) Для каждой группы формируем ops_check и диагностику host, плюс шаблоны balance
    for g in groups:
        plan_sqls.append(build_ops_check_sql(g))
        plan_sqls.append(build_host_diag_sql(g))
        balance_tpl_sqls.append(build_balance_templates_sql(g))

    if not args.apply:
        # DRY RUN: печать и выгрузка в файлы
        print("\n=== DRY RUN: Планируемые SQL (короткий вывод) ===")
        for i, sql in enumerate(plan_sqls, 1):
            head = sql.splitlines()[:5]
            print(f"-- SQL #{i} (первые строки):\n" + "\n".join(head) + "\n...")
        write_plan_files(plan_sqls, balance_tpl_sqls)
        return 0

    # APPLY: выполнить безопасные шаги + диагностические SELECT
    client = get_clickhouse_client()
    for sql in plan_sqls:
        upper = sql.strip().upper()
        if upper.startswith('ALTER TABLE') or upper.startswith('UPDATE'):
            client.execute(sql)
        else:
            try:
                rows = client.execute(sql)
                print(rows)
            except Exception:
                pass
    print("✅ MP3.status_change рассчитан (см. также сгенерированные SQL-шаблоны balance)")
    return 0


if __name__ == '__main__':
    sys.exit(main())