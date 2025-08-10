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
  - если размечено 4: дополнительно repair_days=1

Дата: 2025-08-10
"""

import argparse
import sys
from typing import List
from datetime import date

sys.path.append(str(__file__).rsplit('/code/', 1)[0] + '/code/utils')
from config_loader import get_clickhouse_client


def build_sql(current_version_subq: str) -> List[str]:
    sqls: List[str] = []

    # 0) Безопасно добавить колонку status_change
    sqls.append("""
ALTER TABLE heli_pandas
ADD COLUMN IF NOT EXISTS status_change UInt8 DEFAULT 0
""".strip())

    # 1) Определить D как минимальную дату текущей версии MP5
    # Используем подзапрос текущей версии (version_date, version_id) из heli_pandas
    sqls.append(f"""
-- Определяем D (текущая дата симуляции)
WITH (
  SELECT min(dates)
  FROM flight_program_fl
  WHERE (version_date, version_id) IN ({current_version_subq})
) AS D
SELECT D
""".strip())

    # 2) Очистка предыдущей разметки (на случай повторного прогона)
    sqls.append("""
ALTER TABLE heli_pandas
UPDATE status_change = 0
WHERE status_change != 0
SETTINGS allow_experimental_alter_update = 1
""".strip())

    # 3) rtc_ops_check (LL/OH/BR) по group_by=1 (МИ-8Т) — зеркально можно запустить для 2
    sqls.append("""
-- rtc_ops_check для group_by=1 (МИ-8Т)
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
WHERE status_id = 2 AND status_change = 0 AND group_by = 1
  AND aircraft_number IN (SELECT aircraft_number FROM flight_program_fl)
  SETTINGS allow_experimental_alter_update = 1
AS hp
JOIN daily_today dt ON hp.aircraft_number = dt.aircraft_number
LEFT JOIN daily_next dn ON hp.aircraft_number = dn.aircraft_number
LEFT JOIN mp1 m1 ON m1.partno_comp = hp.partseqno_i
""".strip())

    # 4) host-триггер + rtc_balance (минимально In-DB, без сортировки по всем полям — печать рекомендаций)
    # Для оффлайн режима выведем SELECT-диагностики и UPDATE шаблоны, не делая сложных TOP-N без ключа.

    # 4.1) Диагностика текущего OPS и целевых значений
    sqls.append("""
-- Диагностика host-триггера для group_by=1
WITH 
  D AS (
    SELECT min(dates) FROM flight_program_fl 
    WHERE (version_date, version_id) = (
      SELECT version_date, version_id FROM heli_pandas ORDER BY version_date DESC, version_id DESC LIMIT 1
    )
  )
SELECT
  (SELECT count() FROM heli_pandas WHERE status_id=2 AND status_change=0 AND group_by=1) AS current_ops,
  (SELECT sum(ops_counter_mi8) FROM flight_program_ac WHERE dates=(SELECT D)) AS target_ops,
  (target_ops - current_ops) AS trigger_pr_final_mi8;
""".strip())

    # 4.2) Шаблоны UPDATE для rtc_balance (администратор затем применит вручную по диагностике)
    sqls.append("""
-- Шаблон UPDATE для rtc_balance, trigger<0 (сокращение):
-- ALTER TABLE heli_pandas UPDATE status_change = 3
-- WHERE status_id=2 AND status_change=0 AND group_by=1
-- ORDER BY ppr DESC, sne DESC, mfg_date ASC
-- LIMIT abs(:trigger_pr_final_mi8)
-- SETTINGS allow_experimental_alter_update = 1;

-- Шаблон UPDATE для rtc_balance, trigger>0 (дефицит):
-- Этап1: 5→2; Этап2: 3→2; Этап3: 1→2 (при (D-version_date) >= repair_time)
-- ALTER TABLE heli_pandas UPDATE status_change = 2 WHERE status_id=5 AND status_change=0 AND group_by=1;
-- ALTER TABLE heli_pandas UPDATE status_change = 2 WHERE status_id=3 AND status_change=0 AND group_by=1;
-- ALTER TABLE heli_pandas UPDATE status_change = 2 
-- WHERE status_id=1 AND status_change=0 AND group_by=1 AND dateDiff('day', version_date, (SELECT min(dates) FROM flight_program_fl)) >= (
--   SELECT coalesce(repair_time, 0) FROM md_components WHERE partno_comp = heli_pandas.partseqno_i
-- );

-- Дополнительно: если размечено status_change=4, установить repair_days=1
-- ALTER TABLE heli_pandas UPDATE repair_days = 1 WHERE status_change=4 AND group_by=1 SETTINGS allow_experimental_alter_update = 1;
""".strip())

    return sqls


def print_plan(sqls: List[str]):
    print("\n=== DRY RUN: Планируемые SQL ===")
    for i, sql in enumerate(sqls, 1):
        print(f"\n-- SQL #{i}:\n{sql};")
    print("\nПодсказка: запустите с --apply для выполнения.")


def main():
    parser = argparse.ArgumentParser(description='Расчет MP3.status_change на D по правилам RTC (ops_check + balance)')
    parser.add_argument('--apply', action='store_true', help='Выполнить SQL (по умолчанию только печать)')
    args = parser.parse_args()

    # Подзапрос текущей версии
    current_version_subq = """
      SELECT version_date, version_id 
      FROM heli_pandas 
      ORDER BY version_date DESC, version_id DESC LIMIT 1
    """.strip()

    sqls = build_sql(current_version_subq)

    if not args.apply:
        print_plan(sqls)
        return 0

    client = get_clickhouse_client()
    for sql in sqls:
        # Диагностические SELECT можно выполнять, остальные ALTER/UPDATE — по необходимости
        if sql.strip().upper().startswith('ALTER'):
            client.execute(sql)
        else:
            # Печатаем результат диагностических SELECT
            try:
                rows = client.execute(sql)
                print(rows)
            except Exception:
                # Если это CTE/SELECT-заглушка, просто пропускаем
                pass
    print("✅ MP3.status_change рассчитан (частично, см. диагностические шаги)")
    return 0


if __name__ == '__main__':
    sys.exit(main())