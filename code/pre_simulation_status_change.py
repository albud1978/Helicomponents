#!/usr/bin/env python3
"""
Pre-simulation Status Change Calculator (MP3)

Рассчитывает status_change в таблице heli_pandas на дату D (текущая дата симуляции)
по правилам разметки RTC (ops_check + balance) на базе MP1/MP3/MP4/MP5, без начисления
sne/ppr и без финальных переходов. Поддерживает dry-run (SQL печать) по умолчанию.

Правила (кратко):
- Фильтры по group_by (1=МИ-8Т, 2=МИ-17)
- rtc_ops_check (LL/OH/BR с daily_today/daily_next): выставляет status_change in (4,6)
- host trigger: trigger_pr_final_{grp} = target_ops(D) - current_ops(D) [диагностика]

Дата: 2025-08-10
"""

import argparse
import sys
from typing import List, Dict, Tuple
from datetime import datetime, date, timedelta
from pathlib import Path

sys.path.append(str(__file__).rsplit('/code/', 1)[0] + '/code/utils')
from config_loader import get_clickhouse_client


def fetch_current_version(client) -> Tuple[date, int]:
    row = client.execute(
        "SELECT version_date, version_id FROM heli_pandas ORDER BY version_date DESC, version_id DESC LIMIT 1"
    )[0]
    return row[0], int(row[1])


def fetch_d0_date(client, vdate: date, vid: int) -> date:
    row = client.execute(
        "SELECT min(dates) FROM flight_program_fl WHERE (version_date, version_id) = (%(vd)s, %(vid)s)",
        {"vd": vdate, "vid": vid},
    )[0]
    return row[0]


def fetch_daily_maps(client, d0: date) -> Tuple[Dict[int, int], Dict[int, int]]:
    rows_today = client.execute(
        "SELECT aircraft_number, daily_hours FROM flight_program_fl WHERE dates = %(d)s",
        {"d": d0},
    )
    rows_next = client.execute(
        "SELECT aircraft_number, daily_hours FROM flight_program_fl WHERE dates = %(d)s",
        {"d": d0 + timedelta(days=1)},
    )
    today_map = {int(ac): int(h or 0) for ac, h in rows_today}
    next_map = {int(ac): int(h or 0) for ac, h in rows_next}
    return today_map, next_map


def fetch_br_map(client) -> Dict[int, Tuple[int, int]]:
    # Возвращает карту partno_comp → (br_mi8, br_mi17) в минутах
    rows = client.execute("SELECT partno_comp, br_mi8, br_mi17 FROM md_components")
    return {int(p or 0): (int(b8 or 0), int(b17 or 0)) for p, b8, b17 in rows}


def fetch_mp3_rows(client, vdate: date, vid: int) -> Tuple[List[Tuple], List[str]]:
    fields = [
        'partseqno_i','psn','aircraft_number','group_by','ac_type_mask','status_id','status_change',
        'll','oh','oh_threshold','sne','ppr','mfg_date','version_date'
    ]
    sql = f"""
        SELECT {', '.join(fields)}
        FROM heli_pandas
        WHERE version_date = %(vd)s AND version_id = %(vid)s
    """
    rows = client.execute(sql, {"vd": vdate, "vid": vid})
    return rows, fields


def ensure_status_change_column(client) -> None:
    # Добавим колонку при отсутствии
    client.execute("""
        ALTER TABLE heli_pandas
        ADD COLUMN IF NOT EXISTS status_change UInt8 DEFAULT 0
    """)


def reset_status_change(client, vdate: date, vid: int) -> None:
    client.execute(
        """
        ALTER TABLE heli_pandas
        UPDATE status_change = 0
        WHERE status_change != 0 AND version_date = %(vd)s AND version_id = %(vid)s
        """,
        {"vd": vdate, "vid": vid},
    )


def compute_status_change_sets(rows: List[Tuple], fields: List[str], today_map: Dict[int,int], next_map: Dict[int,int], br_map: Dict[int,Tuple[int,int]], group_filter: List[int]) -> Tuple[List[int], List[int]]:
    idx = {name: i for i, name in enumerate(fields)}
    to_4: List[int] = []
    to_6: List[int] = []
    for r in rows:
        gb = int(r[idx['group_by']] or 0)
        if gb not in group_filter:
            continue
        status_id = int(r[idx['status_id']] or 0)
        if status_id != 2:
            continue
        if int(r[idx['status_change']] or 0) != 0:
            continue
        ac = int(r[idx['aircraft_number']] or 0)
        dt = int(today_map.get(ac, 0))
        dn = int(next_map.get(ac, 0))
        partseq = int(r[idx['partseqno_i']] or 0)
        ll = int(r[idx['ll']] or 0)
        oh = int(r[idx['oh']] or 0)
        sne = int(r[idx['sne']] or 0)
        ppr = int(r[idx['ppr']] or 0)
        # Выбор BR по маске типов (минуты)
        mask = int(r[idx['ac_type_mask']] or 0)
        br_mi8, br_mi17 = br_map.get(partseq, (0, 0))
        br = 0
        if mask & 32:
            br = br_mi8
        elif mask & 64:
            br = br_mi17
        else:
            # fallback: если нет маски, используем максимальный из доступных
            br = max(br_mi8, br_mi17)
        # Логика rtc_ops_check
        ll_edge = (ll - sne)
        oh_edge = (oh - ppr)
        # 4: ремонтопригодный OH переход через границу сегодня→завтра
        cond_oh_cross = (oh_edge >= dt) and (oh_edge < (dt + dn))
        cond_ll_cross = (ll_edge >= dt) and (ll_edge < (dt + dn))
        if cond_oh_cross and (sne + dt) < br:
            to_4.append(int(r[idx['psn']] or 0))
            continue
        # 6: либо LL пересечение, либо OH пересечение и неремонтопригодность
        if cond_ll_cross or (cond_oh_cross and (sne + dt) >= br):
            to_6.append(int(r[idx['psn']] or 0))
            continue
    # Убираем нули и дубликаты
    to_4 = [psn for psn in set(to_4) if psn]
    to_6 = [psn for psn in set(to_6) if psn and psn not in to_4]
    return to_4, to_6


def chunked(lst: List[int], size: int = 1000) -> List[List[int]]:
    return [lst[i:i+size] for i in range(0, len(lst), size)]


def apply_updates(client, vdate: date, vid: int, psn_list: List[int], new_val: int) -> None:
    if not psn_list:
        return
    for chunk in chunked(psn_list, 1000):
        sql = """
        ALTER TABLE heli_pandas
        UPDATE status_change = %(val)s
        WHERE version_date = %(vd)s AND version_id = %(vid)s AND psn IN ({ids})
        """.replace("{ids}", ",".join(str(x) for x in chunk))
        client.execute(sql, {"val": int(new_val), "vd": vdate, "vid": vid})


def main():
    parser = argparse.ArgumentParser(description='Расчет MP3.status_change на D по правилам RTC (ops_check)')
    parser.add_argument('--apply', action='store_true', help='Выполнить UPDATE (по умолчанию только печать)')
    parser.add_argument('--group', choices=['all', '1', '2'], default='all', help='Ограничить расчёт группой (1=МИ‑8Т, 2=МИ‑17)')
    args = parser.parse_args()

    client = get_clickhouse_client()

    # Гарантируем колонку
    ensure_status_change_column(client)

    vdate, vid = fetch_current_version(client)
    d0 = fetch_d0_date(client, vdate, vid)
    today_map, next_map = fetch_daily_maps(client, d0)
    br_map = fetch_br_map(client)
    rows, fields = fetch_mp3_rows(client, vdate, vid)

    groups = [1, 2] if args.group == 'all' else [int(args.group)]
    to_4, to_6 = compute_status_change_sets(rows, fields, today_map, next_map, br_map, groups)

    print(f"📅 Версия: {vdate} v{vid} | D0={d0}")
    print(f"🧮 Вычислено: to_4={len(to_4)}, to_6={len(to_6)}")

    if not args.apply:
        # DRY RUN — печать первых 10
        print(f"🔎 Примеры to_4 (первые 10): {to_4[:10]}")
        print(f"🔎 Примеры to_6 (первые 10): {to_6[:10]}")
        return 0

    # Сброс прошлой разметки для текущей версии
    reset_status_change(client, vdate, vid)

    # Применяем изменения
    apply_updates(client, vdate, vid, to_4, 4)
    apply_updates(client, vdate, vid, to_6, 6)

    print("✅ MP3.status_change рассчитан и применён")
    return 0


if __name__ == '__main__':
    sys.exit(main())