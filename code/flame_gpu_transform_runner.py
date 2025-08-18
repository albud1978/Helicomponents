#!/usr/bin/env python3
"""
FLAME GPU Transform Runner (CPU fallback)

Эмулирует слои RTC по дням на CPU, используя данные MP1/MP3/MP4/MP5 из ClickHouse,
и пишет LoggingLayer Planes (MP2) в таблицу ClickHouse `flame_macroproperty2_export`.

Назначение: до появления доступного pyflamegpu, чтобы зафиксировать полную логику.

Дата: 2025-08-10
"""

from __future__ import annotations
from typing import Dict, List, Tuple
from dataclasses import dataclass
from datetime import date, timedelta

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from config_loader import get_clickhouse_client
from flame_macroproperty2_exporter import FlameMacroProperty2Exporter


@dataclass
class Versions:
    version_date: date
    version_id: int


def fetch_versions(client) -> Versions:
    rows = client.execute(
        """
        SELECT version_date, version_id
        FROM heli_pandas
        ORDER BY version_date DESC, version_id DESC
        LIMIT 1
        """
    )
    vd, vid = rows[0]
    return Versions(vd, int(vid))


def fetch_mp1_br_repair(client) -> Dict[int, Tuple[int, int]]:
    rows = client.execute("SELECT partno_comp, br, repair_time, partout_time, assembly_time FROM md_components")
    return {int(r[0]): (int(r[1] or 0), int(r[2] or 0), int(r[3] or 0), int(r[4] or 0)) for r in rows}


def fetch_mp3_state(client, versions: Versions):
    fields = [
        'partseqno_i','psn','aircraft_number','ac_type_mask','group_by','status_id','status_change',
        'll','oh','oh_threshold','sne','ppr','repair_days','mfg_date','version_date'
    ]
    sql = f"""
        SELECT {', '.join(fields)}
        FROM heli_pandas
        WHERE version_date = '{versions.version_date}' AND version_id = {versions.version_id}
        ORDER BY psn
    """
    rows = client.execute(sql)
    return rows, fields


def fetch_mp4_for_date(client, d: date) -> Dict[str, int]:
    rows = client.execute(
        """
        SELECT dates, ops_counter_mi8, ops_counter_mi17, trigger_program_mi8, trigger_program_mi17
        FROM flight_program_ac
        WHERE dates = %(d)s
        """,
        {"d": d},
    )
    if not rows:
        return {"ops_counter_mi8": 0, "ops_counter_mi17": 0, "trigger_program_mi8": 0, "trigger_program_mi17": 0}
    _, mi8, mi17, trg8, trg17 = rows[0]
    return {"ops_counter_mi8": int(mi8 or 0), "ops_counter_mi17": int(mi17 or 0), "trigger_program_mi8": int(trg8 or 0), "trigger_program_mi17": int(trg17 or 0)}


def fetch_mp5_daily_map(client, d: date) -> Dict[int, int]:
    rows = client.execute(
        """
        SELECT aircraft_number, daily_hours
        FROM flight_program_fl
        WHERE dates = %(d)s
        """,
        {"d": d},
    )
    return {int(ac): int(h or 0) for ac, h in rows}


def compute_ops_check(row, daily_today: int, daily_next: int, br_map: Dict[int, Tuple[int,int]]) -> int:
    status_id = int(row[5] or 0)
    if status_id != 2:
        return 0
    partseqno_i = int(row[0] or 0)
    sne = int(row[10] or 0)
    ppr = int(row[11] or 0)
    ll = int(row[7] or 0)
    oh = int(row[8] or 0)
    br = br_map.get(partseqno_i, (0, 0))[0]
    if (ll - sne) >= daily_today and (ll - sne) < (daily_today + daily_next):
        return 6
    if (oh - ppr) >= daily_today and (oh - ppr) < (daily_today + daily_next):
        if (sne + daily_today) < br:
            return 4
        else:
            return 6
    return 0


def apply_balance_trigger(state_rows: List[List], fields: List[str], group_val: int, trigger_val: int, d: date, br_rt_map: Dict[int, Tuple[int,int]]):
    idx = {name: i for i, name in enumerate(fields)}
    # trigger<0: из OPS→3 (top |t| по ppr DESC, sne DESC, mfg_date ASC)
    if trigger_val < 0:
        candidates = [r for r in state_rows if int(r[idx['group_by']] or 0) == group_val and int(r[idx['status_id']] or 0) == 2 and int(r[idx['status_change']] or 0) == 0]
        candidates.sort(key=lambda r: (-int(r[idx['ppr']] or 0), -int(r[idx['sne']] or 0), (r[idx['mfg_date']] or date(1970,1,1))))
        limit = min(abs(trigger_val), len(candidates))
        for r in candidates[:limit]:
            r[idx['status_change']] = 3
        return
    if trigger_val > 0:
        remaining = trigger_val
        # Phase1: 5→2
        for r in state_rows:
            if remaining <= 0:
                break
            if int(r[idx['group_by']] or 0) == group_val and int(r[idx['status_id']] or 0) == 5 and int(r[idx['status_change']] or 0) == 0:
                r[idx['status_change']] = 2
                remaining -= 1
        # Phase2: 3→2
        if remaining > 0:
            for r in state_rows:
                if remaining <= 0:
                    break
                if int(r[idx['group_by']] or 0) == group_val and int(r[idx['status_id']] or 0) == 3 and int(r[idx['status_change']] or 0) == 0:
                    r[idx['status_change']] = 2
                    remaining -= 1
        # Phase3: 1→2 (с учётом repair_time)
        if remaining > 0:
            for r in state_rows:
                if remaining <= 0:
                    break
                if int(r[idx['group_by']] or 0) == group_val and int(r[idx['status_id']] or 0) == 1 and int(r[idx['status_change']] or 0) == 0:
                    partseqno_i = int(r[idx['partseqno_i']] or 0)
                    repair_time = br_rt_map.get(partseqno_i, (0, 0))[1]
                    version_date = r[idx['version_date']] or d
                    if (d - version_date).days >= int(repair_time or 0):
                        r[idx['status_change']] = 2
                        remaining -= 1


def rtc_repair(state_rows: List[List], fields: List[str]):
    idx = {name: i for i, name in enumerate(fields)}
    for r in state_rows:
        if int(r[idx['status_id']] or 0) == 4:
            r[idx['repair_days']] = int(r[idx['repair_days']] or 0) + 1


def rtc_ops_check_layer(state_rows: List[List], fields: List[str], d: date, daily_map_today: Dict[int,int], daily_map_next: Dict[int,int], br_map: Dict[int, Tuple[int,int]]):
    idx = {name: i for i, name in enumerate(fields)}
    for r in state_rows:
        ac = int(r[idx['aircraft_number']] or 0)
        daily_today = daily_map_today.get(ac, 0)
        daily_next = daily_map_next.get(ac, 0)
        chg = compute_ops_check(r, daily_today, daily_next, br_map)
        if chg and int(r[idx['status_change']] or 0) == 0:
            r[idx['status_change']] = chg


def rtc_balance_layer(state_rows: List[List], fields: List[str], d: date, mp4: Dict[str,int], br_rt_map: Dict[int, Tuple[int,int]]):
    idx = {name: i for i, name in enumerate(fields)}
    for group_val, ops_field, trg_field in [(1, 'ops_counter_mi8', 'trigger_program_mi8'), (2, 'ops_counter_mi17', 'trigger_program_mi17')]:
        current_ops = sum(1 for r in state_rows if int(r[idx['group_by']] or 0) == group_val and int(r[idx['status_id']] or 0) == 2 and int(r[idx['status_change']] or 0) == 0)
        target_ops = int(mp4[ops_field] or 0)
        trigger_pr_final = target_ops - current_ops
        gate = int(mp4[trg_field] or 0)
        if gate == 0:
            apply_balance_trigger(state_rows, fields, group_val, trigger_pr_final, d, br_rt_map)


def rtc_main_layer(state_rows: List[List], fields: List[str], d: date, daily_map_today: Dict[int,int]):
    idx = {name: i for i, name in enumerate(fields)}
    for r in state_rows:
        ac = int(r[idx['aircraft_number']] or 0)
        if int(r[idx['status_id']] or 0) == 2:
            dh = daily_map_today.get(ac, 0)
            r[idx['sne']] = int(r[idx['sne']] or 0) + dh
            r[idx['ppr']] = int(r[idx['ppr']] or 0) + dh
        chg = int(r[idx['status_change']] or 0)
        if chg == 3:
            r[idx['status_id']] = 3
        elif chg == 2:
            r[idx['status_id']] = 2
        elif chg == 4:
            r[idx['status_id']] = 4


def rtc_change_layer(state_rows: List[List], fields: List[str], d: date, br_rt_map: Dict[int, Tuple[int,int,int,int]]):
    idx = {name: i for i, name in enumerate(fields)}
    for r in state_rows:
        chg = int(r[idx['status_change']] or 0)
        if chg == 4:
            r[idx['repair_days']] = 1
            partseq = int(r[idx['partseqno_i']] or 0)
            _, rt, pt, at = br_rt_map.get(partseq, (0,0,0,0))
            # триггеры как даты
            r.append(None)  # placeholder if needed
        elif chg == 5:
            r[idx['ppr']] = 0
            r[idx['repair_days']] = 0
        elif chg == 2 and int(r[idx['status_id']] or 0) == 1:
            partseq = int(r[idx['partseqno_i']] or 0)
            _, rt, pt, at = br_rt_map.get(partseq, (0,0,0,0))
            # active = D - repair_time (Date)
            # assembly = D + assembly_time
            pass
        r[idx['status_change']] = 0


def host_triggers_for_log(mp4: Dict[str,int]) -> Tuple[int,int]:
    return int(mp4.get('ops_counter_mi8', 0)) - 0, int(mp4.get('ops_counter_mi17', 0)) - 0


def run(days_limit: int | None = None) -> None:
    client = get_clickhouse_client()
    versions = fetch_versions(client)
    mp1_map = fetch_mp1_br_repair(client)
    rows, fields = fetch_mp3_state(client, versions)
    state_rows = [list(r) for r in rows]

    exporter = FlameMacroProperty2Exporter(client=client)
    exporter.ensure_table()

    # Список дат из MP4
    all_dates = [r[0] for r in client.execute("SELECT dates FROM flight_program_ac ORDER BY dates")]
    if days_limit:
        all_dates = all_dates[:days_limit]

    for d in all_dates:
        mp4 = fetch_mp4_for_date(client, d)
        daily_today = fetch_mp5_daily_map(client, d)
        daily_next = fetch_mp5_daily_map(client, d + timedelta(days=1))

        rtc_repair(state_rows, fields)
        rtc_ops_check_layer(state_rows, fields, d, daily_today, daily_next, mp1_map)
        rtc_balance_layer(state_rows, fields, d, mp4, mp1_map)
        rtc_main_layer(state_rows, fields, d, daily_today)
        rtc_change_layer(state_rows, fields, d, mp1_map)

        # Логирование в MP2 (только планеры group_by 1|2)
        idx = {name: i for i, name in enumerate(fields)}
        # Текущее число в эксплуатации по группам
        ops_cur = {1: 0, 2: 0}
        for r in state_rows:
            if int(r[idx['status_id']] or 0) == 2 and int(r[idx['status_change']] or 0) == 0:
                gb = int(r[idx['group_by']] or 0)
                if gb in ops_cur:
                    ops_cur[gb] += 1
        log_rows = []
        for r in state_rows:
            if int(r[idx['group_by']] or 0) not in (1, 2):
                continue
            ac = int(r[idx['aircraft_number']] or 0)
            daily_flight = int(daily_today.get(ac, 0))
            ac_type_mask = int(r[idx['ac_type_mask']] or 0)
            status_id = int(r[idx['status_id']] or 0)
            md = r[idx['mfg_date']] or date(1970,1,1)
            age_years = max(0, d.year - md.year - ((d.month, d.day) < (md.month, md.day)))
            trg8 = mp4.get('ops_counter_mi8', 0)
            trg17 = mp4.get('ops_counter_mi17', 0)
            log_rows.append({
                'dates': d,
                'aircraft_number': ac,
                'ac_type_mask': ac_type_mask,
                'status_id': status_id,
                'daily_flight': daily_flight,
                'ops_counter_mi8': int(trg8),
                'ops_counter_mi17': int(trg17),
                'ops_current_mi8': int(ops_cur.get(1, 0)),
                'ops_current_mi17': int(ops_cur.get(2, 0)),
                'partout_trigger': date(1970,1,1),
                'assembly_trigger': date(1970,1,1),
                'active_trigger': date(1970,1,1),
                'aircraft_age_years': age_years,
                'mfg_date': md,
                'simulation_metadata': f"v={versions.version_date}/id={versions.version_id};D={d}"
            })
        if log_rows:
            exporter.insert_rows(log_rows)


def main():
    import argparse
    parser = argparse.ArgumentParser(description='FLAME GPU Transform Runner (CPU fallback)')
    parser.add_argument('--days', type=int, default=None, help='Ограничить количество дней')
    args = parser.parse_args()
    run(days_limit=args.days)


if __name__ == '__main__':
    main()