#!/usr/bin/env python3
"""
Подготовка окружения симуляции: загрузка данных MP/Property, формирование массивов дня.
Дата: 2025-08-21
"""

from __future__ import annotations
from typing import Dict, List, Tuple
from datetime import date

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from config_loader import get_clickhouse_client


def get_client():
    return get_clickhouse_client()


def fetch_versions(client) -> Tuple[date, int]:
    rows = client.execute(
        "SELECT version_date, version_id FROM heli_pandas ORDER BY version_date DESC, version_id DESC LIMIT 1"
    )
    vd, vid = rows[0]
    return vd, int(vid)


def fetch_mp1_br_rt(client) -> Dict[int, Tuple[int, int, int, int, int]]:
    """Возвращает карту partno_comp → (br_mi8, br_mi17, repair_time, partout_time, assembly_time). BR в минутах."""
    rows = client.execute("SELECT partno_comp, br_mi8, br_mi17, repair_time, partout_time, assembly_time FROM md_components")
    return {int(p): (int(b8 or 0), int(b17 or 0), int(rt or 0), int(pt or 0), int(at or 0)) for p, b8, b17, rt, pt, at in rows}


def fetch_mp3(client, vdate: date, vid: int):
    fields = [
        'partseqno_i','psn','aircraft_number','ac_type_mask','group_by','status_id','status_change',
        'll','oh','oh_threshold','sne','ppr','repair_days','mfg_date','version_date'
    ]
    sql = f"""
    SELECT {', '.join(fields)}
    FROM heli_pandas
    WHERE version_date = '{vdate}' AND version_id = {vid}
    ORDER BY psn
    """
    rows = client.execute(sql)
    return rows, fields


def preload_mp4_by_day(client) -> Dict[date, Dict[str,int]]:
    rows = client.execute(
        """
        SELECT dates, ops_counter_mi8, ops_counter_mi17, trigger_program_mi8, trigger_program_mi17
        FROM flight_program_ac
        ORDER BY dates
        """
    )
    result: Dict[date, Dict[str,int]] = {}
    for d, mi8, mi17, t8, t17 in rows:
        result[d] = {
            "ops_counter_mi8": int(mi8 or 0),
            "ops_counter_mi17": int(mi17 or 0),
            "trigger_program_mi8": int(t8 or 0),
            "trigger_program_mi17": int(t17 or 0),
        }
    return result


def preload_mp5_maps(client) -> Dict[date, Dict[int,int]]:
    rows = client.execute(
        """
        SELECT dates, aircraft_number, daily_hours
        FROM flight_program_fl
        ORDER BY dates, aircraft_number
        """
    )
    result: Dict[date, Dict[int,int]] = {}
    for d, ac, h in rows:
        m = result.setdefault(d, {})
        m[int(ac)] = int(h or 0)
    return result


def build_daily_arrays(mp3_rows, mp3_fields: List[str], mp1_br_rt_map: Dict[int, Tuple[int,int,int,int,int]], daily_today_map: Dict[int,int], daily_next_map: Dict[int,int]) -> Tuple[List[int], List[int], List[int], List[int]]:
    idx = {name: i for i, name in enumerate(mp3_fields)}
    daily_today: List[int] = []
    daily_next: List[int] = []
    partout_arr: List[int] = []
    assembly_arr: List[int] = []
    for r in mp3_rows:
        ac = int(r[idx['aircraft_number']] or 0)
        daily_today.append(int(daily_today_map.get(ac, 0)))
        daily_next.append(int(daily_next_map.get(ac, 0)))
        partseq = int(r[idx['partseqno_i']] or 0)
        # карта теперь (br_mi8, br_mi17, repair_time, partout_time, assembly_time)
        _, _, _, pt, at = mp1_br_rt_map.get(partseq, (0,0,0,0,0))
        partout_arr.append(int(pt))
        assembly_arr.append(int(at))
    return daily_today, daily_next, partout_arr, assembly_arr


