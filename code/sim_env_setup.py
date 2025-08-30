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


def fetch_mp1_oh(client) -> Dict[int, Tuple[int, int]]:
    """Возвращает карту partno_comp → (oh_mi8, oh_mi17). Единицы ожидаются в минутах."""
    rows = client.execute("SELECT partno_comp, oh_mi8, oh_mi17 FROM md_components")
    return {int(p): (int(oh8 or 0), int(oh17 or 0)) for p, oh8, oh17 in rows}


def fetch_mp3(client, vdate: date, vid: int):
    fields = [
        'partseqno_i','psn','aircraft_number','ac_type_mask','group_by','status_id',
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


# === Full‑GPU подготовка окружения (Этап 0) ===

def build_frames_index(mp3_rows, mp3_fields: List[str]) -> Tuple[Dict[int, int], int]:
    idx = {name: i for i, name in enumerate(mp3_fields)}
    ac_set = set()
    for r in mp3_rows:
        # Отбираем только планеры: group_by ∈ {1,2} ИЛИ маска типа с битами 32/64
        ac = int(r[idx['aircraft_number']] or 0)
        if ac <= 0:
            continue
        is_plane = False
        if 'group_by' in idx:
            gb = int(r[idx['group_by']] or 0)
            is_plane = gb in (1, 2)
        elif 'ac_type_mask' in idx:
            m = int(r[idx['ac_type_mask']] or 0)
            is_plane = (m & (32 | 64)) != 0
        else:
            # Если нет признаков — консервативно исключаем
            is_plane = False
        if is_plane:
            ac_set.add(ac)
    # 0 как «нет борта» исключаем
    ac_list = sorted([ac for ac in ac_set if ac > 0])
    frames_index = {ac: i for i, ac in enumerate(ac_list)}
    return frames_index, len(ac_list)


def get_days_sorted(mp4_by_day: Dict[date, Dict[str, int]]) -> List[date]:
    return sorted(mp4_by_day.keys())


def build_mp5_linear(mp5_by_day: Dict[date, Dict[int, int]], days_sorted: List[date], frames_index: Dict[int, int], frames_total: int) -> List[int]:
    days_total = len(days_sorted)
    # Паддинг D+1 в конце
    size = (days_total + 1) * frames_total
    arr = [0] * size
    for d_idx, D in enumerate(days_sorted):
        by_ac = mp5_by_day.get(D, {})
        base = d_idx * frames_total
        for ac, hours in by_ac.items():
            fi = frames_index.get(int(ac), -1)
            if fi >= 0:
                arr[base + fi] = int(hours or 0)
    # Последний день (паддинг) оставляем нулями
    return arr


def build_mp1_arrays(mp1_map: Dict[int, Tuple[int, int, int, int, int]]) -> Tuple[List[int], List[int], List[int], List[int], List[int], Dict[int,int]]:
    """Строит SoA массивы MP1 и индекс partseqno_i->idx."""
    keys = sorted(mp1_map.keys())
    idx_map: Dict[int,int] = {k: i for i, k in enumerate(keys)}
    br8: List[int] = []
    br17: List[int] = []
    rt: List[int] = []
    pt: List[int] = []
    at: List[int] = []
    for k in keys:
        b8, b17, rti, pti, ati = mp1_map.get(k, (0,0,0,0,0))
        br8.append(int(b8 or 0))
        br17.append(int(b17 or 0))
        rt.append(int(rti or 0))
        pt.append(int(pti or 0))
        at.append(int(ati or 0))
    return br8, br17, rt, pt, at, idx_map


def build_mp3_arrays(mp3_rows, mp3_fields: List[str]) -> Dict[str, List[int]]:
    idx = {name: i for i, name in enumerate(mp3_fields)}
    to_u32 = lambda v: int(v or 0)
    to_u16 = lambda v: int(v or 0)
    arr: Dict[str, List[int]] = {
        'mp3_psn': [],
        'mp3_aircraft_number': [],
        'mp3_ac_type_mask': [],
        'mp3_group_by': [],
        'mp3_status_id': [],
        'mp3_sne': [],
        'mp3_ppr': [],
        'mp3_repair_days': [],
        'mp3_ll': [],
        'mp3_oh': [],
        'mp3_mfg_date_days': [],
    }
    from datetime import date as _date
    epoch = _date(1970,1,1)
    for r in mp3_rows:
        arr['mp3_psn'].append(to_u32(r[idx['psn']]))
        arr['mp3_aircraft_number'].append(to_u32(r[idx['aircraft_number']]))
        arr['mp3_ac_type_mask'].append(to_u16(r[idx['ac_type_mask']]))
        arr['mp3_group_by'].append(to_u16(r[idx.get('group_by', -1)] if 'group_by' in idx else 0))
        arr['mp3_status_id'].append(to_u16(r[idx['status_id']]))
        arr['mp3_sne'].append(to_u32(r[idx['sne']]))
        arr['mp3_ppr'].append(to_u32(r[idx['ppr']]))
        arr['mp3_repair_days'].append(to_u16(r[idx['repair_days']]))
        arr['mp3_ll'].append(to_u32(r[idx['ll']]))
        arr['mp3_oh'].append(to_u32(r[idx['oh']]))
        md = r[idx.get('mfg_date', -1)] if 'mfg_date' in idx else None
        ord_days = 0
        if md:
            try:
                ord_days = max(0, int((md - epoch).days))
            except Exception:
                ord_days = 0
        arr['mp3_mfg_date_days'].append(to_u16(ord_days))
    return arr


def build_mp4_arrays(mp4_by_day: Dict[date, Dict[str, int]], days_sorted: List[date]) -> Tuple[List[int], List[int]]:
    ops8: List[int] = []
    ops17: List[int] = []
    for D in days_sorted:
        m = mp4_by_day.get(D, {})
        ops8.append(int(m.get('ops_counter_mi8', 0)))
        ops17.append(int(m.get('ops_counter_mi17', 0)))
    return ops8, ops17


def days_to_epoch_u16(d: date) -> int:
    # ClickHouse Date совместимо: дни от 1970‑01‑01
    from datetime import date as _date
    epoch = _date(1970, 1, 1)
    diff = (d - epoch).days
    return max(0, int(diff))


def prepare_env_arrays(client) -> Dict[str, object]:
    """Формирует все Env массивы/скаляры для full‑GPU окружения (без применения к модели)."""
    vdate, vid = fetch_versions(client)
    mp3_rows, mp3_fields = fetch_mp3(client, vdate, vid)
    mp1_map = fetch_mp1_br_rt(client)
    mp1_oh_map = fetch_mp1_oh(client)
    mp4_by_day = preload_mp4_by_day(client)
    mp5_by_day = preload_mp5_maps(client)

    days_sorted = get_days_sorted(mp4_by_day)
    frames_index, frames_total = build_frames_index(mp3_rows, mp3_fields)
    mp5_linear = build_mp5_linear(mp5_by_day, days_sorted, frames_index, frames_total)
    mp4_ops8, mp4_ops17 = build_mp4_arrays(mp4_by_day, days_sorted)
    mp1_br8, mp1_br17, mp1_rt, mp1_pt, mp1_at, mp1_index = build_mp1_arrays(mp1_map)
    # Соберём массивы OH по индексу MP1
    keys_sorted = sorted(mp1_index.keys(), key=lambda k: mp1_index[k])
    mp1_oh8_arr: List[int] = []
    mp1_oh17_arr: List[int] = []
    for k in keys_sorted:
        oh8, oh17 = mp1_oh_map.get(k, (0, 0))
        mp1_oh8_arr.append(int(oh8 or 0))
        mp1_oh17_arr.append(int(oh17 or 0))
    mp3_arrays = build_mp3_arrays(mp3_rows, mp3_fields)

    env_data = {
        'version_date_u16': days_to_epoch_u16(vdate),
        'frames_total_u16': int(frames_total),
        'days_total_u16': int(len(days_sorted)),
        'days_sorted': days_sorted,
        'frames_index': frames_index,
        'mp4_ops_counter_mi8': mp4_ops8,
        'mp4_ops_counter_mi17': mp4_ops17,
        'mp5_daily_hours_linear': mp5_linear,
        'mp1_br_mi8': mp1_br8,
        'mp1_br_mi17': mp1_br17,
        'mp1_repair_time': mp1_rt,
        'mp1_partout_time': mp1_pt,
        'mp1_assembly_time': mp1_at,
        'mp1_oh_mi8': mp1_oh8_arr,
        'mp1_oh_mi17': mp1_oh17_arr,
        'mp1_index': mp1_index,
        'mp3_arrays': mp3_arrays,
        'mp3_count': len(mp3_rows),
    }
    # Валидации форм и размеров (жёсткие assert'ы для раннего обнаружения ошибок)
    dt = int(env_data['days_total_u16'])
    ft = int(env_data['frames_total_u16'])
    assert len(env_data['mp4_ops_counter_mi8']) == dt, "MP4_mi8 размер не равен days_total"
    assert len(env_data['mp4_ops_counter_mi17']) == dt, "MP4_mi17 размер не равен days_total"
    assert len(env_data['mp5_daily_hours_linear']) == (dt + 1) * ft, "MP5_linear размер != (days_total+1)*frames_total"
    # mp3_arrays длины согласованы
    a = env_data['mp3_arrays']
    n3 = int(env_data['mp3_count'])
    for k in ('mp3_psn','mp3_aircraft_number','mp3_ac_type_mask','mp3_group_by','mp3_status_id','mp3_sne','mp3_ppr','mp3_repair_days','mp3_ll','mp3_oh','mp3_mfg_date_days'):
        assert len(a.get(k, [])) == n3, f"MP3 SoA поле {k} имеет несогласованную длину"
    return env_data


def apply_env_to_sim(sim, env_data: Dict[str, object]):
    """Применяет подготовленные массивы к Env pyflamegpu модели (скаляры и Property Arrays)."""
    # Скаляры
    sim.setEnvironmentPropertyUInt("version_date", int(env_data['version_date_u16']))
    sim.setEnvironmentPropertyUInt("frames_total", int(env_data['frames_total_u16']))
    sim.setEnvironmentPropertyUInt("days_total", int(env_data['days_total_u16']))
    # MP4
    sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8", list(env_data['mp4_ops_counter_mi8']))
    sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi17", list(env_data['mp4_ops_counter_mi17']))
    # MP5 (линейный массив с паддингом)
    sim.setEnvironmentPropertyArrayUInt32("mp5_daily_hours", list(env_data['mp5_daily_hours_linear']))
    # MP1 (SoA)
    if 'mp1_br_mi8' in env_data:
        sim.setEnvironmentPropertyArrayUInt32("mp1_br_mi8", list(env_data['mp1_br_mi8']))
        sim.setEnvironmentPropertyArrayUInt32("mp1_br_mi17", list(env_data['mp1_br_mi17']))
        sim.setEnvironmentPropertyArrayUInt32("mp1_repair_time", list(env_data['mp1_repair_time']))
        sim.setEnvironmentPropertyArrayUInt32("mp1_partout_time", list(env_data['mp1_partout_time']))
        sim.setEnvironmentPropertyArrayUInt32("mp1_assembly_time", list(env_data['mp1_assembly_time']))
        if 'mp1_oh_mi8' in env_data:
            sim.setEnvironmentPropertyArrayUInt32("mp1_oh_mi8", list(env_data['mp1_oh_mi8']))
        if 'mp1_oh_mi17' in env_data:
            sim.setEnvironmentPropertyArrayUInt32("mp1_oh_mi17", list(env_data['mp1_oh_mi17']))
    # MP3 (SoA)
    if 'mp3_arrays' in env_data:
        a = env_data['mp3_arrays']
        sim.setEnvironmentPropertyArrayUInt32("mp3_psn", list(a['mp3_psn']))
        sim.setEnvironmentPropertyArrayUInt32("mp3_aircraft_number", list(a['mp3_aircraft_number']))
        sim.setEnvironmentPropertyArrayUInt32("mp3_ac_type_mask", list(a['mp3_ac_type_mask']))
        sim.setEnvironmentPropertyArrayUInt32("mp3_status_id", list(a['mp3_status_id']))
        sim.setEnvironmentPropertyArrayUInt32("mp3_sne", list(a['mp3_sne']))
        sim.setEnvironmentPropertyArrayUInt32("mp3_ppr", list(a['mp3_ppr']))
        sim.setEnvironmentPropertyArrayUInt32("mp3_repair_days", list(a['mp3_repair_days']))
        sim.setEnvironmentPropertyArrayUInt32("mp3_ll", list(a['mp3_ll']))
        sim.setEnvironmentPropertyArrayUInt32("mp3_oh", list(a['mp3_oh']))
        sim.setEnvironmentPropertyArrayUInt32("mp3_mfg_date_days", list(a['mp3_mfg_date_days']))
    # MP6 будет инициализироваться на GPU из MP4 в отдельном шаге (rtc_quota_init или отдельный init)
    return None


