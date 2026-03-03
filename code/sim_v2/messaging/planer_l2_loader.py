#!/usr/bin/env python3
"""
Loader планерных сигналов для L2 engines (group_by=3/4).

Загружает из sim_masterv2_v9:
- day_u16
- aircraft_number
- status_id
- assembly_trigger
- daily_today_u32 (dt)

Формат массивов:
  pos = day_u16 * MAX_PLANERS + planer_idx

Fallback:
- dt из flight_program_fl, если sim_masterv2_v9 пуст/без dt

Дата: 26.02.2026
"""

import numpy as np
from typing import Dict, Tuple, Optional
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from utils.config_loader import get_clickhouse_client


MAX_PLANERS = 400
MAX_DAYS = 3651


def _version_date_to_yyyymmdd(version_date: str) -> int:
    if isinstance(version_date, str):
        if version_date.isdigit():
            return int(version_date)
        vd = date.fromisoformat(version_date)
        return int(vd.strftime("%Y%m%d"))
    return int(version_date)


def _load_planers_from_sim(client, version_date_int: int, version_id: int) -> Dict[int, int]:
    sql = """
    SELECT DISTINCT aircraft_number
    FROM sim_masterv2_v9
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND group_by IN (1, 2)
    ORDER BY aircraft_number
    """
    planers = client.execute(sql, {
        'version_date': version_date_int,
        'version_id': version_id
    })
    if not planers:
        return {}
    return {row[0]: idx for idx, row in enumerate(planers)}


def _load_planers_from_heli(client, version_date: str, version_id: int) -> Dict[int, int]:
    sql = """
    SELECT DISTINCT aircraft_number
    FROM heli_pandas
    WHERE toString(version_date) = %(version_date)s
      AND version_id = %(version_id)s
      AND group_by IN (1, 2)
      AND aircraft_number > 0
    ORDER BY aircraft_number
    """
    planers = client.execute(sql, {
        'version_date': version_date,
        'version_id': version_id
    })
    if not planers:
        return {}
    return {row[0]: idx for idx, row in enumerate(planers)}


def load_planer_signals_from_sim(version_date: str, version_id: int = 1
                                 ) -> Tuple[Optional[np.ndarray], Optional[np.ndarray],
                                            Optional[np.ndarray], Dict[int, int], int]:
    """
    Загружает dt/status/assembly из sim_masterv2_v9.
    Возвращает arrays + mapping + rows_count.
    """
    client = get_clickhouse_client()
    version_date_ymd = _version_date_to_yyyymmdd(version_date)

    ac_to_idx = _load_planers_from_sim(client, version_date_ymd, version_id)
    if not ac_to_idx:
        print(f"⚠️ Нет planers в sim_masterv2_v9 для {version_date}")
        return None, None, None, {}, 0

    sql = """
    SELECT day_u16, aircraft_number, status_id, assembly_trigger, daily_today_u32
    FROM sim_masterv2_v9
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND group_by IN (1, 2)
    ORDER BY day_u16, aircraft_number
    """
    rows = client.execute(sql, {
        'version_date': version_date_ymd,
        'version_id': version_id
    })

    dt_array = np.zeros(MAX_DAYS * MAX_PLANERS, dtype=np.uint32)
    status_array = np.zeros(MAX_DAYS * MAX_PLANERS, dtype=np.uint32)
    assembly_array = np.zeros(MAX_DAYS * MAX_PLANERS, dtype=np.uint32)

    for day_u16, ac_num, status_id, assembly_trigger, daily_today in rows:
        if ac_num in ac_to_idx and day_u16 < MAX_DAYS:
            planer_idx = ac_to_idx[ac_num]
            pos = int(day_u16) * MAX_PLANERS + planer_idx
            dt_array[pos] = int(daily_today or 0)
            status_array[pos] = int(status_id or 0)
            assembly_array[pos] = int(assembly_trigger or 0)

    total_dt = int(np.sum(dt_array)) if dt_array is not None else 0
    print(f"   ✅ sim_masterv2_v9: rows={len(rows)}, dt_sum={total_dt // 60}ч")

    return dt_array, status_array, assembly_array, ac_to_idx, len(rows)


def load_planer_dt_from_program(version_date: str, version_id: int = 1
                                ) -> Tuple[Optional[np.ndarray], Dict[int, int]]:
    """Fallback dt из flight_program_fl."""
    client = get_clickhouse_client()

    ac_to_idx = _load_planers_from_heli(client, version_date, version_id)
    if not ac_to_idx:
        print(f"⚠️ Нет planers в heli_pandas для {version_date}")
        return None, {}

    program_sql = """
    SELECT
        toUInt32(dates - version_date) as day_idx,
        aircraft_number,
        daily_hours
    FROM flight_program_fl
    WHERE toString(version_date) = %(version_date)s
      AND version_id = %(version_id)s
      AND daily_hours > 0
    ORDER BY day_idx, aircraft_number
    """
    program_data = client.execute(program_sql, {
        'version_date': version_date,
        'version_id': version_id
    })

    dt_array = np.zeros(MAX_DAYS * MAX_PLANERS, dtype=np.uint32)
    for day_idx, ac_num, daily_hours in program_data:
        if ac_num in ac_to_idx and day_idx < MAX_DAYS:
            planer_idx = ac_to_idx[ac_num]
            pos = int(day_idx) * MAX_PLANERS + planer_idx
            dt_array[pos] = int(daily_hours)

    total_dt = int(np.sum(dt_array))
    print(f"   ✅ flight_program_fl: rows={len(program_data)}, dt_sum={total_dt // 60}ч")
    return dt_array, ac_to_idx


def load_planer_signals(version_date: str, version_id: int = 1
                        ) -> Tuple[Optional[np.ndarray], Optional[np.ndarray],
                                   Optional[np.ndarray], Dict[int, int]]:
    """
    Основная функция загрузки планерных сигналов.

    Returns:
        dt_array, status_array, assembly_array, ac_to_idx
    """
    print("📊 Загрузка планерных сигналов L2...")

    dt_array, status_array, assembly_array, ac_to_idx, rows_count = load_planer_signals_from_sim(
        version_date, version_id
    )

    if dt_array is not None and int(np.sum(dt_array)) > 0:
        return dt_array, status_array, assembly_array, ac_to_idx

    print("   ⚠️ sim_masterv2_v9 пуст/без dt, fallback на flight_program_fl")
    dt_fb, ac_fb = load_planer_dt_from_program(version_date, version_id)

    if dt_fb is None:
        print("   ❌ Fallback не дал dt")
        return None, status_array, assembly_array, ac_to_idx

    # Если mapping из sim пуст — используем heli_pandas
    if not ac_to_idx:
        ac_to_idx = ac_fb

    # Если нет status/assembly, считаем планеры в operations
    if rows_count == 0 or status_array is None:
        status_array = np.full(MAX_DAYS * MAX_PLANERS, 2, dtype=np.uint32)
    if assembly_array is None:
        assembly_array = np.zeros(MAX_DAYS * MAX_PLANERS, dtype=np.uint32)

    return dt_fb, status_array, assembly_array, ac_to_idx


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--version-date", required=True)
    parser.add_argument("--version-id", type=int, default=1)
    args = parser.parse_args()

    dt, status, assembly, ac_map = load_planer_signals(args.version_date, args.version_id)
    print(f"planers={len(ac_map)}, dt={'ok' if dt is not None else 'none'}")
