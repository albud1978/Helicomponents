#!/usr/bin/env python3
"""
Этап 0: Env-only смоук — загрузить MP1/MP3/MP4/MP5 и скаляры в Env и прочитать обратно на host.
Без агентов и RTC.
Дата: 2025-08-28
"""

from __future__ import annotations
from typing import Dict, List, Tuple
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from config_loader import get_clickhouse_client
from sim_env_setup import (
    fetch_versions,
    fetch_mp1_br_rt,
    fetch_mp3,
    preload_mp4_by_day,
    preload_mp5_maps,
    build_frames_index,
    build_mp5_linear,
    build_mp4_arrays,
    build_mp1_arrays,
    build_mp3_arrays,
    days_to_epoch_u16,
)


def ensure_cuda_path():
    if not os.environ.get('CUDA_PATH'):
        for p in [
            "/usr/local/cuda",
            "/usr/local/cuda-12.4",
            "/usr/local/cuda-12.3",
            "/usr/local/cuda-12.2",
            "/usr/local/cuda-12.1",
            "/usr/local/cuda-12.0",
        ]:
            if os.path.isdir(p) and os.path.isdir(os.path.join(p, 'include')):
                os.environ['CUDA_PATH'] = p
                break


def main():
    ensure_cuda_path()
    import pyflamegpu

    client = get_clickhouse_client()
    vdate, vid = fetch_versions(client)
    mp3_rows, mp3_fields = fetch_mp3(client, vdate, vid)
    mp1_map = fetch_mp1_br_rt(client)
    mp4_by_day = preload_mp4_by_day(client)
    mp5_by_day = preload_mp5_maps(client)

    days_sorted = sorted(mp4_by_day.keys())
    if not days_sorted:
        print("❌ MP4 пуст: нет дат")
        sys.exit(1)

    frames_index, frames_total = build_frames_index(mp3_rows, mp3_fields, mp5_by_day)
    days_total = len(days_sorted)
    mp5_linear = build_mp5_linear(mp5_by_day, days_sorted, frames_index, frames_total)
    mp4_ops8, mp4_ops17 = build_mp4_arrays(mp4_by_day, days_sorted)
    mp1_br8, mp1_br17, mp1_rt, mp1_pt, mp1_at, _ = build_mp1_arrays(mp1_map)
    mp3_arrays = build_mp3_arrays(mp3_rows, mp3_fields)
    version_date_u16 = days_to_epoch_u16(vdate)

    # Модель: только Env c корректными размерами массивов
    model = pyflamegpu.ModelDescription("HeliSimStage0EnvOnly")
    env = model.Environment()
    env.newPropertyUInt("version_date", 0)
    env.newPropertyUInt("frames_total", 0)
    env.newPropertyUInt("days_total", 0)
    # Host fallback квоты (на D+1)
    env.newPropertyUInt("quota_next_mi8", 0)
    env.newPropertyUInt("quota_next_mi17", 0)
    # MP4
    env.newPropertyArrayUInt32("mp4_ops_counter_mi8", [0] * days_total)
    env.newPropertyArrayUInt32("mp4_ops_counter_mi17", [0] * days_total)
    # MP5
    env.newPropertyArrayUInt32("mp5_daily_hours", [0] * ((days_total + 1) * frames_total))
    # MP1
    env.newPropertyArrayUInt32("mp1_br_mi8", [0] * len(mp1_br8))
    env.newPropertyArrayUInt32("mp1_br_mi17", [0] * len(mp1_br17))
    env.newPropertyArrayUInt32("mp1_repair_time", [0] * len(mp1_rt))
    env.newPropertyArrayUInt32("mp1_partout_time", [0] * len(mp1_pt))
    env.newPropertyArrayUInt32("mp1_assembly_time", [0] * len(mp1_at))
    # MP3
    n_mp3 = len(mp3_arrays['mp3_psn'])
    env.newPropertyArrayUInt32("mp3_psn", [0] * n_mp3)
    env.newPropertyArrayUInt32("mp3_aircraft_number", [0] * n_mp3)
    env.newPropertyArrayUInt32("mp3_ac_type_mask", [0] * n_mp3)
    env.newPropertyArrayUInt32("mp3_status_id", [0] * n_mp3)
    env.newPropertyArrayUInt32("mp3_sne", [0] * n_mp3)
    env.newPropertyArrayUInt32("mp3_ppr", [0] * n_mp3)
    env.newPropertyArrayUInt32("mp3_repair_days", [0] * n_mp3)
    env.newPropertyArrayUInt32("mp3_ll", [0] * n_mp3)
    env.newPropertyArrayUInt32("mp3_oh", [0] * n_mp3)
    env.newPropertyArrayUInt32("mp3_mfg_date_days", [0] * n_mp3)

    sim = pyflamegpu.CUDASimulation(model)
    # Скаляры
    sim.setEnvironmentPropertyUInt("version_date", int(version_date_u16))
    sim.setEnvironmentPropertyUInt("frames_total", int(frames_total))
    sim.setEnvironmentPropertyUInt("days_total", int(days_total))
    # Проставим квоты на D+1 из MP4 (host fallback)
    d1 = 1 if days_total > 1 else 0
    sim.setEnvironmentPropertyUInt32("quota_next_mi8", int(mp4_ops8[d1]))
    sim.setEnvironmentPropertyUInt32("quota_next_mi17", int(mp4_ops17[d1]))
    # Массивы MP4/MP5
    sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8", list(mp4_ops8))
    sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi17", list(mp4_ops17))
    sim.setEnvironmentPropertyArrayUInt32("mp5_daily_hours", list(mp5_linear))
    # Массивы MP1
    sim.setEnvironmentPropertyArrayUInt32("mp1_br_mi8", list(mp1_br8))
    sim.setEnvironmentPropertyArrayUInt32("mp1_br_mi17", list(mp1_br17))
    sim.setEnvironmentPropertyArrayUInt32("mp1_repair_time", list(mp1_rt))
    sim.setEnvironmentPropertyArrayUInt32("mp1_partout_time", list(mp1_pt))
    sim.setEnvironmentPropertyArrayUInt32("mp1_assembly_time", list(mp1_at))
    # Массивы MP3
    sim.setEnvironmentPropertyArrayUInt32("mp3_psn", list(mp3_arrays['mp3_psn']))
    sim.setEnvironmentPropertyArrayUInt32("mp3_aircraft_number", list(mp3_arrays['mp3_aircraft_number']))
    sim.setEnvironmentPropertyArrayUInt32("mp3_ac_type_mask", list(mp3_arrays['mp3_ac_type_mask']))
    sim.setEnvironmentPropertyPropertyArrayUInt32 = sim.setEnvironmentPropertyArrayUInt32  # alias safety
    sim.setEnvironmentPropertyArrayUInt32("mp3_status_id", list(mp3_arrays['mp3_status_id']))
    sim.setEnvironmentPropertyArrayUInt32("mp3_sne", list(mp3_arrays['mp3_sne']))
    sim.setEnvironmentPropertyArrayUInt32("mp3_ppr", list(mp3_arrays['mp3_ppr']))
    sim.setEnvironmentPropertyArrayUInt32("mp3_repair_days", list(mp3_arrays['mp3_repair_days']))
    sim.setEnvironmentPropertyArrayUInt32("mp3_ll", list(mp3_arrays['mp3_ll']))
    sim.setEnvironmentPropertyArrayUInt32("mp3_oh", list(mp3_arrays['mp3_oh']))
    sim.setEnvironmentPropertyArrayUInt32("mp3_mfg_date_days", list(mp3_arrays['mp3_mfg_date_days']))

    # Чтение обратно на host
    vd = sim.getEnvironmentPropertyUInt("version_date")
    ft = sim.getEnvironmentPropertyUInt("frames_total")
    dt = sim.getEnvironmentPropertyUInt("days_total")
    q8 = sim.getEnvironmentPropertyUInt("quota_next_mi8")
    q17 = sim.getEnvironmentPropertyUInt("quota_next_mi17")
    arr_mi8 = sim.getEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8")
    arr_mi17 = sim.getEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi17")
    arr_mp5 = sim.getEnvironmentPropertyArrayUInt32("mp5_daily_hours")
    arr_mp3_status = sim.getEnvironmentPropertyArrayUInt32("mp3_status_id")
    arr_mp3_ll = sim.getEnvironmentPropertyArrayUInt32("mp3_ll")
    arr_mp3_oh = sim.getEnvironmentPropertyArrayUInt32("mp3_oh")
    arr_mp1_br8 = sim.getEnvironmentPropertyArrayUInt32("mp1_br_mi8")

    d0 = 0
    d1 = 1 if days_total > 1 else 0
    idx0 = 0
    base_d0 = d0 * frames_total + idx0
    base_d1 = d1 * frames_total + idx0

    from datetime import date as _date
    epoch = _date(1970,1,1)
    vd_date = epoch.fromordinal(epoch.toordinal() + int(vd)) if isinstance(vd, int) else None
    print(
        "OK EnvOnly:\n"
        f"  scalars: version_date={vd} (~{vd_date}), days_total={dt}, frames_total={ft}\n"
        f"  MP4: mi8[D0]={arr_mi8[d0]}, mi17[D1]={arr_mi17[d1]}\n"
        f"  MP4→quota (host): mi8[D1]={q8}, mi17[D1]={q17}\n"
        f"  MP5: dt0={arr_mp5[base_d0]}, dn1={arr_mp5[base_d1]}\n"
        f"  MP3[0]: status={arr_mp3_status[0]}, ll={arr_mp3_ll[0]}, oh={arr_mp3_oh[0]}\n"
        f"  MP1[0]: br_mi8={arr_mp1_br8[0]}"
    )


if __name__ == "__main__":
    main()


