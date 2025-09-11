#!/usr/bin/env python3
"""
Этап 0: Полный смоук‑тест загрузки Env (MP1/MP3/MP4/MP5 + скаляры, MP6‑скаляры)
Читает реальные данные из ClickHouse, размещает в Env с корректными размерами,
валидирует чтение на GPU (RTC) и атомарный декремент квоты (fallback скаляры).
Дата: 2025-08-28
"""

from __future__ import annotations
from typing import Dict, List, Tuple
from datetime import date
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
    mp1_br8, mp1_br17, mp1_rt, mp1_pt, mp1_at, mp1_idx_map = build_mp1_arrays(mp1_map)
    mp3_arrays = build_mp3_arrays(mp3_rows, mp3_fields)

    version_date_u16 = days_to_epoch_u16(vdate)

    # Сборка модели с корректными размерами Env
    model = pyflamegpu.ModelDescription("HeliSimStage0Probe")
    env = model.Environment()
    # Скаляры
    env.newPropertyUInt("version_date", 0)
    env.newPropertyUInt("frames_total", 0)
    env.newPropertyUInt("days_total", 0)
    # MP4
    env.newPropertyArrayUInt32("mp4_ops_counter_mi8", [0] * days_total)
    env.newPropertyArrayUInt32("mp4_ops_counter_mi17", [0] * days_total)
    # MP5
    env.newPropertyArrayUInt32("mp5_daily_hours", [0] * ((days_total + 1) * frames_total))
    # MP1 (SoA)
    env.newPropertyArrayUInt32("mp1_br_mi8", [0] * len(mp1_br8))
    env.newPropertyArrayUInt32("mp1_br_mi17", [0] * len(mp1_br17))
    env.newPropertyArrayUInt32("mp1_repair_time", [0] * len(mp1_rt))
    env.newPropertyArrayUInt32("mp1_partout_time", [0] * len(mp1_pt))
    env.newPropertyArrayUInt32("mp1_assembly_time", [0] * len(mp1_at))
    # MP3 (SoA)
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
    # MP6 fallback скаляры
    env.newMacroPropertyUInt32("remaining_ops_next_mi8", 1)
    env.newMacroPropertyUInt32("remaining_ops_next_mi17", 1)

    agent = model.newAgent("probe")
    agent.newVariableUInt("idx", 0)
    agent.newVariableUInt("partseq_idx", 0)
    agent.newVariableUInt("mp4_mi8_d0", 0)
    agent.newVariableUInt("mp4_mi17_d1", 0)
    agent.newVariableUInt("mp5_dt0", 0)
    agent.newVariableUInt("mp5_dn1", 0)
    agent.newVariableUInt("mp3_status0", 0)
    agent.newVariableUInt("mp3_ll0", 0)
    agent.newVariableUInt("mp3_oh0", 0)
    agent.newVariableUInt("mp1_br8_0", 0)
    agent.newVariableUInt("q8_old", 0)

    rtc_src = r"""
    FLAMEGPU_AGENT_FUNCTION(rtc_probe_stage0, flamegpu::MessageNone, flamegpu::MessageNone) {
        unsigned int N   = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
        unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        unsigned int d0 = 0u;
        unsigned int d1 = (days_total > 1u ? 1u : 0u);
        // MP4
        unsigned int s8_d0  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", d0);
        unsigned int s17_d1 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", d1);
        FLAMEGPU->setVariable<unsigned int>("mp4_mi8_d0", s8_d0);
        FLAMEGPU->setVariable<unsigned int>("mp4_mi17_d1", s17_d1);
        // MP5
        unsigned int i = FLAMEGPU->getVariable<unsigned int>("idx");
        unsigned int lin0 = d0 * N + i;
        unsigned int lin1 = d1 * N + i;
        unsigned int dt0 = FLAMEGPU->environment.getProperty<unsigned int>("mp5_daily_hours", lin0);
        unsigned int dn1 = FLAMEGPU->environment.getProperty<unsigned int>("mp5_daily_hours", lin1);
        FLAMEGPU->setVariable<unsigned int>("mp5_dt0", dt0);
        FLAMEGPU->setVariable<unsigned int>("mp5_dn1", dn1);
        // MP3 (берем нулевой элемент)
        unsigned int st0 = FLAMEGPU->environment.getProperty<unsigned int>("mp3_status_id", 0u);
        unsigned int ll0 = FLAMEGPU->environment.getProperty<unsigned int>("mp3_ll", 0u);
        unsigned int oh0 = FLAMEGPU->environment.getProperty<unsigned int>("mp3_oh", 0u);
        FLAMEGPU->setVariable<unsigned int>("mp3_status0", st0);
        FLAMEGPU->setVariable<unsigned int>("mp3_ll0", ll0);
        FLAMEGPU->setVariable<unsigned int>("mp3_oh0", oh0);
        // MP1 (по переданному индексу)
        unsigned int pidx = FLAMEGPU->getVariable<unsigned int>("partseq_idx");
        unsigned int br8 = FLAMEGPU->environment.getProperty<unsigned int>("mp1_br_mi8", pidx);
        FLAMEGPU->setVariable<unsigned int>("mp1_br8_0", br8);
        // MP6 fallback: скопировать seed D+1 и выполнить один декремент
        auto q8 = FLAMEGPU->environment.getMacroProperty<unsigned int>("remaining_ops_next_mi8");
        q8.exchange(s17_d1); // просто кладём какое-то положительное значение для проверки
        unsigned int old = q8--;
        FLAMEGPU->setVariable<unsigned int>("q8_old", old);
        return flamegpu::ALIVE;
    }
    """
    agent.newRTCFunction("rtc_probe_stage0", rtc_src)
    lyr = model.newLayer(); lyr.addAgentFunction(agent.getFunction("rtc_probe_stage0"))

    sim = pyflamegpu.CUDASimulation(model)
    # Скаляры
    sim.setEnvironmentPropertyUInt("version_date", int(version_date_u16))
    sim.setEnvironmentPropertyUInt("frames_total", int(frames_total))
    sim.setEnvironmentPropertyUInt("days_total", int(days_total))
    # MP4/MP5
    sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8", list(mp4_ops8))
    sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi17", list(mp4_ops17))
    sim.setEnvironmentPropertyArrayUInt32("mp5_daily_hours", list(mp5_linear))
    # MP1
    sim.setEnvironmentPropertyArrayUInt32("mp1_br_mi8", list(mp1_br8))
    sim.setEnvironmentPropertyArrayUInt32("mp1_br_mi17", list(mp1_br17))
    sim.setEnvironmentPropertyArrayUInt32("mp1_repair_time", list(mp1_rt))
    sim.setEnvironmentPropertyArrayUInt32("mp1_partout_time", list(mp1_pt))
    sim.setEnvironmentPropertyArrayUInt32("mp1_assembly_time", list(mp1_at))
    # MP3
    sim.setEnvironmentPropertyArrayUInt32("mp3_psn", list(mp3_arrays['mp3_psn']))
    sim.setEnvironmentPropertyArrayUInt32("mp3_aircraft_number", list(mp3_arrays['mp3_aircraft_number']))
    sim.setEnvironmentPropertyArrayUInt32("mp3_ac_type_mask", list(mp3_arrays['mp3_ac_type_mask']))
    sim.setEnvironmentPropertyArrayUInt32("mp3_status_id", list(mp3_arrays['mp3_status_id']))
    sim.setEnvironmentPropertyArrayUInt32("mp3_sne", list(mp3_arrays['mp3_sne']))
    sim.setEnvironmentPropertyArrayUInt32("mp3_ppr", list(mp3_arrays['mp3_ppr']))
    sim.setEnvironmentPropertyArrayUInt32("mp3_repair_days", list(mp3_arrays['mp3_repair_days']))
    sim.setEnvironmentPropertyArrayUInt32("mp3_ll", list(mp3_arrays['mp3_ll']))
    sim.setEnvironmentPropertyArrayUInt32("mp3_oh", list(mp3_arrays['mp3_oh']))
    sim.setEnvironmentPropertyArrayUInt32("mp3_mfg_date_days", list(mp3_arrays['mp3_mfg_date_days']))

    # Агент с idx=0 и произвольным существующим partseq_idx (берём первый ключ)
    import pyflamegpu as fg
    av = fg.AgentVector(agent, 1)
    av[0].setVariableUInt("idx", 0)
    first_partseq_idx = 0
    if mp1_idx_map:
        first_partseq_idx = next(iter(mp1_idx_map.values()))
    av[0].setVariableUInt("partseq_idx", int(first_partseq_idx))
    sim.setPopulationData(av)

    sim.step()
    out = fg.AgentVector(agent)
    sim.getPopulationData(out)
    a0 = out[0]
    print(
        "OK Stage0:\n"
        f"  days_total={days_total}, frames_total={frames_total}\n"
        f"  MP4: mi8[D0]={int(a0.getVariableUInt('mp4_mi8_d0'))}, mi17[D1]={int(a0.getVariableUInt('mp4_mi17_d1'))}\n"
        f"  MP5: dt0={int(a0.getVariableUInt('mp5_dt0'))}, dn1={int(a0.getVariableUInt('mp5_dn1'))}\n"
        f"  MP3[0]: status={int(a0.getVariableUInt('mp3_status0'))}, ll={int(a0.getVariableUInt('mp3_ll0'))}, oh={int(a0.getVariableUInt('mp3_oh0'))}\n"
        f"  MP1[idx0]: br_mi8={int(a0.getVariableUInt('mp1_br8_0'))}\n"
        f"  MP6(fallback scalar) old={int(a0.getVariableUInt('q8_old'))}"
    )


if __name__ == "__main__":
    main()


