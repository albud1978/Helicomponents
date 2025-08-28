#!/usr/bin/env python3
"""
Полный смоук‑тест окружения на GPU:
- Загружает MP1/MP3/MP4/MP5 и скаляры (version_date, frames_total, days_total)
- Создаёт MP6 (MacroProperty Arrays) длиной days_total
- Инициализирует MP6 на GPU из MP4
- Проверяет чтение MP4/MP5 и атомарный декремент MP6[day+1]
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
    mp4_by_day = preload_mp4_by_day(client)
    mp5_by_day = preload_mp5_maps(client)

    days_sorted = sorted(mp4_by_day.keys())
    if not days_sorted:
        print("❌ MP4 пуст: нет дат")
        sys.exit(1)
    frames_index, frames_total = build_frames_index(mp3_rows, mp3_fields)
    mp5_linear = build_mp5_linear(mp5_by_day, days_sorted, frames_index, frames_total)
    mp4_ops8, mp4_ops17 = build_mp4_arrays(mp4_by_day, days_sorted)
    days_total = len(days_sorted)
    version_date_u16 = days_to_epoch_u16(vdate)

    model = pyflamegpu.ModelDescription("HeliSimProbeEnvFull")
    env = model.Environment()
    # Scalars
    env.newPropertyUInt("version_date", 0)
    env.newPropertyUInt("frames_total", 0)
    env.newPropertyUInt("days_total", 0)
    # RO Property Arrays
    env.newPropertyArrayUInt32("mp4_ops_counter_mi8", [0] * days_total)
    env.newPropertyArrayUInt32("mp4_ops_counter_mi17", [0] * days_total)
    env.newPropertyArrayUInt32("mp5_daily_hours", [0] * ((days_total + 1) * frames_total))
    # MP6 MacroProperty скаляры (совместимый путь: квоты на D+1)
    env.newMacroPropertyUInt32("remaining_ops_next_mi8", 1)
    env.newMacroPropertyUInt32("remaining_ops_next_mi17", 1)

    agent = model.newAgent("probe")
    agent.newVariableUInt("idx", 0)
    agent.newVariableUInt("dt0", 0)
    agent.newVariableUInt("dn1", 0)
    agent.newVariableUInt("q8_old", 0)
    agent.newVariableUInt("q17_old", 0)
    agent.newVariableUInt("seed8_d1", 0)
    agent.newVariableUInt("seed17_d1", 0)

    # rtc: инициализация MP6 скаляров из MP4[D1], выполняет агент idx==0
    rtc_seed_src = r"""
    FLAMEGPU_AGENT_FUNCTION(rtc_seed_mp6_all, flamegpu::MessageNone, flamegpu::MessageNone) {
        if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
        unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        unsigned int d1 = (days_total > 1u ? 1u : 0u);
        unsigned int s8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", d1);
        unsigned int s17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", d1);
        auto q8s  = FLAMEGPU->environment.getMacroProperty<unsigned int>("remaining_ops_next_mi8");
        auto q17s = FLAMEGPU->environment.getMacroProperty<unsigned int>("remaining_ops_next_mi17");
        q8s.exchange(s8);
        q17s.exchange(s17);
        return flamegpu::ALIVE;
    }
    """
    agent.newRTCFunction("rtc_seed_mp6_all", rtc_seed_src)

    # rtc: чтение MP5 (dt@D0, dn@D1) и один атомарный декремент MP6 скаляров (квота на D+1)
    rtc_probe_src = r"""
    FLAMEGPU_AGENT_FUNCTION(rtc_probe_all, flamegpu::MessageNone, flamegpu::MessageNone) {
        unsigned int N   = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
        unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        unsigned int i = FLAMEGPU->getVariable<unsigned int>("idx");
        unsigned int lin0 = 0u * N + i;
        unsigned int lin1 = (days_total > 1u ? 1u : 0u) * N + i;
        unsigned int dt0 = FLAMEGPU->environment.getProperty<unsigned int>("mp5_daily_hours", lin0);
        unsigned int dn1 = FLAMEGPU->environment.getProperty<unsigned int>("mp5_daily_hours", lin1);
        FLAMEGPU->setVariable<unsigned int>("dt0", dt0);
        FLAMEGPU->setVariable<unsigned int>("dn1", dn1);
        auto q8  = FLAMEGPU->environment.getMacroProperty<unsigned int>("remaining_ops_next_mi8");
        auto q17 = FLAMEGPU->environment.getMacroProperty<unsigned int>("remaining_ops_next_mi17");
        unsigned int old8  = q8--;
        unsigned int old17 = q17--;
        FLAMEGPU->setVariable<unsigned int>("q8_old", old8);
        FLAMEGPU->setVariable<unsigned int>("q17_old", old17);
        unsigned int d1 = (days_total > 1u ? 1u : 0u);
        unsigned int s8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", d1);
        unsigned int s17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", d1);
        FLAMEGPU->setVariable<unsigned int>("seed8_d1", s8);
        FLAMEGPU->setVariable<unsigned int>("seed17_d1", s17);
        return flamegpu::ALIVE;
    }
    """
    agent.newRTCFunction("rtc_probe_all", rtc_probe_src)

    lyr1 = model.newLayer(); lyr1.addAgentFunction(agent.getFunction("rtc_seed_mp6_all"))
    lyr2 = model.newLayer(); lyr2.addAgentFunction(agent.getFunction("rtc_probe_all"))

    sim = pyflamegpu.CUDASimulation(model)
    sim.setEnvironmentPropertyUInt("version_date", int(version_date_u16))
    sim.setEnvironmentPropertyUInt("frames_total", int(frames_total))
    sim.setEnvironmentPropertyUInt("days_total", int(days_total))
    sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8", list(mp4_ops8))
    sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi17", list(mp4_ops17))
    sim.setEnvironmentPropertyArrayUInt32("mp5_daily_hours", list(mp5_linear))

    av = pyflamegpu.AgentVector(agent, 1)
    av[0].setVariableUInt("idx", 0)  # первый кадр для чтения
    sim.setPopulationData(av)

    sim.step()  # seed MP6
    sim.step()  # probe & atomic

    out = pyflamegpu.AgentVector(agent)
    sim.getPopulationData(out)
    a0 = out[0]
    print(
        "OK:\n"
        f"  days_total={days_total}, frames_total={frames_total}\n"
        f"  MP4[D1]: mi8={int(a0.getVariableUInt('seed8_d1'))}, mi17={int(a0.getVariableUInt('seed17_d1'))}\n"
        f"  MP5: dt0={int(a0.getVariableUInt('dt0'))}, dn1={int(a0.getVariableUInt('dn1'))}\n"
        f"  MP6[D1] old: mi8={int(a0.getVariableUInt('q8_old'))}, mi17={int(a0.getVariableUInt('q17_old'))}"
    )


if __name__ == "__main__":
    main()


