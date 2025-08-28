#!/usr/bin/env python3
"""
Смоук‑тест MP5 на GPU:
- Загружает MP3/MP4/MP5, строит frames_index и линейный mp5_daily_hours с паддингом
- Читает на GPU dt@D0 и dn@D1 по формуле base = day*N + idx
Дата: 2025-08-28
"""

from __future__ import annotations
from typing import List, Tuple
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from config_loader import get_clickhouse_client
from sim_env_setup import (
    fetch_versions,
    fetch_mp3,
    preload_mp4_by_day,
    preload_mp5_maps,
    build_frames_index,
    build_mp5_linear,
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
        print("❌ Нет дат в MP4/MP5")
        sys.exit(1)

    frames_index, frames_total = build_frames_index(mp3_rows, mp3_fields)
    mp5_linear = build_mp5_linear(mp5_by_day, days_sorted, frames_index, frames_total)
    days_total = len(days_sorted)

    model = pyflamegpu.ModelDescription("HeliSimProbeMP5")
    env = model.Environment()
    env.newPropertyUInt("frames_total", 0)
    env.newPropertyUInt("days_total", 0)
    env.newPropertyArrayUInt32("mp5_daily_hours", [0] * ((days_total + 1) * frames_total))

    agent = model.newAgent("probe")
    agent.newVariableUInt("idx", 0)
    agent.newVariableUInt("dt0", 0)
    agent.newVariableUInt("dn1", 0)

    rtc_src = r"""
    FLAMEGPU_AGENT_FUNCTION(rtc_probe_mp5, flamegpu::MessageNone, flamegpu::MessageNone) {
        unsigned int N   = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
        unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        unsigned int i = FLAMEGPU->getVariable<unsigned int>("idx");
        unsigned int lin0 = 0u * N + i;
        unsigned int lin1 = (days_total > 1u ? 1u : 0u) * N + i;
        unsigned int dt0 = FLAMEGPU->environment.getProperty<unsigned int>("mp5_daily_hours", lin0);
        unsigned int dn1 = FLAMEGPU->environment.getProperty<unsigned int>("mp5_daily_hours", lin1);
        FLAMEGPU->setVariable<unsigned int>("dt0", dt0);
        FLAMEGPU->setVariable<unsigned int>("dn1", dn1);
        return flamegpu::ALIVE;
    }
    """
    agent.newRTCFunction("rtc_probe_mp5", rtc_src)
    lyr = model.newLayer(); lyr.addAgentFunction(agent.getFunction("rtc_probe_mp5"))

    sim = pyflamegpu.CUDASimulation(model)
    sim.setEnvironmentPropertyUInt("frames_total", int(frames_total))
    sim.setEnvironmentPropertyUInt("days_total", int(days_total))
    sim.setEnvironmentPropertyArrayUInt32("mp5_daily_hours", list(mp5_linear))

    av = pyflamegpu.AgentVector(agent, 1)
    av[0].setVariableUInt("idx", 0)
    sim.setPopulationData(av)

    sim.step()
    out = pyflamegpu.AgentVector(agent)
    sim.getPopulationData(out)
    a0 = out[0]
    print(f"MP5: dt0={int(a0.getVariableUInt('dt0'))}, dn1={int(a0.getVariableUInt('dn1'))}")


if __name__ == "__main__":
    main()


