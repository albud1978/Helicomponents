#!/usr/bin/env python3
"""
Смоук MP6 (fallback скаляры):
- Загружает MP4 (ops_counter_mi8/mi17) в Env
- На GPU (RTC) копирует MP4[D+1] в MacroProperty-скаляры remaining_ops_next_* и делает atomicSub
- Выводит seed и old для проверки атомика
Дата: 2025-08-28
"""

from __future__ import annotations
from typing import List, Tuple
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from config_loader import get_clickhouse_client


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


def load_mp4_from_clickhouse() -> Tuple[List[int], List[int]]:
    client = get_clickhouse_client()
    rows = client.execute(
        """
        SELECT dates, ops_counter_mi8, ops_counter_mi17
        FROM flight_program_ac
        ORDER BY dates
        """
    )
    ops8: List[int] = []
    ops17: List[int] = []
    for d, mi8, mi17 in rows:
        ops8.append(int(mi8 or 0))
        ops17.append(int(mi17 or 0))
    return ops8, ops17


def main():
    ensure_cuda_path()
    import pyflamegpu as fg

    ops8, ops17 = load_mp4_from_clickhouse()
    if not ops8:
        print("❌ MP4 пуст")
        sys.exit(1)
    days_total = len(ops8)

    model = fg.ModelDescription("HeliSimProbeMP6Scalar")
    env = model.Environment()
    env.newPropertyUInt("days_total", 0)
    env.newPropertyArrayUInt32("mp4_ops_counter_mi8", [0] * days_total)
    env.newPropertyArrayUInt32("mp4_ops_counter_mi17", [0] * days_total)
    # MacroProperty скаляры квот
    env.newMacroPropertyUInt32("remaining_ops_next_mi8", 1)
    env.newMacroPropertyUInt32("remaining_ops_next_mi17", 1)

    agent = model.newAgent("probe")
    agent.newVariableUInt("seed8", 0)
    agent.newVariableUInt("seed17", 0)
    agent.newVariableUInt("old8", 0)
    agent.newVariableUInt("old17", 0)

    rtc_src = r"""
    FLAMEGPU_AGENT_FUNCTION(rtc_probe_mp6_scalar, flamegpu::MessageNone, flamegpu::MessageNone) {
        unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        unsigned int d1 = (days_total > 1u ? 1u : 0u);
        unsigned int s8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", d1);
        unsigned int s17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", d1);
        // Инициализируем макроскаляр квот seed-значениями и делаем atomicSub
        auto q8  = FLAMEGPU->environment.getMacroProperty<unsigned int>("remaining_ops_next_mi8");
        auto q17 = FLAMEGPU->environment.getMacroProperty<unsigned int>("remaining_ops_next_mi17");
        q8.exchange(s8);
        q17.exchange(s17);
        unsigned int old8  = q8--; // атомарный декремент
        unsigned int old17 = q17--;
        FLAMEGPU->setVariable<unsigned int>("seed8", s8);
        FLAMEGPU->setVariable<unsigned int>("seed17", s17);
        FLAMEGPU->setVariable<unsigned int>("old8", old8);
        FLAMEGPU->setVariable<unsigned int>("old17", old17);
        return flamegpu::ALIVE;
    }
    """
    agent.newRTCFunction("rtc_probe_mp6_scalar", rtc_src)
    lyr = model.newLayer(); lyr.addAgentFunction(agent.getFunction("rtc_probe_mp6_scalar"))

    sim = fg.CUDASimulation(model)
    sim.setEnvironmentPropertyUInt("days_total", int(days_total))
    sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8", list(ops8))
    sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi17", list(ops17))

    av = fg.AgentVector(agent, 1)
    sim.setPopulationData(av)

    sim.step()

    out = fg.AgentVector(agent)
    sim.getPopulationData(out)
    a0 = out[0]
    print(f"MP6-scalar: seed8={int(a0.getVariableUInt('seed8'))}, old8={int(a0.getVariableUInt('old8'))}, seed17={int(a0.getVariableUInt('seed17'))}, old17={int(a0.getVariableUInt('old17'))}")


if __name__ == "__main__":
    main()


