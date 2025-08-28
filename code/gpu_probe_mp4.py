#!/usr/bin/env python3
"""
Смоук‑тест MP4 на GPU:
- Загружает flight_program_ac (ops_counter_mi8/mi17) из ClickHouse
- Кладёт массивы в Env Property Arrays
- RTC читает значения по day и пишет в агентные переменные для проверки
Дата: 2025-08-28
"""

from __future__ import annotations
from typing import Dict, List, Tuple
from datetime import date
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from config_loader import get_clickhouse_client


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

    ops8, ops17 = load_mp4_from_clickhouse()
    if not ops8:
        print("❌ MP4 пуст: нет данных в flight_program_ac")
        sys.exit(1)
    days_total = len(ops8)

    model = pyflamegpu.ModelDescription("HeliSimProbeMP4")
    env = model.Environment()
    env.newPropertyUInt("days_total", 0)
    # ВАЖНО: базовая длина массива должна совпадать с длиной устанавливаемых данных
    env.newPropertyArrayUInt32("mp4_ops_counter_mi8", [0] * days_total)
    env.newPropertyArrayUInt32("mp4_ops_counter_mi17", [0] * days_total)

    agent = model.newAgent("probe")
    agent.newVariableUInt("seed8", 0)
    agent.newVariableUInt("seed17", 0)

    rtc_src = r"""
    FLAMEGPU_AGENT_FUNCTION(rtc_probe_mp4, flamegpu::MessageNone, flamegpu::MessageNone) {
        unsigned int day = FLAMEGPU->getStepCounter();
        unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        if (days_total == 0u) return flamegpu::ALIVE;
        unsigned int idx = (day < days_total ? day : (days_total - 1u));
        unsigned int s8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", idx);
        unsigned int s17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", idx);
        FLAMEGPU->setVariable<unsigned int>("seed8", s8);
        FLAMEGPU->setVariable<unsigned int>("seed17", s17);
        return flamegpu::ALIVE;
    }
    """
    agent.newRTCFunction("rtc_probe_mp4", rtc_src)

    lyr = model.newLayer(); lyr.addAgentFunction(agent.getFunction("rtc_probe_mp4"))

    sim = pyflamegpu.CUDASimulation(model)
    sim.setEnvironmentPropertyUInt("days_total", int(days_total))
    sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8", list(ops8))
    sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi17", list(ops17))

    av = pyflamegpu.AgentVector(agent, 1)
    av[0].setVariableUInt("seed8", 0)
    av[0].setVariableUInt("seed17", 0)
    sim.setPopulationData(av)

    # Прогоним D0 и D1 (если есть)
    steps = 2 if days_total > 1 else 1
    for s in range(steps):
        sim.step()
        out = pyflamegpu.AgentVector(agent)
        sim.getPopulationData(out)
        a0 = out[0]
        s8 = int(a0.getVariableUInt("seed8"))
        s17 = int(a0.getVariableUInt("seed17"))
        print(f"DAY={s}: mp4_mi8={s8}, mp4_mi17={s17}")


if __name__ == "__main__":
    main()


