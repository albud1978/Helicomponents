#!/usr/bin/env python3
"""
V2 Step 03: add_probe_mp5 (HostFunction версия)
- Строит модель с базовым агентом и RTC `rtc_probe_mp5`
- Использует HostFunction для инициализации MacroProperty mp5_lin
- Фиксированные размеры: MAX_FRAMES=250, MAX_DAYS=3650
"""
from __future__ import annotations

import os
import sys
from typing import List

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from sim_env_setup import get_client, prepare_env_arrays

# Фиксированные максимальные размеры для MacroProperty
MAX_FRAMES = 250  # Максимум кадров
MAX_DAYS = 3650   # Максимум дней (10 лет)
MAX_SIZE = MAX_FRAMES * (MAX_DAYS + 1)


def main() -> int:
    try:
        import pyflamegpu as fg
    except Exception as e:
        print(f"pyflamegpu not installed: {e}")
        return 1

    client = get_client()
    env = prepare_env_arrays(client)
    DAYS_full = int(env['days_total_u16'])

    # FRAMES из снапшота (union)
    snap_path = os.environ.get('HL_V2_ENV_SNAPSHOT', 'tmp/env_snapshot.json')
    FRAMES = int(env['frames_total_u16'])
    import json
    with open(snap_path, 'r', encoding='utf-8') as f:
        snap = json.load(f)
    FRAMES = int(snap.get('frames_union_no_future', FRAMES))

    # Ограничим DAYS для smoke (управляется HL_V2_STEPS)
    STEPS = int(os.environ.get('HL_V2_STEPS', '5'))
    DAYS = min(STEPS, DAYS_full)

    # Проверка ограничений
    if FRAMES > MAX_FRAMES:
        print(f"WARNING: FRAMES={FRAMES} > MAX_FRAMES={MAX_FRAMES}")
    if DAYS > MAX_DAYS:
        print(f"WARNING: DAYS={DAYS} > MAX_DAYS={MAX_DAYS}")

    model = fg.ModelDescription("HeliSim_V2_MP5Probe")
    e = model.Environment()
    e.newPropertyUInt("version_date", int(env['version_date_u16']))
    e.newPropertyUInt("frames_total", FRAMES)
    e.newPropertyUInt("days_total", DAYS)
    # MP5 MacroProperty с фиксированным максимальным размером
    e.newMacroPropertyUInt32("mp5_lin", MAX_SIZE)

    a = model.newAgent("component")
    a.newVariableUInt("idx", 0)
    a.newVariableUInt("daily_today_u32", 0)
    a.newVariableUInt("daily_next_u32", 0)

    # HostFunction для инициализации mp5_lin
    class HF_InitMP5(fg.HostFunction):
        def __init__(self, data: list[int], frames: int, days: int):
            super().__init__()
            self.data = data
            self.frames = frames
            self.days = days

        def run(self, FLAMEGPU):
            mp = FLAMEGPU.environment.getMacroPropertyUInt32("mp5_lin")
            print(f"HF_InitMP5: Инициализация mp5_lin для FRAMES={self.frames}, DAYS={self.days}")
            # Заполняем только используемую часть
            for d in range(self.days + 1):
                for f in range(self.frames):
                    src_idx = d * self.frames + f
                    dst_idx = d * MAX_FRAMES + f  # Используем MAX_FRAMES для индексации
                    if src_idx < len(self.data):
                        mp[dst_idx] = self.data[src_idx]
            print(f"HF_InitMP5: Инициализировано {(self.days+1)*self.frames} элементов")

    func_name = f"rtc_probe_mp5_d{DAYS}"
    rtc_src = f"""
    FLAMEGPU_AGENT_FUNCTION({func_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
        static const unsigned int MAX_FR = {MAX_FRAMES}u;
        static const unsigned int MAX_SZ = {MAX_SIZE}u;
        const unsigned int i = FLAMEGPU->getVariable<unsigned int>("idx");
        const unsigned int FR = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
        const unsigned int DY = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        if (i >= FR) return flamegpu::ALIVE;
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int d = (day < DY ? day : (DY > 0u ? DY - 1u : 0u));
        const unsigned int base = d * MAX_FR + i;  // Важно: используем MAX_FR
        const unsigned int base_next = base + MAX_FR;
        auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_SZ>("mp5_lin");
        const unsigned int dt = mp[base];
        const unsigned int dn = (d < DY ? mp[base_next] : 0u);
        FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
        FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
        return flamegpu::ALIVE;
    }}
    """
    a.newRTCFunction(func_name, rtc_src)

    # Подготовка данных MP5
    mp5 = list(env['mp5_daily_hours_linear'])
    need = (DAYS + 1) * FRAMES
    mp5 = mp5[:need]
    assert len(mp5) == need, f"mp5_src length mismatch: {len(mp5)} != {need}"

    # Слои: инициализация MP5 через HostFunction → probe
    l0 = model.newLayer()
    l0.addHostFunction(HF_InitMP5(mp5, FRAMES, DAYS))
    l1 = model.newLayer()
    l1.addAgentFunction(a.getFunction(func_name))

    # Инициализируем популяцию на FRAMES агентов с idx=0..FRAMES-1
    sim = fg.CUDASimulation(model)
    sim.setEnvironmentPropertyUInt("version_date", int(env['version_date_u16']))
    sim.setEnvironmentPropertyUInt("frames_total", FRAMES)
    sim.setEnvironmentPropertyUInt("days_total", DAYS)

    av = fg.AgentVector(a, FRAMES)
    for i in range(FRAMES):
        av[i].setVariableUInt("idx", i)
    sim.setPopulationData(av)

    # Шаги
    for _ in range(STEPS):
        sim.step()

    # Считываем подмножество и печатаем dt/dn
    out = fg.AgentVector(a)
    sim.getPopulationData(out)
    sample = min(5, FRAMES)
    vals: List[tuple] = []
    for i in range(sample):
        dt = out[i].getVariableUInt("daily_today_u32")
        dn = out[i].getVariableUInt("daily_next_u32")
        vals.append((i, dt, dn))
    print(f"MP5Probe OK: DAYS={DAYS}, FRAMES={FRAMES}, MAX_SIZE={MAX_SIZE}, sample(dt,dn)={vals}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
