#!/usr/bin/env python3
"""
Мини‑стенд с фиксированным размером MacroProperty для больших периодов.
Использует максимальный размер как константу времени компиляции.
"""
from __future__ import annotations

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from sim_env_setup import get_client, prepare_env_arrays


def pick_frame_idx(env: dict) -> int:
    """Выбирает кадр по HL_V2_CHECK_ACN или первый из frames_index."""
    frames_index = env.get('frames_index', {})
    check = os.environ.get('HL_V2_CHECK_ACN', '').strip()
    if check.isdigit():
        acn = int(check)
        return int(frames_index.get(acn, 0))
    if frames_index:
        return int(sorted(frames_index.values())[0])
    return 0


def main() -> int:
    try:
        import pyflamegpu as fg
    except Exception as e:
        print(f"pyflamegpu not installed: {e}")
        return 1

    # Данные из ClickHouse (реальные)
    env = prepare_env_arrays(get_client())
    DAYS_full = int(env['days_total_u16'])
    DAYS = min(int(os.environ.get('HL_V2_STEPS', '365')), DAYS_full)
    fi = pick_frame_idx(env)
    
    # Фиксированные максимальные размеры для MacroProperty
    MAX_FRAMES = 250  # Максимум кадров (с запасом)
    MAX_DAYS = 3650   # Максимум дней (10 лет)
    MAX_SIZE = MAX_FRAMES * (MAX_DAYS + 1)

    # Модель
    model = fg.ModelDescription("Mini_MP5_GPU_Probe_Fixed")
    e = model.Environment()
    e.newPropertyUInt("version_date", int(env['version_date_u16']))
    FRAMES = int(env['frames_total_u16'])
    e.newPropertyUInt("frames_total", FRAMES)
    e.newPropertyUInt("days_total", DAYS)
    # MacroProperty с фиксированным максимальным размером
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
            # Заполняем только используемую часть
            for d in range(self.days + 1):
                for f in range(self.frames):
                    src_idx = d * self.frames + f
                    dst_idx = d * MAX_FRAMES + f  # Используем MAX_FRAMES для индексации
                    if src_idx < len(self.data):
                        mp[dst_idx] = self.data[src_idx]

    # RTC с фиксированными размерами
    rtc = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_probe_mp5, flamegpu::MessageNone, flamegpu::MessageNone) {{
        const unsigned int frame_i = FLAMEGPU->getVariable<unsigned int>("idx");
        const unsigned int FR = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
        const unsigned int DY = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int d = (day < DY ? day : (DY > 0u ? DY - 1u : 0u));
        
        // Используем MAX_FRAMES для правильной индексации
        const unsigned int base = d * {MAX_FRAMES}u + frame_i;
        const unsigned int base_next = base + {MAX_FRAMES}u;
        
        auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_SIZE}u>("mp5_lin");
        const unsigned int dt = mp[base];
        const unsigned int dn = (d < DY ? mp[base_next] : 0u);
        
        FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
        FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
        return flamegpu::ALIVE;
    }}
    """
    a.newRTCFunction("rtc_probe_mp5", rtc)

    # Готовим данные MP5
    mp5_full = list(env['mp5_daily_hours_linear'])
    need = (DAYS + 1) * FRAMES
    mp5_full = mp5_full[:need]

    # Слои: инициализация + чтение
    l0 = model.newLayer(); l0.addHostFunction(HF_InitMP5(mp5_full, FRAMES, DAYS))
    l1 = model.newLayer(); l1.addAgentFunction(a.getFunction("rtc_probe_mp5"))

    # Создаём симуляцию
    sim = fg.CUDASimulation(model)
    sim.setEnvironmentPropertyUInt("version_date", int(env['version_date_u16']))
    sim.setEnvironmentPropertyUInt("frames_total", FRAMES)
    sim.setEnvironmentPropertyUInt("days_total", DAYS)

    av = fg.AgentVector(a, FRAMES)
    for i in range(FRAMES):
        av[i].setVariableUInt("idx", i)
    sim.setPopulationData(av)

    # Шаги и печать (dt, dn) по дням
    print(f"Running for DAYS={DAYS}, FRAMES={FRAMES}, MAX_SIZE={MAX_SIZE}")
    for d in range(DAYS):
        sim.step()
        out = fg.AgentVector(a)
        sim.getPopulationData(out)
        dt = int(out[fi].getVariableUInt("daily_today_u32"))
        dn = int(out[fi].getVariableUInt("daily_next_u32"))
        if d < 5 or d >= DAYS - 5:  # Печатаем первые и последние 5 дней
            print(f"D{d+1}: idx={fi} dt={dt} dn={dn}")
        elif d == 5:
            print("...")

    return 0


if __name__ == "__main__":
    sys.exit(main())
