#!/usr/bin/env python3
"""
V2 Debug: минимальная компиляция rtc_probe_mp5 с печатью исходника и NVRTC ошибки
- Статические FRAMES/DAYS, упрощённые индексы, минимум переменных
- Цель: воспроизвести падение компиляции на DAYS>=90 и увидеть точный source
"""
from __future__ import annotations

import os
import sys


def main() -> int:
    try:
        import pyflamegpu as fg
    except Exception as e:
        print(f"pyflamegpu not installed: {e}")
        return 1

    # Параметры
    FRAMES = int(os.environ.get('HL_V2_FRAMES_DEBUG', '286'))
    DAYS = int(os.environ.get('HL_V2_STEPS', '90'))

    model = fg.ModelDescription("HeliSim_V2_DebugProbe")
    env = model.Environment()
    env.newPropertyUInt("version_date", 0)
    env.newPropertyUInt("frames_total", FRAMES)
    env.newPropertyUInt("days_total", DAYS)
    use_macro = os.environ.get('HL_V2_USE_MACRO', '0') == '1'
    if use_macro:
        env.newMacroPropertyUInt32("mp5_lin", FRAMES * (DAYS + 1))
    else:
        env.newPropertyArrayUInt32("mp5_daily_hours", [0] * ((DAYS + 1) * FRAMES))

    a = model.newAgent("component")
    a.newVariableUInt("idx", 0)
    a.newVariableUInt("daily_today_u32", 0)
    a.newVariableUInt("daily_next_u32", 0)

    func_name = f"rtc_probe_mp5_d{DAYS}"
    if use_macro:
        rtc_src = f"""
FLAMEGPU_AGENT_FUNCTION({func_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    static const unsigned int FRAMES = {FRAMES}u;
    static const unsigned int DAYS   = {DAYS}u;
    const unsigned int i = FLAMEGPU->getVariable<unsigned int>("idx");
    if (i >= FRAMES) return flamegpu::ALIVE;
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int d = (day < DAYS ? day : (DAYS > 0u ? DAYS - 1u : 0u));
    const unsigned int base = d * FRAMES + i;
    static const unsigned int TOTAL_SIZE = FRAMES * (DAYS + 1u);
    auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, TOTAL_SIZE>("mp5_lin");
    const unsigned int dt = mp[base];
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    return flamegpu::ALIVE;
}}
"""
    else:
        rtc_src = f"""
FLAMEGPU_AGENT_FUNCTION({func_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    static const unsigned int FRAMES = {FRAMES}u;
    static const unsigned int DAYS   = {DAYS}u;
    const unsigned int i = FLAMEGPU->getVariable<unsigned int>("idx");
    if (i >= FRAMES) return flamegpu::ALIVE;
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int d = (day < DAYS ? day : (DAYS > 0u ? DAYS - 1u : 0u));
    const unsigned int base = d * FRAMES + i;
    const unsigned int dt = FLAMEGPU->environment.getProperty<unsigned int>("mp5_daily_hours", base);
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    return flamegpu::ALIVE;
}}
"""

    print("\n===== RTC SOURCE (debug) =====\n" + rtc_src + "===== END SOURCE =====\n")
    try:
        a.newRTCFunction(func_name, rtc_src)
    except Exception as e:
        print("[NVRTC ERROR at newRTCFunction]", e)
        return 2

    # Добавим слой для триггера компиляции
    l = model.newLayer(); l.addAgentFunction(a.getFunction(func_name))
    sim = fg.CUDASimulation(model)
    try:
        sim.setEnvironmentPropertyUInt("version_date", 0)
        sim.setEnvironmentPropertyUInt("frames_total", FRAMES)
        sim.setEnvironmentPropertyUInt("days_total", DAYS)
        if use_macro:
            pass
        else:
            sim.setEnvironmentPropertyArrayUInt32("mp5_daily_hours", [0] * ((DAYS + 1) * FRAMES))
    except Exception as e:
        print("[NVRTC ERROR at setEnvironmentProperty]", e)
        return 3

    print("Debug compile OK (no NVRTC errors)")
    return 0


if __name__ == "__main__":
    sys.exit(main())


