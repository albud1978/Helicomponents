#!/usr/bin/env python3
"""
V2 Step 03: add_probe_mp5
- Строит модель с базовым агентом и RTC `rtc_probe_mp5`
- Подключает Env массивы MP5/MP4 из ClickHouse и снапшота FRAMES=union
- Smoke‑тест: 5 шагов, выборка N агентов, печать dt/dn (первые индексы)
"""
from __future__ import annotations

import os
import sys
from typing import List

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from sim_env_setup import get_client, prepare_env_arrays


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

    model = fg.ModelDescription("HeliSim_V2_MP5Probe")
    e = model.Environment()
    e.newPropertyUInt("version_date", int(env['version_date_u16']))
    e.newPropertyUInt("frames_total", FRAMES)
    e.newPropertyUInt("days_total", DAYS)
    # MP5 линейный MacroProperty (UInt32) + источник для копирования
    e.newMacroPropertyUInt32("mp5_lin", FRAMES * (DAYS + 1))
    e.newPropertyArrayUInt32("mp5_src", [0] * ((DAYS + 1) * FRAMES))

    a = model.newAgent("component")
    a.newVariableUInt("idx", 0)
    a.newVariableUInt("daily_today_u32", 0)
    a.newVariableUInt("daily_next_u32", 0)

    func_name = f"rtc_probe_mp5_d{DAYS}"
    # Копирование MP5 из PropertyArray в MacroProperty (каждый агент копирует свою колонку по дням)
    rtc_copy = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_mp5_copy_columns, flamegpu::MessageNone, flamegpu::MessageNone) {{
        static const unsigned int FRAMES = {FRAMES}u;
        static const unsigned int DAYS = {DAYS}u;
        const unsigned int i = FLAMEGPU->getVariable<unsigned int>("idx");
        if (i >= FRAMES) return flamegpu::ALIVE;
        auto dst = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp5_lin");
        for (unsigned int d = 0u; d <= DAYS; ++d) {{
            const unsigned int base = d * FRAMES + i;
            const unsigned int v = FLAMEGPU->environment.getProperty<unsigned int>("mp5_src", base);
            dst[base].exchange(v);
        }}
        return flamegpu::ALIVE;
    }}
    """
    a.newRTCFunction("rtc_mp5_copy_columns", rtc_copy)

    rtc_src = f"""
    FLAMEGPU_AGENT_FUNCTION({func_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
        static const unsigned int FRAMES = {FRAMES}u;
        static const unsigned int DAYS = {DAYS}u;
        const unsigned int i = FLAMEGPU->getVariable<unsigned int>("idx");
        if (i >= FRAMES) return flamegpu::ALIVE;
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int d = (day < DAYS ? day : (DAYS > 0u ? DAYS - 1u : 0u));
        const unsigned int base = d * FRAMES + i;
        const unsigned int base_next = base + FRAMES;
        auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp5_lin");
        const unsigned int dt = mp[base];
        const unsigned int dn = mp[base_next];
        FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
        FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
        return flamegpu::ALIVE;
    }}
    """
    a.newRTCFunction(func_name, rtc_src)

    # Слои: копирование MP5 → probe
    lcopy = model.newLayer(); lcopy.addAgentFunction(a.getFunction("rtc_mp5_copy_columns"))
    l0 = model.newLayer(); l0.addAgentFunction(a.getFunction(func_name))

    # Инициализируем популяцию на FRAMES агентов с idx=0..FRAMES-1
    sim = fg.CUDASimulation(model)
    sim.setEnvironmentPropertyUInt("version_date", int(env['version_date_u16']))
    sim.setEnvironmentPropertyUInt("frames_total", FRAMES)
    sim.setEnvironmentPropertyUInt("days_total", DAYS)
    # Заполняем источник MP5 из ClickHouse
    mp5 = list(env['mp5_daily_hours_linear'])
    need = (DAYS + 1) * FRAMES
    mp5 = mp5[:need]
    assert len(mp5) == need, f"mp5_src length mismatch: {len(mp5)} != {need}"
    sim.setEnvironmentPropertyArrayUInt32("mp5_src", mp5)

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
    print(f"MP5Probe OK: DAYS={DAYS}, FRAMES={FRAMES}, sample(dt,dn)={vals}")
    return 0


if __name__ == "__main__":
    sys.exit(main())


