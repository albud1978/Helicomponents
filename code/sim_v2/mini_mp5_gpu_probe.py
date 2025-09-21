#!/usr/bin/env python3
"""
Мини‑стенд: 1 агент, DAYS (по умолчанию 365). Чтение MP5 на GPU внутри RTC.
Печатает dt/dn на каждом дне.

Запуск (пример):
  PYTHONUNBUFFERED=1 HL_V2_STEPS=365 HL_V2_CHECK_ACN=22418 \
  python3 -u code/sim_v2/mini_mp5_gpu_probe.py | cat
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
    # первый по порядку
    if frames_index:
        return int(sorted(frames_index.values())[0])
    return 0


def build_mp5_slice(env: dict, fi: int, days: int) -> list[int]:
    """Возвращает срез MP5 для одного кадра длиной DAYS+1 (D+1 паддинг)."""
    full = list(env['mp5_daily_hours_linear'])
    FRAMES = int(env['frames_total_u16'])
    out: list[int] = [0] * (days + 1)
    for d in range(days + 1):
        pos = d * FRAMES + fi
        out[d] = int(full[pos]) if 0 <= pos < len(full) else 0
    return out


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
    mp5_one = build_mp5_slice(env, fi, DAYS)  # длина = DAYS+1

    # Модель: FRAMES агентов; загружаем ПОЛНЫЙ MP5 в MacroProperty mp5_lin[(DAYS+1)*FRAMES]
    model = fg.ModelDescription("Mini_MP5_GPU_Probe")
    e = model.Environment()
    e.newPropertyUInt("version_date", int(env['version_date_u16']))
    FRAMES = int(env['frames_total_u16'])
    e.newPropertyUInt("frames_total", FRAMES)
    e.newPropertyUInt("days_total", DAYS)
    e.newMacroPropertyUInt32("mp5_lin", FRAMES * (DAYS + 1))
    # Источник MP5 из ENV (как в проде)
    e.newPropertyArrayUInt32("mp5_daily_hours", [0] * (FRAMES * (DAYS + 1)))

    a = model.newAgent("component")
    a.newVariableUInt("idx", 0)  # индекс кадра и позиция в популяции одинаковы
    a.newVariableUInt("daily_today_u32", 0)
    a.newVariableUInt("daily_next_u32", 0)

    # RTC: однократно материализует mp5_daily_hours -> mp5_lin на шаге 0
    rtc_mat = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_mp5_materialize, flamegpu::MessageNone, flamegpu::MessageNone) {{
        const unsigned int step = FLAMEGPU->getStepCounter();
        if (step > 0u) return flamegpu::ALIVE;
        const unsigned int i = FLAMEGPU->getVariable<unsigned int>("idx");
        static const unsigned int FR = {FRAMES}u;
        static const unsigned int DY = {DAYS}u;
        const unsigned int SZ = FR * (DY + 1u);
        auto dst = FLAMEGPU->environment.getMacroProperty<unsigned int, SZ>("mp5_lin");
        for (unsigned int d = 0u; d <= DY; ++d) {{
            const unsigned int base = d * FR + i;
            const unsigned int v = FLAMEGPU->environment.getProperty<unsigned int>("mp5_daily_hours", base);
            dst[base].exchange(v);
        }}
        return flamegpu::ALIVE;
    }}
    """
    a.newRTCFunction("rtc_mp5_materialize", rtc_mat)

    # RTC: чтение dt и dn из mp5_lin
    SIZE = DAYS + 1
    rtc = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_probe_mp5, flamegpu::MessageNone, flamegpu::MessageNone) {{
        const unsigned int frame_i = FLAMEGPU->getVariable<unsigned int>("idx");
        static const unsigned int FR = {FRAMES}u;
        static const unsigned int DY = {DAYS}u;
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int d = (day < DY ? day : (DY > 0u ? DY - 1u : 0u));
        const unsigned int base = d * FR + frame_i;
        const unsigned int base_next = base + FR;
        const unsigned int SZ = FR * (DY + 1u);
        auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, SZ>("mp5_lin");
        const unsigned int dt = mp[base];
        const unsigned int dn = (d < DY ? mp[base_next] : 0u);
        FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
        FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
        return flamegpu::ALIVE;
    }}
    """
    a.newRTCFunction("rtc_probe_mp5", rtc)

    # Инициализация: готовим линейный MP5 для источника ENV
    mp5_full = list(env['mp5_daily_hours_linear'])
    need = (DAYS + 1) * FRAMES
    mp5_full = mp5_full[:need]
    assert len(mp5_full) == need, f"mp5 size mismatch: {len(mp5_full)} != {need}"
    # Слои: материализация MP5 -> probe ядро
    l0 = model.newLayer(); l0.addAgentFunction(a.getFunction("rtc_mp5_materialize"))
    l1 = model.newLayer(); l1.addAgentFunction(a.getFunction("rtc_probe_mp5"))

    # Создаём симуляцию
    sim = fg.CUDASimulation(model)
    sim.setEnvironmentPropertyUInt("version_date", int(env['version_date_u16']))
    sim.setEnvironmentPropertyUInt("frames_total", FRAMES)
    sim.setEnvironmentPropertyUInt("days_total", DAYS)
    # Заполняем источник ENV массивом MP5
    sim.setEnvironmentPropertyArrayUInt32("mp5_daily_hours", mp5_full)

    av = fg.AgentVector(a, FRAMES)
    for i in range(FRAMES):
        av[i].setVariableUInt("idx", i)
    sim.setPopulationData(av)

    # Шаги и печать (dt, dn) по дням
    for d in range(DAYS):
        sim.step()
        out = fg.AgentVector(a)
        sim.getPopulationData(out)
        dt = int(out[fi].getVariableUInt("daily_today_u32"))
        dn = int(out[fi].getVariableUInt("daily_next_u32"))
        print(f"D{d+1}: idx={fi} dt={dt} dn={dn}")

    return 0


if __name__ == "__main__":
    sys.exit(main())


