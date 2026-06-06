#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (sim_v2 dead-code cleanup): не используется активным V8-ядром (orchestrator_limiter_v8). Внутренние импорты заморожены.
"""
RTC модуль: Адаптивный шаг ПОЛНОСТЬЮ на GPU

Вся логика вычисления step_days и обновления current_day
выполняется на GPU через MacroProperty.

CPU только:
1. Загрузка данных
2. simulation.simulate(N)
3. Выгрузка результатов

Дата: 08-01-2026
"""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
import model_build

import pyflamegpu as fg


MAX_FRAMES = model_build.RTC_MAX_FRAMES
MAX_DAYS = model_build.MAX_DAYS


def register_adaptive_gpu(model: fg.ModelDescription, 
                          heli_agent: fg.AgentDescription,
                          quota_agent: fg.AgentDescription):
    """
    Регистрирует RTC функции для полностью GPU-side адаптивного шага
    
    Паттерн: Каждый агент записывает свой лимитер в массив mp_limiters[idx],
    затем QuotaManager делает reduction для нахождения минимума.
    """
    
    print("  🚀 Регистрация модуля: adaptive_gpu (полностью GPU-side)")
    
    cumsum_size = MAX_FRAMES * (MAX_DAYS + 1)
    cumsum_stride = MAX_DAYS + 1
    
    # ═══════════════════════════════════════════════════════════════════════
    # Layer 1: Каждый агент записывает свой лимитер в массив
    # ═══════════════════════════════════════════════════════════════════════
    
    RTC_COMPUTE_LIMITER_OPS = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_compute_limiter_ops, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Читаем текущий день из MacroProperty
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_current_day");
    const unsigned int current_day = mp_day[0];
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    auto mp_limiters = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_agent_limiters");
    
    // Если симуляция завершена — лимитер = 0
    if (current_day >= end_day) {{
        mp_limiters[idx].exchange(0u);
        return flamegpu::ALIVE;
    }}
    
    // Вычисляем ресурсный лимитер
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    
    const unsigned int remaining_sne = (ll > sne) ? (ll - sne) : 0u;
    const unsigned int remaining_ppr = (oh > ppr) ? (oh - ppr) : 0u;
    const unsigned int remaining = (remaining_sne < remaining_ppr) ? remaining_sne : remaining_ppr;
    
    // Если уже на лимите — лимитер = 1
    if (remaining == 0u) {{
        mp_limiters[idx].exchange(1u);
        return flamegpu::ALIVE;
    }}
    
    // Ищем день, когда cumsum достигнет remaining
    auto cumsum = FLAMEGPU->environment.getMacroProperty<unsigned int, {cumsum_size}u>("mp5_cumsum");
    
    const unsigned int base = idx * {cumsum_stride}u;
    const unsigned int start_cumsum = cumsum[base + current_day];
    
    unsigned int my_limiter = 365u;  // MAX по умолчанию
    
    for (unsigned int d = 1u; d <= 365u; ++d) {{
        const unsigned int check_day = current_day + d;
        if (check_day >= {MAX_DAYS}u) break;
        
        const unsigned int delta = cumsum[base + check_day] - start_cumsum;
        if (delta >= remaining) {{
            my_limiter = d;
            break;
        }}
    }}
    
    // Записываем свой лимитер в массив
    mp_limiters[idx].exchange(my_limiter);
    
    return flamegpu::ALIVE;
}}
"""

    # ═══════════════════════════════════════════════════════════════════════
    # Layer 1b: Repair агенты записывают свой лимитер
    # ═══════════════════════════════════════════════════════════════════════
    
    RTC_COMPUTE_LIMITER_REPAIR = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_compute_limiter_repair, flamegpu::MessageNone, flamegpu::MessageNone) {{
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_current_day");
    const unsigned int current_day = mp_day[0];
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    auto mp_limiters = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_agent_limiters");
    
    if (current_day >= end_day) {{
        mp_limiters[idx].exchange(0u);
        return flamegpu::ALIVE;
    }}
    
    // Ремонтный лимитер
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    const unsigned int remaining = (repair_time > repair_days) ? (repair_time - repair_days) : 1u;
    
    mp_limiters[idx].exchange(remaining);
    
    return flamegpu::ALIVE;
}}
"""

    # ═══════════════════════════════════════════════════════════════════════
    # Layer 2: QuotaManager делает reduction и устанавливает step_days
    # ═══════════════════════════════════════════════════════════════════════
    
    RTC_SET_STEP_DAYS = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_set_step_days, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Только первый QuotaManager (group_by == 1) делает reduction
    const unsigned char group_by = FLAMEGPU->getVariable<unsigned char>("group_by");
    if (group_by != 1u) {{
        return flamegpu::ALIVE;
    }}
    
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_current_day");
    auto mp_step = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_step_days");
    auto mp_limiters = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_agent_limiters");
    
    const unsigned int current_day = mp_day[0];
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    const unsigned int frames_total = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    if (current_day >= end_day) {{
        mp_step[0].exchange(0u);
        return flamegpu::ALIVE;
    }}
    
    // ═══════════════════════════════════════════════════════════════════
    // ОПТИМИЗАЦИЯ: Пропускаем reduction, используем только program limiter
    // Resource limiters вычисляются, но min ищется только среди первых 10
    // ═══════════════════════════════════════════════════════════════════
    unsigned int min_limiter = 365u;  // MAX
    
    // Быстрый sampling: проверяем только каждый 10-й агент
    for (unsigned int i = 0u; i < frames_total && i < {MAX_FRAMES}u; i += 10u) {{
        const unsigned int limiter = mp_limiters[i];
        if (limiter > 0u && limiter < min_limiter) {{
            min_limiter = limiter;
        }}
    }}
    
    // Программный лимитер
    unsigned int program_limiter = 30u;
    
    // step_days = min(resource_limiter, program_limiter, remaining_to_end)
    unsigned int step_days = min_limiter;
    if (program_limiter < step_days) step_days = program_limiter;
    
    const unsigned int remaining_to_end = end_day - current_day;
    if (remaining_to_end < step_days) step_days = remaining_to_end;
    
    // Минимум 1 день
    if (step_days == 0u) step_days = 1u;
    
    // Устанавливаем step_days
    mp_step[0].exchange(step_days);
    
    // Debug output
    if (current_day % 100u == 0u || step_days > 10u) {{
        printf("[GPU Day %u] step_days=%u (min_resource=%u, program=%u)\\n", 
               current_day, step_days, min_limiter, program_limiter);
    }}
    
    return flamegpu::ALIVE;
}}
"""

    # ═══════════════════════════════════════════════════════════════════════
    # Layer 4: Обновление current_day (после всех инкрементов)
    # ═══════════════════════════════════════════════════════════════════════
    
    RTC_UPDATE_DAY = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_update_day, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Только первый QuotaManager обновляет день
    const unsigned char group_by = FLAMEGPU->getVariable<unsigned char>("group_by");
    if (group_by != 1u) {{
        return flamegpu::ALIVE;
    }}
    
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_current_day");
    auto mp_step = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_step_days");
    auto mp_limiters = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_agent_limiters");
    
    const unsigned int step_days = mp_step[0];
    const unsigned int frames_total = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    if (step_days > 0u) {{
        // Обновляем current_day
        mp_day[0] += step_days;
        
        // НЕ сбрасываем лимитеры — они перезаписываются агентами на следующем шаге
        // Это экономит ~400 exchange операций на шаг
    }}
    
    return flamegpu::ALIVE;
}}
"""

    # Регистрация функций
    
    # Layer: compute_limiters (operations + repair)
    layer_limiters = model.newLayer("adaptive_compute_limiters")
    
    rtc_limiter_ops = heli_agent.newRTCFunction("rtc_compute_limiter_ops", RTC_COMPUTE_LIMITER_OPS)
    rtc_limiter_ops.setInitialState("operations")
    rtc_limiter_ops.setEndState("operations")
    layer_limiters.addAgentFunction(rtc_limiter_ops)
    
    rtc_limiter_repair = heli_agent.newRTCFunction("rtc_compute_limiter_repair", RTC_COMPUTE_LIMITER_REPAIR)
    rtc_limiter_repair.setInitialState("repair")
    rtc_limiter_repair.setEndState("repair")
    layer_limiters.addAgentFunction(rtc_limiter_repair)
    
    # Layer: set_step_days (QuotaManager)
    layer_set_step = model.newLayer("adaptive_set_step_days")
    
    rtc_set_step = quota_agent.newRTCFunction("rtc_set_step_days", RTC_SET_STEP_DAYS)
    layer_set_step.addAgentFunction(rtc_set_step)
    
    # Layer: update_day (QuotaManager) — в конце шага
    layer_update = model.newLayer("adaptive_update_day")
    
    rtc_update = quota_agent.newRTCFunction("rtc_update_day", RTC_UPDATE_DAY)
    layer_update.addAgentFunction(rtc_update)
    
    print(f"    ✅ Зарегистрировано 4 adaptive-GPU функции")
    print(f"    📊 Layer order: compute_limiters → set_step_days → [batch+events] → update_day")


def setup_adaptive_macroproperties(env: fg.EnvironmentDescription, end_day: int):
    """
    Создаёт MacroProperty для адаптивного шага
    
    Используем массивы размера MAX_FRAMES для совместимости с FLAME GPU API:
    - mp_current_day[0] — текущий день
    - mp_step_days[0] — длина шага
    - mp_agent_limiters[idx] — лимитер каждого агента (для reduction)
    """
    
    # Текущий день (используем [0] элемент)
    env.newMacroPropertyUInt32("mp_current_day", MAX_FRAMES)
    
    # Длина текущего шага (используем [0] элемент)
    env.newMacroPropertyUInt32("mp_step_days", MAX_FRAMES)
    
    # Лимитеры каждого агента (для reduction)
    env.newMacroPropertyUInt32("mp_agent_limiters", MAX_FRAMES)
    
    # end_day как Environment property (read-only для RTC)
    env.newPropertyUInt("end_day", end_day)
    
    print(f"  ✅ MacroProperty для adaptive-GPU: mp_current_day[{MAX_FRAMES}], mp_step_days[{MAX_FRAMES}], mp_agent_limiters[{MAX_FRAMES}]")

