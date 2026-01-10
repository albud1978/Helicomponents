#!/usr/bin/env python3
"""
RTC модуль: Батчевые инкременты для адаптивного шага

Вместо ежедневного инкремента sne += dt:
- Читаем step_days из Environment
- Вычисляем total_dt = sum(dt[current_day : current_day + step_days])
- Применяем батчевый инкремент: sne += total_dt, ppr += total_dt
"""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
import model_build

import pyflamegpu as fg


MAX_FRAMES = model_build.RTC_MAX_FRAMES
MAX_DAYS = model_build.MAX_DAYS


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции для батчевых инкрементов"""
    
    print("  📦 Регистрация модуля: batch_operations (адаптивный шаг)")
    
    # ═══════════════════════════════════════════════════════════════════════
    # RTC функция для OPERATIONS: батчевый инкремент sne/ppr
    # ═══════════════════════════════════════════════════════════════════════
    
    # Размер cumsum: MAX_FRAMES * (MAX_DAYS + 1) — фиксированный для RTC
    cumsum_size = MAX_FRAMES * (MAX_DAYS + 1)
    cumsum_stride = MAX_DAYS + 1  # Шаг для индексации
    
    RTC_BATCH_INCREMENT_OPS = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_batch_increment_ops, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int step_days = FLAMEGPU->environment.getProperty<unsigned int>("step_days");
    
    if (step_days == 0) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Размер cumsum фиксированный: MAX_FRAMES * (MAX_DAYS + 1)
    const unsigned int cumsum_stride = {cumsum_stride}u;
    
    // Читаем кумулятивные суммы dt
    auto mp5_cumsum = FLAMEGPU->environment.getMacroProperty<unsigned int, {cumsum_size}u>("mp5_cumsum");
    
    const unsigned int base = idx * cumsum_stride;
    const unsigned int start_cumsum = mp5_cumsum[base + current_day];
    const unsigned int end_day = current_day + step_days;
    const unsigned int end_cumsum = mp5_cumsum[base + end_day];
    const unsigned int total_dt = end_cumsum - start_cumsum;
    
    // Батчевый инкремент
    unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    
    sne += total_dt;
    ppr += total_dt;
    
    FLAMEGPU->setVariable<unsigned int>("sne", sne);
    FLAMEGPU->setVariable<unsigned int>("ppr", ppr);
    
    // Сохраняем total_dt для MP2 экспорта
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", total_dt);
    
    // Проверка достижения лимита
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    
    if (sne >= ll || ppr >= oh) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);  // -> repair
    }}
    
    return flamegpu::ALIVE;
}}
"""

    # ═══════════════════════════════════════════════════════════════════════
    # RTC функция для REPAIR: батчевый инкремент repair_days
    # ═══════════════════════════════════════════════════════════════════════
    
    RTC_BATCH_INCREMENT_REPAIR = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_batch_increment_repair, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int step_days = FLAMEGPU->environment.getProperty<unsigned int>("step_days");
    
    if (step_days == 0) {{
        return flamegpu::ALIVE;
    }}
    
    // Инкремент дней ремонта
    unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    repair_days += step_days;
    FLAMEGPU->setVariable<unsigned int>("repair_days", repair_days);
    
    // Проверка завершения ремонта
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    
    if (repair_days >= repair_time) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 5u);  // -> reserve
    }}
    
    return flamegpu::ALIVE;
}}
"""

    # ═══════════════════════════════════════════════════════════════════════
    # RTC функция для RESERVE: батчевый инкремент reserve_days
    # ═══════════════════════════════════════════════════════════════════════
    
    RTC_BATCH_INCREMENT_RESERVE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_batch_increment_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Reserve: инкремент repair_days (используется тот же счётчик что и в repair)
    // Можно оставить пустым — reserve агенты просто ждут
    return flamegpu::ALIVE;
}}
"""

    # ═══════════════════════════════════════════════════════════════════════
    # RTC функция для INACTIVE/SERVICEABLE/STORAGE: noop батч
    # ═══════════════════════════════════════════════════════════════════════
    
    RTC_BATCH_NOOP = """
FLAMEGPU_AGENT_FUNCTION(rtc_batch_noop, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Состояния без батчевых инкрементов
    // (inactive ждёт активации, serviceable — холдинг, storage — конечное)
    return flamegpu::ALIVE;
}
"""

    # Регистрация
    state_rtc = {
        "operations": ("rtc_batch_increment_ops", RTC_BATCH_INCREMENT_OPS),
        "repair": ("rtc_batch_increment_repair", RTC_BATCH_INCREMENT_REPAIR),
        "reserve": ("rtc_batch_increment_reserve", RTC_BATCH_INCREMENT_RESERVE),
        "inactive": ("rtc_batch_noop_inactive", RTC_BATCH_NOOP),
        "serviceable": ("rtc_batch_noop_serviceable", RTC_BATCH_NOOP),
        "storage": ("rtc_batch_noop_storage", RTC_BATCH_NOOP),
    }
    
    for state_name, (func_name, rtc_code) in state_rtc.items():
        layer = model.newLayer(f"batch_{state_name}")
        rtc_func = agent.newRTCFunction(func_name, rtc_code)
        rtc_func.setInitialState(state_name)
        rtc_func.setEndState(state_name)
        layer.addAgentFunction(rtc_func)
    
    print(f"    ✅ Зарегистрировано {len(state_rtc)} batch-функций")

