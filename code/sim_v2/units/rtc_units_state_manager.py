#!/usr/bin/env python3
"""
RTC модуль управления состояниями агрегатов

Обрабатывает переходы между состояниями:
- operations → repair (ppr >= oh)
- operations → storage (sne >= ll или sne >= br)
- repair → reserve (repair_days >= repair_time)
- reserve → operations (назначение через FIFO)
- serviceable → operations (назначение через FIFO)

Дата: 05.01.2026
"""

import pyflamegpu as fg

MAX_GROUPS = 25


def get_rtc_code() -> str:
    """Возвращает CUDA код для управления состояниями"""
    return f"""
// === OPERATIONS: проверка ресурса и инкремент наработки ===
FLAMEGPU_AGENT_FUNCTION(rtc_units_state_operations, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Получаем текущие значения
    unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    // TODO: Получить dt из MP2 планера по aircraft_number
    // Пока используем фиксированное значение 90 минут/день
    const unsigned int dt = 90u;
    
    // Инкремент наработки
    sne += dt;
    ppr += dt;
    FLAMEGPU->setVariable<unsigned int>("sne", sne);
    FLAMEGPU->setVariable<unsigned int>("ppr", ppr);
    
    // Проверка ресурсов
    bool needs_storage = (ll > 0u && sne >= ll) || (br > 0u && sne >= br);
    bool needs_repair = !needs_storage && (oh > 0u && ppr >= oh);
    
    // Особый случай: лопасти (group_by=6) не обнуляют ppr
    if (group_by == 6u && needs_repair) {{
        // Для лопастей: ppr продолжает накапливаться (продление, не ремонт)
        // Но всё равно нужно "продление" через repair
        needs_repair = true;
    }}
    
    if (needs_storage) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);
        FLAMEGPU->setVariable<unsigned int>("transition_2_to_6", 1u);
    }} else if (needs_repair) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);
        FLAMEGPU->setVariable<unsigned int>("transition_2_to_4", 1u);
    }}
    
    return flamegpu::ALIVE;
}}

// === REPAIR: инкремент repair_days и проверка готовности ===
FLAMEGPU_AGENT_FUNCTION(rtc_units_state_repair, flamegpu::MessageNone, flamegpu::MessageNone) {{
    unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    // Инкремент дней в ремонте
    repair_days += 1u;
    FLAMEGPU->setVariable<unsigned int>("repair_days", repair_days);
    
    // Если ремонт завершён — переход в reserve
    if (repair_days >= repair_time) {{
        // Обнуляем ppr (кроме лопастей!)
        if (group_by != 6u) {{
            FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
        }}
        
        FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
        FLAMEGPU->setVariable<unsigned int>("aircraft_number", 0u);  // Снят с планера
        FLAMEGPU->setVariable<unsigned int>("intent_state", 5u);  // reserve
        FLAMEGPU->setVariable<unsigned int>("transition_4_to_5", 1u);
    }}
    
    return flamegpu::ALIVE;
}}

// === RESERVE: ожидание назначения через FIFO ===
FLAMEGPU_AGENT_FUNCTION(rtc_units_state_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Переход 5→2 обрабатывается в rtc_units_fifo_assignment
    // Здесь только stub
    return flamegpu::ALIVE;
}}

// === SERVICEABLE: ожидание назначения через FIFO ===
FLAMEGPU_AGENT_FUNCTION(rtc_units_state_serviceable, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Переход 3→2 обрабатывается в rtc_units_fifo_assignment
    return flamegpu::ALIVE;
}}

// === STORAGE: терминальное состояние ===
FLAMEGPU_AGENT_FUNCTION(rtc_units_state_storage, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Агрегаты в storage не меняют состояние
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции управления состояниями"""
    rtc_code = get_rtc_code()
    
    # Функции для каждого состояния
    fn_ops = agent.newRTCFunction("rtc_units_state_operations", rtc_code)
    fn_ops.setInitialState("operations")
    fn_ops.setEndState("operations")
    
    fn_repair = agent.newRTCFunction("rtc_units_state_repair", rtc_code)
    fn_repair.setInitialState("repair")
    fn_repair.setEndState("repair")
    
    fn_reserve = agent.newRTCFunction("rtc_units_state_reserve", rtc_code)
    fn_reserve.setInitialState("reserve")
    fn_reserve.setEndState("reserve")
    
    fn_serviceable = agent.newRTCFunction("rtc_units_state_serviceable", rtc_code)
    fn_serviceable.setInitialState("serviceable")
    fn_serviceable.setEndState("serviceable")
    
    fn_storage = agent.newRTCFunction("rtc_units_state_storage", rtc_code)
    fn_storage.setInitialState("storage")
    fn_storage.setEndState("storage")
    
    # Слой
    layer = model.newLayer("layer_units_state_manager")
    layer.addAgentFunction(fn_ops)
    layer.addAgentFunction(fn_repair)
    layer.addAgentFunction(fn_reserve)
    layer.addAgentFunction(fn_serviceable)
    layer.addAgentFunction(fn_storage)
    
    print("  RTC модуль units_state_manager зарегистрирован (1 слой)")









