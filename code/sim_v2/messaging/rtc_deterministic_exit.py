#!/usr/bin/env python3
"""
RTC модуль V6: Детерминированные переходы по дате

Переходы:
- repair (4) → serviceable (3): при current_day >= exit_date
- reserve (5) → operations (2): при current_day >= exit_date (spawn)

Логика:
- exit_date рассчитывается при создании модели
- Функции имеют early exit если current_day < exit_date
- Минимальная нагрузка на шагах до exit_date

Дата: 12.01.2026
"""

import pyflamegpu as fg


# ═══════════════════════════════════════════════════════════════════════════════
# RTC: repair → serviceable (4→3) — детерминированный выход из ремонта
# ═══════════════════════════════════════════════════════════════════════════════

RTC_REPAIR_TO_SERVICEABLE = """
FLAMEGPU_AGENT_FUNCTION(rtc_repair_to_serviceable, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int exit_date = FLAMEGPU->getVariable<unsigned int>("exit_date");
    
    // Early exit если ещё не время
    if (current_day < exit_date) {
        return flamegpu::ALIVE;
    }
    
    // Время пришло → переход repair → serviceable
    // Обнуляем repair_days
    FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
    
    // PPR остаётся = 0 (после ремонта)
    // intent_state = 3 (serviceable) для state_manager
    FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
    
    // Transition флаг
    FLAMEGPU->setVariable<unsigned int>("transition_4_to_3", 1u);
    
    // Reset exit_date
    FLAMEGPU->setVariable<unsigned int>("exit_date", 0xFFFFFFFFu);
    
    return flamegpu::ALIVE;
}
"""

RTC_COND_REPAIR_EXIT = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_repair_ready_to_exit) {
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int exit_date = FLAMEGPU->getVariable<unsigned int>("exit_date");
    return current_day >= exit_date;
}
"""

RTC_REPAIR_STAY = """
FLAMEGPU_AGENT_FUNCTION(rtc_repair_stay, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Агент остаётся в repair (ещё не время выхода)
    FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    return flamegpu::ALIVE;
}
"""

RTC_COND_REPAIR_STAY = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_repair_stay) {
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int exit_date = FLAMEGPU->getVariable<unsigned int>("exit_date");
    return current_day < exit_date;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# RTC: reserve → operations (5→2) — детерминированный spawn
# ═══════════════════════════════════════════════════════════════════════════════

RTC_SPAWN_TO_OPERATIONS = """
FLAMEGPU_AGENT_FUNCTION(rtc_spawn_to_operations, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int exit_date = FLAMEGPU->getVariable<unsigned int>("exit_date");
    
    // Early exit если ещё не время
    if (current_day < exit_date) {
        return flamegpu::ALIVE;
    }
    
    // Время пришло → переход reserve → operations
    // intent_state = 2 (operations) для state_manager
    FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
    
    // Transition флаг
    FLAMEGPU->setVariable<unsigned int>("transition_5_to_2", 1u);
    FLAMEGPU->setVariable<unsigned int>("transition_0_to_2", 1u);  // spawn marker
    
    // Reset exit_date
    FLAMEGPU->setVariable<unsigned int>("exit_date", 0xFFFFFFFFu);
    
    return flamegpu::ALIVE;
}
"""

RTC_COND_SPAWN_READY = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_spawn_ready) {
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int exit_date = FLAMEGPU->getVariable<unsigned int>("exit_date");
    return current_day >= exit_date;
}
"""

RTC_SPAWN_STAY = """
FLAMEGPU_AGENT_FUNCTION(rtc_spawn_stay, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Агент остаётся в reserve (ещё не время spawn)
    FLAMEGPU->setVariable<unsigned int>("intent_state", 5u);
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    return flamegpu::ALIVE;
}
"""

RTC_COND_SPAWN_STAY = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_spawn_stay) {
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int exit_date = FLAMEGPU->getVariable<unsigned int>("exit_date");
    return current_day < exit_date;
}
"""


def register_deterministic_repair(model: fg.ModelDescription, agent: fg.AgentDescription):
    """
    Регистрирует детерминированный выход из repair (4→3)
    
    Два пути:
    - exit_date достигнута → repair → serviceable
    - exit_date не достигнута → остаётся в repair
    """
    print("  Регистрация детерминированного выхода repair → serviceable")
    
    # Layer 1: repair → serviceable (при exit_date)
    layer_exit = model.newLayer("L_repair_to_serviceable")
    fn_exit = agent.newRTCFunction("rtc_repair_to_serviceable", RTC_REPAIR_TO_SERVICEABLE)
    fn_exit.setRTCFunctionCondition(RTC_COND_REPAIR_EXIT)
    fn_exit.setInitialState("repair")
    fn_exit.setEndState("serviceable")
    layer_exit.addAgentFunction(fn_exit)
    
    # Layer 2: repair → repair (stay)
    layer_stay = model.newLayer("L_repair_stay")
    fn_stay = agent.newRTCFunction("rtc_repair_stay", RTC_REPAIR_STAY)
    fn_stay.setRTCFunctionCondition(RTC_COND_REPAIR_STAY)
    fn_stay.setInitialState("repair")
    fn_stay.setEndState("repair")
    layer_stay.addAgentFunction(fn_stay)
    
    print("  ✅ Детерминированный repair (4→3) зарегистрирован")


def register_deterministic_spawn(model: fg.ModelDescription, agent: fg.AgentDescription):
    """
    Регистрирует детерминированный spawn (5→2)
    
    Два пути:
    - exit_date достигнута → reserve → operations
    - exit_date не достигнута → остаётся в reserve
    """
    print("  Регистрация детерминированного spawn reserve → operations")
    
    # Layer 1: reserve → operations (при exit_date)
    layer_spawn = model.newLayer("L_spawn_to_operations")
    fn_spawn = agent.newRTCFunction("rtc_spawn_to_operations", RTC_SPAWN_TO_OPERATIONS)
    fn_spawn.setRTCFunctionCondition(RTC_COND_SPAWN_READY)
    fn_spawn.setInitialState("reserve")
    fn_spawn.setEndState("operations")
    layer_spawn.addAgentFunction(fn_spawn)
    
    # Layer 2: reserve → reserve (stay)
    layer_stay = model.newLayer("L_spawn_stay")
    fn_stay = agent.newRTCFunction("rtc_spawn_stay", RTC_SPAWN_STAY)
    fn_stay.setRTCFunctionCondition(RTC_COND_SPAWN_STAY)
    fn_stay.setInitialState("reserve")
    fn_stay.setEndState("reserve")
    layer_stay.addAgentFunction(fn_stay)
    
    print("  ✅ Детерминированный spawn (5→2) зарегистрирован")


def register_all_deterministic(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует все детерминированные переходы"""
    register_deterministic_repair(model, agent)
    register_deterministic_spawn(model, agent)

