#!/usr/bin/env python3
"""
State Manager для состояния inactive (1).
Обрабатывает переходы:
- 1→1 (intent=1): остаться в inactive
- 1→2 (intent=2): переход в operations (промоут)
"""

import pyflamegpu as fg


# Условие для intent_state == 1 (остаться в inactive)
RTC_COND_INTENT_1 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_1) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 1u;
}
"""

# Условие для intent_state == 2 (переход в operations)
RTC_COND_INTENT_2 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_2_from_inactive) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    // DEBUG: логировать все проверки
    if (intent == 2u) {
        const unsigned int ac = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        printf("  [COND] AC %u: intent=%u -> condition TRUE\\n", ac, intent);
    }
    return intent == 2u;
}
"""


# Функция для агентов, остающихся в inactive (1->1)
RTC_APPLY_1_TO_1 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_1_to_1, flamegpu::MessageNone, flamegpu::MessageNone) {
    // В состоянии inactive (замороженные) изменений не происходит; агент остаётся в inactive
    // Обнуляем daily_today_u32 (нет налёта в inactive)
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    return flamegpu::ALIVE;
}
"""

# Функция для перехода inactive → operations (1->2)
RTC_APPLY_1_TO_2 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_1_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // Логирование перехода
    const char* type = (group_by == 1u) ? "Mi-8" : (group_by == 2u) ? "Mi-17" : "Unknown";
    printf("  [TRANSITION 1→2 Day %u] AC %u (idx %u, %s): inactive -> operations (intent=%u)\\n", 
           step_day, aircraft_number, idx, type, intent);
    
    // Агент переходит в operations благодаря setEndState("operations")
    return flamegpu::ALIVE;
}
"""


def register_state_manager_inactive(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует state manager для состояния inactive (1→1, 1→2)."""
    print("  Регистрация state manager для inactive (1→1, 1→2)")

    # ВАЖНО: эти слои выполняются ПОСЛЕ quota_promote_inactive
    # Поэтому intent=2 (установленный квотой) будет виден здесь
    
    # 1→2 (intent=2): переход в operations (промоут из inactive)
    # ПЕРВЫМ — переход для тех кого одобрили
    layer_1_to_2 = model.newLayer("transition_inactive_to_ops")
    rtc_func_1_to_2 = agent.newRTCFunction("rtc_apply_inactive_to_ops", RTC_APPLY_1_TO_2)
    rtc_func_1_to_2.setRTCFunctionCondition(RTC_COND_INTENT_2)
    rtc_func_1_to_2.setInitialState("inactive")
    rtc_func_1_to_2.setEndState("operations")
    layer_1_to_2.addAgentFunction(rtc_func_1_to_2)
    
    # 1→1 (intent=1 или другой): остаться в inactive
    # ВТОРЫМ — для остальных кто не получил одобрение
    layer_1_to_1 = model.newLayer("transition_inactive_stay")
    rtc_func_1_to_1 = agent.newRTCFunction("rtc_apply_inactive_stay", RTC_APPLY_1_TO_1)
    rtc_func_1_to_1.setRTCFunctionCondition(RTC_COND_INTENT_1)
    rtc_func_1_to_1.setInitialState("inactive")
    rtc_func_1_to_1.setEndState("inactive")
    layer_1_to_1.addAgentFunction(rtc_func_1_to_1)

    print("  RTC модуль state_manager_inactive зарегистрирован")
