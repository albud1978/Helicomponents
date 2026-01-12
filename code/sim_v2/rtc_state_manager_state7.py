"""
State Manager V6: Переходы для state 7 (unserviceable)

State 7 = агенты после достижения OH (PPR >= OH)
Ждут промоут P2 от квотирования.

Переходы:
- 7→7: остаться в unserviceable (ожидание)
- 7→2: unserviceable → operations (P2 промоут, PPR=0)

Дата: 12.01.2026
"""

import pyflamegpu as fg


# Условие для intent_state == 7 (остаться в unserviceable)
RTC_COND_INTENT_7 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_7_stay) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 7u;
}
"""

# Условие для intent_state == 2 (переход в operations)
RTC_COND_INTENT_2_FROM_7 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_2_from_state7) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
}
"""


# Функция: остаться в unserviceable (7→7)
RTC_APPLY_7_TO_7 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_state7_stay, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Агент ждёт промоут P2
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    return flamegpu::ALIVE;
}
"""


# Функция: переход unserviceable → operations (7→2)
# КРИТИЧНО: При этом переходе PPR обнуляется!
RTC_APPLY_7_TO_2 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_state7_to_ops, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int ppr_old = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    // ✅ V6: PPR обнуляется при выходе из unserviceable!
    FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
    
    // Transition флаг
    FLAMEGPU->setVariable<unsigned int>("transition_7_to_2", 1u);
    
    // Сброс limiter (будет пересчитан при входе в operations)
    FLAMEGPU->setVariable<unsigned short>("limiter", 0u);
    
    // Логирование перехода
    const char* type = (group_by == 1u) ? "Mi-8" : (group_by == 2u) ? "Mi-17" : "?";
    // DEBUG: printf("[7→2 Day %u] AC %u (idx %u, %s): PPR %u -> 0\\n", 
    //               current_day, aircraft_number, idx, type, ppr_old);
    
    return flamegpu::ALIVE;
}
"""


def register_state_manager_state7(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует state manager для state 7 (unserviceable V6)
    
    Переходы:
    - 7→7: остаться (intent=7)
    - 7→2: unserviceable → operations (intent=2, PPR=0)
    """
    print("  Регистрация state manager для state 7 (unserviceable V6)")
    
    # Layer 1: Transition 7->7 (stay)
    layer_7_to_7 = model.newLayer("transition_state7_stay")
    fn_7_to_7 = agent.newRTCFunction("rtc_apply_state7_stay", RTC_APPLY_7_TO_7)
    fn_7_to_7.setRTCFunctionCondition(RTC_COND_INTENT_7)
    fn_7_to_7.setInitialState("unserviceable")
    fn_7_to_7.setEndState("unserviceable")
    layer_7_to_7.addAgentFunction(fn_7_to_7)
    
    # Layer 2: Transition 7->2 (unserviceable -> operations)
    layer_7_to_2 = model.newLayer("transition_state7_to_ops")
    fn_7_to_2 = agent.newRTCFunction("rtc_apply_state7_to_ops", RTC_APPLY_7_TO_2)
    fn_7_to_2.setRTCFunctionCondition(RTC_COND_INTENT_2_FROM_7)
    fn_7_to_2.setInitialState("unserviceable")
    fn_7_to_2.setEndState("operations")
    layer_7_to_2.addAgentFunction(fn_7_to_2)
    
    print("  ✅ State manager state 7 зарегистрирован (7→7, 7→2)")

