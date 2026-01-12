#!/usr/bin/env python3
"""
State Manager для serviceable (state=3)

Обрабатывает переходы:
- 3→3 (intent=3): холдинг в serviceable
- 3→2 (intent=2): переход в operations (промоут)
"""

import pyflamegpu as fg

def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """
    Регистрирует RTC функции для управления переходами из serviceable
    """
    
    # ═════════════════════════════════════════════════════════════════════════
    # Переход 3→3: холдинг в serviceable (intent=3)
    # ═════════════════════════════════════════════════════════════════════════
    RTC_SERVICEABLE_HOLDING = """
FLAMEGPU_AGENT_FUNCTION(rtc_serviceable_holding_confirm, flamegpu::MessageNone, flamegpu::MessageNone) {
    // ✅ intent=3 в serviceable - остаёмся в serviceable
    return flamegpu::ALIVE;
}
"""

    RTC_COND_INTENT_3 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_3_serviceable) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 3u;
}
"""
    
    rtc_func_holding = agent.newRTCFunction("rtc_serviceable_holding_confirm", RTC_SERVICEABLE_HOLDING)
    rtc_func_holding.setRTCFunctionCondition(RTC_COND_INTENT_3)
    rtc_func_holding.setInitialState("serviceable")
    rtc_func_holding.setEndState("serviceable")  # ✅ Остаёмся в serviceable!
    rtc_func_holding.setAllowAgentDeath(False)
    
    layer_holding = model.newLayer("serviceable_holding_confirm")
    layer_holding.addAgentFunction(rtc_func_holding)
    
    # ═════════════════════════════════════════════════════════════════════════
    # Переход 3→2: serviceable → operations (intent=2)
    # ═════════════════════════════════════════════════════════════════════════
    RTC_SERVICEABLE_TO_OPS = """
FLAMEGPU_AGENT_FUNCTION(rtc_serviceable_to_operations, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    // Логирование перехода
    const char* type = (group_by == 1u) ? "Mi-8" : (group_by == 2u) ? "Mi-17" : "Unknown";
    // PERF OFF: printf("  [TRANSITION 3→2 Day %u] AC %u (idx %u, %s): serviceable -> operations\\n", 
           //        step_day, aircraft_number, idx, type);
    
    return flamegpu::ALIVE;
}
"""

    RTC_COND_INTENT_2 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_2_serviceable) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
}
"""
    
    rtc_func_to_ops = agent.newRTCFunction("rtc_svc_to_ops", RTC_SERVICEABLE_TO_OPS)
    rtc_func_to_ops.setRTCFunctionCondition(RTC_COND_INTENT_2)
    rtc_func_to_ops.setInitialState("serviceable")
    rtc_func_to_ops.setEndState("operations")  # ✅ Переход в operations!
    rtc_func_to_ops.setAllowAgentDeath(False)
    
    layer_to_ops = model.newLayer("transition_svc_to_ops")
    layer_to_ops.addAgentFunction(rtc_func_to_ops)
    
    print("  Регистрация state manager для serviceable (3→3 холдинг, 3→2 промоут)")

