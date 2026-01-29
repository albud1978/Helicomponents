#!/usr/bin/env python3
"""
State Manager for unserviceable (state 7).
Handles transitions:
- 7->7 when intent=0 (queue for repair)
- 7->4 when intent=4 (approved to repair)
"""

import pyflamegpu as fg
import model_build

# Conditions
RTC_COND_INTENT_0 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_0) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 0u;
}
"""

RTC_COND_INTENT_4 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_4) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 4u;
}
"""

RTC_COND_INTENT_2 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_2) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
}
"""

# 7->7 (queue hold)
RTC_APPLY_7_TO_7 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_7_to_7, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}
"""

# 7->4 (approved to repair)
RTC_APPLY_7_TO_4 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_7_to_4, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}
"""

RTC_MAX_FRAMES = model_build.RTC_MAX_FRAMES
RTC_MAX_SIZE = RTC_MAX_FRAMES * (model_build.MAX_DAYS + 1)

RTC_APPLY_7_TO_2 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_apply_7_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int debug_enabled = FLAMEGPU->environment.getProperty<unsigned int>("debug_enabled");
    
    // ✅ Маркер для MP2 postprocess (backfill ремонта)
    FLAMEGPU->setVariable<unsigned int>("active_trigger", 1u);
    const unsigned int mp2_enabled = FLAMEGPU->environment.getProperty<unsigned int>("mp2_enabled");
    if (mp2_enabled) {{
        const unsigned int pos = step_day * {RTC_MAX_FRAMES}u + idx;
        auto mp2_active_source = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_SIZE}u>("mp2_active_source");
        mp2_active_source[pos].exchange(7u);
    }}
    
    // Для 7→2 всегда обнуляем PPR (ремонт обязателен)
    ppr = 0u;
    FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
    
    // Читаем dt из MP5 при переходе в operations
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    auto mp5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_SIZE}u>("mp5_lin");
    const unsigned int base = step_day * frames + idx;
    const unsigned int base_next = base + frames;
    const unsigned int dt = mp5[base];
    const unsigned int dn = (step_day < {model_build.MAX_DAYS}u - 1u) ? mp5[base_next] : 0u;
    
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
    
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    FLAMEGPU->setVariable<unsigned int>("sne", sne + dt);
    FLAMEGPU->setVariable<unsigned int>("ppr", ppr + dt);
    
    if (debug_enabled) {{
        const unsigned int ppr_final = FLAMEGPU->getVariable<unsigned int>("ppr");
        const char* action = (ppr == 0u) ? "ppr=0 (ремонт)" : "ppr сохранён (комплектация)";
        printf("  [TRANSITION 7→2 Day %u] AC %u (group_by=%u): %s, dt=%u, ppr_final=%u\\n", 
               step_day, aircraft_number, group_by, action, dt, ppr_final);
    }}
    
    return flamegpu::ALIVE;
}}
"""


def register_state_manager_unserviceable(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Registers state manager for unserviceable state."""
    print("  Register state_manager_unserviceable (7->7, 7->4)")

    # 7->7 (intent=0)
    layer_7_to_7 = model.newLayer("transition_7_to_7")
    rtc_func_7_to_7 = agent.newRTCFunction("rtc_apply_7_to_7", RTC_APPLY_7_TO_7)
    rtc_func_7_to_7.setRTCFunctionCondition(RTC_COND_INTENT_0)
    rtc_func_7_to_7.setInitialState("unserviceable")
    rtc_func_7_to_7.setEndState("unserviceable")
    layer_7_to_7.addAgentFunction(rtc_func_7_to_7)

    # 7->4 (intent=4)
    layer_7_to_4 = model.newLayer("transition_7_to_4")
    rtc_func_7_to_4 = agent.newRTCFunction("rtc_apply_7_to_4", RTC_APPLY_7_TO_4)
    rtc_func_7_to_4.setRTCFunctionCondition(RTC_COND_INTENT_4)
    rtc_func_7_to_4.setInitialState("unserviceable")
    rtc_func_7_to_4.setEndState("repair")
    layer_7_to_4.addAgentFunction(rtc_func_7_to_4)

    # 7->2 (intent=2) — промоут из очереди сразу в operations с backfill
    layer_7_to_2 = model.newLayer("transition_7_to_2")
    rtc_func_7_to_2 = agent.newRTCFunction("rtc_apply_7_to_2", RTC_APPLY_7_TO_2)
    rtc_func_7_to_2.setRTCFunctionCondition(RTC_COND_INTENT_2)
    rtc_func_7_to_2.setInitialState("unserviceable")
    rtc_func_7_to_2.setEndState("operations")
    layer_7_to_2.addAgentFunction(rtc_func_7_to_2)

    print("  RTC module state_manager_unserviceable registered")



