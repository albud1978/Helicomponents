#!/usr/bin/env python3
"""
State Manager for reserve (state 5).
Handles transitions:
- 5->5_queue when intent=0 (repair queue hold)
- 5->2 when intent=2 (promoted to operations)
- 5->4 when intent=4 (approved to repair)
- 5->5 when intent=5 (general reserve hold)
"""

import pyflamegpu as fg

# Conditions
RTC_COND_INTENT_0 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_0) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 0u;
}
"""

RTC_COND_INTENT_2_RESERVE = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_2_reserve) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
}
"""

RTC_COND_INTENT_4 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_4) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 4u;
}
"""

RTC_COND_INTENT_5 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_5) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 5u;
}
"""

# 5->5_queue (hold in reserve with intent=0)
RTC_APPLY_5_TO_5_QUEUE = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_5_to_5_queue, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Hold in reserve, keep intent=0 (repair queue)
    return flamegpu::ALIVE;
}
"""

# 5->2 (promoted to operations)
RTC_APPLY_5_TO_2 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_5_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    const char* type = (group_by == 1u) ? "Mi-8" : (group_by == 2u) ? "Mi-17" : "Unknown";
    // PERF OFF: printf("  [TRANSITION 5→2 Day %u] AC %u (idx %u, %s): reserve -> operations\\n", 
           //        step_day, aircraft_number, idx, type);
    
    return flamegpu::ALIVE;
}
"""

# 5->4 (approved to repair)
RTC_APPLY_5_TO_4 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_5_to_4, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Immediate transition reserve -> repair when intent=4
    return flamegpu::ALIVE;
}
"""

# 5->5 (general reserve hold)
RTC_APPLY_5_TO_5 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_5_to_5, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}
"""


def register_state_manager_reserve(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Registers state manager for reserve state."""
    print("  Register state_manager_reserve (5->5_queue, 5->2, 5->4, 5->5)")

    # 5->5_queue (intent=0)
    layer_5_to_5_queue = model.newLayer("transition_5_to_5_queue")
    rtc_func_5_to_5_queue = agent.newRTCFunction("rtc_apply_5_to_5_queue", RTC_APPLY_5_TO_5_QUEUE)
    rtc_func_5_to_5_queue.setRTCFunctionCondition(RTC_COND_INTENT_0)
    rtc_func_5_to_5_queue.setInitialState("reserve")
    rtc_func_5_to_5_queue.setEndState("reserve")
    layer_5_to_5_queue.addAgentFunction(rtc_func_5_to_5_queue)

    # 5->2 (intent=2) - promoted to operations
    layer_5_to_2 = model.newLayer("transition_reserve_to_ops")
    rtc_func_5_to_2 = agent.newRTCFunction("rtc_apply_reserve_to_ops", RTC_APPLY_5_TO_2)
    rtc_func_5_to_2.setRTCFunctionCondition(RTC_COND_INTENT_2_RESERVE)
    rtc_func_5_to_2.setInitialState("reserve")
    rtc_func_5_to_2.setEndState("operations")
    layer_5_to_2.addAgentFunction(rtc_func_5_to_2)

    # 5->4 (intent=4)
    layer_5_to_4 = model.newLayer("transition_5_to_4")
    rtc_func_5_to_4 = agent.newRTCFunction("rtc_apply_5_to_4", RTC_APPLY_5_TO_4)
    rtc_func_5_to_4.setRTCFunctionCondition(RTC_COND_INTENT_4)
    rtc_func_5_to_4.setInitialState("reserve")
    rtc_func_5_to_4.setEndState("unserviceable")
    layer_5_to_4.addAgentFunction(rtc_func_5_to_4)

    # 5->5 (intent=5)
    layer_5_to_5 = model.newLayer("transition_5_to_5")
    rtc_func_5_to_5 = agent.newRTCFunction("rtc_apply_5_to_5", RTC_APPLY_5_TO_5)
    rtc_func_5_to_5.setRTCFunctionCondition(RTC_COND_INTENT_5)
    rtc_func_5_to_5.setInitialState("reserve")
    rtc_func_5_to_5.setEndState("reserve")
    layer_5_to_5.addAgentFunction(rtc_func_5_to_5)

    print("  RTC module state_manager_reserve registered")
