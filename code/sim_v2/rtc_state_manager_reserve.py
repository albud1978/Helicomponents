#!/usr/bin/env python3
"""
State Manager for reserve (state 5).
Handles transitions:
- 5->5_queue when intent=0 (repair queue hold)
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
    print("  Register state_manager_reserve (5->5_queue, 5->4, 5->5)")

    # 5->5_queue (intent=0)
    layer_5_to_5_queue = model.newLayer("transition_5_to_5_queue")
    rtc_func_5_to_5_queue = agent.newRTCFunction("rtc_apply_5_to_5_queue", RTC_APPLY_5_TO_5_QUEUE)
    rtc_func_5_to_5_queue.setRTCFunctionCondition(RTC_COND_INTENT_0)
    rtc_func_5_to_5_queue.setInitialState("reserve")
    rtc_func_5_to_5_queue.setEndState("reserve")
    layer_5_to_5_queue.addAgentFunction(rtc_func_5_to_5_queue)

    # 5->4 (intent=4)
    layer_5_to_4 = model.newLayer("transition_5_to_4")
    rtc_func_5_to_4 = agent.newRTCFunction("rtc_apply_5_to_4", RTC_APPLY_5_TO_4)
    rtc_func_5_to_4.setRTCFunctionCondition(RTC_COND_INTENT_4)
    rtc_func_5_to_4.setInitialState("reserve")
    rtc_func_5_to_4.setEndState("repair")
    layer_5_to_4.addAgentFunction(rtc_func_5_to_4)

    # 5->5 (intent=5)
    layer_5_to_5 = model.newLayer("transition_5_to_5")
    rtc_func_5_to_5 = agent.newRTCFunction("rtc_apply_5_to_5", RTC_APPLY_5_TO_5)
    rtc_func_5_to_5.setRTCFunctionCondition(RTC_COND_INTENT_5)
    rtc_func_5_to_5.setInitialState("reserve")
    rtc_func_5_to_5.setEndState("reserve")
    layer_5_to_5.addAgentFunction(rtc_func_5_to_5)

    print("  RTC module state_manager_reserve registered")
