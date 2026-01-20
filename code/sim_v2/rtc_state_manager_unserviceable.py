#!/usr/bin/env python3
"""
State Manager for unserviceable (state 7).
Handles transitions:
- 7->7 when intent=0 (queue for repair)
- 7->4 when intent=4 (approved to repair)
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

    print("  RTC module state_manager_unserviceable registered")

