"""
Переходы из reserve (units_msg).
"""
import pyflamegpu as fg


def get_rtc_code() -> str:
    return """
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_5_to_5, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}

FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_5_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {
    FLAMEGPU->setVariable<unsigned int>("transition_5_to_2", 1u);
    return flamegpu::ALIVE;
}
"""

RTC_COND_5_TO_5 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_5_to_5) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 5u;
}
"""
RTC_COND_5_TO_2 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_5_to_2) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    rtc_code = get_rtc_code()

    fn_5_to_5 = agent.newRTCFunction("rtc_units_apply_5_to_5", rtc_code)
    fn_5_to_5.setRTCFunctionCondition(RTC_COND_5_TO_5)
    fn_5_to_5.setInitialState("reserve")
    fn_5_to_5.setEndState("reserve")

    fn_5_to_2 = agent.newRTCFunction("rtc_units_apply_5_to_2", rtc_code)
    fn_5_to_2.setRTCFunctionCondition(RTC_COND_5_TO_2)
    fn_5_to_2.setInitialState("reserve")
    fn_5_to_2.setEndState("operations")

    layer_5_to_5 = model.newLayer("layer_units_msg_rsv_5_to_5")
    layer_5_to_5.addAgentFunction(fn_5_to_5)

    layer_5_to_2 = model.newLayer("layer_units_msg_rsv_5_to_2")
    layer_5_to_2.addAgentFunction(fn_5_to_2)

    print("  RTC модуль units_transition_reserve_msg зарегистрирован")
