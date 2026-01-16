"""
Переходы из serviceable (units_msg).
"""
import pyflamegpu as fg


def get_rtc_code() -> str:
    return """
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_3_to_3, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}

FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_3_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {
    FLAMEGPU->setVariable<unsigned int>("transition_3_to_2", 1u);
    return flamegpu::ALIVE;
}
"""

RTC_COND_3_TO_3 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_3_to_3) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 3u;
}
"""
RTC_COND_3_TO_2 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_3_to_2) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    rtc_code = get_rtc_code()

    fn_3_to_3 = agent.newRTCFunction("rtc_units_apply_3_to_3", rtc_code)
    fn_3_to_3.setRTCFunctionCondition(RTC_COND_3_TO_3)
    fn_3_to_3.setInitialState("serviceable")
    fn_3_to_3.setEndState("serviceable")

    fn_3_to_2 = agent.newRTCFunction("rtc_units_apply_3_to_2", rtc_code)
    fn_3_to_2.setRTCFunctionCondition(RTC_COND_3_TO_2)
    fn_3_to_2.setInitialState("serviceable")
    fn_3_to_2.setEndState("operations")

    layer_3_to_3 = model.newLayer("layer_units_msg_svc_3_to_3")
    layer_3_to_3.addAgentFunction(fn_3_to_3)

    layer_3_to_2 = model.newLayer("layer_units_msg_svc_3_to_2")
    layer_3_to_2.addAgentFunction(fn_3_to_2)

    print("  RTC модуль units_transition_serviceable_msg зарегистрирован")
