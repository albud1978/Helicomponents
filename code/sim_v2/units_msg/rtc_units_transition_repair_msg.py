"""
Переходы из repair (units_msg).
"""
import pyflamegpu as fg


def get_rtc_code() -> str:
    return """
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_4_to_4, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}

FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_4_to_5, flamegpu::MessageNone, flamegpu::MessageNone) {
    FLAMEGPU->setVariable<unsigned int>("transition_4_to_5", 1u);
    return flamegpu::ALIVE;
}
"""

RTC_COND_4_TO_4 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_4_to_4) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 4u;
}
"""
RTC_COND_4_TO_5 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_4_to_5) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 5u;
}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    rtc_code = get_rtc_code()

    fn_4_to_4 = agent.newRTCFunction("rtc_units_apply_4_to_4", rtc_code)
    fn_4_to_4.setRTCFunctionCondition(RTC_COND_4_TO_4)
    fn_4_to_4.setInitialState("repair")
    fn_4_to_4.setEndState("repair")

    fn_4_to_5 = agent.newRTCFunction("rtc_units_apply_4_to_5", rtc_code)
    fn_4_to_5.setRTCFunctionCondition(RTC_COND_4_TO_5)
    fn_4_to_5.setInitialState("repair")
    fn_4_to_5.setEndState("reserve")

    layer_4_to_4 = model.newLayer("layer_units_msg_rep_4_to_4")
    layer_4_to_4.addAgentFunction(fn_4_to_4)

    layer_4_to_5 = model.newLayer("layer_units_msg_rep_4_to_5")
    layer_4_to_5.addAgentFunction(fn_4_to_5)

    print("  RTC модуль units_transition_repair_msg зарегистрирован")
