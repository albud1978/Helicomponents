"""
Терминальное состояние storage (units_msg).
"""
import pyflamegpu as fg


def get_rtc_code() -> str:
    return """
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_6_to_6, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    rtc_code = get_rtc_code()
    fn = agent.newRTCFunction("rtc_units_apply_6_to_6", rtc_code)
    fn.setInitialState("storage")
    fn.setEndState("storage")

    layer = model.newLayer("layer_units_msg_storage_6_to_6")
    layer.addAgentFunction(fn)

    print("  RTC модуль units_transition_storage_msg зарегистрирован")
