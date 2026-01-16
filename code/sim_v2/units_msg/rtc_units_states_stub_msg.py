"""
RTC модуль stub для фикса intent_state в не-ops состояниях.

Дата: 15.01.2026
"""
import pyflamegpu as fg


def get_rtc_code() -> str:
    return """
FLAMEGPU_AGENT_FUNCTION(rtc_units_stub_serviceable, flamegpu::MessageNone, flamegpu::MessageNone) {
    FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
    return flamegpu::ALIVE;
}

FLAMEGPU_AGENT_FUNCTION(rtc_units_stub_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {
    FLAMEGPU->setVariable<unsigned int>("intent_state", 5u);
    return flamegpu::ALIVE;
}

FLAMEGPU_AGENT_FUNCTION(rtc_units_stub_storage, flamegpu::MessageNone, flamegpu::MessageNone) {
    FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);
    return flamegpu::ALIVE;
}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    rtc_code = get_rtc_code()

    fn_svc = agent.newRTCFunction("rtc_units_stub_serviceable", rtc_code)
    fn_svc.setInitialState("serviceable")
    fn_svc.setEndState("serviceable")

    fn_rsv = agent.newRTCFunction("rtc_units_stub_reserve", rtc_code)
    fn_rsv.setInitialState("reserve")
    fn_rsv.setEndState("reserve")

    fn_sto = agent.newRTCFunction("rtc_units_stub_storage", rtc_code)
    fn_sto.setInitialState("storage")
    fn_sto.setEndState("storage")

    layer = model.newLayer("layer_units_msg_states_stub")
    layer.addAgentFunction(fn_svc)
    layer.addAgentFunction(fn_rsv)
    layer.addAgentFunction(fn_sto)

    print("  RTC модуль units_states_stub_msg зарегистрирован")
