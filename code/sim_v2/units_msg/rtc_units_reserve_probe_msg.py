"""
RTC probe: отмечает запуск agent-функции в reserve слое.
"""
import pyflamegpu as fg


def get_rtc_code() -> str:
    return """
FLAMEGPU_AGENT_FUNCTION(rtc_units_reserve_probe, flamegpu::MessageNone, flamegpu::MessageNone) {
    auto probe = FLAMEGPU->environment.getMacroProperty<unsigned int, 50u>("mp_reserve_probe");
    probe[0].exchange(1u);
    return flamegpu::ALIVE;
}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    rtc_code = get_rtc_code()
    fn = agent.newRTCFunction("rtc_units_reserve_probe", rtc_code)
    fn.setInitialState("reserve")
    fn.setEndState("reserve")

    layer = model.newLayer("layer_units_msg_reserve_probe")
    layer.addAgentFunction(fn)
    print("  RTC модуль units_reserve_probe_msg зарегистрирован")
