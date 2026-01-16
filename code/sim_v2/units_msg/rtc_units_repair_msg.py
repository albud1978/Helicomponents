"""
RTC модуль ремонта агрегатов.

Дата: 15.01.2026
"""
import pyflamegpu as fg


def get_rtc_code() -> str:
    return """
FLAMEGPU_AGENT_FUNCTION(rtc_units_repair_step, flamegpu::MessageNone, flamegpu::MessageNone) {
    unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");

    repair_days += 1u;
    FLAMEGPU->setVariable<unsigned int>("repair_days", repair_days);

    if (repair_days >= repair_time) {
        FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
        FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
        FLAMEGPU->setVariable<unsigned int>("aircraft_number", 0u);
        FLAMEGPU->setVariable<unsigned int>("planer_idx", 0u);
        FLAMEGPU->setVariable<unsigned int>("intent_state", 5u);
    }

    return flamegpu::ALIVE;
}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    rtc_code = get_rtc_code()
    fn = agent.newRTCFunction("rtc_units_repair_step", rtc_code)
    fn.setInitialState("repair")
    fn.setEndState("repair")

    layer = model.newLayer("layer_units_msg_repair")
    layer.addAgentFunction(fn)

    print("  RTC модуль units_repair_msg зарегистрирован")
