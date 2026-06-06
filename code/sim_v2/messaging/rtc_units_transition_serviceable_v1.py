# EXPERIMENTAL / REFERENCE (2026-06-06): пробный L2-контур (group_by=3/4) в messaging. Не production. Боевой L2 — code/sim_v2/units/orchestrator_units.py. Оставлен как справочный черновик.
"""
RTC модуль переходов из serviceable (L2 engines)

Переходы:
- 3→3 (serviceable → serviceable) при intent=3
- 3→2 (serviceable → operations) при intent=2

Дата: 26.02.2026
"""

import pyflamegpu as fg


def get_rtc_code() -> str:
    return """
// 3→3 (остаёмся в serviceable)
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_3_to_3, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}

// 3→2 (serviceable → operations)
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_3_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {
    FLAMEGPU->setVariable<unsigned int>("transition_3_to_2", 1u);
    return flamegpu::ALIVE;
}
"""


RTC_COND_SERVICEABLE_STAY = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_serviceable_stay) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 3u;
}
"""

RTC_COND_SERVICEABLE_TO_OPS = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_serviceable_to_ops) {
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    return aircraft_number > 0u
        && FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    rtc_code = get_rtc_code()

    fn_3_to_3 = agent.newRTCFunction("rtc_units_apply_3_to_3", rtc_code)
    fn_3_to_3.setRTCFunctionCondition(RTC_COND_SERVICEABLE_STAY)
    fn_3_to_3.setInitialState("serviceable")
    fn_3_to_3.setEndState("serviceable")

    fn_3_to_2 = agent.newRTCFunction("rtc_units_apply_3_to_2", rtc_code)
    fn_3_to_2.setRTCFunctionCondition(RTC_COND_SERVICEABLE_TO_OPS)
    fn_3_to_2.setInitialState("serviceable")
    fn_3_to_2.setEndState("operations")

    layer_3_to_3 = model.newLayer("layer_units_serviceable_3_to_3")
    layer_3_to_3.addAgentFunction(fn_3_to_3)

    layer_3_to_2 = model.newLayer("layer_units_serviceable_3_to_2")
    layer_3_to_2.addAgentFunction(fn_3_to_2)

    print("  RTC модуль units_transition_serviceable_v1 зарегистрирован (2 слоя)")
