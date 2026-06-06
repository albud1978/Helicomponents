# EXPERIMENTAL / REFERENCE (2026-06-06): пробный L2-контур (group_by=3/4) в messaging. Не production. Боевой L2 — code/sim_v2/units/orchestrator_units.py. Оставлен как справочный черновик.
"""
RTC модуль переходов из operations (L2 engines)

Переходы:
- 2→2 (operations → operations) при intent=2
- 2→3 (operations → serviceable) при intent=3 или transition_2_to_3=1
- 2→4 (operations → repair) при intent=4
- 2→6 (operations → storage) при intent=6

Дата: 26.02.2026
"""

import pyflamegpu as fg


# Условия по intent/flags (RTC condition blocks)
COND_UNITS_OPS_STAY = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_ops_stay) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
}
"""

COND_UNITS_OPS_TO_SERVICEABLE = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_ops_to_serviceable) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    const unsigned int flag = FLAMEGPU->getVariable<unsigned int>("transition_2_to_3");
    return (intent == 3u) || (flag == 1u);
}
"""

COND_UNITS_OPS_TO_REPAIR = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_ops_to_repair) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 4u;
}
"""

COND_UNITS_OPS_TO_STORAGE = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_ops_to_storage) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 6u;
}
"""


def get_rtc_code() -> str:
    return """
// 2→2 (остаёмся в operations)
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_2_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}

// 2→3 (operations → serviceable)
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_2_to_3, flamegpu::MessageNone, flamegpu::MessageNone) {
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_3", 1u);
    FLAMEGPU->setVariable<unsigned int>("aircraft_number", 0u);
    return flamegpu::ALIVE;
}

// 2→4 (operations → repair)
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_2_to_4, flamegpu::MessageNone, flamegpu::MessageNone) {
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_4", 1u);
    FLAMEGPU->setVariable<unsigned int>("aircraft_number", 0u);
    return flamegpu::ALIVE;
}

// 2→6 (operations → storage)
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_2_to_6, flamegpu::MessageNone, flamegpu::MessageNone) {
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_6", 1u);
    FLAMEGPU->setVariable<unsigned int>("aircraft_number", 0u);
    return flamegpu::ALIVE;
}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    rtc_code = get_rtc_code()

    # 2→2
    fn_2_to_2 = agent.newRTCFunction("rtc_units_apply_2_to_2", rtc_code)
    fn_2_to_2.setRTCFunctionCondition(COND_UNITS_OPS_STAY)
    fn_2_to_2.setInitialState("operations")
    fn_2_to_2.setEndState("operations")

    # 2→3
    fn_2_to_3 = agent.newRTCFunction("rtc_units_apply_2_to_3", rtc_code)
    fn_2_to_3.setRTCFunctionCondition(COND_UNITS_OPS_TO_SERVICEABLE)
    fn_2_to_3.setInitialState("operations")
    fn_2_to_3.setEndState("serviceable")

    # 2→4
    fn_2_to_4 = agent.newRTCFunction("rtc_units_apply_2_to_4", rtc_code)
    fn_2_to_4.setRTCFunctionCondition(COND_UNITS_OPS_TO_REPAIR)
    fn_2_to_4.setInitialState("operations")
    fn_2_to_4.setEndState("repair")

    # 2→6
    fn_2_to_6 = agent.newRTCFunction("rtc_units_apply_2_to_6", rtc_code)
    fn_2_to_6.setRTCFunctionCondition(COND_UNITS_OPS_TO_STORAGE)
    fn_2_to_6.setInitialState("operations")
    fn_2_to_6.setEndState("storage")

    layer_2_to_2 = model.newLayer("layer_units_ops_2_to_2")
    layer_2_to_2.addAgentFunction(fn_2_to_2)

    layer_2_to_3 = model.newLayer("layer_units_ops_2_to_3")
    layer_2_to_3.addAgentFunction(fn_2_to_3)

    layer_2_to_4 = model.newLayer("layer_units_ops_2_to_4")
    layer_2_to_4.addAgentFunction(fn_2_to_4)

    layer_2_to_6 = model.newLayer("layer_units_ops_2_to_6")
    layer_2_to_6.addAgentFunction(fn_2_to_6)

    print("  RTC модуль units_transition_ops_v1 зарегистрирован (4 слоя)")
