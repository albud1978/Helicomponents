"""
Переходы из operations (units_msg).

Дата: 15.01.2026
"""
import pyflamegpu as fg

MAX_GROUPS = 50
MAX_PLANERS = 400
MAX_AC_NUMBER = 2000000


def get_rtc_code() -> str:
    slots_size = MAX_GROUPS * MAX_PLANERS
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_2_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {{
    return flamegpu::ALIVE;
}}

FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_2_to_3, flamegpu::MessageNone, flamegpu::MessageNone) {{
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_3", 1u);
    return flamegpu::ALIVE;
}}

FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_2_to_4, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number > 0u && group_by < {MAX_GROUPS}u) {{
        auto mp_ac_to_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_AC_NUMBER}u>("mp_ac_to_idx");
        auto mp_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, {slots_size}u>("mp_planer_slots");
        unsigned int planer_idx = mp_ac_to_idx[aircraft_number];
        if (planer_idx > 0u && planer_idx < {MAX_PLANERS}u) {{
            const unsigned int slots_pos = group_by * {MAX_PLANERS}u + planer_idx;
            mp_slots[slots_pos]--;
        }}
    }}
    FLAMEGPU->setVariable<unsigned int>("aircraft_number", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_4", 1u);
    return flamegpu::ALIVE;
}}

FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_2_to_6, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number > 0u && group_by < {MAX_GROUPS}u) {{
        auto mp_ac_to_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_AC_NUMBER}u>("mp_ac_to_idx");
        auto mp_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, {slots_size}u>("mp_planer_slots");
        unsigned int planer_idx = mp_ac_to_idx[aircraft_number];
        if (planer_idx > 0u && planer_idx < {MAX_PLANERS}u) {{
            const unsigned int slots_pos = group_by * {MAX_PLANERS}u + planer_idx;
            mp_slots[slots_pos]--;
        }}
    }}
    FLAMEGPU->setVariable<unsigned int>("aircraft_number", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_6", 1u);
    return flamegpu::ALIVE;
}}
"""

RTC_COND_2_TO_2 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_2_to_2) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
}
"""
RTC_COND_2_TO_3 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_2_to_3) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 3u;
}
"""
RTC_COND_2_TO_4 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_2_to_4) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 4u;
}
"""
RTC_COND_2_TO_6 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_2_to_6) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 6u;
}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    rtc_code = get_rtc_code()

    fn_2_to_2 = agent.newRTCFunction("rtc_units_apply_2_to_2", rtc_code)
    fn_2_to_2.setRTCFunctionCondition(RTC_COND_2_TO_2)
    fn_2_to_2.setInitialState("operations")
    fn_2_to_2.setEndState("operations")

    fn_2_to_3 = agent.newRTCFunction("rtc_units_apply_2_to_3", rtc_code)
    fn_2_to_3.setRTCFunctionCondition(RTC_COND_2_TO_3)
    fn_2_to_3.setInitialState("operations")
    fn_2_to_3.setEndState("serviceable")

    fn_2_to_4 = agent.newRTCFunction("rtc_units_apply_2_to_4", rtc_code)
    fn_2_to_4.setRTCFunctionCondition(RTC_COND_2_TO_4)
    fn_2_to_4.setInitialState("operations")
    fn_2_to_4.setEndState("repair")

    fn_2_to_6 = agent.newRTCFunction("rtc_units_apply_2_to_6", rtc_code)
    fn_2_to_6.setRTCFunctionCondition(RTC_COND_2_TO_6)
    fn_2_to_6.setInitialState("operations")
    fn_2_to_6.setEndState("storage")

    layer_2_to_2 = model.newLayer("layer_units_msg_ops_2_to_2")
    layer_2_to_2.addAgentFunction(fn_2_to_2)

    layer_2_to_3 = model.newLayer("layer_units_msg_ops_2_to_3")
    layer_2_to_3.addAgentFunction(fn_2_to_3)

    layer_2_to_4 = model.newLayer("layer_units_msg_ops_2_to_4")
    layer_2_to_4.addAgentFunction(fn_2_to_4)

    layer_2_to_6 = model.newLayer("layer_units_msg_ops_2_to_6")
    layer_2_to_6.addAgentFunction(fn_2_to_6)

    print("  RTC модуль units_transition_ops_msg зарегистрирован")
