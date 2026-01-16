"""
RTC модуль отцепления агрегатов при выходе планера из operations.

Дата: 15.01.2026
"""
import pyflamegpu as fg

MAX_GROUPS = 50
MAX_PLANERS = 400
MAX_AC_NUMBER = 2000000
PLANER_MAX_DAYS = 4000


def get_rtc_code() -> str:
    slots_size = MAX_GROUPS * MAX_PLANERS
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_detach_if_planer_left, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int planer_idx = FLAMEGPU->getVariable<unsigned int>("planer_idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");

    if (aircraft_number == 0u || group_by >= {MAX_GROUPS}u) {{
        return flamegpu::ALIVE;
    }}

    auto mp_ops = FLAMEGPU->environment.getMacroProperty<unsigned char, {MAX_PLANERS}u * ({PLANER_MAX_DAYS}u + 1u)>("mp_planer_in_ops_history");
    auto mp_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, {slots_size}u>("mp_planer_slots");
    if (planer_idx == 0u || planer_idx >= {MAX_PLANERS}u) {{
        return flamegpu::ALIVE;
    }}

    if (day < FLAMEGPU->environment.getProperty<unsigned int>("days_total")) {{
        const unsigned int pos = day * {MAX_PLANERS}u + planer_idx;
        const unsigned int in_ops = mp_ops[pos];
        if (in_ops == 0u) {{
            const unsigned int slots_pos = group_by * {MAX_PLANERS}u + planer_idx;
            mp_slots[slots_pos]--;
            FLAMEGPU->setVariable<unsigned int>("aircraft_number", 0u);
            FLAMEGPU->setVariable<unsigned int>("planer_idx", 0u);
            FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
            FLAMEGPU->setVariable<unsigned int>("transition_planer_exit", 1u);
        }}
    }}

    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    rtc_code = get_rtc_code()
    fn = agent.newRTCFunction("rtc_units_detach_if_planer_left", rtc_code)
    fn.setInitialState("operations")
    fn.setEndState("operations")

    layer = model.newLayer("layer_units_msg_detach")
    layer.addAgentFunction(fn)

    print("  RTC модуль units_detach_msg зарегистрирован")
