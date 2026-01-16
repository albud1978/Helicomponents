"""
RTC: пересчёт mp_planer_slots по текущим ops агрегатам.

Дата: 15.01.2026
"""
import pyflamegpu as fg

MAX_GROUPS = 50
MAX_PLANERS = 400
MAX_AC_NUMBER = 1_000_000


def get_rtc_code() -> str:
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_slots_count_ops, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u || group_by >= {MAX_GROUPS}u) return flamegpu::ALIVE;

    auto mp_ac_to_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_AC_NUMBER}u>("mp_ac_to_idx");
    auto mp_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS * MAX_PLANERS}u>("mp_planer_slots");
    unsigned int planer_idx = mp_ac_to_idx[aircraft_number];
    if (planer_idx == 0u || planer_idx >= {MAX_PLANERS}u) return flamegpu::ALIVE;

    const unsigned int slots_pos = group_by * {MAX_PLANERS}u + planer_idx;
    mp_slots[slots_pos]++;
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    rtc_code = get_rtc_code()
    fn = agent.newRTCFunction("rtc_units_slots_count_ops", rtc_code)
    fn.setInitialState("operations")
    fn.setEndState("operations")

    layer = model.newLayer("layer_units_msg_slots_count_ops")
    layer.addAgentFunction(fn)

    print("  RTC модуль units_slots_count_msg зарегистрирован")
