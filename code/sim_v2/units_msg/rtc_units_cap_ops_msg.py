"""
RTC: жёсткий кап 2 агрегата на планер в ops.

Дата: 15.01.2026
"""
import pyflamegpu as fg

MAX_GROUPS = 50
MAX_PLANERS = 400
MAX_AC_NUMBER = 2000000


def get_rtc_code() -> str:
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_cap_ops, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int planer_idx = FLAMEGPU->getVariable<unsigned int>("planer_idx");
    if (aircraft_number == 0u || group_by < 3u || group_by > 4u) return flamegpu::ALIVE;

    auto mp_cap = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS * MAX_PLANERS}u>("mp_planer_cap");
    if (planer_idx == 0u || planer_idx >= {MAX_PLANERS}u) return flamegpu::ALIVE;

    const unsigned int slots_pos = group_by * {MAX_PLANERS}u + planer_idx;
    unsigned int prev = mp_cap[slots_pos]++;
    if (prev >= 2u) {{
        // лишний агрегат — уводим в serviceable
        FLAMEGPU->setVariable<unsigned int>("aircraft_number", 0u);
        FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
    }}
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    rtc_code = get_rtc_code()
    fn = agent.newRTCFunction("rtc_units_cap_ops", rtc_code)
    fn.setInitialState("operations")
    fn.setEndState("operations")

    layer = model.newLayer("layer_units_msg_cap_ops")
    layer.addAgentFunction(fn)

    print("  RTC модуль units_cap_ops_msg зарегистрирован")
