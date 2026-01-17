"""
RTC модуль назначения агрегатов на планеры через сообщения.

Приоритет:
1) serviceable
2) reserve (если svc_count == 0)
3) spawn (active=0 в reserve) — разрешён после repair_time

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
// === Serviceable → Operations ===
FLAMEGPU_AGENT_FUNCTION(rtc_units_assign_serviceable, flamegpu::MessageBruteForce, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int active = FLAMEGPU->getVariable<unsigned int>("active");
    if (active == 0u || group_by < 3u || group_by > 4u) return flamegpu::ALIVE;

    auto mp_need = FLAMEGPU->environment.getMacroProperty<unsigned int, {slots_size}u>("mp_planer_need");

    // Тип планера: group_by=3 → Mi-8 (1), group_by=4 → Mi-17 (2)
    const unsigned int required_type = (group_by == 3u) ? 1u : 2u;

    for (auto msg : FLAMEGPU->message_in) {{
        const unsigned int in_ops = msg.getVariable<unsigned int>("in_ops");
        const unsigned int planer_type = msg.getVariable<unsigned int>("planer_type");
        if (in_ops == 0u || planer_type != required_type) continue;

        const unsigned int planer_idx = msg.getVariable<unsigned int>("planer_idx");
        if (planer_idx >= {MAX_PLANERS}u) continue;

        const unsigned int slots_pos = group_by * {MAX_PLANERS}u + planer_idx;
        unsigned int prev = mp_need[slots_pos]--;
        if (prev == 0u) {{
            mp_need[slots_pos]++;  // rollback
            continue;
        }}

        const unsigned int ac = msg.getVariable<unsigned int>("aircraft_number");
        FLAMEGPU->setVariable<unsigned int>("aircraft_number", ac);
        FLAMEGPU->setVariable<unsigned int>("planer_idx", planer_idx);
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
        return flamegpu::ALIVE;
    }}

    return flamegpu::ALIVE;
}}

// === Spawn activation (reserve, active=0) ===

// === Reserve → Operations ===
FLAMEGPU_AGENT_FUNCTION(rtc_units_assign_reserve, flamegpu::MessageBruteForce, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int active = FLAMEGPU->getVariable<unsigned int>("active");
    if (group_by < 3u || group_by > 4u) return flamegpu::ALIVE;

    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    if (active == 0u) {{
        if (day < repair_time) return flamegpu::ALIVE;  // запрет spawn до repair_time
    }}

    auto mp_need = FLAMEGPU->environment.getMacroProperty<unsigned int, {slots_size}u>("mp_planer_need");
    const unsigned int required_type = (group_by == 3u) ? 1u : 2u;

    for (auto msg : FLAMEGPU->message_in) {{
        const unsigned int in_ops = msg.getVariable<unsigned int>("in_ops");
        const unsigned int planer_type = msg.getVariable<unsigned int>("planer_type");
        if (in_ops == 0u || planer_type != required_type) continue;

        const unsigned int planer_idx = msg.getVariable<unsigned int>("planer_idx");
        if (planer_idx >= {MAX_PLANERS}u) continue;

        const unsigned int slots_pos = group_by * {MAX_PLANERS}u + planer_idx;
        unsigned int prev = mp_need[slots_pos]--;
        if (prev == 0u) {{
            mp_need[slots_pos]++;  // rollback
            continue;
        }}

        if (active == 0u) {{
            FLAMEGPU->setVariable<unsigned int>("active", 1u);
        }}
        const unsigned int ac = msg.getVariable<unsigned int>("aircraft_number");
        FLAMEGPU->setVariable<unsigned int>("aircraft_number", ac);
        FLAMEGPU->setVariable<unsigned int>("planer_idx", planer_idx);
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
        return flamegpu::ALIVE;
    }}

    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    rtc_code = get_rtc_code()

    fn_svc = agent.newRTCFunction("rtc_units_assign_serviceable", rtc_code)
    fn_svc.setInitialState("serviceable")
    fn_svc.setEndState("serviceable")
    fn_svc.setMessageInput("planer_message")

    fn_rsv = agent.newRTCFunction("rtc_units_assign_reserve", rtc_code)
    fn_rsv.setInitialState("reserve")
    fn_rsv.setEndState("reserve")
    fn_rsv.setMessageInput("planer_message")

    layer_svc = model.newLayer("layer_units_msg_assign_serviceable")
    layer_svc.addAgentFunction(fn_svc)

    layer_rsv = model.newLayer("layer_units_msg_assign_reserve")
    layer_rsv.addAgentFunction(fn_rsv)

    print("  RTC модуль units_assign_msg зарегистрирован")
