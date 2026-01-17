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
FLAMEGPU_AGENT_FUNCTION(rtc_units_assign_serviceable, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int active = FLAMEGPU->getVariable<unsigned int>("active");

    const unsigned int required = 2u;
    auto mp_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, {slots_size}u>("mp_planer_slots");
    auto mp_hits = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_assign_hits");
    auto mp_attempts = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_assign_attempts");
    auto mp_called = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_assign_called");
    auto mp_loop_flag = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_assign_loop_flag");
    auto mp_hit_flag = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_assign_hit_flag");
    if (group_by < {MAX_GROUPS}u) mp_called[group_by] += 1u;
    if (active == 0u || group_by < 3u || group_by > 4u) return flamegpu::ALIVE;

    const unsigned int required_type = (group_by == 3u) ? 1u : 2u;
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int base = day * {MAX_PLANERS}u;
    auto mp_need = FLAMEGPU->environment.getMacroProperty<unsigned int, {slots_size}u>("mp_planer_need");
    auto mp_ops = FLAMEGPU->environment.getMacroProperty<unsigned char, {MAX_PLANERS}u * ({PLANER_MAX_DAYS}u + 1u)>("mp_planer_in_ops_history");
    auto mp_type = FLAMEGPU->environment.getMacroProperty<unsigned char, {MAX_PLANERS}u>("mp_planer_type");
    auto mp_idx_to_ac = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_PLANERS}u>("mp_idx_to_ac");

    if (day == 3649u && group_by == 4u) {{
        const unsigned int ac_debug = mp_idx_to_ac[1u];
        if (ac_debug > 0u) {{
            FLAMEGPU->setVariable<unsigned int>("aircraft_number", ac_debug);
            FLAMEGPU->setVariable<unsigned int>("planer_idx", 1u);
            FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
            mp_hits[group_by] += 1u;
            return flamegpu::ALIVE;
        }}
    }}

    for (unsigned int planer_idx = 0u; planer_idx < {MAX_PLANERS}u; ++planer_idx) {{
        mp_loop_flag[group_by].exchange(1u);
        mp_attempts[group_by] += 1u;
        if (mp_ops[base + planer_idx] == 0u) continue;
        if (mp_type[planer_idx] != required_type) continue;
        const unsigned int slots_pos = group_by * {MAX_PLANERS}u + planer_idx;
        if (mp_need[slots_pos] == 0u) continue;

        unsigned int prev = mp_slots[slots_pos]++;
        if (prev >= required) {{
            mp_slots[slots_pos]--;  // rollback
            continue;
        }}

        const unsigned int ac = mp_idx_to_ac[planer_idx];
        if (ac == 0u) {{
            mp_slots[slots_pos]--;  // rollback
            continue;
        }}
        FLAMEGPU->setVariable<unsigned int>("aircraft_number", ac);
        FLAMEGPU->setVariable<unsigned int>("planer_idx", planer_idx);
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
        mp_hits[group_by] += 1u;
        mp_hit_flag[group_by].exchange(1u);
        return flamegpu::ALIVE;
    }}

    mp_attempts[group_by] += 123u;
    return flamegpu::ALIVE;
}}

// === Spawn activation (reserve, active=0) ===

// === Reserve → Operations ===
FLAMEGPU_AGENT_FUNCTION(rtc_units_assign_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int active = FLAMEGPU->getVariable<unsigned int>("active");

    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    if (active == 0u) {{
        if (day < repair_time) {{
            auto mp_skip = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_assign_skip_repair");
            if (group_by < {MAX_GROUPS}u) mp_skip[group_by] += 1u;
            return flamegpu::ALIVE;  // запрет spawn до repair_time
        }}
    }}

    const unsigned int required = 2u;
    auto mp_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, {slots_size}u>("mp_planer_slots");
    auto mp_hits = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_assign_hits");
    auto mp_attempts = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_assign_attempts");
    auto mp_called = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_assign_called");
    auto mp_loop_flag = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_assign_loop_flag");
    auto mp_hit_flag = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_assign_hit_flag");
    if (group_by < {MAX_GROUPS}u) mp_called[group_by] += 1u;
    if (group_by < 3u || group_by > 4u) return flamegpu::ALIVE;
    const unsigned int required_type = (group_by == 3u) ? 1u : 2u;
    const unsigned int base = day * {MAX_PLANERS}u;
    auto mp_need = FLAMEGPU->environment.getMacroProperty<unsigned int, {slots_size}u>("mp_planer_need");
    auto mp_ops = FLAMEGPU->environment.getMacroProperty<unsigned char, {MAX_PLANERS}u * ({PLANER_MAX_DAYS}u + 1u)>("mp_planer_in_ops_history");
    auto mp_type = FLAMEGPU->environment.getMacroProperty<unsigned char, {MAX_PLANERS}u>("mp_planer_type");
    auto mp_idx_to_ac = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_PLANERS}u>("mp_idx_to_ac");

    for (unsigned int planer_idx = 0u; planer_idx < {MAX_PLANERS}u; ++planer_idx) {{
        mp_loop_flag[group_by].exchange(1u);
        mp_attempts[group_by] += 1u;
        if (mp_ops[base + planer_idx] == 0u) continue;
        if (mp_type[planer_idx] != required_type) continue;
        const unsigned int slots_pos = group_by * {MAX_PLANERS}u + planer_idx;
        if (mp_need[slots_pos] == 0u) continue;

        unsigned int prev = mp_slots[slots_pos]++;
        if (prev >= required) {{
            mp_slots[slots_pos]--;  // rollback
            continue;
        }}

        if (active == 0u) {{
            FLAMEGPU->setVariable<unsigned int>("active", 1u);
        }}
        const unsigned int ac = mp_idx_to_ac[planer_idx];
        if (ac == 0u) {{
            mp_slots[slots_pos]--;  // rollback
            continue;
        }}
        FLAMEGPU->setVariable<unsigned int>("aircraft_number", ac);
        FLAMEGPU->setVariable<unsigned int>("planer_idx", planer_idx);
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
        mp_hits[group_by] += 1u;
        mp_hit_flag[group_by].exchange(1u);
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

    fn_rsv = agent.newRTCFunction("rtc_units_assign_reserve", rtc_code)
    fn_rsv.setInitialState("reserve")
    fn_rsv.setEndState("reserve")

    layer_svc = model.newLayer("layer_units_msg_assign_serviceable")
    layer_svc.addAgentFunction(fn_svc)

    layer_rsv = model.newLayer("layer_units_msg_assign_reserve")
    layer_rsv.addAgentFunction(fn_rsv)

    print("  RTC модуль units_assign_msg зарегистрирован")
