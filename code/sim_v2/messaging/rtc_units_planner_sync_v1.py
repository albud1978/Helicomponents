"""
RTC модуль синхронизации агрегатов с планером (L2 engines).

Логика:
1) Operations guard: если planner_status != 2 → intent_state=3 (svc)
2) Serviceable with acn>0: если planner_status == 2 → intent_state=2
3) Detach на ремонте планера (planner_status==4 или assembly_trigger>0)

Дата: 26.02.2026
"""

import pyflamegpu as fg

MAX_PLANERS = 400
MAX_AC_NUMBER = 2000000


def get_rtc_code(max_days: int) -> str:
    planer_size = MAX_PLANERS * (max_days + 1)
    return f"""
// Helper: compute planner idx + status

FLAMEGPU_AGENT_FUNCTION(rtc_units_planner_guard_ops, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
        FLAMEGPU->setVariable<unsigned int>("transition_2_to_3", 1u);
        return flamegpu::ALIVE;
    }}

    auto mp_ac_to_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_AC_NUMBER}u>("mp_ac_to_idx");
    unsigned int planer_idx = 0u;
    if (aircraft_number < {MAX_AC_NUMBER}u) {{
        planer_idx = mp_ac_to_idx[aircraft_number];
    }}

    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int pos = step_day * {MAX_PLANERS}u + planer_idx;

    unsigned int planner_status = 0u;
    unsigned int assembly_trigger = 0u;
    if (pos < {planer_size}u) {{
        auto mp_status = FLAMEGPU->environment.getMacroProperty<unsigned int, {planer_size}u>("mp_planer_status");
        auto mp_assembly = FLAMEGPU->environment.getMacroProperty<unsigned int, {planer_size}u>("mp_planer_assembly_trigger");
        planner_status = mp_status[pos];
        assembly_trigger = mp_assembly[pos];
    }}

    if (planner_status == 2u) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
        FLAMEGPU->setVariable<unsigned int>("transition_2_to_3", 0u);
        return flamegpu::ALIVE;
    }}

    const bool planner_repair = (planner_status == 4u) || (assembly_trigger > 0u);
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");

    const bool needs_repair = (oh > 0u && ppr >= oh);
    const bool needs_storage = (ll > 0u && sne >= ll) || (br > 0u && ppr >= br);

    if (planner_repair) {{
        if (needs_storage) {{
            FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);
            FLAMEGPU->setVariable<unsigned int>("transition_2_to_3", 0u);
        }} else if (needs_repair) {{
            FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);
            FLAMEGPU->setVariable<unsigned int>("transition_2_to_3", 0u);
        }} else {{
            FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
            FLAMEGPU->setVariable<unsigned int>("transition_2_to_3", 1u);
        }}
    }} else {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
        FLAMEGPU->setVariable<unsigned int>("transition_2_to_3", 1u);
    }}

    return flamegpu::ALIVE;
}}


FLAMEGPU_AGENT_FUNCTION(rtc_units_planner_guard_serviceable, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
        return flamegpu::ALIVE;
    }}

    auto mp_ac_to_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_AC_NUMBER}u>("mp_ac_to_idx");
    unsigned int planer_idx = 0u;
    if (aircraft_number < {MAX_AC_NUMBER}u) {{
        planer_idx = mp_ac_to_idx[aircraft_number];
    }}

    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int pos = step_day * {MAX_PLANERS}u + planer_idx;

    unsigned int planner_status = 0u;
    if (pos < {planer_size}u) {{
        auto mp_status = FLAMEGPU->environment.getMacroProperty<unsigned int, {planer_size}u>("mp_planer_status");
        planner_status = mp_status[pos];
    }}

    if (planner_status == 2u) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
    }} else {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
    }}

    return flamegpu::ALIVE;
}}


FLAMEGPU_AGENT_FUNCTION(rtc_units_planner_guard_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {{
    FLAMEGPU->setVariable<unsigned int>("intent_state", 5u);
    return flamegpu::ALIVE;
}}


FLAMEGPU_AGENT_FUNCTION(rtc_units_planner_guard_storage, flamegpu::MessageNone, flamegpu::MessageNone) {{
    FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);
    return flamegpu::ALIVE;
}}

FLAMEGPU_AGENT_FUNCTION(rtc_units_planner_detach_ops, flamegpu::MessageNone, flamegpu::MessageNone) {{
    FLAMEGPU->setVariable<unsigned int>("aircraft_number", 0u);
    return flamegpu::ALIVE;
}}

FLAMEGPU_AGENT_FUNCTION(rtc_units_planner_detach_svc, flamegpu::MessageNone, flamegpu::MessageNone) {{
    FLAMEGPU->setVariable<unsigned int>("aircraft_number", 0u);
    return flamegpu::ALIVE;
}}

FLAMEGPU_AGENT_FUNCTION(rtc_units_planner_detach_rsv, flamegpu::MessageNone, flamegpu::MessageNone) {{
    FLAMEGPU->setVariable<unsigned int>("aircraft_number", 0u);
    return flamegpu::ALIVE;
}}
"""


def get_detach_condition_code(max_days: int) -> str:
    planer_size = MAX_PLANERS * (max_days + 1)
    return f"""
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_planner_detach) {{
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) {{
        return false;
    }}

    auto mp_ac_to_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_AC_NUMBER}u>("mp_ac_to_idx");
    unsigned int planer_idx = 0u;
    if (aircraft_number < {MAX_AC_NUMBER}u) {{
        planer_idx = mp_ac_to_idx[aircraft_number];
    }}

    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int pos = step_day * {MAX_PLANERS}u + planer_idx;

    if (pos >= {planer_size}u) {{
        return false;
    }}

    auto mp_status = FLAMEGPU->environment.getMacroProperty<unsigned int, {planer_size}u>("mp_planer_status");
    auto mp_assembly = FLAMEGPU->environment.getMacroProperty<unsigned int, {planer_size}u>("mp_planer_assembly_trigger");

    const unsigned int planner_status = mp_status[pos];
    const unsigned int assembly_trigger = mp_assembly[pos];

    return (planner_status == 4u) || (assembly_trigger > 0u);
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription, max_days: int = 3650):
    rtc_code = get_rtc_code(max_days)
    detach_cond = get_detach_condition_code(max_days)

    # Guards (planner sync)
    fn_ops = agent.newRTCFunction("rtc_units_planner_guard_ops", rtc_code)
    fn_ops.setInitialState("operations")
    fn_ops.setEndState("operations")

    fn_svc = agent.newRTCFunction("rtc_units_planner_guard_serviceable", rtc_code)
    fn_svc.setInitialState("serviceable")
    fn_svc.setEndState("serviceable")

    fn_rsv = agent.newRTCFunction("rtc_units_planner_guard_reserve", rtc_code)
    fn_rsv.setInitialState("reserve")
    fn_rsv.setEndState("reserve")

    fn_stg = agent.newRTCFunction("rtc_units_planner_guard_storage", rtc_code)
    fn_stg.setInitialState("storage")
    fn_stg.setEndState("storage")

    layer_guard = model.newLayer("layer_units_planner_sync")
    layer_guard.addAgentFunction(fn_ops)
    layer_guard.addAgentFunction(fn_svc)
    layer_guard.addAgentFunction(fn_rsv)
    layer_guard.addAgentFunction(fn_stg)

    # Detach on planner repair
    fn_det_ops = agent.newRTCFunction("rtc_units_planner_detach_ops", rtc_code)
    fn_det_ops.setRTCFunctionCondition(detach_cond)
    fn_det_ops.setInitialState("operations")
    fn_det_ops.setEndState("operations")

    fn_det_svc = agent.newRTCFunction("rtc_units_planner_detach_svc", rtc_code)
    fn_det_svc.setRTCFunctionCondition(detach_cond)
    fn_det_svc.setInitialState("serviceable")
    fn_det_svc.setEndState("serviceable")

    fn_det_rsv = agent.newRTCFunction("rtc_units_planner_detach_rsv", rtc_code)
    fn_det_rsv.setRTCFunctionCondition(detach_cond)
    fn_det_rsv.setInitialState("reserve")
    fn_det_rsv.setEndState("reserve")

    layer_detach = model.newLayer("layer_units_planner_detach")
    layer_detach.addAgentFunction(fn_det_ops)
    layer_detach.addAgentFunction(fn_det_svc)
    layer_detach.addAgentFunction(fn_det_rsv)

    print("  RTC модуль units_planner_sync_v1 зарегистрирован (guard + detach)")
