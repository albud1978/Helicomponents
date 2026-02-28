"""
RTC модуль записи данных агрегатов в MP2 (messaging v1).

Особенности:
- state пишется как фактическое состояние функции (не intent_state)
- пишет pre_state_id для инвариантов INV-7/INV-10

Дата: 27.02.2026
"""

import pyflamegpu as fg


def _get_rtc_code(fn_name: str, state_id: int, max_frames: int, drain_interval: int) -> str:
    buffer_size = max_frames * (drain_interval + 1)
    return f"""
// MP2 Writer для агрегатов (циклический буфер на {drain_interval} дней)
FLAMEGPU_AGENT_FUNCTION({fn_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int max_frames = {max_frames}u;
    const unsigned int drain_interval = {drain_interval}u;

    // Циклическая позиция в буфере (modulo drain_interval)
    const unsigned int buffer_day = step_day % (drain_interval + 1u);
    const unsigned int pos = buffer_day * max_frames + idx;

    // Получаем значения
    const unsigned int psn = FLAMEGPU->getVariable<unsigned int>("psn");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    const unsigned int queue_position = FLAMEGPU->getVariable<unsigned int>("queue_position");
    const unsigned int partseqno_i = FLAMEGPU->getVariable<unsigned int>("partseqno_i");
    const unsigned int active = FLAMEGPU->getVariable<unsigned int>("active");
    const unsigned int pre_state_id = FLAMEGPU->getVariable<unsigned int>("pre_state_id");

    // Фактическое состояние (по функции)
    const unsigned int state = {state_id}u;
    unsigned int state_out = state;
    unsigned int pre_state_out = pre_state_id;
    if (active == 1u) {{
        if (state_out == 5u) {{
            state_out = 3u;
        }}
        if (pre_state_out == 5u) {{
            pre_state_out = 3u;
        }}
    }}

    // Записываем в MacroProperty массивы через .exchange()
    auto mp2_psn = FLAMEGPU->environment.getMacroProperty<unsigned int, {buffer_size}u>("mp2_units_psn");
    auto mp2_group_by = FLAMEGPU->environment.getMacroProperty<unsigned int, {buffer_size}u>("mp2_units_group_by");
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {buffer_size}u>("mp2_units_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {buffer_size}u>("mp2_units_ppr");
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {buffer_size}u>("mp2_units_state");
    auto mp2_pre_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {buffer_size}u>("mp2_units_pre_state");
    auto mp2_ac = FLAMEGPU->environment.getMacroProperty<unsigned int, {buffer_size}u>("mp2_units_ac");
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {buffer_size}u>("mp2_units_repair_days");
    auto mp2_queue_pos = FLAMEGPU->environment.getMacroProperty<unsigned int, {buffer_size}u>("mp2_units_queue_pos");
    auto mp2_partseqno = FLAMEGPU->environment.getMacroProperty<unsigned int, {buffer_size}u>("mp2_units_partseqno");
    auto mp2_active = FLAMEGPU->environment.getMacroProperty<unsigned int, {buffer_size}u>("mp2_units_active");

    mp2_psn[pos].exchange(psn);
    mp2_group_by[pos].exchange(group_by);
    mp2_sne[pos].exchange(sne);
    mp2_ppr[pos].exchange(ppr);
    mp2_state[pos].exchange(state_out);
    mp2_pre_state[pos].exchange(pre_state_out);
    mp2_ac[pos].exchange(aircraft_number);
    mp2_repair_days[pos].exchange(repair_days);
    mp2_queue_pos[pos].exchange(queue_position);
    mp2_partseqno[pos].exchange(partseqno_i);
    mp2_active[pos].exchange(active);

    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription,
                 max_frames: int = 10000, max_days: int = 4000, drain_interval: int = 10):
    states = {
        "operations": 2,
        "serviceable": 3,
        "repair": 4,
        "reserve": 5,
        "storage": 6,
    }

    layer = model.newLayer("layer_units_mp2_write_v1")

    for state_name, state_id in states.items():
        fn_name = f"rtc_units_mp2_write_{state_name}"
        rtc_code = _get_rtc_code(fn_name, state_id, max_frames, drain_interval)
        fn = agent.newRTCFunction(fn_name, rtc_code)
        fn.setInitialState(state_name)
        fn.setEndState(state_name)
        layer.addAgentFunction(fn)

    print(f"  RTC модуль units_mp2_writer_v1 зарегистрирован (1 слой, {len(states)} функций)")
