"""
Шаблон для генерации RTC функций записи MP2 со ВСЕМИ агентными переменными
"""

def generate_rtc_write_function(state_id: int, state_name: str, MAX_FRAMES: int, MP2_SIZE: int) -> str:
    """
    Генерирует RTC функцию для записи ВСЕХ агентных переменных в MP2
    
    state_id: 1=inactive, 2=operations, 3=serviceable, 4=repair, 5=reserve, 6=storage
    state_name: строковое название состояния для setInitialState/setEndState
    """
    
    return f"""
    rtc_write_{state_name} = agent.newRTCFunction("rtc_mp2_write_{state_name}", f'''
FLAMEGPU_AGENT_FUNCTION(rtc_mp2_write_{state_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{{{
    // Пропускаем агентов с aircraft_number=0 (запас под спавн)
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) {{{{
        return flamegpu::ALIVE;
    }}}}
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int pos = step_day * {{{MAX_FRAMES}}}u + idx;
    
    // Получаем все MacroProperty для записи (ВСЕ поля)
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_idx");
    auto mp2_aircraft = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_aircraft_number");
    auto mp2_partseqno = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_partseqno");
    auto mp2_group_by = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_group_by");
    
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_state");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_intent_state");
    auto mp2_prev_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_prev_intent_state");
    auto mp2_bi_counter = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_bi_counter");
    
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_ppr");
    auto mp2_cso = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_cso");
    
    auto mp2_ll = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_ll");
    auto mp2_oh = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_oh");
    auto mp2_br = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_br");
    
    auto mp2_repair_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_repair_time");
    auto mp2_assembly_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_assembly_time");
    auto mp2_partout_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_partout_time");
    
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_repair_days");
    auto mp2_s6_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_s6_days");
    auto mp2_assembly_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_assembly_trigger");
    auto mp2_active_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_active_trigger");
    auto mp2_partout_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_partout_trigger");
    auto mp2_mfg_date = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_mfg_date_days");
    
    auto mp2_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_dt");
    auto mp2_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_dn");
    
    auto mp2_ops_ticket = FLAMEGPU->environment.getMacroProperty<unsigned int, {{{MP2_SIZE}}}u>("mp2_ops_ticket");
    
    // Записываем ВСЕ поля
    mp2_day[pos].exchange(step_day);
    mp2_idx[pos].exchange(idx);
    mp2_aircraft[pos].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    mp2_partseqno[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partseqno_i"));
    mp2_group_by[pos].exchange(FLAMEGPU->getVariable<unsigned int>("group_by"));
    
    mp2_state[pos].exchange({state_id}u); // state_id
    mp2_intent[pos].exchange(FLAMEGPU->getVariable<unsigned int>("intent_state"));
    mp2_prev_intent[pos].exchange(FLAMEGPU->getVariable<unsigned int>("prev_intent_state"));
    mp2_bi_counter[pos].exchange(1u);  // Служебное поле для BI (всегда 1)
    
    mp2_sne[pos].exchange(FLAMEGPU->getVariable<unsigned int>("sne"));
    mp2_ppr[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ppr"));
    mp2_cso[pos].exchange(FLAMEGPU->getVariable<unsigned int>("cso"));
    
    mp2_ll[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ll"));
    mp2_oh[pos].exchange(FLAMEGPU->getVariable<unsigned int>("oh"));
    mp2_br[pos].exchange(FLAMEGPU->getVariable<unsigned int>("br"));
    
    mp2_repair_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_time"));
    mp2_assembly_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("assembly_time"));
    mp2_partout_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partout_time"));
    
    mp2_repair_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_days"));
    mp2_s6_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("s6_days"));
    mp2_assembly_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("assembly_trigger"));
    mp2_active_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("active_trigger"));
    mp2_partout_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partout_trigger"));
    mp2_mfg_date[pos].exchange(FLAMEGPU->getVariable<unsigned int>("mfg_date"));
    
    mp2_dt[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_today_u32"));
    mp2_dn[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_next_u32"));
    
    mp2_ops_ticket[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ops_ticket"));
    
    return flamegpu::ALIVE;
}}}}
''')
    rtc_write_{state_name}.setInitialState("{state_name}")
    rtc_write_{state_name}.setEndState("{state_name}")
"""

# Пример использования
if __name__ == "__main__":
    states = [
        (1, "inactive"),
        (2, "operations"),
        (3, "serviceable"),
        (4, "repair"),
        (5, "reserve"),
        (6, "storage")
    ]
    
    MAX_FRAMES = 286
    MP2_SIZE = MAX_FRAMES * 3651  # Для примера
    
    for state_id, state_name in states:
        print(f"\n# ===== {state_name.upper()} (state={state_id}) =====")
        print(generate_rtc_write_function(state_id, state_name, MAX_FRAMES, MP2_SIZE))

