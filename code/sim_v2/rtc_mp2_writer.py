"""
RTC модуль для записи в MP2 (device-side export) - ПОЛНАЯ ВЕРСИЯ
Все 27 агентных переменных логируются в СУБД
"""

import pyflamegpu as fg

def register_mp2_writer(model: fg.ModelDescription, agent: fg.AgentDescription, clickhouse_client=None):
    """Регистрирует RTC функции для записи в MP2 и host функцию для дренажа"""
    
    # Получаем MAX_FRAMES и DAYS из модели
    MAX_FRAMES = model.Environment().getPropertyUInt("frames_total")
    MAX_DAYS = model.Environment().getPropertyUInt("days_total")
    MP2_SIZE = MAX_FRAMES * (MAX_DAYS + 1)  # Плотная матрица с D+1 паддингом
    
    # Основные колонки MP2 (ВСЕ агентные переменные)
    model.Environment().newMacroPropertyUInt("mp2_day_u16", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_idx", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_aircraft_number", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_partseqno", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_group_by", MP2_SIZE)
    
    model.Environment().newMacroPropertyUInt("mp2_state", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_intent_state", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_s6_started", MP2_SIZE)
    
    model.Environment().newMacroPropertyUInt("mp2_sne", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_ppr", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_cso", MP2_SIZE)
    
    model.Environment().newMacroPropertyUInt("mp2_ll", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_oh", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_br", MP2_SIZE)
    
    model.Environment().newMacroPropertyUInt("mp2_repair_time", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_assembly_time", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_partout_time", MP2_SIZE)
    
    model.Environment().newMacroPropertyUInt("mp2_repair_days", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_s6_days", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_assembly_trigger", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_active_trigger", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_partout_trigger", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_mfg_date_days", MP2_SIZE)
    
    model.Environment().newMacroPropertyUInt("mp2_dt", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_dn", MP2_SIZE)
    
    model.Environment().newMacroPropertyUInt("mp2_ops_ticket", MP2_SIZE)
    
    # Буфер событий
    MP2_EVENT_SIZE = 10000
    model.Environment().newMacroPropertyUInt("mp2_event_counter")  # Атомарный счетчик (скаляр)
    model.Environment().newMacroPropertyUInt("event_day", MP2_EVENT_SIZE)
    model.Environment().newMacroPropertyUInt("event_idx", MP2_EVENT_SIZE)
    model.Environment().newMacroPropertyUInt("event_type", MP2_EVENT_SIZE)
    model.Environment().newMacroPropertyUInt("event_from_state", MP2_EVENT_SIZE)
    model.Environment().newMacroPropertyUInt("event_to_state", MP2_EVENT_SIZE)
    model.Environment().newMacroPropertyUInt("event_value1", MP2_EVENT_SIZE)
    model.Environment().newMacroPropertyUInt("event_value2", MP2_EVENT_SIZE)

    # inactive (state=1)
    rtc_write_inactive = agent.newRTCFunction("rtc_mp2_write_inactive", f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mp2_write_inactive, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Пропускаем агентов с aircraft_number=0 (запас под спавн)
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int pos = step_day * {MAX_FRAMES}u + idx;
    
    // Получаем все MacroProperty для записи (ВСЕ поля)
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_idx");
    auto mp2_aircraft = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_aircraft_number");
    auto mp2_partseqno = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partseqno");
    auto mp2_group_by = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_group_by");
    
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_state");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_intent_state");
    auto mp2_s6_started = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_s6_started");
    
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ppr");
    auto mp2_cso = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_cso");
    
    auto mp2_ll = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ll");
    auto mp2_oh = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_oh");
    auto mp2_br = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_br");
    
    auto mp2_repair_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_time");
    auto mp2_assembly_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_time");
    auto mp2_partout_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_time");
    
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_days");
    auto mp2_s6_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_s6_days");
    auto mp2_assembly_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_trigger");
    auto mp2_active_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_active_trigger");
    auto mp2_partout_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_trigger");
    auto mp2_mfg_date = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_mfg_date_days");
    
    auto mp2_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dt");
    auto mp2_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dn");
    
    auto mp2_ops_ticket = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ops_ticket");
    
    // Записываем ВСЕ поля
    mp2_day[pos].exchange(step_day);
    mp2_idx[pos].exchange(idx);
    mp2_aircraft[pos].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    mp2_partseqno[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partseqno_i"));
    mp2_group_by[pos].exchange(FLAMEGPU->getVariable<unsigned int>("group_by"));
    
    mp2_state[pos].exchange(1u); // state_id
    mp2_intent[pos].exchange(FLAMEGPU->getVariable<unsigned int>("intent_state"));
    mp2_s6_started[pos].exchange(FLAMEGPU->getVariable<unsigned int>("s6_started"));
    
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
}}
""")
    rtc_write_inactive.setInitialState("inactive")
    rtc_write_inactive.setEndState("inactive")

    # operations (state=2)
    rtc_write_operations = agent.newRTCFunction("rtc_mp2_write_operations", f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mp2_write_operations, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Пропускаем агентов с aircraft_number=0 (запас под спавн)
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int pos = step_day * {MAX_FRAMES}u + idx;
    
    // Получаем все MacroProperty для записи (ВСЕ поля)
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_idx");
    auto mp2_aircraft = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_aircraft_number");
    auto mp2_partseqno = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partseqno");
    auto mp2_group_by = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_group_by");
    
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_state");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_intent_state");
    auto mp2_s6_started = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_s6_started");
    
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ppr");
    auto mp2_cso = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_cso");
    
    auto mp2_ll = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ll");
    auto mp2_oh = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_oh");
    auto mp2_br = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_br");
    
    auto mp2_repair_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_time");
    auto mp2_assembly_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_time");
    auto mp2_partout_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_time");
    
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_days");
    auto mp2_s6_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_s6_days");
    auto mp2_assembly_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_trigger");
    auto mp2_active_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_active_trigger");
    auto mp2_partout_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_trigger");
    auto mp2_mfg_date = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_mfg_date_days");
    
    auto mp2_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dt");
    auto mp2_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dn");
    
    auto mp2_ops_ticket = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ops_ticket");
    
    // Записываем ВСЕ поля
    mp2_day[pos].exchange(step_day);
    mp2_idx[pos].exchange(idx);
    mp2_aircraft[pos].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    mp2_partseqno[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partseqno_i"));
    mp2_group_by[pos].exchange(FLAMEGPU->getVariable<unsigned int>("group_by"));
    
    mp2_state[pos].exchange(2u); // state_id
    mp2_intent[pos].exchange(FLAMEGPU->getVariable<unsigned int>("intent_state"));
    mp2_s6_started[pos].exchange(FLAMEGPU->getVariable<unsigned int>("s6_started"));
    
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
}}
""")
    rtc_write_operations.setInitialState("operations")
    rtc_write_operations.setEndState("operations")

    # serviceable (state=3)
    rtc_write_serviceable = agent.newRTCFunction("rtc_mp2_write_serviceable", f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mp2_write_serviceable, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Пропускаем агентов с aircraft_number=0 (запас под спавн)
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int pos = step_day * {MAX_FRAMES}u + idx;
    
    // Получаем все MacroProperty для записи (ВСЕ поля)
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_idx");
    auto mp2_aircraft = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_aircraft_number");
    auto mp2_partseqno = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partseqno");
    auto mp2_group_by = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_group_by");
    
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_state");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_intent_state");
    auto mp2_s6_started = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_s6_started");
    
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ppr");
    auto mp2_cso = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_cso");
    
    auto mp2_ll = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ll");
    auto mp2_oh = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_oh");
    auto mp2_br = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_br");
    
    auto mp2_repair_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_time");
    auto mp2_assembly_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_time");
    auto mp2_partout_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_time");
    
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_days");
    auto mp2_s6_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_s6_days");
    auto mp2_assembly_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_trigger");
    auto mp2_active_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_active_trigger");
    auto mp2_partout_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_trigger");
    auto mp2_mfg_date = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_mfg_date_days");
    
    auto mp2_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dt");
    auto mp2_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dn");
    
    auto mp2_ops_ticket = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ops_ticket");
    
    // Записываем ВСЕ поля
    mp2_day[pos].exchange(step_day);
    mp2_idx[pos].exchange(idx);
    mp2_aircraft[pos].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    mp2_partseqno[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partseqno_i"));
    mp2_group_by[pos].exchange(FLAMEGPU->getVariable<unsigned int>("group_by"));
    
    mp2_state[pos].exchange(3u); // state_id
    mp2_intent[pos].exchange(FLAMEGPU->getVariable<unsigned int>("intent_state"));
    mp2_s6_started[pos].exchange(FLAMEGPU->getVariable<unsigned int>("s6_started"));
    
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
}}
""")
    rtc_write_serviceable.setInitialState("serviceable")
    rtc_write_serviceable.setEndState("serviceable")

    # repair (state=4)
    rtc_write_repair = agent.newRTCFunction("rtc_mp2_write_repair", f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mp2_write_repair, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Пропускаем агентов с aircraft_number=0 (запас под спавн)
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int pos = step_day * {MAX_FRAMES}u + idx;
    
    // Получаем все MacroProperty для записи (ВСЕ поля)
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_idx");
    auto mp2_aircraft = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_aircraft_number");
    auto mp2_partseqno = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partseqno");
    auto mp2_group_by = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_group_by");
    
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_state");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_intent_state");
    auto mp2_s6_started = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_s6_started");
    
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ppr");
    auto mp2_cso = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_cso");
    
    auto mp2_ll = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ll");
    auto mp2_oh = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_oh");
    auto mp2_br = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_br");
    
    auto mp2_repair_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_time");
    auto mp2_assembly_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_time");
    auto mp2_partout_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_time");
    
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_days");
    auto mp2_s6_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_s6_days");
    auto mp2_assembly_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_trigger");
    auto mp2_active_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_active_trigger");
    auto mp2_partout_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_trigger");
    auto mp2_mfg_date = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_mfg_date_days");
    
    auto mp2_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dt");
    auto mp2_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dn");
    
    auto mp2_ops_ticket = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ops_ticket");
    
    // Записываем ВСЕ поля
    mp2_day[pos].exchange(step_day);
    mp2_idx[pos].exchange(idx);
    mp2_aircraft[pos].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    mp2_partseqno[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partseqno_i"));
    mp2_group_by[pos].exchange(FLAMEGPU->getVariable<unsigned int>("group_by"));
    
    mp2_state[pos].exchange(4u); // state_id
    mp2_intent[pos].exchange(FLAMEGPU->getVariable<unsigned int>("intent_state"));
    mp2_s6_started[pos].exchange(FLAMEGPU->getVariable<unsigned int>("s6_started"));
    
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
}}
""")
    rtc_write_repair.setInitialState("repair")
    rtc_write_repair.setEndState("repair")

    # reserve (state=5)
    rtc_write_reserve = agent.newRTCFunction("rtc_mp2_write_reserve", f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mp2_write_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Пропускаем агентов с aircraft_number=0 (запас под спавн)
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int pos = step_day * {MAX_FRAMES}u + idx;
    
    // Получаем все MacroProperty для записи (ВСЕ поля)
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_idx");
    auto mp2_aircraft = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_aircraft_number");
    auto mp2_partseqno = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partseqno");
    auto mp2_group_by = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_group_by");
    
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_state");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_intent_state");
    auto mp2_s6_started = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_s6_started");
    
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ppr");
    auto mp2_cso = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_cso");
    
    auto mp2_ll = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ll");
    auto mp2_oh = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_oh");
    auto mp2_br = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_br");
    
    auto mp2_repair_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_time");
    auto mp2_assembly_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_time");
    auto mp2_partout_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_time");
    
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_days");
    auto mp2_s6_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_s6_days");
    auto mp2_assembly_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_trigger");
    auto mp2_active_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_active_trigger");
    auto mp2_partout_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_trigger");
    auto mp2_mfg_date = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_mfg_date_days");
    
    auto mp2_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dt");
    auto mp2_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dn");
    
    auto mp2_ops_ticket = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ops_ticket");
    
    // Записываем ВСЕ поля
    mp2_day[pos].exchange(step_day);
    mp2_idx[pos].exchange(idx);
    mp2_aircraft[pos].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    mp2_partseqno[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partseqno_i"));
    mp2_group_by[pos].exchange(FLAMEGPU->getVariable<unsigned int>("group_by"));
    
    mp2_state[pos].exchange(5u); // state_id
    mp2_intent[pos].exchange(FLAMEGPU->getVariable<unsigned int>("intent_state"));
    mp2_s6_started[pos].exchange(FLAMEGPU->getVariable<unsigned int>("s6_started"));
    
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
}}
""")
    rtc_write_reserve.setInitialState("reserve")
    rtc_write_reserve.setEndState("reserve")

    # storage (state=6)
    rtc_write_storage = agent.newRTCFunction("rtc_mp2_write_storage", f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mp2_write_storage, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Пропускаем агентов с aircraft_number=0 (запас под спавн)
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int pos = step_day * {MAX_FRAMES}u + idx;
    
    // Получаем все MacroProperty для записи (ВСЕ поля)
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_idx");
    auto mp2_aircraft = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_aircraft_number");
    auto mp2_partseqno = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partseqno");
    auto mp2_group_by = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_group_by");
    
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_state");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_intent_state");
    auto mp2_s6_started = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_s6_started");
    
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ppr");
    auto mp2_cso = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_cso");
    
    auto mp2_ll = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ll");
    auto mp2_oh = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_oh");
    auto mp2_br = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_br");
    
    auto mp2_repair_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_time");
    auto mp2_assembly_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_time");
    auto mp2_partout_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_time");
    
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_days");
    auto mp2_s6_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_s6_days");
    auto mp2_assembly_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_trigger");
    auto mp2_active_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_active_trigger");
    auto mp2_partout_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_trigger");
    auto mp2_mfg_date = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_mfg_date_days");
    
    auto mp2_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dt");
    auto mp2_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dn");
    
    auto mp2_ops_ticket = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ops_ticket");
    
    // Записываем ВСЕ поля
    mp2_day[pos].exchange(step_day);
    mp2_idx[pos].exchange(idx);
    mp2_aircraft[pos].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    mp2_partseqno[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partseqno_i"));
    mp2_group_by[pos].exchange(FLAMEGPU->getVariable<unsigned int>("group_by"));
    
    mp2_state[pos].exchange(6u); // state_id
    mp2_intent[pos].exchange(FLAMEGPU->getVariable<unsigned int>("intent_state"));
    mp2_s6_started[pos].exchange(FLAMEGPU->getVariable<unsigned int>("s6_started"));
    
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
}}
""")
    rtc_write_storage.setInitialState("storage")
    rtc_write_storage.setEndState("storage")

    # Создаем layer для записи снимков (все states)
    layer_snapshot = model.newLayer("mp2_write_snapshot")
    layer_snapshot.addAgentFunction(rtc_write_inactive)
    layer_snapshot.addAgentFunction(rtc_write_operations)
    layer_snapshot.addAgentFunction(rtc_write_serviceable)
    layer_snapshot.addAgentFunction(rtc_write_repair)
    layer_snapshot.addAgentFunction(rtc_write_reserve)
    layer_snapshot.addAgentFunction(rtc_write_storage)
    
    # Host функция для дренажа (если нужна)
    if clickhouse_client:
        from mp2_drain_host import MP2DrainHostFunction
        drain_func = MP2DrainHostFunction(
            clickhouse_client,
            table_name='sim_masterv2',
            batch_size=250000,
            simulation_steps=MAX_DAYS
        )
        
        # Регистрируем host функцию в отдельном слое ПОСЛЕ всех RTC функций
        layer_drain = model.newLayer("mp2_drain_to_db")
        layer_drain.addHostFunction(drain_func)
        
        return drain_func
    
    return None
