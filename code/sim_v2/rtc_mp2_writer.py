"""
RTC модуль для записи в MP2 (device-side export)
"""

import pyflamegpu as fg

def register_mp2_writer(model: fg.ModelDescription, agent: fg.AgentDescription, clickhouse_client=None):
    """Регистрирует RTC функции для записи в MP2 и host функцию для дренажа"""
    
    # Получаем MAX_FRAMES и DAYS из модели
    MAX_FRAMES = model.Environment().getPropertyUInt("frames_total")
    MAX_DAYS = model.Environment().getPropertyUInt("days_total")
    MP2_SIZE = MAX_FRAMES * (MAX_DAYS + 1)  # Плотная матрица с D+1 паддингом
    
    # Основные колонки MP2
    model.Environment().newMacroPropertyUInt("mp2_day_u16", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_idx", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_aircraft_number", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_state", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_intent_state", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_sne", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_ppr", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_repair_days", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_dt", MP2_SIZE)
    model.Environment().newMacroPropertyUInt("mp2_dn", MP2_SIZE)
    
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
    
    # RTC функция записи полного снимка
    rtc_write_snapshot = agent.newRTCFunction("rtc_mp2_write_snapshot", f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mp2_write_snapshot, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Прямая адресация в плотной матрице
    const unsigned int pos = step_day * {MAX_FRAMES}u + idx;
    
    // Получаем MacroProperty массивы
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_idx");
    auto mp2_aircraft = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_aircraft_number");
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_state");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_intent_state");
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ppr");
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_days");
    auto mp2_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dt");
    auto mp2_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dn");
    
    // Записываем данные агента
    mp2_day[pos].exchange(step_day);
    mp2_idx[pos].exchange(idx);
    mp2_aircraft[pos].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    
    // Маппинг текущего состояния в число
    // В V2 это определяется через getState(), но пока используем переменную
    unsigned int current_state = 0u;
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // Простой маппинг на основе intent (временно)
    if (intent == 1u) current_state = 1u;      // inactive
    else if (intent == 2u) current_state = 2u; // operations
    else if (intent == 3u) current_state = 3u; // serviceable
    else if (intent == 4u) current_state = 4u; // repair
    else if (intent == 5u) current_state = 5u; // reserve
    else if (intent == 6u) current_state = 6u; // storage
    
    mp2_state[pos].exchange(current_state);
    mp2_intent[pos].exchange(intent);
    mp2_sne[pos].exchange(FLAMEGPU->getVariable<unsigned int>("sne"));
    mp2_ppr[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ppr"));
    mp2_repair_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_days"));
    mp2_dt[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_today_u32"));
    mp2_dn[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_next_u32"));
    
    return flamegpu::ALIVE;
}}
""")
    
    # RTC функция записи событий переходов
    rtc_write_event = agent.newRTCFunction("rtc_mp2_write_event", f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mp2_write_event, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Пропускаем агентов с aircraft_number=0 (запас под спавн)
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Детектируем переход состояний
    const unsigned int prev_intent = FLAMEGPU->getVariable<unsigned int>("prev_intent_state");
    const unsigned int curr_intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // Если есть изменение intent и это не "остаемся в том же состоянии"
    if (prev_intent != curr_intent && prev_intent != 0u) {{
        // Атомарно инкрементируем счетчик событий
        auto event_counter = FLAMEGPU->environment.getMacroProperty<unsigned int>("mp2_event_counter");
        unsigned int event_pos = event_counter++;  // Пост-инкремент возвращает старое значение
        event_pos = event_pos % {MP2_EVENT_SIZE}u;  // Кольцевой буфер
        
        // Записываем событие
        auto event_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_EVENT_SIZE}u>("event_day");
        auto event_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_EVENT_SIZE}u>("event_idx");
        auto event_type = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_EVENT_SIZE}u>("event_type");
        auto event_from = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_EVENT_SIZE}u>("event_from_state");
        auto event_to = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_EVENT_SIZE}u>("event_to_state");
        auto event_value1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_EVENT_SIZE}u>("event_value1");
        auto event_value2 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_EVENT_SIZE}u>("event_value2");
        
        event_day[event_pos].exchange(FLAMEGPU->getStepCounter());
        event_idx[event_pos].exchange(FLAMEGPU->getVariable<unsigned int>("idx"));
        event_type[event_pos].exchange(1u);  // 1 = state transition
        event_from[event_pos].exchange(prev_intent);
        event_to[event_pos].exchange(curr_intent);
        event_value1[event_pos].exchange(FLAMEGPU->getVariable<unsigned int>("sne"));
        event_value2[event_pos].exchange(FLAMEGPU->getVariable<unsigned int>("ppr"));
    }}
    
    // Сохраняем текущий intent для следующего шага
    FLAMEGPU->setVariable<unsigned int>("prev_intent_state", curr_intent);
    
    return flamegpu::ALIVE;
}}
""")
    
    # Создаем слои для MP2
    # Нужно создать отдельную функцию записи для каждого состояния
    # чтобы знать текущий state агента
    
    # Функция записи для inactive
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
    
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_state");
    mp2_state[pos].exchange(1u); // 1 = inactive
    
    // Записываем остальные поля
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_idx");
    auto mp2_aircraft = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_aircraft_number");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_intent_state");
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ppr");
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_days");
    auto mp2_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dt");
    auto mp2_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dn");
    
    mp2_day[pos].exchange(step_day);
    mp2_idx[pos].exchange(idx);
    mp2_aircraft[pos].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    mp2_intent[pos].exchange(FLAMEGPU->getVariable<unsigned int>("intent_state"));
    mp2_sne[pos].exchange(FLAMEGPU->getVariable<unsigned int>("sne"));
    mp2_ppr[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ppr"));
    mp2_repair_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_days"));
    mp2_dt[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_today_u32"));
    mp2_dn[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_next_u32"));
    
    return flamegpu::ALIVE;
}}
""")
    rtc_write_inactive.setInitialState("inactive")
    rtc_write_inactive.setEndState("inactive")
    
    # Аналогично для operations (state=2)
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
    
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_state");
    mp2_state[pos].exchange(2u); // 2 = operations
    
    // Остальные поля
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_idx");
    auto mp2_aircraft = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_aircraft_number");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_intent_state");
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ppr");
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_days");
    auto mp2_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dt");
    auto mp2_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dn");
    
    mp2_day[pos].exchange(step_day);
    mp2_idx[pos].exchange(idx);
    mp2_aircraft[pos].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    mp2_intent[pos].exchange(FLAMEGPU->getVariable<unsigned int>("intent_state"));
    mp2_sne[pos].exchange(FLAMEGPU->getVariable<unsigned int>("sne"));
    mp2_ppr[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ppr"));
    mp2_repair_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_days"));
    mp2_dt[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_today_u32"));
    mp2_dn[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_next_u32"));
    
    return flamegpu::ALIVE;
}}
""")
    rtc_write_operations.setInitialState("operations")
    rtc_write_operations.setEndState("operations")
    
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
    
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_state");
    mp2_state[pos].exchange(4u); // 4 = repair
    
    // Остальные поля (код такой же)
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_idx");
    auto mp2_aircraft = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_aircraft_number");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_intent_state");
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ppr");
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_days");
    auto mp2_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dt");
    auto mp2_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dn");
    
    mp2_day[pos].exchange(step_day);
    mp2_idx[pos].exchange(idx);
    mp2_aircraft[pos].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    mp2_intent[pos].exchange(FLAMEGPU->getVariable<unsigned int>("intent_state"));
    mp2_sne[pos].exchange(FLAMEGPU->getVariable<unsigned int>("sne"));
    mp2_ppr[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ppr"));
    mp2_repair_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_days"));
    mp2_dt[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_today_u32"));
    mp2_dn[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_next_u32"));
    
    return flamegpu::ALIVE;
}}
""")
    rtc_write_repair.setInitialState("repair")
    rtc_write_repair.setEndState("repair")
    
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
    
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_state");
    mp2_state[pos].exchange(6u); // 6 = storage
    
    // Остальные поля (код такой же)
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_idx");
    auto mp2_aircraft = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_aircraft_number");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_intent_state");
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ppr");
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_days");
    auto mp2_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dt");
    auto mp2_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dn");
    
    mp2_day[pos].exchange(step_day);
    mp2_idx[pos].exchange(idx);
    mp2_aircraft[pos].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    mp2_intent[pos].exchange(FLAMEGPU->getVariable<unsigned int>("intent_state"));
    mp2_sne[pos].exchange(FLAMEGPU->getVariable<unsigned int>("sne"));
    mp2_ppr[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ppr"));
    mp2_repair_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_days"));
    mp2_dt[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_today_u32"));
    mp2_dn[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_next_u32"));
    
    return flamegpu::ALIVE;
}}
""")
    rtc_write_storage.setInitialState("storage")
    rtc_write_storage.setEndState("storage")
    
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
    
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_state");
    mp2_state[pos].exchange(3u); // 3 = serviceable
    
    // Остальные поля
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_idx");
    auto mp2_aircraft = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_aircraft_number");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_intent_state");
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ppr");
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_days");
    auto mp2_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dt");
    auto mp2_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dn");
    
    mp2_day[pos].exchange(step_day);
    mp2_idx[pos].exchange(idx);
    mp2_aircraft[pos].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    mp2_intent[pos].exchange(FLAMEGPU->getVariable<unsigned int>("intent_state"));
    mp2_sne[pos].exchange(FLAMEGPU->getVariable<unsigned int>("sne"));
    mp2_ppr[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ppr"));
    mp2_repair_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_days"));
    mp2_dt[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_today_u32"));
    mp2_dn[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_next_u32"));
    
    return flamegpu::ALIVE;
}}
""")
    rtc_write_serviceable.setInitialState("serviceable")
    rtc_write_serviceable.setEndState("serviceable")
    
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
    
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_state");
    mp2_state[pos].exchange(5u); // 5 = reserve
    
    // Остальные поля
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_idx");
    auto mp2_aircraft = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_aircraft_number");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_intent_state");
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ppr");
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_days");
    auto mp2_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dt");
    auto mp2_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dn");
    
    mp2_day[pos].exchange(step_day);
    mp2_idx[pos].exchange(idx);
    mp2_aircraft[pos].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    mp2_intent[pos].exchange(FLAMEGPU->getVariable<unsigned int>("intent_state"));
    mp2_sne[pos].exchange(FLAMEGPU->getVariable<unsigned int>("sne"));
    mp2_ppr[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ppr"));
    mp2_repair_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_days"));
    mp2_dt[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_today_u32"));
    mp2_dn[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_next_u32"));
    
    return flamegpu::ALIVE;
}}
""")
    rtc_write_reserve.setInitialState("reserve")
    rtc_write_reserve.setEndState("reserve")
    
    # Слой записи снимков (после всех state transitions)
    layer_snapshot = model.newLayer("mp2_write_snapshot")
    layer_snapshot.addAgentFunction(rtc_write_inactive)
    layer_snapshot.addAgentFunction(rtc_write_operations)
    layer_snapshot.addAgentFunction(rtc_write_serviceable)
    layer_snapshot.addAgentFunction(rtc_write_repair)
    layer_snapshot.addAgentFunction(rtc_write_reserve)
    layer_snapshot.addAgentFunction(rtc_write_storage)
    
    # Слой записи событий (после снимков)
    layer_events = model.newLayer("mp2_write_events")
    layer_events.addAgentFunction(rtc_write_event)
    
    # Добавляем host функцию для дренажа MP2 в СУБД
    if clickhouse_client is not None:
        from mp2_drain_host import MP2DrainHostFunction
        
        # Создаем host функцию для финального дренажа
        mp2_drain = MP2DrainHostFunction(
            client=clickhouse_client,
            table_name='sim_masterv2',
            batch_size=250000,
            simulation_steps=365  # Будет обновлено в orchestrator
        )
        
        # Регистрируем host функцию в отдельном слое после всех RTC функций
        layer_drain = model.newLayer("mp2_drain_to_db")
        layer_drain.addHostFunction(mp2_drain)
        # MP2 drain host функция зарегистрирована
        
        # Возвращаем ссылку на drain функцию для доступа к статистике
        return mp2_drain
    
    print("  MP2 writer зарегистрирован (device-side export)")
    
    return model, agent
