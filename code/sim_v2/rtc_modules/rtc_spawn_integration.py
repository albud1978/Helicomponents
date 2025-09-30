#!/usr/bin/env python3
"""
RTC модуль: Spawn Integration для V2 архитектуры

Интегрирует спавн Ми-17 в модульный пайплайн. 
Решает проблему ошибок NVRTC 425 через чистую архитектуру и минимальные зависимости.
"""

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


def register_spawn_rtc(model: 'fg.ModelDescription', agent: 'fg.AgentDescription', env_data: dict):
    """
    Регистрирует RTC функции для спавна новых агентов (Mi-17)
    
    Args:
        model: FLAME GPU модель
        agent: главный агент (heli)
        env_data: данные окружения с параметрами спавна
    
    Returns:
        tuple: (spawn_mgr_agent, spawn_ticket_agent) для отладки
    """
    
    print("  Регистрация spawn модуля...")
    
    # Создаём вспомогательных агентов для спавна
    spawn_mgr = model.newAgent("spawn_mgr")
    spawn_mgr.newVariableUInt("next_idx", 0)
    spawn_mgr.newVariableUInt("next_acn", 0)
    spawn_mgr.newVariableUInt("next_psn", 0)
    
    spawn_ticket = model.newAgent("spawn_ticket")
    spawn_ticket.newVariableUInt("ticket", 0)
    
    # MacroProperty для передачи данных между менеджером и тикетами
    env = model.Environment()
    env.newMacroPropertyUInt32("spawn_need_u32", 1)
    env.newMacroPropertyUInt32("spawn_base_idx_u32", 1)
    env.newMacroPropertyUInt32("spawn_base_acn_u32", 1)
    env.newMacroPropertyUInt32("spawn_base_psn_u32", 1)
    
    # RTC функция: менеджер спавна
    RTC_SPAWN_MGR = """
    FLAMEGPU_AGENT_FUNCTION(rtc_spawn_mgr, flamegpu::MessageNone, flamegpu::MessageNone) {
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        const unsigned int frames_total = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
        
        // Читаем текущие курсоры
        unsigned int next_idx = FLAMEGPU->getVariable<unsigned int>("next_idx");
        unsigned int next_acn = FLAMEGPU->getVariable<unsigned int>("next_acn");
        unsigned int next_psn = FLAMEGPU->getVariable<unsigned int>("next_psn");
        
        // Читаем потребность в новых бортах на текущий день
        unsigned int need = 0u;
        if (day < days_total) {
            need = FLAMEGPU->environment.getProperty<unsigned int>("mp4_new_counter_mi17_seed", day);
        }
        
        // Клиппинг по доступным слотам
        if (next_idx >= frames_total) need = 0u;
        unsigned int capacity = frames_total - next_idx;
        if (need > capacity) need = capacity;
        
        // Публикуем параметры спавна через MacroProperty
        auto need_out = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("spawn_need_u32");
        auto base_idx_out = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("spawn_base_idx_u32");
        auto base_acn_out = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("spawn_base_acn_u32");
        auto base_psn_out = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("spawn_base_psn_u32");
        
        need_out[0].exchange(need);
        base_idx_out[0].exchange(next_idx);
        base_acn_out[0].exchange(next_acn);
        base_psn_out[0].exchange(next_psn);
        
        // Обновляем курсоры для следующего дня
        FLAMEGPU->setVariable<unsigned int>("next_idx", next_idx + need);
        FLAMEGPU->setVariable<unsigned int>("next_acn", next_acn + need);
        FLAMEGPU->setVariable<unsigned int>("next_psn", next_psn + need);
        
        return flamegpu::ALIVE;
    }
    """
    
    # RTC функция: создание новых агентов
    RTC_SPAWN_TICKET = """
    FLAMEGPU_AGENT_FUNCTION(rtc_spawn_ticket, flamegpu::MessageNone, flamegpu::MessageNone) {
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int frames_total = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
        const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        
        // Читаем параметры спавна
        auto need_in = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("spawn_need_u32");
        auto base_idx_in = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("spawn_base_idx_u32");
        auto base_acn_in = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("spawn_base_acn_u32");
        auto base_psn_in = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("spawn_base_psn_u32");
        
        const unsigned int need = need_in[0];
        const unsigned int base_idx = base_idx_in[0];
        const unsigned int base_acn = base_acn_in[0];
        const unsigned int base_psn = base_psn_in[0];
        
        const unsigned int ticket = FLAMEGPU->getVariable<unsigned int>("ticket");
        if (ticket >= need) return flamegpu::ALIVE;
        
        // Создаём нового агента (Mi-17)
        const unsigned int idx = base_idx + ticket;
        if (idx >= frames_total) return flamegpu::ALIVE;
        
        // Базовые идентификаторы
        FLAMEGPU->agent_out.setVariable<unsigned int>("idx", idx);
        FLAMEGPU->agent_out.setVariable<unsigned int>("aircraft_number", base_acn + ticket);
        FLAMEGPU->agent_out.setVariable<unsigned int>("partseqno_i", 70482u); // Mi-17
        FLAMEGPU->agent_out.setVariable<unsigned int>("group_by", 2u);        // Mi-17
        
        // Нормативы для Mi-17
        const unsigned int ll = FLAMEGPU->environment.getProperty<unsigned int>("mi17_ll_const");
        const unsigned int oh = FLAMEGPU->environment.getProperty<unsigned int>("mi17_oh_const");
        const unsigned int br = FLAMEGPU->environment.getProperty<unsigned int>("mi17_br_const");
        FLAMEGPU->agent_out.setVariable<unsigned int>("ll", ll);
        FLAMEGPU->agent_out.setVariable<unsigned int>("oh", oh);
        FLAMEGPU->agent_out.setVariable<unsigned int>("br", br);
        
        // Начальные значения наработок (без наработок для новорожденных)
        FLAMEGPU->agent_out.setVariable<unsigned int>("sne", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("ppr", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("cso", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("repair_days", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("s6_started", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("s6_days", 0u);
        
        // Intent для новорождённых: хотят в operations (state=2, intent=2)
        FLAMEGPU->agent_out.setVariable<unsigned int>("intent_state", 2u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("prev_intent_state", 0u);
        
        // MP5 переменные (будут заполнены в следующем шаге)
        FLAMEGPU->agent_out.setVariable<unsigned int>("daily_today_u32", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("daily_next_u32", 0u);
        
        // Дата производства: первый день текущего месяца
        unsigned int mfg = 0u;
        if (day < days_total) {
            mfg = FLAMEGPU->environment.getProperty<unsigned int>("month_first_u32", day);
        }
        FLAMEGPU->agent_out.setVariable<unsigned int>("mfg_date", mfg);
        
        // Времена ремонта для Mi-17
        FLAMEGPU->agent_out.setVariable<unsigned int>("repair_time", 
            FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const"));
        FLAMEGPU->agent_out.setVariable<unsigned int>("assembly_time", 
            FLAMEGPU->environment.getProperty<unsigned int>("mi17_assembly_time_const"));
        FLAMEGPU->agent_out.setVariable<unsigned int>("partout_time",
            FLAMEGPU->environment.getProperty<unsigned int>("mi17_partout_time_const"));
        
        // Триггеры обнуляем
        FLAMEGPU->agent_out.setVariable<unsigned int>("active_trigger", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("assembly_trigger", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("partout_trigger", 0u);
        
        return flamegpu::ALIVE;
    }
    """
    
    # Регистрируем RTC функции
    fn_mgr = spawn_mgr.newRTCFunction("rtc_spawn_mgr", RTC_SPAWN_MGR)
    fn_mgr.setInitialState("default")
    fn_mgr.setEndState("default")
    
    fn_ticket = spawn_ticket.newRTCFunction("rtc_spawn_ticket", RTC_SPAWN_TICKET)
    # КРИТИЧНО: новые агенты выводятся в состояние 'serviceable' с intent=2
    # На следующий день они перейдут в operations через state manager
    fn_ticket.setAgentOutput(agent, "serviceable")
    fn_ticket.setInitialState("default")
    fn_ticket.setEndState("default")
    
    # Добавляем слои в конец пайплайна (после state manager и MP2)
    layer_mgr = model.newLayer("spawn_mgr_layer")
    layer_mgr.addAgentFunction(fn_mgr)
    
    layer_ticket = model.newLayer("spawn_ticket_layer")
    layer_ticket.addAgentFunction(fn_ticket)
    
    print(f"  RTC spawn зарегистрирован: mgr + ticket")
    
    return spawn_mgr, spawn_ticket


def initialize_spawn_population(simulation: 'fg.CUDASimulation', env_data: dict):
    """
    Инициализирует популяцию spawn агентов
    
    Args:
        simulation: FLAME GPU симуляция
        env_data: данные окружения с параметрами спавна
    """
    
    # Создаём единственного менеджера спавна
    mgr_pop = fg.AgentVector(simulation.getAgentDescription("spawn_mgr"))
    mgr_agent = mgr_pop.push_back()
    
    # Инициализируем курсоры из env_data
    first_future_idx = env_data.get('first_future_idx', env_data['frames_union_no_future'])
    base_acn_spawn = env_data.get('base_acn_spawn', 100000)
    base_psn_spawn = 70482  # Mi-17 partseqno
    
    mgr_agent.setVariableUInt("next_idx", first_future_idx)
    mgr_agent.setVariableUInt("next_acn", base_acn_spawn)
    mgr_agent.setVariableUInt("next_psn", base_psn_spawn)
    
    simulation.setPopulationData(mgr_pop, "default")
    
    # Создаём пул тикетов (максимум future_spawn_total)
    future_spawn_total = env_data.get('future_spawn_total', 7)
    ticket_pop = fg.AgentVector(simulation.getAgentDescription("spawn_ticket"))
    
    for i in range(future_spawn_total):
        ticket = ticket_pop.push_back()
        ticket.setVariableUInt("ticket", i)
    
    simulation.setPopulationData(ticket_pop, "default")
    
    print(f"  Spawn популяция инициализирована:")
    print(f"    - Менеджер: next_idx={first_future_idx}, next_acn={base_acn_spawn}")
    print(f"    - Тикетов: {future_spawn_total}")
