#!/usr/bin/env python3
"""
Упрощённая версия spawn для отладки NVRTC 425
Минимальный RTC код без сложных обращений к env
"""

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


def register_simple_spawn(model: 'fg.ModelDescription', agent: 'fg.AgentDescription'):
    """
    Регистрирует упрощённую версию spawn для отладки
    """
    
    print("  Регистрация SIMPLE spawn...")
    
    # Менеджер
    spawn_mgr = model.newAgent("spawn_mgr")
    spawn_mgr.newVariableUInt("next_idx", 0)
    
    # Тикет
    spawn_ticket = model.newAgent("spawn_ticket")
    spawn_ticket.newVariableUInt("ticket", 0)
    
    # MacroProperty
    env = model.Environment()
    env.newMacroPropertyUInt32("spawn_need_u32", 1)
    env.newMacroPropertyUInt32("spawn_base_idx_u32", 1)
    
    # УПРОЩЁННЫЙ менеджер
    RTC_MGR = """
    FLAMEGPU_AGENT_FUNCTION(rtc_spawn_mgr_simple, flamegpu::MessageNone, flamegpu::MessageNone) {
        unsigned int next_idx = FLAMEGPU->getVariable<unsigned int>("next_idx");
        
        // Простой счётчик: каждый день +1 агент
        unsigned int need = 1u;
        
        auto need_out = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("spawn_need_u32");
        auto base_idx_out = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("spawn_base_idx_u32");
        
        need_out[0].exchange(need);
        base_idx_out[0].exchange(next_idx);
        
        FLAMEGPU->setVariable<unsigned int>("next_idx", next_idx + need);
        
        return flamegpu::ALIVE;
    }
    """
    
    # УПРОЩЁННЫЙ тикет - только базовые переменные
    RTC_TICKET = """
    FLAMEGPU_AGENT_FUNCTION(rtc_spawn_ticket_simple, flamegpu::MessageNone, flamegpu::MessageNone) {
        auto need_in = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("spawn_need_u32");
        auto base_idx_in = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("spawn_base_idx_u32");
        
        const unsigned int need = need_in[0];
        const unsigned int base_idx = base_idx_in[0];
        const unsigned int ticket = FLAMEGPU->getVariable<unsigned int>("ticket");
        
        if (ticket >= need) return flamegpu::ALIVE;
        
        const unsigned int idx = base_idx + ticket;
        
        // ТОЛЬКО базовые переменные
        FLAMEGPU->agent_out.setVariable<unsigned int>("idx", idx);
        FLAMEGPU->agent_out.setVariable<unsigned int>("aircraft_number", 100000u + idx);
        FLAMEGPU->agent_out.setVariable<unsigned int>("partseqno_i", 70482u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("group_by", 2u);
        
        // Нормативы - хардкод
        FLAMEGPU->agent_out.setVariable<unsigned int>("ll", 1800000u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("oh", 270000u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("br", 1551121u);
        
        // Обнуляем наработки (без status_id - используем States)
        FLAMEGPU->agent_out.setVariable<unsigned int>("sne", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("ppr", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("cso", 0u);
        
        FLAMEGPU->agent_out.setVariable<unsigned int>("repair_days", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("repair_time", 180u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("assembly_time", 180u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("partout_time", 180u);
        
        FLAMEGPU->agent_out.setVariable<unsigned int>("s6_started", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("s6_days", 0u);
        
        FLAMEGPU->agent_out.setVariable<unsigned int>("intent_state", 2u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("prev_intent_state", 0u);
        
        FLAMEGPU->agent_out.setVariable<unsigned int>("daily_today_u32", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("daily_next_u32", 0u);
        
        FLAMEGPU->agent_out.setVariable<unsigned int>("mfg_date", 0u);
        
        FLAMEGPU->agent_out.setVariable<unsigned int>("assembly_trigger", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("active_trigger", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("partout_trigger", 0u);
        
        return flamegpu::ALIVE;
    }
    """
    
    fn_mgr = spawn_mgr.newRTCFunction("rtc_spawn_mgr_simple", RTC_MGR)
    fn_mgr.setInitialState("default")
    fn_mgr.setEndState("default")
    
    fn_ticket = spawn_ticket.newRTCFunction("rtc_spawn_ticket_simple", RTC_TICKET)
    # Новорожденные в operations с intent=2 (как в рабочем коде b6bc62c8)
    fn_ticket.setAgentOutput(agent, "operations")
    fn_ticket.setInitialState("default")
    fn_ticket.setEndState("default")
    
    layer_mgr = model.newLayer("spawn_mgr_layer")
    layer_mgr.addAgentFunction(fn_mgr)
    
    layer_ticket = model.newLayer("spawn_ticket_layer")
    layer_ticket.addAgentFunction(fn_ticket)
    
    print("  SIMPLE spawn зарегистрирован")
    
    return spawn_mgr, spawn_ticket


def initialize_simple_spawn_population(simulation: 'fg.CUDASimulation', env_data: dict):
    """
    Инициализирует популяцию spawn агентов для упрощённой версии
    
    Args:
        simulation: FLAME GPU симуляция
        env_data: данные окружения
    """
    
    # Создаём единственного менеджера спавна
    mgr_pop = fg.AgentVector(simulation.getAgentDescription("spawn_mgr"))
    mgr_agent = mgr_pop.push_back()
    
    # Упрощённая инициализация: начинаем с first_reserved_idx
    first_reserved = env_data.get('first_reserved_idx', 286)
    mgr_agent.setVariableUInt("next_idx", first_reserved)
    
    simulation.setPopulationData(mgr_pop, "default")
    
    # Создаём пул тикетов (небольшой для теста)
    ticket_pop = fg.AgentVector(simulation.getAgentDescription("spawn_ticket"))
    
    for i in range(10):  # 10 тикетов для теста
        ticket = ticket_pop.push_back()
        ticket.setVariableUInt("ticket", i)
    
    simulation.setPopulationData(ticket_pop, "default")
    
    print(f"  SIMPLE spawn популяция инициализирована:")
    print(f"    - Менеджер: next_idx={first_reserved}")
    print(f"    - Тикетов: 10")
