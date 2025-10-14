#!/usr/bin/env python3
"""
RTC Spawn для orchestrator_v2
Адаптирован из sim_master b6bc62c8 под новый набор переменных

ИЗМЕНЕНИЯ относительно sim_master:
- Убрано: status_id, intent_flag, psn, ac_type_mask, ops_ticket
- Добавлено: cso=0, intent_state=2 (вместо intent_flag)
- State для новорожденных: serviceable (нейтральное)
"""

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")

from model_build import MAX_DAYS
from string import Template


def register_rtc(model: 'fg.ModelDescription', agent: 'fg.AgentDescription', env_data: dict):
    """
    Регистрация spawn для orchestrator_v2
    
    mp4_new_counter_mi17_seed УЖЕ создан в base_model
    Дата производства = version_date + day (БЕЗ month_first_u32)
    """
    
    env = model.Environment()
    
    # frames_initial для spawn (ПРАВИЛЬНО: first_reserved_idx!)
    frames_initial = env_data.get('first_reserved_idx', 279)
    env.newPropertyUInt("frames_initial", frames_initial)
    
    # Агенты-утилиты спавна
    spawn_mgr = model.newAgent("spawn_mgr")
    spawn_mgr.newState("default")
    spawn_mgr.newVariableUInt("next_idx", 0)
    spawn_mgr.newVariableUInt("next_acn", 0)
    spawn_mgr.newVariableUInt("next_psn", 0)

    spawn_ticket = model.newAgent("spawn_ticket")
    spawn_ticket.newState("default")
    spawn_ticket.newVariableUInt("ticket", 0)

    # MacroProperty МАССИВЫ по дням (как в sim_master!)
    env.newMacroPropertyUInt("spawn_need_u32", MAX_DAYS)
    env.newMacroPropertyUInt("spawn_base_idx_u32", MAX_DAYS)
    env.newMacroPropertyUInt("spawn_base_acn_u32", MAX_DAYS)
    env.newMacroPropertyUInt("spawn_base_psn_u32", MAX_DAYS)

    # RTC менеджер (как в sim_master с Template)
    RTC_SPAWN_MGR = Template("""
    FLAMEGPU_AGENT_FUNCTION(rtc_spawn_mgr_v2, flamegpu::MessageNone, flamegpu::MessageNone) {
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        const unsigned int safe_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
        
        // Читаем need из PropertyArray
        const unsigned int need = FLAMEGPU->environment.getProperty<unsigned int>("mp4_new_counter_mi17_seed", safe_day);
        
        // Курсоры
        unsigned int next_idx = FLAMEGPU->getVariable<unsigned int>("next_idx");
        unsigned int next_acn = FLAMEGPU->getVariable<unsigned int>("next_acn");
        unsigned int next_psn = FLAMEGPU->getVariable<unsigned int>("next_psn");
        
        // Инициализация в день 0
        if (day == 0u) {
            const unsigned int frames_initial = FLAMEGPU->environment.getProperty<unsigned int>("frames_initial");
            if (next_idx < frames_initial) next_idx = frames_initial;
            if (next_acn < 100000u) next_acn = 100000u;
            // Читаем partseqno из Environment (единая точка определения)
            const unsigned int spawn_psn = FLAMEGPU->environment.getProperty<unsigned int>("spawn_partseqno_mi17");
            if (next_psn < spawn_psn) next_psn = spawn_psn;
        }
        
        // Публикуем в MacroProperty МАССИВЫ
        auto need_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_need_u32");
        auto bidx_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_base_idx_u32");
        auto bacn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_base_acn_u32");
        auto bpsn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_base_psn_u32");
        
        need_mp[safe_day].exchange(need);
        bidx_mp[safe_day].exchange(next_idx);
        bacn_mp[safe_day].exchange(next_acn);
        bpsn_mp[safe_day].exchange(next_psn);
        
        // Логирование как в sim_master
        if (need > 0u) {
            printf("  [SPAWN MGR Day %u] need=%u, next_idx=%u->%u, next_acn=%u->%u\\n",
                   day, need, next_idx, next_idx + need, next_acn, next_acn + need);
        }
        
        // Сдвигаем курсоры
        next_idx += need;
        next_acn += need;
        next_psn += need;
        
        FLAMEGPU->setVariable<unsigned int>("next_idx", next_idx);
        FLAMEGPU->setVariable<unsigned int>("next_acn", next_acn);
        FLAMEGPU->setVariable<unsigned int>("next_psn", next_psn);
        
        return flamegpu::ALIVE;
    }
    """).substitute(MAX_DAYS=str(MAX_DAYS))

    # RTC тикет - АДАПТИРОВАНО под orchestrator_v2
    RTC_SPAWN_TICKET = Template("""
    FLAMEGPU_AGENT_FUNCTION(rtc_spawn_mi17_v2, flamegpu::MessageNone, flamegpu::MessageNone) {
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        const unsigned int frames_total = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
        const unsigned int safe_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
        
        // Читаем из MacroProperty МАССИВОВ
        auto need_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_need_u32");
        auto bidx_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_base_idx_u32");
        auto bacn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_base_acn_u32");
        
        const unsigned int need = need_mp[safe_day];
        const unsigned int base_idx = bidx_mp[safe_day];
        const unsigned int base_acn = bacn_mp[safe_day];
        
        const unsigned int ticket = FLAMEGPU->getVariable<unsigned int>("ticket");
        if (ticket >= need) return flamegpu::ALIVE;
        
        const unsigned int idx = base_idx + ticket;
        if (idx >= frames_total) return flamegpu::ALIVE;
        
        const unsigned int acn = base_acn + ticket;
        
        // ПОРЯДОК КАК В sim_master, АДАПТИРОВАНО под orchestrator_v2:
        
        // 1. Идентификаторы (БЕЗ psn, ac_type_mask)
        FLAMEGPU->agent_out.setVariable<unsigned int>("idx", idx);
        FLAMEGPU->agent_out.setVariable<unsigned int>("aircraft_number", acn);
        // Читаем partseqno и group_by из Environment
        const unsigned int spawn_psn = FLAMEGPU->environment.getProperty<unsigned int>("spawn_partseqno_mi17");
        const unsigned int spawn_gb = FLAMEGPU->environment.getProperty<unsigned int>("spawn_group_by_mi17");
        
        FLAMEGPU->agent_out.setVariable<unsigned int>("partseqno_i", spawn_psn);
        FLAMEGPU->agent_out.setVariable<unsigned int>("group_by", spawn_gb);
        
        // Детальное логирование создания агентов
        printf("  [SPAWN Day %u] Creating AC %u (idx %u), state=serviceable, intent=2 (wants operations)\\n",
               day, acn, idx);
        
        if (ticket == 0u) {
            printf("  [SPAWN Day %u] Creating %u agents Mi-17: idx %u-%u, acn %u-%u\\n",
                   day, need, base_idx, base_idx + need - 1u, base_acn, base_acn + need - 1u);
        }
        
        // 2. Времена из Environment constants
        FLAMEGPU->agent_out.setVariable<unsigned int>("repair_time", 
            FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const"));
        FLAMEGPU->agent_out.setVariable<unsigned int>("assembly_time",
            FLAMEGPU->environment.getProperty<unsigned int>("mi17_assembly_time_const"));
        FLAMEGPU->agent_out.setVariable<unsigned int>("partout_time",
            FLAMEGPU->environment.getProperty<unsigned int>("mi17_partout_time_const"));
        
        // 3. Дата производства = version_date + day
        const unsigned int version_date = FLAMEGPU->environment.getProperty<unsigned int>("version_date");
        const unsigned int mfg_date = version_date + day;
        FLAMEGPU->agent_out.setVariable<unsigned int>("mfg_date", mfg_date);
        
        // 4. Наработки (БЕЗ status_id, intent_flag; ДОБАВЛЕНО cso)
        FLAMEGPU->agent_out.setVariable<unsigned int>("sne", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("ppr", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("cso", 0u);  // +НОВАЯ
        FLAMEGPU->agent_out.setVariable<unsigned int>("repair_days", 0u);
        
        // 5. Intent (ИЗМЕНЕНО: intent_flag → intent_state)
        FLAMEGPU->agent_out.setVariable<unsigned int>("intent_state", 2u);       // operations
        
        // 6. Нормативы из Environment constants
        FLAMEGPU->agent_out.setVariable<unsigned int>("ll", 
            FLAMEGPU->environment.getProperty<unsigned int>("mi17_ll_const"));
        FLAMEGPU->agent_out.setVariable<unsigned int>("oh",
            FLAMEGPU->environment.getProperty<unsigned int>("mi17_oh_const"));
        FLAMEGPU->agent_out.setVariable<unsigned int>("br",
            FLAMEGPU->environment.getProperty<unsigned int>("mi17_br_const"));
        
        // 7. MP5 переменные
        FLAMEGPU->agent_out.setVariable<unsigned int>("daily_today_u32", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("daily_next_u32", 0u);
        
        // 8. Триггеры
        FLAMEGPU->agent_out.setVariable<unsigned int>("active_trigger", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("assembly_trigger", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("partout_trigger", 0u);
        
        // 9. S6 счётчики
        FLAMEGPU->agent_out.setVariable<unsigned int>("s6_days", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("s6_started", 0u);
        
        // ops_ticket НЕ устанавливаем (создаётся условно в base_model)
        
        return flamegpu::ALIVE;
    }
    """).substitute(MAX_DAYS=str(MAX_DAYS))

    fn_mgr = spawn_mgr.newRTCFunction("rtc_spawn_mgr_v2", RTC_SPAWN_MGR)
    fn_mgr.setInitialState("default")
    fn_mgr.setEndState("default")

    fn_ticket = spawn_ticket.newRTCFunction("rtc_spawn_mi17_v2", RTC_SPAWN_TICKET)
    # Новорожденные в serviceable (нейтральное состояние, не задействовано в других RTC)
    fn_ticket.setAgentOutput(agent, "serviceable")
    fn_ticket.setInitialState("default")
    fn_ticket.setEndState("default")

    # Слои: менеджер, потом тикеты
    layer_mgr = model.newLayer("spawn_mgr_v2")
    layer_mgr.addAgentFunction(fn_mgr)
    
    layer_spawn = model.newLayer("spawn_ticket_v2")
    layer_spawn.addAgentFunction(fn_ticket)

    print("  RTC spawn_v2 зарегистрирован (адаптация для orchestrator_v2, state=serviceable)")
    
    return spawn_mgr, spawn_ticket


def initialize_spawn_population(simulation: 'fg.CUDASimulation', model: 'fg.ModelDescription', env_data: dict):
    """
    Инициализация spawn популяции
    """
    
    # Менеджер (1 экземпляр)
    mgr_pop = fg.AgentVector(model.getAgent("spawn_mgr"))
    mgr_pop.push_back()
    
    # ПРАВИЛЬНО: first_reserved_idx (279), НЕ first_future_idx (286)!
    first_reserved_idx = env_data.get('first_reserved_idx', 279)
    base_acn_spawn = 100000  # ХАРДКОД - начинаем с 100000 ВСЕГДА
    # Читаем partseqno из env_data (единая точка определения)
    base_psn_spawn = env_data.get('spawn_partseqno_mi17', 70386)
    
    mgr_pop[0].setVariableUInt("next_idx", first_reserved_idx)
    mgr_pop[0].setVariableUInt("next_acn", base_acn_spawn)
    mgr_pop[0].setVariableUInt("next_psn", base_psn_spawn)
    
    simulation.setPopulationData(mgr_pop, "default")
    
    # Тикеты (фиксированно 16 как в sim_master)
    ticket_count = 16  # ХАРДКОД как в sim_master
    ticket_pop = fg.AgentVector(model.getAgent("spawn_ticket"))
    
    for i in range(ticket_count):
        ticket_pop.push_back()
        ticket_pop[i].setVariableUInt("ticket", i)
    
    simulation.setPopulationData(ticket_pop, "default")
    
    print(f"  Spawn_v2 популяция: mgr next_idx={first_reserved_idx}, тикетов={ticket_count}")

