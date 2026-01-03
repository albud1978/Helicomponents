#!/usr/bin/env python3
"""
RTC Spawn Dynamic для orchestrator_v2
Динамический spawn планеров для покрытия дефицита после P3 quota promotion

ОТЛИЧИЯ от rtc_spawn_v2 (детерминированный):
- Триггер: deficit > 0 после P3 (НЕ MP4 seed)
- Условие активации: day >= repair_time (аналогично P3)
- Состояние: operations (НЕМЕДЛЕННОЕ покрытие дефицита)
- Момент: Слой 7.5 (после P3, до state_manager)
- ACN: Общий диапазон 100000+, начинается с last свободного idx
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
import model_build

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")

from string import Template


def register_rtc(model: 'fg.ModelDescription', agent: 'fg.AgentDescription', env_data: dict):
    """
    Регистрация динамического spawn для orchestrator_v2
    
    Логика:
    1. spawn_dynamic_mgr читает deficit после P3 из MacroProperty
    2. Если deficit > 0 AND day >= repair_time → создаёт тикеты
    3. spawn_dynamic_ticket создаёт новых агентов СРАЗУ в operations (немедленное покрытие дефицита)
    """
    
    env = model.Environment()
    
    # Параметры динамического spawn из env_data
    first_dynamic_idx = env_data.get('first_dynamic_idx', 122)  # Начало динамического резерва
    repair_time_mi17 = env_data.get('mi17_repair_time_const', 180)  # Условие активации
    dynamic_reserve_mi17 = env_data.get('dynamic_reserve_mi17', 50)  # Максимальный резерв
    # ФИКСИРОВАННЫЕ размеры для RTC кэширования
    MAX_FRAMES = model_build.RTC_MAX_FRAMES
    MAX_DAYS = model_build.MAX_DAYS
    MP2_SIZE = MAX_FRAMES * (MAX_DAYS + 1)  # Размер MacroProperty для transition
    
    env.newPropertyUInt("first_dynamic_idx", first_dynamic_idx)
    env.newPropertyUInt("repair_time_mi17", repair_time_mi17)
    env.newPropertyUInt("dynamic_reserve_mi17", dynamic_reserve_mi17)
    
    # Агенты-утилиты динамического spawn
    spawn_dynamic_mgr = model.newAgent("spawn_dynamic_mgr")
    spawn_dynamic_mgr.newState("default")
    spawn_dynamic_mgr.newVariableUInt("next_idx", 0)  # Следующий свободный idx
    spawn_dynamic_mgr.newVariableUInt("next_acn", 0)  # Следующий свободный ACN
    spawn_dynamic_mgr.newVariableUInt("next_psn", 0)  # Следующий PSN
    spawn_dynamic_mgr.newVariableUInt("total_spawned", 0)  # Счётчик созданных агентов
    spawn_dynamic_mgr.newVariableUInt("exhausted_day", 0)  # День исчерпания резерва (0=не исчерпан)

    spawn_dynamic_ticket = model.newAgent("spawn_dynamic_ticket")
    spawn_dynamic_ticket.newState("default")
    spawn_dynamic_ticket.newVariableUInt("ticket", 0)

    # MacroProperty МАССИВЫ по дням
    env.newMacroPropertyUInt("spawn_dynamic_need_u32", MAX_DAYS)
    env.newMacroPropertyUInt("spawn_dynamic_base_idx_u32", MAX_DAYS)
    env.newMacroPropertyUInt("spawn_dynamic_base_acn_u32", MAX_DAYS)
    env.newMacroPropertyUInt("spawn_dynamic_base_psn_u32", MAX_DAYS)
    
    # MacroProperty для deficit (публикуется из quota_promote_inactive)
    # ВАЖНО: Это должно быть создано в rtc_quota_promote_inactive.py!
    # Здесь только читаем
    
    # RTC менеджер динамического spawn
    RTC_SPAWN_DYNAMIC_MGR = Template("""
    FLAMEGPU_AGENT_FUNCTION(rtc_spawn_dynamic_mgr, flamegpu::MessageNone, flamegpu::MessageNone) {
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        // ИСПРАВЛЕНИЕ: Разделяем индексы для чтения target и записи параметров
        // target_day = D+1 (читаем целевую квоту на завтра, как в P1/P2/P3)
        // write_day = D (пишем параметры спавна в текущий день, чтобы тикет их прочитал сегодня)
        const unsigned int target_day = ((day + 1u) < days_total ? (day + 1u) : (days_total > 0u ? days_total - 1u : 0u));
        const unsigned int write_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
        
        // Условие активации: day >= repair_time
        const unsigned int repair_time = FLAMEGPU->environment.getProperty<unsigned int>("repair_time_mi17");
        if (day < repair_time) {
            // Ещё не активирован
            return flamegpu::ALIVE;
        }
        
        // ВАЖНО: Используем КАСКАДНУЮ логику как в P1/P2/P3!
        // Считаем: deficit = target - curr - used
        // где used = одобренные в P1 + P2 + P3
        
        // Считаем текущее количество в operations
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_FRAMES}u>("mi17_ops_count");
        unsigned int curr = 0u;
        for (unsigned int i = 0u; i < ${MAX_FRAMES}u; ++i) {
            if (ops_count[i] == 1u) ++curr;
        }
        
        // Считаем одобренных в P1 (serviceable → operations)
        auto approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_FRAMES}u>("mi17_approve_s3");
        unsigned int used = 0u;
        for (unsigned int i = 0u; i < ${MAX_FRAMES}u; ++i) {
            if (approve_s3[i] == 1u) ++used;
        }
        
        // Считаем одобренных в P2 (reserve → operations)
        auto approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_FRAMES}u>("mi17_approve_s5");
        for (unsigned int i = 0u; i < ${MAX_FRAMES}u; ++i) {
            if (approve_s5[i] == 1u) ++used;
        }
        
        // Считаем одобренных в P3 (inactive → operations)
        auto approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_FRAMES}u>("mi17_approve_s1");
        for (unsigned int i = 0u; i < ${MAX_FRAMES}u; ++i) {
            if (approve_s1[i] == 1u) ++used;
        }
        
        // Считаем pending агентов из динамического spawn (ещё не появились в operations)
        // ВАЖНО: spawn_pending сбрасывается в count_ops в начале дня!
        auto spawn_pending = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_FRAMES}u>("mi17_spawn_pending");
        for (unsigned int i = 0u; i < ${MAX_FRAMES}u; ++i) {
            if (spawn_pending[i] == 1u) ++used;
        }
        
        // Читаем целевое значение из MP4 для D+1
        const unsigned int target_ops = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", target_day);
        
        // Дефицит = target - curr - used (каскадная логика + pending spawn)
        const int deficit_signed = static_cast<int>(target_ops) - static_cast<int>(curr) - static_cast<int>(used);
        
        // DEBUG для дней 824-826 (проблемные дни)
        if (day >= 824u && day <= 826u) {
            printf("[DEBUG Day %u SPAWN] target[%u]=%u, curr=%u, used=%u, deficit=%d\\n", 
                   day, target_day, target_ops, curr, used, deficit_signed);
            
            // Проверяем агента 100006 (idx=285)
            if (ops_count[285u] == 1u) {
                printf("[DEBUG Day %u] Agent idx=285 (100006) IS in ops_count!\\n", day);
            } else {
                printf("[DEBUG Day %u] Agent idx=285 (100006) NOT in ops_count!\\n", day);
            }
        }
        
        if (deficit_signed <= 0) {
            // Нет дефицита или избыток — ничего не делаем
            return flamegpu::ALIVE;
        }
        
        const unsigned int deficit = static_cast<unsigned int>(deficit_signed);
        
        // Курсоры
        unsigned int next_idx = FLAMEGPU->getVariable<unsigned int>("next_idx");
        unsigned int next_acn = FLAMEGPU->getVariable<unsigned int>("next_acn");
        unsigned int next_psn = FLAMEGPU->getVariable<unsigned int>("next_psn");
        unsigned int total_spawned = FLAMEGPU->getVariable<unsigned int>("total_spawned");
        unsigned int exhausted_day = FLAMEGPU->getVariable<unsigned int>("exhausted_day");
        
        // Инициализация в первый день активации
        if (day == repair_time) {
            const unsigned int first_dynamic_idx = FLAMEGPU->environment.getProperty<unsigned int>("first_dynamic_idx");
            if (next_idx < first_dynamic_idx) next_idx = first_dynamic_idx;
            
            // next_acn уже инициализирован в init_population с учётом детерминированного spawn
            // Ничего не делаем здесь
            
            // Читаем partseqno из Environment
            const unsigned int spawn_psn = FLAMEGPU->environment.getProperty<unsigned int>("spawn_partseqno_mi17");
            if (next_psn < spawn_psn) next_psn = spawn_psn;
        }
        
        // Проверка резерва
        const unsigned int dynamic_reserve = FLAMEGPU->environment.getProperty<unsigned int>("dynamic_reserve_mi17");
        const unsigned int available = (total_spawned < dynamic_reserve) ? (dynamic_reserve - total_spawned) : 0u;
        
        // Определяем, сколько можем создать
        unsigned int need = deficit;
        if (need > available) {
            // Исчерпание резерва!
            if (exhausted_day == 0u) {
                exhausted_day = day;  // Запоминаем день исчерпания
                printf("  [SPAWN DYNAMIC WARNING] Day %u: Резерв исчерпан! deficit=%u, available=%u\\n",
                       day, deficit, available);
            }
            need = available;  // Создаём только доступные
        }
        
        if (need == 0u) {
            // Нечего создавать
            return flamegpu::ALIVE;
        }
        
        // Публикуем в MacroProperty МАССИВЫ
        // ИСПРАВЛЕНИЕ: Пишем в write_day (текущий день), а не target_day (D+1)
        auto need_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_need_u32");
        auto bidx_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_idx_u32");
        auto bacn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_acn_u32");
        auto bpsn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_psn_u32");
        
        need_mp[write_day].exchange(need);
        bidx_mp[write_day].exchange(next_idx);
        bacn_mp[write_day].exchange(next_acn);
        bpsn_mp[write_day].exchange(next_psn);
        
        // Логирование
        printf("  [SPAWN DYNAMIC Day %u] deficit=%u, need=%u, next_idx=%u->%u, next_acn=%u->%u\\n",
               day, deficit, need, next_idx, next_idx + need, next_acn, next_acn + need);
        
        // Сдвигаем курсоры
        next_idx += need;
        next_acn += need;
        next_psn += need;
        total_spawned += need;
        
        FLAMEGPU->setVariable<unsigned int>("next_idx", next_idx);
        FLAMEGPU->setVariable<unsigned int>("next_acn", next_acn);
        FLAMEGPU->setVariable<unsigned int>("next_psn", next_psn);
        FLAMEGPU->setVariable<unsigned int>("total_spawned", total_spawned);
        FLAMEGPU->setVariable<unsigned int>("exhausted_day", exhausted_day);
        
        return flamegpu::ALIVE;
    }
    """).substitute(MAX_DAYS=MAX_DAYS, MAX_FRAMES=MAX_FRAMES)
    
    # RTC тикет динамического spawn
    RTC_SPAWN_DYNAMIC_TICKET = Template("""
    FLAMEGPU_AGENT_FUNCTION(rtc_spawn_dynamic_ticket, flamegpu::MessageNone, flamegpu::MessageNone) {
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        const unsigned int safe_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
        const unsigned int ticket = FLAMEGPU->getVariable<unsigned int>("ticket");
        
        // Читаем параметры из MacroProperty
        auto need_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_need_u32");
        auto bidx_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_idx_u32");
        auto bacn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_acn_u32");
        auto bpsn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_psn_u32");
        
        const unsigned int need = need_mp[safe_day];
        const unsigned int base_idx = bidx_mp[safe_day];
        const unsigned int base_acn = bacn_mp[safe_day];
        const unsigned int base_psn = bpsn_mp[safe_day];
        
        if (ticket >= need) {
            // Тикет вне диапазона
            return flamegpu::ALIVE;
        }
        
        // Создаём нового агента
        const unsigned int new_idx = base_idx + ticket;
        const unsigned int new_acn = base_acn + ticket;
        // new_psn не используется - partseqno_i одинаков для всех Mi-17 (base_psn)
        
        // Дата производства = version_date + day
        const unsigned int version_date = FLAMEGPU->environment.getProperty<unsigned int>("version_date");
        const unsigned int mfg_date = version_date + day;
        
        // Нормативы из Environment
        const unsigned int ll = FLAMEGPU->environment.getProperty<unsigned int>("mi17_ll_const");
        const unsigned int oh = FLAMEGPU->environment.getProperty<unsigned int>("mi17_oh_const");
        const unsigned int br = FLAMEGPU->environment.getProperty<unsigned int>("mi17_br_const");
        const unsigned int repair_time = FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
        const unsigned int partout_time = FLAMEGPU->environment.getProperty<unsigned int>("mi17_partout_time_const");
        const unsigned int assembly_time = FLAMEGPU->environment.getProperty<unsigned int>("mi17_assembly_time_const");
        
        // Начальная наработка из MP1 (как в детерминированном spawn)
        const unsigned int sne_new = FLAMEGPU->environment.getProperty<unsigned int>("mi17_sne_new_const");
        const unsigned int ppr_new = FLAMEGPU->environment.getProperty<unsigned int>("mi17_ppr_new_const");
        
        // Создаём агента через agent_out (как в rtc_spawn_v2)
        FLAMEGPU->agent_out.setVariable<unsigned int>("idx", new_idx);
        FLAMEGPU->agent_out.setVariable<unsigned int>("aircraft_number", new_acn);
        FLAMEGPU->agent_out.setVariable<unsigned int>("partseqno_i", base_psn);  // Тип ВС (partseqno индекс)
        FLAMEGPU->agent_out.setVariable<unsigned int>("group_by", 2u);  // Mi-17
        
        // Наработка
        FLAMEGPU->agent_out.setVariable<unsigned int>("sne", sne_new);
        FLAMEGPU->agent_out.setVariable<unsigned int>("ppr", ppr_new);
        FLAMEGPU->agent_out.setVariable<unsigned int>("cso", 0u);  // Для планеров cso=0
        
        // Нормативы
        FLAMEGPU->agent_out.setVariable<unsigned int>("ll", ll);
        FLAMEGPU->agent_out.setVariable<unsigned int>("oh", oh);
        FLAMEGPU->agent_out.setVariable<unsigned int>("br", br);
        
        // Ремонт
        FLAMEGPU->agent_out.setVariable<unsigned int>("repair_time", repair_time);
        FLAMEGPU->agent_out.setVariable<unsigned int>("repair_days", 0u);
        
        // Триггеры (для планеров не используются)
        FLAMEGPU->agent_out.setVariable<unsigned int>("assembly_trigger", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("active_trigger", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("partout_trigger", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("s4_days", 0u);
        
        // Квоты (создаём СРАЗУ в operations для немедленного покрытия дефицита)
        // ВАЖНО: intent=2 означает "хочу продолжать работать" → будет учтён в count_ops!
        FLAMEGPU->agent_out.setVariable<unsigned int>("intent_state", 2u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("mfg_date", mfg_date);
        
        // Устанавливаем флаг pending для этого агента (будет сброшен когда появится в operations)
        auto spawn_pending = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_FRAMES}u>("mi17_spawn_pending");
        spawn_pending[new_idx].exchange(1u);
        
        // Transitions (все 0)
        FLAMEGPU->agent_out.setVariable<unsigned int>("transition_0_to_2", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("transition_0_to_3", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("transition_1_to_2", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("transition_1_to_4", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("transition_2_to_3", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("transition_2_to_4", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("transition_2_to_6", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("transition_3_to_2", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("transition_4_to_2", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("transition_4_to_5", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("transition_5_to_2", 0u);
        
        // Запись флага spawn в MacroProperty (динамический spawn → operations)
        const unsigned int pos = day * ${MAX_FRAMES}u + new_idx;
        auto mp2_transition_0_to_2 = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MP2_SIZE}u>("mp2_transition_0_to_2");
        mp2_transition_0_to_2[pos].exchange(1u);
        
        // BI counter
        FLAMEGPU->agent_out.setVariable<unsigned int>("bi_counter", 1u);
        
        // ОТЛАДКА: Детальное логирование созданного агента
        if (day <= 850u) {
            printf("  [SPAWN DYNAMIC TICKET Day %u #%u] СОЗДАН АГЕНТ:\\n", day, ticket);
            printf("    idx=%u, acn=%u, psn=%u, group_by=%u\\n", new_idx, new_acn, base_psn, 2u);
            printf("    sne=%u, ppr=%u, cso=%u\\n", sne_new, ppr_new, 0u);
            printf("    ll=%u, oh=%u, br=%u\\n", ll, oh, br);
            printf("    intent_state=%u, mfg_date=%u\\n", 2u, mfg_date);
            printf("    repair_time=%u, repair_days=%u\\n", repair_time, 0u);
            printf("    assembly_trigger=%u, active_trigger=%u, partout_trigger=%u\\n", 0u, 0u, 0u);
            printf("    bi_counter=%u\\n", 1u);
        }
        
        return flamegpu::ALIVE;
    }
    """).substitute(MAX_DAYS=MAX_DAYS, MAX_FRAMES=MAX_FRAMES, MP2_SIZE=MP2_SIZE)
    
    # Регистрация функций
    spawn_dynamic_mgr_fn = spawn_dynamic_mgr.newRTCFunction("rtc_spawn_dynamic_mgr", RTC_SPAWN_DYNAMIC_MGR)
    spawn_dynamic_mgr_fn.setInitialState("default")
    spawn_dynamic_mgr_fn.setEndState("default")
    
    spawn_dynamic_ticket_fn = spawn_dynamic_ticket.newRTCFunction("rtc_spawn_dynamic_ticket", RTC_SPAWN_DYNAMIC_TICKET)
    spawn_dynamic_ticket_fn.setAgentOutput(agent, "operations")  # Создаём СРАЗУ в operations для немедленного покрытия дефицита
    spawn_dynamic_ticket_fn.setInitialState("default")
    spawn_dynamic_ticket_fn.setEndState("default")
    
    # Создаём слои (слой 7.5: после P3, перед state_manager)
    layer_mgr = model.newLayer("spawn_dynamic_mgr")
    layer_mgr.addAgentFunction(spawn_dynamic_mgr_fn)
    
    layer_ticket = model.newLayer("spawn_dynamic_ticket")
    layer_ticket.addAgentFunction(spawn_dynamic_ticket_fn)
    
    # Добавление в Layer (будет вызвано из orchestrator_v2.py)
    return {
        'mgr_fn': spawn_dynamic_mgr_fn,
        'ticket_fn': spawn_dynamic_ticket_fn,
        'mgr_agent': spawn_dynamic_mgr,
        'ticket_agent': spawn_dynamic_ticket,
    }


def init_population(simulation: 'fg.CUDASimulation', model: 'fg.ModelDescription', env_data: dict):
    """
    Инициализация популяции утилит динамического spawn
    
    Создаём:
    - 1 spawn_dynamic_mgr (менеджер)
    - N spawn_dynamic_ticket (тикетов, по максимальному deficit)
    """
    first_dynamic_idx = env_data.get('first_dynamic_idx', 122)
    dynamic_reserve_mi17 = env_data.get('dynamic_reserve_mi17', 50)
    base_acn_spawn = env_data.get('base_acn_spawn', 100000)  # Читаем из env_data
    
    # Менеджер (1 экземпляр)
    mgr_pop = fg.AgentVector(model.getAgent("spawn_dynamic_mgr"))
    mgr_pop.push_back()
    mgr_pop[0].setVariableUInt("next_idx", first_dynamic_idx)
    # ВАЖНО: base_acn_spawn уже учитывает все существующие и будущие ACN (включая детерминированный spawn)!
    # Поэтому используем его напрямую
    mgr_pop[0].setVariableUInt("next_acn", base_acn_spawn)
    mgr_pop[0].setVariableUInt("next_psn", env_data.get('spawn_partseqno_mi17', 70386))
    mgr_pop[0].setVariableUInt("total_spawned", 0)
    mgr_pop[0].setVariableUInt("exhausted_day", 0)
    simulation.setPopulationData(mgr_pop, "default")
    
    # Тикеты (по количеству резерва)
    ticket_pop = fg.AgentVector(model.getAgent("spawn_dynamic_ticket"))
    for i in range(dynamic_reserve_mi17):
        ticket_pop.push_back()
        ticket_pop[i].setVariableUInt("ticket", i)
    simulation.setPopulationData(ticket_pop, "default")
    
    print(f"  Spawn_dynamic популяция: mgr next_idx={first_dynamic_idx}, тикетов={dynamic_reserve_mi17}")

