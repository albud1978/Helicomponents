"""
RTC модуль для обработки избытка операций (2->3 демоут)
Реализует трёхфазную архитектуру без race conditions
"""

import pyflamegpu as fg

def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции для квотирования операций"""
    print("  Регистрация модуля квотирования операций (quota_ops_excess)")
    
    # Получаем MAX_FRAMES из модели
    max_frames = model.Environment().getPropertyUInt("frames_total")
    
    # Фаза S2: Пометка кандидатов (агенты в operations с intent=2)
    # Используем mi8_approve/mi17_approve как временную маску
    RTC_MARK_CANDIDATES = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_quota_mark_candidates, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Все агенты в operations участвуют в квотировании, кроме тех, кто уже получил intent 4 или 6
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int day = FLAMEGPU->getStepCounter();
    
    // Логирование для отладки на день 181
    if (day == 181u && idx == 0u) {{
        printf("  [QUOTA MARK Day %u] Function called for idx=0, intent=%u\\n", day, intent);
    }}
    
    // Исключаем только тех, кто уже помечен на технологические переходы
    if (intent == 4u || intent == 6u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    // Используем существующие mi*_approve как временную маску
    // 0 - не кандидат, 1 - кандидат, 2 - выбран на демоут
    if (group_by == 1u) {{
        auto mask = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve");
        mask[idx].exchange(1u);
        if (day == 181u && idx < 5u) {{
            printf("  [QUOTA MARK Day %u] Mi-8 candidate marked: idx=%u\\n", day, idx);
        }}
    }} else if (group_by == 2u) {{
        auto mask = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve");
        mask[idx].exchange(1u);
        if (day == 181u && idx < 5u) {{
            printf("  [QUOTA MARK Day %u] Mi-17 candidate marked: idx=%u\\n", day, idx);
        }}
    }}
    
    return flamegpu::ALIVE;
}}
"""

    # Условие: только агенты в состоянии operations
    RTC_COND_OPERATIONS = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_state_operations) {
    // Условие проверяется для всех агентов
    return true; // Фильтрация происходит внутри функции
}
"""

    # Фаза S3: Применение квоты (пер-агентно, детерминированно, без idx==0)
    RTC_QUOTA_MANAGER = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_quota_manager, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = ((day + 1u) < days_total ? (day + 1u) : (days_total > 0u ? days_total - 1u : 0u));

    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int current_intent = FLAMEGPU->getVariable<unsigned int>("intent_state");

    if (group_by == 1u) {{
        auto mask = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve");
        unsigned int target = 0u;
        try {{ target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", safe_day); }} catch (...) {{}}

        // Подсчитываем число кандидатов
        unsigned int curr = 0u;
        for (unsigned int i = 0u; i < frames; ++i) {{ if (mask[i] == 1u) ++curr; }}

        // Диагностика на ключевых днях
        if ((day == 180u || day == 181u || day == 182u) && idx == 0u) {{
            const unsigned int Kdbg = (curr > target ? (curr - target) : 0u);
            printf("  [QUOTA DECIDE Day %u] Mi-8: safe_day=%u, Curr=%u, Target=%u, K=%u\\n", day, safe_day, curr, target, Kdbg);
        }}

        if (curr > target && mask[idx] == 1u) {{
            const unsigned int K = curr - target;
            const unsigned int my_mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", idx);
            unsigned int rank = 0u;
            for (unsigned int i = 0u; i < frames; ++i) {{
                if (i == idx) continue;
                if (mask[i] == 1u) {{
                    const unsigned int other_mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", i);
                    if (other_mfg < my_mfg || (other_mfg == my_mfg && i < idx)) ++rank;
                }}
            }}
            if (rank < K) {{
                // Я в числе K самых старых → демоут intent=3
                FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
                if (day == 180u || day == 181u || day == 182u) {{
                    printf("  [QUOTA DEMOTE Day %u] Mi-8 idx=%u rank=%u of K=%u\\n", day, idx, rank, K);
                }}
            }} else if (current_intent == 2u) {{
                // Остаюсь в operations
                FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
            }}
        }} else if (current_intent == 2u) {{
            // Снижения нет или не кандидат → подтверждаем intent 2
            FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
            if ((day == 180u || day == 181u || day == 182u) && idx == 0u && curr <= target) {{
                printf("  [QUOTA DECIDE Day %u] Mi-8: no demotion (Curr=%u, Target=%u)\\n", day, curr, target);
            }}
        }}
        // Маску не очищаем в этом слое, чтобы избежать гонок при чтении другими агентами
    }} else if (group_by == 2u) {{
        auto mask = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve");
        unsigned int target = 0u;
        try {{ target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day); }} catch (...) {{}}

        unsigned int curr = 0u;
        for (unsigned int i = 0u; i < frames; ++i) {{ if (mask[i] == 1u) ++curr; }}

        if ((day == 180u || day == 181u || day == 182u) && idx == 0u) {{
            const unsigned int Kdbg = (curr > target ? (curr - target) : 0u);
            printf("  [QUOTA DECIDE Day %u] Mi-17: safe_day=%u, Curr=%u, Target=%u, K=%u\\n", day, safe_day, curr, target, Kdbg);
        }}

        if (curr > target && mask[idx] == 1u) {{
            const unsigned int K = curr - target;
            const unsigned int my_mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", idx);
            unsigned int rank = 0u;
            for (unsigned int i = 0u; i < frames; ++i) {{
                if (i == idx) continue;
                if (mask[i] == 1u) {{
                    const unsigned int other_mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", i);
                    if (other_mfg < my_mfg || (other_mfg == my_mfg && i < idx)) ++rank;
                }}
            }}
            if (rank < K) {{
                FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
                if (day == 180u || day == 181u || day == 182u) {{
                    printf("  [QUOTA DEMOTE Day %u] Mi-17 idx=%u rank=%u of K=%u\\n", day, idx, rank, K);
                }}
            }} else if (current_intent == 2u) {{
                FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
            }}
        }} else if (current_intent == 2u) {{
            FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
            if ((day == 180u || day == 181u || day == 182u) && idx == 0u && curr <= target) {{
                printf("  [QUOTA DECIDE Day %u] Mi-17: no demotion (Curr=%u, Target=%u)\\n", day, curr, target);
            }}
        }}
        // Маску не очищаем в этом слое, чтобы избежать гонок при чтении другими агентами
    }}

    return flamegpu::ALIVE;
}}
"""

    # Фаза S4: Массовое изменение intent для агентов в operations
    RTC_SET_INTENTS_BULK = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_quota_set_intents_bulk, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Фильтруем только агенты в текущем состоянии operations
    // (в будущем это будет автоматически через state filter)
    const unsigned int current_intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int day = FLAMEGPU->getStepCounter();
    
    // Отладка для idx=0
    if (day == 181u && idx == 0u) {{
        printf("  [QUOTA BULK Day %u] Set intents called for idx=0, current_intent=%u\\n", day, current_intent);
    }}
    
    // Проверяем, что агент действительно в operations (intent от предыдущего шага)
    // Это временное решение пока нет полноценной state-based архитектуры
    if (current_intent != 2u && current_intent != 3u && current_intent != 4u && current_intent != 6u) {{
        return flamegpu::ALIVE; // Не в operations
    }}
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    unsigned int decision = 0u;
    
    // Читаем решение из соответствующей маски
    if (group_by == 1u) {{
        auto mask = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve");
        decision = mask[idx];
        
        if (decision == 2u) {{
            // Помечен на демоут -> переходим в serviceable
            FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
            const unsigned int day = FLAMEGPU->getStepCounter();
            const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
            printf("  [QUOTA Day %u] AC %u: Demoted to serviceable (Mi-8)\\n", day, aircraft_number);
        }} else if (current_intent == 2u) {{
            // Остаемся в operations (подтверждаем intent)
            FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
        }}
        // Иначе сохраняем текущий intent (4 или 6 для технологических переходов)
        
        // Очищаем свой флаг (безусловно)
        if (mask[idx] != 0u) {{ mask[idx].exchange(0u); }}
    }} else if (group_by == 2u) {{
        auto mask = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve");
        decision = mask[idx];
        
        if (decision == 2u) {{
            FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
            const unsigned int day = FLAMEGPU->getStepCounter();
            const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
            printf("  [QUOTA Day %u] AC %u: Demoted to serviceable (Mi-17)\\n", day, aircraft_number);
        }} else if (current_intent == 2u) {{
            FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
        }}
        
        // Очищаем свой флаг (безусловно)
        if (mask[idx] != 0u) {{ mask[idx].exchange(0u); }}
    }}
    
    return flamegpu::ALIVE;
}}
"""

    # Регистрация слоев
    
    # Слой S2: Пометка кандидатов
    layer_mark = model.newLayer("quota_mark_candidates")
    rtc_func_mark = agent.newRTCFunction("rtc_quota_mark_candidates", RTC_MARK_CANDIDATES)
    rtc_func_mark.setRTCFunctionCondition(RTC_COND_OPERATIONS)
    rtc_func_mark.setInitialState("operations")
    rtc_func_mark.setEndState("operations")
    layer_mark.addAgentFunction(rtc_func_mark)
    
    # Слой S3: Менеджер квот
    layer_manager = model.newLayer("quota_manager")
    rtc_func_manager = agent.newRTCFunction("rtc_quota_manager", RTC_QUOTA_MANAGER)
    rtc_func_manager.setAllowAgentDeath(False)
    # Менеджер выполняется для ВСЕХ агентов (проверка idx==0 внутри)
    # Не устанавливаем initial/end state - функция работает для всех
    layer_manager.addAgentFunction(rtc_func_manager)
    
    # Слой S4: Массовое изменение intent
    layer_bulk = model.newLayer("quota_set_intents_bulk")
    rtc_func_bulk = agent.newRTCFunction("rtc_quota_set_intents_bulk", RTC_SET_INTENTS_BULK)
    rtc_func_bulk.setAllowAgentDeath(False)
    # Выполняется для всех агентов (фильтрация внутри функции)
    layer_bulk.addAgentFunction(rtc_func_bulk)
    
    print("  RTC модуль quota_ops_excess зарегистрирован")
    print("  RTC модуль quota_ops_excess зарегистрирован")