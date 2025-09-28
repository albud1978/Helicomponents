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
    // Только агенты с intent=2 (остаются в operations) участвуют в квотировании
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    if (intent != 2u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    // Используем существующие mi*_approve как временную маску
    // 0 - не кандидат, 1 - кандидат, 2 - выбран на демоут
    if (group_by == 1u) {{
        auto mask = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve");
        mask[idx].exchange(1u);
    }} else if (group_by == 2u) {{
        auto mask = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve");
        mask[idx].exchange(1u);
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

    # Фаза S3: Менеджер квот (только для idx=0)
    RTC_QUOTA_MANAGER = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_quota_manager, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Только агент с idx=0 выполняет роль менеджера
    if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
    
    // Обрабатываем Mi-8 (group_by=1)
    {{
        auto mask = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve");
        // Доступ к property array по индексам
        
        // Получаем целевое количество из MP4
        unsigned int target = 0u;
        try {{
            target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", safe_day);
        }} catch (...) {{
            // Если нет данных квот, пропускаем
        }}
        
        // Считаем текущее количество кандидатов
        unsigned int curr = 0u;
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (mask[i] == 1u) {{
                ++curr;
            }}
        }}
        
        if (curr > target) {{
            // Есть избыток, нужно убрать K агентов
            unsigned int K = curr - target;
            
            // Выбираем K старейших по mfg_date (детерминированно)
            for (unsigned int picked = 0u; picked < K; ++picked) {{
                unsigned int best_mfg_date = 0xFFFFFFFF;
                unsigned int best_idx = frames;
                
                // Линейный проход для поиска старейшего
                for (unsigned int i = 0u; i < frames; ++i) {{
                    if (mask[i] != 1u) continue; // Только среди кандидатов
                    
                    unsigned int current_mfg_date = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", i);
                    
                    if (current_mfg_date < best_mfg_date) {{
                        best_mfg_date = current_mfg_date;
                        best_idx = i;
                    }} else if (current_mfg_date == best_mfg_date && i < best_idx) {{
                        // Tie-breaker: меньший idx
                        best_idx = i;
                    }}
                }}
                
                if (best_idx < frames) {{
                    mask[best_idx].exchange(2u); // Помечаем на демоут
                }}
            }}
            
            // Очищаем непомеченных кандидатов
            for (unsigned int i = 0u; i < frames; ++i) {{
                if (mask[i] == 1u) {{
                    mask[i].exchange(0u);
                }}
            }}
        }} else {{
            // Нет избытка, очищаем всю маску
            for (unsigned int i = 0u; i < frames; ++i) {{
                if (mask[i] != 0u) {{
                    mask[i].exchange(0u);
                }}
            }}
        }}
    }}
    
    // Обрабатываем Mi-17 (group_by=2) - аналогично
    {{
        auto mask = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve");
        // Доступ к property array по индексам
        
        unsigned int target = 0u;
        try {{
            target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);
        }} catch (...) {{}}
        
        unsigned int curr = 0u;
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (mask[i] == 1u) {{
                ++curr;
            }}
        }}
        
        if (curr > target) {{
            unsigned int K = curr - target;
            
            for (unsigned int picked = 0u; picked < K; ++picked) {{
                unsigned int best_mfg_date = 0xFFFFFFFF;
                unsigned int best_idx = frames;
                
                for (unsigned int i = 0u; i < frames; ++i) {{
                    if (mask[i] != 1u) continue;
                    
                    unsigned int current_mfg_date = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", i);
                    
                    if (current_mfg_date < best_mfg_date) {{
                        best_mfg_date = current_mfg_date;
                        best_idx = i;
                    }} else if (current_mfg_date == best_mfg_date && i < best_idx) {{
                        best_idx = i;
                    }}
                }}
                
                if (best_idx < frames) {{
                    mask[best_idx].exchange(2u);
                }}
            }}
            
            for (unsigned int i = 0u; i < frames; ++i) {{
                if (mask[i] == 1u) {{
                    mask[i].exchange(0u);
                }}
            }}
        }} else {{
            for (unsigned int i = 0u; i < frames; ++i) {{
                if (mask[i] != 0u) {{
                    mask[i].exchange(0u);
                }}
            }}
        }}
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
    
    // Проверяем, что агент действительно в operations (intent от предыдущего шага)
    // Это временное решение пока нет полноценной state-based архитектуры
    if (current_intent != 2u && current_intent != 3u && current_intent != 4u && current_intent != 6u) {{
        return flamegpu::ALIVE; // Не в operations
    }}
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    unsigned int decision = 0u;
    
    // Читаем решение из соответствующей маски
    if (group_by == 1u) {{
        auto mask = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve");
        decision = mask[idx];
        
        if (decision == 2u) {{
            // Помечен на демоут -> переходим в serviceable
            FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
        }} else if (current_intent == 2u) {{
            // Остаемся в operations (подтверждаем intent)
            FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
        }}
        // Иначе сохраняем текущий intent (4 или 6 для технологических переходов)
        
        // Очищаем свой флаг
        if (decision != 0u) {{
            mask[idx].exchange(0u);
        }}
    }} else if (group_by == 2u) {{
        auto mask = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve");
        decision = mask[idx];
        
        if (decision == 2u) {{
            FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
        }} else if (current_intent == 2u) {{
            FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
        }}
        
        if (decision != 0u) {{
            mask[idx].exchange(0u);
        }}
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
    layer_manager.addAgentFunction(rtc_func_manager)
    
    # Слой S4: Массовое изменение intent
    layer_bulk = model.newLayer("quota_set_intents_bulk")
    rtc_func_bulk = agent.newRTCFunction("rtc_quota_set_intents_bulk", RTC_SET_INTENTS_BULK)
    rtc_func_bulk.setAllowAgentDeath(False)
    layer_bulk.addAgentFunction(rtc_func_bulk)
    
    print("  RTC модуль quota_ops_excess зарегистрирован")