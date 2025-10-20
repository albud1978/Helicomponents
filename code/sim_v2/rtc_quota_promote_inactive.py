"""
RTC модуль для промоута inactive → operations (приоритет 3)
Каскадная архитектура: использует mi8_approve/mi17_approve для подсчёта used
ВАЖНО: Может остаться deficit > 0 (допустимо по бизнес-логике)
"""

import pyflamegpu as fg

def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции для промоута inactive → operations (приоритет 3)"""
    print("  Регистрация модуля квотирования: промоут inactive (приоритет 3)")
    
    # Получаем MAX_FRAMES из модели
    max_frames = model.Environment().getPropertyUInt("frames_total")
    
    # ═══════════════════════════════════════════════════════════════
    # ПРОМОУТ ПРИОРИТЕТ 3: inactive → operations
    # ═══════════════════════════════════════════════════════════════
    # Логика:
    # 1. Считаем used (демоут + serviceable + reserve)
    # 2. Считаем Target (из mp4_ops_counter на D+1)
    # 3. deficit = Target - used
    # 4. ЕСЛИ deficit <= 0: Early exit (все агенты)
    # 5. ИНАЧЕ: Промоут deficit агентов С УСЛОВИЕМ → intent=2 + mi*_approve=1
    # 6. ⚠️ Может остаться deficit > 0 (допустимо!)
    # ═══════════════════════════════════════════════════════════════
    
    RTC_QUOTA_PROMOTE_INACTIVE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_quota_promote_inactive, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Фильтр: только агенты с intent=1 (замороженные в inactive)
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    if (intent != 1u) {{
        return flamegpu::ALIVE;
    }}
    
    // ✅ ВАЖНО: Только агенты, у которых пришло время (step_day >= repair_time)
    // Остальные остаются в frozen состоянии (intent=1) в состоянии inactive
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    if (step_day < repair_time) {{
        // Ещё не готовы - пропускаем
        return flamegpu::ALIVE;
    }}
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = ((day + 1u) < days_total ? (day + 1u) : (days_total > 0u ? days_total - 1u : 0u));
    
    // ═══════════════════════════════════════════════════════════
    // ШАГ 1: Подсчёт curr + учёт одобренных из P1 и P2
    // ═══════════════════════════════════════════════════════════
    unsigned int curr = 0u;
    unsigned int target = 0u;
    unsigned int used = 0u;  // Сколько уже одобрено из P1 + P2
    
    if (group_by == 1u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_ops_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (ops_count[i] == 1u) ++curr;
        }}
        
        // Считаем сколько уже одобрено из serviceable (P1)
        auto approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve_s3");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (approve_s3[i] == 1u) ++used;
        }}
        
        // Считаем сколько одобрено из reserve (P2)
        auto approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve_s5");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (approve_s5[i] == 1u) ++used;
        }}
        
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", safe_day);
        
    }} else if (group_by == 2u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_ops_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (ops_count[i] == 1u) ++curr;
        }}
        
        auto approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve_s3");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (approve_s3[i] == 1u) ++used;
        }}
        
        auto approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve_s5");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (approve_s5[i] == 1u) ++used;
        }}
        
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);
    }} else {{
        return flamegpu::ALIVE;
    }}
    
    // Диагностика на ключевых днях (УБРАНО — избыточное логирование)
    
    // ═══════════════════════════════════════════════════════════
    // ШАГ 2: Расчёт дефицита (сколько не хватает до target с учётом P1+P2)
    // ═══════════════════════════════════════════════════════════
    const int deficit = (int)target - (int)curr - (int)used;
    if (deficit <= 0) {{
        // Уже достаточно (curr + P1+P2 одобрено) или target=0 → выход
        return flamegpu::ALIVE;
    }}
    
    // ═══════════════════════════════════════════════════════════
    // ШАГ 3: Промоут готовых агентов (каскадное квотирование P3)
    // Поднимаем ровно deficit агентов (может остаться нераспределённо - допустимо!)
    // ═══════════════════════════════════════════════════════════
    const unsigned int K = (unsigned int)deficit;  // ✅ Каскадная логика
    
    // Ранжирование: youngest first среди РЕАЛЬНЫХ агентов в inactive (которые ГОТОВЫ)
    const unsigned int my_mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", idx);
    unsigned int rank = 0u;
    
    // ✅ ВАЖНО: Фильтруем по inactive state используя буфер (state=1, intent=1, но готовы: step_day >= repair_time)
    if (group_by == 1u) {{
        auto inactive_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_inactive_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (i == idx) continue;
            if (inactive_count[i] != 1u) continue;  // ✅ Только агенты в inactive
            
            const unsigned int other_mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", i);
            // Youngest first: rank растёт если other МОЛОЖЕ меня
            if (other_mfg > my_mfg || (other_mfg == my_mfg && i < idx)) {{
                ++rank;
            }}
        }}
    }} else if (group_by == 2u) {{
        auto inactive_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_inactive_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (i == idx) continue;
            if (inactive_count[i] != 1u) continue;  // ✅ Только агенты в inactive
            
            const unsigned int other_mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", i);
            // Youngest first: rank растёт если other МОЛОЖЕ меня
            if (other_mfg > my_mfg || (other_mfg == my_mfg && i < idx)) {{
                ++rank;
            }}
        }}
    }}
    
    if (rank < K) {{
        // Я в числе K первых → промоут, меняю intent=1 на intent=2
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);  // Изменяем: 1→2 (одобрены на операции)
        
        // Записываем в ОТДЕЛЬНЫЙ буфер для inactive (избегаем race condition)
        if (group_by == 1u) {{
            auto approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve_s1");
            approve_s1[idx].exchange(1u);  // ✅ Помечаем в отдельном буфере
        }} else if (group_by == 2u) {{
            auto approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve_s1");
            approve_s1[idx].exchange(1u);
        }}
        
        // Диагностика
        const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        if (aircraft_number >= 100000u || day == 226u || day == 227u || day == 228u || day == 229u || day == 230u) {{
            printf("  [PROMOTE P3→2 Day %u] AC %u (idx %u): rank=%u/%u inactive->operations\\n", 
                   day, aircraft_number, idx, rank, K);
        }}
    }} else {{
        // Не вошёл в квоту → intent остаётся 1 (замороженное, ждёт следующего дня)
        // НЕ меняем intent! Агент остаётся в inactive на следующий день
        
        const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        if (aircraft_number >= 100000u || day == 226u || day == 227u || day == 228u || day == 229u || day == 230u) {{
            printf("  [PROMOTE P3 REJECT Day %u] AC %u (idx %u): rank=%u >= K=%u, staying in inactive\\n", 
                   day, aircraft_number, idx, rank, K);
        }}
    }}
    
    return flamegpu::ALIVE;
}}
"""
    
    # ═══════════════════════════════════════════════════════════
    # Регистрация слоя
    # ═══════════════════════════════════════════════════════════
    layer_promote = model.newLayer("quota_promote_inactive")
    rtc_func_promote = agent.newRTCFunction("rtc_quota_promote_inactive", RTC_QUOTA_PROMOTE_INACTIVE)
    rtc_func_promote.setAllowAgentDeath(False)
    rtc_func_promote.setInitialState("inactive")  # ✅ Фильтр по state
    rtc_func_promote.setEndState("inactive")      # Остаются в inactive (intent изменён)
    layer_promote.addAgentFunction(rtc_func_promote)
    
    print("  RTC модуль quota_promote_inactive зарегистрирован (1 слой, приоритет 3)")
