"""
RTC модуль для промоута serviceable → operations (приоритет 1)
Каскадная архитектура: использует mi8_approve/mi17_approve для подсчёта used
"""

import pyflamegpu as fg

def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции для промоута serviceable → operations (приоритет 1)"""
    print("  Регистрация модуля квотирования: промоут serviceable (приоритет 1)")
    
    # Получаем MAX_FRAMES из модели
    max_frames = model.Environment().getPropertyUInt("frames_total")
    
    # ═══════════════════════════════════════════════════════════════
    # ПРОМОУТ ПРИОРИТЕТ 1: serviceable → operations
    # ═══════════════════════════════════════════════════════════════
    # Логика:
    # 1. Считаем used (сколько уже одобрено в демоуте)
    # 2. Считаем Target (из mp4_ops_counter на D+1)
    # 3. deficit = Target - used
    # 4. ЕСЛИ deficit <= 0: Early exit (все агенты)
    # 5. ИНАЧЕ: Промоут deficit агентов → intent=2 + mi*_approve=1
    # ═══════════════════════════════════════════════════════════════
    
    RTC_QUOTA_PROMOTE_SERVICEABLE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_quota_promote_serviceable, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Фильтр: только агенты с intent=3 (в холдинге, ждут решения)
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    if (intent != 3u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = ((day + 1u) < days_total ? (day + 1u) : (days_total > 0u ? days_total - 1u : 0u));
    
    // ═══════════════════════════════════════════════════════════
    // ШАГ 1: Подсчёт curr (реальное количество в operations)
    // ═══════════════════════════════════════════════════════════
    unsigned int curr = 0u;
    unsigned int target = 0u;
    
    if (group_by == 1u) {
        // Mi-8: считаем текущих в operations
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_ops_count");
        for (unsigned int i = 0u; i < frames; ++i) {
            if (ops_count[i] == 1u) ++curr;
        }
        
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", safe_day);
    } else if (group_by == 2u) {
        // Mi-17: аналогично
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_ops_count");
        for (unsigned int i = 0u; i < frames; ++i) {
            if (ops_count[i] == 1u) ++curr;
        }
        
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);
    } else {
        return flamegpu::ALIVE;  // Неизвестный group_by
    }
    
    // ═══════════════════════════════════════════════════════════
    // ШАГ 2: Расчёт дефицита (сколько не хватает до target)
    // ═══════════════════════════════════════════════════════════
    const int deficit = (int)target - (int)curr;
    if (deficit <= 0) {
        // Уже достаточно агентов в operations или target=0 → выход
        return flamegpu::ALIVE;  // ✅ Оптимизация
    }
    
    // ═══════════════════════════════════════════════════════════
    // ШАГ 3: Промоут готовых агентов (каскадное квотирование P1)
    // Поднимаем ровно deficit агентов (не больше, чтобы остаток шёл в P2)
    // ═══════════════════════════════════════════════════════════
    const unsigned int K = (unsigned int)deficit;  // ✅ Каскадная логика
    
    // Ранжирование: youngest first среди РЕАЛЬНЫХ агентов в serviceable
    const unsigned int my_mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", idx);
    unsigned int rank = 0u;
    
    // Используем svc_count буфер для фильтрации
    if (group_by == 1u) {{
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_svc_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (i == idx) continue;
            if (svc_count[i] != 1u) continue;  // ✅ Только агенты в serviceable
            
            const unsigned int other_mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", i);
            // Youngest first: rank растёт если other МОЛОЖЕ меня
            if (other_mfg > my_mfg || (other_mfg == my_mfg && i < idx)) {{
                ++rank;
            }}
        }}
    }} else if (group_by == 2u) {{
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_svc_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (i == idx) continue;
            if (svc_count[i] != 1u) continue;  // ✅ Только агенты в serviceable
            
            const unsigned int other_mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", i);
            // Youngest first: rank растёт если other МОЛОЖЕ меня
            if (other_mfg > my_mfg || (other_mfg == my_mfg && i < idx)) {{
                ++rank;
            }}
        }}
    }}
    
    if (rank < K) {{
        // Я в числе K первых → промоут, меняю intent=5 на intent=2
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);  // Изменяем: 5→2 (одобрены на операции)
        
        // Записываем в ОТДЕЛЬНЫЙ буфер для serviceable (избегаем race condition)
        if (group_by == 1u) {{
            auto approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve_s3");
            approve_s3[idx].exchange(1u);  // ✅ Помечаем в отдельном буфере
        }} else if (group_by == 2u) {{
            auto approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve_s3");
            approve_s3[idx].exchange(1u);
        }}
        

        
    }} else {{
        // Не вошёл в квоту → intent остаётся 3 (холдинг, ждёт следующего дня)
        // НЕ меняем intent! Агент остаётся в serviceable на следующий день
        

    }}
    
    return flamegpu::ALIVE;
}}
"""
    
    # ═══════════════════════════════════════════════════════════
    # Регистрация слоя
    # ═══════════════════════════════════════════════════════════
    layer_promote = model.newLayer("quota_promote_serviceable")
    rtc_func_promote = agent.newRTCFunction("rtc_quota_promote_serviceable", RTC_QUOTA_PROMOTE_SERVICEABLE)
    rtc_func_promote.setAllowAgentDeath(False)
    rtc_func_promote.setInitialState("serviceable")  # ✅ Фильтр по state
    rtc_func_promote.setEndState("serviceable")      # Остаются в serviceable (intent изменён)
    layer_promote.addAgentFunction(rtc_func_promote)
    
    print("  RTC модуль quota_promote_serviceable зарегистрирован (1 слой, приоритет 1)")
