"""
RTC модуль для обработки избытка операций (2->3 демоут)
Однослойная архитектура с early exit и каскадным буфером
"""

import pyflamegpu as fg

def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции для демоута operations → serviceable"""
    print("  Регистрация модуля квотирования: демоут (operations → serviceable)")
    
    # Получаем MAX_FRAMES из модели
    max_frames = model.Environment().getPropertyUInt("frames_total")
    
    # ═══════════════════════════════════════════════════════════════
    # ОДНОСЛОЙНЫЙ ДЕМОУТ С EARLY EXIT
    # ═══════════════════════════════════════════════════════════════
    # Логика:
    # 1. Считаем Curr (агенты в operations с intent=2, кроме 4/6)
    # 2. Считаем Target (из mp4_ops_counter на D+1)
    # 3. Balance = Curr - Target
    # 4. ЕСЛИ Balance <= 0: Early exit (все агенты)
    # 5. ИНАЧЕ: Демоут K самых старых → intent=3 + mi*_approve=1
    # ═══════════════════════════════════════════════════════════════
    
    RTC_QUOTA_DEMOUNT = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_quota_demount, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // ═══════════════════════════════════════════════════════════
    // ШАГ 0: Фильтрация (исключаем технологические переходы)
    // ═══════════════════════════════════════════════════════════
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    if (intent == 4u || intent == 6u) {{
        return flamegpu::ALIVE;  // Технологические переходы не участвуют
    }}
    
    // ✅ ВАЖНО: Работаем ТОЛЬКО с intent=2 (хотят в operations)
    if (intent != 2u) {{
        return flamegpu::ALIVE;  // Любые другие intent - пропускаем
    }}
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = ((day + 1u) < days_total ? (day + 1u) : (days_total > 0u ? days_total - 1u : 0u));
    
    // ═══════════════════════════════════════════════════════════
    // ШАГ 1: Подсчёт Curr и Target (из ops_count буфера)
    // ═══════════════════════════════════════════════════════════
    unsigned int curr = 0u;
    unsigned int target = 0u;
    
    if (group_by == 1u) {{
        // Mi-8: считаем из ops_count буфера (заполненного в count_ops)
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_ops_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (ops_count[i] == 1u) ++curr;
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", safe_day);
        
    }} else if (group_by == 2u) {{
        // Mi-17: аналогично
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_ops_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (ops_count[i] == 1u) ++curr;
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);
    }} else {{
        return flamegpu::ALIVE;  // Неизвестный group_by
    }}
    
    const int balance = (int)curr - (int)target;
    
    // ═══════════════════════════════════════════════════════════
    // ШАГ 2: Early exit при отсутствии избытка
    // ═══════════════════════════════════════════════════════════
    if (balance <= 0) {{
        // Нет избытка → все агенты выходят без вычислений
        return flamegpu::ALIVE;  // ✅ Оптимизация: warp divergence минимален
    }}
    
    // ═══════════════════════════════════════════════════════════
    // ШАГ 3: Демоут K самых старых (ТОЛЬКО если Balance > 0)
    // ═══════════════════════════════════════════════════════════
    const unsigned int K = (unsigned int)balance;
    
    // Ранжирование по mfg_date (oldest_first) среди агентов в operations
    const unsigned int my_mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", idx);
    unsigned int rank = 0u;
    
    if (group_by == 1u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_ops_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (i == idx) continue;
            if (ops_count[i] != 1u) continue;  // ✅ Только агенты в operations
            
            const unsigned int other_mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", i);
            if (other_mfg < my_mfg || (other_mfg == my_mfg && i < idx)) {{
                ++rank;
            }}
        }}
    }} else if (group_by == 2u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_ops_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (i == idx) continue;
            if (ops_count[i] != 1u) continue;  // ✅ Только агенты в operations
            
            const unsigned int other_mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", i);
            if (other_mfg < my_mfg || (other_mfg == my_mfg && i < idx)) {{
                ++rank;
            }}
        }}
    }}
    
    if (rank < K) {{
        // Я в числе K самых старых → демоут
        FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
        
        // Записываем в каскадный буфер для учёта в промоуте
        if (group_by == 1u) {{
            auto approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve");
            approve[idx].exchange(1u);  // ✅ Помечаем (для подсчёта used в промоуте)
        }} else if (group_by == 2u) {{
            auto approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve");
            approve[idx].exchange(1u);
        }}
        
        // Диагностика
        if (day == 180u || day == 181u || day == 182u) {{
            const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
            printf("  [DEMOUNT Day %u] AC %u: rank=%u/%u (mfg=%u, balance=%d)\\n", 
                   day, aircraft_number, rank, K, my_mfg, balance);
        }}
    }}
    // Иначе intent остаётся = 2 (остаюсь в operations)
    
    return flamegpu::ALIVE;
}}
"""
    
    # ═══════════════════════════════════════════════════════════
    # Регистрация единого слоя
    # ═══════════════════════════════════════════════════════════
    layer_demount = model.newLayer("quota_demount")
    rtc_func_demount = agent.newRTCFunction("rtc_quota_demount", RTC_QUOTA_DEMOUNT)
    rtc_func_demount.setAllowAgentDeath(False)
    rtc_func_demount.setInitialState("operations")  # ✅ Фильтр по state
    rtc_func_demount.setEndState("operations")      # Остаются в operations (intent изменён)
    layer_demount.addAgentFunction(rtc_func_demount)
    
    print("  RTC модуль quota_demount зарегистрирован (1 слой, early exit)")