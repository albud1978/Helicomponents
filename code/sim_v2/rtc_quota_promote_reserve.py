"""
RTC модуль для промоута reserve → operations (приоритет 2)
Каскадная архитектура: использует mi8_approve/mi17_approve для подсчёта used
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import model_build

import pyflamegpu as fg

def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции для промоута reserve → operations (приоритет 2)"""
    print("  Регистрация модуля квотирования: промоут reserve (приоритет 2)")
    
    # ФИКСИРОВАННЫЙ MAX_FRAMES для RTC кэширования
    max_frames = model_build.RTC_MAX_FRAMES
    
    # ═══════════════════════════════════════════════════════════════
    # ПРОМОУТ ПРИОРИТЕТ 2: reserve → operations
    # ═══════════════════════════════════════════════════════════════
    # Логика:
    # 1. Считаем used (демоут + serviceable)
    # 2. Считаем Target (из mp4_ops_counter на D+1)
    # 3. deficit = Target - used
    # 4. ЕСЛИ deficit <= 0: Early exit (все агенты)
    # 5. ИНАЧЕ: Промоут deficit агентов → intent=2 + mi*_approve=1
    # ═══════════════════════════════════════════════════════════════
    
    RTC_QUOTA_PROMOTE_RESERVE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_quota_promote_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Фильтр: только агенты с intent=5 (холдинг в reserve)
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    if (intent != 5u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = ((day + 1u) < days_total ? (day + 1u) : (days_total > 0u ? days_total - 1u : 0u));
    
    // ═══════════════════════════════════════════════════════════
    // ШАГ 1: Подсчёт curr + учёт одобренных из P1 (serviceable)
    // ═══════════════════════════════════════════════════════════
    unsigned int curr = 0u;
    unsigned int target = 0u;
    unsigned int used = 0u;  // Сколько уже одобрено из P1
    
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
        
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);
    }} else {{
        return flamegpu::ALIVE;
    }}
    
    // Диагностика на ключевых днях
    if ((day == 180u || day == 181u || day == 182u) && idx == 0u) {{
        if (group_by == 1u) {{
            printf("  [PROMOTE P2 TARGET Day %u] Mi-8: Curr=%u, Used(P1)=%u, Target=%u\\n", day, curr, used, target);
        }} else if (group_by == 2u) {{
            printf("  [PROMOTE P2 TARGET Day %u] Mi-17: Curr=%u, Used(P1)=%u, Target=%u\\n", day, curr, used, target);
        }}
    }}
    
    // ═══════════════════════════════════════════════════════════
    // ШАГ 2: Расчёт дефицита (сколько не хватает до target с учётом P1)
    // ═══════════════════════════════════════════════════════════
    const int deficit = (int)target - (int)curr - (int)used;
    if (deficit <= 0) {{
        // Уже достаточно (curr + P1 одобрено) или target=0 → выход
        return flamegpu::ALIVE;
    }}
    
    // ═══════════════════════════════════════════════════════════
    // ШАГ 3: Промоут готовых агентов (каскадное квотирование P2)
    // Поднимаем ровно deficit агентов (остаток идёт в P3)
    // ═══════════════════════════════════════════════════════════
    const unsigned int K = (unsigned int)deficit;  // ✅ Каскадная логика
    
    // Ранжирование: youngest first среди РЕАЛЬНЫХ агентов в reserve
    // ✅ КРИТИЧНО: idx УЖЕ отсортирован по mfg_date (старые первые)!
    // Для Mi-8: idx 0-162 (0=самый старый, 162=самый молодой)
    // Для Mi-17: idx 163-278 (163=самый старый, 278=самый молодой)
    // Поэтому для "youngest first": больший idx = моложе!
    
    unsigned int rank = 0u;
    unsigned int total_in_reserve = 0u;  // Диагностика: сколько всего в reserve
    
    // ✅ ВАЖНО: Фильтруем по reserve state используя буфер (state=5, intent=5)
    if (group_by == 1u) {{
        auto reserve_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_reserve_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (reserve_count[i] == 1u) ++total_in_reserve;  // Диагностика
            if (i == idx) continue;
            if (reserve_count[i] != 1u) continue;  // ✅ Только агенты в reserve
            
            // Youngest first: rank растёт если other (i) МОЛОЖЕ меня (больший idx)
            if (i > idx) {{
                ++rank;
            }}
        }}
    }} else if (group_by == 2u) {{
        auto reserve_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_reserve_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (reserve_count[i] == 1u) ++total_in_reserve;  // Диагностика
            if (i == idx) continue;
            if (reserve_count[i] != 1u) continue;  // ✅ Только агенты в reserve
            
            // Youngest first: rank растёт если other (i) МОЛОЖЕ меня (больший idx)
            if (i > idx) {{
                ++rank;
            }}
        }}
    }}
    
    // Диагностика на день 149 для Mi-17
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (day == 149u && group_by == 2u) {{
        printf("  [P2 RANK Day %u] AC %u (idx %u): rank=%u/%u, K=%u, total_in_reserve=%u\\n", 
               day, aircraft_number, idx, rank, total_in_reserve, K, total_in_reserve);
    }}
    
    if (rank < K) {{
        // Я в числе K первых → промоут, меняю intent=5 на intent=2
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);  // Изменяем: 5→2 (одобрены на операции)
        
        /* Логирование выбора для P2 */
        const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        printf("  [PROMOTE P2→2 Day %u] AC %u (idx %u): rank=%u/%u reserve->operations\\n", 
               day, aircraft_number, idx, rank, K);
        
        // Записываем в ОТДЕЛЬНЫЙ буфер для reserve (избегаем race condition)
        if (group_by == 1u) {{
            auto approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve_s5");
            approve_s5[idx].exchange(1u);  // ✅ Помечаем в отдельном буфере
        }} else if (group_by == 2u) {{
            auto approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve_s5");
            approve_s5[idx].exchange(1u);
        }}
    }} else {{
        // Не вошёл в квоту → intent остаётся 5 (холдинг, ждёт следующего дня)
        // REJECT логирование убрано - оставляем только PROMOTE логи для чистоты
    }}
    
    return flamegpu::ALIVE;
}}
"""
    
    # ═══════════════════════════════════════════════════════════
    # Регистрация слоя
    # ═══════════════════════════════════════════════════════════
    layer_promote = model.newLayer("quota_promote_reserve")
    rtc_func_promote = agent.newRTCFunction("rtc_quota_promote_reserve", RTC_QUOTA_PROMOTE_RESERVE)
    rtc_func_promote.setAllowAgentDeath(False)
    rtc_func_promote.setInitialState("reserve")  # ✅ Фильтр по state
    rtc_func_promote.setEndState("reserve")      # Остаются в reserve (intent изменён)
    layer_promote.addAgentFunction(rtc_func_promote)
    
    print("  RTC модуль quota_promote_reserve зарегистрирован (1 слой, приоритет 2)")
