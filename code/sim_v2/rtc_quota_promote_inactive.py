"""
RTC модуль для промоута inactive → operations (приоритет 3)
Каскадная архитектура: использует mi8_approve/mi17_approve для подсчёта used
ВАЖНО: Может остаться deficit > 0 (допустимо по бизнес-логике)
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import model_build

import pyflamegpu as fg

def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции для промоута inactive → operations (приоритет 3)"""
    print("  Регистрация модуля квотирования: промоут inactive (приоритет 3)")
    
    # ФИКСИРОВАННЫЙ MAX_FRAMES для RTC кэширования
    max_frames = model_build.RTC_MAX_FRAMES
    
    # MAX_DAYS для MacroProperty deficit
    MAX_DAYS = model_build.MAX_DAYS
    
    # Создаём MacroProperty для публикации deficit (для динамического spawn)
    env = model.Environment()
    env.newMacroPropertyUInt("quota_deficit_mi8_u32", MAX_DAYS)
    env.newMacroPropertyUInt("quota_deficit_mi17_u32", MAX_DAYS)
    
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
    // MAX_DAYS = {MAX_DAYS} (для MacroProperty deficit)
    // Фильтр: только агенты с intent=1 (замороженные в inactive)
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    if (intent != 1u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    
    // ═══════════════════════════════════════════════════════════════════════
    // ЛОГИКА br2_mi17: Mi-17 с низким ppr НЕ ждут ремонта (комплектация)
    // ═══════════════════════════════════════════════════════════════════════
    // Mi-8 (group_by=1): всегда ждут repair_time (реальный ремонт)
    // Mi-17 (group_by=2):
    //   - ppr < br2_mi17 (3500ч) → комплектация БЕЗ ремонта, сразу готовы
    //   - ppr >= br2_mi17 → ждут repair_time (ремонт с обнулением ppr)
    const unsigned int br2_mi17 = FLAMEGPU->environment.getProperty<unsigned int>("mi17_br2_const");
    
    bool skip_repair = false;
    if (group_by == 2u && ppr < br2_mi17) {{
        // Mi-17 с ppr < порога → комплектация без ремонта, сразу готовы
        skip_repair = true;
    }}
    
    // ✅ Проверка готовности: пришло время (step_day >= repair_time) ИЛИ skip_repair
    if (!skip_repair && step_day < repair_time) {{
        // Ещё не готовы - пропускаем (ждём ремонта)
        return flamegpu::ALIVE;
    }}
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    // group_by уже определён выше
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
    
    // Публикуем deficit в MacroProperty для динамического spawn (слой 7.5)
    // ВАЖНО: Публикуем ВСЕГДА (даже если deficit <= 0), чтобы spawn_dynamic знал
    const unsigned int deficit_u = (deficit > 0) ? (unsigned int)deficit : 0u;
    if (group_by == 1u) {{
        auto deficit_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_DAYS}u>("quota_deficit_mi8_u32");
        deficit_mp[safe_day].exchange(deficit_u);
    }} else if (group_by == 2u) {{
        auto deficit_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_DAYS}u>("quota_deficit_mi17_u32");
        deficit_mp[safe_day].exchange(deficit_u);
    }}
    
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
    // ✅ КРИТИЧНО: idx УЖЕ отсортирован по mfg_date (старые первые)!
    // Для "youngest first": больший idx = моложе!
    unsigned int rank = 0u;
    
    // ✅ ВАЖНО: Фильтруем по inactive state используя буфер (state=1, intent=1, но готовы: step_day >= repair_time)
    if (group_by == 1u) {{
        auto inactive_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_inactive_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (i == idx) continue;
            if (inactive_count[i] != 1u) continue;  // ✅ Только агенты в inactive
            
            // Youngest first: rank растёт если other (i) МОЛОЖЕ меня (больший idx)
            if (i > idx) {{
                ++rank;
            }}
        }}
    }} else if (group_by == 2u) {{
        auto inactive_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_inactive_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (i == idx) continue;
            if (inactive_count[i] != 1u) continue;  // ✅ Только агенты в inactive
            
            // Youngest first: rank растёт если other (i) МОЛОЖЕ меня (больший idx)
            if (i > idx) {{
                ++rank;
            }}
        }}
    }}
    
    if (rank < K) {{
        // Я в числе K первых → промоут, меняю intent=1 на intent=2
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);  // Изменяем: 1→2 (одобрены на операции)
        
        /* Логирование выбора для P3 с информацией о br2_mi17 */
        const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        const char* repair_mode = skip_repair ? "КОМПЛЕКТАЦИЯ (ppr<br2)" : "РЕМОНТ (ppr>=br2)";
        printf("  [PROMOTE P3→2 Day %u] AC %u (group=%u, ppr=%u, br2=%u): rank=%u/%u %s\\n", 
               day, aircraft_number, group_by, ppr, br2_mi17, rank, K, repair_mode);
        
        // Записываем в ОТДЕЛЬНЫЙ буфер для inactive (избегаем race condition)
        if (group_by == 1u) {{
            auto approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve_s1");
            approve_s1[idx].exchange(1u);  // ✅ Помечаем в отдельном буфере
        }} else if (group_by == 2u) {{
            auto approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve_s1");
            approve_s1[idx].exchange(1u);
        }}
    }} else {{
        // Не вошёл в квоту → intent остаётся 1 (замороженное, ждёт следующего дня)
        // REJECT логирование убрано - оставляем только PROMOTE логи для чистоты
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
