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
    # 1. Маркируем кандидатов (ready inactive) без ранжирования
    # 2. Один агент (idx=0) считает дефицит и аллоцирует top-K по idx (youngest first)
    # 3. Проверяем окно ремонта (quota-1) и ставим approve_s1
    # 4. Каждый утверждённый агент применяет intent=2 + резервирует backfill окно
    # 5. ⚠️ Может остаться deficit > 0 (допустимо!)
    # ═══════════════════════════════════════════════════════════════
    
    RTC_MARK_INACTIVE_CANDIDATES = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mark_inactive_candidates, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Фильтр: только агенты с intent=1 (замороженные в inactive)
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    if (intent != 1u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // ЛОГИКА br2_mi17: Mi-17 с низким ppr НЕ ждут ремонта (комплектация)
    const unsigned int br2_mi17 = FLAMEGPU->environment.getProperty<unsigned int>("mi17_br2_const");
    bool skip_repair = false;
    if (group_by == 2u && ppr < br2_mi17) {{
        skip_repair = true;
    }}
    
    // Проверка готовности
    if (!skip_repair && step_day < repair_time) {{
        return flamegpu::ALIVE;
    }}
    
    if (group_by == 1u) {{
        auto cand = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_candidate_s1");
        auto cand_rt = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_candidate_repair_time");
        auto cand_skip = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_candidate_skip_repair");
        cand[idx].exchange(1u);
        cand_rt[idx].exchange(repair_time);
        cand_skip[idx].exchange(0u);
    }} else if (group_by == 2u) {{
        auto cand = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_candidate_s1");
        auto cand_rt = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_candidate_repair_time");
        auto cand_skip = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_candidate_skip_repair");
        cand[idx].exchange(1u);
        cand_rt[idx].exchange(repair_time);
        cand_skip[idx].exchange(skip_repair ? 1u : 0u);
    }}
    
    return flamegpu::ALIVE;
}}
"""
    
    RTC_ALLOCATE_INACTIVE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_allocate_inactive, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    if (idx != 0u) return flamegpu::ALIVE;  // аллокатор только один
    
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = ((day + 1u) < days_total ? (day + 1u) : (days_total > 0u ? days_total - 1u : 0u));
    const unsigned int quota = FLAMEGPU->environment.getProperty<unsigned int>("repair_quota_total");
    const unsigned int limit = (quota > 0u) ? (quota - 1u) : 0u;
    
    // Буферы для подсчёта и кандидатов
    auto mi8_ops = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_ops_count");
    auto mi17_ops = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_ops_count");
    auto mi8_approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve_s3");
    auto mi17_approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve_s3");
    auto mi8_approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve_s5");
    auto mi17_approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve_s5");
    
    auto cand8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_candidate_s1");
    auto cand17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_candidate_s1");
    auto cand8_rt = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_candidate_repair_time");
    auto cand17_rt = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_candidate_repair_time");
    auto cand8_skip = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_candidate_skip_repair");
    auto cand17_skip = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_candidate_skip_repair");
    
    auto approve8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve_s1");
    auto approve17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve_s1");
    
    auto repair_day_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_DAYS}u>("repair_day_count");
    auto repair_backfill = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_DAYS}u>("repair_backfill_load");
    
    // Подсчёт curr/used по группам
    unsigned int curr8 = 0u, curr17 = 0u, used8 = 0u, used17 = 0u;
    for (unsigned int i = 0u; i < frames; ++i) {{
        if (mi8_ops[i] == 1u) ++curr8;
        if (mi17_ops[i] == 1u) ++curr17;
        if (mi8_approve_s3[i] == 1u) ++used8;
        if (mi17_approve_s3[i] == 1u) ++used17;
        if (mi8_approve_s5[i] == 1u) ++used8;
        if (mi17_approve_s5[i] == 1u) ++used17;
    }}
    
    const unsigned int target8 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", safe_day);
    const unsigned int target17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);
    const int deficit8 = (int)target8 - (int)curr8 - (int)used8;
    const int deficit17 = (int)target17 - (int)curr17 - (int)used17;
    const unsigned int K8 = (deficit8 > 0) ? (unsigned int)deficit8 : 0u;
    const unsigned int K17 = (deficit17 > 0) ? (unsigned int)deficit17 : 0u;
    
    // Публикуем deficit для динамического spawn
    auto deficit_mp8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_DAYS}u>("quota_deficit_mi8_u32");
    auto deficit_mp17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_DAYS}u>("quota_deficit_mi17_u32");
    deficit_mp8[safe_day].exchange((deficit8 > 0) ? (unsigned int)deficit8 : 0u);
    deficit_mp17[safe_day].exchange((deficit17 > 0) ? (unsigned int)deficit17 : 0u);
    
    // Локальный учёт доп. нагрузки для окна ремонта
    unsigned int extra[{MAX_DAYS}u];
    for (unsigned int d = 0u; d < {MAX_DAYS}u; ++d) {{
        extra[d] = 0u;
    }}
    
    unsigned int approved8 = 0u;
    unsigned int approved17 = 0u;
    
    // Аллокация: youngest first (idx desc)
    for (int i = (int)frames - 1; i >= 0; --i) {{
        const unsigned int ui = (unsigned int)i;
        if (approved8 < K8 && cand8[ui] == 1u) {{
            const unsigned int R = cand8_rt[ui];
            bool ok = true;
            if (quota > 0u) {{
                const unsigned int start = (day > R) ? (day - R) : 0u;
                for (unsigned int d = start; d < day && d < {MAX_DAYS}u; ++d) {{
                    const unsigned int load = repair_day_count[d] + repair_backfill[d] + extra[d];
                    if (load > limit) {{
                        ok = false;
                        break;
                    }}
                }}
                if (ok) {{
                    for (unsigned int d = start; d < day && d < {MAX_DAYS}u; ++d) {{
                        extra[d] += 1u;
                    }}
                }}
            }}
            if (ok) {{
                approve8[ui].exchange(1u);
                ++approved8;
            }}
        }}
        if (approved17 < K17 && cand17[ui] == 1u) {{
            const unsigned int R = cand17_rt[ui];
            bool ok = true;
            if (quota > 0u) {{
                const unsigned int start = (day > R) ? (day - R) : 0u;
                for (unsigned int d = start; d < day && d < {MAX_DAYS}u; ++d) {{
                    const unsigned int load = repair_day_count[d] + repair_backfill[d] + extra[d];
                    if (load > limit) {{
                        ok = false;
                        break;
                    }}
                }}
                if (ok) {{
                    for (unsigned int d = start; d < day && d < {MAX_DAYS}u; ++d) {{
                        extra[d] += 1u;
                    }}
                }}
            }}
            if (ok) {{
                approve17[ui].exchange(1u);
                ++approved17;
            }}
        }}
    }}
    
    return flamegpu::ALIVE;
}}
"""
    
    RTC_APPLY_INACTIVE_APPROVAL = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_apply_inactive_approval, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    
    unsigned int approved = 0u;
    if (group_by == 1u) {{
        auto approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve_s1");
        approved = approve[idx];
    }} else if (group_by == 2u) {{
        auto approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve_s1");
        approved = approve[idx];
    }} else {{
        return flamegpu::ALIVE;
    }}
    
    if (approved == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // ЛОГИКА br2_mi17: Mi-17 с низким ppr НЕ ждут ремонта (комплектация)
    const unsigned int br2_mi17 = FLAMEGPU->environment.getProperty<unsigned int>("mi17_br2_const");
    bool skip_repair = false;
    if (group_by == 2u && ppr < br2_mi17) {{
        skip_repair = true;
    }}
    
    // Резервируем окно backfill всегда (окно обязано быть свободным)
    auto repair_backfill = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_DAYS}u>("repair_backfill_load");
    const unsigned int start = (day > repair_time) ? (day - repair_time) : 0u;
    for (unsigned int d = start; d < day && d < {MAX_DAYS}u; ++d) {{
        repair_backfill[d]++;
    }}
    
    // Меняем intent на operations
    FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
    
    // Логирование
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const char* repair_mode = skip_repair ? "КОМПЛЕКТАЦИЯ (ppr<br2)" : "РЕМОНТ (ppr>=br2)";
    printf("  [PROMOTE P3→2 Day %u] AC %u (group=%u, ppr=%u, br2=%u): %s\\n",
           day, aircraft_number, group_by, ppr, br2_mi17, repair_mode);
    
    return flamegpu::ALIVE;
}}
"""
    
    # ═══════════════════════════════════════════════════════════
    # Регистрация слоя
    # ═══════════════════════════════════════════════════════════
    layer_candidates = model.newLayer("quota_promote_inactive_candidates")
    fn_candidates = agent.newRTCFunction("rtc_mark_inactive_candidates", RTC_MARK_INACTIVE_CANDIDATES)
    fn_candidates.setAllowAgentDeath(False)
    fn_candidates.setInitialState("inactive")
    fn_candidates.setEndState("inactive")
    layer_candidates.addAgentFunction(fn_candidates)
    
    layer_allocate = model.newLayer("quota_promote_inactive_allocate")
    fn_allocate = agent.newRTCFunction("rtc_allocate_inactive", RTC_ALLOCATE_INACTIVE)
    fn_allocate.setAllowAgentDeath(False)
    layer_allocate.addAgentFunction(fn_allocate)
    
    layer_apply = model.newLayer("quota_promote_inactive_apply")
    fn_apply = agent.newRTCFunction("rtc_apply_inactive_approval", RTC_APPLY_INACTIVE_APPROVAL)
    fn_apply.setAllowAgentDeath(False)
    fn_apply.setInitialState("inactive")
    fn_apply.setEndState("inactive")
    layer_apply.addAgentFunction(fn_apply)
    
    print("  RTC модуль quota_promote_inactive зарегистрирован (3 слоя, приоритет 3)")
