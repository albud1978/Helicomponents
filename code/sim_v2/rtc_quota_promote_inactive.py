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
    # 1. Маркируем кандидатов (inactive)
    # 2. Один агент (idx=0) считает дефицит и аллоцирует top-K по idx (youngest first)
    # 3. Каждый утверждённый агент применяет intent=2 и потребляет линию/окно
    # 4. ⚠️ Может остаться deficit > 0 (допустимо!)
    # ═══════════════════════════════════════════════════════════════
    
    RTC_MARK_INACTIVE_CANDIDATES = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mark_inactive_candidates, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Фильтр: только агенты с intent=1 (замороженные в inactive)
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    if (intent != 1u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    if (group_by == 1u) {{
        auto cand = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_candidate_s1");
        cand[idx].exchange(1u);
    }} else if (group_by == 2u) {{
        auto cand = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_candidate_s1");
        cand[idx].exchange(1u);
    }}
    
    return flamegpu::ALIVE;
}}
"""
    
    class RepairLineAllocatorHost(fg.HostFunction):
        def __init__(self, max_frames: int, max_days: int):
            super().__init__()
            self.max_frames = max_frames
            self.max_days = max_days

        def run(self, FLAMEGPU):
            env = FLAMEGPU.environment
            day = FLAMEGPU.getStepCounter()
            frames = env.getPropertyUInt("frames_total")
            days_total = env.getPropertyUInt("days_total")
            safe_day = (day + 1) if (day + 1) < days_total else (days_total - 1 if days_total > 0 else 0)

            quota = env.getPropertyUInt("repair_quota_total")
            lines_total = min(int(quota), self.max_frames)
            if lines_total == 0:
                return

            # Буферы для подсчёта и кандидатов
            mi8_ops = env.getMacroPropertyUInt32("mi8_ops_count")
            mi17_ops = env.getMacroPropertyUInt32("mi17_ops_count")
            mi8_approve_s3 = env.getMacroPropertyUInt32("mi8_approve_s3")
            mi17_approve_s3 = env.getMacroPropertyUInt32("mi17_approve_s3")
            mi8_approve_s5 = env.getMacroPropertyUInt32("mi8_approve_s5")
            mi17_approve_s5 = env.getMacroPropertyUInt32("mi17_approve_s5")

            cand8 = env.getMacroPropertyUInt32("mi8_candidate_s1")
            cand17 = env.getMacroPropertyUInt32("mi17_candidate_s1")
            approve8 = env.getMacroPropertyUInt32("mi8_approve_s1")
            approve17 = env.getMacroPropertyUInt32("mi17_approve_s1")

            repair_time_by_idx = env.getMacroPropertyUInt32("repair_time_by_idx")
            line_free_days = env.getMacroPropertyUInt32("repair_line_free_days")
            repair_day_count = env.getMacroPropertyUInt32("repair_day_count")
            repair_backfill = env.getMacroPropertyUInt32("repair_backfill_load")

            # Подсчёт curr/used по группам
            curr8 = curr17 = used8 = used17 = 0
            for i in range(frames):
                if mi8_ops[i] == 1:
                    curr8 += 1
                if mi17_ops[i] == 1:
                    curr17 += 1
                if mi8_approve_s3[i] == 1:
                    used8 += 1
                if mi17_approve_s3[i] == 1:
                    used17 += 1
                if mi8_approve_s5[i] == 1:
                    used8 += 1
                if mi17_approve_s5[i] == 1:
                    used17 += 1

            target8 = env.getPropertyUInt("mp4_ops_counter_mi8", safe_day)
            target17 = env.getPropertyUInt("mp4_ops_counter_mi17", safe_day)
            deficit8 = int(target8) - int(curr8) - int(used8)
            deficit17 = int(target17) - int(curr17) - int(used17)
            K8 = deficit8 if deficit8 > 0 else 0
            K17 = deficit17 if deficit17 > 0 else 0

            # Публикуем deficit для динамического spawn
            deficit_mp8 = env.getMacroPropertyUInt32("quota_deficit_mi8_u32")
            deficit_mp17 = env.getMacroPropertyUInt32("quota_deficit_mi17_u32")
            deficit_mp8[safe_day] = K8
            deficit_mp17[safe_day] = K17

            # Аллокация: youngest first (idx desc) с проверкой глобального окна
            approved8 = approved17 = 0
            for i in range(frames - 1, -1, -1):
                if approved8 < K8 and cand8[i] == 1:
                    rt = int(repair_time_by_idx[i])
                    # Проверяем окно [day-rt, day-1]
                    start = day - rt if day > rt else 0
                    ok = True
                    for d in range(start, day):
                        if repair_day_count[d] + repair_backfill[d] >= lines_total:
                            ok = False
                            break
                    if ok:
                        for l in range(lines_total):
                            if line_free_days[l] >= rt:
                                line_free_days[l] = 0
                                approve8[i] = 1
                                approved8 += 1
                                for d in range(start, day):
                                    repair_backfill[d] = repair_backfill[d] + 1
                                break
                if approved17 < K17 and cand17[i] == 1:
                    rt = int(repair_time_by_idx[i])
                    start = day - rt if day > rt else 0
                    ok = True
                    for d in range(start, day):
                        if repair_day_count[d] + repair_backfill[d] >= lines_total:
                            ok = False
                            break
                    if ok:
                        for l in range(lines_total):
                            if line_free_days[l] >= rt:
                                line_free_days[l] = 0
                                approve17[i] = 1
                                approved17 += 1
                                for d in range(start, day):
                                    repair_backfill[d] = repair_backfill[d] + 1
                                break
    
    RTC_APPLY_INACTIVE_APPROVAL = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_apply_inactive_approval, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    
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
    
    // Меняем intent на operations
    FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
    
    // Логирование
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    printf("  [PROMOTE P3→2 Day %u] AC %u (group=%u, ppr=%u): line_consumed\\n",
           day, aircraft_number, group_by, ppr);
    
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
    
    layer_allocate = model.newLayer("quota_promote_inactive_allocate_host")
    layer_allocate.addHostFunction(RepairLineAllocatorHost(max_frames, MAX_DAYS))
    
    layer_apply = model.newLayer("quota_promote_inactive_apply")
    fn_apply = agent.newRTCFunction("rtc_apply_inactive_approval", RTC_APPLY_INACTIVE_APPROVAL)
    fn_apply.setAllowAgentDeath(False)
    fn_apply.setInitialState("inactive")
    fn_apply.setEndState("inactive")
    layer_apply.addAgentFunction(fn_apply)
    
    print("  RTC модуль quota_promote_inactive зарегистрирован (3 слоя, приоритет 3)")
