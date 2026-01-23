"""
RTC модуль для промоута unserviceable → operations (приоритет 2)
Каскадная архитектура: использует repair quota и линии ремонта
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import model_build

import pyflamegpu as fg


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции для промоута unserviceable → operations (приоритет 2)"""
    print("  Регистрация модуля квотирования: промоут unserviceable (приоритет 2)")
    
    # ФИКСИРОВАННЫЙ MAX_FRAMES для RTC кэширования
    max_frames = model_build.RTC_MAX_FRAMES
    MAX_DAYS = model_build.MAX_DAYS
    
    # ═══════════════════════════════════════════════════════════════
    # ПРОМОУТ ПРИОРИТЕТ 2: unserviceable → operations (через repair quota)
    # ═══════════════════════════════════════════════════════════════
    # Логика:
    # 1. Маркируем кандидатов (unserviceable, intent=0)
    # 2. Один host-агент считает дефицит и аллоцирует топ-K по idx (youngest first)
    # 3. Каждый утверждённый агент получает intent=2
    # ═══════════════════════════════════════════════════════════════
    
    RTC_MARK_UNSERVICEABLE_CANDIDATES = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mark_unserviceable_candidates, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Фильтр: очередь на ремонт (intent=0)
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    if (intent != 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    if (group_by == 1u) {{
        auto cand = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_candidate_s7");
        cand[idx].exchange(1u);
    }} else if (group_by == 2u) {{
        auto cand = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_candidate_s7");
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

            cand8 = env.getMacroPropertyUInt32("mi8_candidate_s7")
            cand17 = env.getMacroPropertyUInt32("mi17_candidate_s7")
            approve8 = env.getMacroPropertyUInt32("mi8_approve_s7")
            approve17 = env.getMacroPropertyUInt32("mi17_approve_s7")

            repair_time_by_idx = env.getMacroPropertyUInt32("repair_time_by_idx")
            line_free_days = env.getMacroPropertyUInt32("repair_line_free_days")
            repair_day_count = env.getMacroPropertyUInt32("repair_day_count")
            repair_backfill = env.getMacroPropertyUInt32("repair_backfill_load")

            # Подсчёт curr/used по группам (used = P1 serviceable)
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

            target8 = env.getPropertyUInt("mp4_ops_counter_mi8", safe_day)
            target17 = env.getPropertyUInt("mp4_ops_counter_mi17", safe_day)
            deficit8 = int(target8) - int(curr8) - int(used8)
            deficit17 = int(target17) - int(curr17) - int(used17)
            K8 = deficit8 if deficit8 > 0 else 0
            K17 = deficit17 if deficit17 > 0 else 0

            # Аллокация: youngest first (idx desc) с проверкой глобального окна
            approved8 = approved17 = 0
            for i in range(frames - 1, -1, -1):
                if approved8 < K8 and cand8[i] == 1:
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
    
    RTC_APPLY_UNSERVICEABLE_APPROVAL = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_apply_unserviceable_approval, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int debug_enabled = FLAMEGPU->environment.getProperty<unsigned int>("debug_enabled");
    
    unsigned int approved = 0u;
    if (group_by == 1u) {{
        auto approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve_s7");
        approved = approve[idx];
    }} else if (group_by == 2u) {{
        auto approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve_s7");
        approved = approve[idx];
    }} else {{
        return flamegpu::ALIVE;
    }}
    
    if (approved == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Меняем intent на operations (дальше обработает state_manager_unserviceable)
    FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
    
    if (debug_enabled) {{
        const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        printf("  [PROMOTE P2→2 Day %u] AC %u (group=%u): unserviceable->operations\\n",
               day, aircraft_number, group_by);
    }}
    
    return flamegpu::ALIVE;
}}
"""
    
    # ═══════════════════════════════════════════════════════════
    # Регистрация слоёв
    # ═══════════════════════════════════════════════════════════
    layer_candidates = model.newLayer("quota_promote_unserviceable_candidates")
    fn_candidates = agent.newRTCFunction("rtc_mark_unserviceable_candidates", RTC_MARK_UNSERVICEABLE_CANDIDATES)
    fn_candidates.setAllowAgentDeath(False)
    fn_candidates.setInitialState("unserviceable")
    fn_candidates.setEndState("unserviceable")
    layer_candidates.addAgentFunction(fn_candidates)
    
    layer_allocate = model.newLayer("quota_promote_unserviceable_allocate_host")
    layer_allocate.addHostFunction(RepairLineAllocatorHost(max_frames, MAX_DAYS))
    
    layer_apply = model.newLayer("quota_promote_unserviceable_apply")
    fn_apply = agent.newRTCFunction("rtc_apply_unserviceable_approval", RTC_APPLY_UNSERVICEABLE_APPROVAL)
    fn_apply.setAllowAgentDeath(False)
    fn_apply.setInitialState("unserviceable")
    fn_apply.setEndState("unserviceable")
    layer_apply.addAgentFunction(fn_apply)
    
    print("  RTC модуль quota_promote_unserviceable зарегистрирован (3 слоя, приоритет 2)")

