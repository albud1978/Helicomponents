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
    
    RTC_ALLOCATE_UNSERVICEABLE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_allocate_unserviceable_lines, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    if (idx != 0u) return flamegpu::ALIVE;
    
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = (day + 1u) < days_total ? (day + 1u) : (days_total > 0u ? days_total - 1u : 0u);
    
    const unsigned int quota = FLAMEGPU->environment.getProperty<unsigned int>("repair_quota_total");
    const unsigned int lines_total = (quota < {max_frames}u) ? quota : {max_frames}u;
    if (lines_total == 0u) return flamegpu::ALIVE;
    
    auto mi8_ops = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_ops_count");
    auto mi17_ops = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_ops_count");
    auto mi8_approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve_s3");
    auto mi17_approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve_s3");
    auto cand8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_candidate_s7");
    auto cand17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_candidate_s7");
    auto approve8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve_s7");
    auto approve17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve_s7");
    auto repair_time_by_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("repair_time_by_idx");
    auto aircraft_number_by_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("aircraft_number_by_idx");
    auto line_free_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("repair_line_free_days");
    auto line_assign = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("repair_line_assign_by_idx");
    
    unsigned int curr8 = 0u, curr17 = 0u, used8 = 0u, used17 = 0u;
    unsigned int demount8 = 0u, demount17 = 0u;
    auto demount8_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve");
    auto demount17_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve");
    for (unsigned int i = 0u; i < frames; ++i) {{
        if (mi8_ops[i] == 1u) curr8++;
        if (mi17_ops[i] == 1u) curr17++;
        if (mi8_approve_s3[i] == 1u) used8++;
        if (mi17_approve_s3[i] == 1u) used17++;
        if (demount8_mp[i] == 1u) demount8++;
        if (demount17_mp[i] == 1u) demount17++;
    }}
    if (demount8 < curr8) curr8 -= demount8; else curr8 = 0u;
    if (demount17 < curr17) curr17 -= demount17; else curr17 = 0u;
    
    const unsigned int target8 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", safe_day);
    const unsigned int target17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);
    const int deficit8 = (int)target8 - (int)curr8 - (int)used8;
    const int deficit17 = (int)target17 - (int)curr17 - (int)used17;
    unsigned int K8 = deficit8 > 0 ? (unsigned int)deficit8 : 0u;
    unsigned int K17 = deficit17 > 0 ? (unsigned int)deficit17 : 0u;
    
    unsigned int line_ids[{max_frames}u];
    unsigned int line_days[{max_frames}u];
    unsigned int lines = 0u;
    for (unsigned int i = 0u; i < lines_total; ++i) {{
        line_ids[i] = 0xFFFFFFFFu;
        line_days[i] = 0u;
    }}
    for (unsigned int i = 0u; i < lines_total; ++i) {{
        const unsigned int free_days = line_free_days[i];
        if (lines >= {max_frames}u) break;
        unsigned int pos = lines;
        while (pos > 0u && free_days < line_days[pos - 1u]) {{
            line_days[pos] = line_days[pos - 1u];
            line_ids[pos] = line_ids[pos - 1u];
            --pos;
        }}
        line_days[pos] = free_days;
        line_ids[pos] = i;
        ++lines;
    }}
    unsigned int used_lines[{max_frames}u];
    for (unsigned int i = 0u; i < lines_total; ++i) used_lines[i] = 0u;
    
    unsigned int approved8 = 0u, approved17 = 0u;
    for (int i = (int)frames - 1; i >= 0; --i) {{
        if (approved8 < K8 && cand8[i] == 1u) {{
            const unsigned int rt = repair_time_by_idx[i];
            unsigned int chosen = 0xFFFFFFFFu;
            for (unsigned int s = 0u; s < lines; ++s) {{
                if (used_lines[s] == 1u) continue;
                if (line_days[s] >= rt) {{ chosen = line_ids[s]; used_lines[s] = 1u; break; }}
            }}
            if (chosen != 0xFFFFFFFFu) {{
                const unsigned int acn = aircraft_number_by_idx[i];
                if (acn == 0u) continue;
                approve8[i].exchange(1u);
                line_assign[i].exchange(chosen);
                approved8++;
            }}
        }}
        if (approved17 < K17 && cand17[i] == 1u) {{
            const unsigned int rt = repair_time_by_idx[i];
            unsigned int chosen = 0xFFFFFFFFu;
            for (unsigned int s = 0u; s < lines; ++s) {{
                if (used_lines[s] == 1u) continue;
                if (line_days[s] >= rt) {{ chosen = line_ids[s]; used_lines[s] = 1u; break; }}
            }}
            if (chosen != 0xFFFFFFFFu) {{
                const unsigned int acn = aircraft_number_by_idx[i];
                if (acn == 0u) continue;
                approve17[i].exchange(1u);
                line_assign[i].exchange(chosen);
                approved17++;
            }}
        }}
    }}
    return flamegpu::ALIVE;
}}
"""
    
    RTC_APPLY_UNSERVICEABLE_LINES = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_apply_unserviceable_lines, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    if (idx != 0u) return flamegpu::ALIVE;
    
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    auto approve8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve_s7");
    auto approve17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve_s7");
    auto line_assign = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("repair_line_assign_by_idx");
    auto aircraft_number_by_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("aircraft_number_by_idx");
    auto line_free_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("repair_line_free_days");
    auto line_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("repair_line_aircraft_number");
    
    for (unsigned int i = 0u; i < frames; ++i) {{
        if (approve8[i] == 1u || approve17[i] == 1u) {{
            const unsigned int line_id = line_assign[i];
            if (line_id == 0xFFFFFFFFu) continue;
            const unsigned int acn = aircraft_number_by_idx[i];
            if (acn == 0u) continue;
            line_free_days[line_id].exchange(0u);
            line_acn[line_id].exchange(acn);
        }}
    }}
    
    return flamegpu::ALIVE;
}}
"""
    
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
    auto line_assign = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("repair_line_assign_by_idx");
    const unsigned int line_id = line_assign[idx];
    if (line_id != 0xFFFFFFFFu) {{
        FLAMEGPU->setVariable<unsigned int>("repair_line_id", line_id);
    }}
    
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
    
    layer_allocate = model.newLayer("quota_promote_unserviceable_allocate_rtc")
    # В state-based архитектуре глобальный alloc должен быть доступен из любого состояния
    global_states = ['inactive', 'operations', 'serviceable', 'repair', 'reserve', 'storage', 'unserviceable']
    for state_name in global_states:
        fn_allocate = agent.newRTCFunction(f"rtc_allocate_unserviceable_lines_{state_name}", RTC_ALLOCATE_UNSERVICEABLE)
        fn_allocate.setInitialState(state_name)
        fn_allocate.setEndState(state_name)
        layer_allocate.addAgentFunction(fn_allocate)
    
    layer_apply_lines = model.newLayer("quota_promote_unserviceable_apply_lines")
    for state_name in global_states:
        fn_apply_lines = agent.newRTCFunction(f"rtc_apply_unserviceable_lines_{state_name}", RTC_APPLY_UNSERVICEABLE_LINES)
        fn_apply_lines.setInitialState(state_name)
        fn_apply_lines.setEndState(state_name)
        layer_apply_lines.addAgentFunction(fn_apply_lines)
    
    layer_apply = model.newLayer("quota_promote_unserviceable_apply")
    fn_apply = agent.newRTCFunction("rtc_apply_unserviceable_approval", RTC_APPLY_UNSERVICEABLE_APPROVAL)
    fn_apply.setAllowAgentDeath(False)
    fn_apply.setInitialState("unserviceable")
    fn_apply.setEndState("unserviceable")
    layer_apply.addAgentFunction(fn_apply)
    
    print("  RTC модуль quota_promote_unserviceable зарегистрирован (3 слоя, приоритет 2)")


