#!/usr/bin/env python3
"""
RTC модуль V8: локальные копии V7 квот (reset/count/demote/P1/post).

Назначение:
- Развязать V8 от прямой зависимости на rtc_quota_v7.py
- Хранить стабильную логику квот для V8 в одном файле
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from model_build import RTC_MAX_FRAMES, MAX_DAYS

# ═══════════════════════════════════════════════════════════════════════════════
# СБРОС БУФЕРОВ
# ═══════════════════════════════════════════════════════════════════════════════

RTC_RESET_BUFFERS = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_reset_quota_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Сброс буферов квотирования (idx=0 агент)
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    if (idx != 0u) return flamegpu::ALIVE;
    
    // КРИТИЧНО: сбрасываем ВСЕ RTC_MAX_FRAMES слотов,
    // чтобы включить динамически спавненных агентов!
    auto mi8_ops = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_ops_count");
    auto mi17_ops = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_ops_count");
    auto mi8_svc = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_svc_count");
    auto mi17_svc = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_svc_count");
    auto mi8_unsvc = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_unsvc_count");
    auto mi17_unsvc = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_unsvc_count");
    auto mi8_unsvc_ready = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_unsvc_ready_count");
    auto mi17_unsvc_ready = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_unsvc_ready_count");
    auto mi8_inactive = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_inactive_count");
    auto mi17_inactive = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_inactive_count");
    
    for (unsigned int i = 0u; i < {RTC_MAX_FRAMES}u; ++i) {{
        mi8_ops[i].exchange(0u);
        mi17_ops[i].exchange(0u);
        mi8_svc[i].exchange(0u);
        mi17_svc[i].exchange(0u);
        mi8_unsvc[i].exchange(0u);
        mi17_unsvc[i].exchange(0u);
        mi8_unsvc_ready[i].exchange(0u);
        mi17_unsvc_ready[i].exchange(0u);
        mi8_inactive[i].exchange(0u);
        mi17_inactive[i].exchange(0u);
    }}
    
    return flamegpu::ALIVE;
}}
"""

# ═══════════════════════════════════════════════════════════════════════════════
# ПОДСЧЁТ АГЕНТОВ
# ═══════════════════════════════════════════════════════════════════════════════

RTC_COUNT_OPS = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_count_ops_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    if (group_by == 1u) {{
        auto count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_ops_count");
        count[idx].exchange(1u);
    }} else if (group_by == 2u) {{
        auto count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_ops_count");
        count[idx].exchange(1u);
    }}
    return flamegpu::ALIVE;
}}
"""

RTC_COUNT_SVC = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_count_svc_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    if (group_by == 1u) {{
        auto count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_svc_count");
        count[idx].exchange(1u);
    }} else if (group_by == 2u) {{
        auto count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_svc_count");
        count[idx].exchange(1u);
    }}
    return flamegpu::ALIVE;
}}
"""

RTC_COUNT_INACTIVE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_count_inactive_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    if (group_by == 1u) {{
        auto count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_inactive_count");
        count[idx].exchange(1u);
    }} else if (group_by == 2u) {{
        auto count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_inactive_count");
        count[idx].exchange(1u);
    }}
    return flamegpu::ALIVE;
}}
"""

# ═══════════════════════════════════════════════════════════════════════════════
# ДЕМОУТ (operations → serviceable при избытке)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_DEMOTE_OPS = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_demote_ops_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Демоут: если в operations избыток, отмечаем агентов для выхода
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    auto mp_result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
    unsigned int step_days = mp_result[0];
    if (step_days == 0u) step_days = 1u;
    unsigned int safe_day = day + step_days;
    if (safe_day >= days_total) safe_day = (days_total > 0u ? days_total - 1u : 0u);
    unsigned int target_day = day;
    if (target_day >= days_total) target_day = (days_total > 0u ? days_total - 1u : 0u);
    
    // Подсчёт текущих в operations (включая динамические спавны!)
    // КРИТИЧНО: используем RTC_MAX_FRAMES, а не frames_total, чтобы учесть спавненных агентов
    unsigned int curr = 0u;
    unsigned int target = 0u;
    
    if (group_by == 1u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_ops_count");
        for (unsigned int i = 0u; i < {RTC_MAX_FRAMES}u; ++i) {{
            if (ops_count[i] == 1u) ++curr;
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", target_day);
    }} else if (group_by == 2u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_ops_count");
        for (unsigned int i = 0u; i < {RTC_MAX_FRAMES}u; ++i) {{
            if (ops_count[i] == 1u) ++curr;
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", target_day);
    }} else {{
        return flamegpu::ALIVE;
    }}
    
    // Избыток?
    if (curr <= target) {{
        return flamegpu::ALIVE;
    }}
    
    // Нужно демоутить (curr - target) агентов
    // Ранжирование: младшие idx демоутятся первыми
    unsigned int excess = curr - target;
    
    // Считаем сколько агентов с меньшим idx
    unsigned int rank = 0u;
    if (group_by == 1u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_ops_count");
        for (unsigned int i = 0u; i < idx; ++i) {{
            if (ops_count[i] == 1u) ++rank;
        }}
    }} else {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_ops_count");
        for (unsigned int i = 0u; i < idx; ++i) {{
            if (ops_count[i] == 1u) ++rank;
        }}
    }}
    
    // Если rank < excess — демоутим
    if (rank < excess) {{
        FLAMEGPU->setVariable<unsigned int>("needs_demote", 1u);
        FLAMEGPU->setVariable<unsigned int>("debug_needs_demote", 1u);
    }}
    
    return flamegpu::ALIVE;
}}
"""

# ═══════════════════════════════════════════════════════════════════════════════
# P1 ПРОМОУТ: serviceable → operations
# ═══════════════════════════════════════════════════════════════════════════════

RTC_PROMOTE_SVC = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_promote_svc_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // P1: serviceable → operations (если дефицит)
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    auto mp_result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
    unsigned int step_days = mp_result[0];
    if (step_days == 0u) step_days = 1u;
    unsigned int safe_day = day + step_days;
    if (safe_day >= days_total) safe_day = (days_total > 0u ? days_total - 1u : 0u);
    unsigned int target_day = day;
    if (target_day >= days_total) target_day = (days_total > 0u ? days_total - 1u : 0u);
    
    // Подсчёт текущих в operations (включая динамические спавны!)
    // КРИТИЧНО: используем RTC_MAX_FRAMES, а не frames_total
    unsigned int curr = 0u;
    unsigned int target = 0u;
    unsigned int svc_available = 0u;
    
    if (group_by == 1u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_svc_count");
        for (unsigned int i = 0u; i < {RTC_MAX_FRAMES}u; ++i) {{
            if (ops_count[i] == 1u) ++curr;
            if (svc_count[i] == 1u) ++svc_available;
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", target_day);
    }} else if (group_by == 2u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_svc_count");
        for (unsigned int i = 0u; i < {RTC_MAX_FRAMES}u; ++i) {{
            if (ops_count[i] == 1u) ++curr;
            if (svc_count[i] == 1u) ++svc_available;
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", target_day);
    }} else {{
        return flamegpu::ALIVE;
    }}
    
    // Дефицит?
    if (curr >= target) {{
        return flamegpu::ALIVE;
    }}
    
    unsigned int deficit = target - curr;
    unsigned int K = (deficit < svc_available) ? deficit : svc_available;
    
    if (K == 0u) return flamegpu::ALIVE;
    
    // Ранжирование: младшие idx промоутятся первыми
    unsigned int rank = 0u;
    if (group_by == 1u) {{
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_svc_count");
        for (unsigned int i = 0u; i < idx; ++i) {{
            if (svc_count[i] == 1u) ++rank;
        }}
    }} else {{
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_svc_count");
        for (unsigned int i = 0u; i < idx; ++i) {{
            if (svc_count[i] == 1u) ++rank;
        }}
    }}
    
    // Если rank < K — промоутим
    if (rank < K) {{
        FLAMEGPU->setVariable<unsigned int>("promoted", 1u);
        FLAMEGPU->setVariable<unsigned int>("debug_promoted", 1u);
    }}
    
    return flamegpu::ALIVE;
}}
"""

# ═══════════════════════════════════════════════════════════════════════════════
# P3 POST-КВОТА: дополнительный добор из inactive до target
# ═══════════════════════════════════════════════════════════════════════════════

RTC_PROMOTE_INACTIVE_POST = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_promote_inactive_post_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // POST P3: inactive → operations (добор после всех переходов)
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    auto mp_result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
    unsigned int step_days = mp_result[0];
    if (step_days == 0u) step_days = 1u;
    unsigned int safe_day = day + step_days;
    if (safe_day >= days_total) safe_day = (days_total > 0u ? days_total - 1u : 0u);
    unsigned int target_day = day;
    if (target_day >= days_total) target_day = (days_total > 0u ? days_total - 1u : 0u);
    
    unsigned int ops_curr = 0u;
    unsigned int inactive_available = 0u;
    unsigned int target = 0u;
    
    if (group_by == 1u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_ops_count");
        auto inactive_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_inactive_count");
        for (unsigned int i = 0u; i < {RTC_MAX_FRAMES}u; ++i) {{
            if (ops_count[i] == 1u) ++ops_curr;
            if (inactive_count[i] == 1u) ++inactive_available;
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", target_day);
    }} else if (group_by == 2u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_ops_count");
        auto inactive_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_inactive_count");
        for (unsigned int i = 0u; i < {RTC_MAX_FRAMES}u; ++i) {{
            if (ops_count[i] == 1u) ++ops_curr;
            if (inactive_count[i] == 1u) ++inactive_available;
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", target_day);
    }} else {{
        return flamegpu::ALIVE;
    }}
    
    if (ops_curr >= target) {{
        return flamegpu::ALIVE;
    }}
    
    unsigned int deficit = target - ops_curr;
    unsigned int K = (deficit < inactive_available) ? deficit : inactive_available;
    if (K == 0u) return flamegpu::ALIVE;
    
    // Ранжирование
    unsigned int rank = 0u;
    if (group_by == 1u) {{
        auto inactive_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_inactive_count");
        for (unsigned int i = 0u; i < idx; ++i) {{
            if (inactive_count[i] == 1u) ++rank;
        }}
    }} else {{
        auto inactive_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_inactive_count");
        for (unsigned int i = 0u; i < idx; ++i) {{
            if (inactive_count[i] == 1u) ++rank;
        }}
    }}
    
    if (rank < K) {{
        FLAMEGPU->setVariable<unsigned int>("promoted", 1u);
        FLAMEGPU->setVariable<unsigned int>("debug_promoted", 1u);
    }}
    
    return flamegpu::ALIVE;
}}
"""

COND_INACTIVE_PROMOTED_POST = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_inactive_promoted_post_v8) {
    return FLAMEGPU->getVariable<unsigned int>("promoted") == 1u;
}
"""

RTC_INACTIVE_TO_OPS_POST = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_inactive_to_ops_post_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // P3 POST: PPR по правилам group_by
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    
    // Mi-17: если PPR < br2_mi17, сохраняем; иначе обнуляем
    const unsigned int br2_mi17 = FLAMEGPU->environment.getProperty<unsigned int>("mi17_br2_const");
    
    if (group_by == 2u && ppr < br2_mi17) {{
        // Комплектация без ремонта — PPR сохраняется
    }} else {{
        // Ремонт — PPR обнуляется
        FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
    }}
    
    FLAMEGPU->setVariable<unsigned int>("transition_1_to_2", 1u);
    FLAMEGPU->setVariable<unsigned short>("limiter", 0u);  // Будет вычислен
    FLAMEGPU->setVariable<unsigned int>("promoted", 0u);  // Сброс флага
    return flamegpu::ALIVE;
}}
"""

# ═══════════════════════════════════════════════════════════════════════════════
# СБРОС ФЛАГОВ (в начале каждого шага)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_RESET_FLAGS = """
FLAMEGPU_AGENT_FUNCTION(rtc_reset_flags_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Сброс флагов квотирования
    FLAMEGPU->setVariable<unsigned int>("needs_demote", 0u);
    FLAMEGPU->setVariable<unsigned int>("promoted", 0u);
    FLAMEGPU->setVariable<unsigned int>("repair_candidate", 0u);
    FLAMEGPU->setVariable<unsigned int>("repair_line_id", 0xFFFFFFFFu);
    FLAMEGPU->setVariable<unsigned int>("repair_line_day", 0xFFFFFFFFu);
    FLAMEGPU->setVariable<unsigned int>("debug_needs_demote", 0u);
    FLAMEGPU->setVariable<unsigned int>("debug_promoted", 0u);
    FLAMEGPU->setVariable<unsigned int>("debug_repair_candidate", 0u);
    FLAMEGPU->setVariable<unsigned int>("debug_repair_line_id", 0xFFFFFFFFu);
    FLAMEGPU->setVariable<unsigned int>("debug_repair_line_day", 0xFFFFFFFFu);
    
    // Сброс transition флагов
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_3", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_6", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_7", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_3_to_2", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_7_to_2", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_1_to_2", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_4_to_3", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_5_to_2", 0u);
    
    return flamegpu::ALIVE;
}
"""

