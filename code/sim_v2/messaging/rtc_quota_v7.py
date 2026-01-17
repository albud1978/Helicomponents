#!/usr/bin/env python3
"""
RTC модуль V7: Квотирование без intent

Архитектура:
- НЕТ промежуточной переменной intent_state
- Используем флаги:
  - needs_demote: агент должен выйти из operations (при избытке)
  - promoted: агент получил промоут (переход в operations)

Порядок:
1. Сброс буферов (MacroProperty)
2. Подсчёт агентов по состояниям
3. Демоут (если избыток в operations)
4. P1 промоут: serviceable → operations
5. P2 промоут: unserviceable → operations
6. P3 промоут: inactive → operations

Дата: 12.01.2026
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from model_build import RTC_MAX_FRAMES, MAX_DAYS

import pyflamegpu as fg


# ═══════════════════════════════════════════════════════════════════════════════
# СБРОС БУФЕРОВ
# ═══════════════════════════════════════════════════════════════════════════════

RTC_RESET_BUFFERS = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_reset_quota_v7, flamegpu::MessageNone, flamegpu::MessageNone) {{
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
FLAMEGPU_AGENT_FUNCTION(rtc_count_ops_v7, flamegpu::MessageNone, flamegpu::MessageNone) {{
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
FLAMEGPU_AGENT_FUNCTION(rtc_count_svc_v7, flamegpu::MessageNone, flamegpu::MessageNone) {{
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

RTC_COUNT_UNSVC = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_count_unsvc_v7, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    auto mp_result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
    unsigned int step_days = mp_result[0];
    if (step_days == 0u) step_days = 1u;
    const unsigned int safe_day = day + step_days;
    
    const unsigned int exit_date = FLAMEGPU->getVariable<unsigned int>("exit_date");
    const bool ready = (exit_date > 0u && exit_date != 0xFFFFFFFFu && exit_date <= safe_day);
    
    if (group_by == 1u) {{
        auto count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_unsvc_count");
        auto ready_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_unsvc_ready_count");
        count[idx].exchange(1u);
        if (ready) ready_count[idx].exchange(1u);
    }} else if (group_by == 2u) {{
        auto count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_unsvc_count");
        auto ready_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_unsvc_ready_count");
        count[idx].exchange(1u);
        if (ready) ready_count[idx].exchange(1u);
    }}
    return flamegpu::ALIVE;
}}
"""

RTC_COUNT_INACTIVE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_count_inactive_v7, flamegpu::MessageNone, flamegpu::MessageNone) {{
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
FLAMEGPU_AGENT_FUNCTION(rtc_demote_ops_v7, flamegpu::MessageNone, flamegpu::MessageNone) {{
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
    
    // Подсчёт текущих в operations (включая динамические спавны!)
    // КРИТИЧНО: используем RTC_MAX_FRAMES, а не frames_total, чтобы учесть спавненных агентов
    unsigned int curr = 0u;
    unsigned int target = 0u;
    
    if (group_by == 1u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_ops_count");
        for (unsigned int i = 0u; i < {RTC_MAX_FRAMES}u; ++i) {{
            if (ops_count[i] == 1u) ++curr;
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", safe_day);
    }} else if (group_by == 2u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_ops_count");
        for (unsigned int i = 0u; i < {RTC_MAX_FRAMES}u; ++i) {{
            if (ops_count[i] == 1u) ++curr;
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);
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
    }}
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# P1 ПРОМОУТ: serviceable → operations
# ═══════════════════════════════════════════════════════════════════════════════

RTC_PROMOTE_SVC = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_promote_svc_v7, flamegpu::MessageNone, flamegpu::MessageNone) {{
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
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", safe_day);
    }} else if (group_by == 2u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_svc_count");
        for (unsigned int i = 0u; i < {RTC_MAX_FRAMES}u; ++i) {{
            if (ops_count[i] == 1u) ++curr;
            if (svc_count[i] == 1u) ++svc_available;
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);
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
    }}
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# P2 ПРОМОУТ: unserviceable → operations
# ═══════════════════════════════════════════════════════════════════════════════

RTC_PROMOTE_UNSVC = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_promote_unsvc_v7, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // P2: unserviceable → operations (если дефицит после P1)
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    auto mp_result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
    unsigned int step_days = mp_result[0];
    if (step_days == 0u) step_days = 1u;
    unsigned int safe_day = day + step_days;
    if (safe_day >= days_total) safe_day = (days_total > 0u ? days_total - 1u : 0u);
    
    // КРИТИЧНО: Проверяем exit_date — агент должен отбыть repair_time перед возвратом в ops
    const unsigned int exit_date = FLAMEGPU->getVariable<unsigned int>("exit_date");
    if (exit_date > 0u && exit_date != 0xFFFFFFFFu && safe_day < exit_date) {{
        // Ещё не готов — ждём repair_time
        return flamegpu::ALIVE;
    }}
    
    // Подсчёт текущих в operations + P1 промоуты
    // КРИТИЧНО: используем RTC_MAX_FRAMES, чтобы учесть спавненных агентов
    unsigned int ops_curr = 0u;
    unsigned int svc_available = 0u;  // P1 промоутит всех svc → учитываем
    unsigned int unsvc_available = 0u;
    unsigned int target = 0u;
    
    // P2 КАСКАДНАЯ ЛОГИКА: дефицит = target - ops - svc (P1 промоутит всех svc)
    if (group_by == 1u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_svc_count");
        auto unsvc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_unsvc_ready_count");
        for (unsigned int i = 0u; i < {RTC_MAX_FRAMES}u; ++i) {{
            if (ops_count[i] == 1u) ++ops_curr;
            if (svc_count[i] == 1u) ++svc_available;
            if (unsvc_count[i] == 1u) ++unsvc_available;
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", safe_day);
    }} else if (group_by == 2u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_svc_count");
        auto unsvc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_unsvc_ready_count");
        for (unsigned int i = 0u; i < {RTC_MAX_FRAMES}u; ++i) {{
            if (ops_count[i] == 1u) ++ops_curr;
            if (svc_count[i] == 1u) ++svc_available;
            if (unsvc_count[i] == 1u) ++unsvc_available;
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);
    }} else {{
        return flamegpu::ALIVE;
    }}
    
    
    // P1 промоутит min(deficit_p1, svc_available)
    // P2 получает остаток: deficit_p2 = target - ops - min(deficit_p1, svc)
    unsigned int deficit_p1 = (target > ops_curr) ? (target - ops_curr) : 0u;
    unsigned int p1_will_promote = (deficit_p1 < svc_available) ? deficit_p1 : svc_available;
    unsigned int curr_after_p1 = ops_curr + p1_will_promote;
    
    
    // P2 дефицит (после P1)
    if (curr_after_p1 >= target) {{
        return flamegpu::ALIVE;
    }}
    
    unsigned int deficit = target - curr_after_p1;
    unsigned int K = (deficit < unsvc_available) ? deficit : unsvc_available;
    
    if (K == 0u) return flamegpu::ALIVE;
    
    // Ранжирование
    unsigned int rank = 0u;
    if (group_by == 1u) {{
        auto unsvc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_unsvc_ready_count");
        for (unsigned int i = 0u; i < idx; ++i) {{
            if (unsvc_count[i] == 1u) ++rank;
        }}
    }} else {{
        auto unsvc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_unsvc_ready_count");
        for (unsigned int i = 0u; i < idx; ++i) {{
            if (unsvc_count[i] == 1u) ++rank;
        }}
    }}
    
    if (rank < K) {{
        FLAMEGPU->setVariable<unsigned int>("promoted", 1u);
    }}
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# P3 ПРОМОУТ: inactive → operations
# ═══════════════════════════════════════════════════════════════════════════════

RTC_PROMOTE_INACTIVE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_promote_inactive_v7, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // P3: inactive → operations (если дефицит после P1+P2)
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    auto mp_result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
    unsigned int step_days = mp_result[0];
    if (step_days == 0u) step_days = 1u;
    unsigned int safe_day = day + step_days;
    if (safe_day >= days_total) safe_day = (days_total > 0u ? days_total - 1u : 0u);
    
    // P3 КАСКАДНАЯ ЛОГИКА: дефицит = target - ops - svc - unsvc (P1+P2 промоутят всех)
    // КРИТИЧНО: используем RTC_MAX_FRAMES, чтобы учесть спавненных агентов
    unsigned int ops_curr = 0u;
    unsigned int svc_available = 0u;
    unsigned int unsvc_available = 0u;
    unsigned int inactive_available = 0u;
    unsigned int target = 0u;
    
    if (group_by == 1u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_svc_count");
        auto unsvc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_unsvc_ready_count");
        auto inactive_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_inactive_count");
        for (unsigned int i = 0u; i < {RTC_MAX_FRAMES}u; ++i) {{
            if (ops_count[i] == 1u) ++ops_curr;
            if (svc_count[i] == 1u) ++svc_available;
            if (unsvc_count[i] == 1u) ++unsvc_available;
            if (inactive_count[i] == 1u) ++inactive_available;
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", safe_day);
    }} else if (group_by == 2u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_svc_count");
        auto unsvc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_unsvc_ready_count");
        auto inactive_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_inactive_count");
        for (unsigned int i = 0u; i < {RTC_MAX_FRAMES}u; ++i) {{
            if (ops_count[i] == 1u) ++ops_curr;
            if (svc_count[i] == 1u) ++svc_available;
            if (unsvc_count[i] == 1u) ++unsvc_available;
            if (inactive_count[i] == 1u) ++inactive_available;
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);
    }} else {{
        return flamegpu::ALIVE;
    }}
    
    
    // P1 промоутит min(deficit_p1, svc_available)
    unsigned int deficit_p1 = (target > ops_curr) ? (target - ops_curr) : 0u;
    unsigned int p1_will_promote = (deficit_p1 < svc_available) ? deficit_p1 : svc_available;
    unsigned int curr_after_p1 = ops_curr + p1_will_promote;
    
    // P2 промоутит min(deficit_p2, unsvc_available)
    unsigned int deficit_p2 = (target > curr_after_p1) ? (target - curr_after_p1) : 0u;
    unsigned int p2_will_promote = (deficit_p2 < unsvc_available) ? deficit_p2 : unsvc_available;
    unsigned int curr_after_p1p2 = curr_after_p1 + p2_will_promote;
    
    // P3 дефицит (после P1+P2)
    
    if (curr_after_p1p2 >= target) {{
        return flamegpu::ALIVE;
    }}
    
    unsigned int deficit = target - curr_after_p1p2;
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
    }}
    
    return flamegpu::ALIVE;
}}
"""

# ═══════════════════════════════════════════════════════════════════════════════
# P3 POST-КВОТА: дополнительный добор из inactive до target
# (использует ПОСТ-квотные буферы counts)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_PROMOTE_INACTIVE_POST = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_promote_inactive_post_v7, flamegpu::MessageNone, flamegpu::MessageNone) {{
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
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", safe_day);
    }} else if (group_by == 2u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_ops_count");
        auto inactive_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_inactive_count");
        for (unsigned int i = 0u; i < {RTC_MAX_FRAMES}u; ++i) {{
            if (ops_count[i] == 1u) ++ops_curr;
            if (inactive_count[i] == 1u) ++inactive_available;
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);
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
    }}
    
    return flamegpu::ALIVE;
}}
"""

COND_INACTIVE_PROMOTED_POST = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_inactive_promoted_post_v7) {
    return FLAMEGPU->getVariable<unsigned int>("promoted") == 1u;
}
"""

RTC_INACTIVE_TO_OPS_POST = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_inactive_to_ops_post_v7, flamegpu::MessageNone, flamegpu::MessageNone) {{
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
FLAMEGPU_AGENT_FUNCTION(rtc_reset_flags_v7, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Сброс флагов квотирования
    FLAMEGPU->setVariable<unsigned int>("needs_demote", 0u);
    FLAMEGPU->setVariable<unsigned int>("promoted", 0u);
    FLAMEGPU->setVariable<unsigned int>("repair_candidate", 0u);
    FLAMEGPU->setVariable<unsigned int>("repair_line_id", 0xFFFFFFFFu);
    FLAMEGPU->setVariable<unsigned int>("repair_line_day", 0xFFFFFFFFu);
    
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


# ═══════════════════════════════════════════════════════════════════════════════
# РЕГИСТРАЦИЯ
# ═══════════════════════════════════════════════════════════════════════════════

def register_quota_v7(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует квотирование V7 (без intent)"""
    print("\n📊 V7: Регистрация квотирования...")
    
    # MacroProperty exit_flags не используем
    
    # Сброс флагов в начале (все агенты во всех состояниях)
    layer_reset_flags = model.newLayer("v7_reset_flags")
    for state in ["inactive", "operations", "serviceable", "repair", "reserve", "storage", "unserviceable"]:
        fn_name = f"rtc_reset_flags_v7_{state}"
        fn = agent.newRTCFunction(fn_name, RTC_RESET_FLAGS)
        fn.setInitialState(state)
        fn.setEndState(state)
        layer_reset_flags.addAgentFunction(fn)
    print("  ✅ Сброс флагов")
    
    # Сброс буферов (idx=0 из ЛЮБОГО состояния)
    layer_reset_buf = model.newLayer("v7_reset_buffers")
    for state in ["inactive", "operations", "serviceable", "repair", "reserve", "storage", "unserviceable"]:
        fn_name = f"rtc_reset_quota_v7_{state}"
        fn = agent.newRTCFunction(fn_name, RTC_RESET_BUFFERS)
        fn.setInitialState(state)
        fn.setEndState(state)
        layer_reset_buf.addAgentFunction(fn)
    print("  ✅ Сброс буферов (все состояния)")
    
    # Подсчёт агентов
    layer_count = model.newLayer("v7_count_agents")
    
    fn = agent.newRTCFunction("rtc_count_ops_v7", RTC_COUNT_OPS)
    fn.setInitialState("operations")
    fn.setEndState("operations")
    layer_count.addAgentFunction(fn)
    
    fn = agent.newRTCFunction("rtc_count_svc_v7", RTC_COUNT_SVC)
    fn.setInitialState("serviceable")
    fn.setEndState("serviceable")
    layer_count.addAgentFunction(fn)
    
    fn = agent.newRTCFunction("rtc_count_unsvc_v7", RTC_COUNT_UNSVC)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")
    layer_count.addAgentFunction(fn)
    
    fn = agent.newRTCFunction("rtc_count_inactive_v7", RTC_COUNT_INACTIVE)
    fn.setInitialState("inactive")
    fn.setEndState("inactive")
    layer_count.addAgentFunction(fn)
    
    print("  ✅ Подсчёт агентов")
    
    # Демоут
    layer_demote = model.newLayer("v7_demote")
    fn = agent.newRTCFunction("rtc_demote_ops_v7", RTC_DEMOTE_OPS)
    fn.setInitialState("operations")
    fn.setEndState("operations")
    layer_demote.addAgentFunction(fn)
    print("  ✅ Демоут")
    
    # P1: serviceable → operations
    layer_p1 = model.newLayer("v7_promote_svc")
    fn = agent.newRTCFunction("rtc_promote_svc_v7", RTC_PROMOTE_SVC)
    fn.setInitialState("serviceable")
    fn.setEndState("serviceable")
    layer_p1.addAgentFunction(fn)
    print("  ✅ P1 промоут (serviceable)")
    
    # P2: unserviceable → operations
    layer_p2 = model.newLayer("v7_promote_unsvc")
    fn = agent.newRTCFunction("rtc_promote_unsvc_v7", RTC_PROMOTE_UNSVC)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")
    layer_p2.addAgentFunction(fn)
    print("  ✅ P2 промоут (unserviceable)")
    
    # P3: inactive → operations
    layer_p3 = model.newLayer("v7_promote_inactive")
    fn = agent.newRTCFunction("rtc_promote_inactive_v7", RTC_PROMOTE_INACTIVE)
    fn.setInitialState("inactive")
    fn.setEndState("inactive")
    layer_p3.addAgentFunction(fn)
    print("  ✅ P3 промоут (inactive)")
    
    print("✅ Квотирование V7 зарегистрировано\n")


def register_post_quota_counts_v7(model: fg.ModelDescription, agent: fg.AgentDescription):
    """
    Пересчёт буферов после пост-квотных переходов.
    Нужно для корректного динамического спавна: он должен видеть актуальные counts.
    """
    print("\n📊 V7: Пересчёт буферов ПОСЛЕ квотирования...")
    
    # Сброс буферов (idx=0 из ЛЮБОГО состояния)
    layer_reset_buf = model.newLayer("v7_reset_buffers_post_quota")
    for state in ["inactive", "operations", "serviceable", "repair", "reserve", "storage", "unserviceable"]:
        fn_name = f"rtc_reset_quota_v7_post_{state}"
        fn = agent.newRTCFunction(fn_name, RTC_RESET_BUFFERS)
        fn.setInitialState(state)
        fn.setEndState(state)
        layer_reset_buf.addAgentFunction(fn)
    print("  ✅ Сброс буферов (post)")
    
    # Подсчёт агентов
    layer_count = model.newLayer("v7_count_agents_post_quota")
    
    fn = agent.newRTCFunction("rtc_count_ops_v7_post", RTC_COUNT_OPS)
    fn.setInitialState("operations")
    fn.setEndState("operations")
    layer_count.addAgentFunction(fn)
    
    fn = agent.newRTCFunction("rtc_count_svc_v7_post", RTC_COUNT_SVC)
    fn.setInitialState("serviceable")
    fn.setEndState("serviceable")
    layer_count.addAgentFunction(fn)
    
    fn = agent.newRTCFunction("rtc_count_unsvc_v7_post", RTC_COUNT_UNSVC)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")
    layer_count.addAgentFunction(fn)
    
    fn = agent.newRTCFunction("rtc_count_inactive_v7_post", RTC_COUNT_INACTIVE)
    fn.setInitialState("inactive")
    fn.setEndState("inactive")
    layer_count.addAgentFunction(fn)
    
    print("  ✅ Подсчёт агентов (post)")

    # Доп. добор до target после пост-квотных переходов
    layer_promote_post = model.newLayer("v7_promote_inactive_post")
    fn = agent.newRTCFunction("rtc_promote_inactive_post_v7", RTC_PROMOTE_INACTIVE_POST)
    fn.setInitialState("inactive")
    fn.setEndState("inactive")
    layer_promote_post.addAgentFunction(fn)
    
    layer_to_ops_post = model.newLayer("v7_inactive_to_ops_post")
    fn = agent.newRTCFunction("rtc_inactive_to_ops_post_v7", RTC_INACTIVE_TO_OPS_POST)
    fn.setRTCFunctionCondition(COND_INACTIVE_PROMOTED_POST)
    fn.setInitialState("inactive")
    fn.setEndState("operations")
    layer_to_ops_post.addAgentFunction(fn)
    
    print("  ✅ Доп. добор из inactive (post)")
    print("✅ Post-quota пересчёт зарегистрирован\n")

