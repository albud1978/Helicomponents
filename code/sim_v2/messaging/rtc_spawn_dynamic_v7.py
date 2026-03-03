#!/usr/bin/env python3
"""
RTC Spawn Dynamic V7 — Динамический спавн для V7 архитектуры

Логика:
- После P3 промоута проверяем остаточный дефицит Mi-17
- Если дефицит > 0 и день >= repair_time → создаём новых агентов
- Агенты создаются СРАЗУ в operations (немедленное покрытие)

Отличия от baseline rtc_spawn_dynamic.py:
- Не использует intent_state
- Использует V7 count буферы (mi17_ops_count, mi17_svc_count, etc.)
- Учитывает промоутнутых через promoted флаг
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from model_build import RTC_MAX_FRAMES, MAX_DAYS

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")

from string import Template

CUMSUM_SIZE = RTC_MAX_FRAMES * (MAX_DAYS + 1)

DEVICE_FN_COMPUTE_LIMITER = """
FLAMEGPU_DEVICE_FUNCTION unsigned short compute_limiter_inline(
    flamegpu::DeviceAPI<flamegpu::MessageNone, flamegpu::MessageNone>* FLAMEGPU,
    const unsigned int sne,
    const unsigned int ppr,
    const unsigned int ll,
    const unsigned int oh,
    const unsigned int idx,
    const unsigned int current_day
) {
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    unsigned int remaining_ll = (sne < ll) ? (ll - sne) : 0u;
    unsigned int remaining_oh = (ppr < oh) ? (oh - ppr) : 0u;
    
    if (remaining_ll == 0u || remaining_oh == 0u) {
        return 0u;
    }
    
    auto cumsum = FLAMEGPU->environment.getMacroProperty<unsigned int, __CUMSUM_SIZE__u>("mp5_cumsum");
    const unsigned int base_cumsum = cumsum[current_day * frames + idx];
    
    unsigned int days_to_oh = end_day - current_day;
    bool found_oh = false;
    {
        unsigned int lo = current_day + 1u;
        unsigned int hi = end_day;
        while (lo < hi) {
            unsigned int mid = (lo + hi) / 2u;
            unsigned int accumulated = cumsum[mid * frames + idx] - base_cumsum;
            if (accumulated >= remaining_oh) {
                hi = mid;
            } else {
                lo = mid + 1u;
            }
        }
        if (lo <= end_day) {
            unsigned int final_accumulated = cumsum[lo * frames + idx] - base_cumsum;
            if (final_accumulated >= remaining_oh) {
                days_to_oh = lo - current_day;
                found_oh = true;
            }
        }
    }
    if (!found_oh) {
        days_to_oh = (end_day - current_day) + 1u;
    }
    
    unsigned int days_to_ll = end_day - current_day;
    bool found_ll = false;
    {
        unsigned int lo = current_day + 1u;
        unsigned int hi = end_day;
        while (lo < hi) {
            unsigned int mid = (lo + hi) / 2u;
            unsigned int accumulated = cumsum[mid * frames + idx] - base_cumsum;
            if (accumulated >= remaining_ll) {
                hi = mid;
            } else {
                lo = mid + 1u;
            }
        }
        if (lo <= end_day) {
            unsigned int final_accumulated = cumsum[lo * frames + idx] - base_cumsum;
            if (final_accumulated >= remaining_ll) {
                days_to_ll = lo - current_day;
                found_ll = true;
            }
        }
    }
    if (!found_ll) {
        days_to_ll = (end_day - current_day) + 1u;
    }
    
    unsigned int limiter = (days_to_oh < days_to_ll) ? days_to_oh : days_to_ll;
    
    if (limiter > 65535u) limiter = 65535u;
    if (limiter == 0u) limiter = 1u;
    
    return (unsigned short)limiter;
}

FLAMEGPU_DEVICE_FUNCTION unsigned short compute_limiter_inline(
    flamegpu::DeviceAPI<flamegpu::MessageNone, flamegpu::MessageNone>* FLAMEGPU
) {
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    
    return compute_limiter_inline(FLAMEGPU, sne, ppr, ll, oh, idx, current_day);
}
"""
DEVICE_FN_COMPUTE_LIMITER = DEVICE_FN_COMPUTE_LIMITER.replace("__CUMSUM_SIZE__", str(CUMSUM_SIZE))

# ═══════════════════════════════════════════════════════════════════════════════
# RTC ФУНКЦИИ ДИНАМИЧЕСКОГО СПАВНА
# ═══════════════════════════════════════════════════════════════════════════════

RTC_SPAWN_DYNAMIC_MGR_V7 = Template("""
FLAMEGPU_AGENT_FUNCTION(rtc_spawn_dynamic_mgr_v7, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    auto mp_result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
    unsigned int step_days = mp_result[0];
    if (step_days == 0u) step_days = 1u;
    unsigned int safe_day = day + step_days;
    if (safe_day >= days_total) safe_day = (days_total > 0u ? days_total - 1u : 0u);
    const unsigned int target_day = safe_day;
    const unsigned int write_day = safe_day;
    
    // Условие активации: day >= repair_time
    const unsigned int repair_time = FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    if (day < repair_time) {
        return flamegpu::ALIVE;
    }
    
    // Считаем текущее количество Mi-17 в operations
    // КРИТИЧНО: используем MAX_FRAMES чтобы учесть динамически спавненных агентов!
    auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_FRAMES}u>("mi17_ops_count");
    unsigned int curr_ops = 0u;
    for (unsigned int i = 0u; i < ${MAX_FRAMES}u; ++i) {
        if (ops_count[i] == 1u) ++curr_ops;
    }
    // NOTE: выходы из ops обрабатываются общим квотированием
    
    // Считаем промоутнутых P1 (serviceable)
    auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_FRAMES}u>("mi17_svc_count");
    unsigned int svc_available = 0u;
    for (unsigned int i = 0u; i < ${MAX_FRAMES}u; ++i) {
        if (svc_count[i] == 1u) ++svc_available;
    }
    
    // Считаем промоутнутых P2 (unserviceable)
    auto unsvc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_FRAMES}u>("mi17_unsvc_ready_count");
    unsigned int unsvc_available = 0u;
    for (unsigned int i = 0u; i < ${MAX_FRAMES}u; ++i) {
        if (unsvc_count[i] == 1u) ++unsvc_available;
    }
    
    // Считаем промоутнутых P3 (inactive)
    auto inactive_count = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_FRAMES}u>("mi17_inactive_count");
    unsigned int inactive_available = 0u;
    for (unsigned int i = 0u; i < ${MAX_FRAMES}u; ++i) {
        if (inactive_count[i] == 1u) ++inactive_available;
    }
    
    // Целевое значение из MP4
    const unsigned int target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", target_day);
    
    // Каскадный расчёт: сколько промоутит P1, P2, P3
    unsigned int deficit_p1 = (target > curr_ops) ? (target - curr_ops) : 0u;
    unsigned int p1_will = (deficit_p1 < svc_available) ? deficit_p1 : svc_available;
    unsigned int after_p1 = curr_ops + p1_will;
    
    unsigned int deficit_p2 = (target > after_p1) ? (target - after_p1) : 0u;
    unsigned int p2_will = (deficit_p2 < unsvc_available) ? deficit_p2 : unsvc_available;
    unsigned int after_p2 = after_p1 + p2_will;
    
    unsigned int deficit_p3 = (target > after_p2) ? (target - after_p2) : 0u;
    unsigned int p3_will = (deficit_p3 < inactive_available) ? deficit_p3 : inactive_available;
    unsigned int after_p3 = after_p2 + p3_will;
    
    // Остаточный дефицит после всех промоутов
    if (after_p3 >= target) {
        return flamegpu::ALIVE;
    }
    
    unsigned int deficit = target - after_p3;
    
    // Курсоры
    unsigned int next_idx = FLAMEGPU->getVariable<unsigned int>("next_idx");
    unsigned int next_acn = FLAMEGPU->getVariable<unsigned int>("next_acn");
    unsigned int total_spawned = FLAMEGPU->getVariable<unsigned int>("total_spawned");
    const unsigned int dynamic_reserve = FLAMEGPU->environment.getProperty<unsigned int>("dynamic_reserve_mi17");
    
    // Доступный резерв
    unsigned int available = (total_spawned < dynamic_reserve) ? (dynamic_reserve - total_spawned) : 0u;
    unsigned int need = (deficit < available) ? deficit : available;
    
    if (need == 0u) {
        return flamegpu::ALIVE;
    }
    
    // Публикуем параметры в MacroProperty
    auto need_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_need");
    auto bidx_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_idx");
    auto bacn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_acn");
    
    need_mp[write_day].exchange(need);
    bidx_mp[write_day].exchange(next_idx);
    bacn_mp[write_day].exchange(next_acn);
    
    
    // Сдвигаем курсоры
    FLAMEGPU->setVariable<unsigned int>("next_idx", next_idx + need);
    FLAMEGPU->setVariable<unsigned int>("next_acn", next_acn + need);
    FLAMEGPU->setVariable<unsigned int>("total_spawned", total_spawned + need);
    
    return flamegpu::ALIVE;
}
""").substitute(MAX_FRAMES=RTC_MAX_FRAMES, MAX_DAYS=MAX_DAYS)

RTC_SPAWN_DYNAMIC_TICKET_V8 = Template(DEVICE_FN_COMPUTE_LIMITER + """
FLAMEGPU_AGENT_FUNCTION(rtc_spawn_dynamic_ticket_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
    const unsigned int prev_day = FLAMEGPU->environment.getProperty<unsigned int>("prev_day");
    const unsigned int ticket = FLAMEGPU->getVariable<unsigned int>("ticket");
    
    // Читаем параметры
    auto need_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_need");
    auto bidx_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_idx");
    auto bacn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_acn");
    
    const unsigned int need = need_mp[safe_day];
    const unsigned int base_idx = bidx_mp[safe_day];
    const unsigned int base_acn = bacn_mp[safe_day];
    
    if (ticket >= need) {
        return flamegpu::ALIVE;
    }
    
    // Создаём нового агента
    const unsigned int new_idx = base_idx + ticket;
    const unsigned int new_acn = base_acn + ticket;
    
    // Нормативы Mi-17
    const unsigned int ll = FLAMEGPU->environment.getProperty<unsigned int>("mi17_ll_const");
    const unsigned int oh = FLAMEGPU->environment.getProperty<unsigned int>("mi17_oh_const");
    const unsigned int br = FLAMEGPU->environment.getProperty<unsigned int>("mi17_br_const");
    const unsigned int repair_time = FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    
    // Начальная наработка (новый вертолёт)
    const unsigned int sne_new = 0u;
    const unsigned int ppr_new = 0u;
    
    // Устанавливаем переменные агента
    FLAMEGPU->agent_out.setVariable<unsigned int>("idx", new_idx);
    FLAMEGPU->agent_out.setVariable<unsigned int>("aircraft_number", new_acn);
    FLAMEGPU->agent_out.setVariable<unsigned int>("partseqno_i", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("group_by", 2u);  // Mi-17
    
    FLAMEGPU->agent_out.setVariable<unsigned int>("sne", sne_new);
    FLAMEGPU->agent_out.setVariable<unsigned int>("ppr", ppr_new);
    FLAMEGPU->agent_out.setVariable<unsigned int>("cso", 0u);
    
    FLAMEGPU->agent_out.setVariable<unsigned int>("ll", ll);
    FLAMEGPU->agent_out.setVariable<unsigned int>("oh", oh);
    FLAMEGPU->agent_out.setVariable<unsigned int>("br", br);
    
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_time", repair_time);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_days", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("status_change_day", day);
    FLAMEGPU->agent_out.setVariable<unsigned int>("status_id", 2u);  // operations
    FLAMEGPU->agent_out.setVariable<unsigned int>("pre_status_id", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("commit_p1", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("commit_p2", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("commit_p3", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("promoted", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("needs_demote", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("decision_p2", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("decision_p3", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_candidate", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_line_id", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_line_day", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_claim_start_day", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_claim_end_day", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_claim_source", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("debug_promoted", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("debug_needs_demote", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("debug_repair_candidate", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("debug_repair_line_id", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("debug_repair_line_day", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("debug_bucket_seen", 0u);
    
    // DISABLED (state5-unused): FLAMEGPU->agent_out.setVariable<unsigned int>("transition_5_to_2", 1u);
    FLAMEGPU->agent_out.setVariable<unsigned short>(
        "limiter",
        compute_limiter_inline(
            FLAMEGPU,
            sne_new,
            ppr_new,
            ll,
            oh,
            new_idx,
            FLAMEGPU->environment.getProperty<unsigned int>("current_day")
        )
    );
    
    return flamegpu::ALIVE;
}
""").substitute(MAX_DAYS=MAX_DAYS)

RTC_SPAWN_DYNAMIC_MGR_V8 = Template("""
FLAMEGPU_AGENT_FUNCTION(rtc_spawn_dynamic_mgr_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int target_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
    const unsigned int write_day = target_day;
    
    // Условие активации: day >= repair_time
    const unsigned int repair_time = FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    if (day < repair_time) {
        return flamegpu::ALIVE;
    }
    
    // Целевые значения из MP4 (текущий день)
    const unsigned int target_mi8 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", target_day);
    const unsigned int target_mi17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", target_day);
    // Используем пересчитанные ops после post-квотных переходов
    auto ops_mi8 = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_FRAMES}u>("mi8_ops_count");
    auto ops_mi17 = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_FRAMES}u>("mi17_ops_count");
    unsigned int curr_ops_mi8 = 0u;
    unsigned int curr_ops_mi17 = 0u;
    for (unsigned int i = 0u; i < ${MAX_FRAMES}u; ++i) {
        curr_ops_mi8 += ops_mi8[i];
        curr_ops_mi17 += ops_mi17[i];
    }
    
    unsigned int deficit_mi8 = 0u;
    unsigned int deficit_mi17 = 0u;
    // FIX: curr_ops уже включает промоуты P1/P2/P3 (layers 35-38), used не нужен
    if (target_mi8 > curr_ops_mi8) {
        deficit_mi8 = target_mi8 - curr_ops_mi8;
    }
    if (target_mi17 > curr_ops_mi17) {
        deficit_mi17 = target_mi17 - curr_ops_mi17;
    }
    
    FLAMEGPU->setVariable<unsigned int>("debug_curr_ops", curr_ops_mi17);
    FLAMEGPU->setVariable<unsigned int>("debug_target", target_mi17);
    FLAMEGPU->setVariable<unsigned int>("debug_need", deficit_mi17);
    FLAMEGPU->setVariable<unsigned int>("debug_curr_ops_mi8", curr_ops_mi8);
    FLAMEGPU->setVariable<unsigned int>("debug_target_mi8", target_mi8);
    FLAMEGPU->setVariable<unsigned int>("debug_need_mi8", deficit_mi8);
    
    const unsigned int repair_time_mi8 = FLAMEGPU->environment.getProperty<unsigned int>("mi8_repair_time_const");
    const unsigned int repair_time_mi17 = FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    if (day < repair_time_mi8) deficit_mi8 = 0u;
    if (day < repair_time_mi17) deficit_mi17 = 0u;
    
    // Курсоры Mi-17
    unsigned int next_idx_17 = FLAMEGPU->getVariable<unsigned int>("next_idx_mi17");
    unsigned int next_acn_17 = FLAMEGPU->getVariable<unsigned int>("next_acn_mi17");
    unsigned int total_spawned_17 = FLAMEGPU->getVariable<unsigned int>("total_spawned_mi17");
    const unsigned int dynamic_reserve_17 = FLAMEGPU->environment.getProperty<unsigned int>("dynamic_reserve_mi17");
    unsigned int available_17 = (total_spawned_17 < dynamic_reserve_17) ? (dynamic_reserve_17 - total_spawned_17) : 0u;
    unsigned int need_17 = (deficit_mi17 < available_17) ? deficit_mi17 : available_17;
    
    if (need_17 > 0u) {
        auto need_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_need");
        auto bidx_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_idx");
        auto bacn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_acn");
        
        need_mp[write_day].exchange(need_17);
        bidx_mp[write_day].exchange(next_idx_17);
        bacn_mp[write_day].exchange(next_acn_17);
        
        FLAMEGPU->setVariable<unsigned int>("next_idx_mi17", next_idx_17 + need_17);
        FLAMEGPU->setVariable<unsigned int>("next_acn_mi17", next_acn_17 + need_17);
        FLAMEGPU->setVariable<unsigned int>("total_spawned_mi17", total_spawned_17 + need_17);
    }
    
    // Курсоры Mi-8
    unsigned int next_idx_8 = FLAMEGPU->getVariable<unsigned int>("next_idx_mi8");
    unsigned int next_acn_8 = FLAMEGPU->getVariable<unsigned int>("next_acn_mi8");
    unsigned int total_spawned_8 = FLAMEGPU->getVariable<unsigned int>("total_spawned_mi8");
    const unsigned int dynamic_reserve_8 = FLAMEGPU->environment.getProperty<unsigned int>("dynamic_reserve_mi8");
    unsigned int available_8 = (total_spawned_8 < dynamic_reserve_8) ? (dynamic_reserve_8 - total_spawned_8) : 0u;
    unsigned int need_8 = (deficit_mi8 < available_8) ? deficit_mi8 : available_8;
    
    if (need_8 > 0u) {
        auto need_mp_8 = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_need_mi8");
        auto bidx_mp_8 = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_idx_mi8");
        auto bacn_mp_8 = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_acn_mi8");
        
        need_mp_8[write_day].exchange(need_8);
        bidx_mp_8[write_day].exchange(next_idx_8);
        bacn_mp_8[write_day].exchange(next_acn_8);
        
        FLAMEGPU->setVariable<unsigned int>("next_idx_mi8", next_idx_8 + need_8);
        FLAMEGPU->setVariable<unsigned int>("next_acn_mi8", next_acn_8 + need_8);
        FLAMEGPU->setVariable<unsigned int>("total_spawned_mi8", total_spawned_8 + need_8);
    }
    
    return flamegpu::ALIVE;
}
""").substitute(MAX_FRAMES=RTC_MAX_FRAMES, MAX_DAYS=MAX_DAYS)

RTC_SPAWN_DYNAMIC_TICKET_V8_MI8 = Template(DEVICE_FN_COMPUTE_LIMITER + """
FLAMEGPU_AGENT_FUNCTION(rtc_spawn_dynamic_ticket_v8_mi8, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
    const unsigned int prev_day = FLAMEGPU->environment.getProperty<unsigned int>("prev_day");
    const unsigned int ticket = FLAMEGPU->getVariable<unsigned int>("ticket");
    
    // Читаем параметры
    auto need_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_need_mi8");
    auto bidx_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_idx_mi8");
    auto bacn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_acn_mi8");
    
    const unsigned int need = need_mp[safe_day];
    const unsigned int base_idx = bidx_mp[safe_day];
    const unsigned int base_acn = bacn_mp[safe_day];
    
    if (ticket >= need) {
        return flamegpu::ALIVE;
    }
    
    // Создаём нового агента
    const unsigned int new_idx = base_idx + ticket;
    const unsigned int new_acn = base_acn + ticket;
    
    // Нормативы Mi-8
    const unsigned int ll = FLAMEGPU->environment.getProperty<unsigned int>("mi8_ll_const");
    const unsigned int oh = FLAMEGPU->environment.getProperty<unsigned int>("mi8_oh_const");
    const unsigned int br = FLAMEGPU->environment.getProperty<unsigned int>("mi8_br_const");
    const unsigned int repair_time = FLAMEGPU->environment.getProperty<unsigned int>("mi8_repair_time_const");
    
    // Начальная наработка (новый вертолёт)
    const unsigned int sne = 0u;
    const unsigned int ppr = 0u;
    
    // Создаём агента (state=operations)
    FLAMEGPU->agent_out.setVariable<unsigned int>("idx", new_idx);
    FLAMEGPU->agent_out.setVariable<unsigned int>("aircraft_number", new_acn);
    FLAMEGPU->agent_out.setVariable<unsigned int>("group_by", 1u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("sne", sne);
    FLAMEGPU->agent_out.setVariable<unsigned int>("ppr", ppr);
    FLAMEGPU->agent_out.setVariable<unsigned int>("ll", ll);
    FLAMEGPU->agent_out.setVariable<unsigned int>("oh", oh);
    FLAMEGPU->agent_out.setVariable<unsigned int>("br", br);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_time", repair_time);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_days", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("status_change_day", day);
    FLAMEGPU->agent_out.setVariable<unsigned int>("status_id", 2u);  // operations
    FLAMEGPU->agent_out.setVariable<unsigned int>("pre_status_id", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("commit_p1", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("commit_p2", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("commit_p3", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("promoted", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("needs_demote", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("decision_p2", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("decision_p3", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_candidate", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_line_id", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_line_day", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_claim_start_day", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_claim_end_day", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_claim_source", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("debug_promoted", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("debug_needs_demote", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("debug_repair_candidate", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("debug_repair_line_id", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("debug_repair_line_day", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("debug_bucket_seen", 0u);
    
    // DISABLED (state5-unused): FLAMEGPU->agent_out.setVariable<unsigned int>("transition_5_to_2", 1u);
    FLAMEGPU->agent_out.setVariable<unsigned short>(
        "limiter",
        compute_limiter_inline(
            FLAMEGPU,
            sne,
            ppr,
            ll,
            oh,
            new_idx,
            FLAMEGPU->environment.getProperty<unsigned int>("current_day")
        )
    );
    
    return flamegpu::ALIVE;
}
""").substitute(MAX_DAYS=MAX_DAYS)

RTC_SPAWN_DYNAMIC_TICKET_V7 = Template("""
FLAMEGPU_AGENT_FUNCTION(rtc_spawn_dynamic_ticket_v7, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
    const unsigned int ticket = FLAMEGPU->getVariable<unsigned int>("ticket");
    
    // Читаем параметры
    auto need_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_need");
    auto bidx_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_idx");
    auto bacn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_acn");
    
    const unsigned int need = need_mp[safe_day];
    const unsigned int base_idx = bidx_mp[safe_day];
    const unsigned int base_acn = bacn_mp[safe_day];
    
    if (ticket >= need) {
        return flamegpu::ALIVE;
    }
    
    // Создаём нового агента
    const unsigned int new_idx = base_idx + ticket;
    const unsigned int new_acn = base_acn + ticket;
    
    // Нормативы Mi-17
    const unsigned int ll = FLAMEGPU->environment.getProperty<unsigned int>("mi17_ll_const");
    const unsigned int oh = FLAMEGPU->environment.getProperty<unsigned int>("mi17_oh_const");
    const unsigned int br = FLAMEGPU->environment.getProperty<unsigned int>("mi17_br_const");
    const unsigned int repair_time = FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    
    // Начальная наработка (новый вертолёт)
    const unsigned int sne_new = 0u;
    const unsigned int ppr_new = 0u;
    
    // Устанавливаем переменные агента
    FLAMEGPU->agent_out.setVariable<unsigned int>("idx", new_idx);
    FLAMEGPU->agent_out.setVariable<unsigned int>("aircraft_number", new_acn);
    FLAMEGPU->agent_out.setVariable<unsigned int>("partseqno_i", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("group_by", 2u);  // Mi-17
    
    FLAMEGPU->agent_out.setVariable<unsigned int>("sne", sne_new);
    FLAMEGPU->agent_out.setVariable<unsigned int>("ppr", ppr_new);
    FLAMEGPU->agent_out.setVariable<unsigned int>("cso", 0u);
    
    FLAMEGPU->agent_out.setVariable<unsigned int>("ll", ll);
    FLAMEGPU->agent_out.setVariable<unsigned int>("oh", oh);
    FLAMEGPU->agent_out.setVariable<unsigned int>("br", br);
    
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_time", repair_time);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_days", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("exit_date", 0u);
    
    // V7 флаги
    FLAMEGPU->agent_out.setVariable<unsigned int>("promoted", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("needs_demote", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("commit_p1", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("commit_p2", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("commit_p3", 0u);
    
    // Limiter (будет вычислен в limiter_on_entry)
    FLAMEGPU->agent_out.setVariable<unsigned short>("limiter", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("computed_adaptive_days", 1u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("daily_today_u32", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("daily_next_u32", 0u);
    
    // Дополнительные переменные
    FLAMEGPU->agent_out.setVariable<unsigned int>("status_id", 2u);  // operations
    FLAMEGPU->agent_out.setVariable<unsigned int>("intent_state", 2u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("bi_counter", 1u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("mfg_date", day);
    FLAMEGPU->agent_out.setVariable<unsigned int>("second_ll", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("assembly_time", 30u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("partout_time", 20u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("assembly_trigger", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("active_trigger", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("partout_trigger", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("s4_days", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("limiter_date", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("prev_intent", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("status_change_day", day);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_candidate", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_line_id", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_line_day", 0xFFFFFFFFu);
    
    // Transitions (все 0) - только те что есть в модели
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_0_to_2", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_2_to_3", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_2_to_6", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_2_to_7", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_3_to_2", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_4_to_3", 0u);
    // DISABLED (state5-unused): FLAMEGPU->agent_out.setVariable<unsigned int>("transition_5_to_2", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_7_to_2", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_1_to_2", 0u);
    
    return flamegpu::ALIVE;
}
""").substitute(MAX_DAYS=MAX_DAYS)


def register_spawn_dynamic_v7(model: fg.ModelDescription, heli_agent: fg.AgentDescription, env_data: dict):
    """Регистрация динамического спавна V7"""
    print("\n📦 V7: Динамический спавн...")
    
    env = model.Environment()
    
    # Параметры
    first_dynamic_idx = env_data.get('first_dynamic_idx', 340)  # После всех существующих агентов
    dynamic_reserve_mi17 = env_data.get('dynamic_reserve_mi17', 50)
    base_acn_spawn = env_data.get('base_acn_spawn', 100000)
    
    env.newPropertyUInt("first_dynamic_idx", first_dynamic_idx)
    env.newPropertyUInt("dynamic_reserve_mi17", dynamic_reserve_mi17)
    
    # MacroProperty для параметров спавна
    env.newMacroPropertyUInt("spawn_dynamic_need", MAX_DAYS)
    env.newMacroPropertyUInt("spawn_dynamic_base_idx", MAX_DAYS)
    env.newMacroPropertyUInt("spawn_dynamic_base_acn", MAX_DAYS)
    
    # Агент-менеджер
    spawn_mgr = model.newAgent("SpawnDynamicMgr")
    spawn_mgr.newState("default")
    spawn_mgr.newVariableUInt("next_idx", first_dynamic_idx)
    spawn_mgr.newVariableUInt("next_acn", base_acn_spawn)
    spawn_mgr.newVariableUInt("total_spawned", 0)
    spawn_mgr.newVariableUInt("debug_curr_ops", 0)
    spawn_mgr.newVariableUInt("debug_target", 0)
    spawn_mgr.newVariableUInt("debug_need", 0)
    
    # Агенты-тикеты
    spawn_ticket = model.newAgent("SpawnDynamicTicket")
    spawn_ticket.newState("default")
    spawn_ticket.newVariableUInt("ticket", 0)
    
    # RTC функции
    mgr_fn = spawn_mgr.newRTCFunction("rtc_spawn_dynamic_mgr_v7", RTC_SPAWN_DYNAMIC_MGR_V7)
    mgr_fn.setInitialState("default")
    mgr_fn.setEndState("default")
    
    ticket_fn = spawn_ticket.newRTCFunction("rtc_spawn_dynamic_ticket_v7", RTC_SPAWN_DYNAMIC_TICKET_V7)
    ticket_fn.setAgentOutput(heli_agent, "operations")  # Создаём сразу в operations
    ticket_fn.setInitialState("default")
    ticket_fn.setEndState("default")
    
    # Слои (после P3 промоута)
    layer_mgr = model.newLayer("v7_spawn_dynamic_mgr")
    layer_mgr.addAgentFunction(mgr_fn)
    
    layer_ticket = model.newLayer("v7_spawn_dynamic_ticket")
    layer_ticket.addAgentFunction(ticket_fn)
    
    print(f"  ✅ Менеджер: first_idx={first_dynamic_idx}, reserve={dynamic_reserve_mi17}")
    print(f"  ✅ Слои: v7_spawn_dynamic_mgr, v7_spawn_dynamic_ticket")
    
    return {
        'mgr_agent': spawn_mgr,
        'ticket_agent': spawn_ticket,
        'first_dynamic_idx': first_dynamic_idx,
        'dynamic_reserve': dynamic_reserve_mi17,
        'base_acn': base_acn_spawn
    }


def register_spawn_dynamic_v8(model: fg.ModelDescription, heli_agent: fg.AgentDescription, env_data: dict):
    """Регистрация динамического спавна V8 (с учётом RepairLine слотов)"""
    print("\n📦 V8: Динамический спавн...")
    
    env = model.Environment()
    
    first_dynamic_idx = env_data.get('first_dynamic_idx', 340)
    dynamic_reserve_mi17 = env_data.get('dynamic_reserve_mi17', 50)
    dynamic_reserve_mi8 = env_data.get('dynamic_reserve_mi8', 8)
    first_dynamic_idx_mi17 = env_data.get('first_dynamic_idx_mi17', first_dynamic_idx)
    first_dynamic_idx_mi8 = env_data.get('first_dynamic_idx_mi8', first_dynamic_idx_mi17 + dynamic_reserve_mi17)
    base_acn_spawn_mi17 = env_data.get('base_acn_spawn_mi17', env_data.get('base_acn_spawn', 100000))
    base_acn_spawn_mi8 = env_data.get('base_acn_spawn_mi8', base_acn_spawn_mi17 + dynamic_reserve_mi17)
    
    env.newPropertyUInt("first_dynamic_idx", first_dynamic_idx)
    env.newPropertyUInt("dynamic_reserve_mi17", dynamic_reserve_mi17)
    env.newPropertyUInt("dynamic_reserve_mi8", dynamic_reserve_mi8)
    
    env.newMacroPropertyUInt("spawn_dynamic_need", MAX_DAYS)
    env.newMacroPropertyUInt("spawn_dynamic_base_idx", MAX_DAYS)
    env.newMacroPropertyUInt("spawn_dynamic_base_acn", MAX_DAYS)
    env.newMacroPropertyUInt("spawn_dynamic_need_mi8", MAX_DAYS)
    env.newMacroPropertyUInt("spawn_dynamic_base_idx_mi8", MAX_DAYS)
    env.newMacroPropertyUInt("spawn_dynamic_base_acn_mi8", MAX_DAYS)
    
    spawn_mgr = model.newAgent("SpawnDynamicMgr")
    spawn_mgr.newState("default")
    spawn_mgr.newVariableUInt("next_idx_mi17", first_dynamic_idx_mi17)
    spawn_mgr.newVariableUInt("next_acn_mi17", base_acn_spawn_mi17)
    spawn_mgr.newVariableUInt("total_spawned_mi17", 0)
    spawn_mgr.newVariableUInt("next_idx_mi8", first_dynamic_idx_mi8)
    spawn_mgr.newVariableUInt("next_acn_mi8", base_acn_spawn_mi8)
    spawn_mgr.newVariableUInt("total_spawned_mi8", 0)
    spawn_mgr.newVariableUInt("debug_curr_ops", 0)
    spawn_mgr.newVariableUInt("debug_target", 0)
    spawn_mgr.newVariableUInt("debug_need", 0)
    spawn_mgr.newVariableUInt("debug_curr_ops_mi8", 0)
    spawn_mgr.newVariableUInt("debug_target_mi8", 0)
    spawn_mgr.newVariableUInt("debug_need_mi8", 0)
    
    spawn_ticket = model.newAgent("SpawnDynamicTicket")
    spawn_ticket.newState("default")
    spawn_ticket.newVariableUInt("ticket", 0)
    spawn_ticket_mi8 = model.newAgent("SpawnDynamicTicketMi8")
    spawn_ticket_mi8.newState("default")
    spawn_ticket_mi8.newVariableUInt("ticket", 0)
    
    mgr_fn = spawn_mgr.newRTCFunction("rtc_spawn_dynamic_mgr_v8", RTC_SPAWN_DYNAMIC_MGR_V8)
    mgr_fn.setInitialState("default")
    mgr_fn.setEndState("default")
    
    ticket_fn = spawn_ticket.newRTCFunction("rtc_spawn_dynamic_ticket_v8", RTC_SPAWN_DYNAMIC_TICKET_V8)
    ticket_fn_mi8 = spawn_ticket_mi8.newRTCFunction("rtc_spawn_dynamic_ticket_v8_mi8", RTC_SPAWN_DYNAMIC_TICKET_V8_MI8)
    ticket_fn.setAgentOutput(heli_agent, "operations")
    ticket_fn.setInitialState("default")
    ticket_fn.setEndState("default")
    ticket_fn_mi8.setAgentOutput(heli_agent, "operations")
    ticket_fn_mi8.setInitialState("default")
    ticket_fn_mi8.setEndState("default")
    
    layer_mgr = model.newLayer("v8_spawn_dynamic_mgr")
    layer_mgr.addAgentFunction(mgr_fn)
    
    layer_ticket = model.newLayer("v8_spawn_dynamic_ticket")
    layer_ticket.addAgentFunction(ticket_fn)
    layer_ticket_mi8 = model.newLayer("v8_spawn_dynamic_ticket_mi8")
    layer_ticket_mi8.addAgentFunction(ticket_fn_mi8)
    
    print(f"  ✅ Менеджер: first_idx_mi17={first_dynamic_idx_mi17}, reserve_mi17={dynamic_reserve_mi17}, reserve_mi8={dynamic_reserve_mi8}")
    print(f"  ✅ Слои: v8_spawn_dynamic_mgr, v8_spawn_dynamic_ticket, v8_spawn_dynamic_ticket_mi8")
    
    return {
        'mgr_agent': spawn_mgr,
        'ticket_agent': spawn_ticket,
        'first_dynamic_idx': first_dynamic_idx,
        'first_dynamic_idx_mi17': first_dynamic_idx_mi17,
        'first_dynamic_idx_mi8': first_dynamic_idx_mi8,
        'dynamic_reserve_mi17': dynamic_reserve_mi17,
        'dynamic_reserve_mi8': dynamic_reserve_mi8,
        'base_acn_mi17': base_acn_spawn_mi17,
        'base_acn_mi8': base_acn_spawn_mi8
    }


def init_spawn_dynamic_population_v7(simulation: fg.CUDASimulation, model: fg.ModelDescription, 
                                      first_dynamic_idx: int, dynamic_reserve: int, base_acn: int):
    """Инициализация популяции агентов спавна"""
    
    # Менеджер (1 агент)
    mgr_pop = fg.AgentVector(model.getAgent("SpawnDynamicMgr"))
    mgr_pop.push_back()
    mgr_pop[0].setVariableUInt("next_idx", first_dynamic_idx)
    mgr_pop[0].setVariableUInt("next_acn", base_acn)
    mgr_pop[0].setVariableUInt("total_spawned", 0)
    simulation.setPopulationData(mgr_pop, "default")
    
    # Тикеты (по количеству резерва)
    ticket_pop = fg.AgentVector(model.getAgent("SpawnDynamicTicket"))
    for i in range(dynamic_reserve):
        ticket_pop.push_back()
        ticket_pop[i].setVariableUInt("ticket", i)
    simulation.setPopulationData(ticket_pop, "default")
    
    print(f"  ✅ Spawn популяция: mgr next_idx={first_dynamic_idx}, тикетов={dynamic_reserve}")


def init_spawn_dynamic_population_v8(simulation: fg.CUDASimulation, model: fg.ModelDescription,
                                     first_dynamic_idx_mi17: int, dynamic_reserve_mi17: int, base_acn_mi17: int,
                                     first_dynamic_idx_mi8: int, dynamic_reserve_mi8: int, base_acn_mi8: int):
    """Инициализация популяции агентов спавна (V8: Mi-17 + Mi-8)."""
    
    # Менеджер (1 агент)
    mgr_pop = fg.AgentVector(model.getAgent("SpawnDynamicMgr"))
    mgr_pop.push_back()
    mgr_pop[0].setVariableUInt("next_idx_mi17", first_dynamic_idx_mi17)
    mgr_pop[0].setVariableUInt("next_acn_mi17", base_acn_mi17)
    mgr_pop[0].setVariableUInt("total_spawned_mi17", 0)
    mgr_pop[0].setVariableUInt("next_idx_mi8", first_dynamic_idx_mi8)
    mgr_pop[0].setVariableUInt("next_acn_mi8", base_acn_mi8)
    mgr_pop[0].setVariableUInt("total_spawned_mi8", 0)
    simulation.setPopulationData(mgr_pop, "default")
    
    # Тикеты Mi-17
    ticket_pop = fg.AgentVector(model.getAgent("SpawnDynamicTicket"))
    for i in range(dynamic_reserve_mi17):
        ticket_pop.push_back()
        ticket_pop[i].setVariableUInt("ticket", i)
    simulation.setPopulationData(ticket_pop, "default")
    
    # Тикеты Mi-8
    ticket_pop_mi8 = fg.AgentVector(model.getAgent("SpawnDynamicTicketMi8"))
    for i in range(dynamic_reserve_mi8):
        ticket_pop_mi8.push_back()
        ticket_pop_mi8[i].setVariableUInt("ticket", i)
    simulation.setPopulationData(ticket_pop_mi8, "default")
    
    print(
        f"  ✅ Spawn популяция: mi17_idx={first_dynamic_idx_mi17}, mi17_tickets={dynamic_reserve_mi17}, "
        f"mi8_idx={first_dynamic_idx_mi8}, mi8_tickets={dynamic_reserve_mi8}"
    )

