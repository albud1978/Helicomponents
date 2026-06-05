#!/usr/bin/env python3
"""
RTC модуль V8: Квотирование через RepairLine

АРХИТЕКТУРА V8 (отличия от V7):
1. P2/P3 используют слоты RepairLine (free_days >= repair_time, при занятии free_days обнуляется)
2. Условия:
   - current_day >= repair_time (глобальный барьер)
   - есть свободная линия в списке слотов

Порядок:
1. Сброс буферов (MacroProperty) — V7
2. Подсчёт агентов по состояниям — V7
3. Демоут (если избыток) — V7
4. P1 промоут: serviceable → operations — V7
5. P2 промоут: unserviceable → operations — V8!
6. P3 промоут: inactive → operations — V8!

См. docs/architecture/adaptive_steps_logic.md для полной архитектуры.

Дата: 16.01.2026
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from model_build import RTC_MAX_FRAMES, MAX_DAYS

# Максимум ремонтных линий (MacroProperty размер)
REPAIR_LINES_MAX = 64
# Максимум bank-окон на линию
REPAIR_BANK_MAX = 64

import pyflamegpu as fg

# Активный путь RepairLine использует CAS на MacroProperty без message-ветки.

# Активный путь квотирования: register_quota_v8_messages (MessageBucket).

# Локальные копии V7-квот для V8 (reset/count/demote/P1/post)
from rtc_quota_v8_base import (
    RTC_RESET_FLAGS,
    RTC_RESET_BUFFERS,
    RTC_COUNT_OPS,
    RTC_COUNT_SVC,
    RTC_COUNT_INACTIVE,
    RTC_DEMOTE_OPS,
    RTC_PROMOTE_SVC,
)

# ═══════════════════════════════════════════════════════════════════════════════
# V8: QuotaManager (MessageBucket broadcast)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_QUOTA_MANAGER_V8_BUCKET = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_quota_manager_v8_bucket, flamegpu::MessageNone, flamegpu::MessageBucket) {{
    const unsigned char group_by = FLAMEGPU->getVariable<unsigned char>("group_by");
    if (group_by != 1u) return flamegpu::ALIVE;  // один QM для всех типов
    
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int target_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
    
    const unsigned int target_mi8 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", target_day);
    const unsigned int target_mi17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", target_day);
    const unsigned int mi8_rt = FLAMEGPU->environment.getProperty<unsigned int>("mi8_repair_time_const");
    const unsigned int mi17_rt = FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    
    const unsigned int ops8 = FLAMEGPU->environment.getMacroProperty<unsigned int, 1u>("mi8_ops_total");
    const unsigned int ops17 = FLAMEGPU->environment.getMacroProperty<unsigned int, 1u>("mi17_ops_total");
    const unsigned int svc8 = FLAMEGPU->environment.getMacroProperty<unsigned int, 1u>("mi8_svc_total");
    const unsigned int svc17 = FLAMEGPU->environment.getMacroProperty<unsigned int, 1u>("mi17_svc_total");
    const unsigned int unsvc8 = FLAMEGPU->environment.getMacroProperty<unsigned int, 1u>("mi8_unsvc_ready_total");
    const unsigned int unsvc17 = FLAMEGPU->environment.getMacroProperty<unsigned int, 1u>("mi17_unsvc_ready_total");
    const unsigned int ina8 = FLAMEGPU->environment.getMacroProperty<unsigned int, 1u>("mi8_inactive_total");
    const unsigned int ina17 = FLAMEGPU->environment.getMacroProperty<unsigned int, 1u>("mi17_inactive_total");

    // Передаём текущие ops в MacroProperty для SpawnManager
    auto qm_ops_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, 2u>("qm_ops_mp");
    qm_ops_mp[0].exchange(ops8);
    qm_ops_mp[1].exchange(ops17);
    
    // Подсчет доступных слотов ремонта (shared pool)
    auto line_days_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_free_days_mp");
    auto line_rt_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_rt_mp");
    auto line_acn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_acn_mp");
    auto bank_count_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_bank_count_mp");
    const unsigned int repair_quota = FLAMEGPU->environment.getProperty<unsigned int>("repair_quota");
    const unsigned int max_lines = (repair_quota < {REPAIR_LINES_MAX}u) ? repair_quota : {REPAIR_LINES_MAX}u;

    unsigned int today_ready_slots = 0u;
    unsigned int today_committable_slots = 0u;
    unsigned int bank_ready_slots = 0u;
    for (unsigned int i = 0u; i < max_lines; ++i) {{
        const unsigned int line_rt = line_rt_mp[i];
        const unsigned int line_acn = line_acn_mp[i];
        const bool today_ready = (line_rt > 0u && line_days_mp[i] >= line_rt);
        const bool today_committable = (line_acn == 0u && line_rt > 0u && line_days_mp[i] >= line_rt);
        const bool bank_ready = (bank_count_mp[i] > 0u);
        if (today_ready) {{
            today_ready_slots++;
        }} else if (bank_ready) {{
            bank_ready_slots++;
        }}
        if (today_committable) {{
            today_committable_slots++;
        }}
    }}
    unsigned int available_slots = today_ready_slots + bank_ready_slots;
    
    const unsigned int ina_ready8 = (day >= mi8_rt) ? ina8 : 0u;
    const unsigned int ina_ready17 = (day >= mi17_rt) ? ina17 : 0u;
    
    unsigned int deficit8 = (target_mi8 > ops8) ? (target_mi8 - ops8) : 0u;
    unsigned int deficit17 = (target_mi17 > ops17) ? (target_mi17 - ops17) : 0u;
    
    unsigned int p1_8 = (deficit8 < svc8) ? deficit8 : svc8;
    deficit8 -= p1_8;
    unsigned int p1_17 = (deficit17 < svc17) ? deficit17 : svc17;
    deficit17 -= p1_17;
    
    // P2 И P3 требуют RepairLine slot (free_days обнуляется при занятии)
    // Приоритет: по типу (Mi-17 → Mi-8), внутри типа P2 (unsvc) → P3 (inactive)
    unsigned int slots_left = available_slots;
    
    // P2: Mi-17 first — ТРЕБУЕТ RepairLine slot
    unsigned int p2_17 = (deficit17 < unsvc17) ? deficit17 : unsvc17;
    p2_17 = (p2_17 < slots_left) ? p2_17 : slots_left;
    deficit17 -= p2_17;
    slots_left -= p2_17;
    
    // P3: Mi-17 — на оставшиеся слоты после P2 (того же типа)
    unsigned int p3_17 = (deficit17 < ina_ready17) ? deficit17 : ina_ready17;
    p3_17 = (p3_17 < slots_left) ? p3_17 : slots_left;
    deficit17 -= p3_17;
    slots_left -= p3_17;
    
    // P2: Mi-8 — ТРЕБУЕТ RepairLine slot (после Mi-17 P2/P3)
    unsigned int p2_8 = (deficit8 < unsvc8) ? deficit8 : unsvc8;
    p2_8 = (p2_8 < slots_left) ? p2_8 : slots_left;
    deficit8 -= p2_8;
    slots_left -= p2_8;
    
    // P3: Mi-8 — на оставшиеся слоты
    unsigned int p3_8 = (deficit8 < ina_ready8) ? deficit8 : ina_ready8;
    p3_8 = (p3_8 < slots_left) ? p3_8 : slots_left;
    
    FLAMEGPU->message_out.setKey(0);
    FLAMEGPU->message_out.setVariable<unsigned int>("promote_p1_mi8", p1_8);
    FLAMEGPU->message_out.setVariable<unsigned int>("promote_p1_mi17", p1_17);
    FLAMEGPU->message_out.setVariable<unsigned int>("promote_p2_mi8", p2_8);
    FLAMEGPU->message_out.setVariable<unsigned int>("promote_p2_mi17", p2_17);
    FLAMEGPU->message_out.setVariable<unsigned int>("promote_p3_mi8", p3_8);
    FLAMEGPU->message_out.setVariable<unsigned int>("promote_p3_mi17", p3_17);
    FLAMEGPU->message_out.setVariable<unsigned int>("deficit_mi8", deficit8);
    FLAMEGPU->message_out.setVariable<unsigned int>("deficit_mi17", deficit17);
    FLAMEGPU->message_out.setVariable<unsigned int>("today_ready_slots", today_ready_slots);
    FLAMEGPU->message_out.setVariable<unsigned int>("today_committable_slots", today_committable_slots);
    FLAMEGPU->message_out.setVariable<unsigned int>("bank_ready_slots", bank_ready_slots);
    
    // Debug: итоги QM
    FLAMEGPU->setVariable<unsigned int>("debug_qm_ops_mi8", ops8);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_ops_mi17", ops17);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_target_mi8", target_mi8);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_target_mi17", target_mi17);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_quota_left_mi8", deficit8);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_quota_left_mi17", deficit17);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_unsvc_cnt", unsvc8 + unsvc17);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_inactive_cnt", ina_ready8 + ina_ready17);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_p1_mi8", p1_8);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_p1_mi17", p1_17);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_p2_total", p2_8 + p2_17);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_p3_total", p3_8 + p3_17);
    // FLAMEGPU->setVariable<unsigned int>("debug_qm_available_slots", available_slots); // add variable if declared
    FLAMEGPU->setVariable<int>("debug_qm_balance_mi8", (int)target_mi8 - (int)ops8);
    FLAMEGPU->setVariable<int>("debug_qm_balance_mi17", (int)target_mi17 - (int)ops17);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_target_day", target_day);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_ops_cnt_mi8", ops8);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_ops_cnt_mi17", ops17);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_svc_cnt_mi8", svc8);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_svc_cnt_mi17", svc17);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_unsvc_ready_mi8", unsvc8);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_unsvc_ready_mi17", unsvc17);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_inactive_mi8", ina_ready8);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_inactive_mi17", ina_ready17);
    
    return flamegpu::ALIVE;
}}
"""

# ═══════════════════════════════════════════════════════════════════════════════
# V8: Snapshot RepairLine MacroProperty (RO)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_REPAIR_LINE_SNAPSHOT_V8 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_repair_line_snapshot_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned char group_by = FLAMEGPU->getVariable<unsigned char>("group_by");
    if (group_by != 1u) return flamegpu::ALIVE;  // один QM делает snapshot
    
    auto line_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_free_days_mp");
    auto line_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_acn_mp");
    auto bank_count_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_bank_count_mp");
    auto bank_end_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX * REPAIR_BANK_MAX}u>("repair_line_bank_end_mp");
    
    auto line_mp_ro = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_free_days_ro_mp");
    auto line_acn_ro = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_acn_ro_mp");
    auto bank_count_ro = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_bank_count_ro_mp");
    auto bank_head_end_ro = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_bank_head_end_ro_mp");
    
    for (unsigned int i = 0u; i < {REPAIR_LINES_MAX}u; ++i) {{
        unsigned int fd = line_mp[i].exchange(0u);
        line_mp[i].exchange(fd);
        line_mp_ro[i].exchange(fd);
        
        unsigned int acn = line_acn[i].exchange(0u);
        line_acn[i].exchange(acn);
        line_acn_ro[i].exchange(acn);
        
        unsigned int bc = bank_count_mp[i].exchange(0u);
        bank_count_mp[i].exchange(bc);
        bank_count_ro[i].exchange(bc);
        
        const unsigned int base = i * {REPAIR_BANK_MAX}u;
        unsigned int head_end = bank_end_mp[base].exchange(0u);
        bank_end_mp[base].exchange(head_end);
        bank_head_end_ro[i].exchange(head_end);
    }}
    
    return flamegpu::ALIVE;
}}
"""

# C1: дубликат snapshot для пересоздания RO-снэпшота МЕЖДУ P2-commit и P3-commit.
# Тело идентично rtc_repair_line_snapshot_v8 (копирует live→ro для
# line_acn/line_mp/bank_count/bank_head_end — всё, что читает P3), отличается только
# именем функции, т.к. FLAME GPU требует уникальное имя RTC-функции для каждого слоя.
RTC_REPAIR_LINE_SNAPSHOT_V8_P3 = RTC_REPAIR_LINE_SNAPSHOT_V8.replace(
    "rtc_repair_line_snapshot_v8", "rtc_repair_line_snapshot_v8_p3"
)

# ═══════════════════════════════════════════════════════════════════════════════
# V8: Промоуты по MessageBucket (P1/P2/P3)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_PROMOTE_SVC_BUCKET_V8 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_promote_svc_bucket_v8, flamegpu::MessageBucket, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    if (group_by != 1u && group_by != 2u) return flamegpu::ALIVE;
    
    unsigned int promote = 0u;
    for (auto &msg : FLAMEGPU->message_in(0)) {{
        FLAMEGPU->setVariable<unsigned int>("debug_bucket_seen", 1u);
        promote = (group_by == 1u)
            ? msg.getVariable<unsigned int>("promote_p1_mi8")
            : msg.getVariable<unsigned int>("promote_p1_mi17");
        break;
    }}
    if (promote == 0u) return flamegpu::ALIVE;
    
    auto svc_count = (group_by == 1u)
        ? FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_svc_count")
        : FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_svc_count");
    const unsigned int frames = {RTC_MAX_FRAMES}u;
    
    unsigned int rank = 0u;
    for (unsigned int i = 0u; i < frames; ++i) {{
        if (i <= idx) continue;
        rank += svc_count[i];
    }}
    
    if (rank < promote) {{
        FLAMEGPU->setVariable<unsigned int>("promoted", 1u);
    }}
    return flamegpu::ALIVE;
}}
"""

RTC_PROMOTE_UNSVC_BUCKET_V8 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_promote_unsvc_bucket_v8, flamegpu::MessageBucket, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    const unsigned int repair_line_id = FLAMEGPU->getVariable<unsigned int>("repair_line_id");
    const unsigned int status_change_day = FLAMEGPU->getVariable<unsigned int>("status_change_day");
    if (group_by != 1u && group_by != 2u) return flamegpu::ALIVE;
    if (repair_days != 0u || repair_line_id != 0xFFFFFFFFu) return flamegpu::ALIVE;
    
    unsigned int promote_total = 0u;
    unsigned int today_committable_slots = 0u;
    unsigned int bank_ready_slots = 0u;
    for (auto &msg : FLAMEGPU->message_in(0)) {{
        FLAMEGPU->setVariable<unsigned int>("debug_bucket_seen", 1u);
        promote_total = (group_by == 1u)
            ? msg.getVariable<unsigned int>("promote_p2_mi8")
            : msg.getVariable<unsigned int>("promote_p2_mi17");
        today_committable_slots = msg.getVariable<unsigned int>("today_committable_slots");
        bank_ready_slots = msg.getVariable<unsigned int>("bank_ready_slots");
        break;
    }}
    if (promote_total == 0u) return flamegpu::ALIVE;
    
    const bool bank_only_day = (today_committable_slots == 0u && bank_ready_slots > 0u);
    
    const unsigned int frames = {RTC_MAX_FRAMES}u;
    auto unsvc8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_unsvc_ready_count");
    auto unsvc17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_unsvc_ready_count");
    auto unsvc8_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_unsvc_status_day");
    auto unsvc17_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_unsvc_status_day");
    
    unsigned int rank = 0u;
    if (group_by == 1u) {{
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (unsvc8[i] == 0u) continue;
            const unsigned int other_day = unsvc8_day[i];
            if (!bank_only_day) {{
                if (other_day > status_change_day || (other_day == status_change_day && i > idx)) {{
                    rank += 1u;
                }}
            }} else {{
                if (other_day < status_change_day || (other_day == status_change_day && i < idx)) {{
                    rank += 1u;
                }}
            }}
        }}
    }} else {{
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (unsvc17[i] == 0u) continue;
            const unsigned int other_day = unsvc17_day[i];
            if (!bank_only_day) {{
                if (other_day > status_change_day || (other_day == status_change_day && i > idx)) {{
                    rank += 1u;
                }}
            }} else {{
                if (other_day < status_change_day || (other_day == status_change_day && i < idx)) {{
                    rank += 1u;
                }}
            }}
        }}
    }}
    
    if (rank < promote_total) {{
        FLAMEGPU->setVariable<unsigned int>("repair_candidate", 1u);
        FLAMEGPU->setVariable<unsigned int>("repair_line_id", 0xFFFFFFFFu);
        FLAMEGPU->setVariable<unsigned int>("decision_p2", 1u);
        // N1: не помечаем кандидатом stale-агента (status_change_day > claim_start_day),
        // иначе он инфлирует commit_pos соседей. Используем ту же формулу claim_start_day,
        // что и commit (current_day - repair_time), для согласованности позиций.
        const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
        const unsigned int repair_time = (group_by == 1u)
            ? FLAMEGPU->environment.getProperty<unsigned int>("mi8_repair_time_const")
            : FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
        const unsigned int claim_start_day = current_day - repair_time;
        if (status_change_day <= claim_start_day) {{
            auto p2_candidate = (group_by == 1u)
                ? FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_commit_p2_candidate")
                : FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_commit_p2_candidate");
            p2_candidate[idx].exchange(1u);
        }}
    }}
    return flamegpu::ALIVE;
}}
"""

RTC_PROMOTE_INACTIVE_BUCKET_V8 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_promote_inactive_bucket_v8, flamegpu::MessageBucket, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    const unsigned int status_change_day = FLAMEGPU->getVariable<unsigned int>("status_change_day");
    if (group_by != 1u && group_by != 2u) return flamegpu::ALIVE;
    if (repair_days != 0u) return flamegpu::ALIVE;
    
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int rt = (group_by == 1u)
        ? FLAMEGPU->environment.getProperty<unsigned int>("mi8_repair_time_const")
        : FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    if (day < rt) return flamegpu::ALIVE;
    
    unsigned int promote_total = 0u;
    unsigned int today_committable_slots = 0u;
    unsigned int bank_ready_slots = 0u;
    for (auto &msg : FLAMEGPU->message_in(0)) {{
        FLAMEGPU->setVariable<unsigned int>("debug_bucket_seen", 1u);
        promote_total = (group_by == 1u)
            ? msg.getVariable<unsigned int>("promote_p3_mi8")
            : msg.getVariable<unsigned int>("promote_p3_mi17");
        today_committable_slots = msg.getVariable<unsigned int>("today_committable_slots");
        bank_ready_slots = msg.getVariable<unsigned int>("bank_ready_slots");
        break;
    }}
    if (promote_total == 0u) return flamegpu::ALIVE;
    
    const bool bank_only_day = (today_committable_slots == 0u && bank_ready_slots > 0u);
    
    const unsigned int frames = {RTC_MAX_FRAMES}u;
    auto ina8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_inactive_count");
    auto ina17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_inactive_count");
    auto ina8_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_inactive_status_day");
    auto ina17_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_inactive_status_day");
    
    unsigned int rank = 0u;
    if (group_by == 1u) {{
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (ina8[i] == 0u) continue;
            const unsigned int other_day = ina8_day[i];
            if (!bank_only_day) {{
                if (other_day > status_change_day || (other_day == status_change_day && i > idx)) {{
                    rank += 1u;
                }}
            }} else {{
                if (other_day < status_change_day || (other_day == status_change_day && i < idx)) {{
                    rank += 1u;
                }}
            }}
        }}
    }} else {{
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (ina17[i] == 0u) continue;
            const unsigned int other_day = ina17_day[i];
            if (!bank_only_day) {{
                if (other_day > status_change_day || (other_day == status_change_day && i > idx)) {{
                    rank += 1u;
                }}
            }} else {{
                if (other_day < status_change_day || (other_day == status_change_day && i < idx)) {{
                    rank += 1u;
                }}
            }}
        }}
    }}
    
    if (rank < promote_total) {{
        FLAMEGPU->setVariable<unsigned int>("repair_candidate", 1u);
        FLAMEGPU->setVariable<unsigned int>("repair_line_id", 0xFFFFFFFFu);
        FLAMEGPU->setVariable<unsigned int>("decision_p3", 1u);
        // N1: не помечаем кандидатом stale-агента (status_change_day > claim_start_day),
        // чтобы не инфлировать commit_pos соседей. day>=rt гарантирован проверкой выше.
        const unsigned int claim_start_day = day - rt;
        if (status_change_day <= claim_start_day) {{
            auto p3_candidate = (group_by == 1u)
                ? FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_commit_p3_candidate")
                : FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_commit_p3_candidate");
            p3_candidate[idx].exchange(1u);
        }}
    }}
    return flamegpu::ALIVE;
}}
"""

# ═══════════════════════════════════════════════════════════════════════════════
# V8: Подсчёт unsvc с readiness по repair_days
# ═══════════════════════════════════════════════════════════════════════════════

RTC_COUNT_UNSVC_V8 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_count_unsvc_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    const unsigned int repair_line_id = FLAMEGPU->getVariable<unsigned int>("repair_line_id");
    const unsigned int status_change_day = FLAMEGPU->getVariable<unsigned int>("status_change_day");
    
    unsigned int repair_time = 0u;
    if (group_by == 1u) {{
        repair_time = FLAMEGPU->environment.getProperty<unsigned int>("mi8_repair_time_const");
    }} else if (group_by == 2u) {{
        repair_time = FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    }} else {{
        return flamegpu::ALIVE;
    }}
    
    const bool ready = (day >= repair_time && repair_days == 0u && repair_line_id == 0xFFFFFFFFu);
    
    if (group_by == 1u) {{
        auto count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_unsvc_count");
        auto ready_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_unsvc_ready_count");
        auto status_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_unsvc_status_day");
        count[idx].exchange(1u);
        if (ready) {{
            auto total = FLAMEGPU->environment.getMacroProperty<unsigned int, 1u>("mi8_unsvc_ready_total");
            ready_count[idx].exchange(1u);
            status_days[idx].exchange(status_change_day);
            total += 1u;
        }} else {{
            status_days[idx].exchange(0u);
        }}
    }} else if (group_by == 2u) {{
        auto count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_unsvc_count");
        auto ready_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_unsvc_ready_count");
        auto status_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_unsvc_status_day");
        count[idx].exchange(1u);
        if (ready) {{
            auto total = FLAMEGPU->environment.getMacroProperty<unsigned int, 1u>("mi17_unsvc_ready_total");
            ready_count[idx].exchange(1u);
            status_days[idx].exchange(status_change_day);
            total += 1u;
        }} else {{
            status_days[idx].exchange(0u);
        }}
    }}
    return flamegpu::ALIVE;
}}
"""

RTC_PROMOTE_UNSVC_COMMIT_V8 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_promote_unsvc_commit_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int candidate = FLAMEGPU->getVariable<unsigned int>("repair_candidate");
    if (candidate == 0u) return flamegpu::ALIVE;
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int frames = {RTC_MAX_FRAMES}u;
    
    // P2: unsvc→ops ЧЕРЕЗ RepairLine slot (free_days обнуляется)
    const unsigned int line_id = FLAMEGPU->getVariable<unsigned int>("repair_line_id");
    auto line_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_free_days_mp");
    auto line_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_acn_mp");
    auto line_gb = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_gb_mp");
    auto line_rt = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_rt_mp");
    auto line_last_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_last_acn_mp");
    auto line_last_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_last_day_mp");
    auto bank_count_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_bank_count_mp");
    auto bank_lock_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_bank_lock_mp");
    auto bank_start_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX * REPAIR_BANK_MAX}u>("repair_line_bank_start_mp");
    auto bank_end_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX * REPAIR_BANK_MAX}u>("repair_line_bank_end_mp");
    
    auto line_mp_ro = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_free_days_ro_mp");
    auto line_acn_ro = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_acn_ro_mp");
    auto bank_count_ro = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_bank_count_ro_mp");
    auto bank_head_end_ro = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_bank_head_end_ro_mp");
    
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int acn = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int status_change_day = FLAMEGPU->getVariable<unsigned int>("status_change_day");
    const unsigned int repair_time = (group_by == 1u)
        ? FLAMEGPU->environment.getProperty<unsigned int>("mi8_repair_time_const")
        : FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    const unsigned int claim_start_day = current_day - repair_time;
    
    const unsigned int repair_quota = FLAMEGPU->environment.getProperty<unsigned int>("repair_quota");
    const unsigned int max_lines = (repair_quota < {REPAIR_LINES_MAX}u) ? repair_quota : {REPAIR_LINES_MAX}u;

    // N1/C2: stale-агент (status_change_day > claim_start_day) не может занять ни source1
    // (личное окно), ни прошлое bank-окно (source2_guard) — выходим до вычисления позиции
    // и любых CAS. Это исключает stale из гонки за bank и согласуется с фильтром
    // кандидатов в bucket-фазе (commit_pos считает только нестейл-кандидатов).
    if (status_change_day > claim_start_day) {{
        FLAMEGPU->setVariable<unsigned int>("repair_line_id", 0xFFFFFFFFu);
        FLAMEGPU->setVariable<unsigned int>("repair_candidate", 0u);
        return flamegpu::ALIVE;
    }}

    auto mi8_p2_candidate = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_commit_p2_candidate");
    auto mi17_p2_candidate = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_commit_p2_candidate");

    unsigned int commit_pos = 0u;
    if (group_by == 1u) {{
        for (unsigned int i = 0u; i < frames; ++i) {{
            commit_pos += mi17_p2_candidate[i];
        }}
        for (unsigned int i = idx + 1u; i < frames; ++i) {{
            commit_pos += mi8_p2_candidate[i];
        }}
    }} else {{
        for (unsigned int i = idx + 1u; i < frames; ++i) {{
            commit_pos += mi17_p2_candidate[i];
        }}
    }}

    // C2: число source1-пригодных свободных линий в RO-снэпшоте. Одинаково у всех бортов
    // фазы (RO неизменен в пределах фазы) → детерминированная граница source1/bank,
    // не зависящая от исхода отдельных CAS. Заменяет неустойчивый free_line_count для bank_pos.
    unsigned int free_total = 0u;
    for (unsigned int i = 0u; i < max_lines; ++i) {{
        if (line_acn_ro[i] != 0u) continue;
        if (line_mp_ro[i] < repair_time) continue;
        ++free_total;
    }}

    bool claimed = false;
    unsigned int chosen_line = 0xFFFFFFFFu;
    unsigned int claim_start = 0xFFFFFFFFu;
    unsigned int claim_end = 0xFFFFFFFFu;
    unsigned int claim_source = 0u;
    unsigned int carry_days = 0u;
    
    // Попытка занять предвыбранную линию
    if (line_id != 0xFFFFFFFFu) {{
        const unsigned int prev_acn = line_acn[line_id].exchange(acn);
        if (prev_acn == 0u) {{
            const unsigned int old_days = line_mp[line_id].exchange(0u);
            if (old_days >= repair_time && status_change_day <= claim_start_day) {{
                chosen_line = line_id;
                claimed = true;
                claim_start = claim_start_day;
                claim_end = current_day;
                claim_source = 1u;
                carry_days = old_days;
            }} else {{
                line_mp[line_id].exchange(old_days);
                line_acn[line_id].exchange(0u);
            }}
        }} else {{
            line_acn[line_id].exchange(prev_acn);
        }}
    }}
    
    unsigned int free_line_count = 0u;

    // Fallback: deterministic line-first/best-fit by commit_pos.
    if (!claimed) {{
        bool tried[{REPAIR_LINES_MAX}u];
        for (unsigned int i = 0u; i < max_lines; ++i) {{
            tried[i] = false;
        }}
        
        for (unsigned int attempt = 0u; attempt < max_lines && !claimed; ++attempt) {{
            unsigned int best_line = 0xFFFFFFFFu;
            unsigned int best_days = 0u;
            
            for (unsigned int i = 0u; i < max_lines; ++i) {{
                if (tried[i]) continue;
                if (line_acn_ro[i] != 0u) continue;
                const unsigned int old_days = line_mp_ro[i];
                if (old_days < repair_time) continue;
                if (best_line == 0xFFFFFFFFu || old_days < best_days || (old_days == best_days && i < best_line)) {{
                    best_line = i;
                    best_days = old_days;
                }}
            }}
            
            if (best_line == 0xFFFFFFFFu) {{
                break;
            }}
            tried[best_line] = true;
            
            if (free_line_count != commit_pos) {{
                ++free_line_count;
                continue;
            }}
            ++free_line_count;

            if (status_change_day > claim_start_day) {{
                break;
            }}

            const unsigned int prev_acn = line_acn[best_line].exchange(acn);
            if (prev_acn != 0u) {{
                line_acn[best_line].exchange(prev_acn);
                break;
            }}
            
            const unsigned int old_days = line_mp[best_line].exchange(0u);
            if (old_days < repair_time || status_change_day > claim_start_day) {{
                line_mp[best_line].exchange(old_days);
                line_acn[best_line].exchange(0u);
                break;
            }}
            chosen_line = best_line;
            claimed = true;
            claim_start = claim_start_day;
            claim_end = current_day;
            claim_source = 1u;
            carry_days = old_days;
        }}
    }}
    
    // Fallback: deterministic bank windows (newest-first) after source1 positions.
    // C2: bank_pos = позиция борта среди НЕ получивших source1 = commit_pos - free_total.
    // Уникальна для каждого борта (commit_pos уникален) и НЕ схлопывается в 0, т.к. не
    // зависит от free_line_count внутри неуспешной попытки. Вход только при исчерпании
    // source1-ёмкости (commit_pos >= free_total) → один bank_lock CAS на свою линию.
    if (!claimed && commit_pos >= free_total) {{
        const unsigned int bank_pos = commit_pos - free_total;
        unsigned int seen_bank = 0u;
        bool tried[{REPAIR_LINES_MAX}u];
        for (unsigned int i = 0u; i < max_lines; ++i) {{
            tried[i] = false;
        }}
        
        for (unsigned int attempt = 0u; attempt < max_lines && !claimed; ++attempt) {{
            unsigned int best_line = 0xFFFFFFFFu;
            unsigned int best_end = 0u;
            
            for (unsigned int i = 0u; i < max_lines; ++i) {{
                if (tried[i]) continue;
                if (bank_count_ro[i] == 0u) continue;
                const unsigned int head_end = bank_head_end_ro[i];
                if (head_end == 0xFFFFFFFFu) continue;
                if (best_line == 0xFFFFFFFFu || head_end > best_end || (head_end == best_end && i < best_line)) {{
                    best_line = i;
                    best_end = head_end;
                }}
            }}
            
            if (best_line == 0xFFFFFFFFu) {{
                break;
            }}
            tried[best_line] = true;
            
            if (seen_bank != bank_pos) {{
                ++seen_bank;
                continue;
            }}

            const unsigned int prev_lock = bank_lock_mp[best_line].exchange(1u);
            if (prev_lock != 0u) {{
                break;
            }}
            
            unsigned int bank_count = bank_count_ro[best_line];
            if (bank_count == 0u) {{
                bank_lock_mp[best_line].exchange(0u);
                break;
            }}
            
            const unsigned int base = best_line * {REPAIR_BANK_MAX}u;
            unsigned int window_start = bank_start_mp[base].exchange(0u);
            unsigned int window_end = bank_end_mp[base].exchange(0u);
            if (window_start == 0xFFFFFFFFu || window_end == 0xFFFFFFFFu || window_end <= window_start) {{
                bank_start_mp[base].exchange(window_start);
                bank_end_mp[base].exchange(window_end);
                bank_lock_mp[best_line].exchange(0u);
                break;
            }}
            if (status_change_day > window_start) {{
                bank_start_mp[base].exchange(window_start);
                bank_end_mp[base].exchange(window_end);
                bank_lock_mp[best_line].exchange(0u);
                break;
            }}
            for (unsigned int i = 1u; i < bank_count; ++i) {{
                const unsigned int from = base + i;
                const unsigned int to = base + (i - 1u);
                const unsigned int prev_start = bank_start_mp[from].exchange(0u);
                const unsigned int prev_end = bank_end_mp[from].exchange(0u);
                bank_start_mp[to].exchange(prev_start);
                bank_end_mp[to].exchange(prev_end);
            }}
            const unsigned int tail = base + (bank_count - 1u);
            bank_start_mp[tail].exchange(0xFFFFFFFFu);
            bank_end_mp[tail].exchange(0xFFFFFFFFu);
            bank_count -= 1u;
            bank_count_mp[best_line].exchange(bank_count);
            bank_lock_mp[best_line].exchange(0u);
            
            chosen_line = best_line;
            claimed = true;
            claim_start = window_start;
            claim_end = window_end;
            claim_source = 2u;
        }}
    }}
    
    // Резервирование прошлых окон (newest-first)
    if (claimed && claim_source == 1u && carry_days >= repair_time) {{
        unsigned int windows_total = carry_days / repair_time;
        if (windows_total > 1u) {{
            unsigned int carry = windows_total - 1u;
            if (carry > {REPAIR_BANK_MAX}u) {{
                carry = {REPAIR_BANK_MAX}u;
            }}
            unsigned int bank_count = bank_count_ro[chosen_line];
            for (unsigned int n = carry; n > 0u; --n) {{
                const unsigned int win_idx = n - 1u;
                const unsigned int end_day = current_day - repair_time * (win_idx + 1u);
                const unsigned int start_day = end_day - repair_time;
                
                unsigned int shift_limit = (bank_count < {REPAIR_BANK_MAX}u)
                    ? bank_count
                    : ({REPAIR_BANK_MAX}u - 1u);
                for (int j = (int)shift_limit; j > 0; --j) {{
                    const unsigned int from = chosen_line * {REPAIR_BANK_MAX}u + (unsigned int)(j - 1);
                    const unsigned int to = chosen_line * {REPAIR_BANK_MAX}u + (unsigned int)j;
                    const unsigned int prev_start = bank_start_mp[from].exchange(0u);
                    const unsigned int prev_end = bank_end_mp[from].exchange(0u);
                    bank_start_mp[to].exchange(prev_start);
                    bank_end_mp[to].exchange(prev_end);
                }}
                const unsigned int head = chosen_line * {REPAIR_BANK_MAX}u;
                bank_start_mp[head].exchange(start_day);
                bank_end_mp[head].exchange(end_day);
                if (bank_count < {REPAIR_BANK_MAX}u) {{
                    bank_count += 1u;
                }}
            }}
            bank_count_mp[chosen_line].exchange(bank_count);
        }}
    }}
    
    if (claimed) {{
        line_rt[chosen_line].exchange(repair_time);
        line_last_acn[chosen_line].exchange(acn);
        line_last_day[chosen_line].exchange(current_day);
        line_gb[chosen_line].exchange(group_by);
        FLAMEGPU->setVariable<unsigned int>("repair_line_id", chosen_line);
        FLAMEGPU->setVariable<unsigned int>("promoted", 1u);
        FLAMEGPU->setVariable<unsigned int>("debug_promoted", 1u);
        FLAMEGPU->setVariable<unsigned int>("repair_claim_start_day", claim_start);
        FLAMEGPU->setVariable<unsigned int>("repair_claim_end_day", claim_end);
        FLAMEGPU->setVariable<unsigned int>("repair_claim_source", claim_source);
        
        if (group_by == 1u) {{
            auto p2 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_approve");
            p2[idx].exchange(1u);
        }} else {{
            auto p2 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_approve");
            p2[idx].exchange(1u);
        }}
    }} else {{
        FLAMEGPU->setVariable<unsigned int>("repair_line_id", 0xFFFFFFFFu);
    }}
    
    FLAMEGPU->setVariable<unsigned int>("repair_candidate", 0u);
    return flamegpu::ALIVE;
}}
"""


RTC_PROMOTE_INACTIVE_COMMIT_V8 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_promote_inactive_commit_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int candidate = FLAMEGPU->getVariable<unsigned int>("repair_candidate");
    if (candidate == 0u) return flamegpu::ALIVE;
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int frames = {RTC_MAX_FRAMES}u;
    
    const unsigned int line_id = FLAMEGPU->getVariable<unsigned int>("repair_line_id");
    auto line_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_free_days_mp");
    auto line_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_acn_mp");
    auto line_gb = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_gb_mp");
    auto line_rt = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_rt_mp");
    auto line_last_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_last_acn_mp");
    auto line_last_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_last_day_mp");
    auto bank_count_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_bank_count_mp");
    auto bank_lock_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_bank_lock_mp");
    auto bank_start_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX * REPAIR_BANK_MAX}u>("repair_line_bank_start_mp");
    auto bank_end_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX * REPAIR_BANK_MAX}u>("repair_line_bank_end_mp");
    
    auto line_mp_ro = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_free_days_ro_mp");
    auto line_acn_ro = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_acn_ro_mp");
    auto bank_count_ro = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_bank_count_ro_mp");
    auto bank_head_end_ro = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_bank_head_end_ro_mp");
    
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int acn = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int status_change_day = FLAMEGPU->getVariable<unsigned int>("status_change_day");
    const unsigned int repair_time = (group_by == 1u)
        ? FLAMEGPU->environment.getProperty<unsigned int>("mi8_repair_time_const")
        : FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    const unsigned int claim_start_day = current_day - repair_time;
    
    const unsigned int repair_quota = FLAMEGPU->environment.getProperty<unsigned int>("repair_quota");
    const unsigned int max_lines = (repair_quota < {REPAIR_LINES_MAX}u) ? repair_quota : {REPAIR_LINES_MAX}u;

    // N1/C2: см. P2 — stale-агент не может занять ни source1, ни bank, выходим заранее.
    if (status_change_day > claim_start_day) {{
        FLAMEGPU->setVariable<unsigned int>("repair_line_id", 0xFFFFFFFFu);
        FLAMEGPU->setVariable<unsigned int>("repair_candidate", 0u);
        return flamegpu::ALIVE;
    }}

    auto mi8_p3_candidate = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_commit_p3_candidate");
    auto mi17_p3_candidate = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_commit_p3_candidate");

    unsigned int commit_pos = 0u;
    if (group_by == 1u) {{
        for (unsigned int i = 0u; i < frames; ++i) {{
            commit_pos += mi17_p3_candidate[i];
        }}
        for (unsigned int i = idx + 1u; i < frames; ++i) {{
            commit_pos += mi8_p3_candidate[i];
        }}
    }} else {{
        for (unsigned int i = idx + 1u; i < frames; ++i) {{
            commit_pos += mi17_p3_candidate[i];
        }}
    }}

    // C2: см. P2 — детерминированная граница source1/bank по RO-снэпшоту (после C1
    // re-snapshot RO у P3 уже отражает занятые P2 линии/банк).
    unsigned int free_total = 0u;
    for (unsigned int i = 0u; i < max_lines; ++i) {{
        if (line_acn_ro[i] != 0u) continue;
        if (line_mp_ro[i] < repair_time) continue;
        ++free_total;
    }}

    bool claimed = false;
    unsigned int chosen_line = 0xFFFFFFFFu;
    unsigned int claim_start = 0xFFFFFFFFu;
    unsigned int claim_end = 0xFFFFFFFFu;
    unsigned int claim_source = 0u;
    unsigned int carry_days = 0u;
    
    if (line_id != 0xFFFFFFFFu) {{
        const unsigned int prev_acn = line_acn[line_id].exchange(acn);
        if (prev_acn == 0u) {{
            const unsigned int old_days = line_mp[line_id].exchange(0u);
            if (old_days >= repair_time && status_change_day <= claim_start_day) {{
                chosen_line = line_id;
                claimed = true;
                claim_start = claim_start_day;
                claim_end = current_day;
                claim_source = 1u;
                carry_days = old_days;
            }} else {{
                line_mp[line_id].exchange(old_days);
                line_acn[line_id].exchange(0u);
            }}
        }} else {{
            line_acn[line_id].exchange(prev_acn);
        }}
    }}
    
    unsigned int free_line_count = 0u;

    if (!claimed) {{
        bool tried[{REPAIR_LINES_MAX}u];
        for (unsigned int i = 0u; i < max_lines; ++i) {{
            tried[i] = false;
        }}
        
        for (unsigned int attempt = 0u; attempt < max_lines && !claimed; ++attempt) {{
            unsigned int best_line = 0xFFFFFFFFu;
            unsigned int best_days = 0u;
            
            for (unsigned int i = 0u; i < max_lines; ++i) {{
                if (tried[i]) continue;
                if (line_acn_ro[i] != 0u) continue;
                const unsigned int old_days = line_mp_ro[i];
                if (old_days < repair_time) continue;
                if (best_line == 0xFFFFFFFFu || old_days < best_days || (old_days == best_days && i < best_line)) {{
                    best_line = i;
                    best_days = old_days;
                }}
            }}
            
            if (best_line == 0xFFFFFFFFu) {{
                break;
            }}
            tried[best_line] = true;
            
            if (free_line_count != commit_pos) {{
                ++free_line_count;
                continue;
            }}
            ++free_line_count;

            if (status_change_day > claim_start_day) {{
                break;
            }}

            const unsigned int prev_acn = line_acn[best_line].exchange(acn);
            if (prev_acn != 0u) {{
                line_acn[best_line].exchange(prev_acn);
                break;
            }}
            
            const unsigned int old_days = line_mp[best_line].exchange(0u);
            if (old_days < repair_time || status_change_day > claim_start_day) {{
                line_mp[best_line].exchange(old_days);
                line_acn[best_line].exchange(0u);
                break;
            }}
            chosen_line = best_line;
            claimed = true;
            claim_start = claim_start_day;
            claim_end = current_day;
            claim_source = 1u;
            carry_days = old_days;
        }}
    }}
    
    // Fallback: deterministic bank windows (newest-first) after source1 positions.
    // C2: bank_pos = позиция борта среди НЕ получивших source1 = commit_pos - free_total.
    // Уникальна для каждого борта (commit_pos уникален) и НЕ схлопывается в 0, т.к. не
    // зависит от free_line_count внутри неуспешной попытки. Вход только при исчерпании
    // source1-ёмкости (commit_pos >= free_total) → один bank_lock CAS на свою линию.
    if (!claimed && commit_pos >= free_total) {{
        const unsigned int bank_pos = commit_pos - free_total;
        unsigned int seen_bank = 0u;
        bool tried[{REPAIR_LINES_MAX}u];
        for (unsigned int i = 0u; i < max_lines; ++i) {{
            tried[i] = false;
        }}
        
        for (unsigned int attempt = 0u; attempt < max_lines && !claimed; ++attempt) {{
            unsigned int best_line = 0xFFFFFFFFu;
            unsigned int best_end = 0u;
            
            for (unsigned int i = 0u; i < max_lines; ++i) {{
                if (tried[i]) continue;
                if (bank_count_ro[i] == 0u) continue;
                const unsigned int head_end = bank_head_end_ro[i];
                if (head_end == 0xFFFFFFFFu) continue;
                if (best_line == 0xFFFFFFFFu || head_end > best_end || (head_end == best_end && i < best_line)) {{
                    best_line = i;
                    best_end = head_end;
                }}
            }}
            
            if (best_line == 0xFFFFFFFFu) {{
                break;
            }}
            tried[best_line] = true;
            
            if (seen_bank != bank_pos) {{
                ++seen_bank;
                continue;
            }}

            const unsigned int prev_lock = bank_lock_mp[best_line].exchange(1u);
            if (prev_lock != 0u) {{
                break;
            }}
            
            unsigned int bank_count = bank_count_ro[best_line];
            if (bank_count == 0u) {{
                bank_lock_mp[best_line].exchange(0u);
                break;
            }}
            
            const unsigned int base = best_line * {REPAIR_BANK_MAX}u;
            unsigned int window_start = bank_start_mp[base].exchange(0u);
            unsigned int window_end = bank_end_mp[base].exchange(0u);
            if (window_start == 0xFFFFFFFFu || window_end == 0xFFFFFFFFu || window_end <= window_start) {{
                bank_start_mp[base].exchange(window_start);
                bank_end_mp[base].exchange(window_end);
                bank_lock_mp[best_line].exchange(0u);
                break;
            }}
            if (status_change_day > window_start) {{
                bank_start_mp[base].exchange(window_start);
                bank_end_mp[base].exchange(window_end);
                bank_lock_mp[best_line].exchange(0u);
                break;
            }}
            for (unsigned int i = 1u; i < bank_count; ++i) {{
                const unsigned int from = base + i;
                const unsigned int to = base + (i - 1u);
                const unsigned int prev_start = bank_start_mp[from].exchange(0u);
                const unsigned int prev_end = bank_end_mp[from].exchange(0u);
                bank_start_mp[to].exchange(prev_start);
                bank_end_mp[to].exchange(prev_end);
            }}
            const unsigned int tail = base + (bank_count - 1u);
            bank_start_mp[tail].exchange(0xFFFFFFFFu);
            bank_end_mp[tail].exchange(0xFFFFFFFFu);
            bank_count -= 1u;
            bank_count_mp[best_line].exchange(bank_count);
            bank_lock_mp[best_line].exchange(0u);
            
            chosen_line = best_line;
            claimed = true;
            claim_start = window_start;
            claim_end = window_end;
            claim_source = 2u;
        }}
    }}
    
    // Резервирование прошлых окон (newest-first)
    if (claimed && claim_source == 1u && carry_days >= repair_time) {{
        unsigned int windows_total = carry_days / repair_time;
        if (windows_total > 1u) {{
            unsigned int carry = windows_total - 1u;
            if (carry > {REPAIR_BANK_MAX}u) {{
                carry = {REPAIR_BANK_MAX}u;
            }}
            unsigned int bank_count = bank_count_ro[chosen_line];
            for (unsigned int n = carry; n > 0u; --n) {{
                const unsigned int win_idx = n - 1u;
                const unsigned int end_day = current_day - repair_time * (win_idx + 1u);
                const unsigned int start_day = end_day - repair_time;
                
                unsigned int shift_limit = (bank_count < {REPAIR_BANK_MAX}u)
                    ? bank_count
                    : ({REPAIR_BANK_MAX}u - 1u);
                for (int j = (int)shift_limit; j > 0; --j) {{
                    const unsigned int from = chosen_line * {REPAIR_BANK_MAX}u + (unsigned int)(j - 1);
                    const unsigned int to = chosen_line * {REPAIR_BANK_MAX}u + (unsigned int)j;
                    const unsigned int prev_start = bank_start_mp[from].exchange(0u);
                    const unsigned int prev_end = bank_end_mp[from].exchange(0u);
                    bank_start_mp[to].exchange(prev_start);
                    bank_end_mp[to].exchange(prev_end);
                }}
                const unsigned int head = chosen_line * {REPAIR_BANK_MAX}u;
                bank_start_mp[head].exchange(start_day);
                bank_end_mp[head].exchange(end_day);
                if (bank_count < {REPAIR_BANK_MAX}u) {{
                    bank_count += 1u;
                }}
            }}
            bank_count_mp[chosen_line].exchange(bank_count);
        }}
    }}
    
    if (claimed) {{
        line_rt[chosen_line].exchange(repair_time);
        line_last_acn[chosen_line].exchange(acn);
        line_last_day[chosen_line].exchange(current_day);
        line_gb[chosen_line].exchange(group_by);
        FLAMEGPU->setVariable<unsigned int>("repair_line_id", chosen_line);
        FLAMEGPU->setVariable<unsigned int>("promoted", 1u);
        FLAMEGPU->setVariable<unsigned int>("debug_promoted", 1u);
        FLAMEGPU->setVariable<unsigned int>("repair_claim_start_day", claim_start);
        FLAMEGPU->setVariable<unsigned int>("repair_claim_end_day", claim_end);
        FLAMEGPU->setVariable<unsigned int>("repair_claim_source", claim_source);
        
        if (group_by == 1u) {{
            auto p3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_approve_s1");
            p3[idx].exchange(1u);
        }} else {{
            auto p3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_approve_s1");
            p3[idx].exchange(1u);
        }}
    }} else {{
        FLAMEGPU->setVariable<unsigned int>("repair_line_id", 0xFFFFFFFFu);
    }}
    
    FLAMEGPU->setVariable<unsigned int>("repair_candidate", 0u);
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# MacroProperty для слотов RepairLine (Mi-8/Mi-17)
# ═══════════════════════════════════════════════════════════════════════════════

def setup_quota_v8_macroproperties(env):
    """Создаёт MacroProperty для остатков квот"""
    env.newMacroPropertyUInt("qm_ops_mp", 2)
    print("  ✅ V8 MacroProperty: qm_ops_mp")


def register_quota_v8_messages(model, agent, quota_agent):
    """
    V8 квотирование (MessageBucket broadcast):
    - reset buffers + count (MacroProperty)
    - QuotaManager → MessageBucket (одно сообщение)
    - P1/P2/P3 решаются агентами по rank
    - commit P2/P3 (RepairLine)
    """
    print("\n📊 V8: Регистрация квотирования (message-only)...")
    
    # Сброс флагов
    layer_reset_flags = model.newLayer("v8_reset_flags")
    for state in ["inactive", "operations", "serviceable", "repair", "reserve", "storage", "unserviceable"]:
        fn_name = f"rtc_reset_flags_v8_{state}"
        fn = agent.newRTCFunction(fn_name, RTC_RESET_FLAGS)
        fn.setInitialState(state)
        fn.setEndState(state)
        layer_reset_flags.addAgentFunction(fn)
    print("  ✅ Сброс флагов")

    # Сброс буферов подсчёта
    layer_reset_buf = model.newLayer("v8_reset_buffers")
    for state in ["inactive", "operations", "serviceable", "repair", "reserve", "storage", "unserviceable"]:
        fn_name = f"rtc_reset_quota_v8_{state}"
        fn = agent.newRTCFunction(fn_name, RTC_RESET_BUFFERS)
        fn.setInitialState(state)
        fn.setEndState(state)
        layer_reset_buf.addAgentFunction(fn)
    print("  ✅ Сброс буферов")
    
    # Подсчёт агентов по состояниям (V8 readiness)
    layer_count_ops = model.newLayer("v8_count_ops")
    fn = agent.newRTCFunction("rtc_count_ops_v8", RTC_COUNT_OPS)
    fn.setInitialState("operations")
    fn.setEndState("operations")
    layer_count_ops.addAgentFunction(fn)
    
    layer_count_svc = model.newLayer("v8_count_svc")
    fn = agent.newRTCFunction("rtc_count_svc_v8", RTC_COUNT_SVC)
    fn.setInitialState("serviceable")
    fn.setEndState("serviceable")
    layer_count_svc.addAgentFunction(fn)
    
    layer_count_unsvc = model.newLayer("v8_count_unsvc")
    fn = agent.newRTCFunction("rtc_count_unsvc_v8", RTC_COUNT_UNSVC_V8)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")
    layer_count_unsvc.addAgentFunction(fn)
    
    layer_count_inactive = model.newLayer("v8_count_inactive")
    fn = agent.newRTCFunction("rtc_count_inactive_v8", RTC_COUNT_INACTIVE)
    fn.setInitialState("inactive")
    fn.setEndState("inactive")
    layer_count_inactive.addAgentFunction(fn)
    
    # QuotaManager → MessageBucket
    layer_qm = model.newLayer("v8_quota_manager_bucket")
    fn = quota_agent.newRTCFunction("rtc_quota_manager_v8_bucket", RTC_QUOTA_MANAGER_V8_BUCKET)
    fn.setInitialState("default")
    fn.setEndState("default")
    fn.setMessageOutput("QuotaBucket")
    fn.setMessageOutputOptional(True)
    layer_qm.addAgentFunction(fn)
    
    # Демоут (V7)
    layer_demote = model.newLayer("v8_demote")
    fn = agent.newRTCFunction("rtc_demote_ops_v8", RTC_DEMOTE_OPS)
    fn.setInitialState("operations")
    fn.setEndState("operations")
    layer_demote.addAgentFunction(fn)
    
    # P1/P2/P3 (MessageBucket)
    layer_p1 = model.newLayer("v8_promote_svc_bucket")
    fn = agent.newRTCFunction("rtc_promote_svc_bucket_v8", RTC_PROMOTE_SVC_BUCKET_V8)
    fn.setInitialState("serviceable")
    fn.setEndState("serviceable")
    fn.setMessageInput("QuotaBucket")
    layer_p1.addAgentFunction(fn)
    
    layer_p2 = model.newLayer("v8_promote_unsvc_bucket")
    fn = agent.newRTCFunction("rtc_promote_unsvc_bucket_v8", RTC_PROMOTE_UNSVC_BUCKET_V8)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")
    fn.setMessageInput("QuotaBucket")
    layer_p2.addAgentFunction(fn)
    
    layer_p3 = model.newLayer("v8_promote_inactive_bucket")
    fn = agent.newRTCFunction("rtc_promote_inactive_bucket_v8", RTC_PROMOTE_INACTIVE_BUCKET_V8)
    fn.setInitialState("inactive")
    fn.setEndState("inactive")
    fn.setMessageInput("QuotaBucket")
    layer_p3.addAgentFunction(fn)
    
    # Snapshot RepairLine MacroProperty (RO) перед commit P2/P3
    layer_snapshot = model.newLayer("v8_repair_line_snapshot")
    fn = quota_agent.newRTCFunction("rtc_repair_line_snapshot_v8", RTC_REPAIR_LINE_SNAPSHOT_V8)
    fn.setInitialState("default")
    fn.setEndState("default")
    layer_snapshot.addAgentFunction(fn)
    
    # Commit P2/P3
    layer_p2_commit = model.newLayer("v8_promote_unsvc_commit")
    fn = agent.newRTCFunction("rtc_promote_unsvc_commit_v8", RTC_PROMOTE_UNSVC_COMMIT_V8)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")
    layer_p2_commit.addAgentFunction(fn)
    
    # C1: пересоздать RO-снэпшот после P2-commit, чтобы P3 видел занятые P2 линии/банк
    layer_snapshot_p3 = model.newLayer("v8_repair_line_snapshot_p3")
    fn = quota_agent.newRTCFunction("rtc_repair_line_snapshot_v8_p3", RTC_REPAIR_LINE_SNAPSHOT_V8_P3)
    fn.setInitialState("default")
    fn.setEndState("default")
    layer_snapshot_p3.addAgentFunction(fn)

    layer_p3_commit = model.newLayer("v8_promote_inactive_commit")
    fn = agent.newRTCFunction("rtc_promote_inactive_commit_v8", RTC_PROMOTE_INACTIVE_COMMIT_V8)
    fn.setInitialState("inactive")
    fn.setEndState("inactive")
    layer_p3_commit.addAgentFunction(fn)
    
    print("  ✅ V8 MessageBucket квоты зарегистрированы")




def register_post_quota_counts_v8(model: fg.ModelDescription, agent: fg.AgentDescription):
    """
    Пересчёт буферов после пост-квотных переходов для V8.
    Использует readiness unserviceable по repair_days.
    """
    print("\n📊 V8: Пересчёт буферов ПОСЛЕ квотирования...")
    
    # Сброс буферов (idx=0 из ЛЮБОГО состояния)
    layer_reset_buf = model.newLayer("v8_reset_buffers_post_quota")
    for state in ["inactive", "operations", "serviceable", "repair", "reserve", "storage", "unserviceable"]:
        fn_name = f"rtc_reset_quota_v8_post_{state}"
        fn = agent.newRTCFunction(fn_name, RTC_RESET_BUFFERS)
        fn.setInitialState(state)
        fn.setEndState(state)
        layer_reset_buf.addAgentFunction(fn)
    print("  ✅ Сброс буферов (post)")
    
    # Подсчёт агентов
    layer_count = model.newLayer("v8_count_agents_post_quota")
    
    fn = agent.newRTCFunction("rtc_count_ops_v8_post", RTC_COUNT_OPS)
    fn.setInitialState("operations")
    fn.setEndState("operations")
    layer_count.addAgentFunction(fn)
    
    fn = agent.newRTCFunction("rtc_count_svc_v8_post", RTC_COUNT_SVC)
    fn.setInitialState("serviceable")
    fn.setEndState("serviceable")
    layer_count.addAgentFunction(fn)
    
    fn = agent.newRTCFunction("rtc_count_unsvc_v8_post", RTC_COUNT_UNSVC_V8)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")
    layer_count.addAgentFunction(fn)
    
    fn = agent.newRTCFunction("rtc_count_inactive_v8_post", RTC_COUNT_INACTIVE)
    fn.setInitialState("inactive")
    fn.setEndState("inactive")
    layer_count.addAgentFunction(fn)
    
    print("  ✅ Подсчёт агентов (post)")

    # REMOVED (dedup-spawn-recount): слои v8_reset_buffers_spawn + v8_count_agents_spawn удалены.
    # Между count_post и spawn-слоями популяции состояний не меняются, а count_* пишут в те же
    # mi*_count буферы → spawn-пересчёт давал значения, идентичные count_post. commit_p1/p2/p3
    # уже обнулены reset_post (spawn их не читает). Профиль: ~18% GPU-kernel time + треть memset.
    # ИНВАРИАНТ: между v8_count_agents_post_quota и v8_spawn_dynamic_mgr ЗАПРЕЩЕНЫ слои,
    # мутирующие heli-популяции или mi*_*_count буферы — иначе spawn увидит устаревший ops_count.
    print("✅ Post-quota пересчёт зарегистрирован\n")

