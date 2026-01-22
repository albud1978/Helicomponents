#!/usr/bin/env python3
"""
RTC модуль V8: Квотирование через RepairLine

АРХИТЕКТУРА V8 (отличия от V7):
1. P2/P3 используют слоты RepairLine (free_days >= repair_time)
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

См. docs/adaptive_steps_logic.md для полной архитектуры.

Дата: 16.01.2026
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from model_build import RTC_MAX_FRAMES, MAX_DAYS

# Максимум ремонтных линий (MacroProperty размер)
REPAIR_LINES_MAX = 64

import pyflamegpu as fg

# Локальные копии V7-квот для V8 (reset/count/demote/P1/post)
from rtc_quota_v8_base import (
    RTC_RESET_FLAGS,
    RTC_RESET_BUFFERS,
    RTC_RESET_BUFFERS_SPAWN,
    RTC_COUNT_OPS,
    RTC_COUNT_SVC,
    RTC_COUNT_INACTIVE,
    RTC_DEMOTE_OPS,
    RTC_PROMOTE_SVC,
    RTC_PROMOTE_INACTIVE_POST,
    RTC_INACTIVE_TO_OPS_POST,
    COND_INACTIVE_PROMOTED_POST,
)
from rtc_publish_report import register_rtc as register_publish_report
from rtc_apply_decisions import register_rtc as register_apply_decisions

# ═══════════════════════════════════════════════════════════════════════════════
# V8: Слоты RepairLine (RepairLine → QM, адресные MessageArray)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_REPAIR_LINE_SLOTS_V8 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_repair_line_slots_v8, flamegpu::MessageArray, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    if (group_by != 1u) return flamegpu::ALIVE;  // один QM формирует слоты
    
    const unsigned int repair_quota = FLAMEGPU->environment.getProperty<unsigned int>("repair_quota");
    const unsigned int max_lines = (repair_quota < {REPAIR_LINES_MAX}u) ? repair_quota : {REPAIR_LINES_MAX}u;
    
    auto slots_all = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_slots_all");
    auto slots_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_slots_days");
    auto slots_count = FLAMEGPU->environment.getMacroProperty<unsigned int, 1u>("repair_line_slots_count_mp");
    
    unsigned int ids[{REPAIR_LINES_MAX}u];
    unsigned int days[{REPAIR_LINES_MAX}u];
    unsigned int count = 0u;
    
    for (unsigned int i = 0u; i < max_lines; ++i) {{
        slots_all[i].exchange(0xFFFFFFFFu);
        slots_days[i].exchange(0u);
        ids[i] = 0xFFFFFFFFu;
    }}
    
    for (unsigned int i = 0u; i < max_lines; ++i) {{
        auto msg = FLAMEGPU->message_in.at(i);
        const unsigned int free_days = msg.getVariable<unsigned int>("free_days");
        const unsigned int acn = msg.getVariable<unsigned int>("aircraft_number");
        
        if (acn == 0u && count < {REPAIR_LINES_MAX}u) {{
            unsigned int pos = count;
            while (pos > 0u && free_days < days[pos - 1u]) {{
                days[pos] = days[pos - 1u];
                ids[pos] = ids[pos - 1u];
                --pos;
            }}
            days[pos] = free_days;
            ids[pos] = i;
            ++count;
        }}
    }}
    
    for (unsigned int i = 0u; i < count; ++i) {{
        slots_all[i].exchange(ids[i]);
        slots_days[i].exchange(days[i]);
    }}
    
    slots_count[0].exchange(count);
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# V8: QuotaManager (message-only, единый модуль)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_QUOTA_MANAGER_V8_MSG = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_quota_manager_v8_msg, flamegpu::MessageBruteForce, flamegpu::MessageArray) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    if (group_by != 1u) return flamegpu::ALIVE;  // один QM для всех типов
    
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    unsigned int target_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
    
    const unsigned int target_mi8 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", target_day);
    const unsigned int target_mi17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", target_day);
    
    const unsigned int mi8_rt = FLAMEGPU->environment.getProperty<unsigned int>("mi8_repair_time_const");
    const unsigned int mi17_rt = FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    
    // Буферы кандидатов (локальные массивы)
    unsigned short ops_mi8[{RTC_MAX_FRAMES}u];
    unsigned short ops_mi17[{RTC_MAX_FRAMES}u];
    unsigned short svc_mi8[{RTC_MAX_FRAMES}u];
    unsigned short svc_mi17[{RTC_MAX_FRAMES}u];
    unsigned short unsvc_idx[{RTC_MAX_FRAMES}u];
    unsigned char unsvc_group[{RTC_MAX_FRAMES}u];
    unsigned short ina_idx[{RTC_MAX_FRAMES}u];
    unsigned char ina_group[{RTC_MAX_FRAMES}u];
    
    unsigned int ops_cnt_mi8 = 0u;
    unsigned int ops_cnt_mi17 = 0u;
    unsigned int svc_cnt_mi8 = 0u;
    unsigned int svc_cnt_mi17 = 0u;
    unsigned int unsvc_cnt = 0u;
    unsigned int ina_cnt = 0u;
    
    // Сбор сообщений
    for (const auto& msg : FLAMEGPU->message_in) {{
        const unsigned int idx = msg.getVariable<unsigned short>("idx");
        const unsigned char msg_group = msg.getVariable<unsigned char>("group_by");
        const unsigned char state = msg.getVariable<unsigned char>("state");
        const unsigned int repair_days = msg.getVariable<unsigned int>("repair_days");
        const unsigned int repair_line_id = msg.getVariable<unsigned int>("repair_line_id");
        const unsigned char skip_repair = msg.getVariable<unsigned char>("skip_repair");
        
        if (state == 6u) continue;  // storage не участвует
        
        if (state == 2u) {{
            if (msg_group == 1u && ops_cnt_mi8 < {RTC_MAX_FRAMES}u) {{
                ops_mi8[ops_cnt_mi8++] = (unsigned short)idx;
            }} else if (msg_group == 2u && ops_cnt_mi17 < {RTC_MAX_FRAMES}u) {{
                ops_mi17[ops_cnt_mi17++] = (unsigned short)idx;
            }}
        }} else if (state == 3u) {{
            if (msg_group == 1u && svc_cnt_mi8 < {RTC_MAX_FRAMES}u) {{
                svc_mi8[svc_cnt_mi8++] = (unsigned short)idx;
            }} else if (msg_group == 2u && svc_cnt_mi17 < {RTC_MAX_FRAMES}u) {{
                svc_mi17[svc_cnt_mi17++] = (unsigned short)idx;
            }}
        }} else if (state == 7u) {{
            if (repair_days == 0u && repair_line_id == 0xFFFFFFFFu) {{
                if (unsvc_cnt < {RTC_MAX_FRAMES}u) {{
                    unsvc_idx[unsvc_cnt] = (unsigned short)idx;
                    unsvc_group[unsvc_cnt] = msg_group;
                    ++unsvc_cnt;
                }}
            }}
        }} else if (state == 1u) {{
            unsigned int rt = (msg_group == 1u) ? mi8_rt : mi17_rt;
            if (day >= rt || skip_repair == 1u) {{
                if (ina_cnt < {RTC_MAX_FRAMES}u) {{
                    ina_idx[ina_cnt] = (unsigned short)idx;
                    ina_group[ina_cnt] = msg_group;
                    ++ina_cnt;
                }}
            }}
        }}
    }}
    
    unsigned int quota_left_mi8 = (target_mi8 > ops_cnt_mi8) ? (target_mi8 - ops_cnt_mi8) : 0u;
    unsigned int quota_left_mi17 = (target_mi17 > ops_cnt_mi17) ? (target_mi17 - ops_cnt_mi17) : 0u;
    const unsigned int deficit_mi8 = quota_left_mi8;
    const unsigned int deficit_mi17 = quota_left_mi17;
    
    // Пишем дефицит ДО коммитов (дальше будет декремент по факту)
    auto qm_ops_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, 2u>("qm_ops_mp");
    qm_ops_mp[0].exchange(ops_cnt_mi8);
    qm_ops_mp[1].exchange(ops_cnt_mi17);
    
    // Сбрасываем commit-флаги (будут выставлены по факту переходов)
    auto mi8_p1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_approve_s3");
    auto mi17_p1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_approve_s3");
    auto mi8_p2 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_approve");
    auto mi17_p2 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_approve");
    auto mi8_p3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_approve_s1");
    auto mi17_p3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_approve_s1");
    for (unsigned int i = 0u; i < {RTC_MAX_FRAMES}u; ++i) {{
        mi8_p1[i].exchange(0u);
        mi17_p1[i].exchange(0u);
        mi8_p2[i].exchange(0u);
        mi17_p2[i].exchange(0u);
        mi8_p3[i].exchange(0u);
        mi17_p3[i].exchange(0u);
    }}
    
    if (quota_left_mi8 == 0u && quota_left_mi17 == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Demote по типам (oldest first = меньший idx)
    if (ops_cnt_mi8 > target_mi8) {{
        unsigned int K = ops_cnt_mi8 - target_mi8;
        for (unsigned int i = 0u; i + 1u < ops_cnt_mi8; ++i) {{
            for (unsigned int j = 0u; j + 1u < ops_cnt_mi8 - i; ++j) {{
                if (ops_mi8[j] > ops_mi8[j + 1u]) {{
                    unsigned short tmp = ops_mi8[j];
                    ops_mi8[j] = ops_mi8[j + 1u];
                    ops_mi8[j + 1u] = tmp;
                }}
            }}
        }}
        if (K > ops_cnt_mi8) K = ops_cnt_mi8;
        for (unsigned int i = 0u; i < K; ++i) {{
        FLAMEGPU->message_out.setIndex(ops_mi8[i]);
            FLAMEGPU->message_out.setVariable<unsigned char>("action", 1u);
            FLAMEGPU->message_out.setVariable<unsigned char>("group_by", 1u);
            FLAMEGPU->message_out.setVariable<unsigned int>("line_id", 0xFFFFFFFFu);
        }}
        quota_left_mi8 = 0u;
    }}
    if (ops_cnt_mi17 > target_mi17) {{
        unsigned int K = ops_cnt_mi17 - target_mi17;
        for (unsigned int i = 0u; i + 1u < ops_cnt_mi17; ++i) {{
            for (unsigned int j = 0u; j + 1u < ops_cnt_mi17 - i; ++j) {{
                if (ops_mi17[j] > ops_mi17[j + 1u]) {{
                    unsigned short tmp = ops_mi17[j];
                    ops_mi17[j] = ops_mi17[j + 1u];
                    ops_mi17[j + 1u] = tmp;
                }}
            }}
        }}
        if (K > ops_cnt_mi17) K = ops_cnt_mi17;
        for (unsigned int i = 0u; i < K; ++i) {{
        FLAMEGPU->message_out.setIndex(ops_mi17[i]);
            FLAMEGPU->message_out.setVariable<unsigned char>("action", 1u);
            FLAMEGPU->message_out.setVariable<unsigned char>("group_by", 2u);
            FLAMEGPU->message_out.setVariable<unsigned int>("line_id", 0xFFFFFFFFu);
        }}
        quota_left_mi17 = 0u;
    }}
    
    // P1: serviceable (youngest first = больший idx)
    if (quota_left_mi8 > 0u && svc_cnt_mi8 > 0u) {{
        for (unsigned int i = 0u; i + 1u < svc_cnt_mi8; ++i) {{
            for (unsigned int j = 0u; j + 1u < svc_cnt_mi8 - i; ++j) {{
                if (svc_mi8[j] < svc_mi8[j + 1u]) {{
                    unsigned short tmp = svc_mi8[j];
                    svc_mi8[j] = svc_mi8[j + 1u];
                    svc_mi8[j + 1u] = tmp;
                }}
            }}
        }}
        unsigned int K = (quota_left_mi8 < svc_cnt_mi8) ? quota_left_mi8 : svc_cnt_mi8;
        for (unsigned int i = 0u; i < K; ++i) {{
        FLAMEGPU->message_out.setIndex(svc_mi8[i]);
            FLAMEGPU->message_out.setVariable<unsigned char>("action", 2u);
            FLAMEGPU->message_out.setVariable<unsigned char>("group_by", 1u);
            FLAMEGPU->message_out.setVariable<unsigned int>("line_id", 0xFFFFFFFFu);
        }}
        quota_left_mi8 -= K;
    }}
    if (quota_left_mi17 > 0u && svc_cnt_mi17 > 0u) {{
        for (unsigned int i = 0u; i + 1u < svc_cnt_mi17; ++i) {{
            for (unsigned int j = 0u; j + 1u < svc_cnt_mi17 - i; ++j) {{
                if (svc_mi17[j] < svc_mi17[j + 1u]) {{
                    unsigned short tmp = svc_mi17[j];
                    svc_mi17[j] = svc_mi17[j + 1u];
                    svc_mi17[j + 1u] = tmp;
                }}
            }}
        }}
        unsigned int K = (quota_left_mi17 < svc_cnt_mi17) ? quota_left_mi17 : svc_cnt_mi17;
        for (unsigned int i = 0u; i < K; ++i) {{
        FLAMEGPU->message_out.setIndex(svc_mi17[i]);
            FLAMEGPU->message_out.setVariable<unsigned char>("action", 2u);
            FLAMEGPU->message_out.setVariable<unsigned char>("group_by", 2u);
            FLAMEGPU->message_out.setVariable<unsigned int>("line_id", 0xFFFFFFFFu);
        }}
        quota_left_mi17 -= K;
    }}
    
    auto line_days_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_free_days_mp");
    auto line_acn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_acn_mp");
    unsigned int line_ids[{REPAIR_LINES_MAX}u];
    unsigned int line_days[{REPAIR_LINES_MAX}u];
    unsigned int slots = 0u;
    for (unsigned int i = 0u; i < {REPAIR_LINES_MAX}u; ++i) {{
        line_ids[i] = 0xFFFFFFFFu;
        line_days[i] = 0u;
    }}
    for (unsigned int i = 0u; i < {REPAIR_LINES_MAX}u; ++i) {{
        const unsigned int acn = line_acn_mp[i];
        if (acn != 0u) continue;
        const unsigned int free_days = line_days_mp[i];
        if (slots >= {REPAIR_LINES_MAX}u) break;
        unsigned int pos = slots;
        while (pos > 0u && free_days < line_days[pos - 1u]) {{
            line_days[pos] = line_days[pos - 1u];
            line_ids[pos] = line_ids[pos - 1u];
            --pos;
        }}
        line_days[pos] = free_days;
        line_ids[pos] = i;
        ++slots;
    }}
    unsigned int slots_left = slots;
    unsigned int used_slots[{REPAIR_LINES_MAX}u];
    for (unsigned int i = 0u; i < {REPAIR_LINES_MAX}u; ++i) used_slots[i] = 0u;
    
    // P2: unsvc (youngest first)
    if (unsvc_cnt > 1u) {{
        for (unsigned int i = 0u; i + 1u < unsvc_cnt; ++i) {{
            for (unsigned int j = 0u; j + 1u < unsvc_cnt - i; ++j) {{
                if (unsvc_idx[j] < unsvc_idx[j + 1u]) {{
                    unsigned short t_idx = unsvc_idx[j];
                    unsigned char t_grp = unsvc_group[j];
                    unsvc_idx[j] = unsvc_idx[j + 1u];
                    unsvc_group[j] = unsvc_group[j + 1u];
                    unsvc_idx[j + 1u] = t_idx;
                    unsvc_group[j + 1u] = t_grp;
                }}
            }}
        }}
    }}
    
    for (unsigned int i = 0u; i < unsvc_cnt; ++i) {{
        unsigned char g = unsvc_group[i];
        if (g == 1u && quota_left_mi8 == 0u) continue;
        if (g == 2u && quota_left_mi17 == 0u) continue;
        if (slots_left == 0u) break;
        
        unsigned int repair_time = (g == 1u) ? mi8_rt : mi17_rt;
        unsigned int chosen_line = 0xFFFFFFFFu;
        for (unsigned int s = 0u; s < slots; ++s) {{
            if (used_slots[s] == 1u) continue;
            if (line_days[s] >= repair_time) {{
                chosen_line = line_ids[s];
                used_slots[s] = 1u;
                break;
            }}
        }}
        if (chosen_line == 0xFFFFFFFFu) continue;
        
        FLAMEGPU->message_out.setIndex(unsvc_idx[i]);
        FLAMEGPU->message_out.setVariable<unsigned char>("action", 3u);
        FLAMEGPU->message_out.setVariable<unsigned char>("group_by", g);
        FLAMEGPU->message_out.setVariable<unsigned int>("line_id", chosen_line);
        
        if (g == 1u) quota_left_mi8--;
        else quota_left_mi17--;
        if (slots_left > 0u) slots_left--;
    }}
    
    // P3: inactive (youngest first), после P2
    if (ina_cnt > 1u) {{
        for (unsigned int i = 0u; i + 1u < ina_cnt; ++i) {{
            for (unsigned int j = 0u; j + 1u < ina_cnt - i; ++j) {{
                if (ina_idx[j] < ina_idx[j + 1u]) {{
                    unsigned short t_idx = ina_idx[j];
                    unsigned char t_grp = ina_group[j];
                    ina_idx[j] = ina_idx[j + 1u];
                    ina_group[j] = ina_group[j + 1u];
                    ina_idx[j + 1u] = t_idx;
                    ina_group[j + 1u] = t_grp;
                }}
            }}
        }}
    }}
    
    for (unsigned int i = 0u; i < ina_cnt; ++i) {{
        unsigned char g = ina_group[i];
        if (g == 1u && quota_left_mi8 == 0u) continue;
        if (g == 2u && quota_left_mi17 == 0u) continue;
        if (slots_left == 0u) break;
        
        unsigned int repair_time = (g == 1u) ? mi8_rt : mi17_rt;
        unsigned int chosen_line = 0xFFFFFFFFu;
        for (unsigned int s = 0u; s < slots; ++s) {{
            if (used_slots[s] == 1u) continue;
            if (line_days[s] >= repair_time) {{
                chosen_line = line_ids[s];
                used_slots[s] = 1u;
                break;
            }}
        }}
        if (chosen_line == 0xFFFFFFFFu) continue;
        
        FLAMEGPU->message_out.setIndex(ina_idx[i]);
        FLAMEGPU->message_out.setVariable<unsigned char>("action", 4u);
        FLAMEGPU->message_out.setVariable<unsigned char>("group_by", g);
        FLAMEGPU->message_out.setVariable<unsigned int>("line_id", chosen_line);
        
        if (g == 1u) quota_left_mi8--;
        else quota_left_mi17--;
        if (slots_left > 0u) slots_left--;
    }}
    
    // Debug: итоги QM (по входному дефициту)
    FLAMEGPU->setVariable<unsigned int>("debug_qm_ops_mi8", ops_cnt_mi8);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_ops_mi17", ops_cnt_mi17);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_target_mi8", target_mi8);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_target_mi17", target_mi17);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_quota_left_mi8", deficit_mi8);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_quota_left_mi17", deficit_mi17);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_unsvc_cnt", unsvc_cnt);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_inactive_cnt", ina_cnt);
    
    return flamegpu::ALIVE;
}}
"""

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
    const unsigned int frames = {RTC_MAX_FRAMES}u;
    
    // Буферы подсчёта
    auto ops_mi8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_ops_count");
    auto ops_mi17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_ops_count");
    auto svc_mi8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_svc_count");
    auto svc_mi17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_svc_count");
    auto unsvc_ready_mi8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_unsvc_ready_count");
    auto unsvc_ready_mi17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_unsvc_ready_count");
    auto ina_mi8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_inactive_count");
    auto ina_mi17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_inactive_count");
    
    unsigned int ops8 = 0u, ops17 = 0u;
    unsigned int svc8 = 0u, svc17 = 0u;
    unsigned int unsvc8 = 0u, unsvc17 = 0u;
    unsigned int ina8 = 0u, ina17 = 0u;
    
    for (unsigned int i = 0u; i < frames; ++i) {{
        ops8 += ops_mi8[i];
        ops17 += ops_mi17[i];
        svc8 += svc_mi8[i];
        svc17 += svc_mi17[i];
        unsvc8 += unsvc_ready_mi8[i];
        unsvc17 += unsvc_ready_mi17[i];
        ina8 += ina_mi8[i];
        ina17 += ina_mi17[i];
    }}

    // Передаём текущие ops в MacroProperty для SpawnManager
    auto qm_ops_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, 2u>("qm_ops_mp");
    qm_ops_mp[0].exchange(ops8);
    qm_ops_mp[1].exchange(ops17);
    
    const unsigned int ina_ready8 = (day >= mi8_rt) ? ina8 : 0u;
    const unsigned int ina_ready17 = (day >= mi17_rt) ? ina17 : 0u;
    
    unsigned int deficit8 = (target_mi8 > ops8) ? (target_mi8 - ops8) : 0u;
    unsigned int deficit17 = (target_mi17 > ops17) ? (target_mi17 - ops17) : 0u;
    
    unsigned int p1_8 = (deficit8 < svc8) ? deficit8 : svc8;
    deficit8 -= p1_8;
    unsigned int p1_17 = (deficit17 < svc17) ? deficit17 : svc17;
    deficit17 -= p1_17;
    
    unsigned int deficit_total = deficit8 + deficit17;
    const unsigned int unsvc_total = unsvc8 + unsvc17;
    unsigned int p2_total = (deficit_total < unsvc_total) ? deficit_total : unsvc_total;
    deficit_total -= p2_total;
    
    const unsigned int ina_total = ina_ready8 + ina_ready17;
    const unsigned int p3_total = (deficit_total < ina_total) ? deficit_total : ina_total;
    
    FLAMEGPU->message_out.setKey(0);
    FLAMEGPU->message_out.setVariable<unsigned int>("promote_p1_mi8", p1_8);
    FLAMEGPU->message_out.setVariable<unsigned int>("promote_p1_mi17", p1_17);
    FLAMEGPU->message_out.setVariable<unsigned int>("promote_p2_total", p2_total);
    FLAMEGPU->message_out.setVariable<unsigned int>("promote_p3_total", p3_total);
    FLAMEGPU->message_out.setVariable<unsigned int>("deficit_mi8", deficit8);
    FLAMEGPU->message_out.setVariable<unsigned int>("deficit_mi17", deficit17);
    
    // Debug: итоги QM
    FLAMEGPU->setVariable<unsigned int>("debug_qm_ops_mi8", ops8);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_ops_mi17", ops17);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_target_mi8", target_mi8);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_target_mi17", target_mi17);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_quota_left_mi8", deficit8);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_quota_left_mi17", deficit17);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_unsvc_cnt", unsvc_total);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_inactive_cnt", ina_total);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_p1_mi8", p1_8);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_p1_mi17", p1_17);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_p2_total", p2_total);
    FLAMEGPU->setVariable<unsigned int>("debug_qm_p3_total", p3_total);
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
    if (group_by != 1u && group_by != 2u) return flamegpu::ALIVE;
    if (repair_days != 0u || repair_line_id != 0xFFFFFFFFu) return flamegpu::ALIVE;
    
    unsigned int promote_total = 0u;
    unsigned int deficit8 = 0u;
    unsigned int deficit17 = 0u;
    for (auto &msg : FLAMEGPU->message_in(0)) {{
        FLAMEGPU->setVariable<unsigned int>("debug_bucket_seen", 1u);
        promote_total = msg.getVariable<unsigned int>("promote_p2_total");
        deficit8 = msg.getVariable<unsigned int>("deficit_mi8");
        deficit17 = msg.getVariable<unsigned int>("deficit_mi17");
        break;
    }}
    if (promote_total == 0u) return flamegpu::ALIVE;
    if (group_by == 1u && deficit8 == 0u) return flamegpu::ALIVE;
    if (group_by == 2u && deficit17 == 0u) return flamegpu::ALIVE;
    
    const unsigned int frames = {RTC_MAX_FRAMES}u;
    auto unsvc8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_unsvc_ready_count");
    auto unsvc17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_unsvc_ready_count");
    const unsigned int enable8 = (deficit8 > 0u) ? 1u : 0u;
    const unsigned int enable17 = (deficit17 > 0u) ? 1u : 0u;
    
    unsigned int rank = 0u;
    for (unsigned int i = 0u; i < frames; ++i) {{
        if (i <= idx) continue;
        rank += (enable8 ? unsvc8[i] : 0u);
        rank += (enable17 ? unsvc17[i] : 0u);
    }}
    
    if (rank < promote_total) {{
        FLAMEGPU->setVariable<unsigned int>("repair_candidate", 1u);
        FLAMEGPU->setVariable<unsigned int>("repair_line_id", 0xFFFFFFFFu);
        FLAMEGPU->setVariable<unsigned int>("decision_p2", 1u);
    }}
    return flamegpu::ALIVE;
}}
"""

RTC_PROMOTE_INACTIVE_BUCKET_V8 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_promote_inactive_bucket_v8, flamegpu::MessageBucket, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    if (group_by != 1u && group_by != 2u) return flamegpu::ALIVE;
    if (repair_days != 0u) return flamegpu::ALIVE;
    
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int rt = (group_by == 1u)
        ? FLAMEGPU->environment.getProperty<unsigned int>("mi8_repair_time_const")
        : FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    if (day < rt) return flamegpu::ALIVE;
    
    unsigned int promote_total = 0u;
    unsigned int deficit8 = 0u;
    unsigned int deficit17 = 0u;
    for (auto &msg : FLAMEGPU->message_in(0)) {{
        FLAMEGPU->setVariable<unsigned int>("debug_bucket_seen", 1u);
        promote_total = msg.getVariable<unsigned int>("promote_p3_total");
        deficit8 = msg.getVariable<unsigned int>("deficit_mi8");
        deficit17 = msg.getVariable<unsigned int>("deficit_mi17");
        break;
    }}
    if (promote_total == 0u) return flamegpu::ALIVE;
    if (group_by == 1u && deficit8 == 0u) return flamegpu::ALIVE;
    if (group_by == 2u && deficit17 == 0u) return flamegpu::ALIVE;
    
    const unsigned int frames = {RTC_MAX_FRAMES}u;
    auto ina8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_inactive_count");
    auto ina17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_inactive_count");
    const unsigned int enable8 = (deficit8 > 0u) ? 1u : 0u;
    const unsigned int enable17 = (deficit17 > 0u) ? 1u : 0u;
    
    unsigned int rank = 0u;
    for (unsigned int i = 0u; i < frames; ++i) {{
        if (i <= idx) continue;
        rank += (enable8 ? ina8[i] : 0u);
        rank += (enable17 ? ina17[i] : 0u);
    }}
    
    if (rank < promote_total) {{
        FLAMEGPU->setVariable<unsigned int>("repair_candidate", 1u);
        FLAMEGPU->setVariable<unsigned int>("repair_line_id", 0xFFFFFFFFu);
        FLAMEGPU->setVariable<unsigned int>("decision_p3", 1u);
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

# ═══════════════════════════════════════════════════════════════════════════════
# V8: P2 ПРОМОУТ через слоты RepairLine
# ═══════════════════════════════════════════════════════════════════════════════

RTC_PROMOTE_UNSVC_V8 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_promote_unsvc_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // V8: P2 unserviceable → operations
    // Перевод только при наличии окна ремонта в прошлом
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int frames = {RTC_MAX_FRAMES}u;
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    
    // repair_time по типу
    unsigned int repair_time = 0u;
    if (group_by == 1u) {{
        repair_time = FLAMEGPU->environment.getProperty<unsigned int>("mi8_repair_time_const");
    }} else if (group_by == 2u) {{
        repair_time = FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    }} else {{
        return flamegpu::ALIVE;
    }}
    
    // Глобальный барьер: не раньше repair_time от day0
    if (day < repair_time) {{
        return flamegpu::ALIVE;
    }}
    
    // Планер должен досидеть repair_days до 0
    if (repair_days > 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Доступные линии ремонта (слоты) по типу
    auto slots_count_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, 2u>("repair_line_slots_count_mp");
    const unsigned int slots_count = (group_by == 1u) ? slots_count_mp[0] : slots_count_mp[1];
    auto slots = (group_by == 1u) ?
        FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_slots_mi8") :
        FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_slots_mi17");
    auto line_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_free_days_mp");
    
    unsigned int available_lines = slots_count;
    if (available_lines == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Читаем target из mp4 (PropertyArray, не MacroProperty!)
    const unsigned int safe_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
    
    unsigned int ops_curr = 0u;
    unsigned int svc_available = 0u;
    unsigned int unsvc_available = 0u;
    unsigned int target = 0u;
    
    // Подсчёт по типу
    if (group_by == 1u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_svc_count");
        auto unsvc_ready = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_unsvc_ready_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            ops_curr += ops_count[i];
            svc_available += svc_count[i];
            unsvc_available += unsvc_ready[i];
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", safe_day);
    }} else if (group_by == 2u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_svc_count");
        auto unsvc_ready = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_unsvc_ready_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            ops_curr += ops_count[i];
            svc_available += svc_count[i];
            unsvc_available += unsvc_ready[i];
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);
    }} else {{
        return flamegpu::ALIVE;
    }}
    
    // P1 промоутит всех svc — учитываем
    unsigned int deficit_p1 = (target > ops_curr) ? (target - ops_curr) : 0u;
    unsigned int p1_will_promote = (deficit_p1 < svc_available) ? deficit_p1 : svc_available;
    unsigned int curr_after_p1 = ops_curr + p1_will_promote;
    
    // P2 дефицит
    if (curr_after_p1 >= target) {{
        return flamegpu::ALIVE;
    }}
    unsigned int deficit = target - curr_after_p1;
    
    // Промоут по дефициту с учётом доступных линий
    unsigned int needed = (deficit < unsvc_available) ? deficit : unsvc_available;
    
    if (available_lines < needed) {{
        needed = available_lines;
    }}
    
    if (needed == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Ранжирование по idx
    auto unsvc_ready = (group_by == 1u) ? 
        FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_unsvc_ready_count") :
        FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_unsvc_ready_count");
    
    unsigned int rank = 0u;
    for (unsigned int i = 0u; i < idx; ++i) {{
        rank += unsvc_ready[i];
    }}
    
    if (rank < needed) {{
        const unsigned int line_id = slots[rank];
        if (line_id != 0xFFFFFFFFu) {{
            const unsigned int best_days = line_mp[line_id];
            if (best_days >= repair_time) {{
                FLAMEGPU->setVariable<unsigned int>("repair_candidate", 1u);
                FLAMEGPU->setVariable<unsigned int>("repair_line_id", line_id);
                FLAMEGPU->setVariable<unsigned int>("repair_line_day", best_days);
                FLAMEGPU->setVariable<unsigned int>("debug_repair_candidate", 1u);
                FLAMEGPU->setVariable<unsigned int>("debug_repair_line_id", line_id);
                FLAMEGPU->setVariable<unsigned int>("debug_repair_line_day", best_days);
            }}
        }}
    }}
    
    return flamegpu::ALIVE;
}}
"""


RTC_PROMOTE_UNSVC_COMMIT_V8 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_promote_unsvc_commit_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int candidate = FLAMEGPU->getVariable<unsigned int>("repair_candidate");
    if (candidate == 0u) return flamegpu::ALIVE;
    
    const unsigned int line_id = FLAMEGPU->getVariable<unsigned int>("repair_line_id");
    
    auto line_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_free_days_mp");
    auto line_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_acn_mp");
    auto line_rt = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_rt_mp");
    auto line_last_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_last_acn_mp");
    auto line_last_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_last_day_mp");
    
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int acn = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int repair_time = (group_by == 1u)
        ? FLAMEGPU->environment.getProperty<unsigned int>("mi8_repair_time_const")
        : FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    
    const unsigned int repair_quota = FLAMEGPU->environment.getProperty<unsigned int>("repair_quota");
    const unsigned int max_lines = (repair_quota < {REPAIR_LINES_MAX}u) ? repair_quota : {REPAIR_LINES_MAX}u;
    
    bool claimed = false;
    unsigned int chosen_line = 0xFFFFFFFFu;
    
    if (line_id != 0xFFFFFFFFu) {{
        const unsigned int prev_acn = line_acn[line_id].exchange(acn);
        if (prev_acn == 0u) {{
            const unsigned int old_days = line_mp[line_id].exchange(0u);
            if (old_days >= repair_time) {{
                chosen_line = line_id;
                claimed = true;
            }} else {{
                line_mp[line_id].exchange(old_days);
                line_acn[line_id].exchange(0u);
            }}
        }} else {{
            line_acn[line_id].exchange(prev_acn);
        }}
    }}
    
    if (!claimed) {{
        for (unsigned int i = 0u; i < max_lines; ++i) {{
            const unsigned int prev_acn = line_acn[i].exchange(acn);
            if (prev_acn != 0u) {{
                line_acn[i].exchange(prev_acn);
                continue;
            }}
            const unsigned int old_days = line_mp[i].exchange(0u);
            if (old_days < repair_time) {{
                line_mp[i].exchange(old_days);
                line_acn[i].exchange(0u);
                continue;
            }}
            chosen_line = i;
            claimed = true;
            break;
        }}
    }}
    
    if (claimed) {{
        line_rt[chosen_line].exchange(repair_time);
        line_last_acn[chosen_line].exchange(acn);
        line_last_day[chosen_line].exchange(current_day);
        FLAMEGPU->setVariable<unsigned int>("repair_line_id", chosen_line);
        FLAMEGPU->setVariable<unsigned int>("promoted", 1u);
        FLAMEGPU->setVariable<unsigned int>("debug_promoted", 1u);
        
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
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


# ═══════════════════════════════════════════════════════════════════════════════
# V8: P3 ПРОМОУТ через слоты RepairLine
# ═══════════════════════════════════════════════════════════════════════════════

RTC_PROMOTE_INACTIVE_V8 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_promote_inactive_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // V8: P3 inactive → operations с проверкой ремонтной линии
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int frames = {RTC_MAX_FRAMES}u;
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    
    // repair_time по типу
    unsigned int repair_time = 0u;
    if (group_by == 1u) {{
        repair_time = FLAMEGPU->environment.getProperty<unsigned int>("mi8_repair_time_const");
    }} else if (group_by == 2u) {{
        repair_time = FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    }} else {{
        return flamegpu::ALIVE;
    }}
    
    // Глобальный барьер: не раньше repair_time от day0
    if (day < repair_time) {{
        return flamegpu::ALIVE;
    }}
    
    // Планер должен досидеть repair_days до 0
    if (repair_days > 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Доступные линии ремонта: free_days >= repair_time
    auto slots_count_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, 2u>("repair_line_slots_count_mp");
    const unsigned int slots_count = (group_by == 1u) ? slots_count_mp[0] : slots_count_mp[1];
    auto slots = (group_by == 1u) ?
        FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_slots_mi8") :
        FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_slots_mi17");
    auto line_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_free_days_mp");
    
    if (slots_count == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Читаем target (PropertyArray, не MacroProperty!)
    const unsigned int safe_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
    
    unsigned int ops_curr = 0u;
    unsigned int svc_available = 0u;
    unsigned int unsvc_available = 0u;
    unsigned int inactive_available = 0u;
    unsigned int target = 0u;
    
    if (group_by == 1u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_svc_count");
        auto unsvc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_unsvc_count");
        auto inactive_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_inactive_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            ops_curr += ops_count[i];
            svc_available += svc_count[i];
            unsvc_available += unsvc_count[i];
            inactive_available += inactive_count[i];
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", safe_day);
    }} else if (group_by == 2u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_svc_count");
        auto unsvc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_unsvc_count");
        auto inactive_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_inactive_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            ops_curr += ops_count[i];
            svc_available += svc_count[i];
            unsvc_available += unsvc_count[i];
            inactive_available += inactive_count[i];
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);
    }} else {{
        return flamegpu::ALIVE;
    }}
    
    // P1+P2 учёт
    unsigned int deficit_p1 = (target > ops_curr) ? (target - ops_curr) : 0u;
    unsigned int p1_will_promote = (deficit_p1 < svc_available) ? deficit_p1 : svc_available;
    unsigned int curr_after_p1 = ops_curr + p1_will_promote;
    
    unsigned int deficit_p2 = (target > curr_after_p1) ? (target - curr_after_p1) : 0u;
    unsigned int p2_will_promote = (deficit_p2 < unsvc_available) ? deficit_p2 : unsvc_available;
    if (p2_will_promote > slots_count) {{
        p2_will_promote = slots_count;
    }}
    unsigned int curr_after_p2 = curr_after_p1 + p2_will_promote;
    
    // P3 дефицит
    if (curr_after_p2 >= target) {{
        return flamegpu::ALIVE;
    }}
    unsigned int deficit = target - curr_after_p2;
    
    // Ограничиваем количеством доступных линий (остаток после P2)
    unsigned int available_lines = (slots_count > p2_will_promote) ? (slots_count - p2_will_promote) : 0u;
    unsigned int needed = (deficit < inactive_available) ? deficit : inactive_available;
    if (available_lines < needed) {{
        needed = available_lines;
    }}
    
    if (needed == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Ранжирование по idx
    auto inactive_count = (group_by == 1u) ?
        FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_inactive_count") :
        FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_inactive_count");
    
    unsigned int rank = 0u;
    for (unsigned int i = 0u; i < idx; ++i) {{
        rank += inactive_count[i];
    }}
    
    if (rank < needed) {{
        const unsigned int slot_idx = p2_will_promote + rank;
        const unsigned int line_id = slots[slot_idx];
        if (line_id != 0xFFFFFFFFu) {{
            const unsigned int best_days = line_mp[line_id];
            if (best_days >= repair_time) {{
                FLAMEGPU->setVariable<unsigned int>("repair_candidate", 1u);
                FLAMEGPU->setVariable<unsigned int>("repair_line_id", line_id);
                FLAMEGPU->setVariable<unsigned int>("repair_line_day", best_days);
                FLAMEGPU->setVariable<unsigned int>("debug_repair_candidate", 1u);
                FLAMEGPU->setVariable<unsigned int>("debug_repair_line_id", line_id);
                FLAMEGPU->setVariable<unsigned int>("debug_repair_line_day", best_days);
            }}
        }}
    }}
    
    return flamegpu::ALIVE;
}}
"""


RTC_PROMOTE_INACTIVE_COMMIT_V8 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_promote_inactive_commit_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int candidate = FLAMEGPU->getVariable<unsigned int>("repair_candidate");
    if (candidate == 0u) return flamegpu::ALIVE;
    
    const unsigned int line_id = FLAMEGPU->getVariable<unsigned int>("repair_line_id");
    auto line_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_free_days_mp");
    auto line_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_acn_mp");
    auto line_rt = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_rt_mp");
    auto line_last_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_last_acn_mp");
    auto line_last_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_last_day_mp");
    
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int acn = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int repair_time = (group_by == 1u)
        ? FLAMEGPU->environment.getProperty<unsigned int>("mi8_repair_time_const")
        : FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    
    const unsigned int repair_quota = FLAMEGPU->environment.getProperty<unsigned int>("repair_quota");
    const unsigned int max_lines = (repair_quota < {REPAIR_LINES_MAX}u) ? repair_quota : {REPAIR_LINES_MAX}u;
    
    bool claimed = false;
    unsigned int chosen_line = 0xFFFFFFFFu;
    
    if (line_id != 0xFFFFFFFFu) {{
        const unsigned int prev_acn = line_acn[line_id].exchange(acn);
        if (prev_acn == 0u) {{
            const unsigned int old_days = line_mp[line_id].exchange(0u);
            if (old_days >= repair_time) {{
                chosen_line = line_id;
                claimed = true;
            }} else {{
                line_mp[line_id].exchange(old_days);
                line_acn[line_id].exchange(0u);
            }}
        }} else {{
            line_acn[line_id].exchange(prev_acn);
        }}
    }}
    
    if (!claimed) {{
        for (unsigned int i = 0u; i < max_lines; ++i) {{
            const unsigned int prev_acn = line_acn[i].exchange(acn);
            if (prev_acn != 0u) {{
                line_acn[i].exchange(prev_acn);
                continue;
            }}
            const unsigned int old_days = line_mp[i].exchange(0u);
            if (old_days < repair_time) {{
                line_mp[i].exchange(old_days);
                line_acn[i].exchange(0u);
                continue;
            }}
            chosen_line = i;
            claimed = true;
            break;
        }}
    }}
    
    if (claimed) {{
        line_rt[chosen_line].exchange(repair_time);
        line_last_acn[chosen_line].exchange(acn);
        line_last_day[chosen_line].exchange(current_day);
        FLAMEGPU->setVariable<unsigned int>("repair_line_id", chosen_line);
        FLAMEGPU->setVariable<unsigned int>("promoted", 1u);
        FLAMEGPU->setVariable<unsigned int>("debug_promoted", 1u);
        
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
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
# DEBUG: P2 показатели в QuotaManager (без влияния на логику)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_QUOTA_DEBUG_P2 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_quota_debug_p2_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned char group_by = FLAMEGPU->getVariable<unsigned char>("group_by");
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int frames = {RTC_MAX_FRAMES}u;
    
    unsigned int ops_curr = 0u;
    unsigned int svc_available = 0u;
    unsigned int unsvc_available = 0u;
    unsigned int target = 0u;
    
    const unsigned int safe_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
    if (group_by == 1u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_svc_count");
        auto unsvc_ready = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_unsvc_ready_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            ops_curr += ops_count[i];
            svc_available += svc_count[i];
            unsvc_available += unsvc_ready[i];
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", safe_day);
    }} else if (group_by == 2u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_svc_count");
        auto unsvc_ready = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_unsvc_ready_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            ops_curr += ops_count[i];
            svc_available += svc_count[i];
            unsvc_available += unsvc_ready[i];
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);
    }} else {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int deficit_p1 = (target > ops_curr) ? (target - ops_curr) : 0u;
    const unsigned int p1_will_promote = (deficit_p1 < svc_available) ? deficit_p1 : svc_available;
    const unsigned int curr_after_p1 = ops_curr + p1_will_promote;
    const unsigned int deficit = (target > curr_after_p1) ? (target - curr_after_p1) : 0u;
    
    auto slots_count_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, 2u>("repair_line_slots_count_mp");
    const unsigned int slots_count = (group_by == 1u) ? slots_count_mp[0] : slots_count_mp[1];
    unsigned int needed = (deficit < unsvc_available) ? deficit : unsvc_available;
    if (slots_count < needed) needed = slots_count;
    
    FLAMEGPU->setVariable<unsigned int>("debug_p2_ops", ops_curr);
    FLAMEGPU->setVariable<unsigned int>("debug_p2_target", target);
    FLAMEGPU->setVariable<unsigned int>("debug_p2_deficit", deficit);
    FLAMEGPU->setVariable<unsigned int>("debug_p2_needed", needed);
    FLAMEGPU->setVariable<unsigned int>("debug_p2_slots", slots_count);
    FLAMEGPU->setVariable<unsigned int>("debug_p2_svc", svc_available);
    FLAMEGPU->setVariable<unsigned int>("debug_p2_unsvc", unsvc_available);
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# MacroProperty для слотов RepairLine (Mi-8/Mi-17)
# ═══════════════════════════════════════════════════════════════════════════════

def setup_quota_v8_macroproperties(env):
    """Создаёт MacroProperty для остатков квот"""
    env.newMacroPropertyUInt("qm_ops_mp", 2)
    env.newMacroPropertyUInt("quota_left_mp", 2)
    print("  ✅ V8 MacroProperty: qm_ops_mp + quota_left_mp")


# ═══════════════════════════════════════════════════════════════════════════════
# Регистрация V8 квотирования
# ═══════════════════════════════════════════════════════════════════════════════

def register_quota_p2_p3_v8(model, agent):
    """
    Регистрирует V8 P2/P3 квотирование через слоты RepairLine.
    
    ВАЖНО: Вызывается ВМЕСТО V7 P2/P3 слоёв.
    
    Слои:
    - v8_promote_unsvc — P2 через слоты RepairLine
    - v8_promote_inactive — P3 через слоты RepairLine
    """
    print("  📦 V8: P2/P3 через RepairLine слоты...")
    
    # P2: unserviceable → operations (V8)
    layer_p2_decide = model.newLayer("v8_promote_unsvc_decide")
    fn = agent.newRTCFunction("rtc_promote_unsvc_v8", RTC_PROMOTE_UNSVC_V8)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")  # Переход в operations через post_quota
    layer_p2_decide.addAgentFunction(fn)
    
    layer_p2_commit = model.newLayer("v8_promote_unsvc_commit")
    fn = agent.newRTCFunction("rtc_promote_unsvc_commit_v8", RTC_PROMOTE_UNSVC_COMMIT_V8)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")
    layer_p2_commit.addAgentFunction(fn)
    
    # P3: inactive → operations (V8)
    layer_p3_decide = model.newLayer("v8_promote_inactive_decide")
    fn = agent.newRTCFunction("rtc_promote_inactive_v8", RTC_PROMOTE_INACTIVE_V8)
    fn.setInitialState("inactive")
    fn.setEndState("inactive")  # Переход в operations через post_quota
    layer_p3_decide.addAgentFunction(fn)
    
    layer_p3_commit = model.newLayer("v8_promote_inactive_commit")
    fn = agent.newRTCFunction("rtc_promote_inactive_commit_v8", RTC_PROMOTE_INACTIVE_COMMIT_V8)
    fn.setInitialState("inactive")
    fn.setEndState("inactive")
    layer_p3_commit.addAgentFunction(fn)
    
    print("  ✅ V8 P2 (unsvc) + P3 (inactive)")


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
    
    # Commit P2/P3
    layer_p2_commit = model.newLayer("v8_promote_unsvc_commit")
    fn = agent.newRTCFunction("rtc_promote_unsvc_commit_v8", RTC_PROMOTE_UNSVC_COMMIT_V8)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")
    layer_p2_commit.addAgentFunction(fn)
    
    layer_p3_commit = model.newLayer("v8_promote_inactive_commit")
    fn = agent.newRTCFunction("rtc_promote_inactive_commit_v8", RTC_PROMOTE_INACTIVE_COMMIT_V8)
    fn.setInitialState("inactive")
    fn.setEndState("inactive")
    layer_p3_commit.addAgentFunction(fn)
    
    print("  ✅ V8 MessageBucket квоты зарегистрированы")


def register_quota_v8_full(model, agent, quota_agent):
    """
    Полная регистрация V8 квотирования.
    
    Использует V7 функции для: reset, count, demote, P1
    Использует V8 функции для: P2, P3 (через слоты RepairLine)
    """
    print("\n📊 V8: Регистрация квотирования (полная версия)...")
    
    # ═══ V7: Сброс флагов ═══
    layer_reset_flags = model.newLayer("v8_reset_flags")
    for state in ["inactive", "operations", "serviceable", "repair", "reserve", "storage", "unserviceable"]:
        fn_name = f"rtc_reset_flags_v8_{state}"
        fn = agent.newRTCFunction(fn_name, RTC_RESET_FLAGS)
        fn.setInitialState(state)
        fn.setEndState(state)
        layer_reset_flags.addAgentFunction(fn)
    print("  ✅ Сброс флагов")
    
    # ═══ V7: Сброс буферов ═══
    layer_reset_buf = model.newLayer("v8_reset_buffers")
    for state in ["inactive", "operations", "serviceable", "repair", "reserve", "storage", "unserviceable"]:
        fn_name = f"rtc_reset_quota_v8_{state}"
        fn = agent.newRTCFunction(fn_name, RTC_RESET_BUFFERS)
        fn.setInitialState(state)
        fn.setEndState(state)
        layer_reset_buf.addAgentFunction(fn)
    print("  ✅ Сброс буферов")
    
    # ═══ V7: Подсчёт агентов ═══
    layer_count = model.newLayer("v8_count_agents")
    
    fn = agent.newRTCFunction("rtc_count_ops_v8", RTC_COUNT_OPS)
    fn.setInitialState("operations")
    fn.setEndState("operations")
    layer_count.addAgentFunction(fn)
    
    fn = agent.newRTCFunction("rtc_count_svc_v8", RTC_COUNT_SVC)
    fn.setInitialState("serviceable")
    fn.setEndState("serviceable")
    layer_count.addAgentFunction(fn)
    
    fn = agent.newRTCFunction("rtc_count_unsvc_v8", RTC_COUNT_UNSVC_V8)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")
    layer_count.addAgentFunction(fn)
    
    fn = agent.newRTCFunction("rtc_count_inactive_v8", RTC_COUNT_INACTIVE)
    fn.setInitialState("inactive")
    fn.setEndState("inactive")
    layer_count.addAgentFunction(fn)
    print("  ✅ Подсчёт агентов")
    
    # ═══ V8: Слоты RepairLine (QM) ═══
    layer_slots = model.newLayer("v8_repair_line_slots")
    fn = quota_agent.newRTCFunction("rtc_repair_line_slots_v8", RTC_REPAIR_LINE_SLOTS_V8)
    fn.setInitialState("default")
    fn.setEndState("default")
    fn.setMessageInput("RepairLineStatus")
    layer_slots.addAgentFunction(fn)
    print("  ✅ Слоты RepairLine (QM)")
    
    # ═══ DEBUG: P2 метрики (QM) ═══
    layer_debug_p2 = model.newLayer("v8_debug_p2")
    fn = quota_agent.newRTCFunction("rtc_quota_debug_p2_v8", RTC_QUOTA_DEBUG_P2)
    fn.setInitialState("default")
    fn.setEndState("default")
    layer_debug_p2.addAgentFunction(fn)
    print("  ✅ Debug P2 (QM)")
    
    # ═══ V7: Демоут ═══
    layer_demote = model.newLayer("v8_demote")
    fn = agent.newRTCFunction("rtc_demote_ops_v8", RTC_DEMOTE_OPS)
    fn.setInitialState("operations")
    fn.setEndState("operations")
    layer_demote.addAgentFunction(fn)
    print("  ✅ Демоут")
    
    # ═══ V7: P1 serviceable → operations ═══
    layer_p1 = model.newLayer("v8_promote_svc")
    fn = agent.newRTCFunction("rtc_promote_svc_v8", RTC_PROMOTE_SVC)
    fn.setInitialState("serviceable")
    fn.setEndState("serviceable")
    layer_p1.addAgentFunction(fn)
    print("  ✅ P1 промоут (serviceable)")
    
    # ═══ V8: P2 unserviceable → operations (через RepairLine слоты) ═══
    layer_p2_decide = model.newLayer("v8_promote_unsvc_decide")
    fn = agent.newRTCFunction("rtc_promote_unsvc_v8", RTC_PROMOTE_UNSVC_V8)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")
    layer_p2_decide.addAgentFunction(fn)
    
    layer_p2_commit = model.newLayer("v8_promote_unsvc_commit")
    fn = agent.newRTCFunction("rtc_promote_unsvc_commit_v8", RTC_PROMOTE_UNSVC_COMMIT_V8)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")
    layer_p2_commit.addAgentFunction(fn)
    print("  ✅ P2 промоут (unsvc → RepairLine)")
    
    # ═══ V8: P3 inactive → operations (через RepairLine слоты) ═══
    layer_p3_decide = model.newLayer("v8_promote_inactive_decide")
    fn = agent.newRTCFunction("rtc_promote_inactive_v8", RTC_PROMOTE_INACTIVE_V8)
    fn.setInitialState("inactive")
    fn.setEndState("inactive")
    layer_p3_decide.addAgentFunction(fn)
    
    layer_p3_commit = model.newLayer("v8_promote_inactive_commit")
    fn = agent.newRTCFunction("rtc_promote_inactive_commit_v8", RTC_PROMOTE_INACTIVE_COMMIT_V8)
    fn.setInitialState("inactive")
    fn.setEndState("inactive")
    layer_p3_commit.addAgentFunction(fn)
    print("  ✅ P3 промоут (inactive → RepairLine)")
    
    print("✅ Квотирование V8 зарегистрировано\n")


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
    
    # Доп. добор до target после пост-квотных переходов
    layer_promote_post = model.newLayer("v8_promote_inactive_post")
    fn = agent.newRTCFunction("rtc_promote_inactive_post_v8", RTC_PROMOTE_INACTIVE_POST)
    fn.setInitialState("inactive")
    fn.setEndState("inactive")
    layer_promote_post.addAgentFunction(fn)
    
    layer_to_ops_post = model.newLayer("v8_inactive_to_ops_post")
    fn = agent.newRTCFunction("rtc_inactive_to_ops_post_v8", RTC_INACTIVE_TO_OPS_POST)
    fn.setRTCFunctionCondition(COND_INACTIVE_PROMOTED_POST)
    fn.setInitialState("inactive")
    fn.setEndState("operations")
    layer_to_ops_post.addAgentFunction(fn)
    
    print("  ✅ Доп. добор из inactive (post)")

    # Обновляем буферы после post-промоутов, чтобы spawn видел актуальный ops
    layer_reset_spawn = model.newLayer("v8_reset_buffers_spawn")
    for state in ["inactive", "operations", "serviceable", "repair", "reserve", "storage", "unserviceable"]:
        fn_name = f"rtc_reset_quota_v8_spawn_{state}"
        fn = agent.newRTCFunction(fn_name, RTC_RESET_BUFFERS_SPAWN)
        fn.setInitialState(state)
        fn.setEndState(state)
        layer_reset_spawn.addAgentFunction(fn)
    print("  ✅ Сброс буферов (spawn)")
    
    layer_count_spawn = model.newLayer("v8_count_agents_spawn")
    fn = agent.newRTCFunction("rtc_count_ops_v8_spawn", RTC_COUNT_OPS)
    fn.setInitialState("operations")
    fn.setEndState("operations")
    layer_count_spawn.addAgentFunction(fn)
    
    fn = agent.newRTCFunction("rtc_count_svc_v8_spawn", RTC_COUNT_SVC)
    fn.setInitialState("serviceable")
    fn.setEndState("serviceable")
    layer_count_spawn.addAgentFunction(fn)
    
    fn = agent.newRTCFunction("rtc_count_unsvc_v8_spawn", RTC_COUNT_UNSVC_V8)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")
    layer_count_spawn.addAgentFunction(fn)
    
    fn = agent.newRTCFunction("rtc_count_inactive_v8_spawn", RTC_COUNT_INACTIVE)
    fn.setInitialState("inactive")
    fn.setEndState("inactive")
    layer_count_spawn.addAgentFunction(fn)
    print("  ✅ Подсчёт агентов (spawn)")
    print("✅ Post-quota пересчёт зарегистрирован\n")

