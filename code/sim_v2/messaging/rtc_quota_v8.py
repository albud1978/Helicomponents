#!/usr/bin/env python3
"""
RTC модуль V8: Квотирование через RepairAgent

АРХИТЕКТУРА V8 (отличия от V7):
1. P2/P3 проверяют RepairAgent.capacity перед промоутом
2. Условия:
   - current_day >= repair_time (прошло достаточно времени)
   - capacity >= repair_time (есть ремонтная мощность)
3. Записывают to_deduct в MacroProperty для RepairAgent

Порядок:
1. Сброс буферов (MacroProperty) — V7
2. Подсчёт агентов по состояниям — V7
3. Демоут (если избыток) — V7
4. P1 промоут: serviceable → operations — V7
5. P2 промоут: unserviceable → operations — V8!
6. P3 промоут: inactive → operations — V8!
7. Запись to_deduct для RepairAgent

См. docs/adaptive_steps_logic.md для полной архитектуры.

Дата: 16.01.2026
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from model_build import RTC_MAX_FRAMES, MAX_DAYS

import pyflamegpu as fg

# Импорт RTC строк из V7 (reset, count, demote, P1)
from rtc_quota_v7 import (
    RTC_RESET_FLAGS,
    RTC_RESET_BUFFERS,
    RTC_COUNT_OPS,
    RTC_COUNT_SVC,
    RTC_COUNT_UNSVC,
    RTC_COUNT_INACTIVE,
    RTC_DEMOTE_OPS,
    RTC_PROMOTE_SVC,  # P1 остаётся V7
)


# ═══════════════════════════════════════════════════════════════════════════════
# V8: P2 ПРОМОУТ с проверкой RepairAgent.capacity
# ═══════════════════════════════════════════════════════════════════════════════

RTC_PROMOTE_UNSVC_V8 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_promote_unsvc_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // V8: P2 unserviceable → operations (с проверкой RepairAgent.capacity)
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int repair_time = FLAMEGPU->environment.getProperty<unsigned int>("repair_time_const");
    
    // V8: Условие 1 — прошло достаточно времени от начала симуляции
    if (day < repair_time) {{
        return flamegpu::ALIVE;  // Рано для P2
    }}
    
    // V8: Читаем capacity от RepairAgent
    auto mp_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("repair_slots_mp");
    const unsigned int available_slots = mp_slots[0];
    
    // V8: Условие 2 — есть ремонтная мощность
    if (available_slots == 0u) {{
        return flamegpu::ALIVE;  // Нет мощности для P2
    }}
    
    // Читаем target из mp4
    auto mp4_mi8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_DAYS + 1}u>("mp4_ops_counter_mi8");
    auto mp4_mi17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_DAYS + 1}u>("mp4_ops_counter_mi17");
    const unsigned int safe_day = ((day + 1u) < days_total ? (day + 1u) : (days_total > 0u ? days_total - 1u : 0u));
    
    unsigned int ops_curr = 0u;
    unsigned int svc_available = 0u;
    unsigned int unsvc_available = 0u;
    unsigned int target = 0u;
    
    // Подсчёт по типу
    if (group_by == 1u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_svc_count");
        auto unsvc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_unsvc_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            ops_curr += ops_count[i];
            svc_available += svc_count[i];
            unsvc_available += unsvc_count[i];
        }}
        target = mp4_mi8[safe_day];
    }} else if (group_by == 2u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_svc_count");
        auto unsvc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_unsvc_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            ops_curr += ops_count[i];
            svc_available += svc_count[i];
            unsvc_available += unsvc_count[i];
        }}
        target = mp4_mi17[safe_day];
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
    
    // V8: Ограничение по slots
    unsigned int approved = (deficit < available_slots) ? deficit : available_slots;
    unsigned int needed = (approved < unsvc_available) ? approved : unsvc_available;
    
    if (needed == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Ранжирование по idx
    auto unsvc_count = (group_by == 1u) ? 
        FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi8_unsvc_count") :
        FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("mi17_unsvc_count");
    
    unsigned int rank = 0u;
    for (unsigned int i = 0u; i < idx; ++i) {{
        rank += unsvc_count[i];
    }}
    
    if (rank < needed) {{
        FLAMEGPU->setVariable<unsigned int>("promoted", 1u);
        
        // V8: Записываем approved в буфер (будет подсчитано в RepairAgent)
        // Используем atomicMin для безопасной записи 1 (агент одобрен)
        auto mp_p2_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("repair_p2_approved");
        mp_p2_count[idx].exchange(1u);
    }}
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# V8: P3 ПРОМОУТ с проверкой RepairAgent.capacity
# ═══════════════════════════════════════════════════════════════════════════════

RTC_PROMOTE_INACTIVE_V8 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_promote_inactive_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // V8: P3 inactive → operations (с проверкой RepairAgent.capacity)
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int repair_time = FLAMEGPU->environment.getProperty<unsigned int>("repair_time_const");
    
    // V8: Условие 1 — прошло достаточно времени
    if (day < repair_time) {{
        return flamegpu::ALIVE;
    }}
    
    // V8: Читаем ОСТАВШИЕСЯ slots (после P2)
    auto mp_cap = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("repair_capacity_mp");
    auto mp_deduct = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("repair_to_deduct_mp");
    const unsigned int capacity = mp_cap[0];
    const unsigned int already_deducted = mp_deduct[0];  // Уже зарезервировано P2
    
    const unsigned int remaining_capacity = (capacity > already_deducted) ? (capacity - already_deducted) : 0u;
    const unsigned int available_slots = (repair_time > 0u) ? (remaining_capacity / repair_time) : 0u;
    
    if (available_slots == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Читаем target
    auto mp4_mi8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_DAYS + 1}u>("mp4_ops_counter_mi8");
    auto mp4_mi17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_DAYS + 1}u>("mp4_ops_counter_mi17");
    const unsigned int safe_day = ((day + 1u) < days_total ? (day + 1u) : (days_total > 0u ? days_total - 1u : 0u));
    
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
        target = mp4_mi8[safe_day];
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
        target = mp4_mi17[safe_day];
    }} else {{
        return flamegpu::ALIVE;
    }}
    
    // P1+P2 учёт
    unsigned int deficit_p1 = (target > ops_curr) ? (target - ops_curr) : 0u;
    unsigned int p1_will_promote = (deficit_p1 < svc_available) ? deficit_p1 : svc_available;
    unsigned int curr_after_p1 = ops_curr + p1_will_promote;
    
    unsigned int deficit_p2 = (target > curr_after_p1) ? (target - curr_after_p1) : 0u;
    unsigned int p2_will_promote = (deficit_p2 < unsvc_available) ? deficit_p2 : unsvc_available;
    unsigned int curr_after_p2 = curr_after_p1 + p2_will_promote;
    
    // P3 дефицит
    if (curr_after_p2 >= target) {{
        return flamegpu::ALIVE;
    }}
    unsigned int deficit = target - curr_after_p2;
    
    // V8: Ограничение по оставшимся slots
    unsigned int approved = (deficit < available_slots) ? deficit : available_slots;
    unsigned int needed = (approved < inactive_available) ? approved : inactive_available;
    
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
        FLAMEGPU->setVariable<unsigned int>("promoted", 1u);
        
        // V8: Записываем approved в буфер (будет подсчитано в RepairAgent)
        auto mp_p3_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("repair_p3_approved");
        mp_p3_count[idx].exchange(1u);
    }}
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# MacroProperty для подсчёта одобренных P2/P3
# ═══════════════════════════════════════════════════════════════════════════════

def setup_quota_v8_macroproperties(env):
    """Создаёт MacroProperty для подсчёта одобренных P2/P3"""
    env.newMacroPropertyUInt("repair_p2_approved", RTC_MAX_FRAMES)  # P2: unsvc
    env.newMacroPropertyUInt("repair_p3_approved", RTC_MAX_FRAMES)  # P3: inactive
    print("  ✅ V8 MacroProperty: repair_p2_approved, repair_p3_approved")


# ═══════════════════════════════════════════════════════════════════════════════
# Регистрация V8 квотирования
# ═══════════════════════════════════════════════════════════════════════════════

def register_quota_p2_p3_v8(model, agent):
    """
    Регистрирует V8 P2/P3 квотирование с проверкой RepairAgent.capacity.
    
    ВАЖНО: Вызывается ВМЕСТО V7 P2/P3 слоёв.
    
    Слои:
    - v8_promote_unsvc — P2 с проверкой capacity
    - v8_promote_inactive — P3 с проверкой capacity
    """
    print("  📦 V8: P2/P3 через RepairAgent...")
    
    # P2: unserviceable → operations (V8)
    layer_p2 = model.newLayer("v8_promote_unsvc")
    fn = agent.newRTCFunction("rtc_promote_unsvc_v8", RTC_PROMOTE_UNSVC_V8)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")  # Переход в operations через post_quota
    layer_p2.addAgentFunction(fn)
    
    # P3: inactive → operations (V8)
    layer_p3 = model.newLayer("v8_promote_inactive")
    fn = agent.newRTCFunction("rtc_promote_inactive_v8", RTC_PROMOTE_INACTIVE_V8)
    fn.setInitialState("inactive")
    fn.setEndState("inactive")  # Переход в operations через post_quota
    layer_p3.addAgentFunction(fn)
    
    print("  ✅ V8 P2 (unsvc) + P3 (inactive)")


def register_quota_v8_full(model, agent):
    """
    Полная регистрация V8 квотирования.
    
    Использует V7 функции для: reset, count, demote, P1
    Использует V8 функции для: P2, P3 (через RepairAgent.capacity)
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
    
    fn = agent.newRTCFunction("rtc_count_unsvc_v8", RTC_COUNT_UNSVC)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")
    layer_count.addAgentFunction(fn)
    
    fn = agent.newRTCFunction("rtc_count_inactive_v8", RTC_COUNT_INACTIVE)
    fn.setInitialState("inactive")
    fn.setEndState("inactive")
    layer_count.addAgentFunction(fn)
    print("  ✅ Подсчёт агентов")
    
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
    
    # ═══ V8: P2 unserviceable → operations (через RepairAgent!) ═══
    layer_p2 = model.newLayer("v8_promote_unsvc")
    fn = agent.newRTCFunction("rtc_promote_unsvc_v8", RTC_PROMOTE_UNSVC_V8)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")
    layer_p2.addAgentFunction(fn)
    print("  ✅ P2 промоут (unsvc → RepairAgent)")
    
    # ═══ V8: P3 inactive → operations (через RepairAgent!) ═══
    layer_p3 = model.newLayer("v8_promote_inactive")
    fn = agent.newRTCFunction("rtc_promote_inactive_v8", RTC_PROMOTE_INACTIVE_V8)
    fn.setInitialState("inactive")
    fn.setEndState("inactive")
    layer_p3.addAgentFunction(fn)
    print("  ✅ P3 промоут (inactive → RepairAgent)")
    
    print("✅ Квотирование V8 зарегистрировано\n")

