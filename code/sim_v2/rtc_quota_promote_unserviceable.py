"""
RTC модуль V6: Промоут unserviceable → operations (приоритет 2)

Логика квотирования:
- P1: serviceable → operations (исправные, PPR сохраняется)
- P2: unserviceable → operations (после OH, PPR=0 при переходе) ← ЭТОТ МОДУЛЬ
- P3: inactive → operations (давно запаркованные, PPR по group_by)

Агенты в state 7 (unserviceable):
- Попали сюда из operations при PPR >= OH
- Ждут промоут для возврата в эксплуатацию
- При переходе 7→2: PPR сбрасывается в 0

Дата: 12.01.2026
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import model_build

import pyflamegpu as fg


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC для промоута unserviceable → operations (P2)"""
    print("  Регистрация модуля квотирования: промоут unserviceable (приоритет 2)")
    
    max_frames = model_build.RTC_MAX_FRAMES
    
    # ═══════════════════════════════════════════════════════════════════
    # ПРОМОУТ ПРИОРИТЕТ 2: unserviceable (state 7) → operations
    # 
    # Логика:
    # 1. Фильтр: только агенты с intent=7 (ждут в unserviceable)
    # 2. Считаем curr (текущие в operations)
    # 3. Считаем target (из mp4_ops_counter)
    # 4. deficit = target - curr
    # 5. Если deficit > 0: промоутим по принципу "youngest first"
    # ═══════════════════════════════════════════════════════════════════
    
    RTC_QUOTA_PROMOTE_UNSERVICEABLE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_quota_promote_unserviceable, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Фильтр: только агенты с intent=7 (в unserviceable, ждут решения)
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    if (intent != 7u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = ((day + 1u) < days_total ? (day + 1u) : (days_total > 0u ? days_total - 1u : 0u));
    
    // ═══════════════════════════════════════════════════════════
    // ШАГ 1: Подсчёт curr (текущие в operations + serviceable)
    // P2 выполняется ПОСЛЕ P1, поэтому учитываем и serviceable
    // ═══════════════════════════════════════════════════════════
    unsigned int curr = 0u;
    unsigned int target = 0u;
    
    if (group_by == 1u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_svc_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            // Учитываем и operations, и serviceable с approve
            if (ops_count[i] == 1u) ++curr;
        }}
        // Добавляем P1 (serviceable с intent=2 после P1)
        auto approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (svc_count[i] == 1u && approve[i] == 1u) ++curr;
        }}
        
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", safe_day);
    }} else if (group_by == 2u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_svc_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (ops_count[i] == 1u) ++curr;
        }}
        auto approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (svc_count[i] == 1u && approve[i] == 1u) ++curr;
        }}
        
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);
    }} else {{
        return flamegpu::ALIVE;
    }}
    
    // ═══════════════════════════════════════════════════════════
    // ШАГ 2: Расчёт дефицита
    // ═══════════════════════════════════════════════════════════
    const int deficit = (int)target - (int)curr;
    if (deficit <= 0) {{
        // Достаточно агентов, P2 не нужен
        return flamegpu::ALIVE;
    }}
    
    // ═══════════════════════════════════════════════════════════
    // ШАГ 3: Промоут (youngest first)
    // ═══════════════════════════════════════════════════════════
    const unsigned int K = (unsigned int)deficit;
    
    // Ранжирование среди агентов в unserviceable
    unsigned int rank = 0u;
    
    if (group_by == 1u) {{
        auto unsvc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_unsvc_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (i == idx) continue;
            if (unsvc_count[i] != 1u) continue;
            // Youngest first: больший idx = моложе
            if (i > idx) ++rank;
        }}
    }} else {{
        auto unsvc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_unsvc_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (i == idx) continue;
            if (unsvc_count[i] != 1u) continue;
            if (i > idx) ++rank;
        }}
    }}
    
    // Промоут если rank < K
    if (rank < K) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
        
        // Записываем approve
        if (group_by == 1u) {{
            auto approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve_s7");
            approve[idx].exchange(1u);
        }} else {{
            auto approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve_s7");
            approve[idx].exchange(1u);
        }}
        
        // DEBUG: const unsigned int acn = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        // printf("[P2 PROMOTE Day %u] AC %u: unserviceable -> ops (rank %u < K %u)\\n", 
        //        day, acn, rank, K);
    }}
    
    return flamegpu::ALIVE;
}}
"""
    
    # Регистрация функции
    rtc_func = agent.newRTCFunction("rtc_quota_promote_unserviceable", RTC_QUOTA_PROMOTE_UNSERVICEABLE)
    rtc_func.setInitialState("unserviceable")
    rtc_func.setEndState("unserviceable")  # State manager делает переход
    
    layer = model.newLayer("quota_promote_unserviceable_p2")
    layer.addAgentFunction(rtc_func)
    
    print("  ✅ Модуль quota_promote_unserviceable (P2) зарегистрирован")

