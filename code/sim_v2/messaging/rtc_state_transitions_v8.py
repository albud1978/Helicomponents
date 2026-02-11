#!/usr/bin/env python3
"""
RTC модуль: State Transitions V8 — Next-day dt проверка

АРХИТЕКТУРА V8 (отличия от V7):
1. Проверка ресурса на СЛЕДУЮЩИЙ день (SNE + dt_next >= LL)
2. limiter=0 → обязательный выход (EXCEPTION если нет перехода)
3. ops→unsvc НЕ устанавливает exit_date (квотирование через RepairLine)

Порядок проверок (приоритет):
1. SNE + dt_next >= LL → storage (назначенный ресурс)
2. PPR + dt_next >= OH AND SNE + dt_next >= BR → storage (нерентабельный ремонт)
3. PPR + dt_next >= OH AND SNE + dt_next < BR → unserviceable (ремонт нужен)

dt_next = налёт на день current_day + 1 (из mp5_cumsum)

См. docs/adaptive_steps_logic.md для полной архитектуры.

Дата: 16.01.2026
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from model_build import RTC_MAX_FRAMES, MAX_DAYS

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")

CUMSUM_SIZE = RTC_MAX_FRAMES * (MAX_DAYS + 1)


# ═══════════════════════════════════════════════════════════════════════════════
# V8: Operations инкремент (как V7, но сохраняем dt_next для проверки)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_OPS_INCREMENT_V8 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_ops_increment_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // V8: Инкремент SNE/PPR + сохранение dt_next для проверки ресурсов
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int prev_day = FLAMEGPU->environment.getProperty<unsigned int>("prev_day");
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    auto mp5_cumsum = FLAMEGPU->environment.getMacroProperty<unsigned int, {CUMSUM_SIZE}u>("mp5_cumsum");
    
    // DISABLED (state5-unused): // Если агент только что вошёл в operations (deterministic spawn 5→2) — пропускаем инкремент
    // DISABLED (state5-unused): const unsigned int t5 = FLAMEGPU->getVariable<unsigned int>("transition_5_to_2");
    // DISABLED (state5-unused): if (t5 == 1u) {{
    // DISABLED (state5-unused):     FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    // DISABLED (state5-unused):     FLAMEGPU->setVariable<unsigned int>("daily_next_u32", 0u);
    // DISABLED (state5-unused):     return flamegpu::ALIVE;
    // DISABLED (state5-unused): }}
    
    // dt = cumsum[current_day] - cumsum[prev_day] (налёт за текущий шаг)
    const unsigned int base_curr = current_day * frames + idx;
    const unsigned int base_prev = prev_day * frames + idx;
    const unsigned int cumsum_curr = mp5_cumsum[base_curr];
    const unsigned int cumsum_prev = (prev_day > 0u) ? mp5_cumsum[base_prev] : 0u;
    const unsigned int dt = (cumsum_curr >= cumsum_prev) ? (cumsum_curr - cumsum_prev) : 0u;
    
    // === 1. Инкременты SNE/PPR ===
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    
    const unsigned int sne_new = sne + dt;
    const unsigned int ppr_new = ppr + dt;
    
    FLAMEGPU->setVariable<unsigned int>("sne", sne_new);
    FLAMEGPU->setVariable<unsigned int>("ppr", ppr_new);
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    
    // === 2. V8: Сохраняем dt_next (налёт СЛЕДУЮЩЕГО дня) ===
    // dt_next = cumsum[current_day + 1] - cumsum[current_day]
    unsigned int dt_next = 0u;
    if (current_day < end_day) {{
        const unsigned int next_day = current_day + 1u;
        const unsigned int base_next = next_day * frames + idx;
        const unsigned int cumsum_next = (next_day <= end_day) ? mp5_cumsum[base_next] : cumsum_curr;
        dt_next = (cumsum_next >= cumsum_curr) ? (cumsum_next - cumsum_curr) : 0u;
    }}
    FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dt_next);
    
    // === 3. Декремент limiter ===
    const unsigned int adaptive_days = (current_day > prev_day) ? (current_day - prev_day) : 0u;
    
    unsigned short limiter = FLAMEGPU->getVariable<unsigned short>("limiter");
    if (limiter > 0u) {{
        if (limiter <= (unsigned short)adaptive_days) {{
            limiter = 0u;  // Достигли лимита
        }} else {{
            limiter -= (unsigned short)adaptive_days;
        }}
        FLAMEGPU->setVariable<unsigned short>("limiter", limiter);
    }}
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# V8: Декремент repair_days для unserviceable
# ═══════════════════════════════════════════════════════════════════════════════

RTC_UNSVC_DECREMENT_V8 = """
FLAMEGPU_AGENT_FUNCTION(rtc_unsvc_decrement_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int prev_day = FLAMEGPU->environment.getProperty<unsigned int>("prev_day");
    const unsigned int adaptive_days = (current_day > prev_day) ? (current_day - prev_day) : 0u;
    if (adaptive_days == 0u) return flamegpu::ALIVE;
    
    unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    if (repair_days > 0u) {
        if (repair_days <= adaptive_days) {
            repair_days = 0u;
        } else {
            repair_days -= adaptive_days;
        }
        FLAMEGPU->setVariable<unsigned int>("repair_days", repair_days);
    }
    return flamegpu::ALIVE;
}
"""

# inactive: repair_days не декрементируется (всегда 0)

# ═══════════════════════════════════════════════════════════════════════════════
# V8: Условия переходов с next-day dt проверкой
# ═══════════════════════════════════════════════════════════════════════════════

# Условие V8: SNE + dt_next >= LL ИЛИ (PPR + dt_next >= OH AND SNE + dt_next >= BR)
COND_OPS_TO_STORAGE_V8 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_ops_to_storage_v8) {
    // Early-out: агенты с ненулевым лимитером не выходят из ops
    const unsigned short lim = FLAMEGPU->getVariable<unsigned short>("limiter");
    if (lim > 0u) return false;
    // V8: Проверка на СЛЕДУЮЩИЙ день (предотвращение переналёта)
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int dt_next = FLAMEGPU->getVariable<unsigned int>("daily_next_u32");
    
    const unsigned int sne_next = sne + dt_next;
    
    // 1. LL — безусловно (назначенный ресурс будет исчерпан)
    if (sne_next >= ll) return true;
    
    // 2. PPR + dt_next >= OH и SNE + dt_next >= BR → storage (нерентабельный ремонт)
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    
    const unsigned int ppr_next = ppr + dt_next;
    
    if (ppr_next >= oh && sne_next >= br) return true;
    
    return false;
}
"""

# Условие V8: PPR + dt_next >= OH AND SNE + dt_next < BR (переход в unserviceable)
COND_OPS_TO_UNSVC_V8 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_ops_to_unsvc_v8) {
    // Early-out: агенты с ненулевым лимитером не выходят из ops
    const unsigned short lim = FLAMEGPU->getVariable<unsigned short>("limiter");
    if (lim > 0u) return false;
    // V8: Проверка на СЛЕДУЮЩИЙ день (OH и BR)
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    const unsigned int dt_next = FLAMEGPU->getVariable<unsigned int>("daily_next_u32");
    
    const unsigned int sne_next = sne + dt_next;
    const unsigned int ppr_next = ppr + dt_next;
    
    return (ppr_next >= oh && sne_next < br);
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# V8: Функции переходов
# ═══════════════════════════════════════════════════════════════════════════════

# ops → storage (2→6)
RTC_OPS_TO_STORAGE_V8 = """
FLAMEGPU_AGENT_FUNCTION(rtc_ops_to_storage_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V8: Переход в storage, limiter=0
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_6", 1u);
    FLAMEGPU->setVariable<unsigned short>("limiter", 0u);
    FLAMEGPU->setVariable<unsigned int>("status_change_day", current_day);
    return flamegpu::ALIVE;
}
"""

# ops → unserviceable (2→7)
# V8: Переход в unserviceable без exit_date (квотирование через RepairLine)
RTC_OPS_TO_UNSVC_V8 = """
FLAMEGPU_AGENT_FUNCTION(rtc_ops_to_unsvc_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V8: Переход в unserviceable
    // exit_date НЕ используется в V8 (квотирование через RepairLine)
    
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_7", 1u);
    FLAMEGPU->setVariable<unsigned int>("repair_line_id", 0xFFFFFFFFu);
    FLAMEGPU->setVariable<unsigned int>("repair_days", FLAMEGPU->getVariable<unsigned int>("repair_time"));
    FLAMEGPU->setVariable<unsigned int>("status_change_day", current_day);
    
    return flamegpu::ALIVE;
}
"""




# ═══════════════════════════════════════════════════════════════════════════════
# Регистрация V8 переходов
# ═══════════════════════════════════════════════════════════════════════════════

def register_ops_transitions_v8(model, agent):
    """
    Регистрирует V8 переходы operations с next-day dt проверкой.
    
    Слои:
    1. v8_ops_increment — инкремент SNE/PPR + сохранение dt_next
    2. v8_unsvc_decrement — декремент repair_days в unserviceable
    3. v8_ops_to_storage — переход 2→6
    4. v8_ops_to_unsvc — переход 2→7
    """
    print("\n📦 V8: Регистрация operations переходов (next-day dt)...")
    
    # 1. Инкремент (с dt_next)
    layer_incr = model.newLayer("v8_ops_increment")
    fn = agent.newRTCFunction("rtc_ops_increment_v8", RTC_OPS_INCREMENT_V8)
    fn.setInitialState("operations")
    fn.setEndState("operations")
    layer_incr.addAgentFunction(fn)
    
    # 2. unserviceable: decrement repair_days
    layer_unsvc = model.newLayer("v8_unsvc_decrement")
    fn = agent.newRTCFunction("rtc_unsvc_decrement_v8", RTC_UNSVC_DECREMENT_V8)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")
    layer_unsvc.addAgentFunction(fn)
    
    # 3. ops → storage (приоритет 1)
    layer_storage = model.newLayer("v8_ops_to_storage")
    fn = agent.newRTCFunction("rtc_ops_to_storage_v8", RTC_OPS_TO_STORAGE_V8)
    fn.setRTCFunctionCondition(COND_OPS_TO_STORAGE_V8)
    fn.setInitialState("operations")
    fn.setEndState("storage")
    layer_storage.addAgentFunction(fn)
    
    # 4. ops → unserviceable (приоритет 2)
    layer_unsvc = model.newLayer("v8_ops_to_unsvc")
    fn = agent.newRTCFunction("rtc_ops_to_unsvc_v8", RTC_OPS_TO_UNSVC_V8)
    fn.setRTCFunctionCondition(COND_OPS_TO_UNSVC_V8)
    fn.setInitialState("operations")
    fn.setEndState("unserviceable")
    layer_unsvc.addAgentFunction(fn)
    
    print("  ✅ V8 operations переходы: increment + storage + unsvc")

