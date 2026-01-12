#!/usr/bin/env python3
"""
RTC модуль V7: Однофазные переходы состояний (без intent)

Архитектура:
- Переходы состояний выполняются НАПРЯМУЮ через setInitialState/setEndState
- НЕТ промежуточной переменной intent_state
- Условия переходов проверяются через FunctionCondition

Переходы:
- 2→2: operations stay (PPR < OH && SNE < BR/LL)
- 2→7: operations → unserviceable (PPR >= OH)
- 2→6: operations → storage (SNE >= BR или SNE >= LL)
- 7→7: unserviceable stay (не получил промоут)
- 7→2: unserviceable → operations (P2 промоут, PPR=0)
- 3→3: serviceable stay (не получил промоут)
- 3→2: serviceable → operations (P1 промоут)
- 1→1: inactive stay
- 1→2: inactive → operations (P3 промоут)
- 4→4: repair stay (exit_date не достигнут)
- 4→3: repair → serviceable (exit_date достигнут)
- 5→5: reserve stay (spawn exit_date не достигнут)
- 5→2: reserve → operations (spawn exit_date достигнут)
- 6→6: storage stay (неизменяемый)

Дата: 12.01.2026
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from model_build import RTC_MAX_FRAMES, MAX_DAYS

import pyflamegpu as fg

CUMSUM_SIZE = RTC_MAX_FRAMES * (MAX_DAYS + 1)


# ═══════════════════════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ: Флаги переходов для квотирования
# ═══════════════════════════════════════════════════════════════════════════════
# Вместо intent используем флаги:
# - needs_demote: агент должен выйти из operations (избыток)
# - promoted: агент получил промоут в этом шаге


# ═══════════════════════════════════════════════════════════════════════════════
# ФАЗА -1: Копирование exit_date в MacroProperty для расчёта adaptive_days
# ═══════════════════════════════════════════════════════════════════════════════

# Сброс min_exit_date_mp перед копированием
RTC_RESET_EXIT_DATE = """
FLAMEGPU_AGENT_FUNCTION(rtc_reset_exit_date_v7, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Сброс min_exit_date_mp в начале шага (ТОЛЬКО QuotaManager group_by=1)
    const unsigned char group_by = FLAMEGPU->getVariable<unsigned char>("group_by");
    if (group_by != 1u) return flamegpu::ALIVE;  // Только один агент
    
    auto mp_exit = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("min_exit_date_mp");
    mp_exit[0].exchange(0xFFFFFFFFu);  // MAX = нет exit_date
    
    return flamegpu::ALIVE;
}
"""

# Копирование exit_date из repair агентов
RTC_COPY_EXIT_DATE_REPAIR = """
FLAMEGPU_AGENT_FUNCTION(rtc_copy_exit_date_repair_v7, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V7: Копируем exit_date в min_exit_date_mp (atomicMin, WRITE ONLY)
    const unsigned int exit_date = FLAMEGPU->getVariable<unsigned int>("exit_date");
    
    // Если exit_date не установлен — пропускаем
    if (exit_date == 0u || exit_date == 0xFFFFFFFFu) return flamegpu::ALIVE;
    
    // atomicMin — ТОЛЬКО WRITE, без READ
    auto mp_exit = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("min_exit_date_mp");
    mp_exit[0].min(exit_date);  // atomicMin
    
    return flamegpu::ALIVE;
}
"""

# Копирование exit_date из reserve агентов (spawn)
RTC_COPY_EXIT_DATE_SPAWN = """
FLAMEGPU_AGENT_FUNCTION(rtc_copy_exit_date_spawn_v7, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V7: Копируем exit_date в min_exit_date_mp (atomicMin, WRITE ONLY)
    const unsigned int exit_date = FLAMEGPU->getVariable<unsigned int>("exit_date");
    
    // Если exit_date не установлен — пропускаем
    if (exit_date == 0u || exit_date == 0xFFFFFFFFu) return flamegpu::ALIVE;
    
    // atomicMin — ТОЛЬКО WRITE, без READ
    auto mp_exit = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("min_exit_date_mp");
    mp_exit[0].min(exit_date);  // atomicMin
    
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# ФАЗА 0: Детерминированные переходы (repair, spawn)
# ═══════════════════════════════════════════════════════════════════════════════

# Условие: exit_date достигнут (для repair)
COND_REPAIR_EXIT = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_repair_exit) {
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int exit_date = FLAMEGPU->getVariable<unsigned int>("exit_date");
    return (exit_date > 0u && current_day >= exit_date);
}
"""

# УДАЛЕНО: cond_repair_stay — агенты автоматически остаются если condition=false

# Функция: repair → serviceable (4→3)
RTC_REPAIR_TO_SVC = """
FLAMEGPU_AGENT_FUNCTION(rtc_repair_to_svc_v7, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Переход из ремонта: PPR = 0, limiter сбрасывается
    FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
    FLAMEGPU->setVariable<unsigned short>("limiter", 0u);
    FLAMEGPU->setVariable<unsigned int>("exit_date", 0u);
    FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_4_to_3", 1u);
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    return flamegpu::ALIVE;
}
"""

# УДАЛЕНО: RTC_REPAIR_STAY — избыточно, агенты остаются автоматически

# Условие: spawn exit_date достигнут
COND_SPAWN_EXIT = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_spawn_exit) {
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int exit_date = FLAMEGPU->getVariable<unsigned int>("exit_date");
    return (exit_date > 0u && current_day >= exit_date);
}
"""

# УДАЛЕНО: cond_spawn_stay — агенты автоматически остаются если condition=false

# Функция: reserve → operations (spawn, 5→2)
RTC_SPAWN_TO_OPS = """
FLAMEGPU_AGENT_FUNCTION(rtc_spawn_to_ops_v7, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Spawn: новый агент в operations
    FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
    FLAMEGPU->setVariable<unsigned int>("sne", 0u);
    FLAMEGPU->setVariable<unsigned short>("limiter", 0u);  // Будет вычислен
    FLAMEGPU->setVariable<unsigned int>("exit_date", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_5_to_2", 1u);
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    return flamegpu::ALIVE;
}
"""

# УДАЛЕНО: RTC_SPAWN_STAY — избыточно, агенты остаются автоматически


# ═══════════════════════════════════════════════════════════════════════════════
# ФАЗА 1: Operations — инкременты и определение перехода
# 
# Логика:
# 1. Читаем SNE, PPR, dt из mp5_cumsum
# 2. Инкрементируем SNE, PPR
# 3. Проверяем условия перехода (OH, BR, LL)
# 4. Функции с разными endState выполняют переход
#
# ВАЖНО: Разводим READ и WRITE через FunctionCondition
# ═══════════════════════════════════════════════════════════════════════════════

# Слой 1.1: READ — вычисляем dt, новые SNE/PPR, декрементируем limiter
# Все агенты в operations выполняют эту функцию
# ОПТИМИЗАЦИЯ: три счётчика в одном проходе (sne++, ppr++, limiter--)
RTC_OPS_INCREMENT = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_ops_increment_v7, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Читаем параметры
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int prev_day = FLAMEGPU->environment.getProperty<unsigned int>("prev_day");
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    // mp5_cumsum для налёта
    auto mp5_cumsum = FLAMEGPU->environment.getMacroProperty<unsigned int, {CUMSUM_SIZE}u>("mp5_cumsum");
    
    // dt = cumsum[current_day] - cumsum[prev_day]
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
    
    // === 2. Декремент limiter (горизонт до ресурсного лимита) ===
    // adaptive_days = current_day - prev_day (длина текущего шага)
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

# Слой 1.2: Условия переходов (FunctionCondition)
# Эти условия читают ОБНОВЛЁННЫЕ значения SNE/PPR

# Условие: PPR >= OH (переход в unserviceable)
COND_OPS_TO_UNSVC = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_ops_to_unsvc) {
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    
    // Приоритет: storage (BR/LL) важнее unserviceable (OH)
    // Если SNE >= BR или SNE >= LL — это storage, не unserviceable
    if (sne >= ll) return false;
    if (br > 0u && sne >= br) return false;
    
    // OH проверка
    return (ppr >= oh);
}
"""

# Условие: SNE >= BR или SNE >= LL (переход в storage)
COND_OPS_TO_STORAGE = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_ops_to_storage) {
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    
    // LL приоритетнее BR
    if (sne >= ll) return true;
    if (br > 0u && sne >= br) return true;
    return false;
}
"""

# Условие: демоут (needs_demote == 1)
COND_OPS_DEMOTE = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_ops_demote) {
    return FLAMEGPU->getVariable<unsigned int>("needs_demote") == 1u;
}
"""

# Функция: operations → unserviceable (2→7)
RTC_OPS_TO_UNSVC = """
FLAMEGPU_AGENT_FUNCTION(rtc_ops_to_unsvc_v7, flamegpu::MessageNone, flamegpu::MessageNone) {
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_7", 1u);
    FLAMEGPU->setVariable<unsigned short>("limiter", 0u);
    return flamegpu::ALIVE;
}
"""

# Функция: operations → storage (2→6)
RTC_OPS_TO_STORAGE = """
FLAMEGPU_AGENT_FUNCTION(rtc_ops_to_storage_v7, flamegpu::MessageNone, flamegpu::MessageNone) {
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_6", 1u);
    FLAMEGPU->setVariable<unsigned short>("limiter", 0u);
    return flamegpu::ALIVE;
}
"""

# Функция: operations → serviceable (демоут, 2→3)
RTC_OPS_DEMOTE = """
FLAMEGPU_AGENT_FUNCTION(rtc_ops_demote_v7, flamegpu::MessageNone, flamegpu::MessageNone) {
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_3", 1u);
    FLAMEGPU->setVariable<unsigned short>("limiter", 0u);
    FLAMEGPU->setVariable<unsigned int>("needs_demote", 0u);  // Сброс флага
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# ФАЗА 3: Квотирование — промоуты (P1, P2, P3)
# 
# Используем флаг `promoted` вместо intent:
# - promoted = 0: не промоутен (stay)
# - promoted = 1: промоутен (переход в operations)
# ═══════════════════════════════════════════════════════════════════════════════

# Условие: serviceable промоутен (P1)
COND_SVC_PROMOTED = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_svc_promoted) {
    return FLAMEGPU->getVariable<unsigned int>("promoted") == 1u;
}
"""

# Функция: serviceable → operations (P1, 3→2)
RTC_SVC_TO_OPS = """
FLAMEGPU_AGENT_FUNCTION(rtc_svc_to_ops_v7, flamegpu::MessageNone, flamegpu::MessageNone) {
    // P1: PPR сохраняется
    FLAMEGPU->setVariable<unsigned int>("transition_3_to_2", 1u);
    FLAMEGPU->setVariable<unsigned short>("limiter", 0u);  // Будет вычислен
    FLAMEGPU->setVariable<unsigned int>("promoted", 0u);  // Сброс флага
    return flamegpu::ALIVE;
}
"""

# Условие: unserviceable промоутен (P2)
COND_UNSVC_PROMOTED = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_unsvc_promoted) {
    return FLAMEGPU->getVariable<unsigned int>("promoted") == 1u;
}
"""

# Функция: unserviceable → operations (P2, 7→2)
RTC_UNSVC_TO_OPS = """
FLAMEGPU_AGENT_FUNCTION(rtc_unsvc_to_ops_v7, flamegpu::MessageNone, flamegpu::MessageNone) {
    // P2: PPR обнуляется!
    FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_7_to_2", 1u);
    FLAMEGPU->setVariable<unsigned short>("limiter", 0u);  // Будет вычислен
    FLAMEGPU->setVariable<unsigned int>("promoted", 0u);  // Сброс флага
    return flamegpu::ALIVE;
}
"""

# Условие: inactive промоутен (P3)
COND_INACTIVE_PROMOTED = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_inactive_promoted) {
    return FLAMEGPU->getVariable<unsigned int>("promoted") == 1u;
}
"""

# Функция: inactive → operations (P3, 1→2)
RTC_INACTIVE_TO_OPS = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_inactive_to_ops_v7, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // P3: PPR по правилам group_by
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
# Регистрация функций
# ═══════════════════════════════════════════════════════════════════════════════

def register_exit_date_copy(model: fg.ModelDescription, agent: fg.AgentDescription, quota_agent: fg.AgentDescription = None):
    """Фаза 0.5: Копирование exit_date для расчёта adaptive_days (ПОСЛЕ переходов!)"""
    print("  📦 V7 Фаза 0.5: Копирование exit_date (после переходов)...")
    
    # Сброс min_exit_date_mp (QuotaManager)
    if quota_agent is not None:
        layer_reset = model.newLayer("v7_reset_exit_date")
        fn = quota_agent.newRTCFunction("rtc_reset_exit_date_v7", RTC_RESET_EXIT_DATE)
        fn.setInitialState("default")
        fn.setEndState("default")
        layer_reset.addAgentFunction(fn)
    
    # Копирование exit_date из repair
    layer_copy_repair = model.newLayer("v7_copy_exit_date_repair")
    fn = agent.newRTCFunction("rtc_copy_exit_date_repair_v7", RTC_COPY_EXIT_DATE_REPAIR)
    fn.setInitialState("repair")
    fn.setEndState("repair")
    layer_copy_repair.addAgentFunction(fn)
    
    # Копирование exit_date из reserve (spawn)
    layer_copy_spawn = model.newLayer("v7_copy_exit_date_spawn")
    fn = agent.newRTCFunction("rtc_copy_exit_date_spawn_v7", RTC_COPY_EXIT_DATE_SPAWN)
    fn.setInitialState("reserve")
    fn.setEndState("reserve")
    layer_copy_spawn.addAgentFunction(fn)
    
    print("    ✅ Фаза 0.5 готова (exit_date → min_exit_date_mp)")


def register_phase0_deterministic(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Фаза 0: Детерминированные переходы (repair, spawn)"""
    print("  📦 V7 Фаза 0: Детерминированные переходы...")
    
    # repair → serviceable (4→3)
    layer_rep_to_svc = model.newLayer("v7_repair_to_svc")
    fn = agent.newRTCFunction("rtc_repair_to_svc_v7", RTC_REPAIR_TO_SVC)
    fn.setRTCFunctionCondition(COND_REPAIR_EXIT)
    fn.setInitialState("repair")
    fn.setEndState("serviceable")
    layer_rep_to_svc.addAgentFunction(fn)
    
    # УДАЛЕНО: repair stay (4→4) — избыточно
    
    # reserve → operations (spawn, 5→2)
    layer_spawn_to_ops = model.newLayer("v7_spawn_to_ops")
    fn = agent.newRTCFunction("rtc_spawn_to_ops_v7", RTC_SPAWN_TO_OPS)
    fn.setRTCFunctionCondition(COND_SPAWN_EXIT)
    fn.setInitialState("reserve")
    fn.setEndState("operations")
    layer_spawn_to_ops.addAgentFunction(fn)
    
    # УДАЛЕНО: reserve stay (5→5) — избыточно
    
    print("    ✅ Фаза 0 готова (repair, spawn)")


def register_phase1_operations(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Фаза 1: Operations — инкременты и переходы"""
    print("  📦 V7 Фаза 1: Operations инкременты и переходы...")
    
    # Слой 1.1: Инкремент SNE/PPR (ВСЕ агенты в operations)
    layer_inc = model.newLayer("v7_ops_increment")
    fn_inc = agent.newRTCFunction("rtc_ops_increment_v7", RTC_OPS_INCREMENT)
    fn_inc.setInitialState("operations")
    fn_inc.setEndState("operations")  # Пока остаёмся
    layer_inc.addAgentFunction(fn_inc)
    
    # Слой 1.2: Переходы на основе обновлённых значений
    # ВАЖНО: Слои выполняются последовательно, условия уже видят новые SNE/PPR
    
    # operations → storage (2→6) — ПРИОРИТЕТ 1
    layer_to_storage = model.newLayer("v7_ops_to_storage")
    fn = agent.newRTCFunction("rtc_ops_to_storage_v7", RTC_OPS_TO_STORAGE)
    fn.setRTCFunctionCondition(COND_OPS_TO_STORAGE)
    fn.setInitialState("operations")
    fn.setEndState("storage")
    layer_to_storage.addAgentFunction(fn)
    
    # operations → unserviceable (2→7) — ПРИОРИТЕТ 2
    layer_to_unsvc = model.newLayer("v7_ops_to_unsvc")
    fn = agent.newRTCFunction("rtc_ops_to_unsvc_v7", RTC_OPS_TO_UNSVC)
    fn.setRTCFunctionCondition(COND_OPS_TO_UNSVC)
    fn.setInitialState("operations")
    fn.setEndState("unserviceable")
    layer_to_unsvc.addAgentFunction(fn)
    
    print("    ✅ Фаза 1 готова (инкременты, 2→6, 2→7)")


def register_phase2_demote(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Фаза 2: Демоут (после квотирования)"""
    print("  📦 V7 Фаза 2: Демоут...")
    
    # operations → serviceable (демоут, 2→3)
    layer_demote = model.newLayer("v7_ops_demote")
    fn = agent.newRTCFunction("rtc_ops_demote_v7", RTC_OPS_DEMOTE)
    fn.setRTCFunctionCondition(COND_OPS_DEMOTE)
    fn.setInitialState("operations")
    fn.setEndState("serviceable")
    layer_demote.addAgentFunction(fn)
    
    # V7: _stay функции удалены — FLAME GPU автоматически оставляет агентов
    # в своём состоянии если FunctionCondition = false
    
    print("    ✅ Фаза 2 готова (демоут)")


def register_phase3_promote(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Фаза 3: Промоуты P1, P2, P3 (после квотирования)"""
    print("  📦 V7 Фаза 3: Промоуты P1, P2, P3...")
    
    # P1: serviceable → operations (3→2)
    layer_p1 = model.newLayer("v7_svc_to_ops")
    fn = agent.newRTCFunction("rtc_svc_to_ops_v7", RTC_SVC_TO_OPS)
    fn.setRTCFunctionCondition(COND_SVC_PROMOTED)
    fn.setInitialState("serviceable")
    fn.setEndState("operations")
    layer_p1.addAgentFunction(fn)
    
    # P2: unserviceable → operations (7→2)
    layer_p2 = model.newLayer("v7_unsvc_to_ops")
    fn = agent.newRTCFunction("rtc_unsvc_to_ops_v7", RTC_UNSVC_TO_OPS)
    fn.setRTCFunctionCondition(COND_UNSVC_PROMOTED)
    fn.setInitialState("unserviceable")
    fn.setEndState("operations")
    layer_p2.addAgentFunction(fn)
    
    # P3: inactive → operations (1→2)
    layer_p3 = model.newLayer("v7_inactive_to_ops")
    fn = agent.newRTCFunction("rtc_inactive_to_ops_v7", RTC_INACTIVE_TO_OPS)
    fn.setRTCFunctionCondition(COND_INACTIVE_PROMOTED)
    fn.setInitialState("inactive")
    fn.setEndState("operations")
    layer_p3.addAgentFunction(fn)
    
    # V7: _stay функции удалены — FLAME GPU автоматически оставляет агентов
    # в своём состоянии если FunctionCondition = false
    
    print("    ✅ Фаза 3 готова (P1, P2, P3)")


def register_all_v7(model: fg.ModelDescription, agent: fg.AgentDescription, quota_agent: fg.AgentDescription = None):
    """Регистрирует все V7 переходы состояний"""
    print("\n" + "=" * 60)
    print("📦 V7: Однофазные переходы состояний")
    print("=" * 60)
    
    # Фаза 0: Детерминированные переходы (repair→serviceable, spawn→operations)
    register_phase0_deterministic(model, agent)
    
    # Фаза 0.5: Копирование exit_date ПОСЛЕ переходов для расчёта adaptive_days
    # ВАЖНО: должно идти ПОСЛЕ фазы 0, чтобы исключить агентов которые уже вышли
    register_exit_date_copy(model, agent, quota_agent)
    
    register_phase1_operations(model, agent)
    # ФАЗА 2 и 3 регистрируются ПОСЛЕ квотирования
    
    print("=" * 60)
    print("✅ V7 переходы (фазы 0-1) зарегистрированы")
    print("   Фазы 2-3 регистрируются после квотирования")
    print("=" * 60 + "\n")


def register_post_quota_v7(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует фазы 2-3 (после квотирования)"""
    print("\n📦 V7: Фазы после квотирования (демоут, промоуты)...")
    register_phase2_demote(model, agent)
    register_phase3_promote(model, agent)
    print("✅ V7 фазы 2-3 зарегистрированы\n")

