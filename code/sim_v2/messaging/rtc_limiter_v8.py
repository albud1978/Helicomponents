#!/usr/bin/env python3
"""
RTC модуль: LIMITER V8 — Упрощённая архитектура с deterministic_dates

АРХИТЕКТУРА V8 (отличия от V5/V7):
1. deterministic_dates_mp — ОДИН MacroProperty со всеми фиксированными датами
   (program_changes + repair_exits + spawn_dates + day_0 + end_day)
2. min_dynamic_mp — минимум от ops.limiter + repair.repair_days (ТОЛЬКО day-0 ремонт)
   Хранится в виде (value << 1 | source), где source: 0=limiter, 1=repair_days
   (unsvc НЕ участвует!)
3. adaptive_days = MIN(min_dynamic, days_to_deterministic)

Преимущества:
- Один источник детерминированных дат вместо трёх
- Упрощённая логика compute_global_min
- unserviceable управляется через RepairLine (квотирование), не через exit_date

См. docs/architecture/adaptive_steps_logic.md для полной архитектуры.

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


# ═══════════════════════════════════════════════════════════════════════════════
# КОНСТАНТЫ V8
# ═══════════════════════════════════════════════════════════════════════════════

MAX_DETERMINISTIC_DATES = 500  # Максимум детерминированных дат (program_changes + repairs + spawns)


# ═══════════════════════════════════════════════════════════════════════════════
# MacroProperty для V8
# ═══════════════════════════════════════════════════════════════════════════════

def setup_v8_macroproperties(env, deterministic_dates: list):
    """
    Настраивает MacroProperty для V8.
    
    V8 использует ОДИН массив deterministic_dates вместо отдельных:
    - program_changes_mp
    - min_exit_date_mp (для repair/spawn)
    """
    
    # current_day в MacroProperty (как V5)
    env.newMacroPropertyUInt("current_day_mp", 4)  # [0]=current_day, [1]=prev_day
    
    # adaptive_days результат
    env.newMacroPropertyUInt("adaptive_result_mp", 4)  # [0]=adaptive_days
    
    # V8: ОДИН массив deterministic_dates
    env.newMacroPropertyUInt("deterministic_dates_mp", MAX_DETERMINISTIC_DATES)
    
    # V8: min_dynamic (ops.limiter + repair.repair_days, unsvc исключён)
    env.newMacroPropertyUInt("min_dynamic_mp", 4)  # [0]=min_dynamic
    
    # limiter_buffer для atomicMin (как V5)
    env.newMacroPropertyUInt("limiter_buffer", RTC_MAX_FRAMES)
    
    # mp_min_limiter для совместимости с V7 модулями
    try:
        env.newMacroPropertyUInt("mp_min_limiter", 4)
    except:
        pass  # Уже существует
    
    # min_exit_date_mp для совместимости с V7 state transitions
    try:
        env.newMacroPropertyUInt("min_exit_date_mp", 4)
    except:
        pass  # Уже существует
    
    # Environment properties (значения будут заполнены после populate_agents)
    try:
        env.newPropertyUInt("num_deterministic_dates", 0)
    except Exception:
        pass
    
    print(f"  ✅ V8 MacroProperty: deterministic_dates_mp[{MAX_DETERMINISTIC_DATES}], "
          f"min_dynamic_mp, num_dates=deferred")


# ═══════════════════════════════════════════════════════════════════════════════
# HostFunction для инициализации V8
# ═══════════════════════════════════════════════════════════════════════════════

# HF_InitV8: единственный способ инициализировать MacroProperty до simulate()
# (CUDASimulation не имеет API для MacroProperty, только HostFunction.environment)
class HF_InitV8(fg.HostFunction):
    """HostFunction для инициализации V8 MacroProperty (addInitFunction)."""
    
    def __init__(self, deterministic_dates: list, end_day: int):
        super().__init__()
        # Храним ссылку: список может быть дополнен до старта симуляции
        self.deterministic_dates = deterministic_dates
        self.end_day = end_day
        self.initialized = False
    
    def run(self, FLAMEGPU):
        if self.initialized:
            return
        
        dates = sorted(set(self.deterministic_dates))
        total_dates = len(dates)
        if total_dates > MAX_DETERMINISTIC_DATES:
            print(f"  ⚠️ Превышен лимит {MAX_DETERMINISTIC_DATES} дат: {total_dates} → {MAX_DETERMINISTIC_DATES}")
        print(f"  [HF_InitV8] Загрузка deterministic_dates: {min(total_dates, MAX_DETERMINISTIC_DATES)} дат")
        
        # Инициализация current_day_mp
        mp_day = FLAMEGPU.environment.getMacroPropertyUInt("current_day_mp")
        mp_day[0] = 0  # current_day = 0
        mp_day[1] = 0  # prev_day = 0
        
        # Инициализация deterministic_dates_mp
        mp_dates = FLAMEGPU.environment.getMacroPropertyUInt("deterministic_dates_mp")
        for i, day in enumerate(dates[:MAX_DETERMINISTIC_DATES]):
            mp_dates[i] = int(day)
        
        # Заполняем остаток end_day (чтобы поиск не вышел за границы)
        effective_len = min(total_dates, MAX_DETERMINISTIC_DATES)
        for i in range(effective_len, MAX_DETERMINISTIC_DATES):
            mp_dates[i] = self.end_day
        
        # Синхронизируем num_deterministic_dates (используется в RTC)
        FLAMEGPU.environment.setPropertyUInt("num_deterministic_dates", effective_len)
        
        # Инициализация min_dynamic_mp
        mp_min = FLAMEGPU.environment.getMacroPropertyUInt("min_dynamic_mp")
        mp_min[0] = 0xFFFFFFFF  # MAX (нет данных)
        
        # Инициализация adaptive_result_mp
        mp_result = FLAMEGPU.environment.getMacroPropertyUInt("adaptive_result_mp")
        mp_result[0] = 1  # adaptive_days = 1 по умолчанию
        
        # Инициализация mp_min_limiter (совместимость с V7)
        mp_min_lim = FLAMEGPU.environment.getMacroPropertyUInt("mp_min_limiter")
        mp_min_lim[0] = 0xFFFFFFFF
        
        # Инициализация min_exit_date_mp (совместимость с V7)
        mp_exit = FLAMEGPU.environment.getMacroPropertyUInt("min_exit_date_mp")
        mp_exit[0] = 0xFFFFFFFF
        
        self.initialized = True
        print(f"  [HF_InitV8] ✅ Загружено, первые 5 дат: {dates[:5]}")


# ═══════════════════════════════════════════════════════════════════════════════
# RTC: Сбор min_dynamic от operations (limiter) и repair (repair_days, только day-0)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_COLLECT_MIN_DYNAMIC_OPS = """
FLAMEGPU_AGENT_FUNCTION(rtc_collect_min_dynamic_ops_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V8: operations агенты вносят limiter в min_dynamic
    const unsigned short limiter = FLAMEGPU->getVariable<unsigned short>("limiter");
    
    if (limiter > 0u) {
        auto mp_min = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("min_dynamic_mp");
        // Кодируем источник: 0 = limiter
        const unsigned int combined = ((unsigned int)limiter << 1u);
        mp_min[0].min(combined);  // atomicMin
    }
    
    return flamegpu::ALIVE;
}
"""

RTC_COLLECT_MIN_DYNAMIC_REPAIR = """
FLAMEGPU_AGENT_FUNCTION(rtc_collect_min_dynamic_repair_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V8: repair агенты (day-0) вносят repair_days в min_dynamic
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    
    if (repair_days > 0u) {
        auto mp_min = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("min_dynamic_mp");
        // Кодируем источник: 1 = repair_days
        const unsigned int combined = (repair_days << 1u) | 1u;
        mp_min[0].min(combined);  // atomicMin
    }
    
    return flamegpu::ALIVE;
}
"""

# ВАЖНО: unsvc НЕ вносит данные в min_dynamic! (управляется через RepairAgent)


# ═══════════════════════════════════════════════════════════════════════════════
# RTC: Compute global min V8 (упрощённая логика)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_COMPUTE_GLOBAL_MIN_V8 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_compute_global_min_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // V8: Вычисление adaptive_days = MIN(min_dynamic, days_to_deterministic)
    // ВАЖНО: только агент group_by=1 выполняет вычисления
    
    const uint8_t group_by = FLAMEGPU->getVariable<uint8_t>("group_by");
    if (group_by != 1u) return flamegpu::ALIVE;
    
    // Читаем current_day
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    const unsigned int current_day = mp_day[0];
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    
    // Early return если завершено
    if (current_day >= end_day) {{
        auto result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
        result[0].exchange(0u);
        return flamegpu::ALIVE;
    }}
    
    // 1. Читаем и одновременно сбрасываем min_dynamic (атомарно, без отдельного reset-слоя)
    auto mp_min_dyn = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("min_dynamic_mp");
    const unsigned int min_dynamic_combined = mp_min_dyn[0].exchange(0xFFFFFFFFu);
    unsigned int min_dynamic = 0xFFFFFFFFu;
    if (min_dynamic_combined != 0xFFFFFFFFu) {{
        min_dynamic = (min_dynamic_combined >> 1u);
    }}
    
    // 2. Находим ближайшую детерминированную дату
    auto mp_dates = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_DETERMINISTIC_DATES}u>("deterministic_dates_mp");
    const unsigned int num_dates_prop = FLAMEGPU->environment.getProperty<unsigned int>("num_deterministic_dates");
    const unsigned int num_dates = (num_dates_prop < {MAX_DETERMINISTIC_DATES}u) ? num_dates_prop : {MAX_DETERMINISTIC_DATES}u;
    
    unsigned int next_deterministic = end_day;
    for (unsigned int i = 0u; i < num_dates && i < {MAX_DETERMINISTIC_DATES}u; ++i) {{
        unsigned int det_day = mp_dates[i];
        if (det_day > current_day) {{
            next_deterministic = det_day;
            break;  // Массив отсортирован
        }}
    }}
    
    unsigned int days_to_det = next_deterministic - current_day;
    
    // 3. adaptive_days = MIN(min_dynamic, days_to_det)
    unsigned int adaptive_days = days_to_det;
    
    if (min_dynamic < 0xFFFFFFFFu && min_dynamic > 0u && min_dynamic < adaptive_days) {{
        adaptive_days = min_dynamic;
    }}
    
    // Не выходить за end_day
    unsigned int remaining = end_day - current_day;
    if (adaptive_days > remaining) adaptive_days = remaining;
    
    if (adaptive_days < 1u) adaptive_days = 1u;
    
    // DEBUG: каждые 50 шагов
    unsigned int step = FLAMEGPU->getStepCounter();
    if (step % 50u == 0u || step < 10u) {{
        printf("[V8] step=%u, day=%u, min_dyn=%u, next_det=%u -> adaptive=%u\\n",
               step, current_day, min_dynamic, next_deterministic, adaptive_days);
    }}
    
    // 4. Записываем результат
    auto result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
    result[0].exchange(adaptive_days);
    // [1] хранит источник min_dynamic для логгера (combined)
    result[1].exchange(min_dynamic_combined);
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# HostFunction: Update day V8 (заменяет RTC для избежания race condition)
# ═══════════════════════════════════════════════════════════════════════════════

class HF_UpdateDayV8(fg.HostFunction):
    """
    HostFunction для обновления current_day += adaptive_days.
    Заменяет RTC функцию для избежания race condition при чтении/записи MacroProperty.
    """
    
    def __init__(self, end_day: int):
        super().__init__()
        self.end_day = end_day
    
    def run(self, FLAMEGPU):
        mp_day = FLAMEGPU.environment.getMacroPropertyUInt("current_day_mp")
        mp_result = FLAMEGPU.environment.getMacroPropertyUInt("adaptive_result_mp")
        
        current_day = int(mp_day[0])
        adaptive_days = int(mp_result[0])
        
        if current_day >= self.end_day:
            return
        
        new_day = current_day + adaptive_days
        if new_day > self.end_day:
            new_day = self.end_day
        
        # Сохраняем prev_day
        mp_day[1] = current_day
        # Обновляем current_day
        mp_day[0] = new_day

        # Синхронизация в Environment (для корректного чтения на host после шага)
        FLAMEGPU.environment.setPropertyUInt("prev_day", current_day)
        FLAMEGPU.environment.setPropertyUInt("current_day", new_day)
        FLAMEGPU.environment.setPropertyUInt("adaptive_days", adaptive_days)
        FLAMEGPU.environment.setPropertyUInt("step_days", adaptive_days)


# ═══════════════════════════════════════════════════════════════════════════════
# Регистрация V8 слоёв
# ═══════════════════════════════════════════════════════════════════════════════

def register_v8_pre_quota_layers(model, agent, quota_agent, deterministic_dates: list, end_day: int):
    """
    Регистрирует V8 слои до квотирования (collect/compute).
    
    Слои:
    1. v8_collect_min_ops
    2. v8_collect_min_repair
    3. v8_compute_global_min
    """
    print("\n📦 V8: Регистрация adaptive pre-quota layers...")
    
    # REMOVED (move-hf-init-v8): HF_InitV8 заменён прямой инициализацией MacroProperty.
    
    # 1. Collect min от operations
    layer_ops = model.newLayer("v8_collect_min_ops")
    fn = agent.newRTCFunction("rtc_collect_min_dynamic_ops_v8", RTC_COLLECT_MIN_DYNAMIC_OPS)
    fn.setInitialState("operations")
    fn.setEndState("operations")
    layer_ops.addAgentFunction(fn)
    
    # 2. Collect min от repair
    layer_repair = model.newLayer("v8_collect_min_repair")
    fn = agent.newRTCFunction("rtc_collect_min_dynamic_repair_v8", RTC_COLLECT_MIN_DYNAMIC_REPAIR)
    fn.setInitialState("repair")
    fn.setEndState("repair")
    layer_repair.addAgentFunction(fn)
    
    # 3. Compute global min
    layer_compute = model.newLayer("v8_compute_global_min")
    fn = quota_agent.newRTCFunction("rtc_compute_global_min_v8", RTC_COMPUTE_GLOBAL_MIN_V8)
    fn.setInitialState("default")
    fn.setEndState("default")
    layer_compute.addAgentFunction(fn)
    
    print(f"  ✅ V8 adaptive pre-quota layers зарегистрированы (3 слоя)")
    
    return None


def register_v8_update_day_layer(model, end_day: int):
    """Регистрирует обновление дня после всех переходов и квотирования."""
    hf_update_day = HF_UpdateDayV8(end_day)
    layer_update = model.newLayer("v8_update_day")
    layer_update.addHostFunction(hf_update_day)
    print("  ✅ V8 update_day layer зарегистрирован")


# ═══════════════════════════════════════════════════════════════════════════════
# Exit Condition V8
# ═══════════════════════════════════════════════════════════════════════════════

class HF_ExitConditionV8(fg.HostCondition):
    """Exit condition для V8 — завершение когда current_day >= end_day"""
    
    def __init__(self, end_day: int):
        super().__init__()
        self.end_day = end_day
    
    def run(self, FLAMEGPU) -> bool:
        mp_day = FLAMEGPU.environment.getMacroPropertyUInt("current_day_mp")
        current_day = mp_day[0]
        return current_day >= self.end_day
