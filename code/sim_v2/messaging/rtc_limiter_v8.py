#!/usr/bin/env python3
"""
RTC модуль: LIMITER V8 — Упрощённая архитектура с deterministic_dates

АРХИТЕКТУРА V8 (отличия от V5/V7):
1. deterministic_dates_mp — ОДИН MacroProperty со всеми фиксированными датами
   (program_changes + repair_exits + spawn_dates + day_0 + end_day)
2. min_dynamic_mp — минимум от ops.limiter + repair.repair_days
   (unsvc НЕ участвует!)
3. adaptive_days = MIN(min_dynamic, days_to_deterministic)

Преимущества:
- Один источник детерминированных дат вместо трёх
- Упрощённая логика compute_global_min
- unsvc управляется через RepairAgent.capacity (не через exit_date)

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


# ═══════════════════════════════════════════════════════════════════════════════
# КОНСТАНТЫ V8
# ═══════════════════════════════════════════════════════════════════════════════

MAX_DETERMINISTIC_DATES = 200  # Максимум детерминированных дат (program_changes + repairs + spawns)


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
    
    # Environment properties
    try:
        env.newPropertyUInt("num_deterministic_dates", len(deterministic_dates))
    except:
        env.setPropertyUInt("num_deterministic_dates", len(deterministic_dates))
    
    print(f"  ✅ V8 MacroProperty: deterministic_dates_mp[{MAX_DETERMINISTIC_DATES}], "
          f"min_dynamic_mp, num_dates={len(deterministic_dates)}")


# ═══════════════════════════════════════════════════════════════════════════════
# HostFunction для инициализации V8
# ═══════════════════════════════════════════════════════════════════════════════

class HF_InitV8(fg.HostFunction):
    """HostFunction для инициализации V8 MacroProperty"""
    
    def __init__(self, deterministic_dates: list, end_day: int):
        super().__init__()
        self.deterministic_dates = sorted(set(deterministic_dates))
        self.end_day = end_day
        self.initialized = False
    
    def run(self, FLAMEGPU):
        if self.initialized:
            return
        
        print(f"  [HF_InitV8] Загрузка deterministic_dates: {len(self.deterministic_dates)} дат")
        
        # Инициализация current_day_mp
        mp_day = FLAMEGPU.environment.getMacroPropertyUInt("current_day_mp")
        mp_day[0] = 0  # current_day = 0
        mp_day[1] = 0  # prev_day = 0
        
        # Инициализация deterministic_dates_mp
        mp_dates = FLAMEGPU.environment.getMacroPropertyUInt("deterministic_dates_mp")
        for i, day in enumerate(self.deterministic_dates):
            if i >= MAX_DETERMINISTIC_DATES:
                print(f"  ⚠️ Превышен лимит {MAX_DETERMINISTIC_DATES} дат!")
                break
            mp_dates[i] = int(day)
        
        # Заполняем остаток end_day (чтобы поиск не вышел за границы)
        for i in range(len(self.deterministic_dates), MAX_DETERMINISTIC_DATES):
            mp_dates[i] = self.end_day
        
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
        print(f"  [HF_InitV8] ✅ Загружено, первые 5 дат: {self.deterministic_dates[:5]}")


# ═══════════════════════════════════════════════════════════════════════════════
# RTC: Сброс min_dynamic_mp
# ═══════════════════════════════════════════════════════════════════════════════

RTC_RESET_MIN_DYNAMIC = """
FLAMEGPU_AGENT_FUNCTION(rtc_reset_min_dynamic_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V8: Сброс min_dynamic_mp перед сбором
    // Только один агент (group_by=1) выполняет сброс
    const uint8_t group_by = FLAMEGPU->getVariable<uint8_t>("group_by");
    if (group_by != 1u) return flamegpu::ALIVE;
    
    auto mp_min = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("min_dynamic_mp");
    mp_min[0].exchange(0xFFFFFFFFu);  // MAX = нет данных
    
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# RTC: Сбор min_dynamic от operations (limiter) и repair (repair_days)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_COLLECT_MIN_DYNAMIC_OPS = """
FLAMEGPU_AGENT_FUNCTION(rtc_collect_min_dynamic_ops_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V8: operations агенты вносят limiter в min_dynamic
    const unsigned short limiter = FLAMEGPU->getVariable<unsigned short>("limiter");
    
    if (limiter > 0u) {
        auto mp_min = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("min_dynamic_mp");
        mp_min[0].min((unsigned int)limiter);  // atomicMin
    }
    
    return flamegpu::ALIVE;
}
"""

RTC_COLLECT_MIN_DYNAMIC_REPAIR = """
FLAMEGPU_AGENT_FUNCTION(rtc_collect_min_dynamic_repair_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V8: repair агенты вносят repair_days в min_dynamic
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    
    if (repair_days > 0u) {
        auto mp_min = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("min_dynamic_mp");
        mp_min[0].min(repair_days);  // atomicMin
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
    
    // 1. Читаем min_dynamic (от ops.limiter + repair.repair_days)
    auto mp_min_dyn = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("min_dynamic_mp");
    unsigned int min_dynamic = mp_min_dyn[0];
    
    // 2. Находим ближайшую детерминированную дату
    auto mp_dates = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_DETERMINISTIC_DATES}u>("deterministic_dates_mp");
    const unsigned int num_dates = FLAMEGPU->environment.getProperty<unsigned int>("num_deterministic_dates");
    
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
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# RTC: Update day V8
# ═══════════════════════════════════════════════════════════════════════════════

RTC_UPDATE_DAY_V8 = """
FLAMEGPU_AGENT_FUNCTION(rtc_update_day_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V8: Обновление current_day += adaptive_days
    // Только один агент (group_by=1) выполняет
    
    const uint8_t group_by = FLAMEGPU->getVariable<uint8_t>("group_by");
    if (group_by != 1u) return flamegpu::ALIVE;
    
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    auto mp_result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
    
    const unsigned int current_day = mp_day[0];
    const unsigned int adaptive_days = mp_result[0];
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    
    if (current_day >= end_day) return flamegpu::ALIVE;
    
    unsigned int new_day = current_day + adaptive_days;
    if (new_day > end_day) new_day = end_day;
    
    // Сохраняем prev_day
    mp_day[1] = current_day;
    // Обновляем current_day
    mp_day[0] = new_day;
    
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# Регистрация V8 слоёв
# ═══════════════════════════════════════════════════════════════════════════════

def register_v8_adaptive_layers(model, agent, quota_agent, deterministic_dates: list, end_day: int):
    """
    Регистрирует V8 слои для adaptive steps.
    
    Слои:
    1. v8_reset_min_dynamic — сброс min_dynamic_mp
    2. v8_collect_min_ops — ops.limiter → min_dynamic
    3. v8_collect_min_repair — repair.repair_days → min_dynamic
    4. v8_compute_global_min — вычисление adaptive_days
    5. v8_update_day — current_day += adaptive_days
    """
    print("\n📦 V8: Регистрация adaptive layers...")
    
    # HostFunction для инициализации
    hf_init = HF_InitV8(deterministic_dates, end_day)
    layer_init = model.newLayer("v8_init")
    layer_init.addHostFunction(hf_init)
    
    # 1. Reset min_dynamic
    layer_reset = model.newLayer("v8_reset_min_dynamic")
    fn = quota_agent.newRTCFunction("rtc_reset_min_dynamic_v8", RTC_RESET_MIN_DYNAMIC)
    fn.setInitialState("default")
    fn.setEndState("default")
    layer_reset.addAgentFunction(fn)
    
    # 2. Collect min от operations
    layer_ops = model.newLayer("v8_collect_min_ops")
    fn = agent.newRTCFunction("rtc_collect_min_dynamic_ops_v8", RTC_COLLECT_MIN_DYNAMIC_OPS)
    fn.setInitialState("operations")
    fn.setEndState("operations")
    layer_ops.addAgentFunction(fn)
    
    # 3. Collect min от repair
    layer_repair = model.newLayer("v8_collect_min_repair")
    fn = agent.newRTCFunction("rtc_collect_min_dynamic_repair_v8", RTC_COLLECT_MIN_DYNAMIC_REPAIR)
    fn.setInitialState("repair")
    fn.setEndState("repair")
    layer_repair.addAgentFunction(fn)
    
    # 4. Compute global min
    layer_compute = model.newLayer("v8_compute_global_min")
    fn = quota_agent.newRTCFunction("rtc_compute_global_min_v8", RTC_COMPUTE_GLOBAL_MIN_V8)
    fn.setInitialState("default")
    fn.setEndState("default")
    layer_compute.addAgentFunction(fn)
    
    # 5. Update day
    layer_update = model.newLayer("v8_update_day")
    fn = quota_agent.newRTCFunction("rtc_update_day_v8", RTC_UPDATE_DAY_V8)
    fn.setInitialState("default")
    fn.setEndState("default")
    layer_update.addAgentFunction(fn)
    
    print(f"  ✅ V8 adaptive layers зарегистрированы (5 слоёв)")
    
    return hf_init


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
