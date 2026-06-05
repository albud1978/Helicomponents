#!/usr/bin/env python3
"""
RTC модуль: LIMITER V8 — Упрощённая архитектура с deterministic_dates

АРХИТЕКТУРА V8 (отличия от V5/V7):
1. deterministic_dates_mp — ОДИН MacroProperty со всеми фиксированными датами
   (program_changes + repair_exits + spawn_dates + day_0 + end_day)
2. adaptive_days рассчитывает активный HF_StepController по mp_min_limiter и deterministic_dates

Преимущества:
- Один источник детерминированных дат
- Активный adaptive path не использует legacy pre-quota RTC слои
- unserviceable управляется через RepairLine (квотирование), не через exit_date

См. docs/architecture/adaptive_steps_logic.md для полной архитектуры.

Дата: 16.01.2026
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

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
    """
    
    # current_day в MacroProperty (как V5)
    env.newMacroPropertyUInt("current_day_mp", 4)  # [0]=current_day, [1]=prev_day
    
    # adaptive_days результат
    env.newMacroPropertyUInt("adaptive_result_mp", 4)  # [0]=adaptive_days
    
    # V8: ОДИН массив deterministic_dates
    env.newMacroPropertyUInt("deterministic_dates_mp", MAX_DETERMINISTIC_DATES)
    
    # mp_min_limiter для совместимости с V7 модулями
    try:
        env.newMacroPropertyUInt("mp_min_limiter", 4)
    except:
        pass  # Уже существует
    
    # Environment properties (значения будут заполнены после populate_agents)
    try:
        env.newPropertyUInt("num_deterministic_dates", 0)
    except Exception:
        pass
    
    print(f"  ✅ V8 MacroProperty: deterministic_dates_mp[{MAX_DETERMINISTIC_DATES}], num_dates=deferred")


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
        
        # Инициализация adaptive_result_mp
        mp_result = FLAMEGPU.environment.getMacroPropertyUInt("adaptive_result_mp")
        mp_result[0] = 1  # adaptive_days = 1 по умолчанию
        
        # Инициализация mp_min_limiter (совместимость с V7)
        mp_min_lim = FLAMEGPU.environment.getMacroPropertyUInt("mp_min_limiter")
        mp_min_lim[0] = 0xFFFFFFFF
        
        self.initialized = True
        print(f"  [HF_InitV8] ✅ Загружено, первые 5 дат: {dates[:5]}")


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
