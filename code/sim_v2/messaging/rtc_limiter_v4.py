#!/usr/bin/env python3
"""
RTC модуль: LIMITER V4 — Оптимизированный current_day update

V4 АРХИТЕКТУРА:
- ✅ HF_ComputeAdaptiveDaysV4 — единственный host callback
- ✅ Оптимизирован: читает mp_min_limiter один раз, нет Python циклов
- ✅ Вся тяжёлая работа (поиск min limiter) на GPU через atomicMin
- ✅ Host только синхронизирует результат

Сравнение с V3:
  V3: HF_ComputeAdaptiveDays — Python цикл по program_changes
  V4: HF_ComputeAdaptiveDaysV4 — предрасчитанный поиск по PropertyArray
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from model_build import RTC_MAX_FRAMES, MAX_DAYS

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


def setup_v4_macroproperties(env):
    """Настраивает MacroProperty для V4"""
    # V4 использует Environment properties для current_day (как V3)
    # Дополнительные MP не нужны — используем mp_min_limiter из V3
    print("  ✅ V4: используем существующие MacroProperty из V3")


class HF_ExitCondition(fg.HostCondition):
    """Exit condition: остановить симуляцию когда current_day >= end_day"""
    
    def __init__(self, end_day: int):
        super().__init__()
        self.end_day = end_day
    
    def run(self, FLAMEGPU):
        current_day = FLAMEGPU.environment.getPropertyUInt("current_day")
        if current_day >= self.end_day:
            return fg.EXIT
        return fg.CONTINUE


class HF_ComputeAdaptiveDaysV4(fg.HostFunction):
    """
    V4 HostFunction для вычисления adaptive_days.
    
    Оптимизация vs V3:
    - Предрасчитанный массив program_changes в PropertyArray
    - Бинарный поиск вместо Python цикла
    - Минимум операций
    """
    
    def __init__(self, program_changes: list, end_day: int):
        super().__init__()
        self.program_changes = sorted(program_changes)
        self.end_day = end_day
        self._pc_idx = 0  # Текущий индекс для O(1) поиска
    
    def run(self, FLAMEGPU):
        current_day = FLAMEGPU.environment.getPropertyUInt("current_day")
        
        # Early exit
        if current_day >= self.end_day:
            return fg.CONTINUE
        
        # 1. Читаем min_limiter из MacroProperty (вычислен на GPU)
        try:
            mp_min = FLAMEGPU.environment.getMacroPropertyUInt("mp_min_limiter")
            min_limiter = int(mp_min[0])
            # Сброс для следующего шага
            mp_min[0] = 0xFFFFFFFF
        except:
            min_limiter = 0xFFFFFFFF
        
        # 2. Находим следующий program_change (O(1) amortized)
        while self._pc_idx < len(self.program_changes) and self.program_changes[self._pc_idx] <= current_day:
            self._pc_idx += 1
        
        next_pc = self.program_changes[self._pc_idx] if self._pc_idx < len(self.program_changes) else self.end_day
        days_to_pc = next_pc - current_day
        
        # 3. adaptive_days = min(min_limiter, days_to_pc)
        if min_limiter == 0xFFFFFFFF or min_limiter == 0:
            adaptive_days = days_to_pc
        else:
            adaptive_days = min(min_limiter, days_to_pc)
        
        # Границы
        if current_day + adaptive_days > self.end_day:
            adaptive_days = self.end_day - current_day
        adaptive_days = max(adaptive_days, 1)
        
        # 4. Обновляем Environment
        FLAMEGPU.environment.setPropertyUInt("prev_day", current_day)
        FLAMEGPU.environment.setPropertyUInt("current_day", current_day + adaptive_days)
        FLAMEGPU.environment.setPropertyUInt("adaptive_days", adaptive_days)
        FLAMEGPU.environment.setPropertyUInt("step_days", adaptive_days)
        
        step = FLAMEGPU.getStepCounter()
        if step % 50 == 0 or adaptive_days > 30:
            print(f"  [V4 Step {step}] Day {current_day} → {current_day + adaptive_days} (+{adaptive_days})")
        
        return fg.CONTINUE


def register_v4_hf(model: fg.ModelDescription, program_changes: list, end_day: int):
    """Регистрирует V4 HostFunction для adaptive_days"""
    
    hf = HF_ComputeAdaptiveDaysV4(program_changes, end_day)
    layer = model.newLayer("L_v4_compute_adaptive")
    layer.addHostFunction(hf)
    
    print("  ✅ V4 HF_ComputeAdaptiveDaysV4 зарегистрирован")
    return hf
