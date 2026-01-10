#!/usr/bin/env python3
"""
HostFunction для адаптивного шага симуляции

Вычисляет step_days на основе:
1. Ближайшего изменения программы
2. Ближайшего достижения ресурсного лимита агентом
3. Ближайшего завершения ремонта
"""
import pyflamegpu as fg
from typing import List, Tuple, Optional
import numpy as np


class AdaptiveStepHostFunction(fg.HostFunction):
    """
    HostFunction для вычисления адаптивного шага
    
    Выполняется В НАЧАЛЕ каждого шага симуляции.
    Вычисляет step_days и устанавливает в Environment.
    """
    
    def __init__(self, program_changes: List[Tuple[int, int, int]], 
                 mp5_cumsum: np.ndarray, frames: int, days: int):
        """
        Args:
            program_changes: [(day, mi8_target, mi17_target), ...]
            mp5_cumsum: Кумулятивные суммы dt [frames * (days+1)]
            frames: Количество агентов
            days: Общее количество дней
        """
        super().__init__()
        self.program_changes = program_changes
        self.mp5_cumsum = mp5_cumsum
        self.frames = frames
        self.days = days
        
        # Индекс следующего изменения программы
        self._next_program_idx = 0
    
    def run(self, FLAMEGPU):
        """Вычисляет и устанавливает step_days"""
        
        current_day = FLAMEGPU.environment.getPropertyUInt("current_day")
        end_day = FLAMEGPU.environment.getPropertyUInt("end_day")
        
        # Проверка завершения
        if current_day >= end_day:
            FLAMEGPU.environment.setPropertyUInt("step_days", 0)
            return
        
        # 1. Программный лимитер
        program_days = self._find_next_program_change(current_day)
        min_days = program_days
        
        # 2. Ресурсный лимитер (читаем из агентов в operations)
        resource_days = self._find_min_resource_limit(FLAMEGPU, current_day)
        min_days = min(min_days, resource_days)
        
        # 3. Ремонтный лимитер
        repair_days = self._find_min_repair_limit(FLAMEGPU)
        min_days = min(min_days, repair_days)
        
        # Ограничения
        min_days = min(min_days, 365)  # Не более года за шаг
        min_days = min(min_days, end_day - current_day)  # Не выходить за end_day
        min_days = max(min_days, 1)  # Минимум 1 день
        
        FLAMEGPU.environment.setPropertyUInt("step_days", min_days)
        
        # Логирование (каждые 100 дней или при большом шаге)
        if current_day % 100 == 0 or min_days > 10:
            print(f"[Adaptive Day {current_day}] step_days={min_days} "
                  f"(program={program_days}, resource={resource_days}, repair={repair_days})")
    
    def _find_next_program_change(self, current_day: int) -> int:
        """Найти дней до следующего изменения программы"""
        
        # Найти следующее изменение после current_day
        for day, mi8, mi17 in self.program_changes:
            if day > current_day:
                return day - current_day
        
        return 999999  # Нет больше изменений
    
    def _find_min_resource_limit(self, FLAMEGPU, current_day: int) -> int:
        """Найти минимальный ресурсный лимитер среди агентов в operations"""
        
        min_days = 999999
        
        try:
            # Получаем популяцию HELI в состоянии operations
            agent = FLAMEGPU.agent("HELI", "operations")
            count = agent.count()
            
            if count == 0:
                return min_days
            
            # Итерируем по агентам
            for i in range(count):
                idx = agent.getVariableUInt("idx", i)
                sne = agent.getVariableUInt("sne", i)
                ppr = agent.getVariableUInt("ppr", i)
                ll = agent.getVariableUInt("ll", i)
                oh = agent.getVariableUInt("oh", i)
                
                # Расчёт дней до лимита
                remaining_sne = max(0, ll - sne)
                remaining_ppr = max(0, oh - ppr)
                min_remaining = min(remaining_sne, remaining_ppr)
                
                if min_remaining == 0:
                    return 1  # Уже на лимите — нужен переход немедленно
                
                # Ищем день, когда cumsum достигнет min_remaining
                days_to_limit = self._find_day_for_cumsum(idx, current_day, min_remaining)
                min_days = min(min_days, days_to_limit)
                
        except Exception as e:
            # Если агентов нет или ошибка — игнорируем
            pass
        
        return min_days
    
    def _find_day_for_cumsum(self, idx: int, current_day: int, target_cumsum: int) -> int:
        """Найти количество дней, за которое cumsum dt достигнет target"""
        
        base = idx * (self.days + 1)
        start_cumsum = self.mp5_cumsum[base + current_day]
        
        # Линейный поиск (можно оптимизировать бинарным)
        for d in range(current_day + 1, self.days + 1):
            delta_dt = self.mp5_cumsum[base + d] - start_cumsum
            if delta_dt >= target_cumsum:
                return d - current_day
        
        return 999999  # Не достигнет
    
    def _find_min_repair_limit(self, FLAMEGPU) -> int:
        """Найти минимальный ремонтный лимитер среди агентов в repair"""
        
        min_days = 999999
        
        try:
            agent = FLAMEGPU.agent("HELI", "repair")
            count = agent.count()
            
            for i in range(count):
                repair_time = agent.getVariableUInt("repair_time", i)
                repair_days = agent.getVariableUInt("repair_days", i)
                days_to_complete = max(0, repair_time - repair_days)
                
                if days_to_complete == 0:
                    return 1  # Ремонт завершён — нужен переход немедленно
                
                min_days = min(min_days, days_to_complete)
                
        except Exception:
            pass
        
        return min_days


class UpdateDayHostFunction(fg.HostFunction):
    """
    HostFunction для обновления current_day после шага
    
    Выполняется В КОНЦЕ каждого шага симуляции.
    """
    
    def __init__(self, end_day: int):
        super().__init__()
        self.end_day = end_day
    
    def run(self, FLAMEGPU):
        """Обновляет current_day и проверяет условие завершения"""
        
        current_day = FLAMEGPU.environment.getPropertyUInt("current_day")
        step_days = FLAMEGPU.environment.getPropertyUInt("step_days")
        
        new_day = current_day + step_days
        FLAMEGPU.environment.setPropertyUInt("current_day", new_day)
        
        # Условие завершения
        if new_day >= self.end_day:
            print(f"[Adaptive] Достигнут end_day={self.end_day}, завершаем симуляцию")
            # В FLAME GPU нет прямого setExitCondition, используем simulation.setSimulationSteps
            # Это обрабатывается в orchestrator


def register_adaptive_host_functions(model: fg.ModelDescription,
                                      program_changes: List[Tuple[int, int, int]],
                                      mp5_cumsum: np.ndarray,
                                      frames: int, days: int):
    """
    Регистрирует HostFunction для адаптивного шага
    
    Args:
        model: ModelDescription
        program_changes: Предрасчитанные изменения программы
        mp5_cumsum: Кумулятивные суммы dt
        frames: Количество агентов
        days: Общее количество дней
    """
    
    # Создаём HostFunction для начала шага (вычисление step_days)
    adaptive_func = AdaptiveStepHostFunction(program_changes, mp5_cumsum, frames, days)
    
    # Создаём HostFunction для конца шага (обновление current_day)
    update_func = UpdateDayHostFunction(days)
    
    # Регистрируем как init function (выполняется в начале шага)
    model.addInitFunction(adaptive_func)
    
    # Регистрируем как exit function (выполняется в конце шага)
    model.addExitFunction(update_func)
    
    print(f"  ✅ Адаптивные HostFunction зарегистрированы")
    
    return adaptive_func, update_func

