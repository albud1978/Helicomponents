#!/usr/bin/env python3
"""
HostFunction для подсчёта дефицита агрегатов на планерах

Запускается в начале каждого шага и устанавливает mp_request_count
независимо от наличия агрегатов в serviceable.

Логика:
1. Для каждой группы считаем количество ВС в operations (из history)
2. Умножаем на comp_per_planer
3. Вычитаем текущее количество в operations
4. Если дефицит > 0 → request_count = дефицит

Дата: 14.01.2026
"""

import pyflamegpu as fg
from typing import Dict

MAX_GROUPS = 50
MAX_PLANERS = 400


class DemandHostFunction(fg.HostFunction):
    """
    HostFunction для расчёта дефицита агрегатов
    
    Выполняется в начале каждого шага.
    """
    
    def __init__(self, comp_numbers: Dict[int, int], target_groups: list = None):
        """
        Args:
            comp_numbers: dict {group_by: comp_per_planer}
            target_groups: список групп для проверки (по умолчанию [3, 4] — двигатели)
        """
        super().__init__()
        self.comp_numbers = comp_numbers
        self.target_groups = target_groups or [3, 4]
    
    def run(self, FLAMEGPU):
        step_day = FLAMEGPU.getStepCounter()
        
        # Получаем MacroProperties
        mp_history = FLAMEGPU.environment.getMacroPropertyUInt8("mp_planer_in_ops_history")
        mp_planer_type = FLAMEGPU.environment.getMacroPropertyUInt8("mp_planer_type")
        mp_slots = FLAMEGPU.environment.getMacroPropertyUInt32("mp_planer_slots")
        mp_request_count = FLAMEGPU.environment.getMacroPropertyUInt32("mp_request_count")
        
        # DEBUG: счётчики для дня 0
        debug_day = (step_day == 0 or step_day % 1000 == 0)
        
        for group_by in self.target_groups:
            comp_per_planer = self.comp_numbers.get(group_by, 0)
            if comp_per_planer == 0:
                if debug_day:
                    print(f"     [DemandHost] day={step_day}, group={group_by}: comp_per_planer=0, skip")
                continue
            
            # Определяем требуемый тип планера
            required_type = 0
            if group_by == 3:
                required_type = 1  # Mi-8
            elif group_by == 4:
                required_type = 2  # Mi-17
            
            # Считаем сколько ВС в operations нужны агрегаты этой группы
            deficit = 0
            planers_in_ops = 0
            planers_with_deficit = 0
            for planer_idx in range(1, MAX_PLANERS):
                # Проверяем тип планера
                if required_type > 0:
                    actual_type = int(mp_planer_type[planer_idx])
                    if actual_type != required_type:
                        continue
                
                # Проверяем в operations ли планер
                history_pos = step_day * MAX_PLANERS + planer_idx
                in_ops = int(mp_history[history_pos]) if history_pos < len(mp_history) else 0
                
                if in_ops != 1:
                    continue
                
                planers_in_ops += 1
                
                # Считаем дефицит для этого планера
                slots_pos = group_by * MAX_PLANERS + planer_idx
                current_slots = int(mp_slots[slots_pos]) if slots_pos < len(mp_slots) else 0
                
                if current_slots < comp_per_planer:
                    deficit += (comp_per_planer - current_slots)
                    planers_with_deficit += 1
            
            # Устанавливаем request_count
            if deficit > 0:
                mp_request_count[group_by] = deficit
            else:
                mp_request_count[group_by] = 0
            
            # DEBUG: выводим статистику
            if debug_day:
                print(f"     [DemandHost] day={step_day}, group={group_by}: in_ops={planers_in_ops}, with_deficit={planers_with_deficit}, deficit={deficit}")


def register_demand_host(model: fg.ModelDescription, comp_numbers: Dict[int, int], 
                        target_groups: list = None) -> DemandHostFunction:
    """
    Регистрирует HostFunction для расчёта дефицита
    
    Args:
        model: FLAME GPU модель
        comp_numbers: dict {group_by: comp_per_planer}
        target_groups: список групп для проверки
    
    Returns:
        DemandHostFunction
    """
    hf = DemandHostFunction(comp_numbers, target_groups)
    
    # Создаём слой (должен быть ПЕРВЫМ!)
    layer = model.newLayer("layer_demand_host")
    layer.addHostFunction(hf)
    
    print(f"  ✅ DemandHostFunction зарегистрирован (группы: {target_groups or [3, 4]})")
    
    return hf

