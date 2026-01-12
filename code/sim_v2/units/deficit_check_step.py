#!/usr/bin/env python3
"""
StepFunction для проверки дефицита планеров

После каждого шага проверяет:
- Для каждого летающего планера (dt > 0) проверяет mp_planer_slots
- Если slots < comp_per_planer, устанавливает mp_planer_deficit

Логика:
- Если у планера не хватает агрегатов и он должен летать → дефицит
- Планер в дефиците не летает (increment игнорирует его)
- Когда агрегат назначается → дефицит снимается

Дата: 10.01.2026
"""

import pyflamegpu as fg
from typing import Dict, List, Tuple

MAX_GROUPS = 50
MAX_PLANERS = 400


class DeficitCheckStepFunction(fg.HostFunction):
    """
    StepFunction для проверки и записи дефицитов планеров
    
    Выполняется ПОСЛЕ assembly pass2, перед increment.
    """
    
    def __init__(self, comp_numbers: Dict[int, int], target_groups: List[int] = None):
        """
        Args:
            comp_numbers: dict {group_by: comp_per_planer}
            target_groups: список групп для проверки (по умолчанию все)
        """
        super().__init__()
        self.comp_numbers = comp_numbers
        self.target_groups = target_groups or [3, 4]  # По умолчанию только двигатели
        self.deficit_stats: List[Tuple[int, int, int, int]] = []  # (day, group, planer_idx, deficit)
    
    def run(self, host_api: fg.HostAPI):
        """Проверяет и устанавливает дефициты после assembly"""
        
        # Используем getStepCounter() вместо getCurrentStepIndex()
        step_day = host_api.getStepCounter()
        
        # Получаем MacroProperties
        mp_planer_dt = host_api.environment.getMacroPropertyUInt("mp_planer_dt")
        mp_planer_slots = host_api.environment.getMacroPropertyUInt32("mp_planer_slots")
        mp_planer_deficit = host_api.environment.getMacroPropertyUInt32("mp_planer_deficit")
        mp_planer_type = host_api.environment.getMacroPropertyUInt8("mp_planer_type")
        
        deficits_set = 0
        deficits_cleared = 0
        
        for group_by in self.target_groups:
            comp_per_planer = self.comp_numbers.get(group_by, 0)
            if comp_per_planer == 0:
                continue
            
            # Определяем требуемый тип планера
            # group_by=3 (ТВ2) → Mi-8 (type=1)
            # group_by=4 (ТВ3) → Mi-17 (type=2)
            required_type = 0
            if group_by == 3:
                required_type = 1
            elif group_by == 4:
                required_type = 2
            
            for planer_idx in range(1, MAX_PLANERS):
                # Проверяем тип планера
                if required_type > 0:
                    actual_type = mp_planer_type[planer_idx]
                    if actual_type != required_type:
                        continue
                
                # Проверяем летает ли планер
                dt_pos = step_day * MAX_PLANERS + planer_idx
                dt = mp_planer_dt[dt_pos]
                
                if dt == 0:
                    # Планер не летает — нет дефицита
                    deficit_pos = group_by * MAX_PLANERS + planer_idx
                    current_deficit = mp_planer_deficit[deficit_pos]
                    if current_deficit > 0:
                        mp_planer_deficit[deficit_pos] = 0
                        deficits_cleared += 1
                    continue
                
                # Планер летает — проверяем комплектацию
                slots_pos = group_by * MAX_PLANERS + planer_idx
                assigned = mp_planer_slots[slots_pos]
                
                deficit_pos = group_by * MAX_PLANERS + planer_idx
                
                if assigned < comp_per_planer:
                    # Дефицит!
                    deficit_amount = comp_per_planer - assigned
                    current_deficit = mp_planer_deficit[deficit_pos]
                    
                    if current_deficit != deficit_amount:
                        mp_planer_deficit[deficit_pos] = deficit_amount
                        deficits_set += 1
                        
                        # Записываем в статистику
                        self.deficit_stats.append((step_day, group_by, planer_idx, deficit_amount))
                else:
                    # Полная комплектация — снимаем дефицит
                    current_deficit = mp_planer_deficit[deficit_pos]
                    if current_deficit > 0:
                        mp_planer_deficit[deficit_pos] = 0
                        deficits_cleared += 1
        
        # Логирование (только если есть изменения и каждые 100 дней)
        if (deficits_set > 0 or deficits_cleared > 0) and step_day % 100 == 0:
            print(f"  [DEFICIT Day {step_day}] set={deficits_set}, cleared={deficits_cleared}")
    
    def get_deficit_summary(self) -> Dict:
        """Возвращает сводку по дефицитам для аналитики"""
        if not self.deficit_stats:
            return {'total_events': 0, 'by_group': {}}
        
        by_group = {}
        for day, group, planer_idx, deficit in self.deficit_stats:
            if group not in by_group:
                by_group[group] = {'events': 0, 'total_days': 0, 'planers': set()}
            by_group[group]['events'] += 1
            by_group[group]['planers'].add(planer_idx)
        
        # Конвертируем set в count
        for group in by_group:
            by_group[group]['unique_planers'] = len(by_group[group]['planers'])
            del by_group[group]['planers']
        
        return {
            'total_events': len(self.deficit_stats),
            'by_group': by_group
        }


def register_deficit_check(model: fg.ModelDescription, comp_numbers: Dict[int, int],
                           target_groups: List[int] = None) -> DeficitCheckStepFunction:
    """
    Регистрирует StepFunction для проверки дефицита
    
    ВАЖНО: Должен быть зарегистрирован ПОСЛЕ assembly pass2, но ПЕРЕД increment.
    В orchestrator добавляется как model.addStepFunction()
    """
    
    step_fn = DeficitCheckStepFunction(comp_numbers, target_groups)
    model.addStepFunction(step_fn)
    
    print(f"  RTC модуль deficit_check зарегистрирован (StepFunction, groups={target_groups})")
    
    return step_fn

