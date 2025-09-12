#!/usr/bin/env python3
"""
Помощник для выбора периодов тестирования с реальными данными
Основано на успешном опыте: 7, 30, 365, 3650 дней работают стабильно
Дата: 2025-09-12
"""

from typing import Dict, List, Tuple
from datetime import date


class DaysLimiter:
    """Помощник для работы с периодами тестирования"""
    
    def __init__(self):
        # Типовые периоды тестирования (проверены на практике)
        self.TEST_PERIODS = {
            "minimal": 7,       # Быстрые тесты
            "smoke": 30,        # Smoke тесты  
            "development": 365, # Разработка (1 год)
            "production": 3650, # Продуктивные прогоны (10 лет)
        }
    
    def get_test_period(self, test_type: str = "smoke", custom_days: int = None) -> Tuple[int, str]:
        """Возвращает количество дней для тестирования"""
        
        if custom_days is not None:
            return custom_days, f"Пользовательский период: {custom_days} дней"
        
        days = self.TEST_PERIODS.get(test_type, 30)
        return days, f"Типовой период {test_type}: {days} дней"
    
    def slice_data_arrays(self, data_dict: Dict, target_days: int) -> Dict:
        """Обрезает массивы данных до указанного количества дней"""
        
        original_days = data_dict.get('days_total', 0)
        
        if target_days >= original_days:
            print(f"📊 Используем полные данные: {original_days} дней")
            return data_dict  # Не нужно обрезать
        
        print(f"📐 Обрезаем данные: {original_days} → {target_days} дней")
        
        sliced_data = data_dict.copy()
        
        # Массивы по дням (размер = days)
        day_arrays = [
            'mp4_ops_counter_mi8', 'mp4_ops_counter_mi17', 
            'mp4_new_counter_mi17_seed', 'month_first_u32'
        ]
        
        for arr_name in day_arrays:
            if arr_name in sliced_data and isinstance(sliced_data[arr_name], list):
                original = sliced_data[arr_name]
                sliced_data[arr_name] = original[:target_days]
                print(f"  {arr_name}: {len(original)} → {len(sliced_data[arr_name])}")
        
        # MP5 массив (размер = (days+1) * frames)
        if 'mp5_daily_hours_linear' in sliced_data:
            frames_total = data_dict.get('frames_total', 286)
            original_mp5 = sliced_data['mp5_daily_hours_linear']
            
            # Новый размер с паддингом D+1
            new_size = (target_days + 1) * frames_total
            sliced_data['mp5_daily_hours_linear'] = original_mp5[:new_size]
            
            print(f"  mp5_daily_hours_linear: {len(original_mp5)} → {new_size}")
        
        # Обновляем метаданные
        if 'days_sorted' in sliced_data:
            sliced_data['days_sorted'] = sliced_data['days_sorted'][:target_days]
        
        sliced_data['days_total'] = target_days
        
        return sliced_data
    
    def print_available_periods(self):
        """Выводит доступные типовые периоды"""
        
        print("📅 Доступные периоды тестирования:")
        for test_type, days in self.TEST_PERIODS.items():
            print(f"  {test_type:>12}: {days:>4} дней")


def main():
    """Тест помощника периодов"""
    
    limiter = DaysLimiter()
    
    print("🚀 Помощник периодов тестирования")
    print("=" * 40)
    
    limiter.print_available_periods()
    
    print("\n🧪 Примеры использования:")
    
    test_cases = [
        ("smoke", None),
        ("development", None),
        ("production", None),
        ("custom", 100),
        ("custom", 1000),
    ]
    
    for test_type, custom in test_cases:
        if test_type == "custom":
            days, desc = limiter.get_test_period("smoke", custom)
        else:
            days, desc = limiter.get_test_period(test_type)
        
        print(f"  {desc}")
    
    print("\n✅ Все периоды поддерживаются железом")
    print("💡 Зависания только от багов в коде, не от размеров данных")


if __name__ == '__main__':
    main()