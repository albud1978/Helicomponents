#!/usr/bin/env python3
"""
Тест с реальными данными ClickHouse
Проверка загрузки полных данных и работы с разными периодами
Дата: 2025-09-12
"""

import sys
import argparse
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent / 'utils'))

from sim.env_setup import EnvironmentSetup


def test_real_data_loading(test_type: str = "smoke", custom_days: int = None):
    """Тестирует загрузку реальных данных"""
    
    print(f"🚀 Тест загрузки реальных данных")
    print("=" * 40)
    
    try:
        env_setup = EnvironmentSetup()
        
        # Загрузка данных для указанного периода
        env_data = env_setup.prepare_environment_for_period(test_type, custom_days)
        
        # Анализ загруженных данных
        print(f"\n📊 Статистика загруженных данных:")
        print(f"  Версия: {env_data.get('version_date_u16', 'N/A')}")
        print(f"  Кадров: {env_data.get('frames_total', 'N/A')}")
        print(f"  Дней: {env_data.get('days_total', 'N/A')}")
        print(f"  Агентов: {env_data.get('mp3_count', 'N/A')}")
        
        # Анализ статусов агентов
        if 'mp3_arrays' in env_data:
            mp3_status = env_data['mp3_arrays'].get('mp3_status_id', [])
            status_counts = {}
            for status in mp3_status:
                status_counts[status] = status_counts.get(status, 0) + 1
            
            print(f"\n📈 Распределение статусов агентов:")
            status_names = {1: "Неактивно", 2: "Эксплуатация", 3: "Исправен", 
                           4: "Ремонт", 5: "Резерв", 6: "Хранение"}
            
            for status_id in sorted(status_counts.keys()):
                count = status_counts[status_id]
                name = status_names.get(status_id, f"Статус_{status_id}")
                print(f"    {status_id} ({name}): {count}")
        
        # Анализ квот
        if 'mp4_ops_counter_mi8' in env_data:
            mp4_mi8 = env_data['mp4_ops_counter_mi8']
            mp4_mi17 = env_data['mp4_ops_counter_mi17']
            
            print(f"\n📋 Квоты операций:")
            print(f"  MI-8: мин={min(mp4_mi8)}, макс={max(mp4_mi8)}, среднее={sum(mp4_mi8)//len(mp4_mi8)}")
            print(f"  MI-17: мин={min(mp4_mi17)}, макс={max(mp4_mi17)}, среднее={sum(mp4_mi17)//len(mp4_mi17)}")
        
        # Анализ MP5 налетов
        if 'mp5_daily_hours_linear' in env_data:
            mp5_data = env_data['mp5_daily_hours_linear']
            non_zero = [x for x in mp5_data if x > 0]
            
            print(f"\n✈️ Налеты MP5:")
            print(f"  Всего записей: {len(mp5_data)}")
            print(f"  Ненулевых: {len(non_zero)}")
            if non_zero:
                print(f"  Мин налет: {min(non_zero)} мин")
                print(f"  Макс налет: {max(non_zero)} мин")
                print(f"  Средний: {sum(non_zero)//len(non_zero)} мин")
        
        print(f"\n✅ Реальные данные загружены успешно")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка загрузки реальных данных: {e}")
        import traceback
        traceback.print_exc()
        return False


def create_parser():
    """Создает парсер аргументов"""
    
    parser = argparse.ArgumentParser(description='Тест реальных данных')
    
    parser.add_argument('--type', choices=['minimal', 'smoke', 'development', 'production'],
                       default='smoke', help='Тип тестового периода')
    parser.add_argument('--days', type=int, help='Конкретное количество дней')
    parser.add_argument('--show-periods', action='store_true', help='Показать доступные периоды')
    
    return parser


def main():
    """Главная функция"""
    
    parser = create_parser()
    args = parser.parse_args()
    
    limiter = EnvironmentSetup().days_limiter
    
    if args.show_periods:
        limiter.print_available_periods()
        return 0
    
    # Тест загрузки данных
    success = test_real_data_loading(args.type, args.days)
    
    if success:
        print("\n🎉 Тест с реальными данными прошел успешно!")
        return 0
    else:
        print("\n❌ Есть проблемы с загрузкой реальных данных")
        return 1


if __name__ == '__main__':
    sys.exit(main())


