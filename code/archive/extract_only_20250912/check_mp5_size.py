#!/usr/bin/env python3
"""
Проверка размера MP5 данных
"""
import sys
sys.path.append('.')

from config_loader import get_clickhouse_client
from sim_env_setup import prepare_env_arrays

def main():
    print("Загружаем env_data...")
    env_data = prepare_env_arrays(get_clickhouse_client())
    
    mp5_len = len(env_data.get('mp5_daily_hours_linear', []))
    frames_total = int(env_data.get('frames_total_u16', 0))
    days_total = int(env_data.get('days_total_u16', 0))
    
    print(f"MP5 линейный массив:")
    print(f"  Длина: {mp5_len}")
    print(f"  frames_total: {frames_total}")
    print(f"  days_total: {days_total}")
    print(f"  Ожидаемая длина: (days_total+1)*frames_total = {(days_total+1)*frames_total}")
    
    # Проверяем сколько дней покрывают данные
    if frames_total > 0:
        days_covered = mp5_len // frames_total - 1
        print(f"  Дней покрыто данными: {days_covered}")
        
    # Проверяем размеры для разных DAYS
    for test_days in [7, 30, 365, 3650]:
        required_size = (test_days + 1) * frames_total
        mp2_size = test_days * frames_total
        print(f"\nДля DAYS={test_days}:")
        print(f"  Требуется MP5: {required_size}")
        print(f"  Размер MP2: {mp2_size}")
        print(f"  Хватает данных: {'ДА' if required_size <= mp5_len else 'НЕТ'}")

if __name__ == "__main__":
    main()
