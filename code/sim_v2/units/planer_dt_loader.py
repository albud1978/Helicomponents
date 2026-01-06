#!/usr/bin/env python3
"""
Загрузчик dt планеров для симуляции агрегатов

Для последовательной интеграции:
1. Симуляция планеров → MP2 (dt по дням)
2. Симуляция агрегатов читает dt из MP2 планеров

Данный модуль загружает dt из:
- sim_masterv2 (результаты симуляции планеров) — приоритет
- flight_program_fl (нормативный налёт) — fallback

Формат: mp_planer_dt[day * MAX_PLANERS + planer_idx] = dt в минутах

Дата: 05.01.2026
"""

import numpy as np
from typing import Dict, Tuple, Optional
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from utils.config_loader import get_clickhouse_client


MAX_PLANERS = 400  # Максимум планеров (реально ~280)
MAX_DAYS = 3651    # 10 лет + 1


def load_planer_dt_from_sim(version_date: str, version_id: int = 1) -> Tuple[np.ndarray, Dict[int, int]]:
    """
    Загружает dt из sim_masterv2 (результаты симуляции планеров)
    
    Returns:
        dt_array: np.ndarray shape (MAX_DAYS * MAX_PLANERS,) — линейный массив dt
        ac_to_idx: Dict[aircraft_number → planer_idx] — маппинг номеров бортов
    """
    client = get_clickhouse_client()
    
    # Получаем список планеров
    # Конвертируем version_date в числовой формат (дни от 1970-01-01)
    from datetime import date
    if isinstance(version_date, str):
        vd = date.fromisoformat(version_date)
        version_date_int = (vd - date(1970, 1, 1)).days
    else:
        version_date_int = version_date
    
    planers_sql = """
    SELECT DISTINCT aircraft_number
    FROM sim_masterv2
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND group_by IN (1, 2)
    ORDER BY aircraft_number
    """
    
    planers = client.execute(planers_sql, {
        'version_date': version_date_int,
        'version_id': version_id
    })
    
    if not planers:
        print(f"⚠️ Нет данных симуляции планеров для {version_date}")
        return None, {}
    
    # Создаём маппинг aircraft_number → idx
    ac_to_idx = {row[0]: idx for idx, row in enumerate(planers)}
    print(f"   Загружено {len(ac_to_idx)} планеров из sim_masterv2")
    
    # Загружаем dt по дням
    dt_sql = """
    SELECT day_u16 as day_index, aircraft_number, dt
    FROM sim_masterv2
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND group_by IN (1, 2)
      AND dt > 0
    ORDER BY day_u16, aircraft_number
    """
    
    dt_data = client.execute(dt_sql, {
        'version_date': version_date_int,
        'version_id': version_id
    })
    
    # Инициализируем массив нулями
    dt_array = np.zeros(MAX_DAYS * MAX_PLANERS, dtype=np.uint32)
    
    # Заполняем
    for row in dt_data:
        day_idx, ac_num, dt_val = row[0], row[1], row[2]
        if ac_num in ac_to_idx and day_idx < MAX_DAYS:
            planer_idx = ac_to_idx[ac_num]
            pos = day_idx * MAX_PLANERS + planer_idx
            dt_array[pos] = int(dt_val)
    
    total_dt = np.sum(dt_array)
    print(f"   ✅ Загружено {len(dt_data)} записей dt, сумма = {total_dt / 60:.0f} часов")
    
    # Загружаем состояние планера (для блокировки dt при ремонте/inactive)
    # state - это String: 'operations', 'inactive', 'repair', 'reserve', 'storage'
    state_sql = """
    SELECT day_u16, aircraft_number, state
    FROM sim_masterv2
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND group_by IN (1, 2)
    ORDER BY day_u16, aircraft_number
    """
    
    state_data = client.execute(state_sql, {
        'version_date': version_date_int,
        'version_id': version_id
    })
    
    # Массив состояний: True = operations, False = другое
    is_operations = np.zeros(MAX_DAYS * MAX_PLANERS, dtype=np.bool_)
    
    for row in state_data:
        day_idx, ac_num, state_str = row[0], row[1], row[2]
        if ac_num in ac_to_idx and day_idx < MAX_DAYS:
            planer_idx = ac_to_idx[ac_num]
            pos = day_idx * MAX_PLANERS + planer_idx
            is_operations[pos] = (state_str == 'operations')
    
    # Применяем блокировку: dt = 0 если планер НЕ в operations
    blocked_count = 0
    for i in range(len(dt_array)):
        if not is_operations[i] and dt_array[i] > 0:
            dt_array[i] = 0
            blocked_count += 1
    
    if blocked_count > 0:
        print(f"   🚫 Заблокировано {blocked_count:,} записей dt (планер не в operations)")
    
    return dt_array, ac_to_idx


def load_planer_dt_from_program(version_date: str, version_id: int = 1) -> Tuple[np.ndarray, Dict[int, int]]:
    """
    Загружает dt из flight_program_fl (нормативный налёт)
    Fallback если нет sim_masterv2
    """
    client = get_clickhouse_client()
    
    # Получаем список планеров из heli_pandas
    planers_sql = """
    SELECT DISTINCT aircraft_number
    FROM heli_pandas
    WHERE toString(version_date) = %(version_date)s
      AND version_id = %(version_id)s
      AND group_by IN (1, 2)
      AND aircraft_number > 0
    ORDER BY aircraft_number
    """
    
    planers = client.execute(planers_sql, {
        'version_date': version_date,
        'version_id': version_id
    })
    
    if not planers:
        print(f"⚠️ Нет планеров в heli_pandas для {version_date}")
        return None, {}
    
    ac_to_idx = {row[0]: idx for idx, row in enumerate(planers)}
    print(f"   Загружено {len(ac_to_idx)} планеров из heli_pandas")
    
    # Загружаем нормативный налёт
    # Колонки: dates (Date), daily_hours (UInt32 минуты)
    program_sql = """
    SELECT 
        toUInt32(dates - version_date) as day_idx,
        aircraft_number, 
        daily_hours
    FROM flight_program_fl
    WHERE toString(version_date) = %(version_date)s
      AND version_id = %(version_id)s
      AND daily_hours > 0
    ORDER BY day_idx, aircraft_number
    """
    
    program_data = client.execute(program_sql, {
        'version_date': version_date,
        'version_id': version_id
    })
    
    dt_array = np.zeros(MAX_DAYS * MAX_PLANERS, dtype=np.uint32)
    
    for day_idx, ac_num, daily_hours in program_data:
        if ac_num in ac_to_idx and day_idx < MAX_DAYS:
            planer_idx = ac_to_idx[ac_num]
            pos = day_idx * MAX_PLANERS + planer_idx
            # daily_hours уже в минутах
            dt_array[pos] = int(daily_hours)
    
    total_dt = np.sum(dt_array)
    print(f"   Загружено {len(program_data)} записей программы, сумма = {total_dt / 60:.0f} часов")
    
    return dt_array, ac_to_idx


def load_planer_dt(version_date: str, version_id: int = 1) -> Tuple[Optional[np.ndarray], Dict[int, int]]:
    """
    Основная функция загрузки dt планеров
    
    Логика:
    1. Попытаться загрузить реальный dt из sim_masterv2 (результат симуляции планеров)
    2. Если sim_masterv2 пуст или dt=0 → fallback на flight_program_fl с блокировкой по state
    
    Returns:
        dt_array: массив dt[day * MAX_PLANERS + planer_idx]
        ac_to_idx: маппинг aircraft_number → planer_idx
    """
    print("📊 Загрузка dt планеров...")
    
    # Пробуем загрузить реальный dt из sim_masterv2
    dt_array, ac_to_idx = load_planer_dt_from_sim(version_date, version_id)
    
    if dt_array is not None and np.sum(dt_array) > 0:
        print(f"   ✅ Использован реальный dt из sim_masterv2")
        return dt_array, ac_to_idx
    
    # Fallback: загружаем из программы с блокировкой по state
    print("   ⚠️ sim_masterv2 пуст или dt=0, fallback на flight_program_fl")
    dt_array, ac_to_idx = load_planer_dt_from_program(version_date, version_id)
    
    if dt_array is None or len(ac_to_idx) == 0:
        print("   ⚠️ Нет данных flight_program_fl")
        return None, {}
    
    # Загружаем state из sim_masterv2 для блокировки
    client = get_clickhouse_client()
    
    from datetime import date
    if isinstance(version_date, str):
        vd = date.fromisoformat(version_date)
        version_date_int = (vd - date(1970, 1, 1)).days
    else:
        version_date_int = version_date
    
    state_sql = """
    SELECT day_u16, aircraft_number, state
    FROM sim_masterv2
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND group_by IN (1, 2)
    ORDER BY day_u16, aircraft_number
    """
    
    try:
        state_data = client.execute(state_sql, {
            'version_date': version_date_int,
            'version_id': version_id
        })
        
        if state_data:
            # Применяем блокировку
            is_operations = np.zeros(MAX_DAYS * MAX_PLANERS, dtype=np.bool_)
            
            for row in state_data:
                day_idx, ac_num, state_str = row[0], row[1], row[2]
                if ac_num in ac_to_idx and day_idx < MAX_DAYS:
                    planer_idx = ac_to_idx[ac_num]
                    pos = day_idx * MAX_PLANERS + planer_idx
                    is_operations[pos] = (state_str == 'operations')
            
            blocked_count = 0
            for i in range(len(dt_array)):
                if not is_operations[i] and dt_array[i] > 0:
                    dt_array[i] = 0
                    blocked_count += 1
            
            if blocked_count > 0:
                remaining = np.count_nonzero(dt_array)
                print(f"   🚫 Заблокировано {blocked_count:,} записей (планер не в operations)")
                print(f"   ✅ Осталось {remaining:,} записей dt")
        else:
            print("   ⚠️ Нет данных sim_masterv2 для блокировки")
    except Exception as e:
        print(f"   ⚠️ Ошибка загрузки state: {e}")
    
    return dt_array, ac_to_idx


if __name__ == "__main__":
    # Тест
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--version-date", default="2025-07-04")
    args = parser.parse_args()
    
    dt_array, ac_to_idx = load_planer_dt(args.version_date)
    
    if dt_array is not None:
        print(f"\n✅ Загружено:")
        print(f"   Планеров: {len(ac_to_idx)}")
        print(f"   Размер массива: {len(dt_array)}")
        print(f"   Сумма dt: {np.sum(dt_array) / 60:.0f} часов")
        
        # Примеры
        print(f"\n   Примеры dt (первые 5 планеров, день 100):")
        for ac_num, idx in list(ac_to_idx.items())[:5]:
            pos = 100 * MAX_PLANERS + idx
            print(f"      AC {ac_num}: dt = {dt_array[pos]} мин")

