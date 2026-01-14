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
    
    # ВАЖНО: НЕ блокируем dt по состоянию планера!
    # dt уже содержит 0 для планеров не в operations (симуляция планеров устанавливает dt=0 для non-ops)
    # Агрегат на планере будет получать dt когда планер летит
    
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


def load_assembly_trigger_from_sim(version_date: str, version_id: int = 1, ac_to_idx: Dict[int, int] = None) -> Optional[np.ndarray]:
    """
    Загружает assembly_trigger из sim_masterv2 (результаты симуляции планеров)
    
    assembly_trigger=1 означает, что планер находится в последней стадии ремонта
    и нужно начинать комплектацию агрегатами.
    
    Returns:
        assembly_array: np.ndarray shape (MAX_DAYS * MAX_PLANERS,) — 0 или 1
    """
    if ac_to_idx is None or len(ac_to_idx) == 0:
        print("   ⚠️ ac_to_idx пуст, невозможно загрузить assembly_trigger")
        return None
    
    client = get_clickhouse_client()
    
    from datetime import date
    if isinstance(version_date, str):
        vd = date.fromisoformat(version_date)
        version_date_int = (vd - date(1970, 1, 1)).days
    else:
        version_date_int = version_date
    
    assembly_sql = """
    SELECT day_u16 as day_index, aircraft_number, assembly_trigger
    FROM sim_masterv2
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND group_by IN (1, 2)
      AND assembly_trigger = 1
    ORDER BY day_u16, aircraft_number
    """
    
    assembly_data = client.execute(assembly_sql, {
        'version_date': version_date_int,
        'version_id': version_id
    })
    
    # Инициализируем массив нулями
    assembly_array = np.zeros(MAX_DAYS * MAX_PLANERS, dtype=np.uint8)
    
    # Заполняем
    for row in assembly_data:
        day_idx, ac_num, trigger = row[0], row[1], row[2]
        if ac_num in ac_to_idx and day_idx < MAX_DAYS:
            planer_idx = ac_to_idx[ac_num]
            pos = day_idx * MAX_PLANERS + planer_idx
            assembly_array[pos] = 1
    
    trigger_count = np.sum(assembly_array)
    print(f"   ✅ Загружено {len(assembly_data)} записей assembly_trigger=1 (всего: {trigger_count})")
    
    return assembly_array


def load_planer_in_ops_from_sim(version_date: str, version_id: int = 1, ac_to_idx: Dict[int, int] = None) -> Dict[int, int]:
    """
    Загружает флаги плане в operations из sim_masterv2 (начальное состояние день 0)
    
    Используется для assembly — агрегаты назначаются только на планеры в operations.
    
    Returns:
        planer_in_ops: Dict[planer_idx → 1] для планеров в operations на день 0
    """
    if ac_to_idx is None or len(ac_to_idx) == 0:
        print("   ⚠️ ac_to_idx пуст, невозможно загрузить planer_in_ops")
        return {}
    
    client = get_clickhouse_client()
    
    from datetime import date
    if isinstance(version_date, str):
        vd = date.fromisoformat(version_date)
        version_date_int = (vd - date(1970, 1, 1)).days
    else:
        version_date_int = version_date
    
    # Загружаем состояние планеров на день 0 (начальное состояние)
    ops_sql = """
    SELECT aircraft_number
    FROM sim_masterv2
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND day_u16 = 0
      AND group_by IN (1, 2)
      AND state = 'operations'
    """
    
    ops_data = client.execute(ops_sql, {
        'version_date': version_date_int,
        'version_id': version_id
    })
    
    planer_in_ops = {}
    for row in ops_data:
        ac_num = row[0]
        if ac_num in ac_to_idx:
            planer_idx = ac_to_idx[ac_num]
            planer_in_ops[planer_idx] = 1
    
    print(f"   ✅ Загружено {len(planer_in_ops)} планеров в operations (день 0)")
    
    return planer_in_ops


def load_planer_in_ops_history_from_sim(version_date: str, version_id: int = 1, ac_to_idx: Dict[int, int] = None) -> Optional[np.ndarray]:
    """
    Загружает ПОЛНУЮ ИСТОРИЮ состояний планеров из sim_masterv2.
    
    Формат: planer_in_ops_array[day * MAX_PLANERS + planer_idx] = 1 если в operations, 0 иначе
    
    Используется для детекции выхода планера из operations:
    - Когда планер уходит из operations — агрегаты на нём отцепляются и идут в serviceable
    
    Returns:
        planer_in_ops_array: np.ndarray shape (MAX_DAYS * MAX_PLANERS,) — 0 или 1
    """
    if ac_to_idx is None or len(ac_to_idx) == 0:
        print("   ⚠️ ac_to_idx пуст, невозможно загрузить planer_in_ops_history")
        return None
    
    client = get_clickhouse_client()
    
    from datetime import date
    if isinstance(version_date, str):
        vd = date.fromisoformat(version_date)
        version_date_int = (vd - date(1970, 1, 1)).days
    else:
        version_date_int = version_date
    
    # Загружаем ВСЕ состояния планеров (когда они в operations)
    ops_sql = """
    SELECT day_u16, aircraft_number
    FROM sim_masterv2
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND group_by IN (1, 2)
      AND state = 'operations'
    ORDER BY day_u16, aircraft_number
    """
    
    ops_data = client.execute(ops_sql, {
        'version_date': version_date_int,
        'version_id': version_id
    })
    
    # Инициализируем массив нулями
    planer_in_ops_array = np.zeros(MAX_DAYS * MAX_PLANERS, dtype=np.uint8)
    
    # Заполняем
    for row in ops_data:
        day_idx, ac_num = row[0], row[1]
        if ac_num in ac_to_idx and day_idx < MAX_DAYS:
            planer_idx = ac_to_idx[ac_num]
            pos = day_idx * MAX_PLANERS + planer_idx
            planer_in_ops_array[pos] = 1
    
    ops_count = np.sum(planer_in_ops_array)
    print(f"   ✅ Загружено {len(ops_data)} записей planer_in_ops_history (всего: {ops_count})")
    
    return planer_in_ops_array


def load_planer_type_from_sim(version_date: str, version_id: int = 1, ac_to_idx: Dict[int, int] = None) -> Dict[int, int]:
    """
    Загружает тип планера (Mi-8=1, Mi-17=2) из sim_masterv2
    
    Используется для проверки соответствия типа двигателя типу планера:
    - group_by=3 (ТВ2-117) → только для Mi-8 (planer_type=1)
    - group_by=4 (ТВ3-117) → только для Mi-17 (planer_type=2)
    
    Returns:
        planer_type: Dict[planer_idx → planer_type (1=Mi-8, 2=Mi-17)]
    """
    if ac_to_idx is None or len(ac_to_idx) == 0:
        print("   ⚠️ ac_to_idx пуст, невозможно загрузить planer_type")
        return {}
    
    client = get_clickhouse_client()
    
    from datetime import date
    if isinstance(version_date, str):
        vd = date.fromisoformat(version_date)
        version_date_int = (vd - date(1970, 1, 1)).days
    else:
        version_date_int = version_date
    
    # Загружаем тип планера (group_by: 1=Mi-8, 2=Mi-17)
    type_sql = """
    SELECT DISTINCT aircraft_number, group_by
    FROM sim_masterv2
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND group_by IN (1, 2)
    """
    
    type_data = client.execute(type_sql, {
        'version_date': version_date_int,
        'version_id': version_id
    })
    
    planer_type = {}
    mi8_count = 0
    mi17_count = 0
    
    for row in type_data:
        ac_num, group_by = row[0], row[1]
        if ac_num in ac_to_idx:
            planer_idx = ac_to_idx[ac_num]
            planer_type[planer_idx] = group_by  # 1=Mi-8, 2=Mi-17
            if group_by == 1:
                mi8_count += 1
            else:
                mi17_count += 1
    
    print(f"   ✅ Загружено типов планеров: Mi-8={mi8_count}, Mi-17={mi17_count}")
    
    return planer_type


def load_planer_data(version_date: str, version_id: int = 1) -> Tuple[Optional[np.ndarray], Optional[np.ndarray], Dict[int, int], Dict[int, int], Dict[int, int], Optional[np.ndarray]]:
    """
    Полная загрузка данных планеров для симуляции агрегатов
    
    Returns:
        dt_array: массив dt[day * MAX_PLANERS + planer_idx]
        assembly_array: массив assembly_trigger[day * MAX_PLANERS + planer_idx] (0 или 1)
        ac_to_idx: маппинг aircraft_number → planer_idx
        planer_in_ops: Dict[planer_idx → 1] для планеров в operations (день 0)
        planer_type: Dict[planer_idx → type (1=Mi-8, 2=Mi-17)]
        planer_in_ops_history: массив [day * MAX_PLANERS + planer_idx] = 0/1 (история по всем дням)
    """
    print("📊 Загрузка данных планеров...")
    
    # Загружаем dt
    dt_array, ac_to_idx = load_planer_dt(version_date, version_id)
    
    if dt_array is None:
        return None, None, {}, {}, {}, None
    
    # Загружаем assembly_trigger
    assembly_array = load_assembly_trigger_from_sim(version_date, version_id, ac_to_idx)
    
    # Загружаем planer_in_ops (для assembly — только день 0)
    planer_in_ops = load_planer_in_ops_from_sim(version_date, version_id, ac_to_idx)
    
    # Загружаем тип планера (для проверки соответствия типов двигателей)
    planer_type = load_planer_type_from_sim(version_date, version_id, ac_to_idx)
    
    # NEW: Загружаем ПОЛНУЮ историю состояний планеров (для детекции выхода из operations)
    planer_in_ops_history = load_planer_in_ops_history_from_sim(version_date, version_id, ac_to_idx)
    
    return dt_array, assembly_array, ac_to_idx, planer_in_ops, planer_type, planer_in_ops_history


if __name__ == "__main__":
    # Тест
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--version-date", default="2025-07-04")
    args = parser.parse_args()
    
    dt_array, assembly_array, ac_to_idx, planer_in_ops = load_planer_data(args.version_date)
    
    if dt_array is not None:
        print(f"\n✅ Загружено:")
        print(f"   Планеров: {len(ac_to_idx)}")
        print(f"   Размер массива dt: {len(dt_array)}")
        print(f"   Сумма dt: {np.sum(dt_array) / 60:.0f} часов")
        
        if assembly_array is not None:
            print(f"   assembly_trigger=1: {np.sum(assembly_array)}")
        
        print(f"   planer_in_ops: {len(planer_in_ops)}")
        
        # Примеры
        print(f"\n   Примеры dt (первые 5 планеров, день 100):")
        for ac_num, idx in list(ac_to_idx.items())[:5]:
            pos = 100 * MAX_PLANERS + idx
            print(f"      AC {ac_num}: dt = {dt_array[pos]} мин")

