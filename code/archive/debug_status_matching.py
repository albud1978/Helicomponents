#!/usr/bin/env python3
"""
Отладка сопоставления данных между status_overhaul и heli_pandas
Проверяем почему 7 ВС в капремонте не получают статус 4 (Ремонт)
"""

import pandas as pd
import sys
from pathlib import Path

sys.path.append('code')
from utils.config_loader import get_clickhouse_client

def debug_status_matching():
    """Отладка сопоставления данных статусов"""
    
    client = get_clickhouse_client()
    
    print('🔍 === ОТЛАДКА СОПОСТАВЛЕНИЯ СТАТУСОВ ===')
    
    # 1. Проверяем данные status_overhaul
    print('\n📋 1. Данные status_overhaul (status != "Закрыто"):')
    
    overhaul_query = """
    SELECT 
        ac_registr,
        status,
        act_start_date,
        sched_end_date,
        act_end_date
    FROM status_overhaul 
    WHERE status != 'Закрыто'
    ORDER BY ac_registr
    """
    
    overhaul_result = client.execute(overhaul_query)
    if overhaul_result:
        overhaul_df = pd.DataFrame(overhaul_result, columns=['ac_registr', 'status', 'act_start_date', 'sched_end_date', 'act_end_date'])
        print(f"✅ Найдено {len(overhaul_df)} ВС в капремонте:")
        for _, row in overhaul_df.iterrows():
            print(f"   RA-{row['ac_registr']}: {row['status']}")
    else:
        print("❌ Нет данных в status_overhaul с status != 'Закрыто'")
        return
    
    # 2. Проверяем планеры в heli_pandas
    print('\n🚁 2. Потенциальные планеры в heli_pandas:')
    
    # Ищем записи где location начинается с RA-
    pandas_query = """
    SELECT 
        serialno,
        location,
        partno,
        ac_typ,
        status,
        aircraft_number
    FROM heli_pandas 
    WHERE location LIKE 'RA-%'
    ORDER BY location, serialno
    """
    
    pandas_result = client.execute(pandas_query)
    if pandas_result:
        pandas_df = pd.DataFrame(pandas_result, columns=['serialno', 'location', 'partno', 'ac_typ', 'status', 'aircraft_number'])
        print(f"✅ Найдено {len(pandas_df)} записей с location = 'RA-...':")
        
        # Группируем по location для анализа
        location_groups = pandas_df.groupby('location').size().sort_values(ascending=False)
        print(f"📊 Распределение по location:")
        for location, count in location_groups.head(10).items():
            print(f"   {location}: {count} записей")
    else:
        print("❌ Нет записей с location = 'RA-...' в heli_pandas")
        return
    
    # 3. Проверяем сопоставление планеров
    print('\n🔍 3. Анализ сопоставления планеров:')
    
    # Находим планеры (где serialno совпадает с номером из location)
    def is_aircraft_match(row):
        if pd.isna(row['location']) or not row['location'].startswith('RA-'):
            return False
        try:
            location_number = row['location'].replace('RA-', '')
            return str(row['serialno']) == location_number
        except:
            return False
    
    pandas_df['is_aircraft'] = pandas_df.apply(is_aircraft_match, axis=1)
    aircraft_df = pandas_df[pandas_df['is_aircraft']].copy()
    
    print(f"✅ Найдено {len(aircraft_df)} планеров (serialno = номер из location):")
    for _, row in aircraft_df.iterrows():
        print(f"   serialno={row['serialno']}, location={row['location']}, статус={row['status']}")
    
    # 4. Проверяем пересечение с данными капремонта
    print('\n🔄 4. Проверка пересечения с данными капремонта:')
    
    # Создаем множество номеров ВС из капремонта
    overhaul_numbers = set(str(row['ac_registr']).zfill(5) for _, row in overhaul_df.iterrows())
    print(f"📋 Номера ВС в капремонте: {sorted(overhaul_numbers)}")
    
    # Проверяем каждый планер
    matches_found = 0
    for _, row in aircraft_df.iterrows():
        serialno = str(row['serialno']).zfill(5)
        location = row['location']
        current_status = row['status']
        
        if serialno in overhaul_numbers:
            matches_found += 1
            status_info = overhaul_df[overhaul_df['ac_registr'].astype(str).str.zfill(5) == serialno].iloc[0]
            print(f"✅ СОВПАДЕНИЕ: {location} (serialno={serialno}) - капремонт: {status_info['status']}, текущий статус: {current_status}")
        else:
            print(f"❌ НЕ НАЙДЕН: {location} (serialno={serialno}) - НЕ в капремонте, текущий статус: {current_status}")
    
    print(f"\n📊 Итоговая статистика:")
    print(f"   ВС в капремонте: {len(overhaul_df)}")
    print(f"   Планеров в heli_pandas: {len(aircraft_df)}")
    print(f"   Совпадений найдено: {matches_found}")
    
    # 5. Проверяем статусы планеров
    print('\n📊 5. Статусы планеров в heli_pandas:')
    if len(aircraft_df) > 0:
        status_counts = aircraft_df['status'].value_counts().sort_index()
        status_names = {0: 'По умолчанию', 1: 'Неактивно', 2: 'Эксплуатация', 3: 'Исправен', 4: 'Ремонт', 5: 'Хранение'}
        
        for status_id, count in status_counts.items():
            status_name = status_names.get(status_id, f'Неизвестно({status_id})')
            print(f"   {status_id} - {status_name}: {count} планеров")
    
    # 6. Диагностика проблемы
    print('\n🔍 6. Диагностика проблемы:')
    
    if matches_found == 0:
        print("❌ ПРОБЛЕМА: Нет совпадений между капремонтом и планерами!")
        print("💡 Возможные причины:")
        print("   1. Неправильное сопоставление номеров")
        print("   2. Планеры не загружены в heli_pandas") 
        print("   3. Ошибка в логике определения планеров")
    elif matches_found < len(overhaul_df):
        print(f"⚠️ ЧАСТИЧНАЯ ПРОБЛЕМА: Найдено {matches_found} из {len(overhaul_df)} ожидаемых совпадений")
    else:
        print(f"✅ ВСЕ СОВПАДЕНИЯ НАЙДЕНЫ: {matches_found} из {len(overhaul_df)}")
        
        # Проверяем почему статус не 4
        status_4_count = aircraft_df['status'].value_counts().get(4, 0)
        if status_4_count == 0:
            print("❌ ПРОБЛЕМА: Ни один планер не имеет статус 4 (Ремонт)")
            print("💡 Скорее всего status_processor.py не выполнился или не сработал")
        else:
            print(f"✅ Статус 4 (Ремонт) установлен у {status_4_count} планеров")

if __name__ == "__main__":
    debug_status_matching() 