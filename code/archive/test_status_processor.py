#!/usr/bin/env python3
"""
Тест исправленной логики status_processor.py
"""

import pandas as pd
import sys
from pathlib import Path

sys.path.append('code')
from utils.config_loader import get_clickhouse_client
from status_processor import process_aircraft_status

def test_status_processor():
    """Тестирует исправленную логику status_processor.py на реальных данных"""
    
    client = get_clickhouse_client()
    
    print('🧪 === ТЕСТ STATUS_PROCESSOR ===')
    
    # Получаем небольшую выборку данных с планерами в капремонте
    test_serialnos = ['22215', '22481', '22486', '22489', '22494']  # ВС в ремонте
    serialno_list = "', '".join(test_serialnos)
    
    query = f'''
    SELECT 
        serialno,
        partno,
        location,
        status,
        removal_date,
        target_date
    FROM heli_pandas 
    WHERE serialno IN ('{serialno_list}')
      AND version_date = '2025-06-23'
    ORDER BY serialno
    '''
    
    result = client.execute(query)
    if not result:
        print("❌ Не найдены тестовые данные")
        return
    
    # Создаем тестовый DataFrame
    test_df = pd.DataFrame(result, columns=[
        'serialno', 'partno', 'location', 'status', 'removal_date', 'target_date'
    ])
    
    print(f"📊 Тестовые данные ({len(test_df)} записей):")
    for _, row in test_df.iterrows():
        print(f"   serialno={row['serialno']}, location={row['location']}, current_status={row['status']}")
    
    # Проверяем новую логику поиска планеров
    def is_aircraft_match(row):
        if pd.isna(row['location']) or not row['location'].startswith('RA-'):
            return False
        try:
            location_number = row['location'].replace('RA-', '')
            return str(row['serialno']) == location_number
        except:
            return False
    
    aircraft_mask = test_df.apply(is_aircraft_match, axis=1)
    found_aircraft = test_df[aircraft_mask]
    
    print(f"\n🔍 Новая логика поиска планеров:")
    print(f"   Найдено планеров: {len(found_aircraft)}")
    for _, row in found_aircraft.iterrows():
        print(f"   ✅ Планер: serialno={row['serialno']}, location={row['location']}")
    
    # Тестируем процессор статусов
    print(f"\n🔧 Тестируем process_aircraft_status...")
    processed_df = process_aircraft_status(test_df.copy(), client)
    
    # Сравниваем результаты
    print(f"\n📊 Результаты обработки:")
    for idx, row in processed_df.iterrows():
        original_status = test_df.iloc[idx]['status']
        new_status = row['status']
        status_changed = "✅ ИЗМЕНЕН" if new_status != original_status else "⏭️ Без изменений"
        print(f"   serialno={row['serialno']}: {original_status} → {new_status} ({status_changed})")
        
        if row['removal_date'] is not None:
            print(f"      removal_date = {row['removal_date']}")
        if row['target_date'] is not None:
            print(f"      target_date = {row['target_date']}")
    
    print(f"\n✅ Тест завершен!")

if __name__ == "__main__":
    test_status_processor() 