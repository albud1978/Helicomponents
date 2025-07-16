#!/usr/bin/env python3
"""
Детальный анализ значений поля status в heli_pandas
"""
import sys
sys.path.append('code')
from utils.config_loader import get_clickhouse_client

def analyze_status_values():
    """Анализирует распределение и детали значений поля status"""
    
    client = get_clickhouse_client()
    
    print('🔍 ДЕТАЛЬНЫЙ АНАЛИЗ ПОЛЯ STATUS В heli_pandas')
    print('=' * 60)
    
    # Словарь статусов для расшифровки
    status_names = {
        0: 'По умолчанию',
        1: 'Неактивно', 
        2: 'Эксплуатация',
        3: 'Исправен',
        4: 'Ремонт',
        5: 'Резерв',
        6: 'Хранение'
    }
    
    # 1. Общее распределение по статусам
    print('📊 ОБЩЕЕ РАСПРЕДЕЛЕНИЕ ПО СТАТУСАМ:')
    status_query = """
    SELECT status, COUNT(*) as count
    FROM heli_pandas 
    WHERE version_date = '2025-05-28'
    GROUP BY status
    ORDER BY status
    """
    
    status_stats = client.execute(status_query)
    total_records = sum(count for _, count in status_stats)
    
    for status_id, count in status_stats:
        status_name = status_names.get(status_id, f'Неизвестно({status_id})')
        percentage = (count / total_records) * 100
        print(f'   {status_id} - {status_name}: {count:,} записей ({percentage:.1f}%)')
    
    print(f'\nИтого: {total_records:,} записей')
    
    # 2. Детали компонентов со статусом "Эксплуатация" (2)
    print('\n🚁 КОМПОНЕНТЫ СО СТАТУСОМ "ЭКСПЛУАТАЦИЯ" (2):')
    exploitation_query = """
    SELECT partno, COUNT(*) as count
    FROM heli_pandas 
    WHERE version_date = '2025-05-28' AND status = 2
    GROUP BY partno
    ORDER BY count DESC
    LIMIT 10
    """
    
    exploitation_stats = client.execute(exploitation_query)
    
    if exploitation_stats:
        print('   ТОП-10 партномеров в эксплуатации:')
        for partno, count in exploitation_stats:
            print(f'     {partno}: {count} записей')
    else:
        print('   Нет компонентов в эксплуатации')
    
    # 3. Детали компонентов со статусом "Ремонт" (4)
    print('\n🔧 КОМПОНЕНТЫ СО СТАТУСОМ "РЕМОНТ" (4):')
    repair_query = """
    SELECT partno, serialno, ac_typ, location, removal_date, target_date
    FROM heli_pandas 
    WHERE version_date = '2025-05-28' AND status = 4
    ORDER BY partno, serialno
    LIMIT 20
    """
    
    repair_records = client.execute(repair_query)
    
    if repair_records:
        print('   Все компоненты в ремонте:')
        for partno, serialno, ac_typ, location, removal_date, target_date in repair_records:
            print(f'     {partno} | {serialno} | {ac_typ} | {location} | {removal_date} → {target_date}')
    else:
        print('   Нет компонентов в ремонте')
    
    # 4. Анализ планеров vs компонентов
    print('\n✈️ АНАЛИЗ ПЛАНЕРОВ VS КОМПОНЕНТОВ:')
    
    # Планеры (serialno = location и начинается с RA-)
    aircraft_query = """
    SELECT status, COUNT(*) as count
    FROM heli_pandas 
    WHERE version_date = '2025-05-28' 
      AND serialno = location 
      AND location LIKE 'RA-%'
    GROUP BY status
    ORDER BY status
    """
    
    aircraft_stats = client.execute(aircraft_query)
    total_aircraft = sum(count for _, count in aircraft_stats)
    
    print(f'   Планеры (всего {total_aircraft:,}):')
    for status_id, count in aircraft_stats:
        status_name = status_names.get(status_id, f'Неизвестно({status_id})')
        percentage = (count / total_aircraft) * 100 if total_aircraft > 0 else 0
        print(f'     {status_id} - {status_name}: {count:,} ({percentage:.1f}%)')
    
    # Компоненты (остальные)
    components_query = """
    SELECT status, COUNT(*) as count
    FROM heli_pandas 
    WHERE version_date = '2025-05-28' 
      AND NOT (serialno = location AND location LIKE 'RA-%')
    GROUP BY status
    ORDER BY status
    """
    
    components_stats = client.execute(components_query)
    total_components = sum(count for _, count in components_stats)
    
    print(f'\n   Компоненты (всего {total_components:,}):')
    for status_id, count in components_stats:
        status_name = status_names.get(status_id, f'Неизвестно({status_id})')
        percentage = (count / total_components) * 100 if total_components > 0 else 0
        print(f'     {status_id} - {status_name}: {count:,} ({percentage:.1f}%)')
    
    # 5. Проверка источников статусов
    print('\n📋 ИСТОЧНИКИ СТАТУСОВ:')
    print('   0 - По умолчанию: исходное значение, статус не определен')
    print('   2 - Эксплуатация: установлено через program_ac (активные программы)')
    print('   4 - Ремонт: установлено через status_overhaul (капремонт ВС)')
    
    print('\n✅ Детальный анализ статусов завершен')

if __name__ == "__main__":
    analyze_status_values() 