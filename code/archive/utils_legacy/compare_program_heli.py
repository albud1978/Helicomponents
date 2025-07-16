#!/usr/bin/env python3
"""
Правильное сравнение Program_AC.xlsx и heli_pandas
"""
import pandas as pd
import sys
sys.path.append('code')
from utils.config_loader import get_clickhouse_client

def compare_program_heli():
    """Сравнивает ВС из Program_AC с планерами в heli_pandas"""
    
    client = get_clickhouse_client()
    
    print('🔍 ПРАВИЛЬНОЕ СРАВНЕНИЕ Program_AC vs heli_pandas')
    print('=' * 60)
    
    # 1. Загружаем Program_AC.xlsx - только Ми-8
    program_path = 'data_input/source_data/Program_AC.xlsx'
    program_df = pd.read_excel(program_path, header=0, engine='openpyxl')
    
    # Фильтруем только Ми-8/Ми-17 (новые форматы)
    mi8_types = ['Ми-8Т', 'Ми-17']  # Обновлено для новых данных
    program_mi8 = program_df[program_df['ac_typ'].isin(mi8_types)].copy()
    
    # Получаем ТОЧНЫЕ номера ВС из Program_AC
    program_aircraft = set(str(ac) for ac in program_mi8['ac_registr'].unique())
    
    print(f'📊 Program_AC.xlsx - Ми-8/Ми-17 всего: {len(program_aircraft)} ВС')
    
    # Группируем по типам в Program_AC
    program_by_type = program_mi8.groupby('ac_typ')['ac_registr'].nunique()
    print('   По типам в Program_AC:')
    for ac_type, count in program_by_type.items():
        print(f'     {ac_type}: {count} ВС')
    
    # 2. Проверяем какие ВС из program_ac есть в heli_pandas (ищем по serialno)
    program_aircraft_list = "', '".join(program_aircraft)
    heli_query = f"""
    SELECT DISTINCT serialno, partno, status
    FROM heli_pandas 
    WHERE version_date = '2025-05-28'
      AND serialno IN ('{program_aircraft_list}')
    ORDER BY serialno
    """
    
    heli_data = client.execute(heli_query)
    heli_aircraft = set(str(row[0]) for row in heli_data)
    
    print(f'\n📊 heli_pandas - планеры МИ-*: {len(heli_aircraft)} ВС')
    
    # Группируем по партномерам и статусам
    heli_by_partno = {}
    heli_by_status = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    
    for serialno, partno, status in heli_data:
        # По партномерам
        if partno not in heli_by_partno:
            heli_by_partno[partno] = 0
        heli_by_partno[partno] += 1
        
        # По статусам
        heli_by_status[status] += 1
    
    print('   По партномерам в heli_pandas:')
    for partno, count in heli_by_partno.items():
        print(f'     {partno}: {count} ВС')
    
    status_names = {0: 'По умолчанию', 1: 'Неактивно', 2: 'Эксплуатация', 
                   3: 'Исправен', 4: 'Ремонт', 5: 'Резерв', 6: 'Хранение'}
    print('   По статусам в heli_pandas:')
    for status_id, count in heli_by_status.items():
        if count > 0:
            status_name = status_names.get(status_id, f'Неизвестно({status_id})')
            print(f'     {status_id} - {status_name}: {count} ВС')
    
    # 3. Анализ пересечений
    in_both = program_aircraft & heli_aircraft
    only_program = program_aircraft - heli_aircraft  
    only_heli = heli_aircraft - program_aircraft
    
    print(f'\n🔍 АНАЛИЗ ПЕРЕСЕЧЕНИЙ:')
    print(f'   В обеих таблицах: {len(in_both)} ВС')
    print(f'   Только в Program_AC: {len(only_program)} ВС ❌')
    print(f'   Только в heli_pandas: {len(only_heli)} ВС ➕')
    
    # 4. Детали расхождений
    if only_program:
        print(f'\n❌ ТОЛЬКО В Program_AC ({len(only_program)} ВС):')
        for ac in sorted(only_program):
            # Находим тип в Program_AC
            ac_info = program_mi8[program_mi8['ac_registr'] == int(ac)]
            if not ac_info.empty:
                ac_type = ac_info.iloc[0]['ac_typ']
                print(f'     {ac} (тип: {ac_type})')
            else:
                print(f'     {ac} (тип: неизвестен)')
    
    if only_heli:
        print(f'\n➕ ТОЛЬКО В heli_pandas ({len(only_heli)} ВС):')
        for ac in sorted(only_heli):
            # Находим данные в heli_pandas
            ac_data = [row for row in heli_data if str(row[0]) == ac]
            if ac_data:
                serialno, partno, status = ac_data[0]
                status_name = status_names.get(status, f'{status}')
                print(f'     {ac} (partno: {partno}, status: {status_name})')
            else:
                print(f'     {ac} (данные не найдены)')
    
    # 5. Проверка математики
    print(f'\n🧮 ПРОВЕРКА МАТЕМАТИКИ:')
    print(f'   Program_AC: {len(program_aircraft)} ВС')
    print(f'   heli_pandas: {len(heli_aircraft)} ВС')
    print(f'   Разница: {len(program_aircraft) - len(heli_aircraft)} ВС')
    print(f'   Потеряно: {len(only_program)} ВС')
    print(f'   Лишних: {len(only_heli)} ВС')
    print(f'   Баланс: {len(only_program)} - {len(only_heli)} = {len(only_program) - len(only_heli)}')
    
    if len(only_program) - len(only_heli) == len(program_aircraft) - len(heli_aircraft):
        print('   ✅ Математика сходится!')
    else:
        print('   ❌ Математика НЕ сходится!')
    
    print('\n✅ Сравнение завершено')

if __name__ == "__main__":
    compare_program_heli() 