#!/usr/bin/env python3
"""
Сравнение Program_AC с размеченными ВС в heli_pandas
"""
import pandas as pd
import sys
sys.path.append('code')
from utils.config_loader import get_clickhouse_client

def compare_marked_aircraft():
    """Сравнивает ВС из Program_AC с размеченными в heli_pandas"""
    
    client = get_clickhouse_client()
    
    print('🔍 СРАВНЕНИЕ Program_AC vs РАЗМЕЧЕННЫЕ В heli_pandas')
    print('=' * 60)
    
    # 1. Program_AC - только Ми-8
    program_path = 'data_input/source_data/Program_AC.xlsx'
    program_df = pd.read_excel(program_path, header=0, engine='openpyxl')
    
    mi8_types = ['Ми-8Т', 'Ми-17']  # Обновлено для новых данных
    program_mi8 = program_df[program_df['ac_typ'].isin(mi8_types)]
    program_aircraft = set(str(ac) for ac in program_mi8['ac_registr'].unique())
    
    print(f'📊 Program_AC - Ми-8/Ми-17: {len(program_aircraft)} ВС')
    
    # 2. heli_pandas - ТОЛЬКО размеченные ВС из program_ac (статус 2 или 4)
    program_aircraft_list = "', '".join(program_aircraft)
    marked_query = f"""
    SELECT serialno, partno, status
    FROM heli_pandas 
    WHERE version_date = '2025-05-28'
      AND serialno IN ('{program_aircraft_list}')
      AND status IN (2, 4)
    ORDER BY serialno
    """
    
    marked_data = client.execute(marked_query)
    marked_aircraft = set(str(row[0]) for row in marked_data)
    
    print(f'📊 heli_pandas - размеченные (статус 2,4): {len(marked_aircraft)} ВС')
    
    # Группируем размеченные по статусам
    marked_by_status = {}
    for serialno, partno, status in marked_data:
        if status not in marked_by_status:
            marked_by_status[status] = []
        marked_by_status[status].append(str(serialno))
    
    status_names = {2: 'Эксплуатация', 4: 'Ремонт'}
    print('   Размеченные по статусам:')
    for status_id, aircraft_list in marked_by_status.items():
        status_name = status_names.get(status_id, f'{status_id}')
        print(f'     {status_id} - {status_name}: {len(aircraft_list)} ВС')
    
    # 3. Находим разности
    in_both = program_aircraft & marked_aircraft
    only_program = program_aircraft - marked_aircraft  
    only_marked = marked_aircraft - program_aircraft
    
    print(f'\n🔍 АНАЛИЗ РАЗНОСТЕЙ:')
    print(f'   В обеих таблицах: {len(in_both)} ВС')
    print(f'   Только в Program_AC: {len(only_program)} ВС ❌')
    print(f'   Только размечено в heli_pandas: {len(only_marked)} ВС ➕')
    
    # 4. Показываем потерянные ВС
    if only_program:
        print(f'\n❌ ПОТЕРЯННЫЕ ВС (есть в Program_AC, НЕТ разметки): {len(only_program)}')
        for ac in sorted(only_program):
            ac_info = program_mi8[program_mi8['ac_registr'] == int(ac)]
            if not ac_info.empty:
                ac_type = ac_info.iloc[0]['ac_typ']
                print(f'     {ac} (тип Program_AC: {ac_type})')
    
    # 5. Показываем лишние размеченные ВС
    if only_marked:
        print(f'\n➕ ЛИШНИЕ РАЗМЕЧЕННЫЕ ВС (размечены, но НЕТ в Program_AC): {len(only_marked)}')
        for ac in sorted(only_marked):
            ac_data = [row for row in marked_data if str(row[0]) == ac]
            if ac_data:
                serialno, partno, status = ac_data[0]
                status_name = status_names.get(status, f'{status}')
                print(f'     {ac} (partno: {partno}, статус: {status_name})')
    
    # 6. Математическая проверка
    print(f'\n🧮 БАЛАНС:')
    print(f'   Program_AC: {len(program_aircraft)} ВС')
    print(f'   Размеченные: {len(marked_aircraft)} ВС')
    print(f'   Разница: {len(program_aircraft) - len(marked_aircraft)} ВС')
    print(f'   Потеряно: {len(only_program)} ВС')
    print(f'   Лишних: {len(only_marked)} ВС')
    print(f'   Итоговый баланс: {len(only_program)} - {len(only_marked)} = {len(only_program) - len(only_marked)}')
    
    if len(only_program) - len(only_marked) == len(program_aircraft) - len(marked_aircraft):
        print('   ✅ Математика сходится!')
    else:
        print('   ❌ Математика НЕ сходится!')
    
    print('\n✅ Анализ завершен')

if __name__ == "__main__":
    compare_marked_aircraft() 