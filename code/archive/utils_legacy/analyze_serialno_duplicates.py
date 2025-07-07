#!/usr/bin/env python3
"""
Полный анализ дублей serialno в heli_pandas и heli_raw
Выгружает ВСЕ записи с анализом дублей в Excel раздельно по таблицам
"""

import pandas as pd
import sys
from pathlib import Path

sys.path.append('code')
from utils.config_loader import get_clickhouse_client

def full_serialno_analysis():
    """Полный анализ дублей serialno в heli_pandas и heli_raw с выгрузкой в Excel"""
    
    client = get_clickhouse_client()
    output_dir = Path('data_output')
    output_dir.mkdir(exist_ok=True)
    
    print('🔍 ПОЛНЫЙ АНАЛИЗ ДУБЛЕЙ SERIALNO')
    print('=' * 60)
    
    # ===== АНАЛИЗ heli_pandas =====
    print('\n📊 АНАЛИЗ heli_pandas (планеры):')
    
    # Загружаем все данные heli_pandas
    query_pandas = '''
    SELECT 
        serialno,
        partno,
        ac_typ,
        location,
        mfg_date,
        removal_date,
        target_date,
        condition,
        owner,
        status,
        aircraft_number
    FROM heli_pandas 
    ORDER BY serialno, partno
    '''
    
    print('📥 Загружаем все данные heli_pandas...')
    pandas_data = client.execute(query_pandas)
    pandas_df = pd.DataFrame(pandas_data, columns=[
        'serialno', 'partno', 'ac_typ', 'location', 'mfg_date', 
        'removal_date', 'target_date', 'condition', 'owner', 'status', 'aircraft_number'
    ])
    
    print(f'📊 Всего записей heli_pandas: {len(pandas_df):,}')
    
    # Анализ дублей в heli_pandas
    print('🔄 Анализируем дубли serialno в heli_pandas...')
    pandas_counts = pandas_df['serialno'].value_counts().reset_index()
    pandas_counts.columns = ['serialno', 'duplicate_count']
    
    # Объединяем с исходными данными
    pandas_analysis = pandas_df.merge(pandas_counts, on='serialno', how='left')
    pandas_analysis['table_source'] = 'heli_pandas'
    
    # Сортируем по количеству дублей (убывание) и serialno
    pandas_analysis = pandas_analysis.sort_values(['duplicate_count', 'serialno'], ascending=[False, True])
    
    # Выгружаем полный анализ heli_pandas
    pandas_file = output_dir / 'heli_pandas_serialno_duplicates_full.xlsx'
    print(f'💾 Сохраняем полный анализ heli_pandas...')
    pandas_analysis.to_excel(pandas_file, index=False)
    print(f'📁 Полный анализ heli_pandas сохранен: {pandas_file}')
    
    # Статистика дублей heli_pandas
    pandas_duplicates = pandas_counts[pandas_counts['duplicate_count'] > 1]
    if len(pandas_duplicates) > 0:
        print(f'❌ Найдено {len(pandas_duplicates)} дублированных serialno в heli_pandas')
        print(f'📊 ТОП-5 дублей heli_pandas:')
        for _, row in pandas_duplicates.head(5).iterrows():
            print(f'   serialno "{row["serialno"]}": {row["duplicate_count"]} записей')
    else:
        print('✅ Дублей serialno в heli_pandas НЕ НАЙДЕНО')
    
    # ===== АНАЛИЗ heli_raw =====
    print('\n📊 АНАЛИЗ heli_raw (все компоненты):')
    
    # Сначала получаем статистику
    print('📈 Получаем статистику heli_raw...')
    stats_query = '''
    SELECT 
        COUNT(*) as total,
        COUNT(DISTINCT serialno) as unique_serialno
    FROM heli_raw 
    WHERE serialno IS NOT NULL 
      AND serialno != ''
    '''
    
    total_raw, unique_raw = client.execute(stats_query)[0]
    dups_raw = total_raw - unique_raw
    print(f'📈 Статистика heli_raw: {total_raw:,} записей, {unique_raw:,} уникальных serialno, {dups_raw:,} дублей')
    
    # Загружаем ВСЕ данные heli_raw (может быть большой файл!)
    print('📥 Загружаем ВСЕ данные heli_raw... (может занять время)')
    query_raw = '''
    SELECT 
        serialno,
        partno,
        ac_typ,
        location,
        mfg_date,
        removal_date,
        target_date,
        condition,
        owner
    FROM heli_raw 
    WHERE serialno IS NOT NULL 
      AND serialno != ''
    ORDER BY serialno, partno
    '''
    
    raw_data = client.execute(query_raw)
    raw_df = pd.DataFrame(raw_data, columns=[
        'serialno', 'partno', 'ac_typ', 'location', 'mfg_date', 
        'removal_date', 'target_date', 'condition', 'owner'
    ])
    
    print(f'📊 Загружено записей heli_raw: {len(raw_df):,}')
    
    # Анализ дублей в heli_raw
    print('🔄 Анализируем дубли serialno в heli_raw...')
    raw_counts = raw_df['serialno'].value_counts().reset_index()
    raw_counts.columns = ['serialno', 'duplicate_count']
    
    # Объединяем с исходными данными
    print('🔗 Объединяем данные с анализом дублей...')
    raw_analysis = raw_df.merge(raw_counts, on='serialno', how='left')
    raw_analysis['table_source'] = 'heli_raw'
    
    # Сортируем по количеству дублей (убывание) и serialno
    raw_analysis = raw_analysis.sort_values(['duplicate_count', 'serialno'], ascending=[False, True])
    
    # Выгружаем полный анализ heli_raw
    raw_file = output_dir / 'heli_raw_serialno_duplicates_full.xlsx'
    print(f'💾 Сохраняем полный анализ heli_raw... (может занять время)')
    raw_analysis.to_excel(raw_file, index=False)
    print(f'📁 Полный анализ heli_raw сохранен: {raw_file}')
    
    # Статистика дублей heli_raw
    raw_duplicates = raw_counts[raw_counts['duplicate_count'] > 1]
    print(f'❌ Найдено {len(raw_duplicates):,} дублированных serialno в heli_raw')
    print(f'📊 ТОП-10 дублей heli_raw:')
    for _, row in raw_duplicates.head(10).iterrows():
        print(f'   serialno "{row["serialno"]}": {row["duplicate_count"]} записей')
    
    # ===== СВОДНАЯ СТАТИСТИКА =====
    print('\n' + '=' * 60)
    print('📊 СВОДНАЯ СТАТИСТИКА:')
    print(f'   heli_pandas: {len(pandas_df):,} записей, {len(pandas_duplicates)} дублированных serialno')
    print(f'   heli_raw: {len(raw_df):,} записей, {len(raw_duplicates):,} дублированных serialno')
    
    print(f'\n📁 Файлы полного анализа сохранены:')
    print(f'   {pandas_file}')
    print(f'   {raw_file}')
    
    print('\n✅ Полный анализ дублей serialno завершен')

if __name__ == "__main__":
    full_serialno_analysis() 