#!/usr/bin/env python3
"""
Проверка дублей serialno в heli_pandas
Анализирует ПОЛНЫЕ совпадения serialno без частичных совпадений
"""

import pandas as pd
import sys
from pathlib import Path

sys.path.append('code')
from utils.config_loader import get_clickhouse_client

def check_heli_pandas_duplicates():
    """Проверяет дубли serialno в heli_pandas с выгрузкой в Excel"""
    
    client = get_clickhouse_client()
    output_dir = Path('data_output')
    output_dir.mkdir(exist_ok=True)
    
    print('🔍 === ПРОВЕРКА ДУБЛЕЙ SERIALNO В HELI_PANDAS ===')
    
    # Загружаем все данные heli_pandas
    print('📥 Загружаем все данные heli_pandas...')
    query = '''
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
        aircraft_number,
        version_date
    FROM heli_pandas 
    ORDER BY serialno, partno
    '''
    
    result = client.execute(query)
    if not result:
        print("❌ Нет данных в heli_pandas")
        return
    
    df = pd.DataFrame(result, columns=[
        'serialno', 'partno', 'ac_typ', 'location', 'mfg_date', 
        'removal_date', 'target_date', 'condition', 'owner', 
        'status', 'aircraft_number', 'version_date'
    ])
    
    print(f'📊 Всего записей heli_pandas: {len(df):,}')
    
    # Анализ дублей serialno - ПОЛНЫЕ ТОЧНЫЕ СОВПАДЕНИЯ
    print('🔄 Анализируем дубли serialno (полные совпадения)...')
    
    # Подсчитываем количество каждого serialno
    serialno_counts = df['serialno'].value_counts().reset_index()
    serialno_counts.columns = ['serialno', 'duplicate_count']
    
    # Находим дубли (где count > 1)
    duplicates = serialno_counts[serialno_counts['duplicate_count'] > 1]
    
    print(f'📈 Статистика serialno:')
    print(f'   Всего уникальных serialno: {len(serialno_counts):,}')
    print(f'   Дублированных serialno: {len(duplicates):,}')
    
    if len(duplicates) == 0:
        print('✅ ДУБЛЕЙ SERIALNO НЕ НАЙДЕНО!')
        print('✅ Качество данных: ОТЛИЧНОЕ')
        return
    
    # Анализируем дубли
    print(f'\n❌ НАЙДЕНЫ ДУБЛИ SERIALNO!')
    print(f'📊 ТОП-10 дублированных serialno:')
    
    for i, (_, row) in enumerate(duplicates.head(10).iterrows()):
        serialno = row['serialno']
        count = row['duplicate_count']
        print(f'   {i+1}. serialno "{serialno}": {count} записей')
    
    # Создаем детальный анализ дублей
    print(f'\n📋 Создаем детальный анализ дублей...')
    
    # Объединяем исходные данные с информацией о дублях
    df_with_duplicates = df.merge(serialno_counts, on='serialno', how='left')
    
    # Фильтруем только дубли
    duplicated_records = df_with_duplicates[df_with_duplicates['duplicate_count'] > 1].copy()
    
    # Сортируем по количеству дублей (убывание) и serialno
    duplicated_records = duplicated_records.sort_values(['duplicate_count', 'serialno'], ascending=[False, True])
    
    # Добавляем служебные поля для анализа
    duplicated_records['table_source'] = 'heli_pandas'
    
    # Выгружаем в Excel
    output_file = output_dir / 'heli_pandas_serialno_duplicates_analysis.xlsx'
    print(f'💾 Сохраняем детальный анализ в Excel...')
    duplicated_records.to_excel(output_file, index=False)
    print(f'📁 Анализ дублей сохранен: {output_file}')
    
    # Детальная статистика по типам дублей
    print(f'\n📊 ДЕТАЛЬНАЯ СТАТИСТИКА ДУБЛЕЙ:')
    
    # Группировка по количеству дублей
    duplicate_groups = duplicates['duplicate_count'].value_counts().sort_index()
    print(f'📈 Распределение по количеству дублей:')
    for count, freq in duplicate_groups.items():
        print(f'   {count} копий: {freq} serialno')
    
    # Анализ по типам ВС
    ac_typ_analysis = duplicated_records['ac_typ'].value_counts()
    print(f'\n🚁 Дубли по типам ВС:')
    for ac_typ, count in ac_typ_analysis.head(5).items():
        print(f'   {ac_typ}: {count} записей')
    
    # Анализ по статусам
    status_analysis = duplicated_records['status'].value_counts().sort_index()
    status_names = {0: 'По умолчанию', 1: 'Неактивно', 2: 'Эксплуатация', 3: 'Исправен', 4: 'Ремонт', 5: 'Хранение'}
    print(f'\n📊 Дубли по статусам:')
    for status_id, count in status_analysis.items():
        status_name = status_names.get(status_id, f'Неизвестно({status_id})')
        print(f'   {status_id} - {status_name}: {count} записей')
    
    # Анализ по партномерам
    partno_analysis = duplicated_records.groupby('serialno')['partno'].nunique().sort_values(ascending=False)
    print(f'\n🔧 ТОП-5 serialno с наибольшим разнообразием партномеров:')
    for serialno, partno_count in partno_analysis.head(5).items():
        total_records = duplicates[duplicates['serialno'] == serialno]['duplicate_count'].iloc[0]
        print(f'   serialno "{serialno}": {partno_count} разных партномеров из {total_records} записей')
    
    # Рекомендации по очистке
    print(f'\n💡 РЕКОМЕНДАЦИИ:')
    print(f'   1. Проверить {len(duplicates)} дублированных serialno')
    print(f'   2. Общее количество избыточных записей: {len(duplicated_records) - len(duplicates):,}')
    print(f'   3. Потенциальная экономия места: {(len(duplicated_records) - len(duplicates))/len(df)*100:.1f}%')
    
    if len(duplicates) > 50:
        print(f'   ⚠️ КРИТИЧНО: Очень много дублей! Требуется очистка данных.')
    elif len(duplicates) > 10:
        print(f'   ⚠️ ВНИМАНИЕ: Умеренное количество дублей, рекомендуется проверка.')
    else:
        print(f'   ℹ️ INFO: Небольшое количество дублей, возможно это нормально.')
    
    print(f'\n📁 Детальный анализ сохранен в: {output_file}')
    print(f'✅ Проверка дублей завершена!')

if __name__ == "__main__":
    check_heli_pandas_duplicates() 