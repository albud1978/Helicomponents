import pandas as pd
import sys
from pathlib import Path

sys.path.append('code')
from utils.config_loader import get_clickhouse_client

def analyze_all_duplicates():
    """
    Анализирует ТОЧНЫЕ дубли serialno в heli_pandas и heli_raw с выгрузкой в Excel
    
    ВАЖНО: Анализируются только ТОЧНЫЕ совпадения serialno (serialno = serialno).
    Составные номера типа "12" и "12.22" считаются РАЗНЫМИ компонентами.
    """
    
    client = get_clickhouse_client()
    output_dir = Path('data_output')
    output_dir.mkdir(exist_ok=True)
    
    print('🔍 АНАЛИЗ ТОЧНЫХ ДУБЛЕЙ SERIALNO В БАЗЕ ДАННЫХ')
    print('⚠️  ТОЛЬКО точные совпадения (без составных номеров)')
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
        status
    FROM heli_pandas 
    WHERE version_date = '2025-05-28'
    ORDER BY serialno, partno
    '''
    
    pandas_data = client.execute(query_pandas)
    pandas_df = pd.DataFrame(pandas_data, columns=[
        'serialno', 'partno', 'ac_typ', 'location', 'mfg_date', 
        'removal_date', 'target_date', 'condition', 'owner', 'status'
    ])
    
    print(f'📊 Всего записей heli_pandas: {len(pandas_df)}')
    
    # Анализ ТОЧНЫХ дублей в heli_pandas
    pandas_counts = pandas_df['serialno'].value_counts()
    pandas_duplicates = pandas_counts[pandas_counts > 1]
    
    if len(pandas_duplicates) > 0:
        print(f'❌ Найдено {len(pandas_duplicates)} ТОЧНО дублированных serialno:')
        
        # Создаем детальный анализ ВСЕ дублей
        pandas_dup_analysis = []
        for serialno, count in pandas_duplicates.items():
            dup_records = pandas_df[pandas_df['serialno'] == serialno]
            for _, record in dup_records.iterrows():
                pandas_dup_analysis.append({
                    'serialno': serialno,
                    'duplicate_count': count,
                    'partno': record['partno'],
                    'ac_typ': record['ac_typ'],
                    'location': record['location'],
                    'mfg_date': record['mfg_date'],
                    'removal_date': record['removal_date'],
                    'target_date': record['target_date'],
                    'condition': record['condition'],
                    'owner': record['owner'],
                    'status': record['status']
                })
        
        pandas_dup_df = pd.DataFrame(pandas_dup_analysis)
        
        # Создаем статистику повторяемости для heli_pandas
        pandas_stats_df = pandas_duplicates.reset_index()
        pandas_stats_df.columns = ['serialno', 'duplicate_count']
        pandas_stats_df = pandas_stats_df.sort_values('duplicate_count', ascending=False)
        
        # Выгружаем в Excel с несколькими вкладками
        pandas_file = output_dir / 'heli_pandas_serialno_EXACT_duplicates.xlsx'
        with pd.ExcelWriter(pandas_file, engine='openpyxl') as writer:
            pandas_dup_df.to_excel(writer, sheet_name='All Duplicates', index=False)
            pandas_stats_df.to_excel(writer, sheet_name='Duplicate Stats', index=False)
        
        print(f'📁 ТОЧНЫЕ дубли heli_pandas сохранены: {pandas_file}')
        print(f'   📋 Вкладка "All Duplicates": {len(pandas_dup_analysis)} записей')
        print(f'   📊 Вкладка "Duplicate Stats": {len(pandas_stats_df)} уникальных serialno с дублями')
        
        # Показываем топ дублей
        for _, row in pandas_stats_df.head(5).iterrows():
            print(f'   serialno "{row["serialno"]}": {row["duplicate_count"]} записей')
            
    else:
        print('✅ ТОЧНЫХ дублей serialno в heli_pandas НЕ НАЙДЕНО')
    
    # ===== АНАЛИЗ heli_raw =====
    print('\n📊 АНАЛИЗ heli_raw (все компоненты):')
    
    # Сначала получаем статистику
    stats_query = '''
    SELECT 
        COUNT(*) as total,
        COUNT(DISTINCT serialno) as unique_serialno
    FROM heli_raw 
    WHERE version_date = '2025-05-28' 
      AND serialno IS NOT NULL 
      AND serialno != ''
    '''
    
    total_raw, unique_raw = client.execute(stats_query)[0]
    dups_raw = total_raw - unique_raw
    print(f'📈 Статистика heli_raw: {total_raw} записей, {unique_raw} уникальных serialno, {dups_raw} ТОЧНЫХ дублей')
    
    if dups_raw > 0:
        # Получаем ВСЕ ТОЧНО дублированные serialno
        all_dups_query = '''
        SELECT 
            serialno,
            COUNT(*) as count
        FROM heli_raw 
        WHERE version_date = '2025-05-28'
          AND serialno IS NOT NULL 
          AND serialno != ''
        GROUP BY serialno
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        '''
        
        all_dups = client.execute(all_dups_query)
        print(f'❌ Найдено {len(all_dups)} ТОЧНО дублированных serialno в heli_raw')
        print(f'📊 ТОП-10 ТОЧНЫХ дублей:')
        
        # Показываем только топ-10 в консоли для краткости
        for serialno, count in all_dups[:10]:
            print(f'   serialno "{serialno}": {count} записей')
        
        if len(all_dups) > 10:
            print(f'   ... и еще {len(all_dups) - 10} ТОЧНО дублированных serialno')
        
        # Создаем статистику повторяемости для heli_raw
        raw_stats_df = pd.DataFrame(all_dups, columns=['serialno', 'duplicate_count'])
        raw_stats_df = raw_stats_df.sort_values('duplicate_count', ascending=False)
        
        # Создаем анализ для ВСЕХ дублированных serialno
        print(f'📥 Загружаем детали для всех {len(all_dups)} ТОЧНО дублированных serialno...')
        raw_dup_analysis = []
        
        for i, (serialno, count) in enumerate(all_dups):
            if (i + 1) % 100 == 0:
                print(f'   Обработано {i + 1}/{len(all_dups)} дублей...')
                
            # Получаем детали для этого serialno (ТОЧНОЕ совпадение)
            detail_query = f'''
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
            WHERE version_date = '2025-05-28' 
              AND serialno = '{serialno}'
            ORDER BY partno, location
            '''
            
            details = client.execute(detail_query)
            for detail in details:
                raw_dup_analysis.append({
                    'serialno': detail[0],
                    'duplicate_count': count,
                    'partno': detail[1],
                    'ac_typ': detail[2],
                    'location': detail[3],
                    'mfg_date': detail[4],
                    'removal_date': detail[5],
                    'target_date': detail[6],
                    'condition': detail[7],
                    'owner': detail[8]
                })
        
        # Выгружаем в Excel с несколькими вкладками
        if raw_dup_analysis:
            raw_dup_df = pd.DataFrame(raw_dup_analysis)
            raw_file = output_dir / 'heli_raw_serialno_EXACT_duplicates.xlsx'
            
            with pd.ExcelWriter(raw_file, engine='openpyxl') as writer:
                raw_dup_df.to_excel(writer, sheet_name='All Duplicates', index=False)
                raw_stats_df.to_excel(writer, sheet_name='Duplicate Stats', index=False)
            
            print(f'📁 ТОЧНЫЕ дубли heli_raw сохранены: {raw_file}')
            print(f'   📋 Вкладка "All Duplicates": {len(raw_dup_analysis)} записей')
            print(f'   📊 Вкладка "Duplicate Stats": {len(raw_stats_df)} уникальных serialno с дублями')
    
    else:
        print('✅ ТОЧНЫХ дублей serialno в heli_raw НЕ НАЙДЕНО')
    
    # ===== СВОДНАЯ СТАТИСТИКА =====
    print('\n' + '=' * 60)
    print('📊 СВОДНАЯ СТАТИСТИКА ТОЧНЫХ ДУБЛЕЙ:')
    print(f'   heli_pandas: {len(pandas_df)} записей, ТОЧНЫХ дублей: {len(pandas_duplicates)}')
    print(f'   heli_raw: {total_raw} записей, ТОЧНЫХ дублей: {dups_raw}')
    
    print(f'\n📁 ФАЙЛЫ СОЗДАНЫ:')
    if len(pandas_duplicates) > 0:
        print(f'   data_output/heli_pandas_serialno_EXACT_duplicates.xlsx (с вкладками)')
    if dups_raw > 0:
        print(f'   data_output/heli_raw_serialno_EXACT_duplicates.xlsx (с вкладками)')
    
    print('\n📋 СТРУКТУРА EXCEL ФАЙЛОВ:')
    print('   🔸 Вкладка "All Duplicates": детальные данные всех ТОЧНО дублированных записей')
    print('   🔸 Вкладка "Duplicate Stats": статистика повторяемости по serialno (ТОЧНЫЕ совпадения)')
    
    print('\n⚠️  ВАЖНО: Анализировались только ТОЧНЫЕ совпадения serialno!')
    print('   Составные номера типа "12" и "12.22" считаются РАЗНЫМИ компонентами.')
    
    print('✅ Анализ ТОЧНЫХ дублей serialno завершен')

if __name__ == "__main__":
    analyze_all_duplicates() 