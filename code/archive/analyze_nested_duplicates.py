import pandas as pd
import sys
from pathlib import Path

sys.path.append('code')
from utils.config_loader import get_clickhouse_client

def analyze_nested_duplicates():
    """Анализирует составные/вложенные дубли serialno в heli_raw"""
    
    client = get_clickhouse_client()
    output_dir = Path('data_output')
    output_dir.mkdir(exist_ok=True)
    
    print('🔍 АНАЛИЗ СОСТАВНЫХ/ВЛОЖЕННЫХ ДУБЛЕЙ SERIALNO')
    print('=' * 60)
    
    # Получаем все уникальные serialno из heli_raw
    query = '''
    SELECT DISTINCT serialno
    FROM heli_raw 
    WHERE version_date = '2025-05-28'
      AND serialno IS NOT NULL 
      AND serialno != ''
    ORDER BY serialno
    '''
    
    serialnos = [row[0] for row in client.execute(query)]
    print(f'📊 Всего уникальных serialno: {len(serialnos)}')
    
    # Ищем вложенные дубли
    nested_pairs = []
    
    print('🔍 Поиск вложенных дублей...')
    for i, short_sn in enumerate(serialnos):
        if (i + 1) % 1000 == 0:
            print(f'   Проверено {i + 1}/{len(serialnos)} serialno...')
            
        # Ищем все serialno, которые содержат текущий как подстроку
        for long_sn in serialnos:
            if short_sn != long_sn and short_sn in long_sn:
                # Проверяем, что это действительно вложение, а не случайное совпадение
                # Например, "12" в "12.22" или "12-ABC" - да, но "12" в "ABC123" - нет
                if (long_sn.startswith(short_sn + '.') or 
                    long_sn.startswith(short_sn + '-') or
                    long_sn.startswith(short_sn + '_') or
                    long_sn == short_sn + 'А' or  # русская буква
                    long_sn == short_sn + 'Б' or
                    (short_sn.isdigit() and long_sn.startswith(short_sn) and not long_sn[len(short_sn)].isdigit())):
                    
                    nested_pairs.append({
                        'short_serialno': short_sn,
                        'long_serialno': long_sn,
                        'pattern': 'starts_with'
                    })
    
    print(f'✅ Найдено {len(nested_pairs)} потенциальных вложенных пар')
    
    if len(nested_pairs) > 0:
        # Создаем DataFrame с вложенными парами
        nested_df = pd.DataFrame(nested_pairs)
        
        # Получаем статистику для каждого serialno
        print('📊 Получаем статистику использования для вложенных serialno...')
        
        detailed_analysis = []
        unique_serialnos = set()
        
        for pair in nested_pairs:
            unique_serialnos.add(pair['short_serialno'])
            unique_serialnos.add(pair['long_serialno'])
        
        for sn in unique_serialnos:
            count_query = f'''
            SELECT COUNT(*) 
            FROM heli_raw 
            WHERE version_date = '2025-05-28' 
              AND serialno = '{sn}'
            '''
            count = client.execute(count_query)[0][0]
            
            detailed_analysis.append({
                'serialno': sn,
                'record_count': count,
                'length': len(sn)
            })
        
        # Создаем детальный анализ
        detailed_df = pd.DataFrame(detailed_analysis)
        detailed_df = detailed_df.sort_values(['length', 'record_count'], ascending=[True, False])
        
        # Группируем вложенные пары для лучшего анализа
        nested_groups = {}
        for pair in nested_pairs:
            short = pair['short_serialno']
            long = pair['long_serialno']
            
            if short not in nested_groups:
                nested_groups[short] = []
            nested_groups[short].append(long)
        
        # Создаем сводку по группам
        group_analysis = []
        for short_sn, long_list in nested_groups.items():
            short_count = detailed_df[detailed_df['serialno'] == short_sn]['record_count'].iloc[0]
            
            for long_sn in long_list:
                long_count = detailed_df[detailed_df['serialno'] == long_sn]['record_count'].iloc[0]
                
                group_analysis.append({
                    'short_serialno': short_sn,
                    'short_count': short_count,
                    'long_serialno': long_sn,
                    'long_count': long_count,
                    'total_records': short_count + long_count,
                    'potential_duplicate': 'Возможно' if short_count > 1 and long_count > 1 else 'Маловероятно'
                })
        
        group_df = pd.DataFrame(group_analysis)
        group_df = group_df.sort_values('total_records', ascending=False)
        
        # Выгружаем результаты в Excel
        output_file = output_dir / 'nested_serialno_duplicates_analysis.xlsx'
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            nested_df.to_excel(writer, sheet_name='Nested Pairs', index=False)
            detailed_df.to_excel(writer, sheet_name='Serialno Stats', index=False)
            group_df.to_excel(writer, sheet_name='Group Analysis', index=False)
        
        print(f'📁 Анализ вложенных дублей сохранен: {output_file}')
        print(f'   📋 Вкладка "Nested Pairs": {len(nested_pairs)} пар')
        print(f'   📊 Вкладка "Serialno Stats": {len(detailed_df)} уникальных serialno')
        print(f'   🔍 Вкладка "Group Analysis": {len(group_df)} детальных сравнений')
        
        # Показываем топ-10 подозрительных групп
        print('\n🚨 ТОП-10 ПОДОЗРИТЕЛЬНЫХ ВЛОЖЕННЫХ ГРУПП:')
        suspicious = group_df[group_df['potential_duplicate'] == 'Возможно'].head(10)
        
        if len(suspicious) > 0:
            for _, row in suspicious.iterrows():
                print(f'   "{row["short_serialno"]}" ({row["short_count"]} записей) ⊂ "{row["long_serialno"]}" ({row["long_count"]} записей)')
        else:
            print('   ✅ Подозрительных вложенных дублей не найдено')
        
        # Показываем общую статистику
        print(f'\n📈 СТАТИСТИКА:')
        print(f'   Всего вложенных пар: {len(nested_pairs)}')
        print(f'   Уникальных serialno в анализе: {len(detailed_df)}')
        print(f'   Потенциально подозрительных: {len(group_df[group_df["potential_duplicate"] == "Возможно"])}')
        
    else:
        print('✅ Вложенных дублей serialno НЕ НАЙДЕНО')
    
    print('\n✅ Анализ составных/вложенных дублей завершен')

if __name__ == "__main__":
    analyze_nested_duplicates() 