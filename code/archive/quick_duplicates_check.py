#!/usr/bin/env python3
"""
Быстрая проверка дублей serialno во всех таблицах
"""
import sys
sys.path.append('code')
from utils.config_loader import get_clickhouse_client

def quick_duplicates_check():
    """Быстрая проверка дублей serialno во всех таблицах"""
    
    client = get_clickhouse_client()
    
    print('🔍 БЫСТРАЯ ПРОВЕРКА ДУБЛЕЙ SERIALNO ВО ВСЕХ ТАБЛИЦАХ')
    print('=' * 60)
    
    # Список всех таблиц для проверки
    tables = [
        'heli_raw',
        'heli_pandas', 
        'md_components',
        'dictionary_data',
        'status_overhaul'
    ]
    
    for table in tables:
        print(f'\n📊 ТАБЛИЦА: {table}')
        
        try:
            # Проверяем общее количество записей
            total_query = f'SELECT COUNT(*) FROM {table}'
            total = client.execute(total_query)[0][0]
            
            # Проверяем количество уникальных serialno
            unique_query = f'''
            SELECT COUNT(DISTINCT serialno) 
            FROM {table} 
            WHERE serialno IS NOT NULL AND serialno != ''
            '''
            unique = client.execute(unique_query)[0][0]
            
            # Подсчитываем записи с непустыми serialno
            non_empty_query = f'''
            SELECT COUNT(*) 
            FROM {table} 
            WHERE serialno IS NOT NULL AND serialno != ''
            '''
            non_empty = client.execute(non_empty_query)[0][0]
            
            dups = non_empty - unique
            
            print(f'   📈 Всего записей: {total:,}')
            print(f'   📋 Записей с serialno: {non_empty:,}')
            print(f'   🆔 Уникальных serialno: {unique:,}')
            
            if dups > 0:
                print(f'   ❌ ДУБЛЕЙ serialno: {dups:,}')
                
                # Получаем топ-5 дублированных serialno
                top_dups_query = f'''
                SELECT serialno, COUNT(*) as count
                FROM {table}
                WHERE serialno IS NOT NULL AND serialno != ''
                GROUP BY serialno
                HAVING COUNT(*) > 1
                ORDER BY count DESC
                LIMIT 5
                '''
                
                top_dups = client.execute(top_dups_query)
                if top_dups:
                    print(f'   🔝 ТОП-5 дублей:')
                    for serialno, count in top_dups:
                        print(f'      "{serialno}": {count} записей')
            else:
                print(f'   ✅ Дублей НЕТ')
                
        except Exception as e:
            print(f'   ⚠️  ОШИБКА: {e}')
    
    print('\n' + '=' * 60)
    print('✅ Быстрая проверка дублей завершена')

if __name__ == "__main__":
    quick_duplicates_check() 