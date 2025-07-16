#!/usr/bin/env python3
"""
Проверка статистики heli_pandas после исправления dual_loader
"""
import sys
sys.path.append('code')
from utils.config_loader import get_clickhouse_client

def check_heli_pandas_status():
    """Проверяет статистику heli_pandas и распределение по полю status"""
    
    client = get_clickhouse_client()
    
    print('📊 СТАТИСТИКА heli_pandas ПОСЛЕ ИСПРАВЛЕНИЯ')
    print('=' * 50)
    
    # Общее количество записей
    total_query = "SELECT COUNT(*) FROM heli_pandas WHERE version_date = '2025-05-28'"
    total = client.execute(total_query)[0][0]
    print(f'📈 Всего записей в heli_pandas: {total:,}')
    
    # Структура таблицы
    print('\n🔍 СТРУКТУРА ТАБЛИЦЫ heli_pandas:')
    structure = client.execute('DESCRIBE heli_pandas')
    has_status = False
    for row in structure:
        print(f'   {row[0]}: {row[1]}')
        if row[0] == 'status_id':
            has_status = True
    
    # Проверяем поле status_id если есть
    if has_status:
        print('\n📊 РАСПРЕДЕЛЕНИЕ ПО ПОЛЮ STATUS_ID:')
        
        status_query = """
        SELECT status_id, COUNT(*) as count
        FROM heli_pandas 
        WHERE version_date = '2025-05-28'
        GROUP BY status_id
        ORDER BY status_id
        """
        
        status_stats = client.execute(status_query)
        
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
        
        total_with_status = 0
        for status_id, count in status_stats:
            status_name = status_names.get(status_id, f'Неизвестно({status_id})')
            print(f'   {status_id} - {status_name}: {count:,} записей')
            total_with_status += count
        
        print(f'\n📋 ИТОГО с указанным status_id: {total_with_status:,} записей')
        
    else:
        print('\n⚠️ Поле status_id НЕ НАЙДЕНО в структуре таблицы')
    
    # Уникальные партномера
    unique_partnos_query = """
    SELECT COUNT(DISTINCT partno)
    FROM heli_pandas 
    WHERE version_date = '2025-05-28'
    """
    unique_partnos = client.execute(unique_partnos_query)[0][0]
    print(f'\n📦 Уникальных партномеров: {unique_partnos}')
    
    # Топ-5 партномеров
    top_partnos_query = """
    SELECT partno, COUNT(*) as count
    FROM heli_pandas 
    WHERE version_date = '2025-05-28'
    GROUP BY partno
    ORDER BY count DESC
    LIMIT 5
    """
    top_partnos = client.execute(top_partnos_query)
    print(f'\n🔝 ТОП-5 партномеров по количеству:')
    for partno, count in top_partnos:
        print(f'   {partno}: {count:,} записей')
    
    # Сравнение с ожидаемыми 37 партномерами из md_components
    md_partnos_query = "SELECT COUNT(DISTINCT partno) FROM md_components"
    expected_partnos = client.execute(md_partnos_query)[0][0]
    
    print(f'\n🎯 СООТВЕТСТВИЕ MD_COMPONENTS:')
    print(f'   Ожидалось партномеров: {expected_partnos}')
    print(f'   Найдено в heli_pandas: {unique_partnos}')
    
    if unique_partnos == expected_partnos:
        print(f'   ✅ ПОЛНОЕ СООТВЕТСТВИЕ!')
    else:
        print(f'   ⚠️ Несоответствие - нужна дополнительная проверка')
    
    print('\n✅ Проверка завершена')

if __name__ == "__main__":
    check_heli_pandas_status() 