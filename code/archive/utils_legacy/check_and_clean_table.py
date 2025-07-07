#!/usr/bin/env python3
"""
Анализ и очистка дубликатов в таблице Status_Components_raw
"""
from clickhouse_driver import Client
import yaml
import logging
import os

def setup_logging():
    """Настройка логирования для maintenance скрипта"""
    os.makedirs('test_output', exist_ok=True)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    logger = logging.getLogger('maintenance')
    logger.setLevel(logging.INFO)
    
    # Убираем существующие обработчики
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Файловый обработчик (перезапись при каждом запуске)
    file_handler = logging.FileHandler('test_output/maintenance.log', mode='w')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    
    # Консольный обработчик
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def main():
    logger = setup_logging()
    logger.info('🔧 === АНАЛИЗ И ОЧИСТКА ТАБЛИЦЫ Status_Components_raw ===')
    
    # Подключение через универсальную функцию
    from config_loader import load_database_config
    config = load_database_config()

    client = Client(
        host=config['host'], 
        port=config['port'], 
        user=config['user'], 
        password=config['password'], 
        database=config['database']
    )

    logger.info('✅ Подключение к ClickHouse установлено')

    # Общая статистика
    result = client.execute('SELECT COUNT(*), COUNT(DISTINCT partno, serialno, version_date) FROM Status_Components_raw')
    total, unique = result[0]
    print(f'Всего записей: {total:,}')
    print(f'Уникальных комбинаций: {unique:,}')
    print(f'Дубликатов: {total - unique}')

    # Статистика по датам версий
    result = client.execute('SELECT version_date, COUNT(*) FROM Status_Components_raw GROUP BY version_date ORDER BY version_date')
    print(f'\nРаспределение по датам версий:')
    for row in result:
        print(f'  {row[0]}: {row[1]:,} записей')

    # Если есть дубликаты, очищаем их
    if total > unique:
        print('\n=== ОЧИСТКА ДУБЛИКАТОВ ===')
        
        # Удаляем записи с неправильной датой (оставляем только 2025-05-28)
        result = client.execute("DELETE FROM Status_Components_raw WHERE version_date != '2025-05-28'")
        print("Удалены записи с неправильными датами версий")
        
        # Проверяем результат
        result = client.execute('SELECT COUNT(*) FROM Status_Components_raw')
        final_count = result[0][0]
        print(f'Записей после очистки: {final_count:,}')
        
        # Финальная статистика
        result = client.execute('''
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT partno) as partnos,
                COUNT(DISTINCT serialno) as serials,
                COUNT(DISTINCT ac_typ) as ac_types,
                MIN(version_date) as min_date,
                MAX(version_date) as max_date
            FROM Status_Components_raw
        ''')
        
        if result:
            row = result[0]
            print(f'\n📊 ФИНАЛЬНАЯ СТАТИСТИКА:')
            print(f'  📄 Всего записей: {row[0]:,}')
            print(f'  🔧 Уникальных партномеров: {row[1]:,}')
            print(f'  🏷️  Уникальных серийных номеров: {row[2]:,}')
            print(f'  ✈️  Типов ВС: {row[3]:,}')
            print(f'  📆 Период версий: {row[4]} - {row[5]}')
            
            print(f'\n✅ RAW слой готов для использования!')
    else:
        print('\n✅ Дубликатов нет, таблица в порядке!')

if __name__ == '__main__':
    main() 