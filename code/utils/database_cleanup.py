#!/usr/bin/env python3
"""
Утилита для быстрой зачистки всех ETL таблиц
Helicopter Component Lifecycle Prediction

Использование:
    python3 code/utils/database_cleanup.py

Автор: AI Agent для пользователя budnik_an
"""

import sys
from pathlib import Path
from config_loader import get_clickhouse_client

def cleanup_all_etl_tables():
    """Удаляет все ETL таблицы"""
    
    etl_tables = [
        'md_components',
        'program_ac', 
        'status_overhaul',
        'heli_raw',
        'heli_pandas',
        'flight_program'
    ]
    
    print("🗑️ === ЗАЧИСТКА ETL ТАБЛИЦ ===")
    print("=" * 32)
    
    try:
        client = get_clickhouse_client()
        print("✅ Подключение к ClickHouse успешно!")
    except Exception as e:
        print(f"❌ Ошибка подключения к ClickHouse: {e}")
        return False
    
    success_count = 0
    for table in etl_tables:
        try:
            client.execute(f'DROP TABLE IF EXISTS {table}')
            print(f"✅ Удалена таблица: {table}")
            success_count += 1
        except Exception as e:
            print(f"❌ Ошибка удаления {table}: {e}")
    
    print()
    print(f"📊 Результат: {success_count}/{len(etl_tables)} таблиц удалено")
    
    if success_count == len(etl_tables):
        print("✅ Зачистка завершена успешно!")
        return True
    else:
        print("⚠️ Зачистка завершена с предупреждениями")
        return False

if __name__ == "__main__":
    success = cleanup_all_etl_tables()
    exit(0 if success else 1) 