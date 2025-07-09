#!/usr/bin/env python3
"""
Добавление поля version_id во все основные загрузочные таблицы
"""

import yaml
from clickhouse_driver import Client
from pathlib import Path
import logging
import sys

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config():
    """Загружает конфигурацию базы данных"""
    config_path = Path("config/database_config.yaml")
    if not config_path.exists():
        logger.error("❌ Файл конфигурации не найден")
        return None
    
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def add_version_id_fields(client):
    """Добавляет поле version_id во все основные таблицы"""
    
    # Список таблиц для модификации
    tables_to_modify = [
        'heli_pandas',
        'heli_raw', 
        'md_components',
        'flight_program',
        'program_ac'
    ]
    
    logger.info("🔧 Добавление поля version_id в основные таблицы...")
    
    for table_name in tables_to_modify:
        try:
            logger.info(f"📊 Обработка таблицы: {table_name}")
            
            # Проверяем существование таблицы
            check_table_sql = f"EXISTS TABLE {table_name}"
            exists = client.execute(check_table_sql)[0][0]
            
            if not exists:
                logger.warning(f"⚠️ Таблица {table_name} не существует, пропускаем")
                continue
            
            # Проверяем наличие поля version_id
            check_column_sql = f"""
            SELECT count() 
            FROM system.columns 
            WHERE table = '{table_name}' AND name = 'version_id'
            """
            
            column_exists = client.execute(check_column_sql)[0][0] > 0
            
            if column_exists:
                logger.info(f"✅ Поле version_id уже существует в {table_name}")
                continue
            
            # Добавляем поле version_id
            alter_sql = f"""
            ALTER TABLE {table_name} 
            ADD COLUMN `version_id` UInt8 DEFAULT 1
            """
            
            client.execute(alter_sql)
            logger.info(f"✅ Поле version_id добавлено в {table_name}")
            
            # Проверяем результат
            verify_sql = f"""
            SELECT count() 
            FROM system.columns 
            WHERE table = '{table_name}' AND name = 'version_id'
            """
            
            verified = client.execute(verify_sql)[0][0] > 0
            if verified:
                logger.info(f"✅ Поле version_id успешно добавлено в {table_name}")
            else:
                logger.error(f"❌ Ошибка при добавлении поля в {table_name}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке таблицы {table_name}: {e}")
            continue

def show_table_structures(client):
    """Показывает структуру таблиц после модификации"""
    
    tables_to_show = [
        'heli_pandas',
        'heli_raw', 
        'md_components',
        'flight_program',
        'program_ac'
    ]
    
    logger.info("📋 Проверка структуры таблиц после модификации...")
    
    for table_name in tables_to_show:
        try:
            # Проверяем существование таблицы
            check_table_sql = f"EXISTS TABLE {table_name}"
            exists = client.execute(check_table_sql)[0][0]
            
            if not exists:
                logger.warning(f"⚠️ Таблица {table_name} не существует")
                continue
            
            # Получаем структуру таблицы
            describe_sql = f"DESCRIBE TABLE {table_name}"
            columns = client.execute(describe_sql)
            
            print(f"\n📊 Таблица: {table_name}")
            print("-" * 60)
            
            # Ищем поля version_date и version_id
            version_fields = []
            total_fields = 0
            
            for column in columns:
                total_fields += 1
                col_name = column[0]
                col_type = column[1]
                
                if 'version' in col_name.lower():
                    version_fields.append(f"  🔧 {col_name}: {col_type}")
            
            print(f"📈 Всего полей: {total_fields}")
            print("🔧 Поля версионирования:")
            if version_fields:
                for field in version_fields:
                    print(field)
            else:
                print("  ❌ Поля версионирования не найдены")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке таблицы {table_name}: {e}")

def main():
    """Основная функция"""
    logger.info("🚀 Запуск добавления поля version_id")
    
    # Загружаем конфигурацию
    config = load_config()
    if not config:
        return
    
    # Подключаемся к ClickHouse
    try:
        ch_config = config['clickhouse']
        client = Client(
            host=ch_config['host'],
            port=ch_config['port'],
            user=ch_config['user'],
            password=ch_config['password'],
            database=ch_config['database']
        )
        
        logger.info(f"✅ Подключение к ClickHouse: {ch_config['host']}:{ch_config['port']}")
        
        # Добавляем поля version_id
        add_version_id_fields(client)
        
        # Показываем результат
        show_table_structures(client)
        
        logger.info("✅ Добавление полей version_id завершено")
        
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к ClickHouse: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 