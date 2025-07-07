#!/usr/bin/env python3
"""
ETL Pipeline Runner для проекта Helicopter Component Lifecycle Prediction

Последовательно запускает все ETL скрипты с проверкой зависимостей и логгированием.
Обновлен под новую архитектуру со встроенными ID полями из Excel.
"""

import subprocess
import sys
import time
import logging
from pathlib import Path
from config_loader import get_clickhouse_client

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_clickhouse_client_etl():
    """Создает подключение к ClickHouse как в других скриптах"""
    try:
        client = get_clickhouse_client()
        # Тест подключения
        result = client.execute("SELECT 1")
        return client
    except Exception as e:
        logger.error(f"Ошибка подключения к ClickHouse: {e}")
        return None

def table_exists(client, table_name):
    """Проверяет существование таблицы"""
    if client is None:
        return False
    
    try:
        query = f"""
        SELECT count() 
        FROM system.tables 
        WHERE database = 'default' AND name = '{table_name}'
        """
        result = client.execute(query)[0][0]
        return result > 0
    except Exception as e:
        logger.warning(f"Ошибка проверки таблицы {table_name}: {e}")
        return False

def get_table_count(client, table_name):
    """Получает количество записей в таблице"""
    if client is None or not table_exists(client, table_name):
        return 0
    
    try:
        count = client.execute(f"SELECT count() FROM {table_name}")[0][0]
        return count
    except Exception as e:
        logger.warning(f"Ошибка получения количества записей в {table_name}: {e}")
        return 0

def check_table_structure(client, table_name, expected_columns):
    """Проверяет структуру таблицы"""
    if client is None or not table_exists(client, table_name):
        return False
    
    try:
        query = f"""
        SELECT name 
        FROM system.columns 
        WHERE database = 'default' AND table = '{table_name}'
        ORDER BY position
        """
        columns = [row[0] for row in client.execute(query)]
        
        if len(columns) >= len(expected_columns):
            logger.info(f"Таблица {table_name} имеет {len(columns)} колонок")
            return True
        else:
            logger.warning(f"Таблица {table_name} имеет недостаточно колонок: {len(columns)}")
            return False
    except Exception as e:
        logger.warning(f"Ошибка проверки структуры {table_name}: {e}")
        return False

def run_script(script_name, description, required_tables=None, check_result_table=None):
    """Запускает ETL скрипт с проверками"""
    logger.info(f"🚀 Запуск: {description}")
    
    # Проверка зависимостей
    client = get_clickhouse_client_etl()
    if required_tables and client:
        for table in required_tables:
            if not table_exists(client, table):
                logger.warning(f"⚠️  Требуемая таблица {table} не найдена, но продолжаем выполнение")
    
    # Запуск скрипта
    script_path = Path(__file__).parent.parent / script_name
    if not script_path.exists():
        logger.error(f"❌ Скрипт {script_name} не найден!")
        return False
    
    try:
        start_time = time.time()
        # Запускаем из корневой директории проекта
        project_root = Path(__file__).parent.parent.parent
        result = subprocess.run([sys.executable, str(script_path)], 
                              capture_output=True, text=True, check=True,
                              cwd=str(project_root))
        
        execution_time = time.time() - start_time
        logger.info(f"✅ {description} выполнено за {execution_time:.2f} сек")
        
        # Проверка результата
        if check_result_table and client:
            if table_exists(client, check_result_table):
                count = get_table_count(client, check_result_table)
                logger.info(f"📊 Таблица {check_result_table}: {count:,} записей")
            else:
                logger.warning(f"⚠️  Результирующая таблица {check_result_table} не найдена")
        
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Ошибка в {script_name}:")
        logger.error(f"STDOUT: {e.stdout}")
        logger.error(f"STDERR: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"❌ Неожиданная ошибка в {script_name}: {e}")
        return False

def check_gpu_readiness():
    """Проверяет готовность данных для GPU обработки"""
    logger.info("🔍 Проверка готовности данных для GPU...")
    
    client = get_clickhouse_client_etl()
    if not client:
        logger.error("❌ Нет подключения к ClickHouse")
        return False
    
    # Список критических таблиц с ожидаемыми полями
    critical_tables = {
        'heli_pandas': ['partseqno_i', 'psn', 'address_i', 'ac_type_i', 'ac_type_mask'],
        'status_overhaul': ['partseqno_i', 'psn'],
        'program_ac': ['ac_type_i'],
        'program': ['program_id'],
        'md_components': ['partseqno_i']
    }
    
    all_ready = True
    
    for table_name, key_columns in critical_tables.items():
        if table_exists(client, table_name):
            count = get_table_count(client, table_name)
            if count > 0:
                logger.info(f"✅ {table_name}: {count:,} записей")
                
                # Проверка ключевых полей для heli_pandas
                if table_name == 'heli_pandas':
                    all_ready &= check_table_structure(client, table_name, key_columns)
            else:
                logger.warning(f"⚠️  {table_name}: пустая таблица")
                all_ready = False
        else:
            logger.error(f"❌ {table_name}: таблица не найдена")
            all_ready = False
    
    return all_ready

def main():
    """Основной ETL пайплайн"""
    logger.info("🎯 ===== ЗАПУСК ETL ПАЙПЛАЙНА HELICOPTER COMPONENT LIFECYCLE =====")
    logger.info("📋 Архитектура: встроенные ID поля из Excel (partseqno_i, psn, address_i, ac_type_i)")
    
    start_time = time.time()
    
    # ETL последовательность
    etl_steps = [
        # 1. Базовые справочники
        {
            'script': 'dictionary_creator.py',
            'description': 'Создание базовых справочников',
            'required_tables': None,
            'result_table': 'dict_status_flat'
        },
        
        # 2. Справочник номеров ВС
        {
            'script': 'aircraft_number_dict_creator.py', 
            'description': 'Создание справочника номеров ВС',
            'required_tables': None,
            'result_table': 'aircraft_number_dict'
        },
        
        # 3. Загрузка основных данных со встроенными ID
        {
            'script': 'dual_loader.py',
            'description': 'Загрузка данных Status_Components.xlsx со встроенными ID',
            'required_tables': ['dict_status_flat'],
            'result_table': 'heli_pandas'
        },
        
        # 4. Обогащение данных (только ac_type_mask)
        {
            'script': 'enrich_heli_pandas.py',
            'description': 'Обогащение данных ac_type_mask',
            'required_tables': ['heli_pandas', 'ac_type_dict_flat'],
            'result_table': 'heli_pandas'
        },
        
        # 5. Обработка данных статуса и капремонта
        {
            'script': 'status_overhaul_loader.py',
            'description': 'Загрузка данных статуса и капремонта',
            'required_tables': ['heli_pandas'],
            'result_table': 'status_overhaul'
        },
        
        # 6. Обработка данных программ
        {
            'script': 'program_loader.py',
            'description': 'Загрузка данных программ',
            'required_tables': None,
            'result_table': 'program'
        },
        
        # 7. Обработка связки программ и ВС
        {
            'script': 'program_ac_loader.py',
            'description': 'Загрузка связки программ и ВС',
            'required_tables': ['program'],
            'result_table': 'program_ac'
        },
        
        # 8. Загрузка MD компонентов
        {
            'script': 'md_components_loader.py',
            'description': 'Загрузка MD компонентов',
            'required_tables': ['heli_pandas'],
            'result_table': 'md_components'
        },
        
        # 9. Расчет Beyond Repair
        {
            'script': 'calculate_beyond_repair.py',
            'description': 'Расчет Beyond Repair (br) для md_components',
            'required_tables': ['md_components'],
            'result_table': 'md_components'
        }
    ]
    
    # Выполнение ETL шагов
    success_count = 0
    for step in etl_steps:
        success = run_script(
            step['script'], 
            step['description'], 
            step.get('required_tables'),
            step.get('result_table')
        )
        
        if success:
            success_count += 1
        else:
            logger.error(f"❌ Критическая ошибка в {step['script']}")
            # Продолжаем выполнение несмотря на ошибки
    
    # Финальная проверка
    logger.info("\n🔍 ===== ФИНАЛЬНАЯ ПРОВЕРКА ГОТОВНОСТИ =====")
    gpu_ready = check_gpu_readiness()
    
    # Итоги
    total_time = time.time() - start_time
    logger.info(f"\n📊 ===== ИТОГИ ETL ПАЙПЛАЙНА =====")
    logger.info(f"✅ Успешно выполнено: {success_count}/{len(etl_steps)} шагов")
    logger.info(f"⏱️  Общее время выполнения: {total_time:.2f} сек")
    logger.info(f"🎯 Готовность для GPU: {'✅ ДА' if gpu_ready else '❌ НЕТ'}")
    
    if gpu_ready:
        logger.info("🚀 Система готова для Agent-Based моделирования на Flame GPU!")
    else:
        logger.warning("⚠️  Требуется дополнительная настройка перед GPU обработкой")
    
    return success_count == len(etl_steps) and gpu_ready

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 