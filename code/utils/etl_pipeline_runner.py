#!/usr/bin/env python3
"""
ETL Pipeline Runner для проекта Helicopter Component Lifecycle Prediction

Последовательно запускает все ETL скрипты с проверкой зависимостей и логгированием.
Обновлен под новую архитектуру с централизованным управлением версиями.
"""

import subprocess
import sys
import time
import logging
from pathlib import Path
from datetime import date, datetime
from config_loader import get_clickhouse_client
from etl_version_manager import ETLVersionManager

# Настройка логгирования
Path('logs').mkdir(exist_ok=True)  # Создаем директорию для логов
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

def run_script(script_name, description, version_date, version_id, required_tables=None, check_result_table=None):
    """Запускает ETL скрипт с проверками и параметрами версионирования"""
    logger.info(f"🚀 Запуск: {description}")
    logger.info(f"🔧 Версия: {version_date} (version_id={version_id})")
    
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
        
        # Передаем параметры версионирования скрипту
        result = subprocess.run(
            [sys.executable, str(script_path),
             '--version-date', str(version_date),
             '--version-id', str(version_id)], 
            capture_output=True, 
            text=True, 
            check=True,
            cwd=str(project_root),
            input="1\n1\n1\n1\n1\n"  # Автоматические ответы для диалогов перезаписи
        )
        
        execution_time = time.time() - start_time
        logger.info(f"✅ {description} выполнено за {execution_time:.2f} сек")
        
        # Показываем последние строки вывода для диагностики
        if result.stdout:
            output_lines = result.stdout.strip().split('\n')
            logger.info(f"📝 Последние строки вывода:")
            for line in output_lines[-3:]:  # Последние 3 строки
                logger.info(f"   {line}")
        
        # Проверка результата
        if check_result_table and client:
            if table_exists(client, check_result_table):
                count = get_table_count(client, check_result_table)
                logger.info(f"📊 Таблица {check_result_table}: {count:,} записей")
            else:
                logger.warning(f"⚠️  Результирующая таблица {check_result_table} не найдена")
        
        return True
        
    except subprocess.CalledProcessError as e:
        execution_time = time.time() - start_time
        logger.error(f"❌ Ошибка в {script_name} (выполнялся {execution_time:.1f} сек):")
        logger.error(f"Return code: {e.returncode}")
        if e.stdout:
            logger.error(f"STDOUT (последние 10 строк):")
            for line in e.stdout.strip().split('\n')[-10:]:
                logger.error(f"  {line}")
        if e.stderr:
            logger.error(f"STDERR:")
            logger.error(f"  {e.stderr}")
        return False
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"❌ Неожиданная ошибка в {script_name} (выполнялся {execution_time:.1f} сек): {e}")
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
        'flight_program': ['program_id'],
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

def main(start_from_step=None, version_date=None):
    """Основной ETL пайплайн с централизованным управлением версиями"""
    logger.info("🎯 ===== ЗАПУСК ETL ПАЙПЛАЙНА HELICOPTER COMPONENT LIFECYCLE =====")
    logger.info("📋 Архитектура: централизованное управление версий + встроенные ID поля")
    
    if start_from_step:
        logger.info(f"🎯 Запуск с этапа: {start_from_step}")
    
    start_time = time.time()
    
    # 1. Инициализация версионного менеджера
    client = get_clickhouse_client_etl()
    if not client:
        logger.error("❌ Нет подключения к ClickHouse")
        return False
        
    version_manager = ETLVersionManager(client)
    
    # 2. Добавление поля version_id в существующие таблицы (если нужно)
    logger.info("🔧 ===== ПОДГОТОВКА СХЕМЫ ВЕРСИОНИРОВАНИЯ =====")
    if not version_manager.add_version_id_fields():
        logger.error("❌ Ошибка при добавлении полей version_id")
        return False
    
    # 3. Определение даты версии
    if version_date is None:
        version_date = date.today()
    
    logger.info(f"📅 Дата версии: {version_date}")
    
    # 4. Обработка политики версионирования
    logger.info("📋 ===== УПРАВЛЕНИЕ ВЕРСИЯМИ =====")
    policy, target_version_id = version_manager.handle_version_policy(version_date)
    
    if policy == 'cancel':
        logger.info("❌ ETL Pipeline отменен пользователем")
        return False
    
    # 5. Выполнение политики перезаписи (если выбрана)
    if policy == 'rewrite':
        if not version_manager.execute_rewrite_policy(version_date):
            logger.error("❌ Ошибка при выполнении политики перезаписи")
            return False
    
    logger.info(f"🎯 Целевая версия: version_date={version_date}, version_id={target_version_id}")
    logger.info("✅ Система готова для загрузки данных")
    
    # ETL последовательность
    etl_steps = [
        # ЭТАП 1: Базовые таблицы (источники данных) - ТОЛЬКО НЕЗАВИСИМЫЕ
        {
            'script': 'md_components_loader.py',
            'description': 'Загрузка MD компонентов (КРИТИЧНО ПЕРВЫМ для фильтрации)',
            'required_tables': None,
            'result_table': 'md_components'
        },
        
        {
            'script': 'status_overhaul_loader.py',
            'description': 'Загрузка данных статуса и капремонта',
            'required_tables': None,
            'result_table': 'status_overhaul'
        },
        
        {
            'script': 'program_loader.py',
            'description': 'Загрузка данных программ',
            'required_tables': None,
            'result_table': 'flight_program'
        },
        
        {
            'script': 'program_ac_loader.py',
            'description': 'Загрузка связки программ и ВС',
            'required_tables': ['flight_program'],
            'result_table': 'program_ac'
        },
        
        # ЭТАП 2: Основная таблица (зависит от всех базовых)
        {
            'script': 'dual_loader.py',
            'description': 'Загрузка Status_Components + процессоры статусов + repair_days',
            'required_tables': ['md_components', 'status_overhaul', 'program_ac'],
            'result_table': 'heli_pandas'
        },
        
        # ЭТАП 3: Справочники (зависят от heli_pandas)
        {
            'script': 'dictionary_creator.py',
            'description': 'Создание базовых справочников (после heli_pandas)',
            'required_tables': ['heli_pandas'],
            'result_table': 'dict_status_flat'
        },
        
        {
            'script': 'aircraft_number_dict_creator.py', 
            'description': 'Создание справочника номеров ВС (после heli_pandas)',
            'required_tables': ['heli_pandas'],
            'result_table': 'aircraft_number_dict'
        },
        
        # ЭТАП 4: Обогащение данных
        {
            'script': 'enrich_heli_pandas.py',
            'description': 'Обогащение данных ac_type_mask',
            'required_tables': ['heli_pandas'],
            'result_table': 'heli_pandas'
        },
        
        # ЭТАП 5: Расчеты
        {
            'script': 'calculate_beyond_repair.py',
            'description': 'Расчет Beyond Repair (br) для md_components',
            'required_tables': ['md_components'],
            'result_table': 'md_components'
        }
    ]
    
    # Выполнение ETL шагов
    success_count = 0
    skip_until_found = bool(start_from_step)
    
    for i, step in enumerate(etl_steps, 1):
        # Пропускаем этапы до нужного
        if skip_until_found:
            if step['script'] != start_from_step:
                logger.info(f"⏭️ Пропускаем этап {i}/{len(etl_steps)}: {step['script']}")
                continue
            else:
                skip_until_found = False
                logger.info(f"🎯 Начинаем с этапа {i}/{len(etl_steps)}: {step['script']}")
        
        logger.info(f"🚀 Этап {i}/{len(etl_steps)}: {step['script']}")
        
        success = run_script(
            step['script'], 
            step['description'],
            version_date,
            target_version_id,
            step.get('required_tables'),
            step.get('result_table')
        )
        
        if success:
            success_count += 1
            logger.info(f"✅ Этап {i}/{len(etl_steps)} завершен: {step['script']}")
        else:
            logger.error(f"❌ Критическая ошибка в {step['script']}")
            logger.info(f"⚠️ Продолжаем выполнение следующих этапов...")
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
    import argparse
    
    parser = argparse.ArgumentParser(description='ETL Pipeline Runner для Helicopter Component Lifecycle')
    parser.add_argument('--start-from', dest='start_from_step', 
                        help='Начать с определенного этапа (например: dual_loader.py)')
    
    args = parser.parse_args()
    
    success = main(start_from_step=args.start_from_step)
    sys.exit(0 if success else 1) 