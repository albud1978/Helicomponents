#!/usr/bin/env python3
"""
Умная загрузка Status_Components.xlsx в RAW слой ClickHouse
С защитой от дублирования и версионным управлением данных
"""
import os
import sys
import pandas as pd
import numpy as np
from clickhouse_driver import Client
import logging
from datetime import datetime, date
import yaml
import time
import traceback
from typing import Tuple, List
from pathlib import Path

def setup_logging():
    """Настройка детального логирования"""
    os.makedirs('test_output', exist_ok=True)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    logger = logging.getLogger('smart_loader')
    logger.setLevel(logging.DEBUG)
    
    # Убираем существующие обработчики
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Файловый обработчик
    file_handler = logging.FileHandler('test_output/smart_loader.log', mode='w')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Консольный обработчик
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def load_config():
    """Загрузка конфигурации из YAML"""
    # Используем универсальную функцию загрузки
    sys.path.append(str(Path(__file__).parent / 'utils'))
    from config_loader import load_database_config
    return load_database_config()

def connect_clickhouse(config, logger):
    """Подключение к ClickHouse с retry логикой"""
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            client = Client(
                host=config['host'],
                port=config['port'],
                user=config['user'],
                password=config['password'],
                database=config['database'],
                settings={'strings_encoding': 'utf-8', 'max_threads': 8}
            )
            # Тест подключения
            result = client.execute('SELECT 1 as test')
            if result[0][0] == 1:
                logger.info(f"✓ Подключение к ClickHouse установлено (попытка {attempt + 1})")
                return client
        except Exception as e:
            logger.warning(f"Попытка {attempt + 1} неудачна: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise ConnectionError(f"Не удалось подключиться к ClickHouse после {max_retries} попыток: {e}")

def check_table_exists(client, table_name, logger):
    """Проверка существования таблицы"""
    try:
        result = client.execute(f"EXISTS TABLE {table_name}")
        exists = result[0][0] == 1
        if exists:
            logger.info(f"📋 Таблица {table_name} существует")
            
            # Получаем статистику существующих данных
            stats = client.execute(f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT version_date) as unique_dates,
                    MIN(version_date) as min_date,
                    MAX(version_date) as max_date
                FROM {table_name}
            """)
            
            if stats and stats[0][0] > 0:
                row = stats[0]
                logger.info(f"📊 Существующие данные:")
                logger.info(f"  📄 Всего записей: {row[0]:,}")
                logger.info(f"  📅 Версий данных: {row[1]}")
                logger.info(f"  📆 Период: {row[2]} - {row[3]}")
            else:
                logger.info("📭 Таблица пуста")
        else:
            logger.info(f"❌ Таблица {table_name} не существует")
        
        return exists
    except Exception as e:
        logger.error(f"Ошибка при проверке таблицы: {e}")
        return False

def create_table_if_not_exists(client, table_name, logger):
    """Создание таблицы только если она не существует"""
    if check_table_exists(client, table_name, logger):
        return True
    
    logger.info(f"🔧 Создание таблицы {table_name}...")
    
    create_table_sql = """
    CREATE TABLE Status_Components_raw (
        -- Основные идентификаторы
        `partno` Nullable(String),              -- Партномер (чертежный номер)
        `serialno` Nullable(String),            -- Серийный номер (как строка)
        `ac_typ` Nullable(String),              -- Тип воздушного судна
        `location` Nullable(String),            -- Местоположение/позиция
        
        -- Даты
        `mfg_date` Nullable(Date),              -- Дата изготовления
        `removal_date` Nullable(Date),          -- Дата снятия
        `target_date` Nullable(Date),           -- Целевая дата
        
        -- Состояние и владение
        `condition` Nullable(String),           -- Состояние компонента
        `owner` Nullable(String),               -- Владелец
        `lease_restricted` Nullable(String),    -- Ограничения по лизингу
        
        -- Ресурсные данные
        `oh` Nullable(Float32),                 -- МРР агрегата (межремонтный ресурс)
        `oh_threshold` Nullable(Float32),       -- Порог МРР
        `ll` Nullable(Float32),                 -- НР агрегата (назначенный ресурс)
        `sne` Nullable(Float32),                -- Наработка с начала эксплуатации
        `ppr` Nullable(Float32),                -- Наработка после последнего ремонта
        
        -- Метаданные файла
        `version_date` Date DEFAULT today()     -- Дата версии файла из метаданных Excel
        
    ) ENGINE = MergeTree()
    ORDER BY version_date
    PARTITION BY toYYYYMM(version_date)
    SETTINGS index_granularity = 8192
    """
    
    try:
        client.execute(create_table_sql)
        logger.info(f"✅ Таблица {table_name} создана успешно!")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка создания таблицы: {e}")
        return False

def check_version_conflict(client, table_name, version_date, logger):
    """Проверка конфликта версий данных"""
    try:
        result = client.execute(f"""
            SELECT COUNT(*) FROM {table_name} 
            WHERE version_date = '{version_date}'
        """)
        
        existing_count = result[0][0]
        
        if existing_count > 0:
            logger.warning(f"⚠️  Обнаружен конфликт версий!")
            logger.warning(f"   Данные за {version_date} уже существуют ({existing_count:,} записей)")
            return True
        else:
            logger.info(f"✅ Версия {version_date} свободна для загрузки")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка проверки версий: {e}")
        return False

def ask_user_for_action(version_date, existing_count):
    """Интерактивный запрос действий пользователя при конфликте версий"""
    print(f"\n🚨 КОНФЛИКТ ВЕРСИЙ ДАННЫХ!")
    print(f"   Дата версии: {version_date}")
    print(f"   Существующих записей: {existing_count:,}")
    print(f"\nВыберите действие:")
    print(f"   1. ЗАМЕНИТЬ существующие данные")
    print(f"   2. ОТМЕНИТЬ загрузку")
    print(f"   3. ПРИНУДИТЕЛЬНО добавить (может создать дубликаты)")
    
    while True:
        try:
            choice = input(f"\nВаш выбор (1-3): ").strip()
            
            if choice == '1':
                return 'replace'
            elif choice == '2':
                return 'cancel'
            elif choice == '3':
                return 'force'
            else:
                print("❌ Неверный выбор. Введите 1, 2 или 3.")
        except KeyboardInterrupt:
            print(f"\n❌ Загрузка отменена пользователем")
            return 'cancel'
        except Exception as e:
            print(f"❌ Ошибка ввода: {e}")

def handle_version_conflict(client, table_name, version_date, logger):
    """Обработка конфликта версий"""
    
    # Получаем количество существующих записей
    result = client.execute(f"""
        SELECT COUNT(*) FROM {table_name} 
        WHERE version_date = '{version_date}'
    """)
    existing_count = result[0][0]
    
    # Спрашиваем пользователя
    action = ask_user_for_action(version_date, existing_count)
    
    if action == 'replace':
        logger.info(f"🔄 Замена существующих данных за {version_date}...")
        client.execute(f"""
            DELETE FROM {table_name} 
            WHERE version_date = '{version_date}'
        """)
        logger.info(f"✅ Удалено {existing_count:,} существующих записей")
        return True
        
    elif action == 'cancel':
        logger.info(f"❌ Загрузка отменена пользователем")
        return False
        
    elif action == 'force':
        logger.warning(f"⚠️  Принудительная загрузка (возможны дубликаты)")
        return True
        
    else:
        logger.error(f"❌ Неизвестное действие: {action}")
        return False

def extract_version_date_from_excel(file_path, logger):
    """Извлечение даты версии из метаданных Excel файла"""
    try:
        import openpyxl
        from datetime import datetime
        
        # Открываем файл для чтения метаданных
        workbook = openpyxl.load_workbook(file_path, read_only=True)
        props = workbook.properties
        
        version_date = None
        
        if props.created:
            version_date = props.created.date()
            logger.info(f"📅 Дата версии из свойства 'created': {version_date}")
        elif props.modified:
            version_date = props.modified.date()
            logger.info(f"📅 Дата версии из свойства 'modified': {version_date}")
        else:
            # Если метаданные недоступны, используем дату файла
            import os
            file_mtime = os.path.getmtime(file_path)
            version_date = datetime.fromtimestamp(file_mtime).date()
            logger.info(f"📅 Дата версии из времени модификации файла: {version_date}")
        
        workbook.close()
        return version_date
        
    except Exception as e:
        logger.warning(f"Не удалось извлечь дату версии из метаданных: {e}")
        # Fallback к дате файла
        import os
        file_mtime = os.path.getmtime(file_path)
        version_date = datetime.fromtimestamp(file_mtime).date()
        logger.info(f"📅 Используем дату модификации файла: {version_date}")
        return version_date

def load_excel_data(file_path, config, logger):
    """Загрузка данных из Excel файла с правильным парсингом"""
    logger.info(f"📖 Загрузка Excel файла: {file_path}")
    
    try:
        # Загружаем файл с учетом конфигурации
        header_row = config.get('excel_settings', {}).get('header_row', 8)
        
        # Сначала читаем заголовки
        headers_df = pd.read_excel(file_path, header=header_row, nrows=0)
        logger.info(f"📋 Найдено столбцов: {len(headers_df.columns)}")
        logger.debug(f"Заголовки: {list(headers_df.columns)}")
        
        # Читаем данные, пропуская заголовки
        df = pd.read_excel(file_path, header=header_row, skiprows=0)
        
        logger.info(f"📊 Загружено строк: {len(df):,}")
        logger.info(f"📊 Столбцов: {len(df.columns)}")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки Excel: {e}")
        raise

def prepare_data_for_clickhouse(df, version_date, logger):
    """Подготовка данных для загрузки в ClickHouse"""
    logger.info("🔧 Подготовка данных для ClickHouse...")
    
    # Создаем копию DataFrame
    result_df = df.copy()
    
    # Mapping колонок Excel -> ClickHouse (колонки уже в правильном формате)
    # В файле Excel заголовки уже в нужном формате, не требуют переименования
    target_columns = [
        'partno', 'serialno', 'ac_typ', 'location',
        'mfg_date', 'removal_date', 'target_date', 
        'condition', 'owner', 'lease_restricted',
        'oh', 'oh_threshold', 'll', 'sne', 'ppr'
    ]
    
    # Логируем доступные колонки для отладки
    logger.info(f"🔍 Доступные колонки в Excel: {list(df.columns)}")
    
    # Проверяем какие целевые колонки доступны в файле
    available_target_columns = [col for col in target_columns if col in result_df.columns]
    logger.info(f"📋 Доступные целевые колонки: {available_target_columns}")
    
    # Оставляем только нужные колонки (которые есть)
    result_df = result_df[available_target_columns]
    
    # Добавляем version_date
    result_df['version_date'] = version_date
    
    # Обработка дат
    date_columns = ['mfg_date', 'removal_date', 'target_date']
    for col in date_columns:
        if col in result_df.columns:
            result_df[col] = pd.to_datetime(result_df[col], dayfirst=True, errors='coerce')
    
    # Обработка числовых полей
    numeric_columns = ['oh', 'oh_threshold', 'll', 'sne', 'ppr']
    for col in numeric_columns:
        if col in result_df.columns:
            result_df[col] = pd.to_numeric(result_df[col], errors='coerce')
    
    # Обработка NaN значений для ClickHouse
    result_df = result_df.replace({np.nan: None, pd.NaT: None})
    
    logger.info(f"✅ Подготовлено {len(result_df):,} записей")
    return result_df

def create_batches(df, batch_size, logger):
    """Разбивка DataFrame на батчи"""
    total_rows = len(df)
    batches = []
    
    for i in range(0, total_rows, batch_size):
        batch = df.iloc[i:i + batch_size]
        batches.append(batch)
    
    logger.info(f"📦 Создано {len(batches)} батчей по {batch_size:,} записей")
    return batches

def load_batch_to_clickhouse(client, table_name, batch_df, batch_num, total_batches, logger):
    """Загрузка одного батча в ClickHouse"""
    start_time = time.time()
    
    try:
        # Конвертируем DataFrame в список кортежей
        data_tuples = [tuple(row) for row in batch_df.values]
        
        # Выполняем INSERT
        client.execute(f'INSERT INTO {table_name} VALUES', data_tuples)
        
        batch_time = time.time() - start_time
        rows_per_sec = len(data_tuples) / batch_time if batch_time > 0 else 0
        
        logger.info(f"✅ Батч {batch_num}/{total_batches}: {len(data_tuples):,} записей за {batch_time:.2f}с ({rows_per_sec:,.0f} записей/сек)")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки батча {batch_num}: {e}")
        return False

def main():
    """Основная функция умной загрузки"""
    logger = setup_logging()
    total_start_time = time.time()
    
    try:
        logger.info("🚀 === УМНАЯ ЗАГРУЗКА STATUS_COMPONENTS В RAW СЛОЙ ===")
        
        # Параметры
        TABLE_NAME = 'Status_Components_raw'
        EXCEL_FILE = 'data_input/source_data/Status_Components.xlsx'
        
        # 1. Загрузка конфигурации
        config = load_config()
        logger.info(f"⚙️  Конфигурация: {config['host']}:{config['port']}/{config['database']}")
        
        # 2. Подключение к ClickHouse
        client = connect_clickhouse(config, logger)
        
        # 3. Проверка/создание таблицы
        if not create_table_if_not_exists(client, TABLE_NAME, logger):
            raise Exception("Не удалось создать таблицу")
        
        # 4. Определение версии данных
        version_date = extract_version_date_from_excel(EXCEL_FILE, logger)
        logger.info(f"🗓️  Версия данных для загрузки: {version_date}")
        
        # 5. Проверка конфликта версий
        if check_version_conflict(client, TABLE_NAME, version_date, logger):
            if not handle_version_conflict(client, TABLE_NAME, version_date, logger):
                logger.info("❌ Загрузка остановлена")
                return
        
        # 6. Загрузка и подготовка данных
        logger.info(f"📥 Начинаем загрузку файла {EXCEL_FILE}...")
        
        # Загружаем Excel  
        full_config = {**config, 'excel_settings': {'header_row': 0}}  # 0-based (строка 1 с заголовками)
        df = load_excel_data(EXCEL_FILE, full_config, logger)
        
        # Подготавливаем данные
        prepared_df = prepare_data_for_clickhouse(df, version_date, logger)
        
        # Создаем батчи
        batch_size = 5000
        batches = create_batches(prepared_df, batch_size, logger)
        
        # 7. Загружаем батчи
        logger.info(f"🚀 Начинаем загрузку {len(batches)} батчей...")
        
        success_count = 0
        total_records = 0
        
        for i, batch in enumerate(batches, 1):
            if load_batch_to_clickhouse(client, TABLE_NAME, batch, i, len(batches), logger):
                success_count += 1
                total_records += len(batch)
            else:
                logger.error(f"❌ Критическая ошибка в батче {i}, останавливаем загрузку")
                break
        
        # 8. Финальная статистика
        total_time = time.time() - total_start_time
        
        if success_count == len(batches):
            logger.info(f"🎉 === ЗАГРУЗКА ЗАВЕРШЕНА УСПЕШНО ===")
            logger.info(f"✅ Обработано батчей: {success_count}/{len(batches)}")
            logger.info(f"✅ Загружено записей: {total_records:,}")
            logger.info(f"⏱️  Общее время: {total_time:.2f} секунд")
            logger.info(f"⚡ Производительность: {total_records/total_time:,.0f} записей/сек")
            logger.info(f"📅 Версия данных: {version_date}")
            
            # Проверяем результат в таблице
            final_count = client.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE version_date = '{version_date}'")[0][0]
            logger.info(f"🔍 Проверка в БД: {final_count:,} записей с версией {version_date}")
            
        else:
            logger.error(f"💥 Загрузка завершена с ошибками!")
            logger.error(f"❌ Успешных батчей: {success_count}/{len(batches)}")
        
    except Exception as e:
        total_time = time.time() - total_start_time
        logger.error(f"💥 Критическая ошибка после {total_time:.2f} сек: {e}")
        logger.error("Трассировка ошибки:")
        logger.error(traceback.format_exc())
        raise

if __name__ == '__main__':
    main() 