#!/usr/bin/env python3
"""
Умная загрузка справочников MD_Components.xlsx и MD_Dictionary.xlsx в ClickHouse
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
from typing import Tuple, List, Dict
from pathlib import Path

def setup_logging():
    """Настройка детального логирования"""
    os.makedirs('test_output', exist_ok=True)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    logger = logging.getLogger('master_data_loader')
    logger.setLevel(logging.DEBUG)
    
    # Убираем существующие обработчики
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Файловый обработчик
    file_handler = logging.FileHandler('test_output/master_data_loader.log', mode='w')
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

def create_md_components_table(client, logger):
    """Создание таблицы для MD_Components"""
    table_name = "md_components_master"
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS md_components_master (
        -- Основные поля компонента
        `partno` LowCardinality(String),               -- Чертежный номер (партномер)
        `component_name` LowCardinality(String),       -- Название агрегата
        `qty_per_aircraft` Nullable(UInt8),            -- Количество на ВС
        `group_by` Nullable(UInt8),                    -- Группировка оборота  
        `effectivity_type` Nullable(UInt16),           -- Битовая маска применимости
        `type_restricted` Nullable(UInt8),             -- Запрет на смену типа ВС
        
        -- Метаданные загрузки
        `version_date` Date DEFAULT today(),            -- Дата версии справочника
        `load_timestamp` DateTime DEFAULT now()         -- Время загрузки
        
    ) ENGINE = ReplacingMergeTree(load_timestamp)
    ORDER BY (partno, version_date)
    PARTITION BY toYYYYMM(version_date)
    SETTINGS index_granularity = 8192
    """
    
    try:
        client.execute(create_table_sql)
        logger.info(f"✅ Таблица {table_name} создана/обновлена")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка создания таблицы {table_name}: {e}")
        return False

def create_md_dictionary_table(client, logger):
    """Создание таблицы для MD_Dictionary"""
    table_name = "md_dictionary_master"
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS md_dictionary_master (
        -- Основные поля словаря
        `category` LowCardinality(String),              -- Категория словаря
        `code` LowCardinality(String),                  -- Код элемента
        `name` LowCardinality(String),                  -- Название элемента
        `description` Nullable(String),                 -- Описание
        `sort_order` Nullable(UInt16),                  -- Порядок сортировки
        `is_active` UInt8 DEFAULT 1,                   -- Активность записи
        
        -- Метаданные загрузки  
        `version_date` Date DEFAULT today(),            -- Дата версии справочника
        `load_timestamp` DateTime DEFAULT now()         -- Время загрузки
        
    ) ENGINE = ReplacingMergeTree(load_timestamp)
    ORDER BY (category, code, version_date)
    PARTITION BY toYYYYMM(version_date)
    SETTINGS index_granularity = 8192
    """
    
    try:
        client.execute(create_table_sql)
        logger.info(f"✅ Таблица {table_name} создана/обновлена")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка создания таблицы {table_name}: {e}")
        return False

def extract_version_date_from_excel(file_path, logger):
    """Извлечение даты версии из метаданных Excel файла"""
    try:
        # Получаем дату модификации файла
        mod_time = os.path.getmtime(file_path)
        version_date = datetime.fromtimestamp(mod_time).date()
        
        logger.info(f"📅 Дата версии файла {file_path}: {version_date}")
        return version_date
        
    except Exception as e:
        logger.warning(f"⚠️ Не удалось извлечь дату версии из {file_path}: {e}")
        return date.today()

def load_md_components_excel(file_path, logger):
    """Загрузка данных из MD_Components.xlsx"""
    try:
        logger.info(f"📂 Загружаю MD_Components из {file_path}")
        
        # Читаем с русскими заголовками из строки 8
        df = pd.read_excel(
            file_path,
            sheet_name='Агрегаты',
            header=7,  # Русские заголовки
            engine='openpyxl'
        )
        
        # Маппинг русских названий на английские
        column_mapping = {
            'Агрегат': 'component_name',
            'Чертежный номер': 'partno',
            'Количество установленных на ВС': 'qty_per_aircraft',
            'Группировка оборота': 'group_by',
            'Применимость к типу ВС': 'effectivity_type',
            'Запрет на смену типа ВС': 'type_restricted'
        }
        
        # Переименовываем столбцы
        df_renamed = df.rename(columns=column_mapping)
        
        # Фильтруем только нужные столбцы
        required_columns = [
            'partno', 'component_name', 'qty_per_aircraft', 
            'group_by', 'effectivity_type', 'type_restricted'
        ]
        
        available_columns = [col for col in required_columns if col in df_renamed.columns]
        df_filtered = df_renamed[available_columns].copy()
        
        # Очищаем от пустых строк
        df_filtered = df_filtered.dropna(subset=['partno'])
        
        # Конвертируем битовые маски из строк в числа
        if 'effectivity_type' in df_filtered.columns:
            def convert_binary_mask(mask):
                if isinstance(mask, str) and mask.startswith('0b'):
                    try:
                        return int(mask, 2)
                    except ValueError:
                        return None
                elif pd.isna(mask):
                    return None
                else:
                    return mask
            
            df_filtered['effectivity_type'] = df_filtered['effectivity_type'].apply(convert_binary_mask)
        
        logger.info(f"✅ Загружено {len(df_filtered)} компонентов из MD_Components")
        logger.info(f"📋 Столбцы: {list(df_filtered.columns)}")
        
        return df_filtered
        
    except Exception as e:
        logger.error(f"❌ Ошибка при загрузке MD_Components: {e}")
        raise

def load_md_dictionary_excel(file_path, logger):
    """Загрузка данных из MD_Dictionary.xlsx"""
    try:
        logger.info(f"📂 Загружаю MD_Dictionary из {file_path}")
        
        # Сначала посмотрим какие листы есть в файле
        excel_file = pd.ExcelFile(file_path, engine='openpyxl')
        sheet_names = excel_file.sheet_names
        logger.info(f"📋 Найденные листы в MD_Dictionary: {sheet_names}")
        
        all_dictionary_data = []
        
        # Читаем все листы как категории словаря
        for sheet_name in sheet_names:
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
                
                # Пропускаем пустые листы
                if df.empty:
                    continue
                
                # Добавляем информацию о категории
                df['category'] = sheet_name
                
                # Стандартизируем названия столбцов
                if len(df.columns) >= 2:
                    # Предполагаем что первый столбец - код, второй - название
                    df_dict = pd.DataFrame({
                        'category': sheet_name,
                        'code': df.iloc[:, 0].astype(str),
                        'name': df.iloc[:, 1].astype(str) if len(df.columns) > 1 else '',
                        'description': df.iloc[:, 2].astype(str) if len(df.columns) > 2 else '',
                        'sort_order': range(1, len(df) + 1)
                    })
                    
                    # Убираем пустые записи
                    df_dict = df_dict.dropna(subset=['code', 'name'])
                    df_dict = df_dict[df_dict['code'].str.strip() != '']
                    df_dict = df_dict[df_dict['name'].str.strip() != '']
                    
                    all_dictionary_data.append(df_dict)
                    logger.info(f"  📄 Лист '{sheet_name}': {len(df_dict)} записей")
                    
            except Exception as e:
                logger.warning(f"⚠️ Ошибка обработки листа '{sheet_name}': {e}")
                continue
        
        if all_dictionary_data:
            # Объединяем все данные
            df_combined = pd.concat(all_dictionary_data, ignore_index=True)
            logger.info(f"✅ Загружено {len(df_combined)} записей словаря из {len(all_dictionary_data)} категорий")
            return df_combined
        else:
            logger.warning("⚠️ Не удалось загрузить данные словаря")
            return pd.DataFrame()
        
    except Exception as e:
        logger.error(f"❌ Ошибка при загрузке MD_Dictionary: {e}")
        raise

def check_version_conflict(client, table_name, version_date, logger):
    """Проверка конфликта версий данных"""
    try:
        result = client.execute(f"""
            SELECT COUNT(*) FROM {table_name} 
            WHERE version_date = '{version_date}'
        """)
        
        existing_count = result[0][0]
        
        if existing_count > 0:
            logger.warning(f"⚠️ Конфликт версий в {table_name}!")
            logger.warning(f"   Данные за {version_date} уже существуют ({existing_count:,} записей)")
            return True
        else:
            logger.info(f"✅ Версия {version_date} свободна для загрузки в {table_name}")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка проверки версий для {table_name}: {e}")
        return False

def load_data_to_clickhouse(client, table_name, df, version_date, logger):
    """Загрузка данных в ClickHouse"""
    try:
        if df.empty:
            logger.warning(f"⚠️ Нет данных для загрузки в {table_name}")
            return True
        
        # Добавляем метаданные
        df['version_date'] = version_date
        df['load_timestamp'] = datetime.now()
        
        # Подготавливаем данные для вставки
        data_to_insert = []
        for _, row in df.iterrows():
            data_to_insert.append(tuple(row))
        
        # Вставляем данные
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        client.execute(insert_sql, data_to_insert)
        logger.info(f"✅ Загружено {len(data_to_insert)} записей в {table_name}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки в {table_name}: {e}")
        return False

def handle_version_conflict(client, table_name, version_date, logger):
    """Обработка конфликта версий с автоматической заменой"""
    try:
        # В режиме автоматической замены удаляем старые данные
        logger.info(f"🔄 Заменяю существующие данные в {table_name} за {version_date}")
        
        delete_sql = f"ALTER TABLE {table_name} DELETE WHERE version_date = '{version_date}'"
        client.execute(delete_sql)
        
        # Ждем пока операция завершится
        time.sleep(2)
        
        logger.info(f"✅ Старые данные за {version_date} удалены из {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка при замене данных в {table_name}: {e}")
        return False

def main():
    """Главная функция"""
    print("📚 Загрузка справочников MD_Components и MD_Dictionary в ClickHouse")
    print("=" * 70)
    
    logger = setup_logging()
    
    try:
        # Загружаем конфигурацию
        config = load_config()
        logger.info(f"✅ Конфиг загружен: {config['host']}:{config['port']}/{config['database']}")
        
        # Подключаемся к ClickHouse
        client = connect_clickhouse(config, logger)
        
        # Пути к файлам справочников
        md_components_path = "data_input/master_data/MD_Сomponents.xlsx"
        md_dictionary_path = "data_input/master_data/MD_Dictionary.xlsx"
        
        success_count = 0
        total_count = 2
        
        # 1. Загружаем MD_Components
        logger.info(f"\n🚀 1/2: Загрузка MD_Components...")
        try:
            if os.path.exists(md_components_path):
                # Создаем таблицу
                create_md_components_table(client, logger)
                
                # Загружаем данные из Excel
                df_components = load_md_components_excel(md_components_path, logger)
                version_date = extract_version_date_from_excel(md_components_path, logger)
                
                # Проверяем конфликт версий и заменяем при необходимости
                if check_version_conflict(client, "md_components_master", version_date, logger):
                    handle_version_conflict(client, "md_components_master", version_date, logger)
                
                # Загружаем в ClickHouse
                if load_data_to_clickhouse(client, "md_components_master", df_components, version_date, logger):
                    success_count += 1
                    logger.info("✅ MD_Components успешно загружен!")
                else:
                    logger.error("❌ Ошибка загрузки MD_Components")
            else:
                logger.error(f"❌ Файл не найден: {md_components_path}")
                
        except Exception as e:
            logger.error(f"❌ Критическая ошибка с MD_Components: {e}")
        
        # 2. Загружаем MD_Dictionary
        logger.info(f"\n🚀 2/2: Загрузка MD_Dictionary...")
        try:
            if os.path.exists(md_dictionary_path):
                # Создаем таблицу
                create_md_dictionary_table(client, logger)
                
                # Загружаем данные из Excel
                df_dictionary = load_md_dictionary_excel(md_dictionary_path, logger)
                version_date = extract_version_date_from_excel(md_dictionary_path, logger)
                
                # Проверяем конфликт версий и заменяем при необходимости
                if check_version_conflict(client, "md_dictionary_master", version_date, logger):
                    handle_version_conflict(client, "md_dictionary_master", version_date, logger)
                
                # Загружаем в ClickHouse
                if load_data_to_clickhouse(client, "md_dictionary_master", df_dictionary, version_date, logger):
                    success_count += 1
                    logger.info("✅ MD_Dictionary успешно загружен!")
                else:
                    logger.error("❌ Ошибка загрузки MD_Dictionary")
            else:
                logger.error(f"❌ Файл не найден: {md_dictionary_path}")
                
        except Exception as e:
            logger.error(f"❌ Критическая ошибка с MD_Dictionary: {e}")
        
        # Итоговая статистика
        logger.info(f"\n📊 РЕЗУЛЬТАТ ЗАГРУЗКИ СПРАВОЧНИКОВ:")
        logger.info(f"   ✅ Успешно загружено: {success_count}/{total_count}")
        logger.info(f"   📊 Процент успеха: {success_count/total_count*100:.1f}%")
        
        if success_count == total_count:
            print(f"\n🎉 ВСЕ СПРАВОЧНИКИ УСПЕШНО ЗАГРУЖЕНЫ!")
            return 0
        elif success_count > 0:
            print(f"\n⚠️ Частичный успех: {success_count}/{total_count} справочников загружено")
            return 1
        else:
            print(f"\n❌ НЕ УДАЛОСЬ ЗАГРУЗИТЬ НИ ОДНОГО СПРАВОЧНИКА")
            return 2
            
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return 3
    finally:
        try:
            client.disconnect()
            logger.info("🔌 Соединение с ClickHouse закрыто")
        except:
            pass

if __name__ == "__main__":
    exit(main()) 