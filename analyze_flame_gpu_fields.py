#!/usr/bin/env python3
"""
Анализ полей в Excel файле OLAP MultiBOM Flame GPU.xlsx
и сравнение с текущими таблицами базы данных
"""

import pandas as pd
import yaml
from clickhouse_driver import Client
from pathlib import Path
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Путь к файлу с данными
EXCEL_FILE = "data_input/analytics/OLAP MultiBOM Flame GPU.xlsx"

# Определяем структуру таблиц базы данных
DATABASE_TABLES = {
    'heli_pandas': {
        'partno': 'Nullable(String)',
        'serialno': 'Nullable(String)', 
        'ac_typ': 'Nullable(String)',
        'location': 'Nullable(String)',
        'mfg_date': 'Nullable(Date)',
        'removal_date': 'Nullable(Date)',
        'target_date': 'Nullable(Date)',
        'condition': 'Nullable(String)',
        'owner': 'Nullable(String)',
        'lease_restricted': 'UInt8',
        'oh': 'Nullable(UInt32)',
        'oh_threshold': 'Nullable(UInt32)',
        'll': 'Nullable(UInt32)',
        'sne': 'Nullable(UInt32)',
        'ppr': 'Nullable(UInt32)',
        'version_date': 'Date',
        'partseqno_i': 'Nullable(UInt32)',
        'psn': 'Nullable(UInt32)',
        'address_i': 'Nullable(UInt16)',
        'ac_type_i': 'Nullable(UInt16)',
        'status': 'UInt8',
        'repair_days': 'Nullable(Int16)',
        'aircraft_number': 'UInt16',
        'ac_type_mask': 'UInt8'
    },
    'md_components': {
        'partno': 'Nullable(String)',
        'comp_number': 'Nullable(Float64)',
        'group_by': 'Nullable(String)',
        'ac_typ': 'Nullable(String)',
        'type_restricted': 'Nullable(Float64)',
        'common_restricted1': 'Nullable(Float64)',
        'common_restricted2': 'Nullable(Float64)',
        'trigger_interval': 'Nullable(Float64)',
        'partout_time': 'Nullable(Float64)',
        'assembly_time': 'Nullable(Float64)',
        'repair_time': 'Nullable(Float64)',
        'll_mi8': 'Nullable(Float64)',
        'oh_mi8': 'Nullable(Float64)',
        'oh_threshold_mi8': 'Nullable(Float64)',
        'll_mi17': 'Nullable(Float64)',
        'oh_mi17': 'Nullable(Float64)',
        'repair_price': 'Nullable(Float64)',
        'purchase_price': 'Nullable(Float64)',
        'sne': 'Nullable(Float64)',
        'ppr': 'Nullable(Float64)',
        'version_date': 'Date',
        'partno_comp': 'Nullable(UInt32)'  # Добавлено enricher'ом
    },
    'flight_program': {
        'partno': 'Nullable(UInt8)',
        'serialno': 'Nullable(UInt32)',
        'ac_type': 'Nullable(String)',
        'field_type': 'String',
        'program_date': 'Date',
        'month_number': 'UInt8',
        'program_year': 'UInt16',
        'value': 'UInt32',
        'version_date': 'Date'
    },
    'program_ac': {
        'ac_registr': 'UInt32',
        'ac_typ': 'String',
        'object_type': 'String',
        'description': 'String',
        'owner': 'String',
        'operator': 'String',
        'homebase': 'String',
        'homebase_name': 'String',
        'directorate': 'String',
        'version_date': 'Date'
    },
    'status_overhaul': {
        'ac_registr': 'UInt32',
        'ac_typ': 'String',
        'wpno': 'String',
        'description': 'String',
        'sched_start_date': 'Nullable(Date)',
        'sched_end_date': 'Nullable(Date)',
        'act_start_date': 'Nullable(Date)',
        'act_end_date': 'Nullable(Date)',
        'status': 'String',
        'owner': 'String',
        'operator': 'String',
        'version_date': 'Date'
    },
    'OlapCube_Analytics': {
        # Архивная таблица - структура неизвестна из кода
        'unknown_fields': 'various'
    }
}

def load_config():
    """Загружает конфигурацию базы данных"""
    config_path = Path("config/database_config.yaml")
    if not config_path.exists():
        logger.warning("Файл конфигурации не найден, используем значения по умолчанию")
        return {
            'clickhouse': {
                'host': 'localhost',
                'port': 9000,
                'user': 'default',
                'password': '',
                'database': 'cube'
            }
        }
    
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def analyze_excel_file():
    """Анализирует Excel файл и извлекает информацию о полях"""
    try:
        logger.info(f"📊 Анализ файла: {EXCEL_FILE}")
        
        # Читаем Excel файл
        df = pd.read_excel(EXCEL_FILE, engine='openpyxl')
        
        logger.info(f"📋 Найдено столбцов: {len(df.columns)}")
        logger.info(f"📋 Найдено строк: {len(df)}")
        
        # Выводим информацию о столбцах
        print("\n" + "="*80)
        print("📊 СТРУКТУРА EXCEL ФАЙЛА")
        print("="*80)
        
        for i, col in enumerate(df.columns):
            print(f"{i+1:2d}. {col}")
        
        # Анализируем данные построчно
        print("\n" + "="*80)
        print("📊 АНАЛИЗ ДАННЫХ (первые 20 строк)")
        print("="*80)
        
        # Выбираем важные столбцы для анализа
        important_cols = ['Поле', 'Источник DWH или Flame GPU', 'cudf - Flame GPU TRANSFORM']
        
        for col in important_cols:
            if col in df.columns:
                print(f"\n🔍 Столбец: {col}")
                print("-" * 50)
                non_null_values = df[col].dropna().head(20)
                for idx, val in non_null_values.items():
                    print(f"  {idx+1:2d}: {val}")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Ошибка при анализе Excel файла: {e}")
        return None

def compare_with_database_tables(df):
    """Сравнивает поля из Excel с полями в таблицах базы данных"""
    if df is None:
        return
    
    print("\n" + "="*80)
    print("🔍 СРАВНЕНИЕ С ТАБЛИЦАМИ БАЗЫ ДАННЫХ")
    print("="*80)
    
    # Извлекаем поля из Excel (предполагаем, что они в столбце 'Поле')
    if 'Поле' in df.columns:
        excel_fields = set(df['Поле'].dropna().str.strip())
        print(f"📋 Поля из Excel: {len(excel_fields)}")
        
        # Собираем все поля из всех таблиц
        all_db_fields = set()
        for table_name, fields in DATABASE_TABLES.items():
            all_db_fields.update(fields.keys())
        
        print(f"📋 Поля в базе данных: {len(all_db_fields)}")
        
        # Находим недостающие поля
        missing_in_db = excel_fields - all_db_fields
        missing_in_excel = all_db_fields - excel_fields
        
        print(f"\n❌ Поля есть в Excel, но НЕТ в базе данных: {len(missing_in_db)}")
        for field in sorted(missing_in_db):
            print(f"  • {field}")
        
        print(f"\n❌ Поля есть в базе данных, но НЕТ в Excel: {len(missing_in_excel)}")
        for field in sorted(missing_in_excel):
            print(f"  • {field}")
        
        print(f"\n✅ Поля есть и в Excel, и в базе данных: {len(excel_fields & all_db_fields)}")
        for field in sorted(excel_fields & all_db_fields):
            print(f"  • {field}")
    
    # Анализируем источники данных
    if 'Источник DWH или Flame GPU' in df.columns:
        print(f"\n📊 АНАЛИЗ ИСТОЧНИКОВ ДАННЫХ")
        print("-" * 50)
        sources = df['Источник DWH или Flame GPU'].dropna().value_counts()
        for source, count in sources.items():
            print(f"  • {source}: {count} полей")
    
    # Анализируем TRANSFORM поля
    if 'cudf - Flame GPU TRANSFORM' in df.columns:
        print(f"\n🔄 АНАЛИЗ TRANSFORM ПОЛЕЙ")
        print("-" * 50)
        transforms = df['cudf - Flame GPU TRANSFORM'].dropna().value_counts()
        for transform, count in transforms.items():
            print(f"  • {transform}: {count} полей")

def analyze_field_by_table():
    """Анализирует поля по таблицам"""
    print("\n" + "="*80)
    print("📊 АНАЛИЗ ПОЛЕЙ ПО ТАБЛИЦАМ")
    print("="*80)
    
    for table_name, fields in DATABASE_TABLES.items():
        print(f"\n📋 Таблица: {table_name}")
        print("-" * 40)
        print(f"   Полей: {len(fields)}")
        
        # Группируем по типам данных
        type_groups = {}
        for field_name, field_type in fields.items():
            if field_type not in type_groups:
                type_groups[field_type] = []
            type_groups[field_type].append(field_name)
        
        for data_type, field_list in type_groups.items():
            print(f"   {data_type}: {len(field_list)} полей")
            for field in field_list:
                print(f"     • {field}")

def main():
    """Основная функция"""
    logger.info("🚀 Запуск анализа полей Flame GPU")
    
    # Проверяем существование файла
    if not Path(EXCEL_FILE).exists():
        logger.error(f"❌ Файл {EXCEL_FILE} не найден!")
        return
    
    # Анализируем Excel файл
    df = analyze_excel_file()
    
    # Сравниваем с базой данных
    compare_with_database_tables(df)
    
    # Анализируем поля по таблицам
    analyze_field_by_table()
    
    logger.info("✅ Анализ завершен")

if __name__ == "__main__":
    main() 