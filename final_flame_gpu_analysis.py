#!/usr/bin/env python3
"""
Финальный анализ Excel файла OLAP MultiBOM Flame GPU.xlsx
с правильными заголовками и сопоставлением с базой данных
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
        'partno_comp': 'Nullable(UInt32)'
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
    'dict_partno_flat': {
        'partseqno_i': 'UInt32',
        'partno': 'String'
    },
    'dict_serialno_flat': {
        'psn': 'UInt32',
        'serialno': 'String'
    },
    'dict_owner_flat': {
        'address_i': 'UInt32',
        'owner': 'String'
    },
    'dict_ac_type_flat': {
        'ac_type_mask': 'UInt8',
        'ac_typ': 'String'
    }
}

def analyze_flame_gpu_excel():
    """Анализирует Excel файл с правильными заголовками"""
    
    logger.info(f"📊 Анализ файла: {EXCEL_FILE}")
    
    # Читаем Excel файл с заголовками на строке 4 (индекс 4)
    df = pd.read_excel(EXCEL_FILE, header=4, engine='openpyxl')
    
    # Переименовываем столбцы для удобства
    df.columns = [
        'Unnamed_0', 'Описание', 'Единицы', 'Поле', 'Объект_Flame_GPU',
        'Источник_DWH_или_Flame_GPU', 'Роль_в_Flame_GPU', 'cudf_Flame_GPU_TRANSFORM',
        'DWH_OLAP_CH_MergeTree_LOAD', 'bitmask', 'Примечание'
    ]
    
    # Удаляем пустые строки
    df = df.dropna(how='all')
    
    logger.info(f"📋 Обработано строк: {len(df)}")
    
    print("\n" + "="*100)
    print("📊 АНАЛИЗ ПОЛЕЙ ИЗ EXCEL")
    print("="*100)
    
    # Анализируем поля
    fields_data = []
    
    for idx, row in df.iterrows():
        if pd.notna(row['Поле']) and str(row['Поле']).strip():
            field_info = {
                'field_name': str(row['Поле']).strip(),
                'description': str(row['Описание']).strip() if pd.notna(row['Описание']) else '',
                'units': str(row['Единицы']).strip() if pd.notna(row['Единицы']) else '',
                'flame_gpu_object': str(row['Объект_Flame_GPU']).strip() if pd.notna(row['Объект_Flame_GPU']) else '',
                'source': str(row['Источник_DWH_или_Flame_GPU']).strip() if pd.notna(row['Источник_DWH_или_Flame_GPU']) else '',
                'role': str(row['Роль_в_Flame_GPU']).strip() if pd.notna(row['Роль_в_Flame_GPU']) else '',
                'transform': str(row['cudf_Flame_GPU_TRANSFORM']).strip() if pd.notna(row['cudf_Flame_GPU_TRANSFORM']) else '',
                'load': str(row['DWH_OLAP_CH_MergeTree_LOAD']).strip() if pd.notna(row['DWH_OLAP_CH_MergeTree_LOAD']) else '',
                'bitmask': str(row['bitmask']).strip() if pd.notna(row['bitmask']) else '',
                'note': str(row['Примечание']).strip() if pd.notna(row['Примечание']) else ''
            }
            fields_data.append(field_info)
    
    # Выводим все поля с их характеристиками
    for i, field in enumerate(fields_data, 1):
        print(f"\n{i:2d}. 🔍 {field['field_name']}")
        print(f"    📝 Описание: {field['description']}")
        if field['units']:
            print(f"    📏 Единицы: {field['units']}")
        if field['source']:
            print(f"    🗄️ Источник: {field['source']}")
        if field['transform']:
            print(f"    🔄 TRANSFORM: {field['transform']}")
        if field['load']:
            print(f"    💾 LOAD: {field['load']}")
        if field['bitmask']:
            print(f"    🔢 Bitmask: {field['bitmask']}")
        if field['note']:
            print(f"    📋 Примечание: {field['note'][:100]}{'...' if len(field['note']) > 100 else ''}")
    
    return fields_data

def compare_with_database(fields_data):
    """Сравнивает поля из Excel с полями в базе данных"""
    
    print("\n" + "="*100)
    print("🔍 СРАВНЕНИЕ С БАЗОЙ ДАННЫХ")
    print("="*100)
    
    # Извлекаем поля из Excel
    excel_fields = {field['field_name'] for field in fields_data}
    print(f"📋 Поля из Excel: {len(excel_fields)}")
    
    # Собираем все поля из всех таблиц
    all_db_fields = set()
    db_fields_by_table = {}
    
    for table_name, fields in DATABASE_TABLES.items():
        db_fields_by_table[table_name] = set(fields.keys())
        all_db_fields.update(fields.keys())
    
    print(f"📋 Поля в базе данных: {len(all_db_fields)}")
    
    # Находим пересечения и различия
    missing_in_db = excel_fields - all_db_fields
    missing_in_excel = all_db_fields - excel_fields
    common_fields = excel_fields & all_db_fields
    
    print(f"\n✅ Поля есть и в Excel, и в базе данных: {len(common_fields)}")
    for field in sorted(common_fields):
        # Находим в какой таблице есть поле
        tables_with_field = [table for table, fields in db_fields_by_table.items() if field in fields]
        excel_field_data = next((f for f in fields_data if f['field_name'] == field), None)
        
        print(f"  • {field}")
        print(f"    📊 Таблицы БД: {', '.join(tables_with_field)}")
        if excel_field_data:
            if excel_field_data['source']:
                print(f"    🗄️ Источник: {excel_field_data['source']}")
            if excel_field_data['transform']:
                print(f"    🔄 TRANSFORM: {excel_field_data['transform']}")
        print()
    
    print(f"\n❌ Поля есть в Excel, но НЕТ в базе данных: {len(missing_in_db)}")
    for field in sorted(missing_in_db):
        excel_field_data = next((f for f in fields_data if f['field_name'] == field), None)
        print(f"  • {field}")
        if excel_field_data:
            if excel_field_data['description']:
                print(f"    📝 Описание: {excel_field_data['description']}")
            if excel_field_data['source']:
                print(f"    🗄️ Источник: {excel_field_data['source']}")
            if excel_field_data['transform']:
                print(f"    🔄 TRANSFORM: {excel_field_data['transform']}")
        print()
    
    print(f"\n❌ Поля есть в базе данных, но НЕТ в Excel: {len(missing_in_excel)}")
    for field in sorted(missing_in_excel):
        # Находим в какой таблице есть поле
        tables_with_field = [table for table, fields in db_fields_by_table.items() if field in fields]
        db_type = next((fields[field] for table, fields in DATABASE_TABLES.items() if field in fields), 'unknown')
        
        print(f"  • {field}")
        print(f"    📊 Таблицы БД: {', '.join(tables_with_field)}")
        print(f"    🔧 Тип данных: {db_type}")
        print()

def analyze_sources_and_transforms(fields_data):
    """Анализирует источники данных и типы трансформаций"""
    
    print("\n" + "="*100)
    print("📊 АНАЛИЗ ИСТОЧНИКОВ И ТРАНСФОРМАЦИЙ")
    print("="*100)
    
    # Группируем по источникам
    sources = {}
    transforms = {}
    
    for field in fields_data:
        source = field['source']
        transform = field['transform']
        
        if source:
            if source not in sources:
                sources[source] = []
            sources[source].append(field['field_name'])
        
        if transform:
            if transform not in transforms:
                transforms[transform] = []
            transforms[transform].append(field['field_name'])
    
    print(f"\n🗄️ ИСТОЧНИКИ ДАННЫХ:")
    for source, fields in sources.items():
        print(f"  📂 {source}: {len(fields)} полей")
        for field in fields:
            print(f"    • {field}")
        print()
    
    print(f"\n🔄 ТИПЫ ТРАНСФОРМАЦИЙ:")
    for transform, fields in transforms.items():
        print(f"  🔧 {transform}: {len(fields)} полей")
        for field in fields:
            print(f"    • {field}")
        print()

def main():
    """Основная функция"""
    
    # Проверяем существование файла
    if not Path(EXCEL_FILE).exists():
        logger.error(f"❌ Файл {EXCEL_FILE} не найден!")
        return
    
    # Анализируем Excel файл
    fields_data = analyze_flame_gpu_excel()
    
    # Сравниваем с базой данных
    compare_with_database(fields_data)
    
    # Анализируем источники и трансформации
    analyze_sources_and_transforms(fields_data)
    
    logger.info("✅ Анализ завершен")

if __name__ == "__main__":
    main() 