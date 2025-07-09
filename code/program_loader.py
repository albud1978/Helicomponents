#!/usr/bin/env python3
"""
Загрузчик Program.xlsx в ClickHouse

Функционал:
1. Загружает данные программы полетов в таблицу 'flight_program' 
2. Преобразует помесячные данные в нормализованную форму
3. Сохраняет версионность и метаданные Excel
4. Проверяет качество и целостность данных
5. Диалоги перезаписи существующих данных

Структура данных:
- ops_counter_mi8/mi17: количество операций по типам ВС (помесячно)
- daily_flight: ежедневный налет для типов ВС и конкретных ВС (помесячно)
- partno: код типа ВС (32=МИ-8, 64=МИ-17)
- serialno: серийный номер конкретного ВС
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime, date
import openpyxl
import os
import numpy as np

# Безопасная конфигурация через utils.config_loader

def extract_version_date_from_excel(file_path):
    """Извлекает дату версии из метаданных Excel файла"""
    try:
        print("📅 Определение версии данных из Excel метаданных...")
        
        workbook = openpyxl.load_workbook(file_path, read_only=True)
        props = workbook.properties
        
        version_datetime = None
        source_type = None
        current_year = datetime.now().year
        
        # Приоритет 1: Дата создания файла
        if props.created:
            created_year = props.created.year
            if abs(created_year - current_year) <= 1:
                version_datetime = props.created
                source_type = "Excel created"
                print(f"📅 Дата создания Excel: {version_datetime}")
            else:
                print(f"⚠️ Дата создания {props.created} отличается от текущего года более чем на год")
                
        # Приоритет 2: Дата модификации
        if version_datetime is None and props.modified:
            version_datetime = props.modified
            source_type = "Excel modified"
            print(f"📅 Дата модификации Excel: {version_datetime}")
            
        # Приоритет 3: Время модификации файла в ОС
        if version_datetime is None:
            file_mtime = os.path.getmtime(file_path)
            version_datetime = datetime.fromtimestamp(file_mtime)
            source_type = "OS file mtime"
            print(f"📅 Время модификации файла: {version_datetime}")
        
        workbook.close()
        
        file_size = os.path.getsize(file_path)
        file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
        print(f"📋 Файл: {os.path.basename(file_path)}")
        print(f"📏 Размер: {file_size:,} байт")
        print(f"🕐 Модификация ОС: {file_mtime}")
        print(f"🎯 Источник версии: {source_type}")
        
        return version_datetime.date()
        
    except Exception as e:
        print(f"⚠️ Ошибка извлечения метаданных Excel: {e}")
        
        try:
            file_mtime = os.path.getmtime(file_path)
            version_datetime = datetime.fromtimestamp(file_mtime)
            print(f"📅 Fallback: используем время модификации файла: {version_datetime}")
            return version_datetime.date()
        except Exception as fallback_error:
            print(f"❌ Критическая ошибка определения версии: {fallback_error}")
            version_date = datetime.now().date()
            print(f"🚨 Экстренный fallback: используем сегодняшнюю дату: {version_date}")
            return version_date

def load_program_data():
    """Загружает Program.xlsx"""
    try:
        program_path = Path('data_input/source_data/Program.xlsx')
        
        if not program_path.exists():
            print(f"❌ Файл {program_path} не найден")
            sys.exit(1)
        
        print(f"📖 Загружаем {program_path}...")
        
        # Загружаем лист 2025 с header в первой строке
        df = pd.read_excel(program_path, sheet_name='2025', header=0, engine='openpyxl')
        print("📖 Загружен Excel файл")
        
        # Удаляем служебную колонку "Счет" если она присутствует
        if 'Счет' in df.columns:
            df = df.drop(columns=['Счет'])
            print(f"🗑️ Удалена служебная колонка: Счет")
        
        print(f"📊 Загружено: {len(df):,} записей с {len(df.columns)} колонками")
        print(f"📋 Колонки: {list(df.columns)}")
        
        # Анализируем типы данных
        print(f"\n📈 Анализ данных:")
        print(f"   Типы ВС (partno): {df['partno'].dropna().unique()}")
        print(f"   Серийные номера: {df['serialno'].dropna().unique()}")
        print(f"   Типы полей: {df['Поле'].unique()}")
        
        return df
        
    except Exception as e:
        print(f"❌ Ошибка загрузки Program.xlsx: {e}")
        sys.exit(1)

def transform_program_data(df, version_date, version_id=1):
    """Преобразует данные программы полетов в нормализованную форму"""
    try:
        print(f"📦 Преобразование данных программы полетов в нормализованную форму...")
        
        normalized_data = []
        
        # Получаем колонки с месяцами (1-12)
        month_columns = [col for col in df.columns if isinstance(col, int) and 1 <= col <= 12]
        print(f"📅 Найдены месяцы: {month_columns}")
        
        for idx, row in df.iterrows():
            partno = row['partno'] if pd.notna(row['partno']) else None
            serialno = row['serialno'] if pd.notna(row['serialno']) else None
            field_type = row['Поле']
            
            # Преобразуем каждый месяц в отдельную запись
            for month in month_columns:
                value = row[month] if pd.notna(row[month]) else None
                
                if value is not None:
                    # Создаем дату для месяца (1 число каждого месяца)
                    # Предполагаем что данные за 2025 год (из названия листа)
                    program_date = date(2025, month, 1)
                    
                    normalized_data.append({
                        'partno': int(partno) if partno is not None else None,
                        'serialno': int(serialno) if serialno is not None else None,
                        'ac_type': _get_ac_type_from_partno(partno),
                        'field_type': field_type,
                        'program_date': program_date,
                        'month_number': month,
                        'program_year': 2025,
                        'value': int(value),
                        'version_date': version_date,
                        'version_id': version_id
                    })
        
        result_df = pd.DataFrame(normalized_data)
        print(f"📊 Преобразовано в {len(result_df):,} записей (нормализованная форма)")
        
        return result_df
        
    except Exception as e:
        print(f"❌ Ошибка преобразования данных: {e}")
        sys.exit(1)

def _get_ac_type_from_partno(partno):
    """Преобразует partno в тип ВС"""
    if partno == 32:
        return 'МИ-8'
    elif partno == 64:
        return 'МИ-17'
    else:
        return None

def prepare_program_data(df, version_date, version_id=1):
    """Подготавливает данные программы полетов для ClickHouse"""
    try:
        print(f"📦 Подготовка данных программы полетов для ClickHouse...")
        
        # Сначала нормализуем данные
        normalized_df = transform_program_data(df, version_date, version_id)
        
        # Обработка строковых полей для ClickHouse
        string_columns = ['ac_type', 'field_type']
        for col in string_columns:
            if col in normalized_df.columns:
                normalized_df[col] = normalized_df[col].astype(str)
                normalized_df[col] = normalized_df[col].replace(['nan', 'None', 'NaT'], '')

        # Обработка числовых полей - КРИТИЧНО для ClickHouse!
        numeric_columns = ['partno', 'serialno', 'month_number', 'program_year', 'value']
        for col in numeric_columns:
            if col in normalized_df.columns:
                # Используем подход из памяти о совместимости с clickhouse_driver
                if col in ['partno', 'serialno']:
                    # Nullable UInt8/UInt32 - нужны обычные Python типы!
                    numeric_series = pd.to_numeric(normalized_df[col], errors='coerce')
                    # Сначала заменяем NaN на None
                    numeric_series = numeric_series.replace({np.nan: None})
                    # Затем конвертируем не-None значения в Python int
                    normalized_df[col] = numeric_series.apply(
                        lambda x: int(x) if x is not None and pd.notna(x) else None
                    )
                else:
                    # Обязательные числовые поля - обычные Python int
                    numeric_series = pd.to_numeric(normalized_df[col], errors='coerce')
                    normalized_df[col] = numeric_series.fillna(0).astype('int64')

        # Обработка дат
        if 'program_date' in normalized_df.columns:
            # program_date уже является date объектом, убеждаемся что нет NaN
            pass

        print(f"📊 Подготовлено {len(normalized_df):,} записей с {len(normalized_df.columns)} колонками")
        
        # Отладочная информация
        print(f"🔍 Статистика None значений после обработки:")
        for col in ['partno', 'serialno']:
            none_count = normalized_df[col].isnull().sum()
            total_count = len(normalized_df)
            print(f"   {col}: {none_count}/{total_count} None значений")
        
        return normalized_df
        
    except Exception as e:
        print(f"❌ Ошибка подготовки данных программы полетов: {e}")
        sys.exit(1)

def create_program_table(client):
    """Создает таблицу flight_program в ClickHouse"""
    try:
        create_sql = """
        CREATE TABLE IF NOT EXISTS flight_program (
            -- Идентификаторы ВС
            `partno` Nullable(UInt8),               -- Код типа ВС (32=МИ-8, 64=МИ-17)
            `serialno` Nullable(UInt32),            -- Серийный номер ВС
            `ac_type` Nullable(String),             -- Тип ВС (МИ-8, МИ-17)
            
            -- Тип программных данных
            `field_type` String,                    -- ops_counter_mi8/mi17, daily_flight
            
            -- Временные характеристики
            `program_date` Date,                    -- Дата программы (1 число месяца)
            `month_number` UInt8,                   -- Номер месяца (1-12)
            `program_year` UInt16,                  -- Год программы
            
            -- Значения программы
            `value` UInt32,                         -- Значение (количество операций/часы налета)
            
            -- Метаданные файла
            `version_date` Date DEFAULT today(),    -- Дата версии файла
            `version_id` UInt8 DEFAULT 1            -- ID версии
            
        ) ENGINE = MergeTree()
        ORDER BY (program_year, month_number, field_type, version_date, version_id)
        PARTITION BY (program_year, toYYYYMM(version_date))
        SETTINGS index_granularity = 8192
        """
        
        client.execute(create_sql)
        print("✅ Таблица flight_program готова")
        
    except Exception as e:
        print(f"❌ Ошибка создания таблицы flight_program: {e}")
        sys.exit(1)

def check_version_conflicts(client, version_date, version_id):
    """Проверяет конфликты версий"""
    try:
        count = client.execute(f"SELECT COUNT(*) FROM flight_program WHERE version_date = '{version_date}' AND version_id = {version_id}")[0][0]
        
        if count > 0:
            print(f"\n🚨 НАЙДЕНЫ ДАННЫЕ С ИДЕНТИЧНОЙ ВЕРСИЕЙ!")
            print(f"   Дата версии: {version_date}, version_id: {version_id}")
            print(f"   flight_program: {count:,} записей")
            print(f"\nВыберите действие:")
            print(f"   1. ЗАМЕНИТЬ существующие данные (DELETE + INSERT)")
            print(f"   2. ОТМЕНИТЬ загрузку")
            
            while True:
                try:
                    choice = input(f"\nВаш выбор (1-2): ").strip()
                    if choice == '1':
                        print(f"🔄 Удаляем существующие данные за {version_date} v{version_id}...")
                        client.execute(f"DELETE FROM flight_program WHERE version_date = '{version_date}' AND version_id = {version_id}")
                        print(f"✅ Удалено {count:,} записей из flight_program")
                        return True
                    elif choice == '2':
                        print(f"❌ Загрузка отменена пользователем")
                        return False
                    else:
                        print("❌ Неверный выбор. Введите 1 или 2.")
                except KeyboardInterrupt:
                    print(f"\n❌ Загрузка отменена пользователем")
                    return False
        else:
            print(f"✅ Новая версия данных - продолжаем загрузку")
            return True
            
    except Exception as e:
        print(f"❌ Ошибка проверки версий: {e}")
        return False

def insert_program_data(client, df):
    """Загружает данные программы полетов в таблицу"""
    try:
        print(f"🚀 Загружаем {len(df):,} записей в flight_program...")
        
        # Конвертируем в список кортежей с правильными типами
        data_tuples = []
        for _, row in df.iterrows():
            # Ручная конвертация каждого значения в правильный Python тип
            row_tuple = []
            for i, (col_name, value) in enumerate(row.items()):
                if col_name in ['partno', 'serialno']:
                    # Nullable UInt8/UInt32
                    if pd.isna(value) or value is None or str(value).lower() == 'nan':
                        row_tuple.append(None)
                    else:
                        row_tuple.append(int(float(value)))
                elif col_name in ['month_number', 'program_year', 'value']:
                    # Обязательные int
                    row_tuple.append(int(value) if pd.notna(value) else 0)
                elif col_name in ['ac_type', 'field_type']:
                    # Строки
                    row_tuple.append(str(value) if pd.notna(value) else '')
                else:
                    # Остальные (даты и т.д.)
                    row_tuple.append(value)
            
            data_tuples.append(tuple(row_tuple))
        
        # Проверяем первый кортеж
        if data_tuples:
            sample = data_tuples[0]
            print(f"🔍 Проверка типов в кортеже:")
            print(f"   partno: {sample[0]} ({type(sample[0])})")
            print(f"   serialno: {sample[1]} ({type(sample[1])})")
        
        # Загружаем
        client.execute('INSERT INTO flight_program VALUES', data_tuples)
        
        print(f"✅ Загружено {len(data_tuples):,} записей в flight_program")
        return len(data_tuples)
        
    except Exception as e:
        print(f"❌ Ошибка загрузки в flight_program: {e}")
        return 0

def validate_program_data(client, version_date, version_id, original_count):
    """Проверка качества загруженных данных программы полетов"""
    print(f"\n🔍 === ПРОВЕРКА КАЧЕСТВА FLIGHT_PROGRAM ===")
    
    # Проверяем в БД
    db_count = client.execute(f"SELECT COUNT(*) FROM flight_program WHERE version_date = '{version_date}' AND version_id = {version_id}")[0][0]
    
    print(f"📊 Исходный Excel файл: {original_count:,} записей")
    print(f"📊 flight_program (нормализованные): {db_count:,} записей")
    
    # Анализируем структуру данных
    structure_analysis = client.execute(f"""
        SELECT 
            field_type,
            COUNT(*) as records_count,
            COUNT(DISTINCT partno) as unique_partno,
            COUNT(DISTINCT serialno) as unique_serialno,
            MIN(program_date) as min_date,
            MAX(program_date) as max_date
        FROM flight_program 
        WHERE version_date = '{version_date}' AND version_id = {version_id}
        GROUP BY field_type
        ORDER BY field_type
    """)
    
    print(f"\n📈 Анализ структуры данных:")
    for field_type, records, partno_count, serialno_count, min_date, max_date in structure_analysis:
        print(f"   {field_type}:")
        print(f"     📊 Записей: {records:,}")
        print(f"     🔢 Типов ВС: {partno_count}")
        print(f"     ✈️ Серийных номеров: {serialno_count}")
        print(f"     📅 Период: {min_date} - {max_date}")
    
    # Проверяем помесячное покрытие
    monthly_coverage = client.execute(f"""
        SELECT 
            month_number,
            COUNT(*) as records_count,
            COUNT(DISTINCT field_type) as field_types
        FROM flight_program 
        WHERE version_date = '{version_date}' AND version_id = {version_id}
        GROUP BY month_number
        ORDER BY month_number
    """)
    
    print(f"\n📅 Покрытие по месяцам:")
    for month, records, field_types in monthly_coverage:
        print(f"   Месяц {month:2d}: {records:,} записей, {field_types} типов полей")
    
    # Простые проверки качества
    issues = []
    
    # Ожидаем что нормализованных записей больше чем исходных
    if db_count <= original_count:
        issues.append(f"❌ Нормализованных записей меньше исходных: {db_count} <= {original_count}")
    
    # Проверяем наличие данных по всем месяцам
    months_with_data = len(monthly_coverage)
    if months_with_data < 12:
        issues.append(f"❌ Данные не по всем месяцам: {months_with_data}/12")
    
    # Результат проверки
    if issues:
        print(f"\n⚠️ Обнаружены проблемы:")
        for issue in issues:
            print(f"  {issue}")
        return False
    else:
        print(f"\n✅ Все проверки пройдены успешно!")
        print(f"✅ Коэффициент нормализации: {db_count/original_count:.1f}x")
        print(f"✅ Покрытие месяцев: {months_with_data}/12")
        return True

def prepare_data_for_clickhouse(df):
    """Финальная подготовка данных для ClickHouse - конвертация типов"""
    try:
        print(f"🔧 Финальная конвертация типов для ClickHouse...")
        
        # Создаем копию для безопасности
        result_df = df.copy()
        
        # Простая ручная конвертация nullable полей
        print(f"🔄 Конвертируем partno и serialno...")
        
        for col in ['partno', 'serialno']:
            if col in result_df.columns:
                new_values = []
                for i, value in enumerate(result_df[col]):
                    try:
                        if pd.isna(value) or value is None or str(value).lower() == 'nan':
                            new_values.append(None)
                        else:
                            new_values.append(int(float(value)))
                    except (ValueError, TypeError):
                        new_values.append(None)
                result_df[col] = new_values
        
        # Конвертируем обязательные числовые поля
        for col in ['month_number', 'program_year', 'value']:
            if col in result_df.columns:
                result_df[col] = pd.to_numeric(result_df[col], errors='coerce').fillna(0).astype(int)
        
        # Проверяем результат на первых нескольких записях
        print(f"🔍 Проверка типов после конвертации:")
        for i in range(min(3, len(result_df))):
            sample = result_df.iloc[i]
            partno = sample['partno']
            serialno = sample['serialno']
            print(f"   Строка {i}: partno={partno} ({type(partno)}), serialno={serialno} ({type(serialno)})")
        
        return result_df
        
    except Exception as e:
        print(f"❌ Ошибка конвертации типов: {e}")
        import traceback
        traceback.print_exc()
        return df

def main(version_date=None, version_id=None):
    """Основная функция с поддержкой версионирования"""
    print("🚀 === ЗАГРУЗЧИК PROGRAM (ПРОГРАММА ПОЛЕТОВ) ===")
    
    try:
        # 1. Подключение к ClickHouse через безопасную систему
        # Пароли только из environment variables, НЕТ захардкоженных паролей!
        import sys
        sys.path.append(str(Path(__file__).parent))
        from utils.config_loader import get_clickhouse_client
        client = get_clickhouse_client()
        
        # 2. Создание таблицы
        create_program_table(client)
        
        # 3. Загрузка исходных данных
        df = load_program_data()
        original_count = len(df)
        
        # 4. Определение версии данных
        if version_date is None:
            # Автоматическое извлечение из метаданных Excel (совместимость)
            program_path = Path('data_input/source_data/Program.xlsx')
            version_date = extract_version_date_from_excel(program_path)
            print(f"🗓️ Версия данных (из Excel): {version_date}")
        else:
            print(f"🗓️ Версия данных (из параметров ETL): {version_date}, version_id: {version_id}")
        
        if version_id is None:
            version_id = 1
        
        # 5. Проверка конфликтов версий
        if not check_version_conflicts(client, version_date, version_id):
            return
        
        # 6. Подготовка и нормализация данных
        prepared_df = prepare_program_data(df, version_date, version_id)
        
        # 7. Загрузка данных с автоматической конвертацией типов
        print(f"\n🚀 === НАЧИНАЕМ ЗАГРУЗКУ FLIGHT_PROGRAM ===")
        
        loaded_count = insert_program_data(client, prepared_df)
        
        # 8. Проверка результатов
        if loaded_count > 0:
            print(f"\n🎉 === ЗАГРУЗКА FLIGHT_PROGRAM ЗАВЕРШЕНА ===")
            
            validation_success = validate_program_data(client, version_date, version_id, original_count)
            
            if validation_success:
                print(f"\n🎯 === ИТОГОВАЯ СТАТИСТИКА ===")
                print(f"📅 Версия данных: {version_date} (version_id={version_id})")
                print(f"📊 flight_program: {loaded_count:,} записей (нормализованные)")
                print(f"📈 Программа полетов на 2025 год загружена")
                print(f"🔍 Проверки качества: ✅ ПРОЙДЕНЫ")
            else:
                print(f"\n⚠️ Загрузка завершена, но обнаружены проблемы качества")
                
        else:
            print(f"💥 Загрузка завершена с ошибками!")
            
    except Exception as e:
        print(f"💥 Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Program Loader для Helicopter Component Lifecycle')
    parser.add_argument('--version-date', type=str, help='Дата версии (YYYY-MM-DD)')
    parser.add_argument('--version-id', type=int, help='ID версии')
    
    args = parser.parse_args()
    
    # Передаем параметры версионирования в main, если они заданы
    if args.version_date and args.version_id:
        from datetime import datetime
        version_date = datetime.strptime(args.version_date, '%Y-%m-%d').date()
        main(version_date=version_date, version_id=args.version_id)
    else:
        main() 