"""
Загрузчик Program_AC (Реестр вертолетов в эксплуатации)

ПЯТЫЙ загрузчик в ETL цепочке для проекта Helicopter Component Lifecycle Prediction.
Загружает реестр воздушных судов в эксплуатации из Excel в таблицу ClickHouse.

Источник: data_input/source_data/Program_AC.xlsx
Назначение: program_ac (ClickHouse таблица)

Структура данных:
- Регистрационные номера ВС (ac_registr): 178 уникальных
- Типы ВС (ac_typ): 8 типов (350B3, 355NP, МИ8МТВ, МИ26Т, МИ8, КА32Т, МИ8АМТ, 355N)
- Собственники (owner): 5 организаций (ЮТ-ВУ, CHOPPER LL, ГТЛК, РЕГ ЛИЗИНГ, СБЕР ЛИЗИНГ)
- Базирование (homebase): 40 локаций
- Дирекции (directorate): 8 дирекций

Автор: AI Agent для пользователя budnik_an
Дата создания: 2024-12-19
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import os
from datetime import datetime, date
import openpyxl

# Добавляем путь к utils
sys.path.append(str(Path(__file__).parent / 'utils'))

from config_loader import get_clickhouse_client
from excel_utils import clean_excel_data

# Функция extract_version_date_from_excel удалена - используется общая utils.version_utils.extract_unified_version_date()

def load_program_ac_data():
    """Загружает данные реестра вертолетов в эксплуатации из текущего датасета"""
    try:
        # Получаем путь к датасету из version_utils
        from utils.version_utils import get_dataset_path
        dataset_path = get_dataset_path()
        
        if dataset_path:
            file_path = dataset_path / 'Program_AC.xlsx'
        else:
            file_path = Path('data_input/source_data/Program_AC.xlsx')
        
        print(f"📖 Загружаем {file_path}...")
        
        if not file_path.exists():
            print(f"❌ Файл не найден: {file_path}")
            sys.exit(1)
        
        # Загружаем Excel файл
        df = pd.read_excel(file_path)
        print(f"📖 Загружен Excel файл")
        print(f"📊 Загружено: {len(df)} записей с {len(df.columns)} колонками")
        print(f"📋 Колонки: {list(df.columns)}")
        
        # Анализ данных
        print(f"\n📈 Анализ данных:")
        if 'ac_typ' in df.columns:
            ac_types = df['ac_typ'].dropna().unique()
            ac_type_counts = df['ac_typ'].value_counts()
            print(f"   Типы ВС: {list(ac_types)}")
            print(f"   Распределение по типам:")
            for ac_type, count in ac_type_counts.head(5).items():
                print(f"     {ac_type}: {count} ВС")
        
        if 'owner' in df.columns:
            owners = df['owner'].dropna().unique()
            owner_counts = df['owner'].value_counts()
            print(f"   Собственники: {list(owners)}")
            print(f"   Распределение по собственникам:")
            for owner, count in owner_counts.items():
                print(f"     {owner}: {count} ВС")
        
        if 'directorate' in df.columns:
            directorates = df['directorate'].dropna().unique()
            print(f"   Дирекции: {len(directorates)} шт")
        
        return df
        
    except Exception as e:
        print(f"❌ Ошибка загрузки Program_AC.xlsx: {e}")
        sys.exit(1)

def remove_service_columns(df):
    """Удаляет служебные колонки из DataFrame"""
    # Список служебных колонок для исключения
    service_columns = ['Счет']
    
    removed_columns = []
    for col in service_columns:
        if col in df.columns:
            df = df.drop(columns=[col])
            removed_columns.append(col)
    
    if removed_columns:
        print(f"   🗑️ Удалены служебные колонки: {', '.join(removed_columns)}")
    
    return df

def prepare_program_ac_data(df, version_date, version_id=1):
    """Подготавливает данные реестра вертолетов для ClickHouse"""
    try:
        print(f"📦 Подготовка данных реестра вертолетов для ClickHouse...")
        
        # Создаем копию
        result_df = df.copy()
        
        # Удаляем служебные колонки
        result_df = remove_service_columns(result_df)
        
        # Добавляем версию данных
        result_df['version_date'] = version_date
        result_df['version_id'] = version_id
        
        # Обработка строковых полей для ClickHouse
        string_columns = ['ac_typ', 'object_type', 'description', 'owner', 'operator', 
                         'homebase', 'homebase_name', 'directorate']
        for col in string_columns:
            if col in result_df.columns:
                result_df[col] = result_df[col].astype(str)
                result_df[col] = result_df[col].replace(['nan', 'None', 'NaT'], '')

        # Обработка числовых полей
        if 'ac_registr' in result_df.columns:
            # ac_registr - регистрационный номер ВС (UInt32)
            result_df['ac_registr'] = pd.to_numeric(result_df['ac_registr'], errors='coerce').fillna(0).astype('int64')

        # Специальная обработка version_date
        if 'version_date' in result_df.columns:
            # version_date уже является date объектом
            pass

        print(f"📊 Подготовлено {len(result_df):,} записей с {len(result_df.columns)} колонками")
        
        # Проверка качества данных
        print(f"🔍 Проверка качества данных:")
        
        # Проверяем уникальность регистрационных номеров
        ac_registr_duplicates = result_df['ac_registr'].duplicated().sum()
        if ac_registr_duplicates > 0:
            print(f"   ⚠️ Дубликаты ac_registr: {ac_registr_duplicates}")
        else:
            print(f"   ✅ Все ac_registr уникальные")
        
        # Проверяем типы ВС
        if 'ac_typ' in result_df.columns:
            ac_types = result_df['ac_typ'].unique()
            print(f"   ✅ Типы ВС: {len(ac_types)} уникальных")
        
        return result_df
        
    except Exception as e:
        print(f"❌ Ошибка подготовки данных реестра вертолетов: {e}")
        sys.exit(1)

def create_program_ac_table(client):
    """Создает таблицу для реестра вертолетов в эксплуатации"""
    try:
        print(f"🔧 Создание таблицы program_ac...")
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS program_ac (
            -- Основные идентификаторы ВС
            `ac_registr` UInt32,                    -- Регистрационный номер ВС
            `ac_typ` String,                        -- Тип ВС (350B3, 355NP, МИ8МТВ, МИ26Т, МИ8, КА32Т, МИ8АМТ, 355N)
            `object_type` String,                   -- Тип объекта (HELICOPTER)
            `description` String,                   -- Полное описание модели ВС
            
            -- Собственность и эксплуатация
            `owner` String,                         -- Собственник (ЮТ-ВУ, CHOPPER LL, ГТЛК, РЕГ ЛИЗИНГ, СБЕР ЛИЗИНГ)
            `operator` String,                      -- Эксплуатант (ЮТ-ВУ)
            
            -- Базирование
            `homebase` String,                      -- Код базы приписки (ТЮМ, СУР, НОЯ и т.д.)
            `homebase_name` String,                 -- Полное наименование базы приписки
            
            -- Организационная структура
            `directorate` String,                   -- Дирекция (ЗАПАДНО-СИБИРСКАЯ ДИРЕКЦИЯ и т.д.)
            
            -- Метаданные
            `version_date` Date DEFAULT today(),    -- Дата версии данных
            `version_id` UInt8 DEFAULT 1            -- ID версии
            
        ) ENGINE = MergeTree()
        ORDER BY (ac_registr, ac_typ, version_date, version_id)
        PARTITION BY toYYYYMM(version_date)
        SETTINGS index_granularity = 8192
        """
        
        client.execute(create_table_sql)
        print("✅ Таблица program_ac готова")
        
    except Exception as e:
        print(f"❌ Ошибка создания таблицы program_ac: {e}")
        sys.exit(1)

def check_version_conflicts(client, version_date, version_id):
    """Проверяет конфликты версий для program_ac"""
    try:
        # Проверяем таблицу на точное совпадение версии
        count = client.execute(f"SELECT COUNT(*) FROM program_ac WHERE version_date = '{version_date}' AND version_id = {version_id}")[0][0]
        
        if count > 0:
            print(f"\n🚨 НАЙДЕНЫ ДАННЫЕ С ИДЕНТИЧНОЙ ВЕРСИЕЙ!")
            print(f"   Дата версии: {version_date}, version_id: {version_id}")
            print(f"   program_ac: {count:,} записей")
            print(f"\nВыберите действие:")
            print(f"   1. ЗАМЕНИТЬ существующие данные (DELETE + INSERT)")
            print(f"   2. ОТМЕНИТЬ загрузку")
            
            while True:
                try:
                    choice = input(f"\nВаш выбор (1-2): ").strip()
                    if choice == '1':
                        print(f"🔄 Удаляем существующие данные за {version_date} v{version_id}...")
                        client.execute(f"DELETE FROM program_ac WHERE version_date = '{version_date}' AND version_id = {version_id}")
                        print(f"✅ Удалено {count:,} записей из program_ac")
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

def insert_program_ac_data(client, df):
    """Загружает данные реестра вертолетов в таблицу"""
    try:
        print(f"🚀 Загружаем {len(df):,} записей в program_ac...")
        
        # Конвертируем в список кортежей с правильными типами
        data_tuples = []
        for _, row in df.iterrows():
            # Ручная конвертация каждого значения в правильный Python тип
            row_tuple = []
            for i, (col_name, value) in enumerate(row.items()):
                if col_name == 'ac_registr':
                    # UInt32
                    row_tuple.append(int(value) if pd.notna(value) else 0)
                elif col_name in ['ac_typ', 'object_type', 'description', 'owner', 'operator', 
                                'homebase', 'homebase_name', 'directorate']:
                    # Строки
                    row_tuple.append(str(value) if pd.notna(value) else '')
                else:
                    # Остальные (дата version_date)
                    row_tuple.append(value)
            
            data_tuples.append(tuple(row_tuple))
        
        # Проверяем первый кортеж
        if data_tuples:
            sample = data_tuples[0]
            print(f"🔍 Проверка типов в кортеже:")
            print(f"   ac_registr: {sample[0]} ({type(sample[0])})")
            print(f"   ac_typ: {sample[1]} ({type(sample[1])})")
            print(f"   owner: {sample[4]} ({type(sample[4])})")
        
        # Загружаем
        client.execute('INSERT INTO program_ac VALUES', data_tuples)
        
        print(f"✅ Загружено {len(data_tuples):,} записей в program_ac")
        return len(data_tuples)
        
    except Exception as e:
        print(f"❌ Ошибка загрузки в program_ac: {e}")
        return 0

def validate_program_ac_data(client, version_date, version_id, original_count):
    """Проверка качества загруженных данных реестра вертолетов"""
    print(f"\n🔍 === ПРОВЕРКА КАЧЕСТВА PROGRAM_AC ===")
    
    # Проверяем в БД
    db_count = client.execute(f"SELECT COUNT(*) FROM program_ac WHERE version_date = '{version_date}' AND version_id = {version_id}")[0][0]
    
    print(f"📊 Исходный Excel файл: {original_count:,} записей")
    print(f"📊 program_ac: {db_count:,} записей")
    
    # Анализируем структуру данных
    structure_analysis = client.execute(f"""
        SELECT 
            ac_typ,
            owner,
            COUNT(*) as records_count,
            COUNT(DISTINCT ac_registr) as unique_aircraft,
            COUNT(DISTINCT homebase) as unique_bases,
            COUNT(DISTINCT directorate) as unique_directorates
        FROM program_ac 
        WHERE version_date = '{version_date}' AND version_id = {version_id}
        GROUP BY ac_typ, owner
        ORDER BY ac_typ, records_count DESC
    """)
    
    print(f"\n📈 Анализ по типам ВС и собственникам:")
    for ac_typ, owner, records, aircraft_count, bases, directorates in structure_analysis:
        print(f"   {ac_typ} - {owner}:")
        print(f"     📊 Записей: {records:,}")
        print(f"     ✈️ ВС: {aircraft_count}")
        print(f"     🏢 Баз: {bases}")
        print(f"     🏛️ Дирекций: {directorates}")
    
    # Проверяем распределение по типам ВС
    ac_type_summary = client.execute(f"""
        SELECT 
            ac_typ,
            COUNT(*) as count,
            COUNT(DISTINCT owner) as owners_count,
            COUNT(DISTINCT homebase) as bases_count
        FROM program_ac 
        WHERE version_date = '{version_date}' AND version_id = {version_id}
        GROUP BY ac_typ
        ORDER BY count DESC
    """)
    
    print(f"\n📊 Распределение по типам ВС:")
    for ac_typ, count, owners, bases in ac_type_summary:
        print(f"   {ac_typ}: {count:,} ВС ({owners} собственников, {bases} баз)")
    
    # Проверяем географическое распределение
    geographical_summary = client.execute(f"""
        SELECT 
            directorate,
            COUNT(*) as count,
            COUNT(DISTINCT homebase) as bases_count
        FROM program_ac 
        WHERE version_date = '{version_date}' AND version_id = {version_id}
        GROUP BY directorate
        ORDER BY count DESC
        LIMIT 5
    """)
    
    print(f"\n🗺️ Топ-5 дирекций по количеству ВС:")
    for directorate, count, bases in geographical_summary:
        directorate_short = directorate[:30] + "..." if len(directorate) > 30 else directorate
        print(f"   {directorate_short}: {count:,} ВС ({bases} баз)")
    
    # Простые проверки качества
    issues = []
    
    # Ожидаем что все данные загружены
    if db_count != original_count:
        issues.append(f"❌ Количество записей не совпадает: {db_count} != {original_count}")
    
    # Проверяем наличие обязательных данных
    null_registr = client.execute(f"SELECT COUNT(*) FROM program_ac WHERE ac_registr = 0 AND version_date = '{version_date}' AND version_id = {version_id}")[0][0]
    if null_registr > 0:
        issues.append(f"❌ Записи без регистрационного номера: {null_registr}")
    
    # Проверяем уникальность регистрационных номеров
    duplicate_registr = client.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT ac_registr 
            FROM program_ac 
            WHERE version_date = '{version_date}' AND version_id = {version_id}
            GROUP BY ac_registr 
            HAVING COUNT(*) > 1
        )
    """)[0][0]
    if duplicate_registr > 0:
        issues.append(f"❌ Дубликаты регистрационных номеров: {duplicate_registr}")
    
    # Результат проверки
    if issues:
        print(f"\n⚠️ Обнаружены проблемы:")
        for issue in issues:
            print(f"  {issue}")
        return False
    else:
        print(f"\n✅ Все проверки пройдены успешно!")
        print(f"✅ Загружено записей: {db_count}/{original_count}")
        return True

def main(version_date=None, version_id=None):
    """Основная функция с поддержкой версионирования"""
    print("🚀 === ЗАГРУЗЧИК PROGRAM_AC (РЕЕСТР ВЕРТОЛЕТОВ В ЭКСПЛУАТАЦИИ) ===")
    
    try:
        # 1. Подключение к ClickHouse через безопасную систему
        client = get_clickhouse_client()
        
        # 2. Создание таблицы
        create_program_ac_table(client)
        
        # 3. Загрузка исходных данных
        df = load_program_ac_data()
        original_count = len(df)
        
        # 4. Определение версии данных
        if version_date is None:
            # ЕДИНЫЙ ИСТОЧНИК ВЕРСИОННОСТИ: Status_Components.xlsx
            from utils.version_utils import extract_unified_version_date
            version_date = extract_unified_version_date()
            print(f"🗓️ Версия данных (из Status_Components.xlsx): {version_date}")
        else:
            print(f"🗓️ Версия данных (из параметров ETL): {version_date}, version_id: {version_id}")
        
        if version_id is None:
            version_id = 1
        
        # 5. Проверка конфликтов версий
        if not check_version_conflicts(client, version_date, version_id):
            return
        
        # 6. Подготовка данных
        prepared_df = prepare_program_ac_data(df, version_date, version_id)
        
        # 7. Загрузка данных с автоматической конвертацией типов
        print(f"\n🚀 === НАЧИНАЕМ ЗАГРУЗКУ PROGRAM_AC ===")
        
        loaded_count = insert_program_ac_data(client, prepared_df)
        
        # 8. Проверка результатов
        if loaded_count > 0:
            print(f"\n🎉 === ЗАГРУЗКА PROGRAM_AC ЗАВЕРШЕНА ===")
            
            validation_success = validate_program_ac_data(client, version_date, version_id, original_count)
            
            if validation_success:
                print(f"\n🎯 === ИТОГОВАЯ СТАТИСТИКА ===")
                print(f"📅 Версия данных: {version_date} (version_id={version_id})")
                print(f"📊 program_ac: {loaded_count:,} записей")
                print(f"📈 Реестр вертолетов в эксплуатации загружен")
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
    
    parser = argparse.ArgumentParser(description='Program AC Loader для Helicopter Component Lifecycle')
    parser.add_argument('--version-date', type=str, help='Дата версии (YYYY-MM-DD)')
    parser.add_argument('--version-id', type=int, help='ID версии')
    parser.add_argument('--dataset-path', type=str, help='Путь к папке датасета (v_YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Устанавливаем путь к датасету если передан
    if args.dataset_path:
        from utils.version_utils import set_dataset_path
        set_dataset_path(args.dataset_path)
    
    # Передаем параметры версионирования в main, если они заданы
    if args.version_date and args.version_id:
        from datetime import datetime
        version_date = datetime.strptime(args.version_date, '%Y-%m-%d').date()
        main(version_date=version_date, version_id=args.version_id)
    else:
        main()