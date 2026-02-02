import pandas as pd
import sys
from pathlib import Path
from datetime import datetime, date
import openpyxl
import os

# Безопасная конфигурация через utils.config_loader
code_root = Path(__file__).resolve().parents[1]
sys.path.append(str(code_root))
sys.path.append(str(code_root / 'utils'))
from utils.config_loader import get_clickhouse_client

# Функция extract_version_date_from_excel удалена - используется общая utils.version_utils.extract_unified_version_date()

def load_status_overhaul_data():
    """Загружает данные о статусе капитального ремонта из текущего датасета"""
    try:
        # Получаем путь к датасету из version_utils
        from utils.version_utils import get_dataset_path
        dataset_path = get_dataset_path()
        
        if dataset_path:
            file_path = dataset_path / 'Status_Overhaul.xlsx'
        else:
            file_path = Path('data_input/source_data/Status_Overhaul.xlsx')
        
        print(f"📖 Загружаем {file_path}...")
        
        if not file_path.exists():
            print(f"❌ Файл не найден: {file_path}")
            sys.exit(1)
        
        # Загружаем Excel файл
        df = pd.read_excel(file_path)
        print(f"📖 Загружен Excel файл")
        
        # Удаляем служебную колонку "Счет" если она присутствует
        if 'Счет' in df.columns:
            df = df.drop(columns=['Счет'])
            print(f"🗑️ Удалена служебная колонка: Счет")
        
        print(f"📊 Загружено: {len(df)} записей с {len(df.columns)} колонками")
        print(f"📋 Колонки: {list(df.columns)}")
        
        # Анализ данных
        print(f"\n📈 Анализ данных:")
        if 'ac_typ' in df.columns:
            ac_types = df['ac_typ'].dropna().unique()
            print(f"   Типы ВС: {ac_types}")
        
        if 'status' in df.columns:
            statuses = df['status'].dropna().unique()
            print(f"   Статусы: {statuses}")
            
        if 'owner' in df.columns:
            owners = df['owner'].dropna().unique()
            print(f"   Собственники: {owners}")
        
        return df
        
    except Exception as e:
        print(f"❌ Ошибка загрузки Status_Overhaul.xlsx: {e}")
        sys.exit(1)

def prepare_status_overhaul_data(df, version_date, version_id=1):
    """Подготавливает данные о статусе капитального ремонта для ClickHouse"""
    try:
        print(f"📦 Подготовка данных о статусе капитального ремонта для ClickHouse...")
        
        # Создаем копию
        result_df = df.copy()
        
        # Добавляем версию данных
        result_df['version_date'] = version_date
        result_df['version_id'] = version_id
        
        # Обработка строковых полей для ClickHouse
        string_columns = ['ac_typ', 'wpno', 'description', 'status', 'owner', 'operator']
        for col in string_columns:
            if col in result_df.columns:
                result_df[col] = result_df[col].astype(str)
                result_df[col] = result_df[col].replace(['nan', 'None', 'NaT'], '')

        # Обработка числовых полей
        if 'ac_registr' in result_df.columns:
            # ac_registr - серийный номер ВС (UInt32)
            result_df['ac_registr'] = pd.to_numeric(result_df['ac_registr'], errors='coerce').fillna(0).astype('int64')

        # Обработка дат для ClickHouse (надежный парсер для русских дат)
        def parse_russian_date(date_str):
            """Парсит русские даты формата '05.февр..2024'"""
            if pd.isna(date_str) or date_str == '' or date_str == 'nan':
                return None
            
            try:
                date_str = str(date_str).strip()
                # Словарь русских месяцев
                month_mapping = {
                    'янв': '01', 'февр': '02', 'мар': '03', 'апр': '04',
                    'мая': '05', 'май': '05', 'июн': '06', 'июл': '07', 
                    'авг': '08', 'сент': '09', 'окт': '10', 'нояб': '11', 'дек': '12'
                }
                
                # Ищем паттерн: день.месяц..год
                parts = date_str.split('.')
                if len(parts) >= 3:
                    day = parts[0].zfill(2)
                    month_ru = parts[1].lower().rstrip('.')
                    year = parts[-1]
                    
                    # Находим соответствие месяца
                    month_num = None
                    for ru_month, num in month_mapping.items():
                        if month_ru.startswith(ru_month):
                            month_num = num
                            break
                    
                    if month_num:
                        # Формируем ISO дату и парсим
                        iso_date = f'{year}-{month_num}-{day}'
                        return pd.to_datetime(iso_date).date()
                
                # Если не удалось, пробуем стандартный парсер
                return pd.to_datetime(date_str, errors='coerce').date()
                
            except:
                return None
        
        date_columns = ['sched_start_date', 'sched_end_date', 'act_start_date', 'act_end_date']
        for col in date_columns:
            if col in result_df.columns:
                print(f"   Обрабатываем даты в колонке {col}...")
                # Применяем парсер к каждой дате
                result_df[col] = result_df[col].apply(parse_russian_date)

        # Специальная обработка version_date
        if 'version_date' in result_df.columns:
            # version_date уже является date объектом
            pass

        print(f"📊 Подготовлено {len(result_df):,} записей с {len(result_df.columns)} колонками")
        
        # Отладочная информация
        print(f"🔍 Статистика None значений:")
        for col in date_columns:
            if col in result_df.columns:
                none_count = result_df[col].isnull().sum()
                total_count = len(result_df)
                print(f"   {col}: {none_count}/{total_count} None значений")
        
        return result_df
        
    except Exception as e:
        print(f"❌ Ошибка подготовки данных о статусе капитального ремонта: {e}")
        sys.exit(1)

def create_status_overhaul_table(client):
    """Создает таблицу для статуса капитального ремонта"""
    try:
        print(f"🔧 Создание таблицы status_overhaul...")
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS status_overhaul (
            -- Идентификаторы ВС
            `ac_registr` UInt32,                    -- Регистрационный номер ВС
            `ac_typ` String,                        -- Тип ВС (МИ8, МИ8АМТ)
            
            -- Номер работы и описание
            `wpno` String,                          -- Номер рабочего пакета (22774/H-24)
            `description` String,                   -- Описание работ
            
            -- Плановые даты
            `sched_start_date` Nullable(Date),      -- Плановая дата начала
            `sched_end_date` Nullable(Date),        -- Плановая дата окончания
            
            -- Фактические даты
            `act_start_date` Nullable(Date),        -- Фактическая дата начала
            `act_end_date` Nullable(Date),          -- Фактическая дата окончания
            
            -- Статус и участники
            `status` String,                        -- Статус (Закрыто, В процессе, Открыто)
            `owner` String,                         -- Собственник
            `operator` String,                      -- Оператор
            
            -- Метаданные
            `version_date` Date DEFAULT today(),    -- Дата версии данных
            `version_id` UInt8 DEFAULT 1            -- ID версии
            
        ) ENGINE = MergeTree()
        ORDER BY (ac_registr, wpno, status, version_date, version_id)
        PARTITION BY toYYYYMM(version_date)
        SETTINGS index_granularity = 8192
        """
        
        client.execute(create_table_sql)
        print("✅ Таблица status_overhaul готова")
        
    except Exception as e:
        print(f"❌ Ошибка создания таблицы status_overhaul: {e}")
        sys.exit(1)

def check_version_conflicts(client, version_date, version_id):
    """Проверяет конфликты версий для status_overhaul"""
    try:
        # Проверяем таблицу на точное совпадение версии
        count = client.execute(f"SELECT COUNT(*) FROM status_overhaul WHERE version_date = '{version_date}' AND version_id = {version_id}")[0][0]
        
        if count > 0:
            print(f"\n🚨 НАЙДЕНЫ ДАННЫЕ С ИДЕНТИЧНОЙ ВЕРСИЕЙ!")
            print(f"   Дата версии: {version_date}, version_id: {version_id}")
            print(f"   status_overhaul: {count:,} записей")
            print(f"\nВыберите действие:")
            print(f"   1. ЗАМЕНИТЬ существующие данные (DELETE + INSERT)")
            print(f"   2. ОТМЕНИТЬ загрузку")
            
            while True:
                try:
                    choice = input(f"\nВаш выбор (1-2): ").strip()
                    if choice == '1':
                        print(f"🔄 Удаляем существующие данные за {version_date} v{version_id}...")
                        client.execute(f"DELETE FROM status_overhaul WHERE version_date = '{version_date}' AND version_id = {version_id}")
                        print(f"✅ Удалено {count:,} записей из status_overhaul")
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

def insert_status_overhaul_data(client, df):
    """Загружает данные о статусе капитального ремонта в таблицу"""
    try:
        print(f"🚀 Загружаем {len(df):,} записей в status_overhaul...")
        
        # Конвертируем в список кортежей с правильными типами
        data_tuples = []
        for _, row in df.iterrows():
            # Ручная конвертация каждого значения в правильный Python тип
            row_tuple = []
            for i, (col_name, value) in enumerate(row.items()):
                if col_name == 'ac_registr':
                    # UInt32
                    row_tuple.append(int(value) if pd.notna(value) else 0)
                elif col_name in ['ac_typ', 'wpno', 'description', 'status', 'owner', 'operator']:
                    # Строки
                    row_tuple.append(str(value) if pd.notna(value) else '')
                elif col_name in ['sched_start_date', 'sched_end_date', 'act_start_date', 'act_end_date']:
                    # Nullable Date
                    row_tuple.append(value if value is not None else None)
                else:
                    # Остальные (даты и т.д.)
                    row_tuple.append(value)
            
            data_tuples.append(tuple(row_tuple))
        
        # Проверяем первый кортеж
        if data_tuples:
            sample = data_tuples[0]
            print(f"🔍 Проверка типов в кортеже:")
            print(f"   ac_registr: {sample[0]} ({type(sample[0])})")
            print(f"   ac_typ: {sample[1]} ({type(sample[1])})")
            print(f"   sched_start_date: {sample[4]} ({type(sample[4])})")
        
        # Загружаем
        client.execute('INSERT INTO status_overhaul VALUES', data_tuples)
        
        print(f"✅ Загружено {len(data_tuples):,} записей в status_overhaul")
        return len(data_tuples)
        
    except Exception as e:
        print(f"❌ Ошибка загрузки в status_overhaul: {e}")
        return 0

def validate_status_overhaul_data(client, version_date, version_id, original_count):
    """Проверка качества загруженных данных о статусе капитального ремонта"""
    print(f"\n🔍 === ПРОВЕРКА КАЧЕСТВА STATUS_OVERHAUL ===")
    
    # Проверяем в БД
    db_count = client.execute(f"SELECT COUNT(*) FROM status_overhaul WHERE version_date = '{version_date}' AND version_id = {version_id}")[0][0]
    
    print(f"📊 Исходный Excel файл: {original_count:,} записей")
    print(f"📊 status_overhaul: {db_count:,} записей")
    
    # Анализируем структуру данных
    structure_analysis = client.execute(f"""
        SELECT 
            ac_typ,
            status,
            COUNT(*) as records_count,
            COUNT(DISTINCT ac_registr) as unique_aircraft,
            COUNT(DISTINCT wpno) as unique_workpacks,
            MIN(sched_start_date) as min_sched_date,
            MAX(sched_end_date) as max_sched_date
        FROM status_overhaul 
        WHERE version_date = '{version_date}' AND version_id = {version_id}
        GROUP BY ac_typ, status
        ORDER BY ac_typ, status
    """)
    
    print(f"\n📈 Анализ по типам ВС и статусам:")
    for ac_typ, status, records, aircraft_count, workpack_count, min_date, max_date in structure_analysis:
        print(f"   {ac_typ} - {status}:")
        print(f"     📊 Записей: {records:,}")
        print(f"     ✈️ ВС: {aircraft_count}")
        print(f"     📦 Рабочих пакетов: {workpack_count}")
        print(f"     📅 Плановый период: {min_date} - {max_date}")
    
    # Проверяем статусы работ
    status_summary = client.execute(f"""
        SELECT 
            status,
            COUNT(*) as count,
            COUNT(CASE WHEN act_end_date IS NOT NULL THEN 1 END) as completed_count
        FROM status_overhaul 
        WHERE version_date = '{version_date}' AND version_id = {version_id}
        GROUP BY status
        ORDER BY count DESC
    """)
    
    print(f"\n📊 Статусы работ:")
    for status, count, completed in status_summary:
        completion_rate = (completed / count * 100) if count > 0 else 0
        print(f"   {status}: {count:,} работ ({completion_rate:.1f}% с фактическим окончанием)")
    
    # Простые проверки качества
    issues = []
    
    # Ожидаем что все данные загружены
    if db_count != original_count:
        issues.append(f"❌ Количество записей не совпадает: {db_count} != {original_count}")
    
    # Проверяем наличие обязательных данных
    null_registr = client.execute(f"SELECT COUNT(*) FROM status_overhaul WHERE ac_registr = 0 AND version_date = '{version_date}' AND version_id = {version_id}")[0][0]
    if null_registr > 0:
        issues.append(f"❌ Записи без регистрационного номера: {null_registr}")
    
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
    print("🚀 === ЗАГРУЗЧИК STATUS_OVERHAUL (СТАТУС КАПИТАЛЬНОГО РЕМОНТА) ===")
    
    try:
        # 1. Подключение к ClickHouse через безопасную систему
        client = get_clickhouse_client()
        
        # 2. Создание таблицы
        create_status_overhaul_table(client)
        
        # 3. Загрузка исходных данных
        df = load_status_overhaul_data()
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
        prepared_df = prepare_status_overhaul_data(df, version_date, version_id)
        
        # 7. Загрузка данных с автоматической конвертацией типов
        print(f"\n🚀 === НАЧИНАЕМ ЗАГРУЗКУ STATUS_OVERHAUL ===")
        
        loaded_count = insert_status_overhaul_data(client, prepared_df)
        
        # 8. Проверка результатов
        if loaded_count > 0:
            print(f"\n🎉 === ЗАГРУЗКА STATUS_OVERHAUL ЗАВЕРШЕНА ===")
            
            validation_success = validate_status_overhaul_data(client, version_date, version_id, original_count)
            
            if validation_success:
                print(f"\n🎯 === ИТОГОВАЯ СТАТИСТИКА ===")
                print(f"📅 Версия данных: {version_date} (version_id={version_id})")
                print(f"📊 status_overhaul: {loaded_count:,} записей")
                print(f"📈 Статус капитального ремонта загружен")
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
    
    parser = argparse.ArgumentParser(description='Status Overhaul Loader для Helicopter Component Lifecycle')
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