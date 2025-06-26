#!/usr/bin/env python3
"""
Загрузчик MD_Components.xlsx в ClickHouse

Функционал:
1. Загружает мастер-данные компонентов в таблицу 'md_components' 
2. Сохраняет версионность и метаданные Excel
3. Проверяет качество и целостность данных
4. Диалоги перезаписи существующих данных
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
import yaml
import openpyxl
import os

# Конфигурация теперь загружается через utils.config_loader

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

def load_md_components():
    """Загружает MD_Components.xlsx"""
    try:
        md_path = Path('data_input/master_data/MD_Сomponents.xlsx')
        
        if not md_path.exists():
            print(f"❌ Файл {md_path} не найден")
            sys.exit(1)
        
        print(f"📖 Загружаем {md_path}...")
        
        # Загружаем с правильным header (вторая строка)
        df = pd.read_excel(md_path, sheet_name='Агрегаты', header=1, engine='openpyxl')
        print("📖 Загружен Excel файл")
        
        print(f"📊 Загружено: {len(df):,} записей с {len(df.columns)} колонками")
        print(f"📋 Колонки: {list(df.columns)}")
        return df
        
    except Exception as e:
        print(f"❌ Ошибка загрузки MD_Components: {e}")
        sys.exit(1)

def prepare_md_data(df, version_date):
    """Подготавливает данные MD_Components для ClickHouse"""
    try:
        print(f"📦 Подготовка данных MD_Components...")
        
        # Добавляем версию данных
        df['version_date'] = version_date
        
        # Обработка строковых полей для ClickHouse
        string_columns = ['partno', 'group_by', 'ac_typ']
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)
                df[col] = df[col].replace(['nan', 'None', 'NaT'], '')
                # Очищаем переносы строк в partno
                if col == 'partno':
                    df[col] = df[col].str.replace('\n', '', regex=False)

        # Обработка числовых полей
        numeric_columns = [
            'comp_number', 'type_restricted', 'common_restricted1', 'common_restricted2',
            'trigger_interval', 'partout_time', 'assembly_time', 'repair_time',
            'll_mi8', 'oh_mi8', 'oh_threshold_mi8', 'll_mi17', 'oh_mi17',
            'repair_price', 'purchase_price', 'sne', 'ppr'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                # Конвертируем в числа, NaN заменяем на None
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].where(df[col].notnull(), None)

        print(f"📊 Подготовлено {len(df):,} записей с {len(df.columns)} колонками")
        return df
        
    except Exception as e:
        print(f"❌ Ошибка подготовки данных MD_Components: {e}")
        sys.exit(1)

def create_md_table(client):
    """Создает таблицу md_components в ClickHouse"""
    try:
        create_sql = """
        CREATE TABLE IF NOT EXISTS md_components (
            -- Основные идентификаторы
            `partno` Nullable(String),              -- Чертежный номер
            `comp_number` Nullable(Float64),        -- Количество на ВС
            `group_by` Nullable(String),            -- Группировка
            `ac_typ` Nullable(String),              -- Тип ВС
            
            -- Ограничения
            `type_restricted` Nullable(Float64),    -- Ограничение по типу
            `common_restricted1` Nullable(Float64), -- Общее ограничение 1
            `common_restricted2` Nullable(Float64), -- Общее ограничение 2
            
            -- Временные характеристики
            `trigger_interval` Nullable(Float64),   -- Интервал срабатывания
            `partout_time` Nullable(Float64),       -- Время снятия
            `assembly_time` Nullable(Float64),      -- Время установки
            `repair_time` Nullable(Float64),        -- Время ремонта
            
            -- Ресурсы МИ-8
            `ll_mi8` Nullable(Float64),             -- LL МИ-8
            `oh_mi8` Nullable(Float64),             -- OH МИ-8
            `oh_threshold_mi8` Nullable(Float64),   -- Порог OH МИ-8
            
            -- Ресурсы МИ-17
            `ll_mi17` Nullable(Float64),            -- LL МИ-17
            `oh_mi17` Nullable(Float64),            -- OH МИ-17
            
            -- Стоимостные характеристики
            `repair_price` Nullable(Float64),       -- Цена ремонта
            `purchase_price` Nullable(Float64),     -- Цена покупки
            
            -- Дополнительные ресурсы
            `sne` Nullable(Float64),                -- SNE
            `ppr` Nullable(Float64),                -- PPR
            
            -- Метаданные файла
            `version_date` Date DEFAULT today()     -- Дата версии
            
        ) ENGINE = MergeTree()
        ORDER BY version_date
        PARTITION BY toYYYYMM(version_date)
        SETTINGS index_granularity = 8192
        """
        
        client.execute(create_sql)
        print("✅ Таблица md_components готова")
        
    except Exception as e:
        print(f"❌ Ошибка создания таблицы md_components: {e}")
        sys.exit(1)

def check_version_conflicts(client, version_date):
    """Проверяет конфликты версий"""
    try:
        count = client.execute(f"SELECT COUNT(*) FROM md_components WHERE version_date = '{version_date}'")[0][0]
        
        if count > 0:
            print(f"\n🚨 НАЙДЕНЫ ДАННЫЕ С ИДЕНТИЧНОЙ ДАТОЙ ВЕРСИИ!")
            print(f"   Дата версии: {version_date}")
            print(f"   md_components: {count:,} записей")
            print(f"\nВыберите действие:")
            print(f"   1. ЗАМЕНИТЬ существующие данные (DELETE + INSERT)")
            print(f"   2. ОТМЕНИТЬ загрузку")
            
            while True:
                try:
                    choice = input(f"\nВаш выбор (1-2): ").strip()
                    if choice == '1':
                        print(f"🔄 Удаляем существующие данные за {version_date}...")
                        client.execute(f"DELETE FROM md_components WHERE version_date = '{version_date}'")
                        print(f"✅ Удалено {count:,} записей из md_components")
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

def insert_md_data(client, df):
    """Загружает данные MD_Components в таблицу"""
    try:
        print(f"🚀 Загружаем {len(df):,} записей в md_components...")
        
        # Конвертируем в список кортежей
        data_tuples = [tuple(row) for row in df.values]
        
        # Загружаем
        client.execute('INSERT INTO md_components VALUES', data_tuples)
        
        print(f"✅ Загружено {len(data_tuples):,} записей в md_components")
        return len(data_tuples)
        
    except Exception as e:
        print(f"❌ Ошибка загрузки в md_components: {e}")
        return 0

def validate_md_data(client, version_date, original_count):
    """Проверка качества загруженных данных MD_Components"""
    print(f"\n🔍 === ПРОВЕРКА КАЧЕСТВА MD_COMPONENTS ===")
    
    # Проверяем в БД
    db_count = client.execute(f"SELECT COUNT(*) FROM md_components WHERE version_date = '{version_date}'")[0][0]
    
    print(f"📊 Исходный Excel файл: {original_count:,} записей")
    print(f"📊 md_components: {db_count:,} записей")
    
    # Проверки качества
    issues = []
    
    if db_count != original_count:
        issues.append(f"❌ Количество записей: ожидали {original_count:,}, получили {db_count:,}")
    
    # Проверяем уникальные партномера
    unique_partnos_result = client.execute(f"SELECT COUNT(DISTINCT partno) FROM md_components WHERE version_date = '{version_date}'")
    unique_partnos = unique_partnos_result[0][0]
    
    print(f"📦 Уникальных партномеров: {unique_partnos}")
    
    # Проверяем заполненность ключевых полей
    key_fields_check = client.execute(f"""
        SELECT 
            SUM(CASE WHEN partno IS NOT NULL AND partno != '' THEN 1 ELSE 0 END) as filled_partno,
            SUM(CASE WHEN comp_number IS NOT NULL THEN 1 ELSE 0 END) as filled_comp_number
        FROM md_components WHERE version_date = '{version_date}'
    """)
    
    filled_partno, filled_comp_number = key_fields_check[0]
    
    print(f"📋 Заполненность ключевых полей:")
    print(f"   partno: {filled_partno}/{db_count} ({filled_partno/db_count*100:.1f}%)")
    print(f"   comp_number: {filled_comp_number}/{db_count} ({filled_comp_number/db_count*100:.1f}%)")
    
    if filled_partno < db_count * 0.9:  # Менее 90% заполненности
        issues.append(f"❌ Низкая заполненность partno: {filled_partno/db_count*100:.1f}%")
    
    # Результат проверки
    if issues:
        print(f"\n⚠️ Обнаружены проблемы:")
        for issue in issues:
            print(f"  {issue}")
        return False
    else:
        print(f"\n✅ Все проверки пройдены успешно!")
        print(f"✅ Уникальных компонентов: {unique_partnos}")
        print(f"✅ Качество данных: высокое")
        return True

def main():
    """Основная функция"""
    print("🚀 === ЗАГРУЗЧИК MD_COMPONENTS ===")
    
    try:
        # 1. Подключение к ClickHouse (безопасная конфигурация)
        sys.path.append(str(Path(__file__).parent))
        from utils.config_loader import get_clickhouse_client
        client = get_clickhouse_client()
        
        # 2. Создание таблицы
        create_md_table(client)
        
        # 3. Загрузка исходных данных
        df = load_md_components()
        original_count = len(df)
        
        # 4. Определение версии данных из метаданных Excel
        md_path = Path('data_input/master_data/MD_Сomponents.xlsx')
        version_date = extract_version_date_from_excel(md_path)
        print(f"🗓️ Версия данных: {version_date}")
        
        # 5. Проверка конфликтов версий
        if not check_version_conflicts(client, version_date):
            return
        
        # 6. Подготовка данных
        prepared_df = prepare_md_data(df, version_date)
        
        # 7. Загрузка данных
        print(f"\n🚀 === НАЧИНАЕМ ЗАГРУЗКУ MD_COMPONENTS ===")
        
        loaded_count = insert_md_data(client, prepared_df)
        
        # 8. Проверка результатов
        if loaded_count > 0:
            print(f"\n🎉 === ЗАГРУЗКА MD_COMPONENTS ЗАВЕРШЕНА ===")
            
            validation_success = validate_md_data(client, version_date, original_count)
            
            if validation_success:
                print(f"\n🎯 === ИТОГОВАЯ СТАТИСТИКА ===")
                print(f"📅 Версия данных: {version_date}")
                print(f"📊 md_components: {loaded_count:,} записей")
                print(f"🔍 Проверки качества: ✅ ПРОЙДЕНЫ")
            else:
                print(f"\n⚠️ Загрузка завершена, но обнаружены проблемы качества")
                
        else:
            print(f"💥 Загрузка завершена с ошибками!")
            
    except Exception as e:
        print(f"💥 Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
