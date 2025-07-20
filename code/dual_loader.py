#!/usr/bin/env python3
"""
Двойной загрузчик Status_Components.xlsx в ClickHouse

Функционал:
1. Загружает ВСЕ данные в ClickHouse таблицу 'heli_raw' 
2. Одновременно фильтрует по MD_Components и загружает в 'heli_pandas'
3. Сохраняет версионность и метаданные Excel
4. Проверяет соответствие количества записей
5. Диалоги перезаписи существующих данных

Улучшения v2.1:
- lease_restricted оптимизировано до UInt8 (Y/1→1, остальное→0)

Использование:
    python3 dual_loader.py
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
import yaml
import openpyxl
import os
import time

# Безопасная конфигурация через utils.config_loader

def extract_version_date_from_excel(file_path):
    """Извлекает дату версии из метаданных Excel файла с проверкой корректности года"""
    try:
        print("📅 Определение версии данных из Excel метаданных...")
        
        # Открываем файл для чтения метаданных
        workbook = openpyxl.load_workbook(file_path, read_only=True)
        props = workbook.properties
        
        version_datetime = None
        source_type = None
        current_year = datetime.now().year
        
        # Приоритет 1: Дата создания файла (если не старше года)
        if props.created:
            created_year = props.created.year
            if abs(created_year - current_year) <= 1:  # Не старше года
                version_datetime = props.created
                source_type = "Excel created"
                print(f"📅 Дата создания Excel: {version_datetime}")
            else:
                print(f"⚠️ Дата создания {props.created} отличается от текущего года более чем на год, используем дату модификации")
                
        # Приоритет 2: Дата модификации (если создание некорректно или отсутствует)
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
        
        # Дополнительная информация
        file_size = os.path.getsize(file_path)
        file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
        print(f"📋 Файл: {os.path.basename(file_path)}")
        print(f"📏 Размер: {file_size:,} байт")
        print(f"🕐 Модификация ОС: {file_mtime}")
        print(f"🎯 Источник версии: {source_type}")
        
        # Возвращаем только дату (без времени) для совместимости
        return version_datetime.date()
        
    except Exception as e:
        print(f"⚠️ Ошибка извлечения метаданных Excel: {e}")
        
        # Fallback к дате файла
        try:
            file_mtime = os.path.getmtime(file_path)
            version_datetime = datetime.fromtimestamp(file_mtime)
            print(f"📅 Fallback: используем время модификации файла: {version_datetime}")
            return version_datetime.date()
        except Exception as fallback_error:
            print(f"❌ Критическая ошибка определения версии: {fallback_error}")
            # Последний fallback - сегодняшняя дата
            version_date = datetime.now().date()
            print(f"🚨 Экстренный fallback: используем сегодняшнюю дату: {version_date}")
            return version_date

def get_md_partnos(client):
    """Читает список партномеров из таблицы md_components в ClickHouse"""
    try:
        print("📋 Загружаем список партномеров из таблицы md_components...")
        
        # Проверяем наличие таблицы
        check_table_query = "SELECT COUNT(*) FROM system.tables WHERE name = 'md_components'"
        table_exists = client.execute(check_table_query)[0][0] > 0
        
        if not table_exists:
            print("❌ Таблица md_components не найдена в ClickHouse!")
            print("💡 Сначала запустите: python3 code/md_components_loader.py")
            sys.exit(1)
        
        # Получаем все партномера из таблицы
        query = """
        SELECT DISTINCT partno 
        FROM md_components 
        WHERE partno IS NOT NULL 
        AND partno != ''
        ORDER BY partno
        """
        
        result = client.execute(query)
        partnos_raw = [row[0] for row in result if row[0]]
        
        # Разворачиваем многострочные партномера (если остались после загрузки)
        all_partnos = []
        for partno in partnos_raw:
            if isinstance(partno, str):
                subpartnos = [p.strip() for p in partno.split('\n') if p.strip()]
                all_partnos.extend(subpartnos)
            else:
                all_partnos.append(str(partno).strip())
        
        unique_partnos = sorted(list(set(all_partnos)))
        print(f"✅ Загружено {len(unique_partnos)} партномеров из таблицы md_components")
        
        # Показываем статистику по версиям данных
        version_query = "SELECT version_date, COUNT(*) FROM md_components GROUP BY version_date ORDER BY version_date DESC"
        versions = client.execute(version_query)
        
        if versions:
            print("📊 Статистика данных md_components:")
            for version_date, count in versions:
                print(f"   {version_date}: {count:,} записей")
        
        return unique_partnos
        
    except Exception as e:
        print(f"❌ Ошибка чтения данных из md_components: {e}")
        print("💡 Убедитесь что данные загружены: python3 code/md_components_loader.py")
        sys.exit(1)

def load_status_components():
    """Загружает Status_Components.xlsx"""
    try:
        status_path = Path('data_input/source_data/Status_Components.xlsx')
        
        if not status_path.exists():
            print(f"❌ Файл {status_path} не найден")
            sys.exit(1)
        
        print(f"📖 Загружаем {status_path}...")
        
        # Загружаем без Arrow backend для избежания проблем с pd.NA
        df = pd.read_excel(status_path, header=0, engine='openpyxl')
        print("📖 Загружен Excel файл")
        
        # Удаляем служебную колонку "Счет" если она присутствует
        if 'Счет' in df.columns:
            df = df.drop(columns=['Счет'])
            print("🗑️ Удалена служебная колонка: Счет")
        
        print(f"📊 Загружено: {len(df):,} записей")
        return df
        
    except Exception as e:
        print(f"❌ Ошибка загрузки Status_Components: {e}")
        sys.exit(1)

def prepare_data(df, version_date, version_id=1, filter_partnos=None, table_name='heli_raw'):
    """Подготавливает данные для ClickHouse"""
    try:
        # Фильтрация если нужна
        if filter_partnos:
            print(f"🔍 ДИАГНОСТИКА ФИЛЬТРАЦИИ:")
            print(f"   DataFrame: {len(df):,} записей")
            print(f"   filter_partnos: {type(filter_partnos)} с {len(filter_partnos)} элементами")
            print(f"   Примеры filter_partnos: {filter_partnos[:3]}")
            
            original_count = len(df)
            
            # ДИАГНОСТИКА: проверяем тип данных в partno колонке
            print(f"   Тип данных df['partno']: {df['partno'].dtype}")
            print(f"   Примеры df['partno']: {df['partno'].head(3).tolist()}")
            
            # ОПТИМИЗАЦИЯ: конвертируем filter_partnos в set для быстрого поиска
            print(f"🔧 Конвертирую filter_partnos в set для оптимизации...")
            filter_partnos_set = set(filter_partnos)
            print(f"   Создан set с {len(filter_partnos_set)} элементами")
            
            # ДИАГНОСТИКА: показываем прогресс
            print(f"🔧 Начинаю фильтрацию .isin() для {original_count:,} записей...")
            
            # ОПТИМИЗИРОВАННАЯ фильтрация
            start_time = time.time()
            mask = df['partno'].isin(filter_partnos_set)
            filter_time = time.time() - start_time
            print(f"   ✅ .isin() завершен за {filter_time:.2f} сек")
            
            print(f"   Найдено {mask.sum():,} совпадений из {original_count:,}")
            
            # Применяем фильтр
            print(f"🔧 Применяю фильтр и создаю копию...")
            copy_start = time.time()
            df = df[mask].copy()
            copy_time = time.time() - copy_start
            print(f"   ✅ Копирование завершено за {copy_time:.2f} сек")
            
            print(f"📊 После фильтрации: {len(df):,} из {original_count:,} записей")
        
        print(f"🔧 Продолжаю обработку колонок...")
        
        # Выбираем только нужные колонки согласно схеме таблицы
        required_columns = [
            'partno', 'serialno', 'ac_typ', 'location',
            'mfg_date', 'removal_date', 'target_date',
            'condition', 'owner', 'lease_restricted',
            'oh', 'oh_threshold', 'll', 'sne', 'ppr'
        ]
        
        # Встроенные ID поля из нового Excel (если есть)
        embedded_id_columns = ['partseqno_i', 'psn', 'address_i', 'ac_type_i']
        for col in embedded_id_columns:
            if col in df.columns:
                required_columns.append(col)
        
        # Дополнительные поля status_id и aircraft_number добавляются отдельными скриптами
        # Здесь работаем только с базовыми полями из Excel
        if 'status_id' in df.columns:
            required_columns.append('status_id')
            
        if 'aircraft_number' in df.columns:
            required_columns.append('aircraft_number')
        
        # Фильтруем колонки (оставляем только те что есть в данных)
        available_columns = [col for col in required_columns if col in df.columns]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"⚠️  Отсутствующие колонки: {missing_columns}")
        
        print(f"✅ Используем колонки для {table_name}: {available_columns}")
        
        # Оставляем только нужные колонки
        df = df[available_columns].copy()
        
        # КРИТИЧНО: ДОБАВЛЯЕМ ОТСУТСТВУЮЩИЕ КОЛОНКИ С ДЕФОЛТНЫМИ ЗНАЧЕНИЯМИ!
        print(f"🔧 Добавляем отсутствующие колонки с дефолтными значениями...")
        for col in missing_columns:
            if col == 'lease_restricted':
                df[col] = 0  # UInt8 DEFAULT 0
                print(f"   ➕ {col}: 0 (UInt8)")
            elif col in ['oh', 'oh_threshold', 'll', 'sne', 'ppr']:
                # Создаем колонку с dtype object для правильной обработки None
                df[col] = pd.Series([None] * len(df), dtype=object)
                print(f"   ➕ {col}: None (Nullable UInt32)")
            elif col == 'status_id':
                df[col] = 0  # UInt8 DEFAULT 0  
                print(f"   ➕ {col}: 0 (UInt8)")
            elif col == 'aircraft_number':
                df[col] = 0  # UInt16 DEFAULT 0
                print(f"   ➕ {col}: 0 (UInt16)")
            else:
                # Строковые поля по умолчанию
                df[col] = ''
                print(f"   ➕ {col}: '' (String)")
        
        # КРИТИЧНО: порядок колонок должен соответствовать схеме таблицы!
        # Сначала добавляем version_date и version_id
        df['version_date'] = version_date
        df['version_id'] = version_id
        
        # Базовые поля в правильном порядке согласно схеме таблицы
        # Дополнительные поля (status, aircraft_number) будут добавлены отдельными скриптами
        
        # Обработка дат для ClickHouse - как в рабочем архивном проекте
        date_columns = ['mfg_date', 'removal_date', 'target_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce').dt.date
                df[col] = df[col].where(df[col].notnull(), None)
        
        # Специальная обработка version_date для ClickHouse
        if 'version_date' in df.columns:
            # version_date уже является date объектом, ничего не делаем
            pass

        # Обработка ресурсных полей - ПРОСТОЙ РАБОЧИЙ ПОДХОД как в успешных загрузчиках
        resource_columns = ['oh', 'oh_threshold', 'll', 'sne', 'ppr']
        for col in resource_columns:
            if col in df.columns:
                print(f"🔧 Обрабатываем ресурсное поле {col}...")
                
                # Проверяем есть ли вообще данные для обработки
                non_null_count = df[col].notna().sum()
                if non_null_count == 0:
                    print(f"   {col}: колонка пустая, заполняем нулями")
                    # КРИТИЧНО: заполняем пустые колонки 0 и продолжаем обработку
                    df[col] = 0
                
                # Улучшенная обработка для clickhouse_driver совместимости
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Убираем отрицательные значения
                df[col] = df[col].clip(lower=0)
                # КРИТИЧНО: для clickhouse_driver Nullable колонок используем fillna(0) вместо None
                # clickhouse_driver лучше работает с 0 чем с None для Nullable(UInt32)
                df[col] = df[col].fillna(0).astype('int64')
                
                # Статистика
                none_count = df[col].isnull().sum()
                valid_count = len(df) - none_count
                print(f"   {col}: {valid_count} валидных значений, {none_count} None")

        # Обработка встроенных ID полей из Excel
        embedded_id_columns = ['partseqno_i', 'psn', 'address_i', 'ac_type_i']
        for col in embedded_id_columns:
            if col in df.columns:
                print(f"🔧 Обрабатываем встроенное ID поле {col}...")
                
                # Конвертируем в числовой формат
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # ID поля не могут быть отрицательными
                df[col] = df[col].clip(lower=0)
                # Для Nullable полей оставляем None вместо fillna(0) чтобы сохранить информацию об отсутствии
                df[col] = df[col].where(df[col].notna(), None)
                
                # Статистика
                non_null_count = df[col].notna().sum()
                null_count = df[col].isnull().sum()
                print(f"   {col}: {non_null_count} валидных ID, {null_count} None")

        # Обработка lease_restricted - ИСПРАВЛЯЕМ ПРОБЛЕМУ С NaN
        if 'lease_restricted' in df.columns:
            # КРИТИЧНО: сначала заменяем NaN на пустую строку
            df['lease_restricted'] = df['lease_restricted'].fillna('')
            df['lease_restricted'] = df['lease_restricted'].astype(str)
            df['lease_restricted'] = df['lease_restricted'].apply(
                lambda x: 1 if x in ['Y', '1', '1.0'] else 0
            ).astype(int)
        
        # Обработка строковых полей для ClickHouse
        string_columns = ['partno', 'serialno', 'ac_typ', 'location', 'condition', 'owner']
        for col in string_columns:
            if col in df.columns:
                # Приводим к строкам и заменяем None/NaN на пустые строки
                df[col] = df[col].astype(str)
                df[col] = df[col].replace(['nan', 'None', 'NaT'], '')

        # Убираем оставшиеся NaN (заменяем на значения по умолчанию)
        # Для числовых полей NaN уже заменены на 0, для строк на '', для дат на min_date
        
        print(f"✅ Подготовлено {len(df):,} записей с {len(df.columns)} колонками")
        print(f"📋 Итоговые колонки: {list(df.columns)}")
        return df
        
    except Exception as e:
        print(f"❌ Ошибка подготовки данных: {e}")
        sys.exit(1)

def create_tables(client):
    """Создает таблицы в ClickHouse если не существуют"""
    try:
        # Таблица для всех данных (RAW) - возвращаем к рабочей схеме
        create_raw_sql = """
        CREATE TABLE IF NOT EXISTS heli_raw (
            -- Основные идентификаторы
            `partno` Nullable(String),              
            `serialno` Nullable(String),            
            `ac_typ` Nullable(String),              
            `location` Nullable(String),            
            
            -- Даты
            `mfg_date` Nullable(Date),              
            `removal_date` Nullable(Date),          
            `target_date` Nullable(Date),           
            
            -- Состояние и владение
            `condition` Nullable(String),           
            `owner` Nullable(String),               
            `lease_restricted` UInt8 DEFAULT 0,     
            
            -- Ресурсные данные
            `oh` Nullable(UInt32),                  
            `oh_threshold` Nullable(UInt32),        
            `ll` Nullable(UInt32),                  
            `sne` Nullable(UInt32),                 
            `ppr` Nullable(UInt32),                 
            
            -- Метаданные файла
            `version_date` Date DEFAULT today(),
            `version_id` UInt8 DEFAULT 1
            
        ) ENGINE = MergeTree()
        ORDER BY (version_date, version_id)
        PARTITION BY toYYYYMM(version_date)
        SETTINGS index_granularity = 8192
        """
        
        # Таблица для фильтрованных данных (PANDAS) - полная схема с обогащенными полями
        create_pandas_sql = """
        CREATE TABLE IF NOT EXISTS heli_pandas (
            -- Основные идентификаторы
            `partno` Nullable(String),              
            `serialno` Nullable(String),            
            `ac_typ` Nullable(String),              
            `location` Nullable(String),            
            
            -- Даты
            `mfg_date` Nullable(Date),              
            `removal_date` Nullable(Date),          
            `target_date` Nullable(Date),           
            
            -- Состояние и владение
            `condition` Nullable(String),           
            `owner` Nullable(String),               
            `lease_restricted` UInt8 DEFAULT 0,     
            
            -- Ресурсные данные
            `oh` Nullable(UInt32),                  
            `oh_threshold` Nullable(UInt32),        
            `ll` Nullable(UInt32),                  
            `sne` Nullable(UInt32),                 
            `ppr` Nullable(UInt32),                 
            
            -- Метаданные файла
            `version_date` Date DEFAULT today(),
            `version_id` UInt8 DEFAULT 1,
            
            -- Встроенные ID поля из Excel (новые поля вместо генерируемых словарей)
            `partseqno_i` Nullable(UInt32),         -- Встроенный ID партномера из Excel
            `psn` Nullable(UInt32),                 -- Встроенный ID серийного номера из Excel  
            `address_i` Nullable(UInt16),           -- Встроенный ID владельца из Excel
            `ac_type_i` Nullable(UInt16),           -- Встроенный ID типа ВС из Excel
            
            -- Обогащенные поля (добавляются dual_loader.py и enrich_heli_pandas.py)
            `status_id` UInt8 DEFAULT 0,            -- Статус компонента (через status_processor.py)
            `repair_days` Nullable(UInt16),         -- Остаток дней до окончания ремонта (было Int16 → uint16, без минусов)
            `aircraft_number` UInt32 DEFAULT 0,     -- Номер ВС из RA-XXXXX (расширен для самолетов)
            `ac_type_mask` UInt8 DEFAULT 0          -- Битовая маска типа ВС для multihot (через enrich_heli_pandas.py)
            
        ) ENGINE = MergeTree()
        ORDER BY (version_date, version_id)
        PARTITION BY toYYYYMM(version_date)
        SETTINGS index_granularity = 8192
        """
        
        client.execute(create_raw_sql)
        client.execute(create_pandas_sql)
        print("✅ Таблицы heli_raw и heli_pandas готовы")
        
    except Exception as e:
        print(f"❌ Ошибка создания таблиц: {e}")
        sys.exit(1)

def check_version_conflicts(client, version_date, version_id):
    """Проверяет конфликты версий с улучшенной логикой"""
    try:
        # Проверяем обе таблицы на точное совпадение версии
        raw_count = client.execute(f"SELECT COUNT(*) FROM heli_raw WHERE version_date = '{version_date}' AND version_id = {version_id}")[0][0]
        pandas_count = client.execute(f"SELECT COUNT(*) FROM heli_pandas WHERE version_date = '{version_date}' AND version_id = {version_id}")[0][0]
        
        if raw_count > 0 or pandas_count > 0:
            print(f"\n🚨 НАЙДЕНЫ ДАННЫЕ С ИДЕНТИЧНОЙ ВЕРСИЕЙ!")
            print(f"   Дата версии: {version_date}, version_id: {version_id}")
            print(f"   heli_raw: {raw_count:,} записей")
            print(f"   heli_pandas: {pandas_count:,} записей")
            print(f"\nВыберите действие:")
            print(f"   1. ЗАМЕНИТЬ существующие данные (DELETE + INSERT)")
            print(f"   2. ОТМЕНИТЬ загрузку")
            
            while True:
                try:
                    choice = input(f"\nВаш выбор (1-2): ").strip()
                    if choice == '1':
                        print(f"🔄 Удаляем существующие данные за {version_date} v{version_id}...")
                        if raw_count > 0:
                            client.execute(f"DELETE FROM heli_raw WHERE version_date = '{version_date}' AND version_id = {version_id}")
                            print(f"✅ Удалено {raw_count:,} записей из heli_raw")
                        if pandas_count > 0:
                            client.execute(f"DELETE FROM heli_pandas WHERE version_date = '{version_date}' AND version_id = {version_id}")
                            print(f"✅ Удалено {pandas_count:,} записей из heli_pandas")
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

def insert_data(client, df, table_name, description):
    """Загружает данные в указанную таблицу"""
    try:
        print(f"🚀 Загружаем {len(df):,} записей в {table_name} ({description})...")
        
        # Простая диагностика ресурсных полей (как в рабочих загрузчиках)
        resource_cols = ['oh', 'oh_threshold', 'll', 'sne', 'ppr']
        for col in resource_cols:
            if col in df.columns:
                sample_vals = df[col].dropna().head(2).tolist()
                sample_types = [type(v).__name__ for v in sample_vals]
                none_count = df[col].isnull().sum()
                print(f"🔍 {col}: примеры={sample_vals} типы={sample_types} null={none_count}")
        
        # Простой рабочий подход - как в успешных загрузчиках
        data_tuples = [tuple(row) for row in df.values]
        
        # Загружаем
        client.execute(f'INSERT INTO {table_name} VALUES', data_tuples)
        
        print(f"✅ Загружено {len(data_tuples):,} записей в {table_name}")
        return len(data_tuples)
        
    except Exception as e:
        print(f"❌ Ошибка загрузки в {table_name}: {e}")
        
        # Минимальная диагностика при ошибке
        if "sne" in str(e) and data_tuples:
            print(f"🔍 ПЕРВАЯ ПРОБЛЕМНАЯ ЗАПИСЬ:")
            sne_col_index = list(df.columns).index('sne') if 'sne' in df.columns else -1
            if sne_col_index >= 0:
                sne_value = data_tuples[0][sne_col_index]
                print(f"   sne = {sne_value} ({type(sne_value)})")
        
        return 0

def validate_data_counts(client, version_date, version_id, original_count, raw_count, pandas_count, filtered_partnos_count):
    """Минимальная проверка количества записей"""
    print(f"\n🔍 === ПРОВЕРКА КОЛИЧЕСТВА ЗАПИСЕЙ ===")
    
    # Проверяем в БД
    db_raw_count = client.execute(f"SELECT COUNT(*) FROM heli_raw WHERE version_date = '{version_date}' AND version_id = {version_id}")[0][0]
    db_pandas_count = client.execute(f"SELECT COUNT(*) FROM heli_pandas WHERE version_date = '{version_date}' AND version_id = {version_id}")[0][0]
    
    print(f"📊 Исходный Excel файл: {original_count:,} записей")
    print(f"📊 heli_raw (все данные): {db_raw_count:,} записей")
    print(f"📊 heli_pandas (фильтрованные): {db_pandas_count:,} записей")
    print(f"📦 Фильтр по {filtered_partnos_count} партномерам из MD_Components")
    
    # Проверки
    issues = []
    
    if db_raw_count != original_count:
        issues.append(f"❌ heli_raw: ожидали {original_count:,}, получили {db_raw_count:,}")
    
    if db_pandas_count == 0:
        issues.append(f"❌ heli_pandas: не найдено записей с партномерами из MD_Components")
    
    if db_pandas_count > db_raw_count:
        issues.append(f"❌ heli_pandas больше чем heli_raw - логическая ошибка")
    
    # Проверяем уникальные партномера в pandas
    unique_partnos_result = client.execute(f"SELECT COUNT(DISTINCT partno) FROM heli_pandas WHERE version_date = '{version_date}' AND version_id = {version_id}")
    unique_partnos_in_db = unique_partnos_result[0][0]
    
    print(f"📦 Уникальных партномеров в heli_pandas: {unique_partnos_in_db}")
    
    if unique_partnos_in_db > filtered_partnos_count:
        issues.append(f"❌ Найдено больше партномеров ({unique_partnos_in_db}) чем в MD_Components ({filtered_partnos_count})")
    
    # Результат проверки
    if issues:
        print(f"\n⚠️ Обнаружены проблемы:")
        for issue in issues:
            print(f"  {issue}")
        return False
    else:
        print(f"\n✅ Все проверки пройдены успешно!")
        print(f"✅ Покрытие партномеров: {unique_partnos_in_db}/{filtered_partnos_count} ({unique_partnos_in_db/filtered_partnos_count*100:.1f}%)")
        print(f"✅ Фильтрация: {db_pandas_count/db_raw_count*100:.1f}% записей прошли фильтр")
        return True

# Функция add_aircraft_number_in_memory перенесена в aircraft_number_processor.py

# Функция add_status_in_memory удалена - заменена на status_processor.py

def main(version_date=None, version_id=None):
    """Основная функция с поддержкой версионирования"""
    print("🚀 === ДВОЙНОЙ ЗАГРУЗЧИК STATUS_COMPONENTS ===")
    start_time = time.time()
    
    try:
        # 1. Подключение к ClickHouse через безопасную систему
        print(f"🔗 [ЭТАП 1] Подключение к ClickHouse...")
        import sys
        sys.path.append(str(Path(__file__).parent))
        from utils.config_loader import get_clickhouse_client
        client = get_clickhouse_client()
        print(f"✅ [ЭТАП 1] Подключение установлено за {time.time() - start_time:.2f}с")
        
        # 2. Создание таблиц
        print(f"🏗️ [ЭТАП 2] Создание таблиц...")
        step_start = time.time()
        create_tables(client)
        print(f"✅ [ЭТАП 2] Таблицы созданы за {time.time() - step_start:.2f}с")
        
        # 3. Загрузка исходных данных
        print(f"📖 [ЭТАП 3] Загрузка Excel файла...")
        step_start = time.time()
        df = load_status_components()
        original_count = len(df)
        print(f"✅ [ЭТАП 3] Excel загружен за {time.time() - step_start:.2f}с: {original_count:,} записей")
        
        # 4. Определение версии данных
        print(f"🗓️ [ЭТАП 4] Определение версии...")
        step_start = time.time()
        if version_date is None:
            # Автоматическое извлечение из метаданных Excel (совместимость)
            status_path = Path('data_input/source_data/Status_Components.xlsx')
            version_date = extract_version_date_from_excel(status_path)
            print(f"✅ [ЭТАП 4] Версия определена (из Excel): {version_date}")
        else:
            print(f"✅ [ЭТАП 4] Версия получена (из ETL): {version_date}, version_id: {version_id}")
        
        if version_id is None:
            version_id = 1
        print(f"✅ [ЭТАП 4] Завершен за {time.time() - step_start:.2f}с")
        
        # 5. Проверка конфликтов версий с диалогом
        print(f"🔍 [ЭТАП 5] Проверка конфликтов версий...")
        step_start = time.time()
        if not check_version_conflicts(client, version_date, version_id):
            return
        print(f"✅ [ЭТАП 5] Конфликты проверены за {time.time() - step_start:.2f}с")
        
        # 6. Получение списка партномеров из MD_Components для фильтрации
        print(f"📦 [ЭТАП 6] Получение партномеров из MD_Components...")
        step_start = time.time()
        md_partnos = get_md_partnos(client)
        print(f"✅ [ЭТАП 6] Получено {len(md_partnos)} партномеров за {time.time() - step_start:.2f}с")
        print(f"📋 Первые 10 партномеров: {md_partnos[:10]}")
        if len(md_partnos) > 10:
            print(f"📋 ... и еще {len(md_partnos)-10} партномеров")
        
        # 7. Подготовка данных для обеих таблиц
        print(f"\n📦 [ЭТАП 7] Подготовка данных для загрузки...")
        step_start = time.time()
        
        # Все данные для RAW
        print(f"🔧 [ЭТАП 7a] Подготовка данных для heli_raw...")
        raw_start = time.time()
        raw_df = prepare_data(df.copy(), version_date, version_id=version_id, table_name='heli_raw')
        print(f"✅ [ЭТАП 7a] heli_raw подготовлен за {time.time() - raw_start:.2f}с: {len(raw_df):,} записей")
        
        # КРИТИЧНО: Упорядочиваем колонки для heli_raw согласно схеме (17 полей)
        raw_column_order = [
            'partno', 'serialno', 'ac_typ', 'location',
            'mfg_date', 'removal_date', 'target_date',
            'condition', 'owner', 'lease_restricted',
            'oh', 'oh_threshold', 'll', 'sne', 'ppr',
            'version_date', 'version_id'
        ]
        
        # Проверяем и упорядочиваем колонки для raw
        missing_raw_columns = [col for col in raw_column_order if col not in raw_df.columns]
        if missing_raw_columns:
            print(f"❌ Отсутствующие колонки в heli_raw: {missing_raw_columns}")
        else:
            raw_df = raw_df[raw_column_order]
            print(f"✅ heli_raw: порядок колонок установлен ({len(raw_df.columns)} полей)")
        
        # Фильтрованные данные для PANDAS (САМЫЙ ТЯЖЕЛЫЙ ЭТАП!)
        print(f"🔧 [ЭТАП 7b] Подготовка данных для heli_pandas (фильтрация)...")
        pandas_start = time.time()
        pandas_df = prepare_data(df.copy(), version_date, version_id=version_id, filter_partnos=md_partnos, table_name='heli_pandas')
        print(f"✅ [ЭТАП 7b] heli_pandas подготовлен за {time.time() - pandas_start:.2f}с: {len(pandas_df):,} записей")
        print(f"✅ [ЭТАП 7] Общее время подготовки: {time.time() - step_start:.2f}с")
        
        # 8. Загрузка heli_raw и обработка pandas_df в памяти
        print(f"\n🚀 [ЭТАП 8] НАЧИНАЕМ ОПТИМИЗИРОВАННУЮ ЗАГРУЗКУ")
        step_start = time.time()
        
        # 8.1 Сразу записываем heli_raw (архивная копия - больше не нужна)
        print(f"💾 [ЭТАП 8.1] Загрузка heli_raw в ClickHouse...")
        raw_insert_start = time.time()
        raw_loaded = insert_data(client, raw_df, 'heli_raw', 'все данные')
        print(f"✅ [ЭТАП 8.1] heli_raw записана за {time.time() - raw_insert_start:.2f}с, освобождаем память")
        del raw_df  # Освобождаем память
        
        # 8.2 Обрабатываем pandas_df В ПАМЯТИ для оптимальной производительности
        print(f"\n🔧 [ЭТАП 8.2] ОБРАБОТКА PANDAS_DF В ПАМЯТИ")
        memory_start = time.time()
        
        # КРИТИЧНО: Создаем обогащенные поля ДО обработки процессорами
        print(f"🔧 [ЭТАП 8.2a] Инициализация обогащенных полей...")
        init_start = time.time()
        
        # Поле repair_days - должно быть создано ДО обработки статусов
        if 'repair_days' not in pandas_df.columns:
            pandas_df['repair_days'] = None  # Nullable Int16 поле
            print(f"   ➕ Создано поле repair_days: None (заполнится при обработке статусов)")
        
        # Поле status_id - инициализируем значением по умолчанию
        if 'status_id' not in pandas_df.columns:
            pandas_df['status_id'] = 0  # По умолчанию 0 (не определен)
            print(f"   ➕ Создано поле status_id: 0 (обновится процессорами)")
        
        print(f"✅ [ЭТАП 8.2a] Поля инициализированы за {time.time() - init_start:.2f}с")
        
        # Добавляем поле aircraft_number через извлечение из location
        print(f"🚁 Добавление aircraft_number из поля location...")
        try:
            from aircraft_number_processor import process_aircraft_numbers_in_memory
            pandas_df, aircraft_count, invalid_count = process_aircraft_numbers_in_memory(pandas_df)
            if invalid_count > 0:
                print(f"⚠️ Найдено {invalid_count} значений неправильного формата")
        except ImportError as e:
            print(f"⚠️ Модуль aircraft_number_processor не найден: {e}")
            # Fallback - добавляем пустую колонку
            if 'aircraft_number' not in pandas_df.columns:
                pandas_df['aircraft_number'] = 0
                print(f"➕ Добавлена колонка 'aircraft_number' со значением по умолчанию 0")
        except Exception as e:
            print(f"❌ Ошибка обработки aircraft_number: {e}")
            # Fallback - добавляем пустую колонку
            if 'aircraft_number' not in pandas_df.columns:
                pandas_df['aircraft_number'] = 0
                print(f"➕ Добавлена колонка 'aircraft_number' со значением по умолчанию 0 (fallback)")
        
        # Добавляем поле status_id через обработку статусов (НОВАЯ ЛОГИКА)
        print(f"📊 Обработка статусов и repair_days через систему процессоров...")
        try:
            # ЭТАП 1: Обработка статусов капремонта (status_overhaul) + repair_days
            print(f"🔧 Этап 1: Статусы капремонта + repair_days...")
            from overhaul_status_processor import process_status_field
            pandas_df = process_status_field(pandas_df, client)
            
            # ЭТАП 2: Обработка статусов эксплуатации (program_ac)
            print(f"🔧 Этап 2: Статусы эксплуатации...")
            from program_ac_status_processor import process_program_ac_status_field
            pandas_df = process_program_ac_status_field(pandas_df, client)
            
            # ЭТАП 3: Обработка статусов неактивности планеров (МИ-8Т, МИ-8П и т.д.)
            print(f"🔧 Этап 3: Статусы неактивности планеров...")
            from inactive_planery_processor import process_inactive_planery_status
            pandas_df = process_inactive_planery_status(pandas_df, client)
            
        except ImportError as e:
            print(f"⚠️ Модуль статусов не найден: {e}")
            print(f"💡 Убедитесь что созданы: overhaul_status_processor.py, program_ac_status_processor.py, inactive_planery_processor.py")
        except Exception as e:
            print(f"❌ Ошибка обработки статусов: {e}")
        
        # 8.3 Записываем финальную heli_pandas с полной структурой
        print(f"\n💾 [ЭТАП 8.3] Финальная загрузка heli_pandas...")
        final_start = time.time()
        
        print(f"🔧 [ЭТАП 8.3a] Выравнивание порядка колонок...")
        column_start = time.time()
        
        # Правильный порядок согласно схеме heli_pandas (25 полей: dual_loader создает 24 + enrich_heli_pandas заполняет ac_type_mask)
        correct_column_order = [
            'partno', 'serialno', 'ac_typ', 'location',
            'mfg_date', 'removal_date', 'target_date',
            'condition', 'owner', 'lease_restricted',
            'oh', 'oh_threshold', 'll', 'sne', 'ppr',
            'version_date', 'version_id', 'partseqno_i', 'psn', 'address_i', 'ac_type_i',
            'status_id', 'repair_days', 'aircraft_number', 'ac_type_mask'
        ]
        
        # Проверяем наличие всех колонок
        missing_columns = [col for col in correct_column_order if col not in pandas_df.columns]
        extra_columns = [col for col in pandas_df.columns if col not in correct_column_order]
        
        if missing_columns:
            print(f"⚠️ Отсутствующие колонки: {missing_columns}")
            # Добавляем отсутствующие колонки с дефолтными значениями
            for col in missing_columns:
                if col in ['status_id', 'aircraft_number', 'ac_type_mask']:
                    pandas_df[col] = 0
                    print(f"   ➕ Добавлена колонка {col}: 0")
                elif col == 'repair_days':
                    pandas_df[col] = None  # Nullable Int16 поле
                    print(f"   ➕ Добавлена колонка {col}: None")
                elif col in ['partseqno_i', 'psn', 'address_i', 'ac_type_i']:
                    pandas_df[col] = None  # Nullable UInt полея
                    print(f"   ➕ Добавлена колонка {col}: None")
                elif col in ['oh', 'oh_threshold', 'll', 'sne', 'ppr']:
                    pandas_df[col] = None
                    print(f"   ➕ Добавлена колонка {col}: None")
                elif col == 'lease_restricted':
                    pandas_df[col] = 0
                    print(f"   ➕ Добавлена колонка {col}: 0")
                else:
                    pandas_df[col] = ''
                    print(f"   ➕ Добавлена колонка {col}: ''")
        
        if extra_columns:
            print(f"⚠️ Лишние колонки: {extra_columns}")
        
        # Переупорядочиваем колонки согласно схеме
        available_columns = [col for col in correct_column_order if col in pandas_df.columns]
        pandas_df = pandas_df[available_columns]
        
        print(f"✅ [ЭТАП 8.3a] Колонки выровнены за {time.time() - column_start:.2f}с: {len(pandas_df.columns)} полей")
        print(f"📋 Колонки: {list(pandas_df.columns)}")
        
        print(f"💾 [ЭТАП 8.3b] Загрузка в ClickHouse...")
        insert_start = time.time()
        pandas_loaded = insert_data(client, pandas_df, 'heli_pandas', 'фильтрованные + обогащенные')
        print(f"✅ [ЭТАП 8.3b] Данные загружены за {time.time() - insert_start:.2f}с")
        print(f"✅ [ЭТАП 8.3] Финальная загрузка завершена за {time.time() - final_start:.2f}с")
        print(f"✅ [ЭТАП 8] Общее время загрузки: {time.time() - step_start:.2f}с")
        
        # 9. Проверка результатов
        if raw_loaded > 0 and pandas_loaded > 0:
            print(f"\n🎉 === ЗАГРУЗКА ЗАВЕРШЕНА УСПЕШНО ===")
            
            # Минимальная проверка количества записей
            validation_success = validate_data_counts(
                client, version_date, version_id, original_count, 
                raw_loaded, pandas_loaded, len(md_partnos)
            )
            
            if validation_success:
                print(f"\n🎯 === ИТОГОВАЯ СТАТИСТИКА ===")
                print(f"📅 Версия данных: {version_date}")
                print(f"📊 heli_raw: {raw_loaded:,} записей (все данные)")
                print(f"📊 heli_pandas: {pandas_loaded:,} записей (фильтрованные)")
                print(f"⚡ Улучшенная версионность с проверкой года")
                print(f"🔍 Проверки качества: ✅ ПРОЙДЕНЫ")
                print(f"⏱️ ОБЩЕЕ ВРЕМЯ ВЫПОЛНЕНИЯ: {time.time() - start_time:.2f}с")
            else:
                print(f"\n⚠️ Загрузка завершена, но обнаружены проблемы качества")
                print(f"⏱️ ОБЩЕЕ ВРЕМЯ ВЫПОЛНЕНИЯ: {time.time() - start_time:.2f}с")
                
        else:
            print(f"💥 Загрузка завершена с ошибками!")
            print(f"⏱️ ОБЩЕЕ ВРЕМЯ ВЫПОЛНЕНИЯ: {time.time() - start_time:.2f}с")
            
    except Exception as e:
        print(f"💥 [КРИТИЧЕСКАЯ ОШИБКА] {e}")
        print(f"⏱️ ВРЕМЯ ДО ОШИБКИ: {time.time() - start_time:.2f}с")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Dual Loader для Helicopter Component Lifecycle')
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
 