#!/usr/bin/env python3
"""
Простой загрузчик Status_Components.xlsx в ClickHouse
с фильтрацией по партномерам из MD_Components.xlsx

Функционал:
1. Читает список партномеров из MD_Components.xlsx
2. Фильтрует Status_Components.xlsx по этим партномерам
3. Загружает отфильтрованные данные в ClickHouse

Использование:
    python3 simple_loader.py
"""

import pandas as pd
import clickhouse_connect
import sys
from pathlib import Path
from datetime import datetime
import yaml

def load_config():
    """Загружает конфигурацию ClickHouse"""
    try:
        with open('config/database_config.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)['clickhouse']
        return config
    except Exception as e:
        print(f"❌ Ошибка загрузки конфигурации: {e}")
        sys.exit(1)

def get_md_partnos():
    """Читает список партномеров из MD_Components.xlsx"""
    try:
        md_path = Path('data_input/master_data/MD_Сomponents.xlsx')
        
        if not md_path.exists():
            print(f"❌ Файл {md_path} не найден")
            sys.exit(1)
        
        # Читаем MD_Components
        df = pd.read_excel(md_path, sheet_name='Агрегаты', header=7, engine='openpyxl')
        
        # Очищаем и получаем партномера
        df_clean = df.dropna(subset=['Чертежный номер'])
        df_clean = df_clean[df_clean['Чертежный номер'] != 'partno']
        
        partnos_raw = df_clean['Чертежный номер'].dropna().unique()
        
        # Разворачиваем многострочные партномера (ctrl+enter)
        all_partnos = []
        for partno in partnos_raw:
            if isinstance(partno, str):
                subpartnos = [p.strip() for p in partno.split('\n') if p.strip()]
                all_partnos.extend(subpartnos)
            else:
                all_partnos.append(str(partno).strip())
        
        unique_partnos = sorted(list(set(all_partnos)))
        print(f"✅ Загружено {len(unique_partnos)} партномеров из MD_Components")
        return unique_partnos
        
    except Exception as e:
        print(f"❌ Ошибка чтения MD_Components: {e}")
        sys.exit(1)

def load_status_components():
    """Загружает и фильтрует Status_Components.xlsx"""
    try:
        status_path = Path('data_input/source_data/Status_Components.xlsx')
        
        if not status_path.exists():
            print(f"❌ Файл {status_path} не найден")
            sys.exit(1)
        
        print(f"📖 Загружаем {status_path}...")
        
        # Загружаем с Arrow backend если доступен
        try:
            df = pd.read_excel(status_path, header=0, engine='openpyxl', dtype_backend="pyarrow")
            print("⚡ Используется Arrow backend")
        except:
            df = pd.read_excel(status_path, header=0, engine='openpyxl')
        
        print(f"📊 Исходно: {len(df):,} записей")
        
        # Получаем список партномеров для фильтрации
        md_partnos = get_md_partnos()
        
        # Фильтруем по партномерам
        if 'partno' not in df.columns:
            print("❌ Столбец 'partno' не найден в Status_Components")
            sys.exit(1)
        
        filtered_df = df[df['partno'].isin(md_partnos)].copy()
        print(f"📊 После фильтрации: {len(filtered_df):,} записей")
        
        if len(filtered_df) == 0:
            print("❌ После фильтрации не осталось данных")
            sys.exit(1)
        
        # Показываем найденные партномера
        found_partnos = filtered_df['partno'].value_counts()
        print(f"🔍 Найдено {len(found_partnos)} уникальных партномеров:")
        for partno, count in found_partnos.head(10).items():
            print(f"   📦 {partno}: {count:,} записей")
        if len(found_partnos) > 10:
            print(f"   ... и еще {len(found_partnos)-10} партномеров")
        
        return filtered_df
        
    except Exception as e:
        print(f"❌ Ошибка загрузки Status_Components: {e}")
        sys.exit(1)

def prepare_data(df):
    """Подготавливает данные для ClickHouse"""
    try:
        # Добавляем версию данных
        version_date = datetime.now().date()
        df['version_date'] = version_date
        
        # Базовая очистка данных
        # Заменяем NaN на None для ClickHouse
        df = df.where(pd.notnull(df), None)
        
        # Обработка дат
        date_columns = ['mfg_date', 'removal_date', 'target_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
                df[col] = df[col].dt.date
        
        # Обработка числовых полей
        numeric_columns = ['oh', 'oh_threshold', 'll', 'sne', 'ppr']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(0).clip(lower=0).astype('UInt32')
        
        print(f"✅ Данные подготовлены для загрузки")
        print(f"📅 Версия данных: {version_date}")
        
        return df
        
    except Exception as e:
        print(f"❌ Ошибка подготовки данных: {e}")
        sys.exit(1)

def create_table(client):
    """Создает таблицу в ClickHouse если не существует"""
    try:
        create_sql = """
        CREATE TABLE IF NOT EXISTS heli_raw (
            partno String,
            serialno String,
            ac_typ String,
            location String,
            mfg_date Date,
            removal_date Date,
            target_date Date,
            condition String,
            owner String,
            lease_restricted String,
            oh UInt32,
            oh_threshold UInt32,
            ll UInt32,
            sne UInt32,
            ppr UInt32,
            version_date Date
        ) ENGINE = MergeTree()
        ORDER BY (partno, serialno)
        PARTITION BY toYYYYMM(version_date)
        """
        
        client.execute(create_sql)
        print("✅ Таблица heli_raw готова")
        
    except Exception as e:
        print(f"❌ Ошибка создания таблицы: {e}")
        sys.exit(1)

def insert_data(client, df):
    """Загружает данные в ClickHouse"""
    try:
        print(f"🚀 Начинаем загрузку {len(df):,} записей...")
        
        # Конвертируем в список кортежей
        data_tuples = [tuple(row) for row in df.values]
        
        # Загружаем
        client.execute('INSERT INTO heli_raw VALUES', data_tuples)
        
        print(f"✅ Загружено {len(data_tuples):,} записей")
        
        # Проверяем результат
        result = client.execute("SELECT COUNT(*) FROM heli_raw WHERE version_date = today()")
        print(f"🔍 В таблице: {result[0][0]:,} записей с сегодняшней датой")
        
    except Exception as e:
        print(f"❌ Ошибка загрузки: {e}")
        sys.exit(1)

def main():
    """Основная функция"""
    print("🚀 === ПРОСТОЙ ЗАГРУЗЧИК STATUS_COMPONENTS ===")
    
    try:
        # 1. Подключение к ClickHouse
        config = load_config()
        client = clickhouse_connect.get_client(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            username=config['username'],
            password=config['password']
        )
        print(f"✅ Подключились к ClickHouse: {config['host']}:{config['port']}")
        
        # 2. Создание таблицы
        create_table(client)
        
        # 3. Загрузка и фильтрация данных
        df = load_status_components()
        
        # 4. Подготовка данных
        prepared_df = prepare_data(df)
        
        # 5. Загрузка в ClickHouse
        insert_data(client, prepared_df)
        
        print("🎉 === ЗАГРУЗКА ЗАВЕРШЕНА ===")
        
    except Exception as e:
        print(f"💥 Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 