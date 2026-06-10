#!/usr/bin/env python3
"""
Загрузчик MD_Components.xlsx в ClickHouse

SSoT: data_input/master_data/MD_Сomponents.xlsx → таблица md_components.

Функционал:
1. Пустая md_components (после DROP) — полная загрузка всех строк из Excel
2. Непустая md_components — INSERT новых partno + UPDATE существующих из Excel SSoT
3. Проверяет качество и целостность данных
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
import yaml
import openpyxl
import os
import math
import numpy as np

# Конфигурация теперь загружается через utils.config_loader

# Функция extract_version_date_from_excel удалена - используется общая utils.version_utils.extract_unified_version_date()

def to_int_or_none(v):
    """Преобразует значение в int или None для Nullable полей"""
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, (np.floating,)) and pd.isna(v):
        return None
    return int(v)

def to_nullable_int_series(s):
    """Возвращает object Series с Python int или None для ClickHouse Nullable(UInt*)"""
    return pd.Series([to_int_or_none(v) for v in s], index=s.index, dtype='object')

def load_md_components():
    """Загружает MD_Components.xlsx"""
    try:
        md_path = Path('data_input/master_data/MD_Сomponents.xlsx')
        
        if not md_path.exists():
            print(f"❌ Файл {md_path} не найден")
            sys.exit(1)
        
        print(f"📖 Загружаем {md_path}...")
        
        # Загружаем с правильным header (вторая строка)
        # Пробуем сначала лист 'Агрегаты', если нет — первый лист
        try:
            df = pd.read_excel(md_path, sheet_name='Агрегаты', header=1, engine='openpyxl')
        except ValueError:
            df = pd.read_excel(md_path, sheet_name=0, header=1, engine='openpyxl')
        print("📖 Загружен Excel файл")
        
        # Удаляем служебную колонку "Счет" если она присутствует
        if 'Счет' in df.columns:
            df = df.drop(columns=['Счет'])
            print(f"🗑️ Удалена служебная колонка: Счет")
        
        print(f"📊 Загружено: {len(df):,} записей с {len(df.columns)} колонками")
        print(f"📋 Колонки: {list(df.columns)}")
        return df
        
    except Exception as e:
        print(f"❌ Ошибка загрузки MD_Components: {e}")
        sys.exit(1)

def prepare_md_data(df, version_date, version_id=1):
    """Подготавливает данные MD_Components для ClickHouse"""
    try:
        print(f"📦 Подготовка данных MD_Components...")
        
        # Добавляем версию данных
        df['version_date'] = version_date
        df['version_id'] = version_id

        ssot_required_columns = ['partseqno_i', 'psn_spawn_start']
        missing_ssot_columns = [col for col in ssot_required_columns if col not in df.columns]
        if missing_ssot_columns:
            print(
                "❌ SSoT Excel MD_Сomponents.xlsx должен содержать обязательные поля: "
                f"{missing_ssot_columns}"
            )
            sys.exit(1)
        
        # Обработка строковых полей для ClickHouse
        string_columns = ['partno']
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)
                df[col] = df[col].replace(['nan', 'None', 'NaT'], '')
                # Очищаем переносы строк в partno
                if col == 'partno':
                    df[col] = df[col].str.replace('\n', '', regex=False)
        
        # Обработка ac_type_mask (переименование и преобразование в UInt8)
        if 'ac_type_mask' in df.columns:
            # Преобразуем в числовой формат (32, 64, 96)
            df['ac_type_mask'] = pd.to_numeric(df['ac_type_mask'], errors='coerce')
            df['ac_type_mask'] = df['ac_type_mask'].clip(lower=0, upper=255)
            df['ac_type_mask'] = df['ac_type_mask'].fillna(0).astype('int64')
            print(f"   🔧 ac_type_mask: UInt8 (маски типов ВС: 32, 64, 96)")

        # Обработка числовых полей с валидацией диапазонов для GPU-оптимизированных типов
        
        # UInt8 поля (0-255)
        uint8_columns = [
            'comp_number', 'group_by', 'type_restricted', 'common_restricted1', 'common_restricted2',
            'trigger_interval', 'partout_time', 'assembly_time', 'ac_type_mask'
        ]
        
        # UInt8 Nullable поля (0-255, NULL сохраняется)
        uint8_nullable_columns = ['repair_number']
        
        # UInt16 поля (0-65535)
        uint16_columns = ['repair_time']
        
        # UInt32 поля (0-4294967295)
        uint32_columns = [
            'll_mi8', 'oh_mi8', 'oh_threshold_mi8', 'll_mi17', 'oh_mi17',
            'second_ll', 'br2_mi17', 'psn_spawn_start'
        ]
        
        # Float32 поля (денежные поля оптимизированы для GPU)  
        float32_columns = ['repair_price', 'purchase_price']
        
        # UInt32 поля (переименованные поля, оптимизированы Float64→UInt32)
        uint32_sne_ppr_columns = ['sne_new', 'ppr_new']
        
        # Обработка UInt8 полей
        for col in uint8_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].clip(lower=0, upper=255)  # Валидация диапазона UInt8
                df[col] = df[col].fillna(0).astype('int64')  # Стандартный паттерн как в dual_loader.py
                print(f"   🔧 {col}: UInt8 (0-255)")
        
        # Обработка UInt8 Nullable полей (NULL сохраняется)
        for col in uint8_nullable_columns:
            if col in df.columns:
                s = pd.to_numeric(df[col], errors='coerce')
                # Клипуем только непустые значения
                s = s.clip(lower=0, upper=255)
                df[col] = to_nullable_int_series(s)
                print(f"   🔧 {col}: UInt8 Nullable (NULL сохранён)")
        
        # Обработка UInt16 полей
        for col in uint16_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].clip(lower=0, upper=65535)  # Валидация диапазона UInt16
                df[col] = df[col].fillna(0).astype('int64')  # Стандартный паттерн как в dual_loader.py
                print(f"   🔧 {col}: UInt16 (0-65535)")
        
        # Конвертация часов → минут для ресурсных полей (требование проекта)
        # Поля источника в часах: ll_mi8, ll_mi17, oh_mi8, oh_mi17, sne_new, ppr_new → в Env храним в минутах
        # br2_mi17 - порог для межремонтного ресурса (breakeven для ppr/oh)
        hours_to_minutes_cols = ['ll_mi8', 'll_mi17', 'oh_mi8', 'oh_mi17', 'second_ll', 'br2_mi17']
        for col in hours_to_minutes_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Перевод часов в минуты с округлением до целого и безопасной заменой NaN→0
                df[col] = (df[col] * 60).round().fillna(0).astype('int64')
                print(f"   ⏱ {col}: конвертировано часы→минуты")
        
        # Конвертация часов → минут для sne_new, ppr_new (с сохранением NULL)
        # ⚠️ ВАЖНО: NULL остаётся NULL (признак "агрегат не выпускается")
        hours_to_minutes_nullable_cols = ['sne_new', 'ppr_new']
        for col in hours_to_minutes_nullable_cols:
            if col in df.columns:
                s = pd.to_numeric(df[col], errors='coerce')
                # Перевод часов в минуты только для непустых значений
                s = s * 60
                # Округляем и сохраняем NULL
                df[col] = s.round()
                print(f"   ⏱ {col}: конвертировано часы→минуты (NULL сохранён)")

        # Обработка UInt32 полей
        for col in uint32_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].clip(lower=0, upper=4294967295)  # Валидация диапазона UInt32
                df[col] = df[col].fillna(0).astype('int64')  # Стандартный паттерн как в dual_loader.py
                print(f"   🔧 {col}: UInt32 (0-4294967295)")
        
        # Обработка Float32 полей (оптимизированы для GPU)
        for col in float32_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].where(df[col].notnull(), None).astype('float32')
                print(f"   🔧 {col}: Float32 (GPU-оптимизированный)")
        
        # Обработка UInt32 полей для sne_new, ppr_new (оптимизированы Float64→UInt32)
        # ⚠️ ВАЖНО: Сохраняем NULL для sne_new/ppr_new (признак "агрегат не выпускается")
        # Конвертация часы→минуты уже выполнена выше, здесь только клипуем и преобразуем в int
        for col in uint32_sne_ppr_columns:
            if col in df.columns:
                # df[col] уже содержит значения в минутах после конвертации выше
                # Клипуем только непустые значения
                s = df[col].clip(lower=0, upper=4294967295)
                df[col] = to_nullable_int_series(s)
                print(f"   🔧 {col}: UInt32 Nullable (NULL сохранён, значения в минутах)")

        # partseqno_i приходит из Excel SSoT как штатное поле AMOS-ID компонента.
        s = pd.to_numeric(df['partseqno_i'], errors='coerce').clip(lower=0, upper=4294967295)
        df['partseqno_i'] = to_nullable_int_series(s)
        print("   🔧 partseqno_i: UInt32 Nullable (SSoT Excel, NULL сохранён)")

        # Добавляем дополнительные поля для совместимости с полной схемой таблицы
        if 'br_mi8' not in df.columns:
            df['br_mi8'] = None  # BR для МИ-8 будет вычислен позже
            print("➕ Добавлено поле br_mi8 = None (будет вычислено позже)")

        if 'br_mi17' not in df.columns:
            df['br_mi17'] = None  # BR для МИ-17 будет вычислен позже
            print("➕ Добавлено поле br_mi17 = None (будет вычислено позже)")

        # br2_mi17 - порог межремонтного для подъёма из inactive
        # Если поле есть в Excel - оно уже обработано выше (часы→минуты)
        # Если нет - добавляем как None
        if 'br2_mi17' not in df.columns:
            df['br2_mi17'] = None
            print("➕ Добавлено поле br2_mi17 = None")

        # Добавляем поле restrictions_mask (битовая маска ограничений)
        if 'restrictions_mask' not in df.columns:
            # Простой расчет: type_restricted + common_restricted1*2 + common_restricted2*4 + trigger_interval*8
            df['restrictions_mask'] = (
                df['type_restricted'].fillna(0).astype(int) * 1 +
                df['common_restricted1'].fillna(0).astype(int) * 2 +
                df['common_restricted2'].fillna(0).astype(int) * 4 +
                df['trigger_interval'].fillna(0).astype(int) * 8
            ).astype('int64')
            print("➕ Добавлено поле restrictions_mask (битовая маска ограничений)")
            
            # Диагностика
            mask_min = df['restrictions_mask'].min()
            mask_max = df['restrictions_mask'].max()
            unique_masks = sorted(df['restrictions_mask'].unique())
            print(f"   📊 restrictions_mask: диапазон {mask_min}-{mask_max}, уникальные: {unique_masks}")

        # Приводим порядок колонок к порядку DDL ClickHouse
        column_order = [
            'partno',
            'comp_number', 'group_by', 'ac_type_mask',
            'type_restricted', 'common_restricted1', 'common_restricted2',
            'trigger_interval', 'partout_time', 'assembly_time', 'repair_number', 'repair_time',
            'll_mi8', 'oh_mi8', 'oh_threshold_mi8',
            'll_mi17', 'oh_mi17', 'second_ll',
            'repair_price', 'purchase_price',
            'sne_new', 'ppr_new',
            'version_date', 'version_id',
            'br_mi8', 'br_mi17', 'br2_mi17',
            'partseqno_i', 'psn_spawn_start', 'restrictions_mask'
        ]

        missing_ssot_columns = [col for col in ssot_required_columns if col not in df.columns]
        if missing_ssot_columns:
            print(
                "❌ SSoT Excel MD_Сomponents.xlsx потерял обязательные поля при подготовке: "
                f"{missing_ssot_columns}"
            )
            sys.exit(1)

        # Гарантируем наличие производных/служебных колонок, кроме обязательных SSoT-полей
        for col in column_order:
            if col not in df.columns:
                df[col] = None

        # Установим 0 для non-nullable с DEFAULT 0, если вдруг отсутствуют
        if 'restrictions_mask' in df.columns:
            df['restrictions_mask'] = df['restrictions_mask'].fillna(0).astype('int64')

        # Переупорядочим
        df = df[column_order]

        print(f"📊 Подготовлено {len(df):,} записей с {len(df.columns)} колонками (выравнено под DDL)")
        return df
        
    except Exception as e:
        print(f"❌ Ошибка подготовки данных MD_Components: {e}")
        sys.exit(1)

def create_md_table(client):
    """Создает таблицу md_components в ClickHouse (если не существует)
    
    ВАЖНО: В PROD режиме таблица НЕ удаляется — md_components является
    универсальным справочником номенклатур для всех датасетов.
    """
    try:
        # Проверяем существует ли таблица
        table_exists = client.execute("EXISTS TABLE md_components")[0][0]
        
        if table_exists:
            print("✅ Таблица md_components уже существует, пропускаем создание")
            return
        
        print("📝 Создание таблицы md_components...")
        create_sql = """
        CREATE TABLE IF NOT EXISTS md_components (
            -- Основные идентификаторы
            `partno` Nullable(String),              -- Чертежный номер
            `comp_number` Nullable(UInt8),          -- Количество на ВС (было Float64 → uint8)
            `group_by` Nullable(UInt8),             -- Группировка
            `ac_type_mask` Nullable(UInt8),         -- Тип ВС (маска: 32, 64, 96)
            
            -- Ограничения (оптимизированы для GPU)
            `type_restricted` Nullable(UInt8),      -- Ограничение по типу (было Float64 → uint8 multihot)
            `common_restricted1` Nullable(UInt8),   -- Общее ограничение 1 (было Float64 → uint8 multihot)
            `common_restricted2` Nullable(UInt8),   -- Общее ограничение 2 (было Float64 → uint8 multihot)
            
            -- Временные характеристики (оптимизированы для GPU)
            `trigger_interval` Nullable(UInt8),     -- Интервал срабатывания (было Float64 → uint8)
            `partout_time` Nullable(UInt8),         -- Время снятия (было Float64 → uint8)
            `assembly_time` Nullable(UInt8),        -- Время установки (было Float64 → uint8)
            `repair_number` Nullable(UInt8),        -- Объем ремонта (номер квоты, NULL сохраняется)
            `repair_time` Nullable(UInt16),         -- Время ремонта (было Float64 → uint16)
            
            -- Ресурсы МИ-8 (оптимизированы для GPU)
            `ll_mi8` Nullable(UInt32),              -- LL МИ-8 (было Float64 → uint32)
            `oh_mi8` Nullable(UInt32),              -- OH МИ-8 (было Float64 → uint32)
            `oh_threshold_mi8` Nullable(UInt32),    -- Порог OH МИ-8 (было Float64 → uint32)
            
            -- Ресурсы МИ-17 (оптимизированы для GPU)
            `ll_mi17` Nullable(UInt32),             -- LL МИ-17 (было Float64 → uint32)
            `oh_mi17` Nullable(UInt32),             -- OH МИ-17 (было Float64 → uint32)
            `second_ll` Nullable(UInt32),           -- Дополнительный ресурс (часы→минуты)
            
            -- Стоимостные характеристики (оптимизированы для GPU)
            `repair_price` Nullable(Float32),       -- Цена ремонта (было Float64 → float32)
            `purchase_price` Nullable(Float32),     -- Цена покупки (было Float64 → float32)
            
            -- Дополнительные ресурсы (переименованные поля, оптимизированы для GPU)
            `sne_new` Nullable(UInt32),             -- SNE (переименовано из sne, Float64→UInt32)
            `ppr_new` Nullable(UInt32),             -- PPR (переименовано из ppr, Float64→UInt32)
            
            -- Метаданные файла
            `version_date` Date DEFAULT today(),    -- Дата версии
            `version_id` UInt8 DEFAULT 1,           -- ID версии
            
            -- Дополнительные поля (обогащенные с GPU-оптимизацией)
            `br_mi8` Nullable(UInt32) DEFAULT NULL,     -- Beyond Repair для МИ-8 (sne/ll breakeven)
            `br_mi17` Nullable(UInt32) DEFAULT NULL,    -- Beyond Repair для МИ-17 (sne/ll breakeven)
            `br2_mi17` Nullable(UInt32) DEFAULT NULL,   -- Порог межремонтного для МИ-17 (ppr/oh breakeven, 3500ч)
            `partseqno_i` Nullable(UInt32) DEFAULT NULL,  -- Component ID из Excel SSoT
            `psn_spawn_start` UInt32 DEFAULT 0,      -- Старт PSN-диапазона spawn из Excel SSoT
            `restrictions_mask` UInt8 DEFAULT 0     -- Битовая маска всех ограничений (multihot[u8])
            
        ) ENGINE = MergeTree()
        ORDER BY (version_date, version_id)
        PARTITION BY toYYYYMM(version_date)
        SETTINGS index_granularity = 8192
        """
        
        client.execute(create_sql)
        print("✅ Таблица md_components готова")
        
    except Exception as e:
        print(f"❌ Ошибка создания таблицы md_components: {e}")
        sys.exit(1)

def check_version_conflicts(client, version_date, version_id):
    """Проверяет состояние справочника md_components
    
    ВАЖНО: md_components — ЕДИНЫЙ справочник номенклатур.
    Нет дублирования по version_date — проверка конфликтов не нужна.
    Возвращаем True всегда (добавление новых partno обрабатывается в insert_md_data).
    """
    try:
        total_count = client.execute("SELECT COUNT(*) FROM md_components")[0][0]
        unique_partnos = client.execute("SELECT COUNT(DISTINCT partno) FROM md_components")[0][0]
        
        print(f"📚 Справочник md_components: {total_count} записей, {unique_partnos} уникальных partno")
        if total_count == 0:
            print("   ℹ️ Таблица пуста — будет полная загрузка из Excel SSoT")
        else:
            print("   ℹ️ SSoT sync: INSERT новых partno + UPDATE существующих из Excel")
        return True
            
    except Exception as e:
        print(f"❌ Ошибка проверки md_components: {e}")
        return False

NULLABLE_INSERT_COLUMNS = {
    'sne_new', 'ppr_new', 'br_mi8', 'br_mi17', 'br2_mi17', 'partseqno_i', 'repair_number',
}


def _escape_partno(partno: str) -> str:
    return str(partno).replace("'", "''")


def _format_ch_value(val, col_name: str) -> str:
    if val is None or (isinstance(val, float) and math.isnan(val)) or pd.isna(val):
        if col_name in NULLABLE_INSERT_COLUMNS:
            return "NULL"
        if col_name == 'restrictions_mask':
            return "0"
        if col_name == 'psn_spawn_start':
            return "0"
        return "NULL"
    if col_name == 'version_date':
        return f"toDate('{val}')"
    if isinstance(val, (np.integer, int)):
        return str(int(val))
    if isinstance(val, (np.floating, float)):
        return str(float(val))
    escaped = str(val).replace("'", "''")
    return f"'{escaped}'"


def _prepare_insert_rows(df: pd.DataFrame) -> list[tuple]:
    data_tuples = []
    for _, row in df.iterrows():
        row_list = [None if pd.isna(val) else val for val in row]
        data_tuples.append(tuple(row_list))

    prepared_data = []
    for row in data_tuples:
        prepared_row = []
        for i, val in enumerate(row):
            col_name = df.columns[i]
            if val is None and col_name in NULLABLE_INSERT_COLUMNS:
                prepared_row.append(None)
            else:
                prepared_row.append(val)
        prepared_data.append(tuple(prepared_row))
    return prepared_data


def _insert_rows(client, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    prepared_data = _prepare_insert_rows(df)
    columns = list(df.columns)
    insert_query = f"INSERT INTO md_components ({', '.join(columns)}) VALUES"
    client.execute(insert_query, prepared_data)
    return len(prepared_data)


def _sync_ssot_rows(client, df: pd.DataFrame) -> int:
    """UPDATE существующих partno значениями из Excel SSoT (без дублирования строк)."""
    if df.empty:
        return 0

    partnos_sql = ", ".join(f"'{_escape_partno(partno)}'" for partno in df['partno'])
    # version_date/version_id — ключи ORDER BY MergeTree, UPDATE запрещён ClickHouse
    sync_columns = [
        col for col in df.columns if col not in {'partno', 'version_date', 'version_id'}
    ]

    for col in sync_columns:
        cases = []
        for _, row in df.iterrows():
            partno = _escape_partno(row['partno'])
            value_sql = _format_ch_value(row[col], col)
            cases.append(f"WHEN partno = '{partno}' THEN {value_sql}")
        update_sql = (
            f"ALTER TABLE md_components UPDATE {col} = CASE {' '.join(cases)} ELSE {col} END "
            f"WHERE partno IN ({partnos_sql})"
        )
        client.execute(update_sql, settings={"mutations_sync": 1})

    return len(df)


def insert_md_data(client, df):
    """Синхронизирует md_components с Excel SSoT.

    - Пустая таблица (после DROP): полный INSERT всех строк Excel.
    - Иначе: INSERT только новых partno + UPDATE существующих из Excel.
    """
    try:
        print("📚 Синхронизация md_components с Excel SSoT...")

        result = client.execute("SELECT DISTINCT partno FROM md_components WHERE partno IS NOT NULL")
        existing_partnos = {row[0] for row in result}
        print(f"   📋 В таблице уже есть {len(existing_partnos)} уникальных partno")

        if not existing_partnos:
            print(f"🚀 md_components пуста — полная загрузка {len(df):,} номенклатур из Excel SSoT")
            inserted = _insert_rows(client, df)
            print(f"✅ Загружено {inserted:,} записей в md_components")
            return inserted

        df_new = df[~df['partno'].isin(existing_partnos)].copy()
        df_existing = df[df['partno'].isin(existing_partnos)].copy()

        inserted = _insert_rows(client, df_new)
        synced = _sync_ssot_rows(client, df_existing)

        print(
            f"✅ SSoT sync завершён: inserted={inserted:,}, updated={synced:,}, "
            f"excel_total={len(df):,}"
        )
        return len(df)

    except Exception as e:
        print(f"❌ Ошибка загрузки в md_components: {e}")
        return 0

def validate_md_data(client, version_date, version_id, original_count):
    """Проверка качества ЕДИНОГО справочника MD_Components
    
    md_components — единый справочник без дублей.
    Проверяем общее состояние, не по version_date.
    """
    print(f"\n🔍 === ПРОВЕРКА КАЧЕСТВА MD_COMPONENTS ===")
    
    # Проверяем ОБЩЕЕ количество в БД (единый справочник)
    db_count = client.execute("SELECT COUNT(*) FROM md_components")[0][0]
    unique_partnos = client.execute("SELECT COUNT(DISTINCT partno) FROM md_components")[0][0]
    
    print(f"📊 Исходный Excel файл: {original_count:,} номенклатур")
    print(f"📊 md_components ВСЕГО: {db_count:,} записей, {unique_partnos} уникальных partno")
    
    # Проверки качества
    issues = []
    
    # Справочник должен содержать не меньше записей чем в Excel
    if unique_partnos < original_count:
        issues.append(f"⚠️ В справочнике меньше номенклатур ({unique_partnos}) чем в Excel ({original_count})")
    
    # Проверяем заполненность ключевых полей
    key_fields_check = client.execute("""
        SELECT 
            SUM(CASE WHEN partno IS NOT NULL AND partno != '' THEN 1 ELSE 0 END) as filled_partno,
            SUM(CASE WHEN comp_number IS NOT NULL THEN 1 ELSE 0 END) as filled_comp_number
        FROM md_components
    """)
    
    filled_partno, filled_comp_number = key_fields_check[0]
    
    if db_count > 0:
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
        print(f"✅ Справочник содержит {unique_partnos} уникальных номенклатур")
        print(f"✅ Качество данных: высокое")
        return True

def main(version_date=None, version_id=None):
    """Основная функция с поддержкой версионирования"""
    print("🚀 === ЗАГРУЗЧИК MD_COMPONENTS ===")
    
    try:
        # 1. Подключение к ClickHouse (безопасная конфигурация)
        code_root = Path(__file__).resolve().parents[1]
        sys.path.append(str(code_root))
        sys.path.append(str(code_root / 'utils'))
        from utils.config_loader import get_clickhouse_client
        client = get_clickhouse_client()
        
        # 2. Создание таблицы
        create_md_table(client)
        
        # 3. Загрузка исходных данных
        df = load_md_components()
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
        prepared_df = prepare_md_data(df, version_date, version_id)
        
        # 7. Загрузка данных
        print(f"\n🚀 === НАЧИНАЕМ ЗАГРУЗКУ MD_COMPONENTS ===")
        
        loaded_count = insert_md_data(client, prepared_df)
        
        # 8. Проверка результатов
        if loaded_count > 0:
            print(f"\n🎉 === ЗАГРУЗКА MD_COMPONENTS ЗАВЕРШЕНА ===")
            
            validation_success = validate_md_data(client, version_date, version_id, original_count)
            
            if validation_success:
                print(f"\n🎯 === ИТОГОВАЯ СТАТИСТИКА ===")
                print(f"📅 Версия данных: {version_date} (version_id={version_id})")
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
    import argparse
    
    parser = argparse.ArgumentParser(description='MD Components Loader для Helicopter Component Lifecycle')
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
 