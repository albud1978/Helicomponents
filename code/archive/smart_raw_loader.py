#!/usr/bin/env python3
"""
Умная загрузка Status_Components.xlsx в RAW слой ClickHouse
Helicopter Component Lifecycle Prediction Project

🚀 НОВАЯ ФУНКЦИОНАЛЬНОСТЬ v2.1:
• Интерактивный выбор режима загрузки: ВСЕ данные или ОБОРОТНЫЕ агрегаты (37 партномеров)
• Детальная статистика по партномерам и фильтрации данных  
• Проверка качества отфильтрованных данных

🔧 ОСНОВНЫЕ ВОЗМОЖНОСТИ:
• Arrow оптимизации (dtype_backend="pyarrow") для ускорения transfers
• Защита от дублирования и версионное управление данных
• Комплексная валидация качества данных
• Интеллектуальное определение версии из Excel метаданных
• Batch-загрузка с мониторингом производительности

🎯 ОБОРОТНЫЕ АГРЕГАТЫ (37 партномеров):
ТОП-37 реальных оборотных агрегатов по количеству записей в системе:
- 8А-5104-317-9: 1,135 записей
- П-1 3ПМ.844.004: 1,039 записей
- 8АТ-2710-00: 1,012 записей
- МУ-615А СЕР.1 ИВКЛ.401261.001: 995 записей
- И еще 33 самых активных агрегата...
"""
import os
import sys
import pandas as pd
import numpy as np
from clickhouse_driver import Client
import logging
from datetime import datetime, date
import time
import traceback
from pathlib import Path

def setup_logging():
    """Настройка детального логирования"""
    os.makedirs('test_output', exist_ok=True)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    logger = logging.getLogger('smart_loader')
    logger.setLevel(logging.DEBUG)
    
    # Убираем существующие обработчики
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Файловый обработчик
    file_handler = logging.FileHandler('test_output/smart_loader.log', mode='w')
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

def check_table_exists(client, table_name, logger):
    """Проверка существования таблицы"""
    try:
        result = client.execute(f"EXISTS TABLE {table_name}")
        exists = result[0][0] == 1
        if exists:
            logger.info(f"📋 Таблица {table_name} существует")
            
            # Получаем статистику существующих данных
            stats = client.execute(f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT version_date) as unique_dates,
                    MIN(version_date) as min_date,
                    MAX(version_date) as max_date
                FROM {table_name}
            """)
            
            if stats and stats[0][0] > 0:
                row = stats[0]
                logger.info(f"📊 Существующие данные:")
                logger.info(f"  📄 Всего записей: {row[0]:,}")
                logger.info(f"  📅 Версий данных: {row[1]}")
                logger.info(f"  📆 Период: {row[2]} - {row[3]}")
            else:
                logger.info("📭 Таблица пуста")
        else:
            logger.info(f"❌ Таблица {table_name} не существует")
        
        return exists
    except Exception as e:
        logger.error(f"Ошибка при проверке таблицы: {e}")
        return False

def create_table_if_not_exists(client, table_name, logger):
    """Создание таблицы только если она не существует"""
    if check_table_exists(client, table_name, logger):
        return True
    
    logger.info(f"🔧 Создание таблицы {table_name}...")
    
    create_table_sql = """
    CREATE TABLE heli_raw (
        -- Основные идентификаторы
        `partno` Nullable(String),              -- Партномер (чертежный номер)
        `serialno` Nullable(String),            -- Серийный номер (как строка)
        `ac_typ` Nullable(String),              -- Тип воздушного судна
        `location` Nullable(String),            -- Местоположение/позиция
        
        -- Даты
        `mfg_date` Nullable(Date),              -- Дата изготовления
        `removal_date` Nullable(Date),          -- Дата снятия
        `target_date` Nullable(Date),           -- Целевая дата
        
        -- Состояние и владение
        `condition` Nullable(String),           -- Состояние компонента
        `owner` Nullable(String),               -- Владелец
        `lease_restricted` Nullable(String),    -- Ограничения по лизингу
        
        -- Ресурсные данные (UInt32 для оптимальной GPU обработки)
        `oh` Nullable(UInt32),                  -- МРР агрегата (межремонтный ресурс в часах)
        `oh_threshold` Nullable(UInt32),        -- Порог МРР (в часах)
        `ll` Nullable(UInt32),                  -- НР агрегата (назначенный ресурс в часах)
        `sne` Nullable(UInt32),                 -- Наработка с начала эксплуатации (в часах)
        `ppr` Nullable(UInt32),                 -- Наработка после последнего ремонта (в часах)
        
        -- Метаданные файла
        `version_date` Date DEFAULT today()     -- Дата версии файла из метаданных Excel
        
    ) ENGINE = MergeTree()
    ORDER BY version_date
    PARTITION BY toYYYYMM(version_date)
    SETTINGS index_granularity = 8192
    """
    
    try:
        client.execute(create_table_sql)
        logger.info(f"✅ Таблица {table_name} создана успешно!")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка создания таблицы: {e}")
        return False

def check_data_changes(client, table_name, new_df, version_date, logger):
    """
    Проверка реальных изменений данных перед созданием новой версии
    
    Возвращает:
    - True: данные изменились, нужна новая версия
    - False: данные не изменились, загрузка не нужна
    """
    logger.info("🔍 === АНАЛИЗ ИЗМЕНЕНИЙ ДАННЫХ ===")
    
    try:
        # Получаем последнюю версию данных из ClickHouse
        latest_version_query = f"""
            SELECT version_date, COUNT(*) as record_count
            FROM {table_name} 
            GROUP BY version_date 
            ORDER BY version_date DESC 
            LIMIT 1
        """
        
        latest_version_result = client.execute(latest_version_query)
        
        if not latest_version_result:
            logger.info("📝 Первая загрузка - данных в таблице нет")
            return True
        
        latest_version, latest_count = latest_version_result[0]
        logger.info(f"📊 Последняя версия: {latest_version} ({latest_count:,} записей)")
        
        # Быстрая проверка по количеству записей
        new_count = len(new_df)
        logger.info(f"📊 Новые данные: {new_count:,} записей")
        
        if new_count != latest_count:
            logger.info(f"✅ Обнаружено изменение количества записей: {latest_count:,} → {new_count:,}")
            return True
        
        # Детальная проверка контрольных сумм критичных полей
        logger.info("🔍 Проверка контрольных сумм критичных полей...")
        
        # Подготовим новые данные для сравнения (только ключевые поля)
        compare_fields = ['partno', 'serialno', 'sne', 'ppr', 'condition']
        available_compare_fields = [field for field in compare_fields if field in new_df.columns]
        
        if not available_compare_fields:
            logger.warning("⚠️  Нет полей для сравнения - создаем новую версию")
            return True
        
        logger.info(f"📋 Поля для сравнения: {available_compare_fields}")
        
        # Получаем контрольные суммы из ClickHouse для последней версии
        checksum_fields = []
        for field in available_compare_fields:
            if field in ['sne', 'ppr']:  # Числовые поля
                checksum_fields.append(f"round(sum({field}), 2) as sum_{field}")
                checksum_fields.append(f"count(distinct {field}) as distinct_{field}")
            else:  # Строковые поля
                checksum_fields.append(f"count(distinct {field}) as distinct_{field}")
                checksum_fields.append(f"length(groupArray({field})) as count_{field}")
        
        if checksum_fields:
            checksum_query = f"""
                SELECT {', '.join(checksum_fields)}
                FROM {table_name} 
                WHERE version_date = '{latest_version}'
            """
            
            try:
                old_checksums = client.execute(checksum_query)[0]
                logger.info(f"📊 Контрольные суммы БД: {old_checksums}")
                
                # Вычисляем контрольные суммы для новых данных
                new_checksums = []
                for field in available_compare_fields:
                    if field in ['sne', 'ppr'] and field in new_df.columns:
                        # Числовые поля
                        field_sum = round(new_df[field].sum(), 2) if not new_df[field].isna().all() else 0
                        field_distinct = new_df[field].nunique()
                        new_checksums.extend([field_sum, field_distinct])
                    elif field in new_df.columns:
                        # Строковые поля  
                        field_distinct = new_df[field].nunique()
                        field_count = len(new_df[field])
                        new_checksums.extend([field_distinct, field_count])
                
                new_checksums = tuple(new_checksums)
                logger.info(f"📊 Контрольные суммы новых данных: {new_checksums}")
                
                # Сравниваем контрольные суммы
                if old_checksums == new_checksums:
                    logger.info("✅ Контрольные суммы совпадают - данные не изменились")
                    return False
                else:
                    logger.info("🔄 Контрольные суммы различаются - обнаружены изменения")
                    
                    # Детализируем различия
                    checksum_names = []
                    for field in available_compare_fields:
                        if field in ['sne', 'ppr']:
                            checksum_names.extend([f"sum_{field}", f"distinct_{field}"])
                        else:
                            checksum_names.extend([f"distinct_{field}", f"count_{field}"])
                    
                    for i, (old_val, new_val, name) in enumerate(zip(old_checksums, new_checksums, checksum_names)):
                        if old_val != new_val:
                            logger.info(f"   🔍 {name}: {old_val} → {new_val}")
                    
                    return True
                    
            except Exception as e:
                logger.warning(f"⚠️  Ошибка вычисления контрольных сумм: {e}")
                logger.info("💡 Создаем новую версию для безопасности")
                return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка проверки изменений: {e}")
        logger.info("💡 Создаем новую версию для безопасности")
        return True

def extract_version_date_from_excel(file_path, logger):
    """
    Умное извлечение даты версии из метаданных Excel файла
    
    Приоритет источников версии:
    1. Дата создания файла (props.created) - основной источник
    2. Дата модификации (props.modified) - резервный
    3. Время модификации файла (file mtime) - fallback
    """
    try:
        import openpyxl
        from datetime import datetime
        
        logger.info("📅 Определение версии данных из Excel метаданных...")
        
        # Открываем файл для чтения метаданных
        workbook = openpyxl.load_workbook(file_path, read_only=True)
        props = workbook.properties
        
        version_date = None
        source_type = None
        
        # Приоритет 1: Дата создания (когда файл был впервые создан)
        if props.created:
            version_date = props.created.date()
            source_type = "Excel свойство 'created'"
            logger.info(f"📅 Дата создания Excel: {version_date} ({props.created})")
            
        # Приоритет 2: Дата последней модификации в метаданных
        elif props.modified:
            version_date = props.modified.date()
            source_type = "Excel свойство 'modified'"
            logger.info(f"📅 Дата модификации Excel: {version_date} ({props.modified})")
            
        # Приоритет 3: Время модификации файла в ОС
        else:
            import os
            file_mtime = os.path.getmtime(file_path)
            version_date = datetime.fromtimestamp(file_mtime).date()
            source_type = "время модификации файла ОС"
            logger.warning(f"⚠️  Excel метаданные недоступны")
            logger.info(f"📅 Время модификации файла: {version_date}")
        
        workbook.close()
        
        # Дополнительная информация для отладки
        try:
            import os
            file_size = os.path.getsize(file_path)
            file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
            logger.info(f"📋 Файл: {os.path.basename(file_path)}")
            logger.info(f"📏 Размер: {file_size:,} байт")
            logger.info(f"🕐 Модификация ОС: {file_mtime}")
            logger.info(f"🎯 Источник версии: {source_type}")
        except:
            pass
        
        return version_date
        
    except Exception as e:
        logger.warning(f"⚠️  Ошибка извлечения метаданных Excel: {e}")
        
        # Fallback к дате файла
        try:
            import os
            from datetime import datetime
            file_mtime = os.path.getmtime(file_path)
            version_date = datetime.fromtimestamp(file_mtime).date()
            logger.info(f"📅 Fallback: используем время модификации файла: {version_date}")
            return version_date
        except Exception as fallback_error:
            from datetime import datetime
            logger.error(f"❌ Критическая ошибка определения версии: {fallback_error}")
            # Последний fallback - сегодняшняя дата
            version_date = datetime.now().date()
            logger.warning(f"🚨 Экстренный fallback: используем сегодняшнюю дату: {version_date}")
            return version_date

def load_excel_data(file_path, logger):
    """Загрузка данных из Excel файла с оптимизированным парсингом"""
    logger.info(f"📖 Загрузка Excel файла: {file_path}")
    
    try:
        # Оптимизированное чтение с Arrow backend для максимальной производительности
        try:
            # Используем dtype_backend="pyarrow" - столбцы сразу станут Arrow-массивами
            df = pd.read_excel(
                file_path, 
                header=0, 
                engine='openpyxl',
                dtype_backend="pyarrow"  # 🚀 Arrow backend для ускорения
            )
            logger.info(f"📊 Загружено строк: {len(df):,}")
            logger.info(f"📊 Столбцов: {len(df.columns)}")
            logger.info(f"⚡ Используется Arrow backend для оптимальной производительности")
            
        except Exception as e:
            logger.warning(f"Fallback к стандартному чтению без Arrow backend: {e}")
            df = pd.read_excel(file_path, header=0)
            logger.info(f"📊 Загружено строк: {len(df):,}")
            logger.info(f"📊 Столбцов: {len(df.columns)}")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки Excel: {e}")
        raise

def get_repairable_partno_list():
    """
    Динамически читает список агрегатов из справочника MD_Components.xlsx
    Возвращает актуальный список партномеров для фильтрации данных
    
    Returns:
        list: Список партномеров из справочника MD_Components
    """
    import os
    from pathlib import Path
    
    try:
        # Путь к справочнику MD_Components
        md_components_path = Path('data_input/master_data/MD_Сomponents.xlsx')
        
        if not md_components_path.exists():
            print(f"⚠️ Файл {md_components_path} не найден, используем fallback список")
            # Fallback к основным агрегатам если файл не найден
            return [
                '8АТ-2710-00', '8АТ.2710.000', 'КАУ-115АМ', 'КАУ-30Б', 
                'АИ-9В', '8-1930-000 СЕР.02', '8-1960-000', '8-1950-000'
            ]
        
        # Читаем MD_Components.xlsx
        df = pd.read_excel(
            md_components_path, 
            sheet_name='Агрегаты', 
            header=7,  # Русские заголовки на строке 8
            engine='openpyxl'
        )
        
        # Очищаем от пустых значений и заголовков
        df_clean = df.dropna(subset=['Чертежный номер'])
        df_clean = df_clean[df_clean['Чертежный номер'] != 'partno']  # Убираем заголовок
        
        partnos_raw = df_clean['Чертежный номер'].dropna().unique()
        
        # Разворачиваем многострочные партномера (ctrl+enter в Excel)
        all_partnos = []
        for partno in partnos_raw:
            if isinstance(partno, str):
                # Разбиваем по переносам строк (\n)
                subpartnos = [p.strip() for p in partno.split('\n') if p.strip()]
                all_partnos.extend(subpartnos)
            else:
                # Обрабатываем нестроковые значения
                all_partnos.append(str(partno).strip())
        
        # Убираем дубликаты и сортируем
        unique_partnos = sorted(list(set(all_partnos)))
        
        print(f"✅ Загружено {len(unique_partnos)} партномеров из MD_Components.xlsx")
        print(f"📋 Первые 5: {unique_partnos[:5]}")
        
        return unique_partnos
        
    except Exception as e:
        print(f"❌ Ошибка чтения MD_Components.xlsx: {e}")
        print("🔄 Используем резервный список агрегатов")
        # Резервный список основных агрегатов
        return [
            '8АТ-2710-00', '8АТ.2710.000', 'КАУ-115АМ', 'КАУ-30Б', 'АИ-9В',
            '8-1930-000 СЕР.02', '8-1960-000', '8-1950-000', '8-3904-000 СЕРИЯ 06',
            '246-3904-000 СЕРИИ 01', '8-3922-00', '246-3925-00', 'РА-60Б',
            '246-1517-000', '8М-1517-000', '8А-1515-000', '8М-1515-000',
            '8А-1516-000', '8М-1516-000', '8А-6314-00', '8АТ.6314.000'
        ]

def filter_by_repairable_partnos(df, logger, enable_filter=False):
    """
    Фильтрация данных по динамическому списку агрегатов из MD_Components.xlsx
    
    Args:
        df: DataFrame с данными
        logger: логгер
        enable_filter: True для включения фильтрации, False для загрузки всех данных
        
    Returns:
        Отфильтрованный DataFrame
    """
    if not enable_filter:
        logger.info("🔄 Фильтрация по партномерам ОТКЛЮЧЕНА - загружаем ВСЕ данные")
        return df
    
    if 'partno' not in df.columns:
        logger.warning("⚠️  Столбец 'partno' не найден - пропускаем фильтрацию")
        return df
    
    repairable_partnos = get_repairable_partno_list()
    logger.info(f"🎯 ВКЛЮЧЕНА фильтрация по {len(repairable_partnos)} оборотным агрегатам")
    
    # Исходное количество записей
    original_count = len(df)
    logger.info(f"📊 Исходное количество записей: {original_count:,}")
    
    # Фильтрация
    filtered_df = df[df['partno'].isin(repairable_partnos)].copy()
    filtered_count = len(filtered_df)
    
    # Статистика фильтрации
    logger.info(f"📊 После фильтрации: {filtered_count:,} записей")
    if original_count > 0:
        filter_percentage = (filtered_count / original_count) * 100
        logger.info(f"📊 Отфильтровано: {filter_percentage:.1f}% от исходных данных")
    
    # Детальная статистика по найденным партномерам
    if filtered_count > 0:
        found_partnos = filtered_df['partno'].unique()
        logger.info(f"🔍 Найдено {len(found_partnos)} уникальных партномеров из {len(repairable_partnos)} в списке:")
        
        partno_counts = filtered_df['partno'].value_counts()
        for partno in sorted(found_partnos):
            count = partno_counts[partno]
            logger.info(f"   📦 {partno}: {count:,} записей")
        
        # Показываем какие партномера НЕ найдены
        missing_partnos = set(repairable_partnos) - set(found_partnos)
        if missing_partnos:
            logger.info(f"❌ НЕ найдено {len(missing_partnos)} оборотных агрегатов:")
            for partno in sorted(missing_partnos):
                logger.info(f"   🚫 {partno}")
    else:
        logger.warning("⚠️  НИ ОДИН из оборотных агрегатов не найден в данных!")
        logger.info("💡 Проверьте корректность списка партномеров")
    
    return filtered_df

def prepare_data_for_clickhouse(df, version_date, logger, enable_partno_filter=False):
    """Подготовка данных для загрузки в ClickHouse с Arrow оптимизациями"""
    logger.info("🔧 Подготовка данных для ClickHouse...")
    
    # Проверяем используется ли Arrow backend
    is_arrow_backend = hasattr(df.dtypes.iloc[0], '__array_function__') if len(df.dtypes) > 0 else False
    if is_arrow_backend:
        logger.info("⚡ Обнаружен Arrow backend - оптимизированные преобразования")
    
    # Создаем копию DataFrame
    result_df = df.copy()
    
    # НОВАЯ ФУНКЦИОНАЛЬНОСТЬ: Фильтрация по оборотным агрегатам
    result_df = filter_by_repairable_partnos(result_df, logger, enable_partno_filter)
    
    # Целевые колонки (те что есть в данных)
    target_columns = [
        'partno', 'serialno', 'ac_typ', 'location',
        'mfg_date', 'removal_date', 'target_date', 
        'condition', 'owner', 'lease_restricted',
        'oh', 'oh_threshold', 'll', 'sne', 'ppr'
    ]
    
    # Логируем доступные колонки для отладки
    logger.info(f"🔍 Доступные колонки в Excel: {list(df.columns)}")
    
    # Проверяем какие целевые колонки доступны в файле
    available_target_columns = [col for col in target_columns if col in result_df.columns]
    logger.info(f"📋 Доступные целевые колонки: {available_target_columns}")
    
    # Оставляем только нужные колонки (которые есть)
    result_df = result_df[available_target_columns].copy()
    
    # Добавляем version_date
    result_df['version_date'] = version_date
    
    # Обработка дат для ClickHouse (всегда нужна)
    date_columns = ['mfg_date', 'removal_date', 'target_date']
    for col in date_columns:
        if col in result_df.columns:
            result_df[col] = pd.to_datetime(result_df[col], dayfirst=True, errors='coerce')
            # Конвертируем в date для ClickHouse
            result_df[col] = result_df[col].dt.date
    
    # Обработка ресурсных полей (UInt32 для GPU оптимизации)
    resource_columns = ['oh', 'oh_threshold', 'll', 'sne', 'ppr']
    for col in resource_columns:
        if col in result_df.columns:
            # Конвертируем в числа, отрицательные значения обнуляем
            numeric_series = pd.to_numeric(result_df[col], errors='coerce')
            # Заменяем отрицательные значения на 0 (ресурс не может быть отрицательным)
            numeric_series = numeric_series.clip(lower=0)
            # Приводим к UInt32 для оптимальной GPU обработки
            result_df[col] = numeric_series.astype('UInt32')
    
    # Обработка строковых полей для ClickHouse
    string_columns = ['partno', 'serialno', 'ac_typ', 'location', 'condition', 'owner', 'lease_restricted']
    for col in string_columns:
        if col in result_df.columns:
            # Приводим к строкам и обрабатываем None
            result_df[col] = result_df[col].astype(str)
            result_df[col] = result_df[col].replace('nan', None)
    
    # Финальная очистка NaN для ClickHouse
    result_df = result_df.replace({np.nan: None, pd.NaT: None})
    
    if is_arrow_backend:
        logger.info("🚀 Arrow backend ускоряет преобразования для ClickHouse")
    
    logger.info(f"✅ Подготовлено {len(result_df):,} записей")
    
    # Показываем схему данных
    logger.info("📋 Схема подготовленных данных:")
    for col in result_df.columns:
        logger.info(f"  {col}: {result_df[col].dtype}")
    
    return result_df

def validate_data_quality(df, logger) -> bool:
    """
    Комплексная проверка качества данных перед загрузкой
    """
    logger.info("🔍 === ВАЛИДАЦИЯ КАЧЕСТВА ДАННЫХ ===")
    
    quality_passed = True
    
    # 1. Проверка размера DataFrame
    if df.empty:
        logger.error("❌ DataFrame пустой!")
        return False
    
    logger.info(f"📊 Общий размер DataFrame: {len(df):,} записей")
    
    # 2. Проверка критических полей
    critical_fields = ['partno', 'serialno', 'ac_typ']
    for field in critical_fields:
        if field not in df.columns:
            logger.error(f"❌ Отсутствует критическое поле: {field}")
            quality_passed = False
        else:
            null_count = df[field].isnull().sum()
            null_percent = (null_count / len(df)) * 100
            
            if null_percent > 50:  # Более 50% пустых значений - критично
                logger.error(f"❌ Критическое поле {field}: {null_percent:.1f}% пустых значений")
                quality_passed = False
            elif null_percent > 10:  # Более 10% - предупреждение
                logger.warning(f"⚠️  Поле {field}: {null_percent:.1f}% пустых значений")
            else:
                logger.info(f"✅ Поле {field}: {null_percent:.1f}% пустых значений")
    
    # 3. Проверка дубликатов
    duplicate_cols = ['partno', 'serialno']
    if all(col in df.columns for col in duplicate_cols):
        # Проверяем дубликаты по комбинации партномер+серийный
        subset_df = df.dropna(subset=duplicate_cols)
        if len(subset_df) > 0:
            duplicates = subset_df.duplicated(subset=duplicate_cols, keep=False)
            duplicate_count = duplicates.sum()
            
            if duplicate_count > 0:
                duplicate_percent = (duplicate_count / len(df)) * 100
                logger.warning(f"⚠️  Найдены дубликаты partno+serialno: {duplicate_count} ({duplicate_percent:.1f}%)")
                
                # Показываем примеры дубликатов
                duplicate_examples = subset_df[duplicates][duplicate_cols].head(5)
                logger.warning(f"📋 Примеры дубликатов:")
                for _, row in duplicate_examples.iterrows():
                    logger.warning(f"   {row['partno']} + {row['serialno']}")
            else:
                logger.info("✅ Дубликаты по partno+serialno не найдены")
    
    # 4. Проверка диапазонов ресурсных данных
    resource_fields = ['oh', 'oh_threshold', 'll', 'sne', 'ppr']
    for field in resource_fields:
        if field in df.columns:
            numeric_data = pd.to_numeric(df[field], errors='coerce')
            
            # Проверка отрицательных значений
            negative_count = (numeric_data < 0).sum()
            if negative_count > 0:
                logger.warning(f"⚠️  Поле {field}: {negative_count} отрицательных значений")
            
            # Проверка экстремальных значений (больше 50,000 часов = ~6 лет непрерывной работы)
            extreme_count = (numeric_data > 50000).sum()
            if extreme_count > 0:
                logger.warning(f"⚠️  Поле {field}: {extreme_count} экстремальных значений (>50,000 часов)")
            
            # Статистика по ненулевым значениям
            non_zero_data = numeric_data[numeric_data > 0]
            if len(non_zero_data) > 0:
                logger.info(f"📊 Поле {field}: мин={non_zero_data.min():.0f}, макс={non_zero_data.max():.0f}, медиана={non_zero_data.median():.0f}")
    
    # 5. Проверка типов ВС
    if 'ac_typ' in df.columns:
        unique_ac_types = df['ac_typ'].dropna().unique()
        logger.info(f"✈️  Типы ВС в данных ({len(unique_ac_types)}): {', '.join(unique_ac_types)}")
        
        # Проверка на неизвестные типы ВС
        known_types = ['Ми-26', 'МИ26Т', 'Ми-17', 'Ми-8Т', 'Ка-32', 'AS-350', '350B3', 'AS-355', '355NP', 'R-44']
        unknown_types = [t for t in unique_ac_types if t not in known_types]
        if unknown_types:
            logger.warning(f"⚠️  Неизвестные типы ВС: {', '.join(unknown_types)}")
    
    # 6. Проверка состояний
    if 'condition' in df.columns:
        unique_conditions = df['condition'].dropna().unique()
        logger.info(f"🔧 Состояния компонентов ({len(unique_conditions)}): {', '.join(unique_conditions)}")
        
        known_conditions = ['ИСПРАВНЫЙ', 'НЕИСПРАВНЫЙ', 'ДОНОР', 'СНЯТ ЗАКАЗЧИКОМ', 'СНЯТ', 'НЕ УСТАНОВЛЕН', 'ПОСТАВКА']
        unknown_conditions = [c for c in unique_conditions if c not in known_conditions]
        if unknown_conditions:
            logger.warning(f"⚠️  Неизвестные состояния: {', '.join(unknown_conditions)}")
    
    # 7. Проверка дат
    date_fields = ['mfg_date', 'removal_date', 'target_date']
    for field in date_fields:
        if field in df.columns:
            # Попытка конвертации в даты
            try:
                date_series = pd.to_datetime(df[field], errors='coerce')
                null_dates = date_series.isnull().sum()
                valid_dates = len(date_series) - null_dates
                
                if valid_dates > 0:
                    min_date = date_series.min()
                    max_date = date_series.max()
                    logger.info(f"📅 Поле {field}: {valid_dates} валидных дат ({min_date.date()} - {max_date.date()})")
                    
                    # Проверка на будущие даты (кроме target_date)
                    if field != 'target_date':
                        future_dates = (date_series > pd.Timestamp.now()).sum()
                        if future_dates > 0:
                            logger.warning(f"⚠️  Поле {field}: {future_dates} дат в будущем")
                else:
                    logger.warning(f"⚠️  Поле {field}: нет валидных дат")
            except Exception as e:
                logger.warning(f"⚠️  Ошибка обработки дат в поле {field}: {e}")
    
    # 8. Финальная оценка качества
    if quality_passed:
        logger.info("✅ === ДАННЫЕ ПРОШЛИ ВАЛИДАЦИЮ КАЧЕСТВА ===")
    else:
        logger.error("❌ === ДАННЫЕ НЕ ПРОШЛИ ВАЛИДАЦИЮ КАЧЕСТВА ===")
    
    return quality_passed

def get_file_path(logger) -> str:
    """Интерактивный выбор файла для загрузки"""
    logger.info("📁 === ВЫБОР ФАЙЛА ДЛЯ ЗАГРУЗКИ ===")
    
    # Стандартный путь к файлу
    default_file = 'data_input/source_data/Status_Components.xlsx'
    
    print(f"\n📂 ФАЙЛ ДЛЯ ЗАГРУЗКИ:")
    print(f"1️⃣  Стандартный файл: {default_file}")
    print(f"2️⃣  Указать другой файл")
    print(f"3️⃣  Отмена")
    
    while True:
        try:
            choice = input(f"\nВыберите файл (1-3): ").strip()
            if choice == '1':
                if os.path.exists(default_file):
                    logger.info(f"✅ Выбран стандартный файл: {default_file}")
                    return default_file
                else:
                    logger.error(f"❌ Стандартный файл не найден: {default_file}")
                    print(f"❌ Файл не найден. Выберите другой вариант.")
                    continue
            elif choice == '2':
                custom_path = input(f"Введите путь к файлу: ").strip()
                if os.path.exists(custom_path):
                    logger.info(f"✅ Выбран файл: {custom_path}")
                    return custom_path
                else:
                    logger.error(f"❌ Файл не найден: {custom_path}")
                    print(f"❌ Файл не найден. Попробуйте еще раз.")
                    continue
            elif choice == '3':
                logger.info("❌ Операция отменена пользователем")
                return None
            else:
                print("❌ Введите 1, 2 или 3")
        except KeyboardInterrupt:
            print("\n👋 Операция отменена пользователем")
            return None

def batch_insert_to_clickhouse(client, table_name: str, df: pd.DataFrame, logger):
    """Батчевая загрузка данных в ClickHouse с мониторингом производительности"""
    logger.info("🚀 === БАТЧЕВАЯ ЗАГРУЗКА В CLICKHOUSE ===")
    
    total_start_time = time.time()
    batch_size = 5000
    total_rows = len(df)
    
    success_count = 0
    total_records = 0
    
    logger.info(f"📊 Параметры загрузки:")
    logger.info(f"   📄 Всего записей: {total_rows:,}")
    logger.info(f"   📦 Размер батча: {batch_size:,}")
    logger.info(f"   🔢 Количество батчей: {(total_rows + batch_size - 1) // batch_size}")
    
    for i in range(0, total_rows, batch_size):
        batch = df.iloc[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (total_rows + batch_size - 1) // batch_size
        
        start_time = time.time()
        
        try:
            # Конвертируем DataFrame в список кортежей
            # Arrow backend может ускорить это преобразование
            data_tuples = [tuple(row) for row in batch.values]
            
            # Выполняем INSERT
            client.execute(f'INSERT INTO {table_name} VALUES', data_tuples)
            
            batch_time = time.time() - start_time
            rows_per_sec = len(data_tuples) / batch_time if batch_time > 0 else 0
            
            logger.info(f"✅ Батч {batch_num}/{total_batches}: {len(data_tuples):,} записей за {batch_time:.2f}с ({rows_per_sec:,.0f} записей/сек)")
            
            success_count += 1
            total_records += len(batch)
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки батча {batch_num}: {e}")
            raise  # Прерываем выполнение при ошибке
    
    # Финальная статистика
    total_time = time.time() - total_start_time
    
    if success_count == (total_rows + batch_size - 1) // batch_size:
        logger.info(f"🎉 === БАТЧЕВАЯ ЗАГРУЗКА ЗАВЕРШЕНА УСПЕШНО ===")
        logger.info(f"✅ Обработано батчей: {success_count}")
        logger.info(f"✅ Загружено записей: {total_records:,}")
        logger.info(f"⏱️  Общее время: {total_time:.2f} секунд")
        logger.info(f"⚡ Производительность: {total_records/total_time:,.0f} записей/сек")
        logger.info(f"🚀 Использованы Arrow оптимизации: dtype_backend='pyarrow'")
    else:
        logger.error(f"💥 Загрузка завершена с ошибками!")
        logger.error(f"❌ Успешных батчей: {success_count}")
        raise Exception("Батчевая загрузка не завершена")

def main():
    """Основная функция загрузки с выбором режима обработки"""
    try:
        logger = setup_logging()
        logger.info("🚀 === SMART RAW LOADER v2.1 ===")
        logger.info("📊 Helicopter Component Lifecycle Prediction Project")
        
        # Загрузка конфигурации
        config = load_config()
        logger.info("✅ Конфигурация загружена")
        
        # Интерактивный выбор файла
        file_path = get_file_path(logger)
        if not file_path:
            return
        
        # Проверка существования файла
        if not os.path.exists(file_path):
            logger.error(f"❌ Файл не найден: {file_path}")
            return
        
        # === НОВЫЙ ВЫБОР РЕЖИМА ОБРАБОТКИ ===
        print("\n🎯 РЕЖИМ ОБРАБОТКИ ДАННЫХ:")
        print("1️⃣  Загрузка в ClickHouse RAW слой (как раньше)")
        print("2️⃣  Создание DataFrame с цифровизацией полей (новое)")
        print("3️⃣  Оба режима: DataFrame + ClickHouse")
        
        while True:
            try:
                mode_choice = input("\n🎯 Выберите режим (1-3): ").strip()
                if mode_choice in ['1', '2', '3']:
                    break
                print("❌ Введите 1, 2 или 3")
            except KeyboardInterrupt:
                print("\n👋 Операция отменена пользователем")
                return
        
        mode_choice = int(mode_choice)
        
        # Выбор фильтрации по оборотным агрегатам (для всех режимов)
        print("\n📋 ФИЛЬТРАЦИЯ ДАННЫХ:")
        print("1️⃣  ВСЕ данные из файла")
        print("2️⃣  Только ОБОРОТНЫЕ агрегаты (37 партномеров)")
        
        while True:
            try:
                filter_choice = input("\n📋 Выберите данные для обработки (1-2): ").strip()
                if filter_choice in ['1', '2']:
                    break
                print("❌ Введите 1 или 2")
            except KeyboardInterrupt:
                print("\n👋 Операция отменена пользователем")
                return
        
        enable_partno_filter = (filter_choice == '2')
        
        # Извлечение версии данных из Excel
        version_date = extract_version_date_from_excel(file_path, logger)
        if not version_date:
            logger.error("❌ Не удалось определить версию файла")
            return
        
        # Загрузка данных из Excel
        df = load_excel_data(file_path, logger)
        if df is None or df.empty:
            logger.error("❌ Не удалось загрузить данные из Excel")
            return
        
        # Фильтрация по оборотным агрегатам
        df = filter_by_repairable_partnos(df, logger, enable_partno_filter)
        
        # === РЕЖИМ 2: ТОЛЬКО DATAFRAME С ЦИФРОВИЗАЦИЕЙ ===
        if mode_choice == 2:
            logger.info("🔄 === РЕЖИМ: DATAFRAME С ЦИФРОВИЗАЦИЕЙ ===")
            
            # Подготовка данных с цифровизацией
            processed_df = prepare_data_for_clickhouse(df, version_date, logger, enable_partno_filter)
            
            # Цифровизация текстовых полей
            digital_df = digitize_text_fields(processed_df, logger)
            
            # Сохранение результата в файл
            output_path = f"test_output/digitized_dataframe_{version_date.strftime('%Y%m%d')}.parquet"
            digital_df.to_parquet(output_path, index=False)
            logger.info(f"💾 Цифровизованный DataFrame сохранен: {output_path}")
            
            # Показ статистики
            display_dataframe_stats(digital_df, logger)
            return
        
        # === РЕЖИМ 1 И 3: ПОДКЛЮЧЕНИЕ К CLICKHOUSE ===
        if mode_choice in [1, 3]:
            client = connect_clickhouse(config['database']['clickhouse'], logger)
            
            table_name = "heli_raw"
            
            # Создание таблицы если не существует
            if not create_table_if_not_exists(client, table_name, logger):
                return
            
            # Проверка изменений данных
            if not check_data_changes(client, table_name, df, version_date, logger):
                logger.info("✅ Загрузка завершена - данные не изменились")
                return
            
            # Подготовка данных для ClickHouse
            processed_df = prepare_data_for_clickhouse(df, version_date, logger, enable_partno_filter)
            
            # === РЕЖИМ 3: СОХРАНЕНИЕ DATAFRAME С ЦИФРОВИЗАЦИЕЙ ===
            if mode_choice == 3:
                digital_df = digitize_text_fields(processed_df.copy(), logger)
                output_path = f"test_output/digitized_dataframe_{version_date.strftime('%Y%m%d')}.parquet"
                digital_df.to_parquet(output_path, index=False)
                logger.info(f"💾 Цифровизованный DataFrame сохранен: {output_path}")
            
            # Валидация качества данных
            if not validate_data_quality(processed_df, logger):
                logger.error("❌ Данные не прошли валидацию качества")
                return
            
            # Загрузка в ClickHouse
            batch_insert_to_clickhouse(client, table_name, processed_df, logger)
            
            # Валидация данных в ClickHouse
            validate_data_in_clickhouse(client, table_name, version_date, logger)
        
        logger.info("🎉 === ЗАГРУЗКА ЗАВЕРШЕНА УСПЕШНО ===")
        
    except KeyboardInterrupt:
        logger.info("⚠️ Операция прервана пользователем")
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
        logger.error(f"📋 Детали: {traceback.format_exc()}")

def digitize_text_fields(df: pd.DataFrame, logger) -> pd.DataFrame:
    """
    Цифровизация текстовых полей в DataFrame
    Создание ID-маппингов и битовых масок
    """
    logger.info("🔄 === ЦИФРОВИЗАЦИЯ ТЕКСТОВЫХ ПОЛЕЙ ===")
    
    digital_df = df.copy()
    
    # === 1. СОЗДАНИЕ ID ДЛЯ ПАРТНОМЕРОВ ===
    unique_partnos = df['partno'].dropna().unique()
    partno_mapping = {partno: idx + 1 for idx, partno in enumerate(sorted(unique_partnos))}
    
    digital_df['partno_id'] = df['partno'].map(partno_mapping)
    logger.info(f"✅ Создано {len(partno_mapping)} ID для партномеров")
    
    # === 2. СОЗДАНИЕ ID ДЛЯ СЕРИЙНЫХ НОМЕРОВ ===
    unique_serialnos = df['serialno'].dropna().unique()
    serialno_mapping = {serialno: idx + 1 for idx, serialno in enumerate(sorted(unique_serialnos))}
    
    digital_df['serialno_id'] = df['serialno'].map(serialno_mapping)
    logger.info(f"✅ Создано {len(serialno_mapping)} ID для серийных номеров")
    
    # === 3. СОЗДАНИЕ ID ДЛЯ ЛОКАЦИЙ ===
    unique_locations = df['location'].dropna().unique()
    location_mapping = {location: idx + 1 for idx, location in enumerate(sorted(unique_locations))}
    
    digital_df['location_id'] = df['location'].map(location_mapping)
    logger.info(f"✅ Создано {len(location_mapping)} ID для локаций")
    
    # === 4. БИТОВЫЕ МАСКИ ДЛЯ ТИПОВ ВС ===
    ac_type_masks = {
        'Ми-26': 128, 'МИ26Т': 128,    # 0b10000000
        'Ми-17': 64,                    # 0b01000000  
        'Ми-8Т': 32,                    # 0b00100000
        'Ка-32': 16,                    # 0b00010000
        'AS-350': 8, '350B3': 8,        # 0b00001000
        'AS-355': 4, '355NP': 4,        # 0b00000100
        'R-44': 2,                      # 0b00000010
    }
    
    digital_df['ac_typ_mask'] = df['ac_typ'].map(ac_type_masks).fillna(0).astype('uint8')
    logger.info(f"✅ Создано {len(ac_type_masks)} битовых масок для типов ВС")
    
    # === 5. БИТОВЫЕ МАСКИ ДЛЯ СОСТОЯНИЙ ===
    condition_mapping = {
        'ИСПРАВНЫЙ': 7,        # 0b111 - Эксплуатация
        'НЕИСПРАВНЫЙ': 4,      # 0b100 - Ремонт  
        'ДОНОР': 1,            # 0b001 - Хранение
        'СНЯТ ЗАКАЗЧИКОМ': 0,  # 0b000 - Неактивно
        'СНЯТ': 0,             # 0b000 - Неактивно
        'НЕ УСТАНОВЛЕН': 6,    # 0b110 - Исправен, счетчики не работают
        'ПОСТАВКА': 3,         # 0b011 - Резерв
    }
    
    digital_df['condition_mask'] = df['condition'].map(condition_mapping).fillna(0).astype('uint8')
    logger.info(f"✅ Создано {len(condition_mapping)} битовых масок для состояний")
    
    # === 6. ID ДЛЯ ВЛАДЕЛЬЦЕВ ===
    owner_mapping = {
        'ЮТ-ВУ': 1, 'UTE': 2, 'ГТЛК': 3, 'СБЕР ЛИЗИНГ': 4,
        'ГПМ': 5, 'АО ГПМ': 6, 'ИП': 7, 'АРВ': 8, 'И': 9
    }
    
    digital_df['owner_id'] = df['owner'].map(owner_mapping).fillna(0).astype('uint8')
    logger.info(f"✅ Создано {len(owner_mapping)} ID для владельцев")
    
    # === 7. СОХРАНЕНИЕ МАППИНГОВ ===
    mappings = {
        'partno_mapping': partno_mapping,
        'serialno_mapping': serialno_mapping,
        'location_mapping': location_mapping,
        'ac_type_masks': ac_type_masks,
        'condition_mapping': condition_mapping,
        'owner_mapping': owner_mapping
    }
    
    # Сохранение маппингов в JSON для дальнейшего использования
    import json
    mapping_file = f"test_output/field_mappings_{pd.Timestamp.now().strftime('%Y%m%d')}.json"
    with open(mapping_file, 'w', encoding='utf-8') as f:
        # Конвертируем все в строки для JSON сериализации
        json_mappings = {}
        for key, mapping in mappings.items():
            json_mappings[key] = {str(k): int(v) for k, v in mapping.items()}
        json.dump(json_mappings, f, ensure_ascii=False, indent=2)
    
    logger.info(f"💾 Маппинги сохранены в: {mapping_file}")
    
    return digital_df

def display_dataframe_stats(df: pd.DataFrame, logger):
    """Отображение статистики цифровизованного DataFrame"""
    logger.info("📊 === СТАТИСТИКА ЦИФРОВИЗОВАННОГО DATAFRAME ===")
    
    # Основная информация
    logger.info(f"📄 Всего записей: {len(df):,}")
    logger.info(f"📋 Всего колонок: {len(df.columns)}")
    
    # Исходные текстовые поля
    text_fields = ['partno', 'serialno', 'location', 'ac_typ', 'condition', 'owner']
    digital_fields = ['partno_id', 'serialno_id', 'location_id', 'ac_typ_mask', 'condition_mask', 'owner_id']
    
    logger.info("\n🔄 Сравнение исходных и цифровых полей:")
    for text_field, digital_field in zip(text_fields, digital_fields):
        if text_field in df.columns and digital_field in df.columns:
            unique_text = df[text_field].nunique()
            unique_digital = df[digital_field].nunique()
            logger.info(f"  {text_field:15} → {digital_field:15}: {unique_text:4} → {unique_digital:4} уникальных")
    
    # Битовые маски
    if 'ac_typ_mask' in df.columns:
        mask_stats = df['ac_typ_mask'].value_counts().sort_index()
        logger.info(f"\n🎭 Битовые маски типов ВС:")
        for mask, count in mask_stats.items():
            if mask > 0:
                logger.info(f"  Маска {mask:3} (0b{mask:08b}): {count:,} записей")
    
    # Размер данных
    memory_usage = df.memory_usage(deep=True).sum() / 1024 / 1024
    logger.info(f"\n💾 Размер DataFrame в памяти: {memory_usage:.2f} MB")
    
    # Показать первые несколько записей
    logger.info(f"\n📋 Первые 3 записи цифровизованных данных:")
    display_cols = [col for col in ['partno_id', 'serialno_id', 'ac_typ_mask', 'condition_mask'] if col in df.columns]
    if display_cols:
        sample_data = df[display_cols].head(3)
        for idx, row in sample_data.iterrows():
            logger.info(f"  Запись {idx}: {dict(row)}")

def validate_data_in_clickhouse(client, table_name, version_date, logger):
    """
    Постзагрузочная валидация данных в ClickHouse
    Проверяет целостность и качество данных после загрузки
    """
    logger.info("🔍 Проведение постзагрузочной валидации...")
    
    validation_queries = [
        ("Общее количество записей", f"SELECT count(*) FROM {table_name} WHERE version_date = '{version_date}'"),
        ("Пустые партномера", f"SELECT count(*) FROM {table_name} WHERE version_date = '{version_date}' AND (partno = '' OR partno IS NULL)"),
        ("Пустые серийные номера", f"SELECT count(*) FROM {table_name} WHERE version_date = '{version_date}' AND (serialno = '' OR serialno IS NULL)"),
        ("Дубликаты по ключу", f"SELECT partno, serialno, count(*) as cnt FROM {table_name} WHERE version_date = '{version_date}' GROUP BY partno, serialno HAVING cnt > 1 LIMIT 5"),
        ("Отрицательные SNE", f"SELECT count(*) FROM {table_name} WHERE version_date = '{version_date}' AND sne < 0"),
        ("Отрицательные PPR", f"SELECT count(*) FROM {table_name} WHERE version_date = '{version_date}' AND ppr < 0"),
        ("Статистики по SNE", f"SELECT min(sne), max(sne), avg(sne), median(sne) FROM {table_name} WHERE version_date = '{version_date}' AND sne > 0"),
        ("Статистики по PPR", f"SELECT min(ppr), max(ppr), avg(ppr), median(ppr) FROM {table_name} WHERE version_date = '{version_date}' AND ppr > 0"),
        ("Распределение состояний", f"SELECT condition, count(*) FROM {table_name} WHERE version_date = '{version_date}' GROUP BY condition ORDER BY count(*) DESC"),
        ("Уникальные типы ВС", f"SELECT ac_typ, count(*) FROM {table_name} WHERE version_date = '{version_date}' GROUP BY ac_typ ORDER BY count(*) DESC"),
    ]
    
    validation_passed = True
    
    for description, query in validation_queries:
        try:
            logger.info(f"🔍 {description}...")
            result = client.execute(query)
            
            if "count(*)" in query and len(result) == 1:
                count = result[0][0]
                logger.info(f"   📊 {description}: {count:,}")
                
                # Проверки критических значений
                if "Пустые партномера" in description and count > 0:
                    logger.error(f"   ❌ Найдено {count} записей с пустыми партномерами!")
                    validation_passed = False
                elif "Дубликаты по ключу" in description and len(result) > 0:
                    logger.error(f"   ❌ Найдены дубликаты по ключу!")
                    for row in result:
                        logger.error(f"      partno: {row[0]}, serialno: {row[1]}, количество: {row[2]}")
                    validation_passed = False
                elif "Отрицательные" in description and count > 0:
                    logger.warning(f"   ⚠️  Найдено {count} отрицательных значений")
                    
            elif "Статистики" in description and len(result) == 1:
                stats = result[0]
                field_name = "SNE" if "SNE" in description else "PPR"
                logger.info(f"   📈 {field_name}: мин={stats[0]:.2f}, макс={stats[1]:.2f}, среднее={stats[2]:.2f}, медиана={stats[3]:.2f}")
                
            elif "Распределение" in description or "Уникальные" in description:
                logger.info(f"   📊 Найдено {len(result)} различных значений:")
                for i, row in enumerate(result[:5]):  # Показываем только первые 5
                    logger.info(f"      {row[0]}: {row[1]:,} записей")
                if len(result) > 5:
                    logger.info(f"      ... и еще {len(result) - 5} значений")
                    
        except Exception as e:
            logger.error(f"❌ Ошибка валидации '{description}': {e}")
            validation_passed = False
    
    # Дополнительные проверки целостности
    try:
        logger.info("🔍 Проверка целостности данных...")
        
        # Проверка версий данных
        versions_query = f"SELECT DISTINCT version_date, count(*) FROM {table_name} GROUP BY version_date ORDER BY version_date DESC LIMIT 5"
        versions_result = client.execute(versions_query)
        
        logger.info("📅 Последние версии данных:")
        for version, count in versions_result:
            status = "🟢 ТЕКУЩАЯ" if version.strftime('%Y-%m-%d') == version_date else "🔵"
            logger.info(f"   {status} {version.strftime('%Y-%m-%d')}: {count:,} записей")
        
        # Проверка консистентности дат
        date_check_query = f"""
        SELECT count(*) 
        FROM {table_name} 
        WHERE version_date = '{version_date}' 
          AND mfg_date > removal_date 
          AND removal_date IS NOT NULL 
          AND mfg_date IS NOT NULL
        """
        date_issues = client.execute(date_check_query)[0][0]
        
        if date_issues > 0:
            logger.error(f"❌ Найдено {date_issues} записей с датой снятия раньше даты изготовления!")
            validation_passed = False
        else:
            logger.info("✅ Даты изготовления и снятия логически корректны")
            
    except Exception as e:
        logger.error(f"❌ Ошибка проверки целостности: {e}")
        validation_passed = False
    
    # Итоговая оценка
    if validation_passed:
        logger.info("🎉 === ПОСТЗАГРУЗОЧНАЯ ВАЛИДАЦИЯ УСПЕШНА ===")
        logger.info("✅ Все проверки качества пройдены")
    else:
        logger.error("💥 === ОБНАРУЖЕНЫ ПРОБЛЕМЫ ПОСЛЕ ЗАГРУЗКИ ===")
        logger.error("❌ Некоторые проверки качества не пройдены")
        logger.error("🔧 Рекомендуется проверить и исправить данные")
    
    return validation_passed

if __name__ == '__main__':
    main() 