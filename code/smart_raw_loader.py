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
    Комплексная проверка качества загруженных данных из Excel
    
    Проводит многоуровневую валидацию:
    - Структурная целостность
    - Бизнес-логика данных  
    - Статистическая валидация
    - Проверка совместимости с ClickHouse
    """
    logger.info("🔍 === КОМПЛЕКСНАЯ ПРОВЕРКА КАЧЕСТВА ДАННЫХ ===")
    
    quality_issues = []
    warnings = []
    quality_score = 100  # Начинаем со 100% качества
    
    # =============== БАЗОВЫЕ СТРУКТУРНЫЕ ПРОВЕРКИ ===============
    logger.info("📋 Структурные проверки...")
    
    # 1. Проверка базовой структуры
    if df.empty:
        quality_issues.append("КРИТИЧНО: DataFrame пуст")
        quality_score -= 50
    
    if len(df.columns) == 0:
        quality_issues.append("КРИТИЧНО: Отсутствуют столбцы")
        quality_score -= 50
        
    logger.info(f"📊 Базовая структура: {len(df)} строк, {len(df.columns)} столбцов")
    
    # 2. Проверка на полностью пустые строки/столбцы
    empty_rows = df.isnull().all(axis=1).sum()
    empty_cols = df.isnull().all(axis=0).sum()
    
    if empty_rows > 0:
        warnings.append(f"Найдено {empty_rows} полностью пустых строк")
        quality_score -= min(5, empty_rows * 0.1)
        
    if empty_cols > 0:
        empty_col_names = df.columns[df.isnull().all(axis=0)].tolist()
        warnings.append(f"Найдено {empty_cols} пустых столбцов: {empty_col_names}")
        quality_score -= empty_cols * 2
    
    # =============== КРИТИЧНЫЕ ПОЛЯ ===============
    logger.info("🔑 Проверка критичных полей...")
    
    critical_fields = ['partno', 'serialno']
    for field in critical_fields:
        if field in df.columns:
            null_count = df[field].isnull().sum()
            null_percentage = (null_count / len(df)) * 100
            
            if null_count > 0:
                if null_percentage > 10:  # Более 10% - критично
                    quality_issues.append(f"КРИТИЧНО: {field} - {null_count} пустых значений ({null_percentage:.1f}%)")
                    quality_score -= 15
                else:
                    warnings.append(f"{field}: {null_count} пустых значений ({null_percentage:.1f}%)")
                    quality_score -= min(5, null_percentage * 0.5)
            else:
                logger.info(f"✅ {field}: все значения заполнены")
        else:
            warnings.append(f"Отсутствует критичное поле: {field}")
            quality_score -= 5

    # =============== ДУБЛИКАТЫ ===============
    logger.info("🔍 Проверка дубликатов...")
    
    # Дубликаты по ключевым полям
    if 'partno' in df.columns and 'serialno' in df.columns:
        key_fields = ['partno', 'serialno']
        duplicates = df.duplicated(subset=key_fields, keep=False)
        dup_count = duplicates.sum()
        
        if dup_count > 0:
            dup_percentage = (dup_count / len(df)) * 100
            if dup_percentage > 5:  # Более 5% дубликатов - критично
                quality_issues.append(f"КРИТИЧНО: {dup_count} дубликатов по ключу (partno, serialno) - {dup_percentage:.1f}%")
                quality_score -= 20
            else:
                warnings.append(f"Найдено {dup_count} дубликатов по ключу (partno, serialno)")
                quality_score -= min(10, dup_percentage)
        else:
            logger.info("✅ Дубликаты по ключевым полям не найдены")

    # =============== ВАЛИДАЦИЯ ДАТ ===============
    logger.info("📅 Проверка дат...")
    
    date_fields = ['mfg_date', 'removal_date', 'target_date', 'oh_at_date', 'repair_date']
    current_year = pd.Timestamp.now().year
    
    for field in date_fields:
        if field in df.columns:
            # Конвертируем в даты
            try:
                dates = pd.to_datetime(df[field], errors='coerce', dayfirst=True)
                valid_dates = dates.dropna()
                invalid_count = len(df) - len(valid_dates)
                
                if len(valid_dates) > 0:
                    min_date = valid_dates.min()
                    max_date = valid_dates.max()
                    
                    # Проверка на разумность дат
                    future_dates = (valid_dates > pd.Timestamp.now()).sum()
                    old_dates = (valid_dates < pd.Timestamp('1980-01-01')).sum()
                    
                    if future_dates > 0:
                        warnings.append(f"{field}: {future_dates} дат в будущем")
                        quality_score -= min(5, future_dates * 0.1)
                        
                    if old_dates > 0:
                        warnings.append(f"{field}: {old_dates} дат до 1980 года")
                        quality_score -= min(5, old_dates * 0.1)
                    
                    logger.info(f"📅 {field}: {len(valid_dates)} валидных дат ({min_date.strftime('%Y-%m-%d')} - {max_date.strftime('%Y-%m-%d')})")
                    
                if invalid_count > 0:
                    invalid_percentage = (invalid_count / len(df)) * 100
                    if invalid_percentage > 20:
                        warnings.append(f"{field}: {invalid_count} невалидных дат ({invalid_percentage:.1f}%)")
                        quality_score -= min(10, invalid_percentage * 0.3)
                        
            except Exception as e:
                warnings.append(f"Ошибка обработки дат в {field}: {e}")
                quality_score -= 5

    # =============== РЕСУРСНЫЕ ПОЛЯ (UInt32) ===============
    logger.info("🔢 Проверка ресурсных полей...")
    
    resource_fields = ['oh', 'oh_threshold', 'll', 'sne', 'ppr']
    
    for field in resource_fields:
        if field in df.columns:
            try:
                # Попытка конвертации в числа
                numeric_series = pd.to_numeric(df[field], errors='coerce')
                valid_numbers = numeric_series.dropna()
                invalid_count = len(df) - len(valid_numbers)
                
                if len(valid_numbers) > 0:
                    negative_count = (valid_numbers < 0).sum()
                    zero_count = (valid_numbers == 0).sum()
                    extreme_values = valid_numbers[(valid_numbers > valid_numbers.quantile(0.99)) | 
                                                 (valid_numbers < valid_numbers.quantile(0.01))]
                    
                    # Статистики
                    logger.info(f"🔢 {field}: {len(valid_numbers)} валидных значений, "
                              f"среднее: {valid_numbers.mean():.2f}, "
                              f"медиана: {valid_numbers.median():.2f}")
                    
                    # Проверки
                    if negative_count > 0:
                        # Все ресурсные поля не должны быть отрицательными - будут обнулены
                        quality_issues.append(f"{field}: {negative_count} отрицательных значений (будут обнулены для GPU)")
                        quality_score -= 10
                    
                    if zero_count > len(valid_numbers) * 0.5:  # Более 50% нулей
                        warnings.append(f"{field}: {zero_count} нулевых значений ({(zero_count/len(valid_numbers)*100):.1f}%)")
                        quality_score -= 5
                        
                    if len(extreme_values) > 0:
                        warnings.append(f"{field}: {len(extreme_values)} экстремальных значений")
                        quality_score -= 2
                
                if invalid_count > 0:
                    invalid_percentage = (invalid_count / len(df)) * 100
                    if invalid_percentage > 10:
                        warnings.append(f"{field}: {invalid_count} нечисловых значений ({invalid_percentage:.1f}%)")
                        quality_score -= min(8, invalid_percentage * 0.4)
                        
            except Exception as e:
                warnings.append(f"Ошибка обработки числового поля {field}: {e}")
                quality_score -= 3

    # =============== КОНСИСТЕНТНОСТЬ ДАННЫХ ===============
    logger.info("🔗 Проверка консистентности...")
    
    # Проверка связей между полями
    if 'mfg_date' in df.columns and 'removal_date' in df.columns:
        try:
            mfg_dates = pd.to_datetime(df['mfg_date'], errors='coerce')
            removal_dates = pd.to_datetime(df['removal_date'], errors='coerce')
            
            # Даты снятия раньше даты изготовления
            invalid_sequence = ((removal_dates < mfg_dates) & 
                              mfg_dates.notna() & removal_dates.notna()).sum()
            
            if invalid_sequence > 0:
                quality_issues.append(f"ЛОГИЧЕСКАЯ ОШИБКА: {invalid_sequence} случаев снятия раньше изготовления")
                quality_score -= 15
                
        except Exception as e:
            warnings.append(f"Ошибка проверки последовательности дат: {e}")

    # =============== СОВМЕСТИМОСТЬ С CLICKHOUSE ===============
    logger.info("🗄️ Проверка совместимости с ClickHouse...")
    
    # Проверка на типы данных, которые могут вызвать проблемы
    for col in df.columns:
        # Очень длинные строки
        if df[col].dtype == 'object':
            max_length = df[col].astype(str).str.len().max()
            if max_length > 1000:
                warnings.append(f"Столбец {col}: максимальная длина строки {max_length} символов")
                quality_score -= 2
        
        # Специальные символы в именах столбцов
        if not col.replace('_', '').replace('-', '').isalnum():
            warnings.append(f"Имя столбца содержит специальные символы: {col}")
            quality_score -= 1

    # =============== ИТОГОВАЯ ОЦЕНКА ===============
    quality_score = max(0, min(100, quality_score))  # Ограничиваем 0-100
    
    logger.info(f"📊 === РЕЗУЛЬТАТЫ ПРОВЕРКИ КАЧЕСТВА ===")
    logger.info(f"🎯 Общий балл качества: {quality_score:.1f}/100")
    
    if quality_issues:
        logger.error("❌ КРИТИЧЕСКИЕ ПРОБЛЕМЫ:")
        for issue in quality_issues:
            logger.error(f"  🚨 {issue}")
    
    if warnings:
        logger.warning("⚠️  ПРЕДУПРЕЖДЕНИЯ:")
        for warning in warnings:
            logger.warning(f"  ⚠️  {warning}")
    
    if not quality_issues and not warnings:
        logger.info("✅ Проблем качества данных не обнаружено")
        return True
        
    # Определяем критичность
    is_critical = len(quality_issues) > 0 or quality_score < 60
    
    if is_critical:
        print(f"\n🚨 ОБНАРУЖЕНЫ КРИТИЧЕСКИЕ ПРОБЛЕМЫ КАЧЕСТВА ДАННЫХ!")
        print(f"📊 Оценка качества: {quality_score:.1f}/100")
        print(f"\n❌ Критические проблемы ({len(quality_issues)}):")
        for issue in quality_issues:
            print(f"  🚨 {issue}")
    else:
        print(f"\n⚠️  Обнаружены проблемы качества данных")
        print(f"📊 Оценка качества: {quality_score:.1f}/100")
    
    if warnings:
        print(f"\n⚠️  Предупреждения ({len(warnings)}):")
        for warning in warnings:
            print(f"  ⚠️  {warning}")
    
    # Интерактивный выбор действия
    while True:
        if is_critical:
            print(f"\n🚨 Рекомендация: ИСПРАВИТЬ данные перед загрузкой")
            choice = input("Продолжить несмотря на критические проблемы? (y/n): ").strip().lower()
        else:
            print(f"\n💡 Рекомендация: Можно продолжить с предупреждениями")
            choice = input("Продолжить загрузку? (y/n): ").strip().lower()
            
        if choice == 'y':
            logger.info(f"✅ Пользователь решил продолжить (качество: {quality_score:.1f}/100)")
            return True
        elif choice == 'n':
            logger.info(f"❌ Пользователь отменил загрузку (качество: {quality_score:.1f}/100)")
            return False
        else:
            print("Введите 'y' для продолжения или 'n' для отмены")

def main():
    """Основная функция умной загрузки в ClickHouse с Arrow оптимизациями"""
    logger = setup_logging()
    # Общий таймер запускаем только после пользовательского взаимодействия
    total_start_time = None
    enable_partno_filter = False  # Инициализируем переменную в начале функции
    
    try:
        logger.info("🚀 === УМНАЯ ЗАГРУЗКА STATUS_COMPONENTS В CLICKHOUSE (Arrow оптимизации) ===")
        
        # Параметры
        TABLE_NAME = 'heli_raw'
        EXCEL_FILE = 'data_input/source_data/Status_Components.xlsx'
        
        # 1. Загрузка конфигурации
        config = load_config()
        logger.info(f"⚙️  Конфигурация: {config['host']}:{config['port']}/{config['database']}")
        
        # 2. Подключение к ClickHouse
        client = connect_clickhouse(config, logger)
        
        # 3. Проверка/создание таблицы
        if not create_table_if_not_exists(client, TABLE_NAME, logger):
            raise Exception("Не удалось создать таблицу")
        
        # 4. Предварительная загрузка данных для анализа изменений
        logger.info(f"📥 Предварительная загрузка файла {EXCEL_FILE}...")
        
        # Загружаем Excel с Arrow backend
        df = load_excel_data(EXCEL_FILE, logger)
        
        # 5. Определение версии данных
        version_date = extract_version_date_from_excel(EXCEL_FILE, logger)
        logger.info(f"🗓️  Потенциальная версия данных: {version_date}")
        
        # 5.1. НОВАЯ ФУНКЦИОНАЛЬНОСТЬ: Выбор режима фильтрации данных
        print(f"\n🎯 РЕЖИМ ЗАГРУЗКИ ДАННЫХ:")
        print(f"   1. ВСЕ данные (полная загрузка)")
        print(f"   2. ОБОРОТНЫЕ агрегаты (только 37 критичных партномеров)")
        print(f"   3. ОТМЕНА")
        
        enable_partno_filter = False
        while True:
            try:
                choice = input(f"\nВыберите режим (1-3): ").strip()
                if choice == '1':
                    enable_partno_filter = False
                    logger.info("🌍 Выбран режим: ПОЛНАЯ загрузка всех данных")
                    break
                elif choice == '2':
                    enable_partno_filter = True
                    logger.info("🎯 Выбран режим: ОБОРОТНЫЕ агрегаты (37 партномеров)")
                    break
                elif choice == '3':
                    logger.info("❌ Загрузка отменена пользователем")
                    return
                else:
                    print("❌ Неверный выбор. Введите 1, 2 или 3.")
            except KeyboardInterrupt:
                print(f"\n❌ Загрузка отменена пользователем")
                return
        
        # Подготавливаем данные для анализа
        prepared_df = prepare_data_for_clickhouse(df, version_date, logger, enable_partno_filter)
        
        # Проверяем что после фильтрации остались данные
        if len(prepared_df) == 0:
            logger.error("❌ После обработки не осталось данных для загрузки!")
            if enable_partno_filter:
                logger.error("💡 Оборотные агрегаты не найдены в файле")
            else:
                logger.error("💡 Файл не содержит данных")
            return
        
        # 6. Быстрая проверка конфликта версий
        result = client.execute(f"""
            SELECT COUNT(*) FROM {TABLE_NAME} 
            WHERE version_date = '{version_date}'
        """)
        existing_count = result[0][0]
        
        if existing_count > 0:
            # 6.1. Конфликт версий - всегда спрашиваем пользователя
            logger.warning(f"⚠️  Обнаружен конфликт версий!")
            logger.warning(f"   Данные за {version_date} уже существуют ({existing_count:,} записей)")
            
            print(f"\n🚨 КОНФЛИКТ ВЕРСИЙ ДАННЫХ!")
            print(f"   Дата версии: {version_date}")
            print(f"   Существующих записей: {existing_count:,}")
            print(f"\nВыберите действие:")
            print(f"   1. ЗАМЕНИТЬ существующие данные")
            print(f"   2. ОТМЕНИТЬ загрузку")
            
            while True:
                try:
                    choice = input(f"\nВаш выбор (1-2): ").strip()
                    if choice == '1':
                        logger.info(f"🔄 Замена существующих данных за {version_date}...")
                        client.execute(f"DELETE FROM {TABLE_NAME} WHERE version_date = '{version_date}'")
                        logger.info(f"✅ Удалено {existing_count:,} существующих записей")
                        break
                    elif choice == '2':
                        logger.info(f"❌ Загрузка отменена пользователем")
                        return
                    else:
                        print("❌ Неверный выбор. Введите 1 или 2.")
                except KeyboardInterrupt:
                    print(f"\n❌ Загрузка отменена пользователем")
                    return
        else:
            logger.info(f"✅ Новая версия данных - продолжаем загрузку")
        
        # 7. Запускаем таймер производительности ПОСЛЕ всех пользовательских взаимодействий
        total_start_time = time.time()
        logger.info("⏱️  Начинаем замер производительности системы...")
            
        # 8. Проверка качества данных
        if not validate_data_quality(prepared_df, logger):
            logger.info("❌ Загрузка отменена из-за проблем качества")
            return
        
        # 9. Загрузка в ClickHouse с оптимизацией
        logger.info(f"🚀 Начинаем загрузку в ClickHouse...")
        
        # Создаем батчи для оптимальной загрузки
        batch_size = 5000
        total_rows = len(prepared_df)
        
        success_count = 0
        total_records = 0
        
        for i in range(0, total_rows, batch_size):
            batch = prepared_df.iloc[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_rows + batch_size - 1) // batch_size
            
            start_time = time.time()
            
            try:
                # Конвертируем DataFrame в список кортежей
                # Arrow backend может ускорить это преобразование
                data_tuples = [tuple(row) for row in batch.values]
                
                # Выполняем INSERT
                client.execute(f'INSERT INTO {TABLE_NAME} VALUES', data_tuples)
                
                batch_time = time.time() - start_time
                rows_per_sec = len(data_tuples) / batch_time if batch_time > 0 else 0
                
                logger.info(f"✅ Батч {batch_num}/{total_batches}: {len(data_tuples):,} записей за {batch_time:.2f}с ({rows_per_sec:,.0f} записей/сек)")
                
                success_count += 1
                total_records += len(batch)
                
            except Exception as e:
                logger.error(f"❌ Ошибка загрузки батча {batch_num}: {e}")
                break
        
        # 10. Финальная статистика
        total_time = time.time() - total_start_time if total_start_time else 0
        
        if success_count == (total_rows + batch_size - 1) // batch_size:
            logger.info(f"🎉 === ЗАГРУЗКА ЗАВЕРШЕНА УСПЕШНО ===")
            logger.info(f"✅ Обработано батчей: {success_count}")
            logger.info(f"✅ Загружено записей: {total_records:,}")
            logger.info(f"⏱️  Общее время: {total_time:.2f} секунд")
            logger.info(f"⚡ Производительность: {total_records/total_time:,.0f} записей/сек")
            logger.info(f"📅 Версия данных: {version_date}")
            logger.info(f"🚀 Использованы Arrow оптимизации: dtype_backend='pyarrow'")
            
            # Информация о режиме загрузки
            if enable_partno_filter:
                logger.info(f"🎯 Режим: ОБОРОТНЫЕ агрегаты (37 критичных партномеров)")
                repairable_partnos = get_repairable_partno_list()
                logger.info(f"📦 Оборотные агрегаты: {', '.join(repairable_partnos[:5])}... (всего {len(repairable_partnos)})")
            else:
                logger.info(f"🌍 Режим: ПОЛНАЯ загрузка всех данных")
            
            # Проверяем результат в таблице
            final_count = client.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE version_date = '{version_date}'")[0][0]
            logger.info(f"🔍 Проверка в БД: {final_count:,} записей с версией {version_date}")
            
            # 11. Постзагрузочная валидация качества в ClickHouse
            logger.info("🔍 === ПОСТЗАГРУЗОЧНАЯ ВАЛИДАЦИЯ В CLICKHOUSE ===")
            validate_data_in_clickhouse(client, TABLE_NAME, version_date, logger)
            
        else:
            logger.error(f"💥 Загрузка завершена с ошибками!")
            logger.error(f"❌ Успешных батчей: {success_count}")
        
    except Exception as e:
        total_time = time.time() - total_start_time if total_start_time else 0
        logger.error(f"💥 Критическая ошибка после {total_time:.2f} сек: {e}")
        logger.error("Трассировка ошибки:")
        logger.error(traceback.format_exc())
        raise

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