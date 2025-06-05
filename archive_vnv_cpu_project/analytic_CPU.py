import os
import logging
import pandas as pd
import numpy as np
from clickhouse_driver import Client
from datetime import datetime, timedelta, date

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Настройки подключения к ClickHouse
clickhouse_host = os.getenv('CLICKHOUSE_HOST', '10.95.19.132')
clickhouse_user = os.getenv('CLICKHOUSE_USER', 'default')
clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', 'quie1ahpoo5Su0wohpaedae8keeph6bi')
database_name = os.getenv('CLICKHOUSE_DB', 'default')

def create_analytics_dimension(description="Базовый прогноз"):
    """
    Создаем отдельную двухмерную таблицу с аналитикой по датам с поддержкой версионности
    """
    client = Client(
        host=clickhouse_host,
        user=clickhouse_user,
        password=clickhouse_password,
        port=9000,
        secure=False,
        settings={'strings_encoding': 'utf-8'}
    )
    
    # Проверяем существование таблицы Heli_Components
    check_table_query = f"""
    SELECT count() 
    FROM system.tables 
    WHERE database = '{database_name}' AND name = 'Heli_Components'
    """
    table_exists = client.execute(check_table_query)[0][0] > 0
    
    # Создаем таблицу Heli_Components для хранения агрегированных данных с версионностью, если она не существует
    if not table_exists:
        create_analytics_table_query = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.Heli_Components (
            Dates Date,                 -- Дата, для которой рассчитаны показатели
            version_id String,          -- Уникальный идентификатор версии (штамп самой ранней даты в кубе)
            created_at DateTime,        -- Дата и время создания этой версии прогноза
            description String,         -- Описание версии для удобства аналитиков
            
            -- Аналитические показатели
            ops_count Float32,
            hbs_count Float32,
            repair_count Float32,
            total_operable Float32,
            entry_count Float32,
            exit_count Float32,
            into_repair Float32,
            complete_repair Float32,
            remain_repair Float32,
            remain Float32,
            midlife_repair Float32,
            midlife Float32,
            hours Float32,
            
            -- Вычисляемые поля для ускорения запросов
            year UInt16 MATERIALIZED toYear(Dates),
            month UInt8 MATERIALIZED toMonth(Dates),
            quarter UInt8 MATERIALIZED toQuarter(Dates),
            week UInt8 MATERIALIZED toWeek(Dates),
            day_of_week UInt8 MATERIALIZED toDayOfWeek(Dates)
        ) ENGINE = MergeTree()
        PARTITION BY (version_id, toYYYYMM(Dates))
        ORDER BY (Dates, version_id, created_at)
        SETTINGS index_granularity = 8192
        """
        
        client.execute(create_analytics_table_query)
        logger.info("Таблица Heli_Components создана с поддержкой версионности и партиционированием")
        
        # Создаем индексы для ускорения запросов
        create_indices_queries = [
            f"ALTER TABLE {database_name}.Heli_Components ADD INDEX idx_version_id version_id TYPE minmax GRANULARITY 1",
            f"ALTER TABLE {database_name}.Heli_Components ADD INDEX idx_dates Dates TYPE minmax GRANULARITY 1",
            f"ALTER TABLE {database_name}.Heli_Components ADD INDEX idx_year_month (year, month) TYPE set(100) GRANULARITY 1",
            f"ALTER TABLE {database_name}.Heli_Components ADD INDEX idx_ops_count ops_count TYPE minmax GRANULARITY 4"
        ]
        
        for query in create_indices_queries:
            try:
                client.execute(query)
                logger.info(f"Создан индекс: {query}")
            except Exception as e:
                logger.warning(f"Не удалось создать индекс: {e}")
        
        # Создаем материализованное представление для агрегации по месяцам
        create_monthly_view_query = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {database_name}.Heli_Components_Monthly
        ENGINE = SummingMergeTree()
        PARTITION BY (version_id, toYear(month_date))
        ORDER BY (month_date, version_id)
        AS SELECT
            toStartOfMonth(Dates) as month_date,
            version_id,
            max(created_at) as created_at,
            any(description) as description,
            avg(ops_count) as avg_ops_count,
            avg(hbs_count) as avg_hbs_count,
            avg(repair_count) as avg_repair_count,
            avg(total_operable) as avg_total_operable,
            sum(entry_count) as total_entry_count,
            sum(exit_count) as total_exit_count,
            sum(into_repair) as total_into_repair,
            sum(complete_repair) as total_complete_repair,
            avg(remain_repair) as avg_remain_repair,
            avg(remain) as avg_remain,
            avg(midlife_repair) as avg_midlife_repair,
            avg(midlife) as avg_midlife,
            sum(hours) as total_hours
        FROM {database_name}.Heli_Components
        GROUP BY month_date, version_id
        """
        
        try:
            client.execute(create_monthly_view_query)
            logger.info("Создано материализованное представление для агрегации по месяцам")
        except Exception as e:
            logger.warning(f"Не удалось создать материализованное представление: {e}")
    else:
        logger.info("Таблица Heli_Components уже существует, будем дополнять ее новыми данными")
        
        # Проверяем наличие материализованного представления
        check_view_query = f"""
        SELECT count() 
        FROM system.tables 
        WHERE database = '{database_name}' AND name = 'Heli_Components_Monthly'
        """
        view_exists = client.execute(check_view_query)[0][0] > 0
        
        if not view_exists:
            # Создаем материализованное представление, если оно отсутствует
            create_monthly_view_query = f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {database_name}.Heli_Components_Monthly
            ENGINE = SummingMergeTree()
            PARTITION BY (version_id, toYear(month_date))
            ORDER BY (month_date, version_id)
            AS SELECT
                toStartOfMonth(Dates) as month_date,
                version_id,
                max(created_at) as created_at,
                any(description) as description,
                avg(ops_count) as avg_ops_count,
                avg(hbs_count) as avg_hbs_count,
                avg(repair_count) as avg_repair_count,
                avg(total_operable) as avg_total_operable,
                sum(entry_count) as total_entry_count,
                sum(exit_count) as total_exit_count,
                sum(into_repair) as total_into_repair,
                sum(complete_repair) as total_complete_repair,
                avg(remain_repair) as avg_remain_repair,
                avg(remain) as avg_remain,
                avg(midlife_repair) as avg_midlife_repair,
                avg(midlife) as avg_midlife,
                sum(hours) as total_hours
            FROM {database_name}.Heli_Components
            GROUP BY month_date, version_id
            """
            
            try:
                client.execute(create_monthly_view_query)
                logger.info("Создано материализованное представление для агрегации по месяцам")
            except Exception as e:
                logger.warning(f"Не удалось создать материализованное представление: {e}")
    
    # Получаем минимальную и максимальную даты из основного куба
    min_date_query = f"SELECT MIN(Dates) FROM {database_name}.OlapCube_VNV"
    max_date_query = f"SELECT MAX(Dates) FROM {database_name}.OlapCube_VNV"
    
    min_date = client.execute(min_date_query)[0][0]
    max_date = client.execute(max_date_query)[0][0]
    
    logger.info(f"Найдена min_date = {min_date}")
    logger.info(f"Будем обрабатывать даты в диапазоне [{min_date}, {max_date}]")
    
    # Формируем version_id на основе самой ранней даты в кубе
    version_id = f"v{min_date.strftime('%Y%m%d')}"
    logger.info(f"Сформирован version_id: {version_id}")
    
    # Проверяем, есть ли уже данные с таким version_id
    check_version_query = f"""
    SELECT count() 
    FROM {database_name}.Heli_Components 
    WHERE version_id = '{version_id}'
    """
    version_exists = client.execute(check_version_query)[0][0] > 0
    
    if version_exists:
        logger.info(f"Данные с version_id={version_id} уже существуют в таблице")
        # Можно добавить логику для обработки этого случая, например:
        # - Пропустить обработку
        # - Удалить существующие данные с этим version_id и добавить новые
        # - Создать новый version_id с дополнительным суффиксом
        
        # Для примера, добавим суффикс к version_id
        version_id = f"{version_id}-{datetime.now().strftime('%H%M%S')}"
        logger.info(f"Создан новый version_id: {version_id}")
    
    # Вычисляем аналитические данные для каждой даты
    all_dates = []
    curr_date = min_date
    while curr_date <= max_date:
        all_dates.append(curr_date)
        curr_date += timedelta(days=1)
    
    logger.info(f"Всего дат для обработки: {len(all_dates)}")
    
    analytics_data = []
    
    for i, curr_date in enumerate(all_dates[1:], 1):
        prev_date = all_dates[i-1]
        
        # Вычисляем агрегированные аналитические показатели
        result = calculate_analytics_for_date(client, database_name, prev_date, curr_date)
        
        if result:
            analytics_data.append(result)
        
        if i % 100 == 0 or i == len(all_dates) - 1:
            logger.info(f"Обработано {i} из {len(all_dates)-1} дат")
            
        # Пакетная вставка данных каждые 100 дат или в конце
        if i % 100 == 0 or i == len(all_dates) - 1:
            if analytics_data:
                # Текущее время для created_at
                created_at = datetime.now()
                
                # Подготовка данных для вставки с версией
                insert_data = []
                for row in analytics_data:
                    # Модифицируем кортеж результатов, добавляя версионность
                    date_val, *metrics = row
                    versioned_row = (date_val, version_id, created_at, description, *metrics)
                    insert_data.append(versioned_row)
                
                # Вставка данных
                client.execute(
                    f"INSERT INTO {database_name}.Heli_Components VALUES", 
                    insert_data
                )
                logger.info(f"Вставлено {len(analytics_data)} записей в Heli_Components с версией {version_id}")
                analytics_data = []
    
    # Оптимизируем таблицу после всех вставок
    client.execute(f"OPTIMIZE TABLE {database_name}.Heli_Components FINAL")
    logger.info("Таблица Heli_Components оптимизирована")
    
    # Обновляем материализованное представление
    try:
        client.execute(f"OPTIMIZE TABLE {database_name}.Heli_Components_Monthly FINAL")
        logger.info("Материализованное представление Heli_Components_Monthly обновлено")
    except Exception as e:
        logger.warning(f"Не удалось обновить материализованное представление: {e}")
    
    # Выводим примеры данных
    sample_query = f"""
    SELECT * 
    FROM {database_name}.Heli_Components 
    WHERE version_id = '{version_id}'
    ORDER BY Dates 
    LIMIT 5
    """
    sample_data = client.execute(sample_query)
    
    logger.info(f"Примеры записей из таблицы Heli_Components с version_id={version_id}:")
    for row in sample_data:
        logger.info(row)
    
    # Выводим примеры агрегированных данных по месяцам
    try:
        monthly_sample_query = f"""
        SELECT 
            month_date, 
            version_id, 
            avg_ops_count, 
            avg_hbs_count, 
            total_hours
        FROM {database_name}.Heli_Components_Monthly 
        WHERE version_id = '{version_id}'
        ORDER BY month_date 
        LIMIT 3
        """
        monthly_sample_data = client.execute(monthly_sample_query)
        
        logger.info(f"Примеры агрегированных данных по месяцам для version_id={version_id}:")
        for row in monthly_sample_data:
            logger.info(row)
    except Exception as e:
        logger.warning(f"Не удалось получить примеры агрегированных данных: {e}")
    
    return True

def calculate_analytics_for_date(client, database_name, prev_date, curr_date):
    """
    Вычисляет аналитические показатели для указанной даты
    """
    try:
        # Получаем данные для текущей и предыдущей даты из основного куба
        curr_data_query = f"""
        SELECT 
            serialno,
            Status, 
            Status_P, 
            sne,
            ppr,
            oh,
            ll,
            daily_flight_hours
        FROM {database_name}.OlapCube_VNV
        WHERE Dates = '{curr_date}'
        """
        
        prev_data_query = f"""
        SELECT 
            serialno,
            Status, 
            Status_P
        FROM {database_name}.OlapCube_VNV
        WHERE Dates = '{prev_date}'
        """
        
        curr_data = client.execute(curr_data_query)
        prev_data = client.execute(prev_data_query)
        
        if not curr_data or not prev_data:
            logger.warning(f"Нет данных для даты {curr_date} или {prev_date}")
            return None
            
        # Преобразуем результаты в DataFrame для удобной обработки
        curr_df = pd.DataFrame(curr_data, columns=[
            'serialno', 'Status', 'Status_P', 'sne', 'ppr', 'oh', 'll', 'daily_flight_hours'
        ])
        
        prev_df = pd.DataFrame(prev_data, columns=['serialno', 'Status', 'Status_P'])
        
        # Базовые счетчики
        ops_count = len(curr_df[curr_df['Status'] == 'Эксплуатация'])
        hbs_count = len(curr_df[curr_df['Status'] == 'Исправен'])
        repair_count = len(curr_df[curr_df['Status'] == 'Ремонт'])
        total_operable = ops_count + hbs_count + repair_count
        
        # Объединяем данные для анализа изменений
        merged = pd.merge(
            prev_df, 
            curr_df[['serialno', 'Status', 'Status_P']], 
            on='serialno',
            suffixes=('_prev', '')
        )
        
        # Анализ изменений
        entry_count = len(merged[(merged['Status_prev'] == 'Неактивно') & (merged['Status'] == 'Эксплуатация')])
        exit_count = len(merged[(merged['Status_prev'] == 'Эксплуатация') & (merged['Status'] == 'Хранение')])
        into_repair = len(merged[(merged['Status_prev'] == 'Эксплуатация') & (merged['Status'] == 'Ремонт')])
        complete_repair = len(merged[(merged['Status_prev'] == 'Ремонт') & (merged['Status'] != 'Ремонт')])
        
        # Расчет оставшегося ресурса 
        operating_tech = curr_df[curr_df['Status'].isin(['Эксплуатация', 'Исправен'])]
        remain_repair = (operating_tech['oh'] - operating_tech['ppr']).fillna(0).sum()
        
        serviceable_tech = curr_df[curr_df['Status'].isin(['Эксплуатация', 'Исправен', 'Ремонт'])]
        remain = (serviceable_tech['ll'] - serviceable_tech['sne']).fillna(0).sum()
        
        # Относительные показатели
        total_ll = serviceable_tech['ll'].fillna(0).sum()
        midlife = remain / total_ll if total_ll > 0 else 0
        
        total_oh = operating_tech['oh'].fillna(0).sum() 
        midlife_repair = remain_repair / total_oh if total_oh > 0 else 0
        
        # Общий налет часов
        hours = curr_df[curr_df['Status'] == 'Эксплуатация']['daily_flight_hours'].fillna(0).sum()
        
        return (
            curr_date,  
            float(ops_count), 
            float(hbs_count),
            float(repair_count),
            float(total_operable),
            float(entry_count), 
            float(exit_count),
            float(into_repair),
            float(complete_repair),
            float(remain_repair),
            float(remain),
            float(midlife_repair),
            float(midlife),
            float(hours)
        )
    
    except Exception as e:
        logger.error(f"Ошибка при расчете аналитики для даты {curr_date}: {e}", exc_info=True)
        return None

if __name__ == "__main__":
    create_analytics_dimension()
    logger.info("Создание аналитического измерения завершено")