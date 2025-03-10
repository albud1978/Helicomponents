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

def create_analytics_dimension(forecast_type="baseline", description="Базовый прогноз"):
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
    
    # Создаем таблицу OlapCube_Analytics для хранения агрегированных данных с версионностью
    create_analytics_table_query = f"""
    CREATE TABLE IF NOT EXISTS {database_name}.OlapCube_Analytics (
        Dates Date,                 -- Дата, для которой рассчитаны показатели
        version_id String,          -- Уникальный идентификатор версии (например, 'v20240309-1')
        created_at DateTime,        -- Дата и время создания этой версии прогноза
        forecast_type String,       -- Тип прогноза (например, 'baseline', 'optimistic', 'pessimistic')
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
        hours Float32
    ) ENGINE = MergeTree()
    ORDER BY (Dates, version_id, created_at)
    """
    
    client.execute(f"DROP TABLE IF EXISTS {database_name}.OlapCube_Analytics")
    client.execute(create_analytics_table_query)
    logger.info("Таблица OlapCube_Analytics создана с поддержкой версионности")
    
    # Получаем минимальную и максимальную даты из основного куба
    min_date_query = f"SELECT MIN(Dates) FROM {database_name}.OlapCube_VNV"
    max_date_query = f"SELECT MAX(Dates) FROM {database_name}.OlapCube_VNV"
    
    min_date = client.execute(min_date_query)[0][0]
    max_date = client.execute(max_date_query)[0][0]
    
    logger.info(f"Найдена min_date = {min_date}")
    logger.info(f"Будем обрабатывать даты в диапазоне [{min_date}, {max_date}]")
    
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
                # Генерируем информацию о версии
                now = datetime.now()
                version_id = f"v{now.strftime('%Y%m%d-%H%M%S')}-{forecast_type}"
                created_at = now
                
                # Подготовка данных для вставки с версией
                insert_data = []
                for row in analytics_data:
                    # Модифицируем кортеж результатов, добавляя версионность
                    date_val, *metrics = row
                    versioned_row = (date_val, version_id, created_at, forecast_type, description, *metrics)
                    insert_data.append(versioned_row)
                
                # Вставка данных
                client.execute(
                    f"INSERT INTO {database_name}.OlapCube_Analytics VALUES", 
                    insert_data
                )
                logger.info(f"Вставлено {len(analytics_data)} записей в Analytics Dimension с версией {version_id}")
                analytics_data = []
    
    # Оптимизируем таблицу после всех вставок
    client.execute(f"OPTIMIZE TABLE {database_name}.OlapCube_Analytics FINAL")
    logger.info("Таблица OlapCube_Analytics оптимизирована")
    
    # Выводим примеры данных
    sample_query = f"SELECT * FROM {database_name}.OlapCube_Analytics ORDER BY Dates LIMIT 5"
    sample_data = client.execute(sample_query)
    
    logger.info("Примеры записей из аналитической таблицы:")
    for row in sample_data:
        logger.info(row)
    
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