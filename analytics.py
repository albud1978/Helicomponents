import os
import sys
import logging
from datetime import timedelta
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from clickhouse_driver import Client

# 1. Загрузка переменных окружения
load_dotenv()

# 2. Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def main():
    # 3. Подключение к ClickHouse
    clickhouse_host = os.getenv('CLICKHOUSE_HOST', '10.95.19.132')
    clickhouse_user = os.getenv('CLICKHOUSE_USER', 'default')
    clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', 'quie1ahpoo5Su0wohpaedae8keeph6bi')
    database_name = os.getenv('CLICKHOUSE_DB', 'default')

    client = Client(
        host=clickhouse_host,
        user=clickhouse_user,
        password=clickhouse_password,
        port=9000,
        secure=False,
        settings={'strings_encoding': 'utf-8'}
    )
    logger.info("Подключение к ClickHouse установлено.")

    # 4. Определение minimal_date и периода (4000 дней)
    min_date_query = f"SELECT MIN(Dates) FROM {database_name}.OlapCube_VNV"
    min_date_result = client.execute(min_date_query)
    if not min_date_result or not min_date_result[0][0]:
        logger.error("Не удалось определить MIN(Dates) в OlapCube_VNV.")
        return
    min_date = min_date_result[0][0]
    logger.info(f"Найдена min_date = {min_date}")

    max_date = min_date + pd.Timedelta(days=7)
    logger.info(f"Будем обрабатывать даты в диапазоне [{min_date}, {max_date})")

    # 5. ALTER TABLE (если нужно добавить колонки)
    # Подставьте названия новых полей и их типы (Float32, Nullable(Float32), и т.п.)
    new_columns_ddl = f"""
    ALTER TABLE {database_name}.OlapCube_VNV
    ADD COLUMN IF NOT EXISTS ops_count Float32,
    ADD COLUMN IF NOT EXISTS hbs_count Float32,
    ADD COLUMN IF NOT EXISTS repair_count Float32,
    ADD COLUMN IF NOT EXISTS total_operable Float32,
    ADD COLUMN IF NOT EXISTS entry_count Float32,
    ADD COLUMN IF NOT EXISTS exit_count Float32,
    ADD COLUMN IF NOT EXISTS into_repair Float32,
    ADD COLUMN IF NOT EXISTS complete_repair Float32,
    ADD COLUMN IF NOT EXISTS remain_repair Float32,
    ADD COLUMN IF NOT EXISTS remain Float32,
    ADD COLUMN IF NOT EXISTS midfire_repair Float32,
    ADD COLUMN IF NOT EXISTS midfire Float32,
    ADD COLUMN IF NOT EXISTS hours Float32
    """
    try:
        client.execute(new_columns_ddl)
        logger.info("Новые поля (если их не было) успешно добавлены в OlapCube_VNV.")
    except Exception as e:
        logger.error(f"Ошибка при добавлении новых полей: {e}", exc_info=True)
        return

    # 6. Чтение нужных полей из OlapCube_VNV
    query = f"""
    SELECT 
        serialno,
        Dates,
        Status,
        Status_P,
        sne,
        ppr,
        oh,
        daily_flight_hours
        -- при необходимости добавьте поля ...
    FROM {database_name}.OlapCube_VNV
    WHERE Dates >= '{min_date}' 
      AND Dates <  '{max_date}'
    ORDER BY Dates, serialno
    """

    result = client.execute(query)
    if not result:
        logger.warning("Нет данных в выбранном диапазоне. Завершение.")
        return

    columns = ['serialno','Dates','Status','Status_P','sne','ppr','oh','daily_flight_hours']
    df = pd.DataFrame(result, columns=columns)
    logger.info(f"Загружено {len(df)} строк из OlapCube_VNV.")

    # Приведение типов
    df['Dates'] = pd.to_datetime(df['Dates']).dt.date
    for col in ['sne','ppr','oh','daily_flight_hours']:
        df[col] = df[col].astype(np.float32, errors='ignore')
    for col in ['Status','Status_P']:
        df[col] = df[col].fillna('').astype(str)

    # 7. Создаём столбцы для новых полей (в памяти)
    new_fields = [
        'ops_count','hbs_count','repair_count','total_operable','entry_count','exit_count',
        'into_repair','complete_repair','remain_repair','remain','midfire_repair','midfire','hours'
    ]
    for nf in new_fields:
        df[nf] = np.float32(0.0)  # Или None, если нужно Nullable

    # 8. Список уникальных дат
    all_dates = sorted(df['Dates'].unique())
    if len(all_dates) < 2:
        logger.warning("Мало дат для вычисления (нужна хотя бы 2). Завершение.")
        return

    # 9. Вычисляем поля
    logger.info("Начинаем цикл по датам для вычисления новых полей...")

    for i in range(1, len(all_dates)):
        d_curr = all_dates[i]
        d_prev = all_dates[i-1]

        df_curr = df[df['Dates'] == d_curr]
        df_prev = df[df['Dates'] == d_prev]

        # Пример расчёта "простых" счётчиков:
        ops_count_val = len( df_curr[df_curr['Status']=='Эксплуатация'] )
        hbs_count_val = len( df_curr[df_curr['Status']=='Исправен'] )
        repair_count_val = len( df_curr[df_curr['Status']=='Ремонт'] )
        total_operable_val = ops_count_val + hbs_count_val + repair_count_val

        # Сопоставляем df_prev и df_curr по serialno для счётчиков типа entry_count, exit_count
        merged = df_prev[['serialno','Status','Status_P']].merge(
                    df_curr[['serialno','Status','Status_P']],
                    on='serialno',
                    suffixes=('_prev','_curr')
                 )
        # Пример: exit_count
        exit_count_val = len( merged[
            (merged['Status_prev']=='Эксплуатация') &
            (merged['Status_P_curr']=='Хранение')  # или Status_curr == 'Хранение'
        ] )

        # Пример: hours = сумма daily_flight_hours
        hours_val = df_curr['daily_flight_hours'].sum()

        # И т.д. для остальных полей
        # (into_repair, complete_repair, remain, remain_repair, midfire, midfire_repair, etc.)
        # Подставьте ваш конкретный код / формулы

        # Записываем результаты во все строки даты d_curr
        df.loc[df['Dates']==d_curr, 'ops_count']       = ops_count_val
        df.loc[df['Dates']==d_curr, 'hbs_count']       = hbs_count_val
        df.loc[df['Dates']==d_curr, 'repair_count']    = repair_count_val
        df.loc[df['Dates']==d_curr, 'total_operable']  = total_operable_val
        df.loc[df['Dates']==d_curr, 'exit_count']      = exit_count_val
        df.loc[df['Dates']==d_curr, 'hours']           = hours_val
        # и т.д. для других полей

    logger.info("Расчёт новых полей завершён.")

    # 10. Создаём временную таблицу
    tmp_table_name = "tmp_OlapCube_newfields"
    drop_tmp_ddl = f"DROP TABLE IF EXISTS {database_name}.{tmp_table_name}"
    client.execute(drop_tmp_ddl)
    logger.info(f"Старая временная таблица {tmp_table_name} удалена (если была).")

    create_tmp_ddl = f"""
    CREATE TABLE {database_name}.{tmp_table_name} (
        serialno String,
        Dates Date,
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
        midfire_repair Float32,
        midfire Float32,
        hours Float32
    ) ENGINE = Memory
    """
    client.execute(create_tmp_ddl)
    logger.info(f"Временная таблица {tmp_table_name} создана.")

    # 11. Подготовка к вставке
    # Сохраним только нужные столбцы
    new_cols_list = [
        'serialno','Dates',
        'ops_count','hbs_count','repair_count','total_operable',
        'entry_count','exit_count','into_repair','complete_repair',
        'remain_repair','remain','midfire_repair','midfire','hours'
    ]
    df_for_insert = df[new_cols_list].copy()
    # Преобразуем NaN → None
    df_for_insert = df_for_insert.replace({np.nan: None})

    # Сформируем список кортежей
    records = list(df_for_insert.itertuples(index=False, name=None))
    logger.info(f"Подготовлено {len(records)} строк для вставки во временную таблицу.")

    insert_tmp_query = f"""
    INSERT INTO {database_name}.{tmp_table_name} (
        serialno, Dates,
        ops_count, hbs_count, repair_count, total_operable,
        entry_count, exit_count, into_repair, complete_repair,
        remain_repair, remain, midfire_repair, midfire, hours
    ) VALUES
    """
    client.execute(insert_tmp_query, records)
    logger.info("Вставка во временную таблицу завершена.")

    # 12. Перенос в OlapCube_VNV - только обновление полей, без DELETE + INSERT
    logger.info("Обновляем только новые поля в OlapCube_VNV...")

    # Для ReplaceMergeTree гораздо надёжнее использовать другой подход
    try:
        # Подготавливаем список полей
        update_fields = [
            'ops_count', 'hbs_count', 'repair_count', 'total_operable', 
            'entry_count', 'exit_count', 'into_repair', 'complete_repair',
            'remain_repair', 'remain', 'midfire_repair', 'midfire', 'hours'
        ]

        # Определим версию ClickHouse, так как синтаксис может отличаться
        version_query = "SELECT version()"
        version_result = client.execute(version_query)
        ch_version = version_result[0][0]
        logger.info(f"Версия ClickHouse: {ch_version}")
        
        # Создадим временную таблицу для хранения ключей и новых значений
        for field in update_fields:
            try:
                logger.info(f"Обновление поля: {field}")
                
                # Загружаем данные во временную таблицу для одного поля
                temp_table = f"tmp_update_{field}"
                drop_temp = f"DROP TABLE IF EXISTS {database_name}.{temp_table}"
                client.execute(drop_temp)
                
                create_temp = f"""
                CREATE TABLE {database_name}.{temp_table} (
                    serialno String,
                    Dates Date,
                    value Float32
                ) ENGINE = Memory
                """
                client.execute(create_temp)
                
                # Вставляем данные для обновления из временной таблицы
                insert_temp = f"""
                INSERT INTO {database_name}.{temp_table}
                SELECT serialno, Dates, {field} as value
                FROM {database_name}.{tmp_table_name}
                """
                client.execute(insert_temp)
                
                # Обновляем данные 
                update_query = f"""
                ALTER TABLE {database_name}.OlapCube_VNV
                UPDATE {field} = tv.value
                FROM {database_name}.{temp_table} as tv
                WHERE 
                    OlapCube_VNV.serialno = tv.serialno AND 
                    OlapCube_VNV.Dates = tv.Dates
                """
                
                try:
                    client.execute(update_query)
                    logger.info(f"Успешно обновлено поле: {field}")
                except Exception as e:
                    logger.error(f"Ошибка при обновлении поля {field}: {e}")
                    
                    # Попробуем старый синтаксис
                    try:
                        old_update_query = f"""
                        ALTER TABLE {database_name}.OlapCube_VNV
                        UPDATE {field} = (
                            SELECT value 
                            FROM {database_name}.{temp_table} as tv
                            WHERE 
                                tv.serialno = OlapCube_VNV.serialno AND 
                                tv.Dates = OlapCube_VNV.Dates
                        )
                        WHERE (serialno, Dates) IN (
                            SELECT serialno, Dates 
                            FROM {database_name}.{temp_table}
                        )
                        """
                        client.execute(old_update_query)
                        logger.info(f"Успешно обновлено поле {field} с использованием альтернативного синтаксиса")
                    except Exception as e2:
                        logger.error(f"Не удалось обновить поле {field} альтернативным методом: {e2}")
                        
                        # Пробуем последний вариант - обновление через временную таблицу с материализованным представлением
                        try:
                            logger.info(f"Пробуем обновить через пакетные SQL запросы для поля {field}")
                            
                            # Получаем данные из временной таблицы
                            fetch_query = f"""
                            SELECT serialno, Dates, value 
                            FROM {database_name}.{temp_table}
                            """
                            rows_to_update = client.execute(fetch_query)
                            
                            # Группируем по 100 строк для пакетного обновления
                            batch_size = 100
                            for i in range(0, len(rows_to_update), batch_size):
                                batch = rows_to_update[i:i+batch_size]
                                serials = [f"'{row[0]}'" for row in batch]
                                dates = [f"'{row[1]}'" for row in batch]
                                
                                # Формируем условия для IN
                                conditions = []
                                for j in range(len(batch)):
                                    conditions.append(f"(serialno = {serials[j]} AND Dates = {dates[j]})")
                                
                                where_clause = " OR ".join(conditions)
                                
                                # Формируем запрос CASE WHEN для каждой строки
                                cases = []
                                for j in range(len(batch)):
                                    cases.append(f"WHEN serialno = {serials[j]} AND Dates = {dates[j]} THEN {batch[j][2]}")
                                
                                case_clause = "CASE " + " ".join(cases) + f" ELSE {field} END"
                                
                                # Обновляем данные
                                batch_update = f"""
                                ALTER TABLE {database_name}.OlapCube_VNV
                                UPDATE {field} = {case_clause}
                                WHERE {where_clause}
                                """
                                
                                try:
                                    client.execute(batch_update)
                                    logger.info(f"Пакетно обновлено {len(batch)} строк для поля {field}")
                                except Exception as e3:
                                    logger.error(f"Ошибка при пакетном обновлении поля {field}: {e3}")
                        except Exception as e3:
                            logger.error(f"Не удалось обновить поле {field} пакетным методом: {e3}")
                
                # Удаляем временную таблицу
                client.execute(drop_temp)
                
            except Exception as e:
                logger.error(f"Общая ошибка при обработке поля {field}: {e}")
                continue
                
        logger.info("Обновление полей завершено.")
        
    except Exception as e:
        logger.error(f"Общая ошибка при обновлении данных: {e}", exc_info=True)
        
    # Для ReplaceMergeTree полезно выполнить оптимизацию после обновления
    optimize_query = f"OPTIMIZE TABLE {database_name}.OlapCube_VNV FINAL"
    client.execute(optimize_query)
    logger.info("OPTIMIZE TABLE завершён.")

    # 14. Удаляем временную таблицу
    client.execute(drop_tmp_ddl)
    logger.info("Временная таблица удалена.")

    # Пример проверки
    # SELECT несколько строк для подтверждения
    check_sql = f"""
    SELECT serialno, Dates, ops_count, hbs_count, hours
    FROM {database_name}.OlapCube_VNV
    WHERE Dates = '{all_dates[1]}'  -- или другой тест
    LIMIT 5
    """
    check_data = client.execute(check_sql)
    logger.info(f"Проверка (5 строк) на дату {all_dates[1]}:")
    for row in check_data:
        logger.info(row)

    logger.info("Скрипт завершён успешно.")

if __name__ == "__main__":
    main()
