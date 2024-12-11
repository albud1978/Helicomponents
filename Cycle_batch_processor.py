import pandas as pd
import numpy as np
from clickhouse_driver import Client
from datetime import datetime, timedelta
import logging
import sys
import os
from typing import List, Dict, Any
from dotenv import load_dotenv
import time

# Загрузка переменных окружения
load_dotenv()

class BatchCycleProcessor:
    def __init__(self, window_size=100, batch_size=50):
        # Настройка логирования
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(handler)

        # Параметры подключения к ClickHouse
        self.clickhouse_host = os.getenv('CLICKHOUSE_HOST', '10.95.19.132')
        self.clickhouse_user = os.getenv('CLICKHOUSE_USER', 'default')
        self.clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', 'quie1ahpoo5Su0wohpaedae8keeph6bi')
        self.database_name = os.getenv('CLICKHOUSE_DB', 'default')

        # Инициализация подключения к ClickHouse
        self.client = Client(
            host=self.clickhouse_host,
            user=self.clickhouse_user,
            password=self.clickhouse_password,
            database=self.database_name
        )
        
        # Параметры батчей
        self.window_size = window_size
        self.batch_size = batch_size
        self.temp_df = None
        self.results_to_write = []

    def initialize_window(self, start_date: datetime) -> None:
        """Инициализация первого окна данных"""
        query = """
            WITH
                (
                    SELECT
                        mi8t_count,
                        mi17_count
                    FROM OlapCube_VNV FINAL
                    WHERE Dates = %(start_date)s
                    LIMIT 1
                ) AS counts
            SELECT
                serialno,
                Dates,
                Status,
                Status_P,
                sne,
                ppr,
                repair_days,
                ll,
                oh,
                BR,
                daily_flight_hours,
                RepairTime,
                ac_typ,
                counts.mi8t_count,
                counts.mi17_count
            FROM OlapCube_VNV FINAL
            WHERE Dates >= %(start_date)s 
            AND Dates < dateAdd(day, %(window_size)s, %(start_date)s)
            ORDER BY Dates, serialno
            SETTINGS optimize_read_in_order=1
        """
        
        data = self.client.execute(
            query,
            {'start_date': start_date, 'window_size': self.window_size},
            settings={'max_threads': 8}
        )
        
        if not data:
            raise Exception(f"Нет данных для окна с {start_date} по {start_date + timedelta(days=self.window_size)}")
        
        # Преобразование в DataFrame
        columns = [
            'serialno', 'Dates', 'Status', 'Status_P', 'sne', 'ppr', 'repair_days',
            'll', 'oh', 'BR', 'daily_flight_hours', 'RepairTime', 'ac_typ',
            'mi8t_count', 'mi17_count'
        ]
        
        self.temp_df = pd.DataFrame(data, columns=columns)
        
        # Приведение типов данных
        numeric_columns = ['sne', 'ppr', 'repair_days', 'll', 'oh', 'BR', 
                         'daily_flight_hours', 'RepairTime', 'mi8t_count', 'mi17_count']
        for col in numeric_columns:
            self.temp_df[col] = pd.to_numeric(self.temp_df[col], errors='coerce')
        
        self.logger.info(f"Загружено записей: {len(self.temp_df)}")

    def load_next_batch(self, next_start_date: datetime) -> None:
        """Загрузка следующего батча данных"""
        self.logger.info(f"Загрузка следующего батча с {next_start_date} на {self.batch_size} дней")
        
        # Формируем список дат для батча
        dates = [(next_start_date + timedelta(days=i)).strftime('%Y-%m-%d') 
                for i in range(self.batch_size)]
        dates_str = "', '".join(dates)
        
        query = f"""
            WITH
                (
                    SELECT
                        mi8t_count,
                        mi17_count
                    FROM OlapCube_VNV FINAL
                    WHERE Dates = '{dates[0]}'
                    LIMIT 1
                ) AS counts
            SELECT
                serialno,
                Dates,
                Status,
                Status_P,
                sne,
                ppr,
                repair_days,
                ll,
                oh,
                BR,
                daily_flight_hours,
                RepairTime,
                ac_typ,
                counts.mi8t_count,
                counts.mi17_count
            FROM OlapCube_VNV FINAL
            WHERE Dates IN ('{dates_str}')
            ORDER BY Dates, serialno
            SETTINGS optimize_read_in_order=1
        """
        
        result = self.client.execute(
            query,
            settings={'max_threads': 8}
        )
        
        if not result:
            raise Exception(f"Нет данных для батча с {next_start_date} по {next_start_date + timedelta(days=self.batch_size)}")
        
        # Преобразование в DataFrame
        columns = [
            'serialno', 'Dates', 'Status', 'Status_P', 'sne', 'ppr', 'repair_days',
            'll', 'oh', 'BR', 'daily_flight_hours', 'RepairTime', 'ac_typ',
            'mi8t_count', 'mi17_count'
        ]
        
        new_df = pd.DataFrame(result, columns=columns)
        
        # Приведение типов данных
        numeric_columns = ['sne', 'ppr', 'repair_days', 'll', 'oh', 'BR', 
                         'daily_flight_hours', 'RepairTime', 'mi8t_count', 'mi17_count']
        for col in numeric_columns:
            new_df[col] = pd.to_numeric(new_df[col], errors='coerce')
        
        # Удаляем старые даты и добавляем новые
        self.temp_df = pd.concat([
            self.temp_df[self.temp_df['Dates'] >= next_start_date - timedelta(days=1)],
            new_df
        ]).drop_duplicates(subset=['serialno', 'Dates'])
        
        self.logger.info(f"Загружено новых записей: {len(new_df)}")
        self.logger.info(f"Всего записей в окне: {len(self.temp_df)}")

    def process_batch(self, dates: List[datetime]) -> None:
        """Обработка батча дат"""
        try:
            # Обрабатываем каждую пару дат
            for i in range(1, len(dates)):
                prev_date = dates[i-1]
                curr_date = dates[i]
                
                try:
                    # Получаем данные для текущего и предыдущего дня
                    prev_data = self.temp_df[self.temp_df['Dates'] == prev_date]
                    curr_data = self.temp_df[self.temp_df['Dates'] == curr_date]
                    
                    if prev_data.empty or curr_data.empty:
                        continue
                        
                    # Проверяем соответствие serialno
                    if not set(prev_data['serialno']) == set(curr_data['serialno']):
                        continue
                    
                    # Обработка одного дня
                    result_df = self.process_single_day(curr_date, prev_date)
                    
                    # Если есть результат, добавляем его в список для записи
                    if not result_df.empty:
                        self.results_to_write.append((curr_date, result_df))
                
                except Exception as e:
                    self.logger.error(f"Ошибка при обработке дня {curr_date}: {str(e)}")
                    continue
            
            # Записываем результаты в базу данных
            if self.results_to_write:
                self.write_results_to_db()
                self.results_to_write = []  # Очищаем список после записи
        
        except Exception as e:
            self.logger.error(f"Ошибка при обработке батча: {str(e)}")

    def process_single_day(self, current_date: datetime, prev_date: datetime) -> pd.DataFrame:
        """Обработка одного дня по шагам I-IV"""
        # Получаем данные для текущего и предыдущего дня
        prev_data = self.temp_df[self.temp_df['Dates'] == prev_date]
        curr_data = self.temp_df[self.temp_df['Dates'] == current_date].copy()
        
        # Проверяем наличие данных для обоих дней и соответствие serialno
        if prev_data.empty or curr_data.empty:
            return pd.DataFrame()
            
        # Создаем рабочую копию с данными предыдущего дня
        working_df = curr_data.copy()
        
        # Копируем все поля из предыдущего дня с суффиксом _prev
        prev_fields = ['Status', 'Status_P', 'sne', 'ppr', 'repair_days']
        
        # Используем merge для безопасного копирования данных
        prev_data_subset = prev_data[['serialno'] + prev_fields].copy()
        prev_data_subset.columns = ['serialno'] + [f'{col}_prev' for col in prev_fields]
        
        working_df = pd.merge(
            working_df,
            prev_data_subset,
            on='serialno',
            how='left'
        )
        
        working_df['Status_P'] = None  # Инициализируем Status_P как None

        # Шаг I: Определение Status_P
        # Обработка новых записей
        new_records_mask = working_df['Status'].isna()
        working_df.loc[new_records_mask, 'Status_P'] = 'Неактивно'
        
        # Обработка существующих записей
        existing_records = ~new_records_mask

        # 1. Копирование неизменяемых статусов
        for status in ['Неактивно', 'Хранение', 'Исправен']:
            mask = existing_records & (working_df['Status'] == status)
            working_df.loc[mask, 'Status_P'] = status

        # 2. Обработка статуса "Ремонт"
        repair_mask = existing_records & (working_df['Status'] == 'Ремонт')
        repair_time_check = working_df['repair_days'] < working_df['RepairTime']
        working_df.loc[repair_mask & repair_time_check, 'Status_P'] = 'Ремонт'
        working_df.loc[repair_mask & ~repair_time_check, 'Status_P'] = 'Исправен'

        # 3. Обработка статуса "Эксплуатация"
        explo_mask = existing_records & (working_df['Status'] == 'Эксплуатация')
        
        # Проверка условий для продолжения эксплуатации
        sne_check = working_df['sne_prev'] < (working_df['ll'] - working_df['daily_flight_hours'])
        ppr_check = working_df['ppr_prev'] < (working_df['oh'] - working_df['daily_flight_hours'])
        working_df.loc[explo_mask & sne_check & ppr_check, 'Status_P'] = 'Эксплуатация'

        # Проверка условий для перехода в хранение
        sne_limit = working_df['sne_prev'] >= (working_df['ll'] - working_df['daily_flight_hours'])
        working_df.loc[explo_mask & sne_limit, 'Status_P'] = 'Хранение'

        # Проверка условий для ремонта или хранения
        ppr_limit = working_df['ppr_prev'] >= (working_df['oh'] - working_df['daily_flight_hours'])
        br_check = working_df['sne_prev'] < working_df['BR']
        working_df.loc[explo_mask & ppr_limit & br_check, 'Status_P'] = 'Ремонт'
        working_df.loc[explo_mask & ppr_limit & ~br_check, 'Status_P'] = 'Хранение'

        # Шаг II: Расчет балансов и запасов
        mi8t_count = curr_data['mi8t_count'].iloc[0] if not pd.isna(curr_data['mi8t_count'].iloc[0]) else 0
        mi17_count = curr_data['mi17_count'].iloc[0] if not pd.isna(curr_data['mi17_count'].iloc[0]) else 0

        # Расчеты для Ми-8Т
        balance_mi8t = len(working_df[
            (working_df['Status_P'] == 'Эксплуатация') & 
            (working_df['ac_typ'] == 'Ми-8Т')
        ])
        stock_mi8t = len(working_df[
            (working_df['Status_P'] == 'Исправен') & 
            (working_df['ac_typ'] == 'Ми-8Т')
        ])

        # Расчеты для Ми-17
        balance_mi17 = len(working_df[
            (working_df['Status_P'] == 'Эксплуатация') & 
            (working_df['ac_typ'] == 'Ми-17')
        ])
        stock_mi17 = len(working_df[
            (working_df['Status_P'] == 'Исправен') & 
            (working_df['ac_typ'] == 'Ми-17')
        ])

        # Расчеты для пустых значений ac_typ
        balance_empty = len(working_df[
            (working_df['Status_P'] == 'Эксплуатация') & 
            (working_df['ac_typ'].isna())
        ])
        stock_empty = len(working_df[
            (working_df['Status_P'] == 'Исправен') & 
            (working_df['ac_typ'].isna())
        ])

        # Расчет финальных балансов
        final_balance_mi8t = balance_mi8t - mi8t_count
        final_balance_mi17 = balance_mi17 - mi17_count
        final_balance_total = final_balance_mi8t + final_balance_mi17 + balance_empty

        # Шаг III: Формирование Status на основе balance_total и Status_P
        if final_balance_total > 0:
            # Если balance_total > 0, переводим ВС из 'Эксплуатация' в 'Исправен'
            exploitation_mask = working_df['Status_P'] == 'Эксплуатация'
            exploitation_indices = working_df[exploitation_mask].index.tolist()
            if exploitation_indices:
                change_count = min(int(final_balance_total), len(exploitation_indices))
                working_df.loc[exploitation_indices[:change_count], 'Status'] = 'Исправен'
                if change_count < len(exploitation_indices):
                    working_df.loc[exploitation_indices[change_count:], 'Status'] = working_df.loc[exploitation_indices[change_count:], 'Status_P']
        elif final_balance_total < 0:
            # Если balance_total < 0, переводим ВС из 'Исправен' или 'Неактивно' в 'Эксплуатация'
            serviceable_mask = working_df['Status_P'].isin(['Исправен', 'Неактивно'])
            serviceable_indices = working_df[serviceable_mask].index.tolist()
            if serviceable_indices:
                change_count = min(int(abs(final_balance_total)), len(serviceable_indices))
                working_df.loc[serviceable_indices[:change_count], 'Status'] = 'Эксплуатация'
                if change_count < len(serviceable_indices):
                    working_df.loc[serviceable_indices[change_count:], 'Status'] = working_df.loc[serviceable_indices[change_count:], 'Status_P']
        else:
            # Если balance_total = 0, Status = Status_P
            working_df['Status'] = working_df['Status_P']

        # Шаг IV: Обработка счетчиков
        # Обновляем sne и ppr только для ВС в эксплуатации
        exploitation_mask = working_df['Status'] == 'Эксплуатация'
        working_df.loc[exploitation_mask, 'sne'] = working_df.loc[exploitation_mask, 'sne_prev'].fillna(0) + working_df.loc[exploitation_mask, 'daily_flight_hours']
        working_df.loc[exploitation_mask, 'ppr'] = working_df.loc[exploitation_mask, 'ppr_prev'].fillna(0) + working_df.loc[exploitation_mask, 'daily_flight_hours']
        
        # Для остальных ВС копируем значения с предыдущего дня
        non_exploitation_mask = ~exploitation_mask
        working_df.loc[non_exploitation_mask, 'sne'] = working_df.loc[non_exploitation_mask, 'sne_prev'].fillna(0)
        working_df.loc[non_exploitation_mask, 'ppr'] = working_df.loc[non_exploitation_mask, 'ppr_prev'].fillna(0)

        # Обновляем repair_days для ВС в ремонте
        repair_mask = working_df['Status'] == 'Ремонт'
        working_df.loc[repair_mask, 'repair_days'] = working_df.loc[repair_mask, 'repair_days_prev'].fillna(0) + 1
        working_df.loc[~repair_mask, 'repair_days'] = 0

        # Возвращаем только нужные колонки
        result_columns = ['serialno', 'Dates', 'Status', 'Status_P', 'sne', 'ppr', 'repair_days', 'balance_mi8t', 'balance_mi17', 'balance_empty', 'balance_total', 'stock_mi8t', 'stock_mi17', 'stock_empty', 'stock_total']
        working_df['balance_mi8t'] = final_balance_mi8t
        working_df['balance_mi17'] = final_balance_mi17
        working_df['balance_empty'] = balance_empty
        working_df['balance_total'] = final_balance_total
        working_df['stock_mi8t'] = stock_mi8t
        working_df['stock_mi17'] = stock_mi17
        working_df['stock_empty'] = stock_empty
        working_df['stock_total'] = stock_mi8t + stock_mi17 + stock_empty
        return working_df[result_columns]

    def write_results_to_db(self) -> None:
        """Запись результатов в базу данных"""
        if not self.results_to_write:
            self.logger.warning("Нет данных для записи")
            return
        
        try:
            # Объединяем все результаты в один датафрейм
            results_df = pd.concat([df for _, df in self.results_to_write], ignore_index=True)
            self.logger.info(f"\nЗапись результатов для {len(results_df['Dates'].unique())} дат")
            self.logger.info(f"Общее количество записей: {len(results_df)}")
            
            # Формируем данные для обновления
            update_data = []
            for _, row in results_df.iterrows():
                update_data.append((
                    row['serialno'],
                    row['Dates'],
                    row['Status'],
                    row['Status_P'],
                    float(row['sne']),
                    float(row['ppr']),
                    int(row['repair_days'])
                ))
            
            try:
                # Создаем временную таблицу
                create_temp_table = """
                    CREATE TEMPORARY TABLE IF NOT EXISTS updates_temp (
                        serialno String,
                        Dates Date,
                        Status String,
                        Status_P String,
                        sne Float64,
                        ppr Float64,
                        repair_days Int32
                    ) ENGINE = Memory
                """
                self.client.execute(create_temp_table)
                
                # Вставляем данные во временную таблицу
                insert_query = "INSERT INTO updates_temp VALUES"
                self.client.execute(insert_query, update_data)
                
                # Проверяем данные во временной таблице
                check_temp_query = "SELECT COUNT(*) as cnt FROM updates_temp"
                temp_count = self.client.execute(check_temp_query)[0][0]
                self.logger.info(f"Записей во временной таблице: {temp_count}")
                
                # Проверяем соответствие записей
                check_match_query = """
                    SELECT COUNT(*) as cnt
                    FROM OlapCube_VNV t1
                    INNER JOIN updates_temp t2 
                    ON t1.serialno = t2.serialno 
                    AND t1.Dates = t2.Dates
                """
                match_count = self.client.execute(check_match_query)[0][0]
                self.logger.info(f"Найдено соответствий для обновления: {match_count}")
                
                # Проверяем данные до обновления
                check_before_query = """
                    SELECT serialno, Dates, Status, Status_P, sne, ppr, repair_days
                    FROM OlapCube_VNV t1
                    INNER JOIN updates_temp t2 
                    ON t1.serialno = t2.serialno 
                    AND t1.Dates = t2.Dates
                    LIMIT 5
                """
                before_data = self.client.execute(check_before_query)
                self.logger.info("Данные до обновления (первые 5 записей):")
                for row in before_data:
                    self.logger.info(f"serialno: {row[0]}, Dates: {row[1]}, Status: {row[2]}, Status_P: {row[3]}, sne: {row[4]}, ppr: {row[5]}, repair_days: {row[6]}")

                # Упрощенный UPDATE запрос
                update_query = """
                    ALTER TABLE OlapCube_VNV
                    UPDATE 
                        Status = ut.Status,
                        Status_P = ut.Status_P,
                        sne = ut.sne,
                        ppr = ut.ppr,
                        repair_days = ut.repair_days
                    FROM updates_temp ut
                    WHERE serialno = ut.serialno
                    AND Dates = ut.Dates
                """
                
                self.client.execute(update_query)
                
                # Проверяем данные после обновления
                check_after_query = """
                    SELECT serialno, Dates, Status, Status_P, sne, ppr, repair_days
                    FROM OlapCube_VNV t1
                    INNER JOIN updates_temp t2 
                    ON t1.serialno = t2.serialno 
                    AND t1.Dates = t2.Dates
                    LIMIT 5
                """
                after_data = self.client.execute(check_after_query)
                self.logger.info("\nДанные после обновления (первые 5 записей):")
                for row in after_data:
                    self.logger.info(f"serialno: {row[0]}, Dates: {row[1]}, Status: {row[2]}, Status_P: {row[3]}, sne: {row[4]}, ppr: {row[5]}, repair_days: {row[6]}")
                
            finally:
                # Гарантированно удаляем временную таблицу
                self.client.execute("DROP TABLE IF EXISTS updates_temp")
            
            # Очищаем список результатов только после успешного обновления
            self.results_to_write.clear()
            
        except Exception as e:
            self.logger.error(f"Ошибка при записи результатов в базу данных: {str(e)}")
            raise

    def get_first_date(self) -> datetime:
        """Получение первой даты из куба"""
        query = """
            SELECT MIN(Dates) as first_date 
            FROM OlapCube_VNV 
            WHERE Status IS NOT NULL
        """
        first_date = self.client.execute(query)[0][0]
        if not first_date:
            raise Exception("Не найдена начальная дата")
        
        self.logger.info(f"Первая дата в кубе: {first_date.strftime('%Y-%m-%d')}")
        return first_date

    def run_rolling_cycle(self, start_date: datetime, total_days: int = 4000) -> None:
        """Основной метод запуска обработки"""
        self.logger.info(f"Запуск обработки с параметрами:\n\
            Размер окна: {self.window_size} дней\n\
            Размер батча: {self.batch_size} дней\n\
            Всего дней: {total_days}")
        
        # Получаем первую дату из куба
        first_cube_date = self.get_first_date()
        
        # Начальная дата расчета - следующий день после первой даты куба
        calc_start_date = first_cube_date + timedelta(days=1)
        self.logger.info(f"Начальная дата расчета: {calc_start_date.strftime('%Y-%m-%d')}")
        
        # Инициализация первого окна, начиная с первой даты куба
        self.initialize_window(first_cube_date)
        
        # Цикл по батчам дат
        current_date = calc_start_date
        end_date = calc_start_date + timedelta(days=total_days)
        
        while current_date < end_date:
            batch_end = min(current_date + timedelta(days=self.batch_size), end_date)
            batch_dates = []
            
            # Для первого батча добавляем первую дату куба
            if current_date == calc_start_date:
                batch_dates = [first_cube_date]
            else:
                batch_dates = [current_date - timedelta(days=1)]
                
            # Добавляем даты текущего батча
            batch_dates.extend([
                current_date + timedelta(days=i)
                for i in range((batch_end - current_date).days)
            ])
            
            self.logger.info(f"\nБатч дат: {batch_dates[0].strftime('%Y-%m-%d')} - {batch_dates[-1].strftime('%Y-%m-%d')}")
            
            # Обработка батча
            self.process_batch(batch_dates)
            
            # Сдвигаем окно, если необходимо
            next_window_start = batch_end - timedelta(days=self.window_size)
            if next_window_start > first_cube_date:
                self.load_next_batch(next_window_start)
            
            current_date = batch_end

if __name__ == "__main__":
    # Настройка параметров обработки
    WINDOW_SIZE = 100  # Размер окна в днях
    BATCH_SIZE = 50    # Размер батча в днях
    TOTAL_DAYS = 200   # Общее количество дней для обработки
    
    # Создание процессора
    processor = BatchCycleProcessor(window_size=WINDOW_SIZE, batch_size=BATCH_SIZE)
    
    try:
        # Получаем начальную дату из куба
        first_date = processor.get_first_date()
        start_date = first_date + timedelta(days=1)  # Начинаем со следующего дня
        
        processor.logger.info(f"""Запуск обработки с параметрами:
            Размер окна: {WINDOW_SIZE} дней
            Размер батча: {BATCH_SIZE} дней
            Всего дней: {TOTAL_DAYS}
            Начальная дата: {start_date.strftime('%Y-%m-%d')}
        """)
        
        processor.run_rolling_cycle(start_date, total_days=TOTAL_DAYS)
        
    except Exception as e:
        processor.logger.error(f"Ошибка при обработке: {str(e)}")
    finally:
        processor.logger.info("Завершение обработки")
