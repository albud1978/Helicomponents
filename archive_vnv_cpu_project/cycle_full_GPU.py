import os
import logging
import numpy as np
import cudf
import cupy as cp
import pandas as pd
from clickhouse_driver import Client
from dotenv import load_dotenv
from datetime import datetime, timedelta
import sys
import time

# Загрузка переменных окружения
load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # DEBUG для детальной отладки
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
if not logger.handlers:
    logger.addHandler(handler)


class CycleCUDAProcessor:
    def __init__(self, total_days):
        self.logger = logger
        self.total_days = total_days

        clickhouse_host = os.getenv('CLICKHOUSE_HOST', '10.95.19.132')
        clickhouse_user = os.getenv('CLICKHOUSE_USER', 'default')
        clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', 'quie1ahpoo5Su0wohpaedae8keeph6bi')
        database_name = os.getenv('CLICKHOUSE_DB', 'default')

        self.client = Client(
            host=clickhouse_host,
            user=clickhouse_user,
            password=clickhouse_password,
            port=9000,
            secure=False,
            settings={'strings_encoding': 'utf-8'}
        )
        self.database_name = database_name
        self.gpu_df = None
        
        # Проверка доступности GPU
        try:
            self.logger.info(f"CUDA доступен: {cp.cuda.is_available()}")
            self.logger.info(f"Количество GPU: {cp.cuda.runtime.getDeviceCount()}")
            for i in range(cp.cuda.runtime.getDeviceCount()):
                props = cp.cuda.runtime.getDeviceProperties(i)
                self.logger.info(f"GPU {i}: {props['name'].decode()}, Память: {props['totalGlobalMem'] / (1024**3):.2f} ГБ")
        except Exception as e:
            self.logger.warning(f"Ошибка при проверке GPU: {e}")

    def get_first_date(self) -> datetime:
        query = f"SELECT MIN(Dates) as first_date FROM {self.database_name}.OlapCube_VNV"
        result = self.client.execute(query)
        first_date = result[0][0]
        if not first_date:
            raise Exception("Не найдена начальная дата (база пустая?)")
        return first_date

    def load_all_data(self):
        """
        Загружаем данные из куба за период [first_date, first_date + total_days).
        Используем cuDF для загрузки данных в GPU память.
        """
        start_time = time.time()
        first_date = self.get_first_date()
        self.first_date = first_date
        period_end = first_date + timedelta(days=self.total_days)
        self.logger.info(f"Загружаются данные с {first_date} до {period_end}")

        query = f"""
        SELECT
             trigger_type,
             RepairTime,
             Status_P,
             repair_days,
             sne,
             ppr,
             Status,
             location,
             ac_typ,
             daily_flight_hours,
             daily_flight_hours_f,
             BR,
             ll,
             oh,
             threshold,
             Effectivity,
             serialno,
             Dates,
             mi8t_count,
             mi17_count,
             balance_mi8t,
             balance_mi17,
             balance_empty,
             balance_total,
             stock_mi8t,
             stock_mi17,
             stock_empty,
             stock_total
        FROM {self.database_name}.OlapCube_VNV
        WHERE Dates >= '{first_date.strftime('%Y-%m-%d')}'
          AND Dates < '{period_end.strftime('%Y-%m-%d')}'
        ORDER BY Dates, serialno
        """
        result = self.client.execute(query, settings={'max_threads': 8})
        if not result:
            raise Exception("Нет данных для заданного периода")
        columns = [
             'trigger_type',
             'RepairTime',
             'Status_P',
             'repair_days',
             'sne',
             'ppr',
             'Status',
             'location',
             'ac_typ',
             'daily_flight_hours',
             'daily_flight_hours_f',
             'BR',
             'll',
             'oh',
             'threshold',
             'Effectivity',
             'serialno',
             'Dates',
             'mi8t_count',
             'mi17_count',
             'balance_mi8t',
             'balance_mi17',
             'balance_empty',
             'balance_total',
             'stock_mi8t',
             'stock_mi17',
             'stock_empty',
             'stock_total'
        ]
        
        # Создаем pandas DataFrame для предварительной обработки
        df = pd.DataFrame(result, columns=columns)
        
        # Преобразуем числовые поля
        numeric_cols = [
            'trigger_type',
            'RepairTime', 'repair_days', 'sne', 'ppr', 'daily_flight_hours',
            'daily_flight_hours_f', 'BR', 'll', 'oh', 'threshold',
            'mi8t_count', 'mi17_count', 'balance_mi8t', 'balance_mi17', 'balance_empty',
            'balance_total', 'stock_mi8t', 'stock_mi17', 'stock_empty', 'stock_total'
        ]
        for col in numeric_cols:
            df[col] = df[col].astype(np.float32, errors='ignore')

        # Преобразуем строковые поля
        for col in ['Status_P', 'Status', 'location', 'ac_typ', 'Effectivity']:
            df[col] = df[col].fillna('').astype(str)

        # Преобразуем столбец Dates к типу date
        df['Dates'] = pd.to_datetime(df['Dates']).dt.date
        
        # Округляем числовые поля
        df['daily_flight_hours'] = df['daily_flight_hours'].astype(np.float64).round(2).astype(np.float32)
        df['daily_flight_hours_f'] = df['daily_flight_hours_f'].astype(np.float64).round(2).astype(np.float32)
        
        # Преобразуем даты в строки для cuDF и сохраняем оригинальные даты для сравнения
        self.dates_dict = {str(date): date for date in df['Dates'].unique()}
        df['Dates'] = df['Dates'].astype(str)
        
        # Создаем cuDF DataFrame
        self.gpu_df = cudf.DataFrame.from_pandas(df)
        
        # Не преобразуем обратно в datetime для GPU, оставляем как строки
        # self.gpu_df['Dates'] = self.gpu_df['Dates'].astype('datetime64[D]')
        
        self.logger.info(f"Данные загружены в GPU память: всего {len(self.gpu_df)} записей.")
        self.logger.info(f"Время загрузки данных: {time.time() - start_time:.2f} секунд")

    def run_cycle(self):
        start_time = time.time()
        self.load_all_data()

        period_start = self.first_date
        period_end = period_start + timedelta(days=self.total_days)
        self.logger.info(f"Обработка производится для периода {period_start} - {period_end}")

        self.logger.info("Начинаем обработку дат...")
        self.process_all_dates(period_start, period_end)
        
        self.save_all_results(period_start, period_end)
        self.logger.info("Обработка завершена. Результаты записаны в базу.")
        self.logger.info(f"Общее время выполнения: {time.time() - start_time:.2f} секунд")

        # Статистика за весь обработанный период
        stats_query = f"""
        SELECT 
            toYear(Dates) AS Year,
            toMonth(Dates) AS Month,
            count(*) AS Records,
            countIf(Status = 'Эксплуатация') AS Exploitation,
            countIf(Status = 'Исправен') AS Serviceable,
            countIf(Status = 'Ремонт') AS InRepair
        FROM {self.database_name}.OlapCube_VNV
        WHERE Dates >= '{period_start.strftime('%Y-%m-%d')}'
          AND Dates < '{period_end.strftime('%Y-%m-%d')}'
        GROUP BY Year, Month
        ORDER BY Year, Month
        """
        stats_result = self.client.execute(stats_query)
        self.logger.info("Итоговая статистика по месяцам:")
        for row in stats_result:
            self.logger.info(f"  {row[0]}-{row[1]:02d}: Всего записей: {row[2]}, Эксплуатация: {row[3]}, Исправен: {row[4]}, Ремонт: {row[5]}")

    def process_all_dates(self, period_start, period_end):
        # Получаем уникальные даты с использованием GPU
        unique_dates_str = self.gpu_df['Dates'].unique().to_pandas().sort_values().to_numpy()
        # Преобразуем строковые даты обратно в объекты datetime
        unique_dates = [self.dates_dict[date_str] for date_str in unique_dates_str]
            
        cycle_dates = [d for d in unique_dates if period_start <= d < period_end]
        
        if not cycle_dates:
            self.logger.warning(f"Нет дат для обработки в периоде {period_start} - {period_end}")
            return
            
        self.logger.info(f"Даты для обработки: всего {len(cycle_dates)} дней с {cycle_dates[0]} по {cycle_dates[-1]}")

        # Группировка по месяцам для логирования
        month_groups = {}
        for date in cycle_dates:
            month_key = date.strftime('%Y-%m')
            if month_key not in month_groups:
                month_groups[month_key] = []
            month_groups[month_key].append(date)

        # Вывод информации о количестве дней по месяцам
        for month, dates in month_groups.items():
            self.logger.info(f"Месяц {month}: {len(dates)} дней для обработки")

        # Обработка дат с использованием CUDA
        for i in range(1, len(cycle_dates)):
            curr_date = cycle_dates[i]
            prev_date = cycle_dates[i-1]
            
            # Проверяем наличие данных для текущей и предыдущей даты
            curr_date_str = str(curr_date)
            prev_date_str = str(prev_date)
            curr_data = self.gpu_df[self.gpu_df['Dates'] == curr_date_str]
            prev_data = self.gpu_df[self.gpu_df['Dates'] == prev_date_str]
            
            if len(curr_data) == 0:
                self.logger.warning(f"Нет данных для даты {curr_date}, пропускаем")
                continue
                
            if len(prev_data) == 0:
                self.logger.warning(f"Нет данных для предыдущей даты {prev_date}, пропускаем")
                continue
            
            start_time = time.time()
            self.step_1(prev_date, curr_date)
            step1_time = time.time() - start_time
            
            start_time = time.time()
            self.step_2(curr_date)
            step2_time = time.time() - start_time
            
            start_time = time.time()
            self.step_3(curr_date)
            step3_time = time.time() - start_time
            
            start_time = time.time()
            self.step_4(prev_date, curr_date)
            step4_time = time.time() - start_time

            # Логирование только в конце месяца или при последнем дне обработки
            month_key = curr_date.strftime('%Y-%m')
            is_last_day_of_month = curr_date == month_groups[month_key][-1]
            is_last_day_overall = curr_date == cycle_dates[-1]
            
            if is_last_day_of_month or is_last_day_overall:
                # Собираем статистику за месяц
                curr_date_str = str(curr_date)
                month_data = self.gpu_df[self.gpu_df['Dates'] == curr_date_str]
                status_counts = month_data['Status'].value_counts().to_pandas()
                status_p_counts = month_data['Status_P'].value_counts().to_pandas()

                self.logger.info(f"Статистика на конец {month_key} (дата {curr_date}) - Status:")
                for st, cnt in status_counts.items():
                    self.logger.info(f"  {st}: {cnt}")
                self.logger.info(f"Статистика на конец {month_key} (дата {curr_date}) - Status_P:")
                for st, cnt in status_p_counts.items():
                    self.logger.info(f"  {st}: {cnt}")
                
                self.logger.info(f"Время выполнения шагов для даты {curr_date}: "
                                f"step1={step1_time:.3f}с, step2={step2_time:.3f}с, "
                                f"step3={step3_time:.3f}с, step4={step4_time:.3f}с")

    def step_1(self, prev_date, curr_date):
        """
        Реализация step_1 с использованием CUDA и cuDF
        """
        # Преобразуем даты в строки для работы с cuDF
        prev_date_str = str(prev_date)
        curr_date_str = str(curr_date)
        
        # Получаем данные для текущей и предыдущей даты
        prev_data = self.gpu_df[self.gpu_df['Dates'] == prev_date_str]
        curr_data = self.gpu_df[self.gpu_df['Dates'] == curr_date_str]
        
        if len(curr_data) == 0 or len(prev_data) == 0:
            self.logger.warning(f"Нет данных для {curr_date} или {prev_date}")
            return
        
        # Преобразуем данные в pandas для обработки
        prev_data_pd = prev_data.to_pandas()
        curr_data_pd = curr_data.to_pandas()
        
        # Создаем словарь для быстрого доступа к предыдущим данным
        prev_dict = {}
        for _, row in prev_data_pd.iterrows():
            serialno = row['serialno']
            prev_dict[serialno] = {
                'Status': row['Status'],
                'Status_P': row['Status_P'],
                'sne': row['sne'],
                'ppr': row['ppr'],
                'repair_days': row['repair_days'],
                'RepairTime': row['RepairTime']
            }
        
        # Обновляем Status_P для каждой записи текущей даты
        for idx, row in curr_data_pd.iterrows():
            serialno = row['serialno']
            
            # Если нет данных для этого serialno в предыдущей дате, пропускаем
            if serialno not in prev_dict:
                continue
                
            prev_row = prev_dict[serialno]
            prev_status = prev_row['Status']
            
            # Определяем новый Status_P на основе предыдущего Status
            new_status_p = ''
            
            # Неактивно
            if prev_status == 'Неактивно':
                new_status_p = 'Неактивно'
            # Хранение
            elif prev_status == 'Хранение':
                new_status_p = 'Хранение'
            # Исправен
            elif prev_status == 'Исправен':
                new_status_p = 'Исправен'
            # Ремонт (repair_days < RepairTime)
            elif prev_status == 'Ремонт' and prev_row['repair_days'] is not None and prev_row['RepairTime'] is not None:
                if prev_row['repair_days'] < prev_row['RepairTime']:
                    new_status_p = 'Ремонт'
                else:
                    new_status_p = 'Исправен'
            # Эксплуатация
            elif prev_status == 'Эксплуатация':
                daily_flight_hours = row['daily_flight_hours']
                
                if prev_row['sne'] < (row['ll'] - daily_flight_hours) and prev_row['ppr'] < (row['oh'] - daily_flight_hours):
                    new_status_p = 'Эксплуатация'
                elif prev_row['sne'] >= (row['ll'] - daily_flight_hours):
                    new_status_p = 'Хранение'
                elif prev_row['ppr'] >= (row['oh'] - daily_flight_hours):
                    if prev_row['sne'] < row['BR']:
                        new_status_p = 'Ремонт'
                    else:
                        new_status_p = 'Хранение'
            
            # Обновляем Status_P в основном DataFrame
            self.gpu_df.loc[idx, 'Status_P'] = new_status_p

    def step_2(self, curr_date):
        """
        Реализация step_2 с использованием CUDA и cuDF
        """
        # Преобразуем дату в строку для работы с cuDF
        curr_date_str = str(curr_date)
        
        # Получаем данные для текущей даты
        curr_data = self.gpu_df[self.gpu_df['Dates'] == curr_date_str]
        
        if len(curr_data) == 0:
            self.logger.warning(f"Нет данных для {curr_date}")
            return
        
        # Вычисляем balance_total с явным приведением типов
        balance_mi8t = curr_data['balance_mi8t'].sum()
        balance_mi17 = curr_data['balance_mi17'].sum()
        balance_empty = curr_data['balance_empty'].sum()
        
        # Явно приводим к float32 для избежания предупреждений
        balance_total = np.float32(balance_mi8t + balance_mi17 + balance_empty)
        
        # Обновляем balance_total для всех записей текущей даты
        self.gpu_df.loc[curr_data.index, 'balance_total'] = balance_total

    def step_3(self, curr_date):
        """
        Реализация step_3 с использованием CUDA и cuDF
        """
        # Преобразуем дату в строку для работы с cuDF
        curr_date_str = str(curr_date)
        
        # Получаем данные для текущей даты
        curr_data = self.gpu_df[self.gpu_df['Dates'] == curr_date_str]
        
        if len(curr_data) == 0:
            self.logger.warning(f"Нет данных для {curr_date}")
            return
        
        # Получаем balance_total
        balance_total = curr_data['balance_total'].iloc[0]
        
        # Обновляем Status из Status_P для всех записей
        # Преобразуем индекс в pandas перед итерацией
        for idx in curr_data.index.to_pandas():
            self.gpu_df.loc[idx, 'Status'] = self.gpu_df.loc[idx, 'Status_P']
        
        # Обрабатываем случай balance_total > 0
        if balance_total > 0:
            abs_balance = int(balance_total)
            
            # Находим записи с Status_P == 'Исправен'
            serviceable_mask = (curr_data['Status_P'] == 'Исправен')
            serviceable_indices = curr_data[serviceable_mask].index.to_pandas().tolist()
            
            # Сортируем индексы для стабильного порядка
            serviceable_indices.sort()
            
            serviceable_count = min(abs_balance, len(serviceable_indices))
            
            if serviceable_count > 0:
                # Изменяем Status на 'Ремонт' для первых serviceable_count записей
                for idx in serviceable_indices[:serviceable_count]:
                    self.gpu_df.loc[idx, 'Status'] = 'Ремонт'
                abs_balance -= serviceable_count
            
            # Если еще остались записи для изменения, ищем 'Эксплуатация'
            if abs_balance > 0:
                exploitation_mask = (curr_data['Status_P'] == 'Эксплуатация')
                exploitation_indices = curr_data[exploitation_mask].index.to_pandas().tolist()
                
                # Сортируем индексы для стабильного порядка
                exploitation_indices.sort()
                
                exploitation_count = min(abs_balance, len(exploitation_indices))
                
                if exploitation_count > 0:
                    # Изменяем Status на 'Ремонт' для первых exploitation_count записей
                    for idx in exploitation_indices[:exploitation_count]:
                        self.gpu_df.loc[idx, 'Status'] = 'Ремонт'
        
        # Обрабатываем случай balance_total < 0
        elif balance_total < 0:
            abs_balance = abs(int(balance_total))
            
            # Находим записи с Status_P == 'Исправен'
            serviceable_mask = (curr_data['Status_P'] == 'Исправен')
            serviceable_indices = curr_data[serviceable_mask].index.to_pandas().tolist()
            
            # Сортируем индексы для стабильного порядка
            serviceable_indices.sort()
            
            serviceable_count = min(abs_balance, len(serviceable_indices))
            
            if serviceable_count > 0:
                # Изменяем Status на 'Эксплуатация' для первых serviceable_count записей
                for idx in serviceable_indices[:serviceable_count]:
                    self.gpu_df.loc[idx, 'Status'] = 'Эксплуатация'
                abs_balance -= serviceable_count
            
            # Если еще остались записи для изменения, ищем 'Неактивно'
            if abs_balance > 0:
                inactive_mask = (curr_data['Status_P'] == 'Неактивно')
                inactive_indices = curr_data[inactive_mask].index.to_pandas().tolist()
                
                # Сортируем индексы для стабильного порядка
                inactive_indices.sort()
                
                inactive_count = min(abs_balance, len(inactive_indices))
                
                if inactive_count > 0:
                    # Изменяем Status на 'Эксплуатация' для первых inactive_count записей
                    for idx in inactive_indices[:inactive_count]:
                        self.gpu_df.loc[idx, 'Status'] = 'Эксплуатация'

    def step_4(self, prev_date, curr_date):
        """
        Реализация step_4 с использованием CUDA и cuDF
        """
        # Преобразуем даты в строки для работы с cuDF
        prev_date_str = str(prev_date)
        curr_date_str = str(curr_date)
        
        # Получаем данные для текущей и предыдущей даты
        curr_data = self.gpu_df[self.gpu_df['Dates'] == curr_date_str]
        prev_data = self.gpu_df[self.gpu_df['Dates'] == prev_date_str]
        
        # Преобразуем в pandas DataFrame для итерации
        prev_data_pd = prev_data.to_pandas()
        curr_data_pd = curr_data.to_pandas()
        
        # Создаем словарь для быстрого доступа к предыдущим данным по serialno
        prev_dict = {}
        for i, row in prev_data_pd.iterrows():
            serialno = row['serialno']
            prev_dict[serialno] = {
                'sne': row['sne'],
                'ppr': row['ppr'],
                'repair_days': row['repair_days'],
                'Status': row['Status'],
                'Status_P': row['Status_P']
            }
        
        # Создаем словарь для быстрого доступа к индексам текущих данных
        curr_indices_dict = {}
        for i, row in curr_data_pd.iterrows():
            serialno = row['serialno']
            # Если serialno уже есть в словаре, это дубликат - логируем предупреждение
            if serialno in curr_indices_dict:
                self.logger.warning(f"Найден дубликат serialno: {serialno} для даты {curr_date}")
            # Получаем индекс для текущего serialno
            serialno_indices = curr_data[curr_data['serialno'] == serialno].index.to_pandas()
            if len(serialno_indices) > 0:
                curr_indices_dict[serialno] = serialno_indices[0]
        
        # Обрабатываем каждую запись текущей даты
        for i, row in curr_data_pd.iterrows():
            serialno = row['serialno']
            status = row['Status_P']
            daily_flight_hours = row['daily_flight_hours']
            
            # Проверяем, есть ли данные для этого serialno в предыдущей дате
            if serialno not in prev_dict:
                continue
            
            # Проверяем, есть ли индекс для этого serialno
            if serialno not in curr_indices_dict:
                self.logger.warning(f"serialno {serialno} отсутствует в словаре индексов")
                continue
            
            prev_row = prev_dict[serialno]
            prev_sne = prev_row['sne']
            prev_ppr = prev_row['ppr']
            prev_repair = prev_row['repair_days']
            prev_status = prev_row['Status']
            prev_status_p = prev_row['Status_P']
            
            # Проверка на None или NaN
            if pd.isna(prev_sne) or prev_sne == '':
                continue
            
            # Получаем индекс из словаря
            curr_idx = curr_indices_dict[serialno]
            
            # Обновляем значения в зависимости от статуса с явным приведением типов
            if status == 'Эксплуатация':
                new_sne = np.float32(round(float(prev_sne + daily_flight_hours), 2))
                new_ppr = np.float32(round(float(prev_ppr + daily_flight_hours), 2))
                self.gpu_df.loc[curr_idx, 'sne'] = new_sne
                self.gpu_df.loc[curr_idx, 'ppr'] = new_ppr
            elif status == 'Исправен':
                if prev_status_p == 'Ремонт':
                    self.gpu_df.loc[curr_idx, 'sne'] = np.float32(round(float(prev_sne), 2))
                    self.gpu_df.loc[curr_idx, 'ppr'] = np.float32(0.0)
                    self.gpu_df.loc[curr_idx, 'repair_days'] = None
                else:
                    self.gpu_df.loc[curr_idx, 'sne'] = np.float32(round(float(prev_sne), 2))
                    self.gpu_df.loc[curr_idx, 'ppr'] = np.float32(round(float(prev_ppr), 2))
                    self.gpu_df.loc[curr_idx, 'repair_days'] = prev_repair
            elif status == 'Ремонт':
                if prev_status == 'Эксплуатация':
                    self.gpu_df.loc[curr_idx, 'sne'] = np.float32(round(float(prev_sne), 2))
                    self.gpu_df.loc[curr_idx, 'ppr'] = np.float32(round(float(prev_ppr), 2))
                    self.gpu_df.loc[curr_idx, 'repair_days'] = np.float32(1.0)
                else:
                    self.gpu_df.loc[curr_idx, 'sne'] = np.float32(round(float(prev_sne), 2))
                    self.gpu_df.loc[curr_idx, 'ppr'] = np.float32(round(float(prev_ppr), 2))
                    if prev_repair is not None:
                        self.gpu_df.loc[curr_idx, 'repair_days'] = np.float32(prev_repair + 1)
            elif status in ['Хранение', 'Неактивно']:
                self.gpu_df.loc[curr_idx, 'sne'] = np.float32(round(float(prev_sne), 2))
                self.gpu_df.loc[curr_idx, 'ppr'] = np.float32(round(float(prev_ppr), 2))
                self.gpu_df.loc[curr_idx, 'repair_days'] = prev_repair

    def ensure_tmp_table_exists(self):
        """Создает временную таблицу без аналитических полей"""
        drop_ddl = f"DROP TABLE IF EXISTS {self.database_name}.tmp_OlapCube_update"
        self.client.execute(drop_ddl)
        self.logger.info("Старая временная таблица tmp_OlapCube_update удалена (если существовала).")

        # Создаем структуру таблицы
        ddl = f"""
        CREATE TABLE {self.database_name}.tmp_OlapCube_update
        (
            trigger_type Float32,
            RepairTime Float32,
            Status_P String,
            repair_days Nullable(Float32),
            sne Decimal(10,2),
            ppr Decimal(10,2),
            Status String,
            location String,
            ac_typ String,
            daily_flight_hours Decimal(10,2),
            daily_flight_hours_f Decimal(10,2),
            BR Float32,
            ll Float32,
            oh Float32,
            threshold Float32,
            Effectivity String,
            serialno String,
            Dates Date,
            mi8t_count Float32,
            mi17_count Float32,
            balance_mi8t Float32,
            balance_mi17 Float32,
            balance_empty Float32,
            balance_total Float32,
            stock_mi8t Float32,
            stock_mi17 Float32,
            stock_empty Float32,
            stock_total Float32
        ) ENGINE = Memory
        """
        self.client.execute(ddl)
        self.logger.info("Новая временная таблица tmp_OlapCube_update создана.")

    def save_all_results(self, period_start, period_end):
        """
        Сохраняет результаты обработки в базу данных с использованием GPU-ускорения
        """
        self.logger.info(f"Начинаем сохранение записей за период {period_start} - {period_end}")
        
        # Преобразуем даты в строки для работы с cuDF
        period_start_str = str(period_start)
        period_end_str = str(period_end)
        first_date_str = str(self.first_date)
        
        # Фильтруем данные для сохранения
        # Для строковых дат нужно использовать другой подход
        cycle_dates_str = [str(d) for d in self.dates_dict.values() 
                          if period_start <= d < period_end]
        cycle_mask = self.gpu_df['Dates'].isin(cycle_dates_str)
        cycle_df = self.gpu_df[cycle_mask]
        
        # Проверяем, есть ли данные для сохранения
        if len(cycle_df) == 0:
            self.logger.warning(f"Нет данных для сохранения за период {period_start} - {period_end}")
            return
        
        # Дополнительно фильтруем, чтобы не обновлять первую дату
        update_mask = ~(cycle_df['Dates'] == first_date_str)
        df_to_update = cycle_df[update_mask]
        
        # Проверяем, остались ли данные после фильтрации
        if len(df_to_update) == 0:
            self.logger.warning(f"Нет данных для обновления после фильтрации первой даты")
            return
        
        # Округляем числовые поля
        df_to_update['sne'] = df_to_update['sne'].astype(np.float64).round(2).astype(np.float32)
        df_to_update['ppr'] = df_to_update['ppr'].astype(np.float64).round(2).astype(np.float32)
        df_to_update['daily_flight_hours'] = df_to_update['daily_flight_hours'].astype(np.float64).round(2).astype(np.float32)
        df_to_update['daily_flight_hours_f'] = df_to_update['daily_flight_hours_f'].astype(np.float64).round(2).astype(np.float32)
        
        # Создаем временную таблицу
        self.ensure_tmp_table_exists()
        
        # Список всех колонок
        all_columns = [
             'trigger_type',
             'RepairTime',
             'Status_P',
             'repair_days',
             'sne',
             'ppr',
             'Status',
             'location',
             'ac_typ',
             'daily_flight_hours',
             'daily_flight_hours_f',
             'BR',
             'll',
             'oh',
             'threshold',
             'Effectivity',
             'serialno',
             'Dates',
             'mi8t_count',
             'mi17_count',
             'balance_mi8t',
             'balance_mi17',
             'balance_empty',
             'balance_total',
             'stock_mi8t',
             'stock_mi17',
             'stock_empty',
             'stock_total'
        ]
        
        # Заполняем NaN нулями для числовых полей
        numeric_fields = [
            'trigger_type', 'RepairTime', 'repair_days', 'sne', 'ppr',
            'daily_flight_hours', 'daily_flight_hours_f', 'BR', 'll', 'oh',
            'threshold', 'mi8t_count', 'mi17_count', 'balance_mi8t', 'balance_mi17',
            'balance_empty', 'balance_total', 'stock_mi8t', 'stock_mi17', 'stock_empty',
            'stock_total'
        ]
        
        for col in numeric_fields:
            if col in df_to_update.columns:
                df_to_update[col] = df_to_update[col].fillna(0.0).astype(np.float32)
        
        # Преобразуем cuDF DataFrame в pandas DataFrame для вставки в ClickHouse
        update_records = df_to_update.to_pandas()
        
        # Преобразуем строковые даты обратно в объекты datetime.date для ClickHouse
        update_records['Dates'] = update_records['Dates'].apply(lambda x: self.dates_dict[x])
        
        update_records = update_records.replace({np.nan: None})
        records = list(update_records.itertuples(index=False, name=None))
        
        # Составляем запрос INSERT
        columns_str = ", ".join(all_columns)
        insert_query = f"""
        INSERT INTO {self.database_name}.tmp_OlapCube_update (
            {columns_str}
        ) VALUES
        """
        self.client.execute(insert_query, records, settings={'max_threads': 8})
        self.logger.info("Данные вставлены во временную таблицу.")
        
        # Обновленный INSERT в основную таблицу
        insert_main_query = f"""
        INSERT INTO {self.database_name}.OlapCube_VNV (
            {columns_str}
        )
        SELECT {columns_str}
        FROM {self.database_name}.tmp_OlapCube_update
        """
        self.client.execute(insert_main_query, settings={'max_threads': 8})
        self.logger.info("Новые версии записей вставлены в основную таблицу.")
        
        optimize_query = f"OPTIMIZE TABLE {self.database_name}.OlapCube_VNV FINAL"
        self.client.execute(optimize_query, settings={'max_threads': 8})
        self.logger.info("Оптимизация таблицы выполнена.")
        
        drop_query = f"DROP TABLE {self.database_name}.tmp_OlapCube_update"
        self.client.execute(drop_query)
        
        records = None
        update_records = None


if __name__ == "__main__":
    try:
        start_time = time.time()
        processor = CycleCUDAProcessor(total_days=7)
        processor.run_cycle()
        total_time = time.time() - start_time
        print(f"Скрипт успешно завершен! Общее время выполнения: {total_time:.2f} секунд")
    except Exception as e:
        print(f"Ошибка при выполнении скрипта: {e}")
        import traceback
        traceback.print_exc()
