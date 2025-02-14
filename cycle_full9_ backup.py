import os
import logging
import pandas as pd
import numpy as np
from clickhouse_driver import Client
from dotenv import load_dotenv
from datetime import datetime, timedelta
import sys

# Загрузка переменных окружения
load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # DEBUG для детальной отладки
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
if not logger.handlers:
    logger.addHandler(handler)


class CycleProcessor:
    def __init__(self, total_days=2):  # Тестовый период – 2 дня
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
        self.df = None

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
        Для теста период 2 дня.
        """
        first_date = self.get_first_date()
        self.first_date = first_date
        period_end = first_date + timedelta(days=self.total_days)
        self.logger.info(f"Загружаются данные с {first_date} до {period_end}")

        query = f"""
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
            mi8t_count,
            mi17_count
        FROM {self.database_name}.OlapCube_VNV
        WHERE Dates >= '{first_date.strftime('%Y-%m-%d')}'
          AND Dates < '{period_end.strftime('%Y-%m-%d')}'
        ORDER BY Dates, serialno
        """
        result = self.client.execute(query, settings={'max_threads': 8})
        if not result:
            raise Exception("Нет данных для заданного периода")
        columns = [
            'serialno', 'Dates', 'Status', 'Status_P', 'sne', 'ppr', 'repair_days',
            'll', 'oh', 'BR', 'daily_flight_hours', 'RepairTime', 'ac_typ',
            'mi8t_count', 'mi17_count'
        ]
        try:
            self.df = pd.DataFrame(result, columns=columns)
        except Exception as e:
            self.logger.error(f"Ошибка при создании DataFrame: {e}")
            raise

        numeric_cols = ['sne','ppr','repair_days','ll','oh','BR','daily_flight_hours','RepairTime','mi8t_count','mi17_count']
        for col in numeric_cols:
            self.df[col] = self.df[col].astype(np.float32, errors='ignore')

        self.df['Status'] = self.df['Status'].astype(str)
        self.df['Status_P'] = self.df['Status_P'].astype(str)
        self.df['ac_typ'] = self.df['ac_typ'].astype(str)

        # Приводим столбец Dates к типу date
        self.df['Dates'] = pd.to_datetime(self.df['Dates']).dt.date

        self.logger.info(f"Данные загружены: всего {len(self.df)} записей.")

    def run_cycle(self):
        self.load_all_data()

        period_start = self.first_date
        period_end = period_start + timedelta(days=self.total_days)
        self.logger.info(f"Обработка производится для периода {period_start} - {period_end}")

        self.logger.info("Начинаем обработку дат...")
        self.process_all_dates(period_start, period_end)

        self.logger.info("Выгружаем промежуточный результат в Excel для отладки.")
        df_cycle = self.df[(self.df['Dates'] >= period_start) & (self.df['Dates'] < period_end)]
        df_cycle.to_excel("debug_df_before_save.xlsx", index=False, engine="openpyxl")

        self.save_all_results(period_start, period_end)
        self.logger.info("Обработка завершена. Результаты записаны в базу.")

        check_date = datetime(2024, 11, 26).date()
        check_query = f"""
        SELECT serialno, Dates, Status, Status_P, sne, ppr, repair_days
        FROM {self.database_name}.OlapCube_VNV
        WHERE Dates = '{check_date}'
        LIMIT 10
        """
        after_result = self.client.execute(check_query)
        self.logger.info("Пример данных на 2024-11-26 после обновления:")
        for row in after_result:
            self.logger.info(row)

    def process_all_dates(self, period_start, period_end):
        unique_dates = np.sort(self.df['Dates'].unique())
        cycle_dates = [d for d in unique_dates if period_start <= d < period_end]
        self.logger.info(f"Даты для обработки: {cycle_dates}")

        for curr_date in cycle_dates[1:]:
            prev_date = cycle_dates[cycle_dates.index(curr_date) - 1]

            self.step_1(prev_date, curr_date)
            self.step_2(curr_date)
            self.step_3(curr_date)
            self.step_4(prev_date, curr_date)

            curr_data = self.df[self.df['Dates'] == curr_date]
            status_counts = curr_data['Status'].value_counts()
            status_p_counts = curr_data['Status_P'].value_counts()

            self.logger.info(f"Статистика на {curr_date} - Status:")
            for st, cnt in status_counts.items():
                self.logger.info(f"  {st}: {cnt}")
            self.logger.info(f"Статистика на {curr_date} - Status_P:")
            for st, cnt in status_p_counts.items():
                self.logger.info(f"  {st}: {cnt}")

        if cycle_dates:
            sample_date = cycle_dates[0]
            subset = self.df[self.df['Dates'] == sample_date].head(10)
            self.logger.info(f"Пример данных на {sample_date} (не обновляется, только расчёты):")
            self.logger.info(subset)

    def step_1(self, prev_date, curr_date):
        # Логика обработки для step_1 (без изменений)
        prev_data = self.df[self.df['Dates'] == prev_date]
        curr_data = self.df[self.df['Dates'] == curr_date]
        if len(curr_data) == 0 or len(prev_data) == 0:
            self.logger.warning(f"Нет данных для {curr_date} или {prev_date}")
            return

        working_df = pd.merge(
            curr_data,
            prev_data[['serialno', 'Status', 'Status_P', 'sne', 'ppr', 'repair_days']],
            on='serialno', how='inner', suffixes=('', '_prev')
        )

        working_df['Status_P'] = None
        working_df.loc[working_df['Status_prev'] == 'Неактивно', 'Status_P'] = 'Неактивно'
        working_df.loc[working_df['Status_prev'] == 'Хранение', 'Status_P'] = 'Хранение'
        working_df.loc[working_df['Status_prev'] == 'Исправен', 'Status_P'] = 'Исправен'
        working_df.loc[
            (working_df['Status_prev'] == 'Ремонт') &
            (working_df['repair_days_prev'] < working_df['RepairTime']),
            'Status_P'
        ] = 'Ремонт'
        working_df.loc[
            (working_df['Status_prev'] == 'Ремонт') &
            (working_df['repair_days_prev'] >= working_df['RepairTime']),
            'Status_P'
        ] = 'Исправен'

        explo_mask = (working_df['Status_prev'] == 'Эксплуатация')
        sne_check = working_df['sne_prev'] < (working_df['ll'] - working_df['daily_flight_hours'])
        ppr_check = working_df['ppr_prev'] < (working_df['oh'] - working_df['daily_flight_hours'])
        working_df.loc[explo_mask & sne_check & ppr_check, 'Status_P'] = 'Эксплуатация'
        sne_limit = working_df['sne_prev'] >= (working_df['ll'] - working_df['daily_flight_hours'])
        working_df.loc[explo_mask & sne_limit, 'Status_P'] = 'Хранение'

        def exploitation_to_repair_or_storage(row):
            if (row['Status_prev'] == 'Эксплуатация') and (row['ppr_prev'] >= (row['oh'] - row['daily_flight_hours'])):
                return 'Ремонт' if row['sne_prev'] < row['BR'] else 'Хранение'
            return row['Status_P']

        working_df['Status_P'] = working_df.apply(exploitation_to_repair_or_storage, axis=1)

        curr_mask = (self.df['Dates'] == curr_date)
        curr_indices = self.df[curr_mask].index
        working_df = working_df.reset_index(drop=True)
        if len(curr_indices) != len(working_df):
            self.logger.error(f"Количество строк не совпадает для {curr_date}: {len(curr_indices)} != {len(working_df)}")
            return
        self.df.loc[curr_indices, 'Status_P'] = working_df['Status_P'].values

    def step_2(self, curr_date):
        curr_data = self.df[self.df['Dates'] == curr_date]
        mi8t_count = curr_data['mi8t_count'].iloc[0] if not pd.isna(curr_data['mi8t_count'].iloc[0]) else 0
        mi17_count = curr_data['mi17_count'].iloc[0] if not pd.isna(curr_data['mi17_count'].iloc[0]) else 0

        balance_mi8t = len(curr_data[(curr_data['Status_P'] == 'Эксплуатация') & (curr_data['ac_typ'] == 'Ми-8Т')])
        stock_mi8t = len(curr_data[(curr_data['Status_P'] == 'Исправен') & (curr_data['ac_typ'] == 'Ми-8Т')])
        balance_mi17 = len(curr_data[(curr_data['Status_P'] == 'Эксплуатация') & (curr_data['ac_typ'] == 'Ми-17')])
        stock_mi17 = len(curr_data[(curr_data['Status_P'] == 'Исправен') & (curr_data['ac_typ'] == 'Ми-17')])
        balance_empty = len(curr_data[(curr_data['Status_P'] == 'Эксплуатация') & (curr_data['ac_typ'].isna())])
        stock_empty = len(curr_data[(curr_data['Status_P'] == 'Исправен') & (curr_data['ac_typ'].isna())])

        final_balance_mi8t = balance_mi8t - mi8t_count
        final_balance_mi17 = balance_mi17 - mi17_count
        final_balance_total = final_balance_mi8t + final_balance_mi17 + balance_empty

        mask = (self.df['Dates'] == curr_date)
        self.df.loc[mask, 'balance_mi8t'] = final_balance_mi8t
        self.df.loc[mask, 'balance_mi17'] = final_balance_mi17
        self.df.loc[mask, 'balance_empty'] = balance_empty
        self.df.loc[mask, 'balance_total'] = final_balance_total
        self.df.loc[mask, 'stock_mi8t'] = balance_mi8t
        self.df.loc[mask, 'stock_mi17'] = stock_mi17
        self.df.loc[mask, 'stock_empty'] = stock_empty
        self.df.loc[mask, 'stock_total'] = balance_mi8t + stock_mi17 + stock_empty

    def step_3(self, curr_date):
        curr_data = self.df[self.df['Dates'] == curr_date]
        balance_total = curr_data['balance_total'].iloc[0]
        mask = (self.df['Dates'] == curr_date)
        self.df['Status'] = self.df['Status'].astype(str)
        self.df['Status_P'] = self.df['Status_P'].astype(str)
        self.df.loc[mask, 'Status'] = self.df.loc[mask, 'Status_P']
        if balance_total > 0:
            exploitation = curr_data[curr_data['Status_P'] == 'Эксплуатация'].index.tolist()
            change_count = min(int(balance_total), len(exploitation))
            if change_count > 0:
                self.df.loc[exploitation[:change_count], 'Status'] = 'Исправен'
        elif balance_total < 0:
            abs_balance = abs(int(balance_total))
            serviceable_only = curr_data[curr_data['Status_P'] == 'Исправен'].index.tolist()
            serviceable_count = min(abs_balance, len(serviceable_only))
            if serviceable_count > 0:
                self.df.loc[serviceable_only[:serviceable_count], 'Status'] = 'Эксплуатация'
                abs_balance -= serviceable_count
            if abs_balance > 0:
                inactive = curr_data[curr_data['Status_P'] == 'Неактивно'].index.tolist()
                inactive_count = min(abs_balance, len(inactive))
                if inactive_count > 0:
                    self.df.loc[inactive[:inactive_count], 'Status'] = 'Эксплуатация'
                    abs_balance -= inactive_count

    def step_4(self, prev_date, curr_date):
        curr_data = self.df[self.df['Dates'] == curr_date]
        prev_data = self.df[self.df['Dates'] == prev_date]

        for _, row in curr_data.iterrows():
            idx = row.name
            serialno = row['serialno']
            status = row['Status_P']
            daily_flight_hours = row['daily_flight_hours']

            prev_row = prev_data[prev_data['serialno'] == serialno]
            if len(prev_row) == 0:
                continue

            prev_sne = prev_row['sne'].iloc[0]
            prev_ppr = prev_row['ppr'].iloc[0]
            prev_repair = prev_row['repair_days'].iloc[0]
            prev_status = prev_row['Status'].iloc[0]
            prev_status_p = prev_row['Status_P'].iloc[0]

            if pd.isna(prev_sne):
                continue

            try:
                if status == 'Эксплуатация':
                    self.df.loc[idx, 'sne'] = np.float32(round(float(prev_sne + daily_flight_hours), 2))
                    self.df.loc[idx, 'ppr'] = np.float32(round(float(prev_ppr + daily_flight_hours), 2))
                elif status == 'Исправен':
                    if prev_status_p == 'Ремонт':
                        self.df.loc[idx, 'sne'] = np.float32(round(float(prev_sne), 2))
                        self.df.loc[idx, 'ppr'] = np.float32(0.0)
                        self.df.loc[idx, 'repair_days'] = None
                    else:
                        self.df.loc[idx, 'sne'] = np.float32(round(float(prev_sne), 2))
                        self.df.loc[idx, 'ppr'] = np.float32(round(float(prev_ppr), 2))
                        self.df.loc[idx, 'repair_days'] = prev_repair
                elif status == 'Ремонт':
                    if prev_status == 'Эксплуатация':
                        self.df.loc[idx, 'sne'] = np.float32(round(float(prev_sne), 2))
                        self.df.loc[idx, 'ppr'] = np.float32(round(float(prev_ppr), 2))
                        self.df.loc[idx, 'repair_days'] = 1
                    else:
                        self.df.loc[idx, 'sne'] = np.float32(round(float(prev_sne), 2))
                        self.df.loc[idx, 'ppr'] = np.float32(round(float(prev_ppr), 2))
                        if prev_repair is not None:
                            self.df.loc[idx, 'repair_days'] = prev_repair + 1
                elif status in ['Хранение', 'Неактивно']:
                    self.df.loc[idx, 'sne'] = np.float32(round(float(prev_sne), 2))
                    self.df.loc[idx, 'ppr'] = np.float32(round(float(prev_ppr), 2))
                    self.df.loc[idx, 'repair_days'] = prev_repair
            except Exception as e:
                self.logger.error(f"Ошибка при обновлении для serialno={serialno}: {e}")
                continue

    def ensure_tmp_table_exists(self):
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.database_name}.tmp_OlapCube_update
        (
            serialno String,
            Dates Date,
            Status String,
            Status_P String,
            sne Decimal(10,2),
            ppr Decimal(10,2),
            repair_days Nullable(Float32),
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
        self.logger.info("Временная таблица tmp_OlapCube_update создана (если отсутствовала).")
    
    def save_all_results(self, period_start, period_end):
        self.logger.info(f"Начинаем сохранение записей за период {period_start} - {period_end}")
        cycle_df = self.df[(self.df['Dates'] >= period_start) & (self.df['Dates'] < period_end)]
        # Обновляем данные только для дат, отличных от начальной (если это требуется)
        df_to_update = cycle_df[cycle_df['Dates'] > self.first_date]

        # Округляем поля sne и ppr до 2 знаков после запятой
        df_to_update['sne'] = df_to_update['sne'].astype(float).round(2)
        df_to_update['ppr'] = df_to_update['ppr'].astype(float).round(2)

        # Убедимся, что временная таблица существует
        self.ensure_tmp_table_exists()

        update_records = df_to_update[[ 
             'serialno', 'Dates', 'Status', 'Status_P', 'sne', 'ppr', 'repair_days',
             'mi8t_count', 'mi17_count', 'balance_mi8t', 'balance_mi17', 'balance_empty',
             'balance_total', 'stock_mi8t', 'stock_mi17', 'stock_empty', 'stock_total'
        ]].copy()
        update_records = update_records.where(pd.notnull(update_records), None)
        records = list(update_records.itertuples(index=False, name=None))

        insert_query = f"""
        INSERT INTO {self.database_name}.tmp_OlapCube_update (
             serialno, Dates, Status, Status_P, sne, ppr, repair_days,
             mi8t_count, mi17_count, balance_mi8t, balance_mi17, balance_empty,
             balance_total, stock_mi8t, stock_mi17, stock_empty, stock_total
        ) VALUES
        """
        self.client.execute(insert_query, records, settings={'max_threads': 8})
        self.logger.info("Данные вставлены во временную таблицу.")

        count_query = f"SELECT count(*) FROM {self.database_name}.tmp_OlapCube_update"
        tmp_count = self.client.execute(count_query)[0][0]
        self.logger.info(f"Количество записей во временной таблице: {tmp_count}")

        insert_main_query = f"""
        INSERT INTO {self.database_name}.OlapCube_VNV (
             serialno, Dates, Status, Status_P, sne, ppr, repair_days,
             mi8t_count, mi17_count, balance_mi8t, balance_mi17, balance_empty,
             balance_total, stock_mi8t, stock_mi17, stock_empty, stock_total
        )
        SELECT
             serialno, Dates, Status, Status_P, sne, ppr, repair_days,
             mi8t_count, mi17_count, balance_mi8t, balance_mi17, balance_empty,
             balance_total, stock_mi8t, stock_mi17, stock_empty, stock_total
        FROM {self.database_name}.tmp_OlapCube_update
        """
        self.client.execute(insert_main_query, settings={'max_threads': 8})
        self.logger.info("Новые версии записей вставлены в основную таблицу.")

        optimize_query = f"OPTIMIZE TABLE {self.database_name}.OlapCube_VNV FINAL"
        self.client.execute(optimize_query, settings={'max_threads': 8})
        self.logger.info("Оптимизация таблицы выполнена (FINAL).")

        # Удаляем временную таблицу после использования
        drop_query = f"DROP TABLE {self.database_name}.tmp_OlapCube_update"
        self.client.execute(drop_query, settings={'max_threads': 8})
        self.logger.info("Временная таблица удалена после обновления.")

        # Очистка временных переменных
        records = None
        update_records = None

        main_sample_query = f"""
        SELECT serialno, Dates, Status, Status_P, sne, ppr, repair_days
        FROM {self.database_name}.OlapCube_VNV
        WHERE Dates = '2024-11-26'
        LIMIT 10
        """
        main_sample_result = self.client.execute(main_sample_query)
        self.logger.info("Пример данных из основной таблицы на 26-11 после обновления:")
        for row in main_sample_result:
            self.logger.info(row)
    
if __name__ == "__main__":
    processor = CycleProcessor(total_days=7)
    processor.run_cycle()
