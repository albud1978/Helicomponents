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
        
        # Поля для аналитики
        self.analytics_fields = [
            'ops_count','hbs_count','repair_count','total_operable','entry_count','exit_count',
            'into_repair','complete_repair','remain_repair','remain','midlife_repair','midlife','hours'
        ]
        
        # Добавляем колонки для аналитики
        self.ensure_analytics_columns()
        
    def ensure_analytics_columns(self):
        """Добавляем колонки для аналитики, если их нет"""
        try:
            # ALTER TABLE для добавления необходимых полей
            new_columns_ddl = f"""
            ALTER TABLE {self.database_name}.OlapCube_VNV
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
            ADD COLUMN IF NOT EXISTS midlife_repair Float32,
            ADD COLUMN IF NOT EXISTS midlife Float32,
            ADD COLUMN IF NOT EXISTS hours Float32
            """
            self.client.execute(new_columns_ddl)
            self.logger.info("Аналитические поля (если их не было) успешно добавлены в OlapCube_VNV.")
        except Exception as e:
            self.logger.error(f"Ошибка при добавлении аналитических полей: {e}", exc_info=True)

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
        """
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
        try:
            self.df = pd.DataFrame(result, columns=columns)
        except Exception as e:
            self.logger.error(f"Ошибка при создании DataFrame: {e}")
            raise

        # Приводим числовые поля к требуемому типу (trigger_type теперь тоже числовой)
        numeric_cols = [
            'trigger_type',
            'RepairTime', 'repair_days', 'sne', 'ppr', 'daily_flight_hours',
            'daily_flight_hours_f', 'BR', 'll', 'oh', 'threshold',
            'mi8t_count', 'mi17_count', 'balance_mi8t', 'balance_mi17', 'balance_empty',
            'balance_total', 'stock_mi8t', 'stock_mi17', 'stock_empty', 'stock_total'
        ]
        for col in numeric_cols:
            self.df[col] = self.df[col].astype(np.float32, errors='ignore')

        # Приводим оставшиеся строковые поля к типу str
        for col in ['Status_P', 'Status', 'location', 'ac_typ', 'Effectivity']:
            self.df[col] = self.df[col].fillna('').astype(str)

        # Приводим столбец Dates к типу date
        self.df['Dates'] = pd.to_datetime(self.df['Dates']).dt.date

        # Округляем daily_flight_hours и daily_flight_hours_f до двух знаков
        self.df['daily_flight_hours'] = self.df['daily_flight_hours'].astype(np.float64).round(2).astype(np.float32)
        self.df['daily_flight_hours_f'] = self.df['daily_flight_hours_f'].astype(np.float64).round(2).astype(np.float32)

        self.logger.info(f"Данные загружены: всего {len(self.df)} записей.")

    def run_cycle(self):
        self.load_all_data()

        period_start = self.first_date
        period_end = period_start + timedelta(days=self.total_days)
        self.logger.info(f"Обработка производится для периода {period_start} - {period_end}")

        self.logger.info("Начинаем обработку дат...")
        self.process_all_dates(period_start, period_end)
        
        # Добавляем расчет аналитических метрик
        self.calculate_analytics_metrics()
        
        # Удаляем выгрузки в Excel
        # Сохраняем разбивку для отладки если нужно - удалено
        
        self.save_all_results(period_start, period_end)
        self.logger.info("Обработка завершена. Результаты записаны в базу.")

        # Вместо проверки конкретного дня, выведем статистику за весь обработанный период
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
        unique_dates = np.sort(self.df['Dates'].unique())
        cycle_dates = [d for d in unique_dates if period_start <= d < period_end]
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

        for curr_date in cycle_dates[1:]:
            prev_date = cycle_dates[cycle_dates.index(curr_date) - 1]

            self.step_1(prev_date, curr_date)
            self.step_2(curr_date)
            self.step_3(curr_date)
            self.step_4(prev_date, curr_date)

            # Логирование только в конце месяца или при последнем дне обработки
            month_key = curr_date.strftime('%Y-%m')
            is_last_day_of_month = curr_date == month_groups[month_key][-1]
            is_last_day_overall = curr_date == cycle_dates[-1]
            
            if is_last_day_of_month or is_last_day_overall:
                # Собираем статистику за месяц
                month_data = self.df[self.df['Dates'] == curr_date]
                status_counts = month_data['Status'].value_counts()
                status_p_counts = month_data['Status_P'].value_counts()

                self.logger.info(f"Статистика на конец {month_key} (дата {curr_date}) - Status:")
                for st, cnt in status_counts.items():
                    self.logger.info(f"  {st}: {cnt}")
                self.logger.info(f"Статистика на конец {month_key} (дата {curr_date}) - Status_P:")
                for st, cnt in status_p_counts.items():
                    self.logger.info(f"  {st}: {cnt}")

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
        
        # Новое условие для пустого значения ac_typ: учитываем NaN, пустые строки и, при необходимости, строку 'None'
        empty_ac_typ = (curr_data['ac_typ'].isna() | (curr_data['ac_typ'].str.strip() == '') 
                        | (curr_data['ac_typ'].str.strip().str.lower() == 'none'))
        balance_empty = len(curr_data[(curr_data['Status_P'] == 'Эксплуатация') & empty_ac_typ])
        stock_empty = len(curr_data[(curr_data['Status_P'] == 'Исправен') & empty_ac_typ])

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
        """Создает временную таблицу с учетом аналитических полей"""
        drop_ddl = f"DROP TABLE IF EXISTS {self.database_name}.tmp_OlapCube_update"
        self.client.execute(drop_ddl)
        self.logger.info("Старая временная таблица tmp_OlapCube_update удалена (если существовала).")

        # Добавляем аналитические поля в структуру таблицы
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
            stock_total Float32,
            
            /* Аналитические поля */
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
        ) ENGINE = Memory
        """
        self.client.execute(ddl)
        self.logger.info("Новая временная таблица tmp_OlapCube_update создана (с аналитическими полями).")

    def save_all_results(self, period_start, period_end):
        self.logger.info(f"Начинаем сохранение записей за период {period_start} - {period_end}")
        cycle_df = self.df[(self.df['Dates'] >= period_start) & (self.df['Dates'] < period_end)]
        df_to_update = cycle_df[cycle_df['Dates'] > self.first_date].copy()

        # Округляем sne и ppr, как и раньше
        df_to_update.loc[:, 'sne'] = df_to_update['sne'].astype(np.float64).round(2).astype(np.float32)
        df_to_update.loc[:, 'ppr'] = df_to_update['ppr'].astype(np.float64).round(2).astype(np.float32)
        # Аналогично для daily_flight_hours и daily_flight_hours_f
        df_to_update.loc[:, 'daily_flight_hours'] = df_to_update['daily_flight_hours'].astype(np.float64).round(2).astype(np.float32)
        df_to_update.loc[:, 'daily_flight_hours_f'] = df_to_update['daily_flight_hours_f'].astype(np.float64).round(2).astype(np.float32)
        
        self.ensure_tmp_table_exists()
        
        # Добавляем аналитические поля в колонки для временной таблицы
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
             'stock_total',
             # Добавляем аналитические поля
             'ops_count',
             'hbs_count',
             'repair_count',
             'total_operable',
             'entry_count', 
             'exit_count',
             'into_repair',
             'complete_repair',
             'remain_repair',
             'remain',
             'midlife_repair',
             'midlife',
             'hours'
        ]
      
        update_records = df_to_update[all_columns]
        
        # Заполняем NaN нулями для числовых полей
        numeric_fields = [
            'trigger_type', 'RepairTime', 'repair_days', 'sne', 'ppr',
            'daily_flight_hours', 'daily_flight_hours_f', 'BR', 'll', 'oh',
            'threshold', 'mi8t_count', 'mi17_count', 'balance_mi8t', 'balance_mi17',
            'balance_empty', 'balance_total', 'stock_mi8t', 'stock_mi17', 'stock_empty',
            'stock_total'
        ] + self.analytics_fields
        
        for col in numeric_fields:
            if col in update_records.columns:
                update_records[col] = update_records[col].fillna(0.0).astype(np.float32)
        
        update_records = update_records.replace({np.nan: None})
        records = list(update_records.itertuples(index=False, name=None))

        # Составляем запрос INSERT с учетом аналитических полей
        columns_str = ", ".join(all_columns)
        insert_query = f"""
        INSERT INTO {self.database_name}.tmp_OlapCube_update (
            {columns_str}
        ) VALUES
        """
        self.client.execute(insert_query, records, settings={'max_threads': 8})
        self.logger.info("Данные вставлены во временную таблицу.")

        count_query = f"SELECT count(*) FROM {self.database_name}.tmp_OlapCube_update"
        tmp_count = self.client.execute(count_query)[0][0]
        self.logger.info(f"Количество записей во временной таблице: {tmp_count}")

        # Обновленный INSERT в основную таблицу с учетом аналитических полей
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
        self.logger.info("Оптимизация таблицы выполнена (FINAL).")

        drop_query = f"DROP TABLE {self.database_name}.tmp_OlapCube_update"
        self.client.execute(drop_query)
        self.logger.info("Временная таблица удалена после обновления.")

        records = None
        update_records = None
        
        # Проверяем результаты, включая аналитические поля
        check_query = f"""
        SELECT serialno, Dates, Status, Status_P, sne, ppr, repair_days, 
               ops_count, hbs_count, repair_count, hours
        FROM {self.database_name}.OlapCube_VNV
        WHERE Dates = '2024-11-26'
        LIMIT 10
        """
        check_result = self.client.execute(check_query)
        self.logger.info("Пример данных из основной таблицы на 2024-11-26 после обновления (включая аналитику):")
        for row in check_result:
            self.logger.info(row)

    def calculate_analytics_metrics(self):
        """Вычисляет значения аналитических метрик только для заданного периода"""
        if self.df is None or len(self.df) == 0:
            self.logger.warning("Нет данных для расчета аналитических метрик")
            return
        
        # Определяем период для расчета аналитических метрик
        period_start = self.first_date
        period_end = period_start + timedelta(days=self.total_days)
        
        # Отфильтровываем только даты из указанного периода
        period_dates = sorted([d for d in self.df['Dates'].unique() if period_start <= d < period_end])
        
        if len(period_dates) < 2:
            self.logger.warning("Недостаточно дат в указанном периоде для вычисления аналитических метрик")
            return
                
        self.logger.info(f"Начинаем расчет аналитических метрик для периода {period_start} - {period_end}...")
        
        # Создаем отдельный DataFrame для хранения аналитических значений по датам
        analytics_by_date = pd.DataFrame(index=period_dates[1:])
        for field in self.analytics_fields:
            analytics_by_date[field] = np.float32(0.0)
        
        for i in range(1, len(period_dates)):
            d_curr = period_dates[i]
            d_prev = period_dates[i-1]
            
            df_curr = self.df[self.df['Dates'] == d_curr]
            df_prev = self.df[self.df['Dates'] == d_prev]
            
            # Расчет базовых счетчиков
            ops_count_val = len(df_curr[df_curr['Status'] == 'Эксплуатация'])
            hbs_count_val = len(df_curr[df_curr['Status'] == 'Исправен'])
            repair_count_val = len(df_curr[df_curr['Status'] == 'Ремонт'])
            total_operable_val = ops_count_val + hbs_count_val + repair_count_val
            
            # Расчет на основе сравнения с предыдущим днем
            merged = df_prev[['serialno', 'Status', 'Status_P']].merge(
                df_curr[['serialno', 'Status', 'Status_P']],
                on='serialno',
                suffixes=('_prev', '_curr')
            )
            
            # Переходы между статусами
            entry_count_val = len(merged[
                (merged['Status_prev'] == 'Неактивно') &
                (merged['Status_curr'] == 'Эксплуатация')
            ])
            
            exit_count_val = len(merged[
                (merged['Status_prev'] == 'Эксплуатация') &
                (merged['Status_curr'] == 'Хранение')
            ])
            
            into_repair_val = len(merged[
                (merged['Status_prev'] == 'Эксплуатация') &
                (merged['Status_curr'] == 'Ремонт')
            ])
            
            complete_repair_val = len(merged[
                (merged['Status_prev'] == 'Ремонт') &
                (merged['Status_curr'] != 'Ремонт')
            ])
            
            # Расчет оставшегося ресурса
            operating_tech = df_curr[df_curr['Status'].isin(['Эксплуатация', 'Исправен'])]
            remain_repair_val = (operating_tech['oh'] - operating_tech['ppr']).fillna(0).sum() if len(operating_tech) > 0 else 0
            
            serviceable_tech = df_curr[df_curr['Status'].isin(['Эксплуатация', 'Исправен', 'Ремонт'])]
            remain_val = (serviceable_tech['ll'] - serviceable_tech['sne']).fillna(0).sum() if len(serviceable_tech) > 0 else 0
            
            # Относительные показатели
            total_ll = serviceable_tech['ll'].fillna(0).sum() if len(serviceable_tech) > 0 else 0
            midlife_val = remain_val / total_ll if total_ll > 0 else 0
            
            total_oh = operating_tech['oh'].fillna(0).sum() if len(operating_tech) > 0 else 0
            midlife_repair_val = remain_repair_val / total_oh if total_oh > 0 else 0
            
            # Общий налет часов
            hours_val = df_curr[df_curr['Status'] == 'Эксплуатация']['daily_flight_hours'].sum()
            
            # Сохраняем значения в промежуточном DataFrame
            analytics_by_date.loc[d_curr, 'ops_count'] = ops_count_val
            analytics_by_date.loc[d_curr, 'hbs_count'] = hbs_count_val
            analytics_by_date.loc[d_curr, 'repair_count'] = repair_count_val
            analytics_by_date.loc[d_curr, 'total_operable'] = total_operable_val
            analytics_by_date.loc[d_curr, 'entry_count'] = entry_count_val
            analytics_by_date.loc[d_curr, 'exit_count'] = exit_count_val
            analytics_by_date.loc[d_curr, 'into_repair'] = into_repair_val
            analytics_by_date.loc[d_curr, 'complete_repair'] = complete_repair_val
            analytics_by_date.loc[d_curr, 'remain_repair'] = remain_repair_val
            analytics_by_date.loc[d_curr, 'remain'] = remain_val
            analytics_by_date.loc[d_curr, 'midlife_repair'] = midlife_repair_val
            analytics_by_date.loc[d_curr, 'midlife'] = midlife_val
            analytics_by_date.loc[d_curr, 'hours'] = hours_val
            
            # Логирование прогресса каждые 100 дней или каждые 10% от общего количества
            log_interval = max(100, int(len(period_dates) * 0.1))
            if i % log_interval == 0 or i == len(period_dates) - 1:
                self.logger.info(f"Обработано {i} дат из {len(period_dates)-1} ({i/(len(period_dates)-1)*100:.1f}%)")
        
        self.logger.info("Копирование аналитических метрик во все строки...")
        
        # Оптимизированная копия значений в основной DataFrame
        # Создаем словарь {дата: {поле: значение}} для быстрого доступа
        analytics_dict = {date: {} for date in period_dates[1:]}
        for date in period_dates[1:]:
            for field in self.analytics_fields:
                analytics_dict[date][field] = analytics_by_date.loc[date, field]
        
        # Применяем значения ко всем записям с соответствующей датой
        for date in period_dates[1:]:
            date_mask = (self.df['Dates'] == date)
            if not date_mask.any():
                continue
                
            for field in self.analytics_fields:
                self.df.loc[date_mask, field] = analytics_dict[date][field]
        
        self.logger.info("Расчет аналитических метрик завершен")
    
if __name__ == "__main__":
    processor = CycleProcessor(total_days=4000)
    processor.run_cycle()
