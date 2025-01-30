import os
import logging
import pandas as pd
import numpy as np
from clickhouse_driver import Client
from dotenv import load_dotenv
from datetime import datetime, timedelta
import sys

# Загрузка переменных окружений
load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
if not logger.handlers:
    logger.addHandler(handler)

class CycleProcessor:
    def __init__(self, total_days=100):
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
        """Загружаем данные за total_days дней"""
        first_date = self.get_first_date()
        start_date = first_date
        end_date = first_date + timedelta(days=self.total_days)

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
        WHERE Dates >= '{start_date.strftime('%Y-%m-%d')}'
          AND Dates < '{end_date.strftime('%Y-%m-%d')}'
        ORDER BY Dates, serialno
        """

        result = self.client.execute(query, settings={'max_threads':8})
        if not result:
            raise Exception(f"Нет данных для диапазона {start_date} - {end_date}")

        columns = [
            'serialno', 'Dates', 'Status', 'Status_P', 'sne', 'ppr', 'repair_days',
            'll', 'oh', 'BR', 'daily_flight_hours', 'RepairTime', 'ac_typ',
            'mi8t_count', 'mi17_count'
        ]

        self.df = pd.DataFrame(result, columns=columns)

        numeric_cols = ['sne','ppr','repair_days','ll','oh','BR','daily_flight_hours','RepairTime','mi8t_count','mi17_count']
        for col in numeric_cols:
            self.df[col] = self.df[col].astype(np.float32, errors='ignore')

        self.df['Status'] = self.df['Status'].astype('category')
        self.df['Status_P'] = self.df['Status_P'].astype('category')
        self.df['ac_typ'] = self.df['ac_typ'].astype('category')

        self.logger.warning(f"Данные загружены: всего {len(self.df)} записей.")

    def run_cycle(self):
        self.load_all_data()
        self.logger.warning("Начинаем обработку дат...")
        self.process_all_dates()

        # Добавляем отладочную выгрузку в Excel перед сохранением результатов
        self.logger.warning("Выгружаем промежуточный результат в Excel для отладки.")
        self.df.to_excel("debug_df_before_save.xlsx", index=False, engine="openpyxl")

        self.save_all_results()
        self.logger.warning("Обработка завершена. Результаты записаны в базу.")

        # Дополнительная проверка после записи в БД
        check_date = datetime(2024,11,26).date()
        check_query = f"""
        SELECT serialno, Dates, Status, Status_P, sne, ppr, repair_days
        FROM {self.database_name}.OlapCube_VNV
        WHERE Dates = '{check_date}'
        LIMIT 10
        """
        after_result = self.client.execute(check_query)
        self.logger.warning("Пример данных на 2024-11-26 после записи в базу:")
        for row in after_result:
            self.logger.warning(row)

    def process_all_dates(self):
        self.df.sort_values(by='Dates', inplace=True)
        unique_dates = self.df['Dates'].unique()
        unique_dates = np.sort(unique_dates)

        for i in range(1, len(unique_dates)):
            prev_date = unique_dates[i-1]
            curr_date = unique_dates[i]

            self.step_1(prev_date, curr_date)
            self.step_2(curr_date)
            self.step_3(curr_date)
            self.step_4(prev_date, curr_date)

            # Логирование раз в месяц
            curr_dt = pd.to_datetime(curr_date)
            if curr_dt.day == 1:
                prev_month_end = curr_dt - timedelta(days=1)
                month_data = self.df[self.df['Dates'] == prev_month_end.date()]
                status_counts = month_data['Status'].value_counts()
                status_p_counts = month_data['Status_P'].value_counts()

                self.logger.warning(f"Статистика на последний день предыдущего месяца {prev_month_end.strftime('%Y-%m-%d')} - Status:")
                for st, cnt in status_counts.items():
                    self.logger.warning(f"  {st}: {cnt}")
                self.logger.warning(f"Статистика на последний день предыдущего месяца {prev_month_end.strftime('%Y-%m-%d')} - Status_P:")
                for st, cnt in status_p_counts.items():
                    self.logger.warning(f"  {st}: {cnt}")

        # Дополнительная проверка перед записью в базу: посмотрим часть данных на 2024-11-26
        check_date = datetime(2024,11,26).date()
        subset = self.df[self.df['Dates']==check_date].head(10)
        self.logger.warning("Пример данных на 2024-11-26 перед записью в базу:")
        self.logger.warning(subset)

    def step_1(self, prev_date, curr_date):
        prev_data = self.df[self.df['Dates'] == prev_date]
        curr_data = self.df[self.df['Dates'] == curr_date]

        # Проверяем наличие данных
        if len(curr_data) == 0:
            self.logger.warning(f"Нет данных для даты {curr_date}")
            return
        
        if len(prev_data) == 0:
            self.logger.warning(f"Нет данных для даты {prev_date}")
            return

        # Создаем рабочую копию с объединением данных
        working_df = pd.merge(
            curr_data,
            prev_data[['serialno','Status','Status_P','sne','ppr','repair_days']],
            on='serialno', how='inner', suffixes=('', '_prev')
        )

        # Инициализация Status_P
        working_df['Status_P'] = None

        # Обработка статусов
        working_df.loc[working_df['Status_prev'] == 'Неактивно', 'Status_P'] = 'Неактивно'
        working_df.loc[working_df['Status_prev'] == 'Хранение', 'Status_P'] = 'Хранение'
        working_df.loc[working_df['Status_prev'] == 'Исправен', 'Status_P'] = 'Исправен'

        # Обработка ремонта
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

        # Обработка эксплуатации
        explo_mask = (working_df['Status_prev'] == 'Эксплуатация')
        sne_check = working_df['sne_prev'] < (working_df['ll'] - working_df['daily_flight_hours'])
        ppr_check = working_df['ppr_prev'] < (working_df['oh'] - working_df['daily_flight_hours'])
        working_df.loc[explo_mask & sne_check & ppr_check, 'Status_P'] = 'Эксплуатация'

        sne_limit = working_df['sne_prev'] >= (working_df['ll'] - working_df['daily_flight_hours'])
        working_df.loc[explo_mask & sne_limit, 'Status_P'] = 'Хранение'

        # Применяем функцию для определения статуса
        def exploitation_to_repair_or_storage(row):
            if (row['Status_prev'] == 'Эксплуатация') and (row['ppr_prev'] >= (row['oh'] - row['daily_flight_hours'])):
                return 'Ремонт' if row['sne_prev'] < row['BR'] else 'Хранение'
            return row['Status_P']

        working_df['Status_P'] = working_df.apply(exploitation_to_repair_or_storage, axis=1)

        # Обновляем Status_P в основном DataFrame
        curr_mask = (self.df['Dates'] == curr_date)
        curr_indices = self.df[curr_mask].index
        working_df = working_df.reset_index(drop=True)
        if len(curr_indices) != len(working_df):
            self.logger.error(f"Количество строк не совпадает: curr_indices={len(curr_indices)}, working_df={len(working_df)}")
            return
            
        # Убедимся, что все необходимые категории присутствуют
        current_categories = self.df['Status_P'].cat.categories
        new_values = working_df['Status_P'].unique()
        missing_categories = [cat for cat in new_values if cat not in current_categories]
        
        if missing_categories:
            # Добавляем недостающие категории
            self.df['Status_P'] = self.df['Status_P'].cat.add_categories(missing_categories)
            
        # Теперь можно безопасно присвоить новые значения
        self.df.loc[curr_indices, 'Status_P'] = working_df['Status_P'].values

    def step_2(self, curr_date):
        curr_data = self.df[self.df['Dates']==curr_date]
        mi8t_count = curr_data['mi8t_count'].iloc[0] if not pd.isna(curr_data['mi8t_count'].iloc[0]) else 0
        mi17_count = curr_data['mi17_count'].iloc[0] if not pd.isna(curr_data['mi17_count'].iloc[0]) else 0

        balance_mi8t = len(curr_data[(curr_data['Status_P']=='Эксплуатация')&(curr_data['ac_typ']=='Ми-8Т')])
        stock_mi8t = len(curr_data[(curr_data['Status_P']=='Исправен')&(curr_data['ac_typ']=='Ми-8Т')])

        balance_mi17 = len(curr_data[(curr_data['Status_P']=='Эксплуатация')&(curr_data['ac_typ']=='Ми-17')])
        stock_mi17 = len(curr_data[(curr_data['Status_P']=='Исправен')&(curr_data['ac_typ']=='Ми-17')])

        balance_empty = len(curr_data[(curr_data['Status_P']=='Эксплуатация')&(curr_data['ac_typ'].isna())])
        stock_empty = len(curr_data[(curr_data['Status_P']=='Исправен')&(curr_data['ac_typ'].isna())])

        final_balance_mi8t = balance_mi8t - mi8t_count
        final_balance_mi17 = balance_mi17 - mi17_count
        final_balance_total = final_balance_mi8t + final_balance_mi17 + balance_empty

        mask = (self.df['Dates']==curr_date)
        self.df.loc[mask,'balance_mi8t']=final_balance_mi8t
        self.df.loc[mask,'balance_mi17']=final_balance_mi17
        self.df.loc[mask,'balance_empty']=balance_empty
        self.df.loc[mask,'balance_total']=final_balance_total
        self.df.loc[mask,'stock_mi8t']=stock_mi8t
        self.df.loc[mask,'stock_mi17']=stock_mi17
        self.df.loc[mask,'stock_empty']=stock_empty
        self.df.loc[mask,'stock_total']=stock_mi8t+stock_mi17+stock_empty

    def step_3(self, curr_date):
        curr_data = self.df[self.df['Dates']==curr_date]
        balance_total = curr_data['balance_total'].iloc[0]
        mask = (self.df['Dates']==curr_date)
        
        # Убедимся что категории в Status и Status_P идентичны
        status_categories = set(self.df['Status'].cat.categories)
        status_p_categories = set(self.df['Status_P'].cat.categories)
        
        # Добавим недостающие категории в оба столбца
        all_categories = sorted(status_categories.union(status_p_categories))
        
        self.df['Status'] = self.df['Status'].cat.add_categories(
            [cat for cat in all_categories if cat not in status_categories]
        )
        self.df['Status_P'] = self.df['Status_P'].cat.add_categories(
            [cat for cat in all_categories if cat not in status_p_categories]
        )
        
        # Теперь можно безопасно копировать значения
        self.df.loc[mask,'Status'] = self.df.loc[mask,'Status_P']

        if balance_total > 0:
            exploitation = curr_data[curr_data['Status_P']=='Эксплуатация'].index.tolist()
            change_count=min(int(balance_total),len(exploitation))
            if change_count>0:
                self.df.loc[exploitation[:change_count],'Status']='Исправен'
        elif balance_total < 0:
            abs_balance = abs(int(balance_total))
            # Сначала переводим объекты со статусом "Исправен"
            serviceable_only = curr_data[curr_data['Status_P'] == 'Исправен'].index.tolist()
            serviceable_count = min(abs_balance, len(serviceable_only))
            if serviceable_count > 0:
                self.df.loc[serviceable_only[:serviceable_count], 'Status'] = 'Эксплуатация'
                abs_balance -= serviceable_count

            # Если не хватило, переводим объекты со статусом "Неактивно"
            if abs_balance > 0:
                inactive = curr_data[curr_data['Status_P'] == 'Неактивно'].index.tolist()
                inactive_count = min(abs_balance, len(inactive))
                if inactive_count > 0:
                    self.df.loc[inactive[:inactive_count], 'Status'] = 'Эксплуатация'
                    abs_balance -= inactive_count

    def step_4(self, prev_date, curr_date):
        """Рассчитываем значения sne, ppr и repair_days"""
        curr_data = self.df[self.df['Dates'] == curr_date]
        prev_data = self.df[self.df['Dates'] == prev_date]

        for _, row in curr_data.iterrows():
            idx = row.name
            serialno = row['serialno']
            status = row['Status_P']
            daily_flight_hours = row['daily_flight_hours']

            # Получаем предыдущие значения
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
                self.logger.error(f"Ошибка при обновлении значений для serialno={serialno}: {str(e)}")
                continue

    def save_all_results(self):
        """Запись результатов в куб"""
        self.logger.warning(f"Начинаем сохранение {len(self.df)} записей")
        self.logger.warning(f"Уникальные даты: {self.df['Dates'].unique()}")
        self.logger.warning(f"Уникальные serialno: {len(self.df['serialno'].unique())}")

        # Создаем временную таблицу только с нужными колонками
        create_temp_table = """
        CREATE TABLE IF NOT EXISTS default.updates_temp (
            serialno String,
            Dates Date,
            Status Nullable(String),
            Status_P Nullable(String),
            sne Nullable(Float32),
            ppr Nullable(Float32),
            repair_days Nullable(Float32),
            mi8t_count Nullable(Float32),
            mi17_count Nullable(Float32),
            balance_mi8t Nullable(Float32),
            balance_mi17 Nullable(Float32),
            balance_empty Nullable(Float32),
            balance_total Nullable(Float32),
            stock_mi8t Nullable(Float32),
            stock_mi17 Nullable(Float32),
            stock_empty Nullable(Float32),
            stock_total Nullable(Float32)
        ) ENGINE = Memory
        """
        self.client.execute(create_temp_table)

        # Подготавливаем данные для вставки
        self.logger.warning(f"Подготовка {len(self.df)} записей для вставки")
        insert_temp_query = """
        INSERT INTO default.updates_temp (
            serialno, Dates, Status, Status_P, sne, ppr, repair_days,
            mi8t_count, mi17_count,
            balance_mi8t, balance_mi17, balance_empty, balance_total,
            stock_mi8t, stock_mi17, stock_empty, stock_total
        )
        VALUES
        """
        
        # Вставляем данные во временную таблицу
        values = []
        for _, row in self.df.iterrows():
            values.append((
                row['serialno'], row['Dates'], row['Status'], row['Status_P'],
                row['sne'], row['ppr'], row['repair_days'],
                row['mi8t_count'], row['mi17_count'],
                row['balance_mi8t'], row['balance_mi17'], row['balance_empty'],
                row['balance_total'], row['stock_mi8t'], row['stock_mi17'],
                row['stock_empty'], row['stock_total']
            ))
        
        self.client.execute(insert_temp_query, values)
        self.logger.warning(f"Вставлено {len(values)} записей во временную таблицу")

        # Обновляем основную таблицу используя данные из временной
        update_query = f"""
        ALTER TABLE {self.database_name}.OlapCube_VNV
        UPDATE 
            Status = coalesce((SELECT t2.Status FROM {self.database_name}.updates_temp t2 WHERE t2.serialno = serialno AND t2.Dates = Dates LIMIT 1), Status),
            Status_P = coalesce((SELECT t2.Status_P FROM {self.database_name}.updates_temp t2 WHERE t2.serialno = serialno AND t2.Dates = Dates LIMIT 1), Status_P),
            sne = round(coalesce((SELECT t2.sne FROM {self.database_name}.updates_temp t2 WHERE t2.serialno = serialno AND t2.Dates = Dates LIMIT 1), sne), 2),
            ppr = round(coalesce((SELECT t2.ppr FROM {self.database_name}.updates_temp t2 WHERE t2.serialno = serialno AND t2.Dates = Dates LIMIT 1), ppr), 2),
            repair_days = coalesce((SELECT t2.repair_days FROM {self.database_name}.updates_temp t2 WHERE t2.serialno = serialno AND t2.Dates = Dates LIMIT 1), repair_days),
            mi8t_count = coalesce((SELECT t2.mi8t_count FROM {self.database_name}.updates_temp t2 WHERE t2.serialno = serialno AND t2.Dates = Dates LIMIT 1), mi8t_count),
            mi17_count = coalesce((SELECT t2.mi17_count FROM {self.database_name}.updates_temp t2 WHERE t2.serialno = serialno AND t2.Dates = Dates LIMIT 1), mi17_count),
            balance_mi8t = coalesce((SELECT t2.balance_mi8t FROM {self.database_name}.updates_temp t2 WHERE t2.serialno = serialno AND t2.Dates = Dates LIMIT 1), balance_mi8t),
            balance_mi17 = coalesce((SELECT t2.balance_mi17 FROM {self.database_name}.updates_temp t2 WHERE t2.serialno = serialno AND t2.Dates = Dates LIMIT 1), balance_mi17),
            balance_empty = coalesce((SELECT t2.balance_empty FROM {self.database_name}.updates_temp t2 WHERE t2.serialno = serialno AND t2.Dates = Dates LIMIT 1), balance_empty),
            balance_total = coalesce((SELECT t2.balance_total FROM {self.database_name}.updates_temp t2 WHERE t2.serialno = serialno AND t2.Dates = Dates LIMIT 1), balance_total),
            stock_mi8t = coalesce((SELECT t2.stock_mi8t FROM {self.database_name}.updates_temp t2 WHERE t2.serialno = serialno AND t2.Dates = Dates LIMIT 1), stock_mi8t),
            stock_mi17 = coalesce((SELECT t2.stock_mi17 FROM {self.database_name}.updates_temp t2 WHERE t2.serialno = serialno AND t2.Dates = Dates LIMIT 1), stock_mi17),
            stock_empty = coalesce((SELECT t2.stock_empty FROM {self.database_name}.updates_temp t2 WHERE t2.serialno = serialno AND t2.Dates = Dates LIMIT 1), stock_empty),
            stock_total = coalesce((SELECT t2.stock_total FROM {self.database_name}.updates_temp t2 WHERE t2.serialno = serialno AND t2.Dates = Dates LIMIT 1), stock_total)
        WHERE EXISTS(
            SELECT 1 
            FROM {self.database_name}.updates_temp t2 
            WHERE t2.serialno = serialno AND t2.Dates = Dates
        )
        """
        
        try:
            self.client.execute(update_query)
            self.logger.warning("Данные успешно обновлены")
        except Exception as e:
            self.logger.error(f"Ошибка при обновлении данных: {str(e)}")
            raise e
        finally:
            # Удаляем временную таблицу
            self.client.execute("DROP TABLE IF EXISTS default.updates_temp")
            self.logger.warning("Временная таблица удалена")

if __name__ == "__main__":
    processor = CycleProcessor(total_days=100)
    processor.run_cycle()
