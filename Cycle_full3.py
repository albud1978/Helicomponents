import os
import logging
import pandas as pd
from clickhouse_driver import Client
from dotenv import load_dotenv
from datetime import datetime, timedelta
import numpy as np
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
        self.save_all_results()
        self.logger.warning("Обработка завершена. Результаты записаны в базу.")

        # Дополнительная проверка после записи в БД: SELECT для 2024-11-26
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

        working_df = pd.merge(
            curr_data,
            prev_data[['serialno','Status','Status_P','sne','ppr','repair_days']],
            on='serialno', how='left', suffixes=('', '_prev')
        )

        # Шаг I
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

        explo_mask = (working_df['Status_prev']=='Эксплуатация')
        sne_check = working_df['sne_prev']<(working_df['ll']-working_df['daily_flight_hours'])
        ppr_check = working_df['ppr_prev']<(working_df['oh']-working_df['daily_flight_hours'])
        working_df.loc[explo_mask & sne_check & ppr_check,'Status_P']='Эксплуатация'

        sne_limit = working_df['sne_prev']>=(working_df['ll']-working_df['daily_flight_hours'])
        working_df.loc[explo_mask & sne_limit,'Status_P']='Хранение'

        def exploitation_to_repair_or_storage(row):
            if (row['Status_prev'] == 'Эксплуатация') and (row['ppr_prev'] >= (row['oh'] - row['daily_flight_hours'])):
                return 'Ремонт' if row['sne_prev'] < row['BR'] else 'Хранение'
            return row['Status_P']

        working_df['Status_P'] = working_df.apply(exploitation_to_repair_or_storage, axis=1)

        part_of_df = self.df[self.df['Dates']==curr_date].copy()
        working_df = working_df.sort_values('serialno')
        part_of_df = part_of_df.sort_values('serialno')

        if len(part_of_df) != len(working_df):
            self.logger.error("Количество строк не совпадает, проверьте данные!")
        else:
            self.df.loc[part_of_df.index, 'Status_P'] = working_df['Status_P'].values

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
        self.df.loc[mask,'Status']=self.df.loc[mask,'Status_P']

        if balance_total > 0:
            exploitation = curr_data[curr_data['Status_P']=='Эксплуатация'].index.tolist()
            change_count=min(int(balance_total),len(exploitation))
            if change_count>0:
                self.df.loc[exploitation[:change_count],'Status']='Исправен'
        elif balance_total<0:
            abs_balance=abs(int(balance_total))
            serviceable = curr_data[curr_data['Status_P'].isin(['Исправен','Неактивно'])].index.tolist()
            change_count=min(abs_balance,len(serviceable))
            if change_count>0:
                self.df.loc[serviceable[:change_count],'Status']='Эксплуатация'

    def step_4(self, prev_date, curr_date):
        prev_data = self.df[self.df['Dates']==prev_date][['serialno','Status','Status_P','sne','ppr','repair_days']].set_index('serialno')
        curr_mask = (self.df['Dates']==curr_date)
        for idx,row in self.df[curr_mask].iterrows():
            serialno=row['serialno']
            status=row['Status']
            daily_flight_hours=row['daily_flight_hours']
            if serialno in prev_data.index:
                prev_sne=prev_data.at[serialno,'sne']
                prev_ppr=prev_data.at[serialno,'ppr']
                prev_repair=prev_data.at[serialno,'repair_days']
                prev_status=prev_data.at[serialno,'Status']
                prev_status_p=prev_data.at[serialno,'Status_P']
            else:
                prev_sne=None
                prev_ppr=None
                prev_repair=None
                prev_status=None
                prev_status_p=None

            if status=='Эксплуатация':
                if prev_sne is not None:
                    self.df.at[idx,'sne']=np.float32(prev_sne+daily_flight_hours)
                    self.df.at[idx,'ppr']=np.float32(prev_ppr+daily_flight_hours)
            elif status=='Исправен':
                if prev_status_p=='Ремонт':
                    self.df.at[idx,'sne']=np.float32(prev_sne)
                    self.df.at[idx,'ppr']=np.float32(0)
                    self.df.at[idx,'repair_days']=np.nan
                else:
                    self.df.at[idx,'sne']=np.float32(prev_sne)
                    self.df.at[idx,'ppr']=np.float32(prev_ppr)
                    self.df.at[idx,'repair_days']=prev_repair
            elif status=='Ремонт':
                if prev_status=='Эксплуатация':
                    self.df.at[idx,'sne']=np.float32(prev_sne)
                    self.df.at[idx,'ppr']=np.float32(prev_ppr)
                    self.df.at[idx,'repair_days']=1
                else:
                    self.df.at[idx,'sne']=np.float32(prev_sne)
                    self.df.at[idx,'ppr']=np.float32(prev_ppr)
                    if prev_repair is not None:
                        self.df.at[idx,'repair_days']=prev_repair+1
            elif status in ['Хранение','Неактивно']:
                self.df.at[idx,'sne']=np.float32(prev_sne)
                self.df.at[idx,'ppr']=np.float32(prev_ppr)
                self.df.at[idx,'repair_days']=prev_repair

    def save_all_results(self):
        """Запись результатов в куб"""
        float_cols = ['sne','ppr','repair_days','ll','oh','BR','daily_flight_hours','RepairTime','mi8t_count','mi17_count','balance_mi8t','balance_mi17','balance_empty','balance_total','stock_mi8t','stock_mi17','stock_empty','stock_total']
        for col in float_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(np.float64, errors='ignore')

        required_cols = ['serialno','Dates','Status','Status_P','sne','ppr','repair_days','balance_mi8t','balance_mi17','balance_empty','balance_total','stock_mi8t','stock_mi17','stock_empty','stock_total']
        for col in required_cols:
            if col not in self.df.columns:
                self.df[col]=np.nan

        if len(self.df)==0:
            self.logger.warning("Нет данных для записи в базу (df пуст).")
            return

        update_data=[]
        for _,row in self.df.iterrows():
            update_data.append((
                row['serialno'],
                row['Dates'],
                row['Status'],
                row['Status_P'],
                row['sne'],
                row['ppr'],
                row['repair_days'],
                row['balance_mi8t'],
                row['balance_mi17'],
                row['balance_empty'],
                row['balance_total'],
                row['stock_mi8t'],
                row['stock_mi17'],
                row['stock_empty'],
                row['stock_total']
            ))

        create_temp_table = f"""
        CREATE TEMPORARY TABLE IF NOT EXISTS updates_temp (
            serialno String,
            Dates Date,
            Status String,
            Status_P String,
            sne Float64,
            ppr Float64,
            repair_days Float64,
            balance_mi8t Float64,
            balance_mi17 Float64,
            balance_empty Float64,
            balance_total Float64,
            stock_mi8t Float64,
            stock_mi17 Float64,
            stock_empty Float64,
            stock_total Float64
        ) ENGINE = Memory
        """
        self.client.execute(create_temp_table)
        
        insert_query = "INSERT INTO updates_temp VALUES"
        self.client.execute(insert_query, update_data)

        # Проверка перед UPDATE
        check_updates = self.client.execute(f"SELECT * FROM updates_temp LIMIT 10")
        self.logger.warning("Пример данных в updates_temp перед UPDATE:")
        for r in check_updates:
            self.logger.warning(r)

        update_query = f"""
        ALTER TABLE {self.database_name}.OlapCube_VNV
        UPDATE 
            Status = (SELECT Status FROM updates_temp WHERE updates_temp.serialno = serialno AND updates_temp.Dates = Dates LIMIT 1),
            Status_P = (SELECT Status_P FROM updates_temp WHERE updates_temp.serialno = serialno AND updates_temp.Dates = Dates LIMIT 1),
            sne = (SELECT sne FROM updates_temp WHERE updates_temp.serialno = serialno AND updates_temp.Dates = Dates LIMIT 1),
            ppr = (SELECT ppr FROM updates_temp WHERE updates_temp.serialno = serialno AND updates_temp.Dates = Dates LIMIT 1),
            repair_days = (SELECT repair_days FROM updates_temp WHERE updates_temp.serialno = serialno AND updates_temp.Dates = Dates LIMIT 1),
            balance_mi8t = (SELECT balance_mi8t FROM updates_temp WHERE updates_temp.serialno = serialno AND updates_temp.Dates = Dates LIMIT 1),
            balance_mi17 = (SELECT balance_mi17 FROM updates_temp WHERE updates_temp.serialno = serialno AND updates_temp.Dates = Dates LIMIT 1),
            balance_empty = (SELECT balance_empty FROM updates_temp WHERE updates_temp.serialno = serialno AND updates_temp.Dates = Dates LIMIT 1),
            balance_total = (SELECT balance_total FROM updates_temp WHERE updates_temp.serialno = serialno AND updates_temp.Dates = Dates LIMIT 1),
            stock_mi8t = (SELECT stock_mi8t FROM updates_temp WHERE updates_temp.serialno = serialno AND updates_temp.Dates = Dates LIMIT 1),
            stock_mi17 = (SELECT stock_mi17 FROM updates_temp WHERE updates_temp.serialno = serialno AND updates_temp.Dates = Dates LIMIT 1),
            stock_empty = (SELECT stock_empty FROM updates_temp WHERE updates_temp.serialno = serialno AND updates_temp.Dates = Dates LIMIT 1),
            stock_total = (SELECT stock_total FROM updates_temp WHERE updates_temp.serialno = serialno AND updates_temp.Dates = Dates LIMIT 1)
        WHERE EXISTS(
            SELECT 1 
            FROM updates_temp
            WHERE updates_temp.serialno = serialno
            AND updates_temp.Dates = Dates
        )
        """

        self.client.execute(update_query)
        self.logger.warning("Все результаты успешно сохранены")

        # Проверим данные после обновления
        check_date = datetime(2024,11,26).date()
        after_query = f"""
        SELECT serialno, Dates, Status, Status_P, sne, ppr, repair_days 
        FROM {self.database_name}.OlapCube_VNV 
        WHERE Dates = '{check_date}'
        LIMIT 10
        """
        after_result = self.client.execute(after_query)
        self.logger.warning("Пример данных на 2024-11-26 после записи в базу:")
        for row in after_result:
            self.logger.warning(row)

        self.client.execute("DROP TABLE IF EXISTS updates_temp")

if __name__ == "__main__":
    processor = CycleProcessor(total_days=100)
    processor.run_cycle()
