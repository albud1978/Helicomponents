import os
import logging
import cudf
import cupy as cp
from numba import cuda
from clickhouse_driver import Client
from dotenv import load_dotenv
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class CycleProcessor:
    def __init__(self, total_days=2):
        self.logger = logger
        self.total_days = total_days

        load_dotenv()
        clickhouse_host = os.getenv('CLICKHOUSE_HOST', '10.95.19.132')
        clickhouse_user = os.getenv('CLICKHOUSE_USER', 'default')
        clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', 'password')
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
        self.analytics_fields = [
            'ops_count','hbs_count','repair_count','total_operable',
            'entry_count','exit_count','into_repair','complete_repair',
            'remain_repair','remain','midlife_repair','midlife','hours'
        ]
        self.ensure_analytics_columns()

    def ensure_analytics_columns(self):
        query = f"""
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
        self.client.execute(query)
        self.logger.info("Analytics columns ensured.")

    def get_first_date(self):
        query = f"SELECT MIN(Dates) FROM {self.database_name}.OlapCube_VNV"
        result = self.client.execute(query)
        first_date = result[0][0]
        if not first_date:
            raise Exception("No start date found.")
        return first_date

    def load_all_data(self):
        self.first_date = self.get_first_date()
        period_end = self.first_date + timedelta(days=self.total_days)
        self.logger.info(f"Loading data from {self.first_date} to {period_end}")

        query = f"""
        SELECT 
          trigger_type, RepairTime, Status_P, repair_days, sne, ppr, Status,
          location, ac_typ, daily_flight_hours, daily_flight_hours_f, BR, ll,
          oh, threshold, Effectivity, serialno, Dates, mi8t_count, mi17_count,
          balance_mi8t, balance_mi17, balance_empty, balance_total, stock_mi8t,
          stock_mi17, stock_empty, stock_total
        FROM {self.database_name}.OlapCube_VNV
        WHERE Dates >= '{self.first_date.strftime('%Y-%m-%d')}'
          AND Dates < '{period_end.strftime('%Y-%m-%d')}'
        ORDER BY Dates, serialno
        """
        data = self.client.execute(query)
        if not data:
            raise Exception("No data found.")

        columns = [
          'trigger_type','RepairTime','Status_P','repair_days','sne','ppr','Status',
          'location','ac_typ','daily_flight_hours','daily_flight_hours_f','BR','ll','oh',
          'threshold','Effectivity','serialno','Dates','mi8t_count','mi17_count','balance_mi8t',
          'balance_mi17','balance_empty','balance_total','stock_mi8t','stock_mi17','stock_empty','stock_total'
        ]
        self.df = cudf.DataFrame(data, columns=columns)

        # Приведение типов
        num_cols = [
          'trigger_type','RepairTime','repair_days','sne','ppr','daily_flight_hours',
          'daily_flight_hours_f','BR','ll','oh','threshold','mi8t_count','mi17_count',
          'balance_mi8t','balance_mi17','balance_empty','balance_total','stock_mi8t',
          'stock_mi17','stock_empty','stock_total'
        ]
        for c in num_cols:
            self.df[c] = self.df[c].astype('float32')

        # Строки
        for c in ['Status','Status_P','location','ac_typ','Effectivity']:
            self.df[c] = self.df[c].fillna('').astype('str')

        self.df['Dates'] = cudf.to_datetime(self.df['Dates'])

        self.df['daily_flight_hours'] = self.df['daily_flight_hours'].round(2)
        self.df['daily_flight_hours_f'] = self.df['daily_flight_hours_f'].round(2)

        self.logger.info(f"Loaded {len(self.df)} records.")

    def run_cycle(self):
        self.load_all_data()
        period_end = self.first_date + timedelta(days=self.total_days)
        self.process_all_dates(self.first_date, period_end)
        self.calculate_analytics_metrics()
        self.save_all_results(self.first_date, period_end)
        self.logger.info("GPU processing complete.")

    def process_all_dates(self, start, end):
        all_dates = self.df['Dates'].unique().values.sort()
        cycle_dates = [d for d in all_dates if (d >= cp.datetime64(start)) and (d < cp.datetime64(end))]
        if len(cycle_dates) < 2:
            return
        for i in range(1, len(cycle_dates)):
            prev_date = cycle_dates[i - 1]
            curr_date = cycle_dates[i]
            self.step_1(prev_date, curr_date)
            self.step_2(curr_date)
            self.step_3(curr_date)
            self.step_4(prev_date, curr_date)

    def step_1(self, prev_date, curr_date):
        df_prev = self.df[self.df['Dates'] == prev_date]
        df_curr = self.df[self.df['Dates'] == curr_date]
        if len(df_prev) == 0 or len(df_curr) == 0:
            return

        merged = df_curr.merge(
            df_prev[['serialno','Status','Status_P','sne','ppr','repair_days']],
            on='serialno', how='inner', suffixes=('','_prev')
        )
        merged['Status_P'] = None
        # Логика как на CPU
        mask_na = merged['Status_prev'] == 'Неактивно'
        merged.loc[mask_na,'Status_P'] = 'Неактивно'
        mask_storage = merged['Status_prev'] == 'Хранение'
        merged.loc[mask_storage,'Status_P'] = 'Хранение'
        mask_serv = merged['Status_prev'] == 'Исправен'
        merged.loc[mask_serv,'Status_P'] = 'Исправен'
        r_mask = (merged['Status_prev']=='Ремонт')&(merged['repair_days_prev']<merged['RepairTime'])
        merged.loc[r_mask,'Status_P'] = 'Ремонт'
        r_done = (merged['Status_prev']=='Ремонт')&(merged['repair_days_prev']>=merged['RepairTime'])
        merged.loc[r_done,'Status_P'] = 'Исправен'

        # Эксплуатация + ресурс
        explo_mask = merged['Status_prev']=='Эксплуатация'
        sne_ok = merged['sne_prev']<(merged['ll']-merged['daily_flight_hours'])
        ppr_ok = merged['ppr_prev']<(merged['oh']-merged['daily_flight_hours'])
        merged.loc[explo_mask & sne_ok & ppr_ok, 'Status_P'] = 'Эксплуатация'
        sne_lim = merged['sne_prev']>=(merged['ll']-merged['daily_flight_hours'])
        merged.loc[explo_mask & sne_lim, 'Status_P'] = 'Хранение'

        def explo_to_repair_or_storage(row):
            if row['Status_prev']=='Эксплуатация' and (row['ppr_prev']>=(row['oh']-row['daily_flight_hours'])):
                return 'Ремонт' if row['sne_prev']<row['BR'] else 'Хранение'
            return row['Status_P']

        merged['Status_P'] = merged.applymap(explo_to_repair_or_storage, axis=1)

        # Обновляем
        for i, row in merged.iterrows():
            idx = row._cudf_index
            self.df.loc[idx,'Status_P'] = row['Status_P']

    def step_2(self, curr_date):
        df_curr = self.df[self.df['Dates'] == curr_date]
        if len(df_curr)==0:
            return
        mi8t = df_curr['mi8t_count'].iloc[0]
        mi17 = df_curr['mi17_count'].iloc[0]

        balance_mi8t = len(df_curr[(df_curr['Status_P']=='Эксплуатация')&(df_curr['ac_typ']=='Ми-8Т')])
        balance_mi17 = len(df_curr[(df_curr['Status_P']=='Эксплуатация')&(df_curr['ac_typ']=='Ми-17')])
        stock_mi8t = len(df_curr[(df_curr['Status_P']=='Исправен')&(df_curr['ac_typ']=='Ми-8Т')])
        stock_mi17 = len(df_curr[(df_curr['Status_P']=='Исправен')&(df_curr['ac_typ']=='Ми-17')])

        empty_ac_typ = (df_curr['ac_typ'].str.strip().isin(['','none']))
        balance_empty = len(df_curr[(df_curr['Status_P']=='Эксплуатация') & empty_ac_typ])
        stock_empty = len(df_curr[(df_curr['Status_P']=='Исправен') & empty_ac_typ])

        final_mi8t = balance_mi8t - mi8t
        final_mi17 = balance_mi17 - mi17
        final_total = final_mi8t + final_mi17 + balance_empty

        idxes = df_curr.index
        self.df.loc[idxes,'balance_mi8t'] = final_mi8t
        self.df.loc[idxes,'balance_mi17'] = final_mi17
        self.df.loc[idxes,'balance_empty'] = balance_empty
        self.df.loc[idxes,'balance_total'] = final_total
        self.df.loc[idxes,'stock_mi8t'] = balance_mi8t
        self.df.loc[idxes,'stock_mi17'] = balance_mi17
        self.df.loc[idxes,'stock_empty'] = stock_empty
        self.df.loc[idxes,'stock_total'] = balance_mi8t+stock_mi17+stock_empty

    def step_3(self, curr_date):
        df_curr = self.df[self.df['Dates']==curr_date]
        if len(df_curr)==0:
            return
        balance = df_curr['balance_total'].iloc[0]
        idxes = df_curr.index
        self.df.loc[idxes,'Status'] = self.df.loc[idxes,'Status_P']
        if balance>0:
            # Меняем часть 'Эксплуатации' -> 'Исправен'
            explo_idx = df_curr[df_curr['Status_P']=='Эксплуатация'].index
            change_count = int(balance)
            if change_count>0 and change_count<=len(explo_idx):
                self.df.loc[explo_idx[:change_count],'Status']='Исправен'
        elif balance<0:
            need = abs(int(balance))
            serv_idx = df_curr[df_curr['Status_P']=='Исправен'].index
            scount = min(need,len(serv_idx))
            if scount>0:
                self.df.loc[serv_idx[:scount],'Status']='Эксплуатация'
                need-=scount
            if need>0:
                na_idx = df_curr[df_curr['Status_P']=='Неактивно'].index
                nacount = min(need,len(na_idx))
                if nacount>0:
                    self.df.loc[na_idx[:nacount],'Status']='Эксплуатация'

    def step_4(self, prev_date, curr_date):
        df_curr = self.df[self.df['Dates']==curr_date]
        df_prev = self.df[self.df['Dates']==prev_date]
        if len(df_prev)==0 or len(df_curr)==0:
            return
        for i,row in df_curr.iterrows():
            sid = row['serialno']
            sp = row['Status_P']
            dfl = row['daily_flight_hours']
            old = df_prev[df_prev['serialno']==sid]
            if len(old)==0:
                continue
            sne_p = old['sne'].iloc[0]
            ppr_p = old['ppr'].iloc[0]
            rep_p = old['repair_days'].iloc[0]
            st_p = old['Status'].iloc[0]
            stp_p = old['Status_P'].iloc[0]
            try:
                if sp=='Эксплуатация':
                    self.df.loc[i,'sne'] = cp.float32(round(sne_p + dfl,2))
                    self.df.loc[i,'ppr'] = cp.float32(round(ppr_p + dfl,2))
                elif sp=='Исправен':
                    if stp_p=='Ремонт':
                        self.df.loc[i,'sne']=cp.float32(round(sne_p,2))
                        self.df.loc[i,'ppr']=cp.float32(0.0)
                        self.df.loc[i,'repair_days']=None
                    else:
                        self.df.loc[i,'sne']=cp.float32(round(sne_p,2))
                        self.df.loc[i,'ppr']=cp.float32(round(ppr_p,2))
                        self.df.loc[i,'repair_days']=rep_p
                elif sp=='Ремонт':
                    self.df.loc[i,'sne']=cp.float32(round(sne_p,2))
                    self.df.loc[i,'ppr']=cp.float32(round(ppr_p,2))
                    if st_p=='Эксплуатация':
                        self.df.loc[i,'repair_days']=1
                    elif rep_p is not None:
                        self.df.loc[i,'repair_days']=rep_p+1
                else:
                    self.df.loc[i,'sne']=cp.float32(round(sne_p,2))
                    self.df.loc[i,'ppr']=cp.float32(round(ppr_p,2))
                    self.df.loc[i,'repair_days']=rep_p
            except:
                pass

    def ensure_tmp_table_exists(self):
        drop_q = f"DROP TABLE IF EXISTS {self.database_name}.tmp_OlapCube_update"
        self.client.execute(drop_q)
        create_q = f"""
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
        ) ENGINE=Memory
        """
        self.client.execute(create_q)

    def save_all_results(self, start, end):
        df_period = self.df[(self.df['Dates']>=cp.datetime64(start)) & (self.df['Dates']<cp.datetime64(end))]
        # Оставляем строки, где дата > first_date
        df_to_update = df_period[df_period['Dates']>cp.datetime64(self.first_date)]
        self.ensure_tmp_table_exists()

        # Преобразуем в python-список кортежей для передачи в ClickHouse
        all_cols = [
            'trigger_type','RepairTime','Status_P','repair_days','sne','ppr','Status',
            'location','ac_typ','daily_flight_hours','daily_flight_hours_f','BR','ll','oh',
            'threshold','Effectivity','serialno','Dates','mi8t_count','mi17_count','balance_mi8t',
            'balance_mi17','balance_empty','balance_total','stock_mi8t','stock_mi17','stock_empty',
            'stock_total','ops_count','hbs_count','repair_count','total_operable','entry_count',
            'exit_count','into_repair','complete_repair','remain_repair','remain','midlife_repair',
            'midlife','hours'
        ]
        for c in ['repair_days','sne','ppr','daily_flight_hours','daily_flight_hours_f']:
            df_to_update[c] = df_to_update[c].round(2)

        # Заполняем нулями вместо NaN
        for c in all_cols:
            if str(df_to_update[c].dtype).startswith('float'):
                df_to_update[c] = df_to_update[c].fillna(0)

        records = df_to_update[all_cols].to_pandas().itertuples(index=False, name=None)
        insert_q = f"""
        INSERT INTO {self.database_name}.tmp_OlapCube_update ({', '.join(all_cols)}) VALUES
        """
        self.client.execute(insert_q, list(records))

        insert_main = f"""
        INSERT INTO {self.database_name}.OlapCube_VNV ({', '.join(all_cols)})
        SELECT {', '.join(all_cols)} FROM {self.database_name}.tmp_OlapCube_update
        """
        self.client.execute(insert_main)
        self.client.execute(f"OPTIMIZE TABLE {self.database_name}.OlapCube_VNV FINAL")
        self.client.execute(f"DROP TABLE {self.database_name}.tmp_OlapCube_update")

    def calculate_analytics_metrics(self):
        if len(self.df)==0:
            return
        start = cp.datetime64(self.first_date)
        end = start + cp.timedelta64(self.total_days,'D')
        all_dates = sorted(d for d in self.df['Dates'].unique().values if d>=start and d<end)
        if len(all_dates)<2:
            return
        # По очереди считаем метрики и заливаем
        for i in range(1,len(all_dates)):
            curr_d = all_dates[i]
            prev_d = all_dates[i-1]
            df_curr = self.df[self.df['Dates']==curr_d]
            df_prev = self.df[self.df['Dates']==prev_d]
            ops_count = len(df_curr[df_curr['Status']=='Эксплуатация'])
            hbs_count = len(df_curr[df_curr['Status']=='Исправен'])
            rep_count = len(df_curr[df_curr['Status']=='Ремонт'])
            total_op = ops_count+hbs_count+rep_count

            merged = df_prev[['serialno','Status']].merge(df_curr[['serialno','Status']],on='serialno',
                       suffixes=('_prev','_curr'))
            entry_count = len(merged[(merged['Status_prev']=='Неактивно')&(merged['Status_curr']=='Эксплуатация')])
            exit_count = len(merged[(merged['Status_prev']=='Эксплуатация')&(merged['Status_curr']=='Хранение')])
            into_repair = len(merged[(merged['Status_prev']=='Эксплуатация')&(merged['Status_curr']=='Ремонт')])
            complete_rep = len(merged[(merged['Status_prev']=='Ремонт')&(merged['Status_curr']!='Ремонт')])

            op_tech = df_curr[df_curr['Status'].isin(['Эксплуатация','Исправен'])]
            remain_rep = (op_tech['oh'] - op_tech['ppr']).fillna(0).sum()
            serv_tech = df_curr[df_curr['Status'].isin(['Эксплуатация','Исправен','Ремонт'])]
            remain_val = (serv_tech['ll'] - serv_tech['sne']).fillna(0).sum()
            total_ll = serv_tech['ll'].fillna(0).sum()
            midlife = remain_val/total_ll if total_ll>0 else 0
            total_oh = op_tech['oh'].fillna(0).sum()
            midlife_r = remain_rep/total_oh if total_oh>0 else 0
            hours_val = df_curr[df_curr['Status']=='Эксплуатация']['daily_flight_hours'].sum()

            idxes = df_curr.index
            self.df.loc[idxes,'ops_count'] = ops_count
            self.df.loc[idxes,'hbs_count'] = hbs_count
            self.df.loc[idxes,'repair_count'] = rep_count
            self.df.loc[idxes,'total_operable'] = total_op
            self.df.loc[idxes,'entry_count'] = entry_count
            self.df.loc[idxes,'exit_count'] = exit_count
            self.df.loc[idxes,'into_repair'] = into_repair
            self.df.loc[idxes,'complete_repair'] = complete_rep
            self.df.loc[idxes,'remain_repair'] = remain_rep
            self.df.loc[idxes,'remain'] = remain_val
            self.df.loc[idxes,'midlife_repair'] = midlife_r
            self.df.loc[idxes,'midlife'] = midlife
            self.df.loc[idxes,'hours'] = hours_val

if __name__ == "__main__":
    processor = CycleProcessor(total_days=4000)
    processor.run_cycle()