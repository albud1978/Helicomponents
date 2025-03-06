import os
import logging
import sys
from datetime import datetime, timedelta

import cupy as cp              # вместо numpy
import cudf                    # вместо pandas
from clickhouse_driver import Client
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
if not logger.handlers:
    logger.addHandler(handler)


class CycleProcessorGPU:
    def __init__(self, total_days=2):
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
        
        self.df = None  # Здесь будет cudf DataFrame
        
        # Аналитические поля
        self.analytics_fields = [
            'ops_count','hbs_count','repair_count','total_operable','entry_count','exit_count',
            'into_repair','complete_repair','remain_repair','remain','midlife_repair','midlife','hours'
        ]

    def ensure_analytics_columns(self):
        """
        Аналог CPU-варианта: добавляет недостающие поля в таблицу ClickHouse.
        """
        try:
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
            self.logger.info("GPU: Аналитические поля добавлены/проверены в OlapCube_VNV.")
        except Exception as e:
            self.logger.error(f"GPU: Ошибка при добавлении аналитических полей: {e}", exc_info=True)

    def get_first_date(self) -> datetime:
        query = f"SELECT MIN(Dates) as first_date FROM {self.database_name}.OlapCube_VNV"
        result = self.client.execute(query)
        first_date = result[0][0]
        if not first_date:
            raise Exception("Не найдена начальная дата (база пустая?)")
        return first_date

    def load_all_data(self):
        """
        Загружаем данные в cudf DataFrame.
        """
        self.ensure_analytics_columns()
        first_date = self.get_first_date()
        self.first_date = first_date
        period_end = first_date + timedelta(days=self.total_days)
        self.logger.info(f"GPU: Загружаем данные с {first_date} до {period_end}")

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
            stock_total,
            
            -- Аналитические поля
            ops_count,
            hbs_count,
            repair_count,
            total_operable,
            entry_count,
            exit_count,
            into_repair,
            complete_repair,
            remain_repair,
            remain,
            midlife_repair,
            midlife,
            hours
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
            'stock_total',
            # Аналитические поля
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
        
        # Переводим результат в cudf DataFrame
        import pandas as pd  # временно, чтобы из результата сделать pd.DataFrame
        df_pd = pd.DataFrame(result, columns=columns)
        self.df = cudf.from_pandas(df_pd)
        
        # Приводим типы при необходимости (пример)
        # В cudf можно делать self.df['col'] = self.df['col'].astype('float32')
        numeric_cols = [
            'trigger_type', 'RepairTime', 'repair_days', 'sne', 'ppr',
            'daily_flight_hours', 'daily_flight_hours_f', 'BR', 'll', 'oh',
            'threshold', 'mi8t_count', 'mi17_count', 'balance_mi8t', 'balance_mi17',
            'balance_empty', 'balance_total', 'stock_mi8t', 'stock_mi17', 'stock_empty',
            'stock_total'
        ] + self.analytics_fields
        
        for col in numeric_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].astype('float32')
        
        # Dates -> datetime64[ns] (cudf)
        self.df['Dates'] = cudf.to_datetime(self.df['Dates'])
        
        # Прочие поля как строки
        for col in ['Status_P', 'Status', 'location', 'ac_typ', 'Effectivity', 'serialno']:
            self.df[col] = self.df[col].fillna('').astype('str')
        
        self.logger.info(f"GPU: Данные загружены в cudf DF: всего {len(self.df)} записей")

    def run_cycle(self):
        """
        Основной метод, аналогичный CPU-версии.
        """
        self.load_all_data()
        period_start = self.first_date
        period_end = period_start + timedelta(days=self.total_days)
        
        self.logger.info(f"GPU: Начинаем обработку дат... {period_start} - {period_end}")
        
        # Собираем уникальные даты
        unique_dates = self.df['Dates'].unique().sort_values()
        # cudf.Series -> нужно перевести в host для фильтра, либо сразу сравнивать c cudf.Timestamp
        # Для упрощения делаем datetime -> фильтруем на CPU, но можно и полностью в GPU
        unique_dates_pd = unique_dates.to_pandas()
        cycle_dates = [d for d in unique_dates_pd if period_start <= d < period_end]
        cycle_dates.sort()
        
        if len(cycle_dates) < 2:
            self.logger.warning("GPU: Недостаточно дат для цикла обработки.")
            return
        
        # Шаги step_1..step_4 векторизованно
        for i in range(1, len(cycle_dates)):
            prev_date = cycle_dates[i-1]
            curr_date = cycle_dates[i]
            
            self.step_1(prev_date, curr_date)
            self.step_2(curr_date)
            self.step_3(curr_date)
            self.step_4(prev_date, curr_date)
        
        # Расчет аналитики
        self.calculate_analytics_metrics()
        
        # Сохранение в ClickHouse
        self.save_all_results(period_start, period_end)
        
        self.logger.info("GPU: Обработка завершена.")
    
    def step_1(self, prev_date, curr_date):
        """
        Переносим логику CPU-шного step_1, но в стиле векторных операций cudf.
        """
        prev_data = self.df[self.df['Dates'] == prev_date]
        curr_data = self.df[self.df['Dates'] == curr_date]
        
        if len(prev_data) == 0 or len(curr_data) == 0:
            self.logger.warning(f"GPU: Нет данных для {prev_date} или {curr_date}")
            return
        
        # Объединяем по serialno (inner join)
        merged = curr_data.merge(
            prev_data[['serialno','Status','Status_P','sne','ppr','repair_days']],
            on='serialno',
            suffixes=('', '_prev'),
            how='inner'
        )
        
        # Создаём столбец Status_P заново
        merged['new_Status_P'] = None
        
        # Пример масочных присвоений:
        mask1 = (merged['Status_prev'] == 'Неактивно') | (merged['Status_prev'] == 'Хранение') | (merged['Status_prev'] == 'Исправен')
        merged.loc[mask1, 'new_Status_P'] = merged['Status_prev']
        
        # Ремонт: если repair_days_prev < RepairTime, тогда 'Ремонт', иначе 'Исправен'
        mask_repair_not_done = (merged['Status_prev'] == 'Ремонт') & (merged['repair_days_prev'] < merged['RepairTime'])
        merged.loc[mask_repair_not_done, 'new_Status_P'] = 'Ремонт'
        
        mask_repair_complete = (merged['Status_prev'] == 'Ремонт') & (merged['repair_days_prev'] >= merged['RepairTime'])
        merged.loc[mask_repair_complete, 'new_Status_P'] = 'Исправен'
        
        # Эксплуатация
        explo_mask = (merged['Status_prev'] == 'Эксплуатация')
        sne_check = merged['sne_prev'] < (merged['ll'] - merged['daily_flight_hours'])
        ppr_check = merged['ppr_prev'] < (merged['oh'] - merged['daily_flight_hours'])
        
        mask_explo = explo_mask & sne_check & ppr_check
        merged.loc[mask_explo, 'new_Status_P'] = 'Эксплуатация'
        
        # sne_limit
        sne_limit = merged['sne_prev'] >= (merged['ll'] - merged['daily_flight_hours'])
        mask_to_storage = explo_mask & sne_limit
        merged.loc[mask_to_storage, 'new_Status_P'] = 'Хранение'
        
        # Если ppr_prev >= (oh - daily_flight_hours),
        # то либо 'Ремонт', либо 'Хранение' (проверка sne_prev < BR?)
        # Вместо функции exploitation_to_repair_or_storage - делаем ещё одну маску:
        mask_explo_to_repair_or_storage = explo_mask & (merged['ppr_prev'] >= (merged['oh'] - merged['daily_flight_hours']))
        
        # Делим внутри по sne_prev < BR
        # Для таких логик нам придётся сделать 2 подмаски
        mask_to_repair = mask_explo_to_repair_or_storage & (merged['sne_prev'] < merged['BR'])
        merged.loc[mask_to_repair, 'new_Status_P'] = 'Ремонт'
        
        # Остальные -> 'Хранение'
        mask_to_storage2 = mask_explo_to_repair_or_storage & (merged['sne_prev'] >= merged['BR'])
        merged.loc[mask_to_storage2, 'new_Status_P'] = 'Хранение'
        
        # Теперь нужно записать обратно в self.df
        # Для этого можно сделать update через merge
        # merged[['serialno','Dates','new_Status_P']] -> join self.df
        # Или применить индексы, если они совпадают. Проще - ещё раз merge + mask.
        
        # Способ: соберём нужные поля, затем объединим:
        updated = merged[['serialno','Dates','new_Status_P']]
        # left_on='serialno', right_on='serialno'
        
        # Выполним merge с self.df, там, где Dates == curr_date
        # потом присвоим new_Status_P -> self.df['Status_P']
        
        # Но проще делать такую конструкцию:
        self.df = self.df.merge(
            updated.rename(columns={'new_Status_P':'merged_Status_P'}),
            on=['serialno','Dates'],
            how='left'
        )
        # Теперь в self.df есть новый столбец merged_Status_P (NaN, если не было в merge)
        
        # И присваиваем:
        mask_curr = (self.df['Dates'] == curr_date) & (self.df['merged_Status_P'].notnull())
        self.df.loc[mask_curr, 'Status_P'] = self.df.loc[mask_curr, 'merged_Status_P']
        
        # Удалим вспомогательный столбец:
        self.df = self.df.drop('merged_Status_P', axis=1)
    
    def step_2(self, curr_date):
        """
        Аналог CPU: расчёт баланса.
        """
        mask_curr = (self.df['Dates'] == curr_date)
        curr_data = self.df[mask_curr]
        if len(curr_data) == 0:
            return
        
        # Возьмём первое значение mi8t_count, mi17_count
        # (В cudf может быть .iloc, но старайтесь избегать, если много строк. Допустим, тут 1 строка?)
        mi8t_count = curr_data['mi8t_count'].iloc[0]
        mi17_count = curr_data['mi17_count'].iloc[0]
        
        # Считаем balance_mi8t как кол-во Status_P=Эксплуатация & ac_typ=Ми-8Т
        # и т.д.
        balance_mi8t_val = len(curr_data[(curr_data['Status_P'] == 'Эксплуатация') & (curr_data['ac_typ'] == 'Ми-8Т')])
        stock_mi8t_val = len(curr_data[(curr_data['Status_P'] == 'Исправен') & (curr_data['ac_typ'] == 'Ми-8Т')])
        balance_mi17_val = len(curr_data[(curr_data['Status_P'] == 'Эксплуатация') & (curr_data['ac_typ'] == 'Ми-17')])
        stock_mi17_val = len(curr_data[(curr_data['Status_P'] == 'Исправен') & (curr_data['ac_typ'] == 'Ми-17')])
        
        # Пустое ac_typ
        empty_mask = (curr_data['ac_typ'].str.strip() == '') | (curr_data['ac_typ'].str.lower() == 'none')
        balance_empty_val = len(curr_data[(curr_data['Status_P'] == 'Эксплуатация') & empty_mask])
        stock_empty_val = len(curr_data[(curr_data['Status_P'] == 'Исправен') & empty_mask])
        
        final_balance_mi8t = balance_mi8t_val - mi8t_count
        final_balance_mi17 = balance_mi17_val - mi17_count
        final_balance_total = final_balance_mi8t + final_balance_mi17 + balance_empty_val
        
        # Присваиваем всем строкам этого дня
        self.df.loc[mask_curr, 'balance_mi8t'] = final_balance_mi8t
        self.df.loc[mask_curr, 'balance_mi17'] = final_balance_mi17
        self.df.loc[mask_curr, 'balance_empty'] = balance_empty_val
        self.df.loc[mask_curr, 'balance_total'] = final_balance_total
        
        self.df.loc[mask_curr, 'stock_mi8t'] = balance_mi8t_val
        self.df.loc[mask_curr, 'stock_mi17'] = stock_mi17_val
        self.df.loc[mask_curr, 'stock_empty'] = stock_empty_val
        self.df.loc[mask_curr, 'stock_total'] = (balance_mi8t_val + stock_mi17_val + stock_empty_val)
    
    def step_3(self, curr_date):
        """
        Распределяем 'Эксплуатация' <-> 'Исправен' в зависимости от balance_total.
        """
        mask_curr = (self.df['Dates'] == curr_date)
        curr_data = self.df[mask_curr]
        if len(curr_data) == 0:
            return
        
        balance_total = curr_data['balance_total'].iloc[0]
        
        # Сначала все Status = Status_P
        self.df.loc[mask_curr, 'Status'] = self.df.loc[mask_curr, 'Status_P']
        
        if balance_total > 0:
            # перевод части 'Эксплуатация' -> 'Исправен'
            explo_idx = curr_data[curr_data['Status_P'] == 'Эксплуатация'].index
            change_count = min(int(balance_total), len(explo_idx))
            if change_count > 0:
                # Меняем у первых change_count в explo_idx
                idx_to_change = explo_idx[:change_count]
                self.df.loc[idx_to_change, 'Status'] = 'Исправен'
        elif balance_total < 0:
            abs_balance = abs(int(balance_total))
            # перевод 'Исправен' -> 'Эксплуатация'
            serviceable_idx = curr_data[curr_data['Status_P'] == 'Исправен'].index
            serviceable_count = min(abs_balance, len(serviceable_idx))
            if serviceable_count > 0:
                idx_to_change = serviceable_idx[:serviceable_count]
                self.df.loc[idx_to_change, 'Status'] = 'Эксплуатация'
                abs_balance -= serviceable_count
            
            # Если ещё есть остаток
            if abs_balance > 0:
                # перевод 'Неактивно' -> 'Эксплуатация'
                inactive_idx = curr_data[curr_data['Status_P'] == 'Неактивно'].index
                inactive_count = min(abs_balance, len(inactive_idx))
                if inactive_count > 0:
                    idx_to_change2 = inactive_idx[:inactive_count]
                    self.df.loc[idx_to_change2, 'Status'] = 'Эксплуатация'
                    abs_balance -= inactive_count
    
    def step_4(self, prev_date, curr_date):
        """
        Обновление sne/ppr/repair_days и т.д.
        Задача: также сделать максимально векторно.
        """
        prev_data = self.df[self.df['Dates'] == prev_date][['serialno','Status','Status_P','sne','ppr','repair_days']]
        curr_data = self.df[self.df['Dates'] == curr_date][['serialno','Status','Status_P','daily_flight_hours','sne','ppr','repair_days']]
        if len(prev_data) == 0 or len(curr_data) == 0:
            return
        
        merged = curr_data.merge(
            prev_data.rename(columns={
                'Status':'Status_prev',
                'Status_P':'Status_P_prev',
                'sne':'sne_prev',
                'ppr':'ppr_prev',
                'repair_days':'repair_days_prev'
            }),
            on='serialno',
            how='left'
        )
        
        # Создаём новые колонки (new_sne, new_ppr, new_repair)
        merged['new_sne'] = merged['sne']   # default
        merged['new_ppr'] = merged['ppr']
        merged['new_repair_days'] = merged['repair_days']
        
        # Условие: если Status == 'Эксплуатация'
        mask_explo = (merged['Status'] == 'Эксплуатация')
        merged.loc[mask_explo, 'new_sne'] = merged['sne_prev'] + merged['daily_flight_hours']
        merged.loc[mask_explo, 'new_ppr'] = merged['ppr_prev'] + merged['daily_flight_hours']
        
        # Если Status == 'Исправен'
        mask_hbs = (merged['Status'] == 'Исправен')
        # Если раньше был ремонт, теперь исправен => обнуляем ppr, repair_days
        mask_hbs_from_repair = mask_hbs & (merged['Status_P_prev'] == 'Ремонт')
        merged.loc[mask_hbs_from_repair, 'new_sne'] = merged['sne_prev']
        merged.loc[mask_hbs_from_repair, 'new_ppr'] = cp.float32(0.0)
        merged.loc[mask_hbs_from_repair, 'new_repair_days'] = cp.float32(0.0)
        
        # Если Status == 'Ремонт'
        mask_rep = (merged['Status'] == 'Ремонт')
        mask_rep_from_explo = mask_rep & (merged['Status_prev'] == 'Эксплуатация')
        # если вчера была Эксплуатация, значит начинаем repair_days=1
        merged.loc[mask_rep_from_explo, 'new_repair_days'] = cp.float32(1.0)
        
        mask_rep_other = mask_rep & (merged['Status_prev'] != 'Эксплуатация')
        merged.loc[mask_rep_other, 'new_repair_days'] = merged['repair_days_prev'] + cp.float32(1.0)
        
        # Если 'Хранение' или 'Неактивно' - sne/ppr/repair_days = prev
        mask_storage = (merged['Status'].isin(['Хранение','Неактивно']))
        merged.loc[mask_storage, 'new_sne'] = merged['sne_prev']
        merged.loc[mask_storage, 'new_ppr'] = merged['ppr_prev']
        merged.loc[mask_storage, 'new_repair_days'] = merged['repair_days_prev']
        
        # Теперь пишем обратно в self.df
        # Готовим updated
        updated = merged[['serialno','new_sne','new_ppr','new_repair_days']]
        updated = updated.rename(columns={
            'new_sne':'sne',
            'new_ppr':'ppr',
            'new_repair_days':'repair_days'
        })
        
        # merge с self.df
        self.df = self.df.merge(
            updated,
            on='serialno',
            how='left',
            suffixes=('', '_upd')
        )
        
        # Присваиваем только для строк curr_date
        mask_curr_date = (self.df['Dates'] == curr_date) & self.df['sne_upd'].notnull()
        self.df.loc[mask_curr_date, 'sne'] = self.df.loc[mask_curr_date, 'sne_upd']
        self.df.loc[mask_curr_date, 'ppr'] = self.df.loc[mask_curr_date, 'ppr_upd']
        self.df.loc[mask_curr_date, 'repair_days'] = self.df.loc[mask_curr_date, 'repair_days_upd']
        
        # Удаляем временные поля
        self.df.drop(['sne_upd','ppr_upd','repair_days_upd'], axis=1, inplace=True)
    
    def calculate_analytics_metrics(self):
        """
        В целом можно повторить логику CPU-версии.
        Важно делать группировки/merge в cudf без apply().
        """
        if len(self.df) == 0:
            self.logger.warning("GPU: Нет данных для метрик")
            return
        
        period_start = self.first_date
        period_end = period_start + timedelta(days=self.total_days)
        
        # Собираем нужные даты
        dates_unique = self.df['Dates'].unique().sort_values().to_pandas()
        period_dates = [d for d in dates_unique if period_start <= d < period_end]
        
        if len(period_dates) < 2:
            self.logger.warning("GPU: Недостаточно дат для метрик")
            return
        
        self.logger.info(f"GPU: Расчёт метрик для {period_start} - {period_end}")
        
        # Создадим промежуточный словарь {дата -> {field: value}}
        analytics_results = {}
        for i in range(1, len(period_dates)):
            d_curr = period_dates[i]
            d_prev = period_dates[i-1]
            
            df_curr = self.df[self.df['Dates'] == d_curr]
            df_prev = self.df[self.df['Dates'] == d_prev]
            
            ops_count_val = len(df_curr[df_curr['Status'] == 'Эксплуатация'])
            hbs_count_val = len(df_curr[df_curr['Status'] == 'Исправен'])
            repair_count_val = len(df_curr[df_curr['Status'] == 'Ремонт'])
            total_operable_val = ops_count_val + hbs_count_val + repair_count_val
            
            # Сравнение статусов (merge)
            merged = df_prev[['serialno','Status']].merge(
                df_curr[['serialno','Status']],
                on='serialno',
                suffixes=('_prev','_curr'),
                how='inner'
            )
            
            entry_count_val = len(merged[
                (merged['Status_prev'] == 'Неактивно') & (merged['Status_curr'] == 'Эксплуатация')
            ])
            exit_count_val = len(merged[
                (merged['Status_prev'] == 'Эксплуатация') & (merged['Status_curr'] == 'Хранение')
            ])
            into_repair_val = len(merged[
                (merged['Status_prev'] == 'Эксплуатация') & (merged['Status_curr'] == 'Ремонт')
            ])
            complete_repair_val = len(merged[
                (merged['Status_prev'] == 'Ремонт') & (merged['Status_curr'] != 'Ремонт')
            ])
            
            # Остаток
            operating_tech = df_curr[df_curr['Status'].isin(['Эксплуатация','Исправен'])]
            remain_repair_val = ((operating_tech['oh'] - operating_tech['ppr']).fillna(0)).sum()
            
            serviceable_tech = df_curr[df_curr['Status'].isin(['Эксплуатация','Исправен','Ремонт'])]
            remain_val = ((serviceable_tech['ll'] - serviceable_tech['sne']).fillna(0)).sum()
            
            total_ll = serviceable_tech['ll'].fillna(0).sum()
            midlife_val = (remain_val / total_ll) if total_ll > 0 else 0.0
            
            total_oh = operating_tech['oh'].fillna(0).sum()
            midlife_repair_val = (remain_repair_val / total_oh) if total_oh > 0 else 0.0
            
            hours_val = df_curr[df_curr['Status'] == 'Эксплуатация']['daily_flight_hours'].fillna(0).sum()
            
            analytics_results[d_curr] = {
                'ops_count': ops_count_val,
                'hbs_count': hbs_count_val,
                'repair_count': repair_count_val,
                'total_operable': total_operable_val,
                'entry_count': entry_count_val,
                'exit_count': exit_count_val,
                'into_repair': into_repair_val,
                'complete_repair': complete_repair_val,
                'remain_repair': float(remain_repair_val),
                'remain': float(remain_val),
                'midlife_repair': float(midlife_repair_val),
                'midlife': float(midlife_val),
                'hours': float(hours_val)
            }
        
        # Применяем к self.df
        for d in period_dates[1:]:
            if d not in analytics_results:
                continue
            row = analytics_results[d]
            mask = (self.df['Dates'] == d)
            for field in self.analytics_fields:
                val = row[field]
                self.df.loc[mask, field] = val
        
        self.logger.info("GPU: Метрики рассчитаны и записаны в self.df")
    
    def ensure_tmp_table_exists(self):
        drop_ddl = f"DROP TABLE IF EXISTS {self.database_name}.tmp_OlapCube_update"
        self.client.execute(drop_ddl)
        self.logger.info("GPU: Старая временная таблица удалена (если была).")
        
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
        self.logger.info("GPU: Временная таблица tmp_OlapCube_update создана.")
    
    def save_all_results(self, period_start, period_end):
        self.logger.info(f"GPU: Сохранение записей за {period_start} - {period_end}")
        # Фильтруем нужные даты
        mask_period = (self.df['Dates'] >= str(period_start)) & (self.df['Dates'] < str(period_end))
        cycle_df = self.df[mask_period].copy()
        
        # Не обновляем самую первую дату
        # (если логика такая же, как в CPU)
        cycle_df = cycle_df[cycle_df['Dates'] > str(self.first_date)]
        
        # Округления
        # В cudf можно .round(decimals=2), но иногда нужно переводить в float
        cycle_df['sne'] = cycle_df['sne'].round(2).astype('float32')
        cycle_df['ppr'] = cycle_df['ppr'].round(2).astype('float32')
        cycle_df['daily_flight_hours'] = cycle_df['daily_flight_hours'].round(2).astype('float32')
        cycle_df['daily_flight_hours_f'] = cycle_df['daily_flight_hours_f'].round(2).astype('float32')
        
        self.ensure_tmp_table_exists()
        
        # Формируем список колонок
        all_columns = [
            'trigger_type','RepairTime','Status_P','repair_days','sne','ppr','Status',
            'location','ac_typ','daily_flight_hours','daily_flight_hours_f','BR','ll','oh','threshold',
            'Effectivity','serialno','Dates','mi8t_count','mi17_count','balance_mi8t','balance_mi17',
            'balance_empty','balance_total','stock_mi8t','stock_mi17','stock_empty','stock_total'
        ] + self.analytics_fields
        
        df_to_insert = cycle_df[all_columns].fillna(0)  # или нан -> 0
        # Вставка в ClickHouse требует либо list of tuples, либо pandas DataFrame + мы делаем .to_records()
        
        # Переводим cudf -> pandas
        df_insert_pd = df_to_insert.to_pandas()
        records = list(df_insert_pd.itertuples(index=False, name=None))
        
        columns_str = ", ".join(all_columns)
        insert_query = f"""
        INSERT INTO {self.database_name}.tmp_OlapCube_update ({columns_str}) VALUES
        """
        self.client.execute(insert_query, records, settings={'max_threads': 8})
        
        tmp_count = self.client.execute(f"SELECT count(*) FROM {self.database_name}.tmp_OlapCube_update")[0][0]
        self.logger.info(f"GPU: Временная таблица заполнена {tmp_count} записями.")
        
        # Переносим в основную
        insert_main_query = f"""
        INSERT INTO {self.database_name}.OlapCube_VNV ({columns_str})
        SELECT {columns_str}
        FROM {self.database_name}.tmp_OlapCube_update
        """
        self.client.execute(insert_main_query, settings={'max_threads': 8})
        
        self.logger.info("GPU: Записи вставлены в основную таблицу OlapCube_VNV.")
        
        optimize_query = f"OPTIMIZE TABLE {self.database_name}.OlapCube_VNV FINAL"
        self.client.execute(optimize_query)
        self.logger.info("GPU: OPTIMIZE выполнен.")
        
        drop_query = f"DROP TABLE {self.database_name}.tmp_OlapCube_update"
        self.client.execute(drop_query)
        self.logger.info("GPU: Временная таблица удалена.")
        
        # По желанию — проверка
        # ...
    
if __name__ == "__main__":
    processor = CycleProcessorGPU(total_days=4000)
    processor.run_cycle()
