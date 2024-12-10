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

class CycleProcessor:
    def __init__(self):
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
        
        self.temp_df = None
        self.target_date = None
        self.prev_date = None

    def process_step_1(self):
        """Шаг I: Определение Status_P на основе предыдущего состояния"""
        prev_data = self.temp_df[self.temp_df['Dates'] == self.prev_date]
        curr_data = self.temp_df[self.temp_df['Dates'] == self.target_date]
        
        self.logger.info(f"Количество записей на предыдущую дату: {len(prev_data)}")
        self.logger.info(f"Количество записей на текущую дату: {len(curr_data)}")
        
        # Логируем распределение Status на предыдущую дату
        self.logger.info(f"Распределение Status на {self.prev_date.strftime('%Y-%m-%d')}:")
        status_counts = prev_data['Status'].value_counts()
        for status, count in status_counts.items():
            self.logger.info(f"  {status}: {count}")

        working_df = pd.merge(
            curr_data,
            prev_data[['serialno', 'Status', 'Status_P', 'sne', 'ppr', 'repair_days']],
            on='serialno',
            how='left',  # Используем left join чтобы сохранить все записи из curr_data
            suffixes=('', '_prev')
        )
        
        self.logger.info(f"Количество записей после merge: {len(working_df)}")
        
        # Обработка новых записей (которых не было в предыдущий день)
        new_records_mask = working_df['Status_prev'].isna()
        working_df.loc[new_records_mask, 'Status_P'] = 'Неактивно'
        
        # Обработка существующих записей
        existing_records = ~new_records_mask

        # 1. Копирование неизменяемых статусов
        for status in ['Неактивно', 'Хранение', 'Исправен']:
            mask = existing_records & (working_df['Status_prev'] == status)
            working_df.loc[mask, 'Status_P'] = status

        # 2. Обработка статуса "Ремонт"
        repair_mask = existing_records & (working_df['Status_prev'] == 'Ремонт')
        working_df.loc[
            repair_mask & (working_df['repair_days_prev'] < working_df['RepairTime']),
            'Status_P'
        ] = 'Ремонт'
        working_df.loc[
            repair_mask & (working_df['repair_days_prev'] >= working_df['RepairTime']),
            'Status_P'
        ] = 'Исправен'

        # 3. Обработка статуса "Эксплуатация"
        explo_mask = existing_records & (working_df['Status_prev'] == 'Эксплуатация')
        working_df.loc[
            explo_mask & 
            (working_df['sne_prev'] < (working_df['ll'] - working_df['daily_flight_hours'])) &
            (working_df['ppr_prev'] < (working_df['oh'] - working_df['daily_flight_hours'])),
            'Status_P'
        ] = 'Эксплуатация'

        working_df.loc[
            explo_mask & 
            (working_df['sne_prev'] >= (working_df['ll'] - working_df['daily_flight_hours'])),
            'Status_P'
        ] = 'Хранение'

        ppr_mask = explo_mask & (working_df['ppr_prev'] >= (working_df['oh'] - working_df['daily_flight_hours']))
        working_df.loc[
            ppr_mask & (working_df['sne_prev'] < working_df['BR']),
            'Status_P'
        ] = 'Ремонт'
        working_df.loc[
            ppr_mask & (working_df['sne_prev'] >= working_df['BR']),
            'Status_P'
        ] = 'Хранение'

        self.temp_df.loc[self.temp_df['Dates'] == self.target_date, 'Status_P'] = \
            working_df['Status_P'].values

        # Логируем результат после Шага I
        self.logger.info(f"Шаг I завершен для даты {self.target_date.strftime('%Y-%m-%d')}")
        self.logger.info("Распределение Status_P:")
        final_counts = self.temp_df[self.temp_df['Dates'] == self.target_date]['Status_P'].value_counts()
        for status, count in final_counts.items():
            self.logger.info(f"  {status}: {count}")

    def process_step_2(self):
        """Шаг II: Расчет балансов и запасов"""
        curr_data = self.temp_df[self.temp_df['Dates'] == self.target_date].copy()
        
        # Получаем значения счетчиков для текущей даты
        mi8t_count = curr_data['mi8t_count'].iloc[0] if not pd.isna(curr_data['mi8t_count'].iloc[0]) else 0
        mi17_count = curr_data['mi17_count'].iloc[0] if not pd.isna(curr_data['mi17_count'].iloc[0]) else 0

        self.logger.info(f"Начальные счетчики: mi8t_count={mi8t_count}, mi17_count={mi17_count}")

        # 1. Расчеты для Ми-8Т
        balance_mi8t = len(curr_data[
            (curr_data['Status_P'] == 'Эксплуатация') & 
            (curr_data['ac_typ'] == 'Ми-8Т')
        ])
        stock_mi8t = len(curr_data[
            (curr_data['Status_P'] == 'Исправен') & 
            (curr_data['ac_typ'] == 'Ми-8Т')
        ])

        self.logger.info(f"Ми-8Т: balance_raw={balance_mi8t}, stock={stock_mi8t}")

        # 2. Расчеты для Ми-17
        balance_mi17 = len(curr_data[
            (curr_data['Status_P'] == 'Эксплуатация') & 
            (curr_data['ac_typ'] == 'Ми-17')
        ])
        stock_mi17 = len(curr_data[
            (curr_data['Status_P'] == 'Исправен') & 
            (curr_data['ac_typ'] == 'Ми-17')
        ])

        self.logger.info(f"Ми-17: balance_raw={balance_mi17}, stock={stock_mi17}")

        # 3. Расчеты для пустых значений ac_typ
        balance_empty = len(curr_data[
            (curr_data['Status_P'] == 'Эксплуатация') & 
            (curr_data['ac_typ'].isna())
        ])
        stock_empty = len(curr_data[
            (curr_data['Status_P'] == 'Исправен') & 
            (curr_data['ac_typ'].isna())
        ])

        self.logger.info(f"Empty: balance={balance_empty}, stock={stock_empty}")

        # 4. Присваиваем значения
        curr_mask = self.temp_df['Dates'] == self.target_date
        
        # Расчет балансов с учетом счетчиков
        final_balance_mi8t = balance_mi8t - mi8t_count
        final_balance_mi17 = balance_mi17 - mi17_count
        final_balance_total = final_balance_mi8t + final_balance_mi17 + balance_empty
        
        self.logger.info(
            f"Финальные значения балансов:\n"
            f"- balance_mi8t: {final_balance_mi8t} (raw={balance_mi8t} - count={mi8t_count})\n"
            f"- balance_mi17: {final_balance_mi17} (raw={balance_mi17} - count={mi17_count})\n"
            f"- balance_empty: {balance_empty}\n"
            f"- balance_total: {final_balance_total}"
        )

        # Обновляем значения в DataFrame
        self.temp_df.loc[curr_mask, 'balance_mi8t'] = final_balance_mi8t
        self.temp_df.loc[curr_mask, 'balance_mi17'] = final_balance_mi17
        self.temp_df.loc[curr_mask, 'balance_empty'] = balance_empty
        self.temp_df.loc[curr_mask, 'balance_total'] = final_balance_total

        self.temp_df.loc[curr_mask, 'stock_mi8t'] = stock_mi8t
        self.temp_df.loc[curr_mask, 'stock_mi17'] = stock_mi17
        self.temp_df.loc[curr_mask, 'stock_empty'] = stock_empty
        self.temp_df.loc[curr_mask, 'stock_total'] = stock_mi8t + stock_mi17 + stock_empty

        # Выводим состояние DataFrame после обновления
        updated_data = self.temp_df[self.temp_df['Dates'] == self.target_date]
        self.logger.info(
            f"Состояние DataFrame после обновления:\n"
            f"- balance_total: {updated_data['balance_total'].iloc[0]}\n"
            f"- stock_total: {updated_data['stock_total'].iloc[0]}\n"
            f"- Количество записей: {len(updated_data)}"
        )

        self.logger.info(f"Шаг II завершен для даты {self.target_date.strftime('%Y-%m-%d')}")

    def process_step_3(self):
        """Шаг III: Формирование Status на основе balance_total и Status_P.
        Status_P остается неизменным, Status формируется по правилам:
        1. При balance_total > 0: первые balance_total записей с Status_P='Эксплуатация' получают Status='Исправен'
        2. При balance_total < 0: первые |balance_total| записей с Status_P='Исправен' получают Status='Эксплуатация'
        3. Если записей с Status_P='Исправен' недостаточно, берутся записи с Status_P='Неактивно'
        4. Для остальных записей Status = Status_P
        """
        curr_data = self.temp_df[self.temp_df['Dates'] == self.target_date].copy()
        balance_total = curr_data['balance_total'].iloc[0]
        
        # Сначала копируем Status_P в Status для всех записей текущей даты
        curr_mask = self.temp_df['Dates'] == self.target_date
        self.temp_df.loc[curr_mask, 'Status'] = self.temp_df.loc[curr_mask, 'Status_P']
        
        self.logger.info(f"Начало Шага III:")
        self.logger.info(f"balance_total: {balance_total}")
        self.logger.info("Исходное распределение Status_P:")
        for status, count in curr_data['Status_P'].value_counts().items():
            self.logger.info(f"  {status}: {count}")

        indices_to_change = []
        if balance_total > 0:
            # Находим записи с Status_P = 'Эксплуатация'
            exploitation_mask = curr_data['Status_P'] == 'Эксплуатация'
            exploitation_indices = curr_data[exploitation_mask].index.tolist()
            
            # Берем только нужное количество индексов
            indices_to_change = exploitation_indices[:int(balance_total)]
            
            if indices_to_change:
                # Для этих записей Status = 'Исправен' (отличается от их Status_P)
                self.temp_df.loc[indices_to_change, 'Status'] = 'Исправен'
                self.logger.info(f"Изменен Status на 'Исправен' для {len(indices_to_change)} записей с Status_P='Эксплуатация'")

        elif balance_total < 0:
            abs_balance = abs(int(balance_total))
            
            # Находим записи с Status_P = 'Исправен'
            ready_mask = curr_data['Status_P'] == 'Исправен'
            ready_indices = curr_data[ready_mask].index.tolist()
            
            # Берем только нужное количество индексов
            indices_to_change = ready_indices[:abs_balance]
            
            if indices_to_change:
                # Для этих записей Status = 'Эксплуатация' (отличается от их Status_P)
                self.temp_df.loc[indices_to_change, 'Status'] = 'Эксплуатация'
                
                # Если нужно больше записей, берем из Status_P = 'Неактивно'
                if len(indices_to_change) < abs_balance:
                    remaining = abs_balance - len(indices_to_change)
                    
                    # Находим записи с Status_P = 'Неактивно'
                    inactive_mask = curr_data['Status_P'] == 'Неактивно'
                    inactive_indices = curr_data[inactive_mask].index.tolist()
                    additional_indices = inactive_indices[:remaining]
                    
                    if additional_indices:
                        # Для этих записей тоже Status = 'Эксплуатация'
                        self.temp_df.loc[additional_indices, 'Status'] = 'Эксплуатация'
                        indices_to_change.extend(additional_indices)
                        
                        self.logger.info(
                            f"Изменен Status на 'Эксплуатация' для:\n"
                            f"- {len(indices_to_change) - len(additional_indices)} записей с Status_P='Исправен'\n"
                            f"- {len(additional_indices)} записей с Status_P='Неактивно'"
                        )
                else:
                    self.logger.info(f"Изменен Status на 'Эксплуатация' для {len(indices_to_change)} записей с Status_P='Исправен'")
        
        # Выводим итоговое распределение
        curr_data = self.temp_df[self.temp_df['Dates'] == self.target_date]
        self.logger.info("\nИтоговое распределение:")
        self.logger.info("Status_P (не изменился):")
        for status, count in curr_data['Status_P'].value_counts().items():
            self.logger.info(f"  {status}: {count}")
        self.logger.info("Status (новые значения):")
        for status, count in curr_data['Status'].value_counts().items():
            self.logger.info(f"  {status}: {count}")
        self.logger.info(f"Всего изменено записей: {len(indices_to_change)}")

    def process_step_4(self):
        """Обработка счетчиков sne, ppr и repair_days"""
        self.logger.info("\nШаг 4: Обработка счетчиков")
        
        # Получаем данные для текущей даты
        current_data = self.temp_df[self.temp_df['Dates'] == self.target_date].copy()
        
        # Получаем предыдущие значения из базы
        prev_data = pd.DataFrame(columns=['serialno', 'Status', 'Status_P', 'sne', 'ppr', 'repair_days'])
        if self.prev_date is not None:
            prev_query = f"""
                SELECT 
                    serialno,
                    Status,
                    Status_P,
                    sne,
                    ppr,
                    repair_days
                FROM OlapCube_VNV 
                WHERE Dates = '{self.prev_date.strftime('%Y-%m-%d')}'
            """
            prev_data = pd.DataFrame(
                self.client.execute(prev_query),
                columns=['serialno', 'Status', 'Status_P', 'sne', 'ppr', 'repair_days']
            )
        
        # Обрабатываем каждую запись
        for idx, row in current_data.iterrows():
            serialno = row['serialno']
            status = row['Status']
            daily_flight_hours = row['daily_flight_hours']
            
            # Находим предыдущие значения
            prev_row = prev_data[prev_data['serialno'] == serialno]
            
            if not prev_row.empty:
                prev_sne = prev_row['sne'].iloc[0]
                prev_ppr = prev_row['ppr'].iloc[0]
                prev_repair = prev_row['repair_days'].iloc[0]
                prev_status = prev_row['Status'].iloc[0]
                prev_status_p = prev_row['Status_P'].iloc[0]
            else:
                prev_sne = None
                prev_ppr = None
                prev_repair = None
                prev_status = None
                prev_status_p = None
            
            # Логируем значения для отладки
            self.logger.debug(
                f"\nОбработка {serialno}:\n"
                f"  Текущий статус: {status}\n"
                f"  Предыдущий статус: {prev_status}\n"
                f"  Предыдущий Status_P: {prev_status_p}\n"
                f"  daily_flight_hours: {daily_flight_hours}\n"
                f"  Предыдущие значения: sne={prev_sne}, ppr={prev_ppr}, repair={prev_repair}"
            )
            
            # Применяем правила из алгоритма
            if status == 'Эксплуатация':
                # Status (target_date) == "Эксплуатация"
                if prev_sne is not None:
                    self.temp_df.at[idx, 'sne'] = prev_sne + daily_flight_hours
                    self.temp_df.at[idx, 'ppr'] = prev_ppr + daily_flight_hours
                
            elif status == 'Исправен':
                if prev_status_p == 'Ремонт':
                    # Status (target_date) == "Исправен" И Status_P (target_date-1) == "Ремонт"
                    self.temp_df.at[idx, 'sne'] = prev_sne
                    self.temp_df.at[idx, 'ppr'] = 0
                    self.temp_df.at[idx, 'repair_days'] = None
                else:
                    # Status (target_date) == "Исправен" И Status_P (target_date-1) != "Ремонт"
                    self.temp_df.at[idx, 'sne'] = prev_sne
                    self.temp_df.at[idx, 'ppr'] = prev_ppr
                    self.temp_df.at[idx, 'repair_days'] = prev_repair
            
            elif status == 'Ремонт':
                if prev_status == 'Эксплуатация':
                    # Status (target_date) == "Ремонт" И Status_P (target_date-1) == "Эксплуатация"
                    self.temp_df.at[idx, 'sne'] = prev_sne
                    self.temp_df.at[idx, 'ppr'] = prev_ppr
                    self.temp_df.at[idx, 'repair_days'] = 1
                else:
                    # Status (target_date) == "Ремонт" И Status_P (target_date-1) != "Эксплуатация"
                    self.temp_df.at[idx, 'sne'] = prev_sne
                    self.temp_df.at[idx, 'ppr'] = prev_ppr
                    if prev_repair is not None:
                        self.temp_df.at[idx, 'repair_days'] = prev_repair + 1
            
            elif status in ['Хранение', 'Неактивно']:
                # Status (target_date) == "Хранение" или "Неактивно"
                self.temp_df.at[idx, 'sne'] = prev_sne
                self.temp_df.at[idx, 'ppr'] = prev_ppr
                self.temp_df.at[idx, 'repair_days'] = prev_repair
            
            # Логируем новые значения для отладки
            self.logger.debug(
                f"  Новые значения: "
                f"sne={self.temp_df.at[idx, 'sne']}, "
                f"ppr={self.temp_df.at[idx, 'ppr']}, "
                f"repair={self.temp_df.at[idx, 'repair_days']}"
            )

        self.logger.info("Шаг 4 завершен")

    def save_results(self, date: datetime) -> None:
        """Обновление результатов в кубе OlapCube_VNV"""
        data_to_update = self.temp_df[self.temp_df['Dates'] == date].copy()
        
        self.logger.info(f"\nПодготовка данных для сохранения на дату {date.strftime('%Y-%m-%d')}:")
        self.logger.info(f"Количество записей: {len(data_to_update)}")
        
        # Подготовка данных для обновления
        updates = []
        for _, row in data_to_update.iterrows():
            # Проверяем наличие значений перед сохранением
            sne_value = float(row['sne']) if pd.notnull(row['sne']) else None
            ppr_value = float(row['ppr']) if pd.notnull(row['ppr']) else None
            repair_days_value = float(row['repair_days']) if pd.notnull(row['repair_days']) else None
            
            update = {
                'serialno': row['serialno'],
                'Dates': row['Dates'],
                'Status': row['Status'],
                'Status_P': row['Status_P'],
                'sne': sne_value,
                'ppr': ppr_value,
                'repair_days': repair_days_value,
                'balance_mi8t': float(row['balance_mi8t']) if pd.notnull(row['balance_mi8t']) else None,
                'balance_mi17': float(row['balance_mi17']) if pd.notnull(row['balance_mi17']) else None,
                'balance_empty': float(row['balance_empty']) if pd.notnull(row['balance_empty']) else None,
                'balance_total': float(row['balance_total']) if pd.notnull(row['balance_total']) else None,
                'stock_mi8t': float(row['stock_mi8t']) if pd.notnull(row['stock_mi8t']) else None,
                'stock_mi17': float(row['stock_mi17']) if pd.notnull(row['stock_mi17']) else None,
                'stock_empty': float(row['stock_empty']) if pd.notnull(row['stock_empty']) else None,
                'stock_total': float(row['stock_total']) if pd.notnull(row['stock_total']) else None
            }
            
            # Логируем значения счетчиков для отладки
            self.logger.debug(
                f"Запись {row['serialno']}: "
                f"Status={row['Status']}, "
                f"sne={sne_value}, "
                f"ppr={ppr_value}, "
                f"repair_days={repair_days_value}"
            )
            
            updates.append(update)

        # Обновление данных в кубе
        for update in updates:
            query = """
                ALTER TABLE OlapCube_VNV
                UPDATE 
                    Status = %(Status)s,
                    Status_P = %(Status_P)s,
                    sne = %(sne)s,
                    ppr = %(ppr)s,
                    repair_days = %(repair_days)s,
                    balance_mi8t = %(balance_mi8t)s,
                    balance_mi17 = %(balance_mi17)s,
                    balance_empty = %(balance_empty)s,
                    balance_total = %(balance_total)s,
                    stock_mi8t = %(stock_mi8t)s,
                    stock_mi17 = %(stock_mi17)s,
                    stock_empty = %(stock_empty)s,
                    stock_total = %(stock_total)s
                WHERE 
                    serialno = %(serialno)s AND 
                    Dates = %(Dates)s
            """
            try:
                self.client.execute(query, update)
            except Exception as e:
                self.logger.error(f"Ошибка при обновлении записи {update['serialno']} на дату {update['Dates']}: {str(e)}")
                raise

        # Проверяем результат сохранения
        verify_query = f"""
            SELECT 
                Status,
                Status_P,
                sne,
                ppr,
                repair_days,
                balance_total
            FROM OlapCube_VNV FINAL
            WHERE Dates = '{date.strftime('%Y-%m-%d')}'
            LIMIT 5
            SETTINGS optimize_read_in_order=1
        """
        verify_result = self.client.execute(verify_query)
        if verify_result:
            self.logger.info("\nПроверка сохранения (первые 5 записей):")
            fields = ['Status', 'Status_P', 'sne', 'ppr', 'repair_days', 'balance_total']
            for record in verify_result:
                values = dict(zip(fields, record))
                self.logger.info(f"  {values}")

        self.logger.info(f"\nОбновлены данные в кубе для даты {date.strftime('%Y-%m-%d')}: {len(updates)} записей")

    def load_data(self):
        # Загрузка данных из куба
        query = f"""
            WITH
                (
                    SELECT
                        mi8t_count,
                        mi17_count
                    FROM OlapCube_VNV
                    WHERE Dates = '{self.prev_date.strftime('%Y-%m-%d')}'
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
            FROM OlapCube_VNV
            WHERE Dates IN ('{self.prev_date.strftime('%Y-%m-%d')}', '{self.target_date.strftime('%Y-%m-%d')}')
            ORDER BY Dates, serialno
        """
        
        result = self.client.execute(
            query,
            settings={'max_threads': 8}
        )
        
        if not result:
            self.logger.error(f"Нет данных для обработки на даты {self.prev_date.strftime('%Y-%m-%d')} - {self.target_date.strftime('%Y-%m-%d')}")
            raise Exception("Нет данных для обработки")
        
        # Преобразование в DataFrame
        columns = [
            'serialno', 'Dates', 'Status', 'Status_P', 'sne', 'ppr', 'repair_days',
            'll', 'oh', 'BR', 'daily_flight_hours', 'RepairTime', 'ac_typ',
            'mi8t_count', 'mi17_count'
        ]
        
        self.temp_df = pd.DataFrame(result, columns=columns)
        
        # Приведение типов данных
        numeric_columns = ['sne', 'ppr', 'repair_days', 'll', 'oh', 'BR', 
                         'daily_flight_hours', 'RepairTime', 'mi8t_count', 'mi17_count']
        for col in numeric_columns:
            self.temp_df[col] = pd.to_numeric(self.temp_df[col], errors='coerce')

    def load_data_for_dates(self, dates):
        # Загрузка данных из куба
        query = f"""
            WITH
                (
                    SELECT
                        mi8t_count,
                        mi17_count
                    FROM OlapCube_VNV FINAL
                    WHERE Dates = '{dates[0].strftime('%Y-%m-%d')}'
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
            WHERE Dates IN ({', '.join([f"'{date.strftime('%Y-%m-%d')}'" for date in dates])})
            ORDER BY Dates, serialno
            SETTINGS optimize_read_in_order=1
        """
        
        self.logger.info(f"\nЗагрузка данных для дат: {[date.strftime('%Y-%m-%d') for date in dates]}")
        
        result = self.client.execute(
            query,
            settings={'max_threads': 8}
        )
        
        if not result:
            self.logger.error(f"Нет данных для обработки на даты {[date.strftime('%Y-%m-%d') for date in dates]}")
            raise Exception("Нет данных для обработки")
        
        # Преобразование в DataFrame
        columns = [
            'serialno', 'Dates', 'Status', 'Status_P', 'sne', 'ppr', 'repair_days',
            'll', 'oh', 'BR', 'daily_flight_hours', 'RepairTime', 'ac_typ',
            'mi8t_count', 'mi17_count'
        ]
        
        self.temp_df = pd.DataFrame(result, columns=columns)
        
        # Логируем состояние загруженных данных
        for date in dates:
            date_data = self.temp_df[self.temp_df['Dates'] == date]
            self.logger.info(f"\nДанные на {date.strftime('%Y-%m-%d')}:")
            self.logger.info(f"Количество записей: {len(date_data)}")
            self.logger.info("Распределение Status:")
            status_counts = date_data['Status'].value_counts()
            for status, count in status_counts.items():
                self.logger.info(f"  {status}: {count}")
            self.logger.info("Распределение Status_P:")
            status_p_counts = date_data['Status_P'].value_counts()
            for status, count in status_p_counts.items():
                self.logger.info(f"  {status}: {count}")
        
        # Приведение типов данных
        numeric_columns = ['sne', 'ppr', 'repair_days', 'll', 'oh', 'BR', 
                         'daily_flight_hours', 'RepairTime', 'mi8t_count', 'mi17_count']
        for col in numeric_columns:
            self.temp_df[col] = pd.to_numeric(self.temp_df[col], errors='coerce')

    def run_cycle(self):
        """Выполнение цикла обработки для каждой даты"""
        try:
            # Получаем первую дату из куба
            query = "SELECT MIN(Dates) as first_date FROM OlapCube_VNV"
            first_date = self.client.execute(query)[0][0]
            
            # Начинаем со следующего дня после первой даты
            current_date = first_date + timedelta(days=1)
            
            self.logger.info(f"Первая дата в кубе: {first_date.strftime('%Y-%m-%d')}")
            self.logger.info(f"Начинаем обработку с даты: {current_date.strftime('%Y-%m-%d')}")
            
            # Обрабатываем 2 дня
            for day in range(2):  # для теста 2 дня, потом будет больше
                self.target_date = current_date
                self.prev_date = current_date - timedelta(days=1)
                
                self.logger.info(f"\nОбработка даты: {self.target_date.strftime('%Y-%m-%d')}")
                self.logger.info(f"Предыдущая дата: {self.prev_date.strftime('%Y-%m-%d')}")
                
                # Загружаем данные для текущей итерации
                self.load_data_for_dates([self.prev_date, self.target_date])
                
                # Проверяем наличие данных
                curr_data = self.temp_df[self.temp_df['Dates'] == self.target_date]
                prev_data = self.temp_df[self.temp_df['Dates'] == self.prev_date]
                
                if curr_data.empty:
                    self.logger.warning(f"Нет данных для текущей даты {self.target_date.strftime('%Y-%m-%d')}")
                    continue
                    
                if prev_data.empty:
                    self.logger.warning(f"Нет данных для предыдущей даты {self.prev_date.strftime('%Y-%m-%d')}")
                    continue
                
                # Выполняем шаги обработки
                self.process_step_1()
                self.process_step_2()
                self.process_step_3()
                self.process_step_4()
                
                # Сохраняем результаты
                self.save_results(self.target_date)
                
                # Добавляем паузу между днями
                if day < 1:  # Не ждем после последнего дня
                    self.logger.info(f"\nОжидаем 20 секунд перед обработкой следующего дня...")
                    time.sleep(20)
                
                # Переходим к следующей дате
                current_date += timedelta(days=1)
            
            self.logger.info("Цикл обработки завершен")
            
        except Exception as e:
            self.logger.error(f"Ошибка при обработке: {str(e)}")
            raise

# Создание и запуск процессора
if __name__ == "__main__":
    processor = CycleProcessor()
    processor.run_cycle()