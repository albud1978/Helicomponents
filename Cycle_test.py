import pandas as pd
import numpy as np
from clickhouse_driver import Client
from datetime import datetime, timedelta
import logging
import sys
import os
from typing import List, Dict, Any
from dotenv import load_dotenv

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
        
        working_df = pd.merge(
            curr_data,
            prev_data[['serialno', 'Status', 'Status_P', 'sne', 'ppr', 'repair_days']],
            on='serialno',
            suffixes=('', '_prev')
        )

        # 1. Копирование неизменяемых статусов
        for status in ['Неактивно', 'Хранение', 'Исправен']:
            mask = (working_df['Status_prev'] == status)
            working_df.loc[mask, 'Status_P'] = status

        # 2. Обработка статуса "Ремонт"
        repair_mask = (working_df['Status_prev'] == 'Ремонт')
        working_df.loc[
            repair_mask & (working_df['repair_days_prev'] < working_df['RepairTime']),
            'Status_P'
        ] = 'Ремонт'
        working_df.loc[
            repair_mask & (working_df['repair_days_prev'] >= working_df['RepairTime']),
            'Status_P'
        ] = 'Исправен'

        # 3. Обработка статуса "Эксплуатация"
        explo_mask = (working_df['Status_prev'] == 'Эксплуатация')
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

        self.logger.info(f"Шаг I завершен для даты {self.target_date.strftime('%Y-%m-%d')}")

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
        """Шаг III: Корректировка Status_P на основе balance_total"""
        curr_data = self.temp_df[self.temp_df['Dates'] == self.target_date].copy()
        balance_total = curr_data['balance_total'].iloc[0]
        
        self.logger.info(f"Начало Шага III. Исходный balance_total: {balance_total}")

        if balance_total > 0:
            # Получаем индексы записей в статусе 'Эксплуатация'
            exploitation_mask = curr_data['Status_P'] == 'Эксплуатация'
            exploitation_indices = curr_data[exploitation_mask].index.tolist()
            
            # Берем только нужное количество индексов
            indices_to_change = exploitation_indices[:int(balance_total)]
            
            if indices_to_change:
                self.temp_df.loc[indices_to_change, 'Status_P'] = 'Исправен'
                self.logger.info(f"Корректировка избытка: {len(indices_to_change)} записей из 'Эксплуатация' в 'Исправен'")

        elif balance_total < 0:
            abs_balance = abs(int(balance_total))
            
            # Получаем индексы записей в статусе 'Исправен'
            ready_mask = curr_data['Status_P'] == 'Исправен'
            ready_indices = curr_data[ready_mask].index.tolist()
            
            # Берем только нужное количество индексов
            indices_to_change = ready_indices[:abs_balance]
            
            if indices_to_change:
                self.temp_df.loc[indices_to_change, 'Status_P'] = 'Эксплуатация'
                
                # Если нужно больше записей, берем из 'Неактивно'
                if len(indices_to_change) < abs_balance:
                    remaining = abs_balance - len(indices_to_change)
                    
                    # Получаем индексы записей в статусе 'Неактивно'
                    inactive_mask = curr_data['Status_P'] == 'Неактивно'
                    inactive_indices = curr_data[inactive_mask].index.tolist()
                    additional_indices = inactive_indices[:remaining]
                    
                    if additional_indices:
                        self.temp_df.loc[additional_indices, 'Status_P'] = 'Эксплуатация'
                        
                        self.logger.info(
                            f"Корректировка недостатка: переведено в 'Эксплуатация':\n"
                            f"- из 'Исправен': {len(indices_to_change)} записей\n"
                            f"- из 'Неактивно': {len(additional_indices)} записей"
                        )
                else:
                    self.logger.info(f"Корректировка недостатка: {len(indices_to_change)} записей из 'Исправен' в 'Эксплуатация'")

        # Контрольный пересчет балансов
        self.process_step_2()
        
        final_balance = self.temp_df[self.temp_df['Dates'] == self.target_date]['balance_total'].iloc[0]
        self.logger.info(f"Завершение Шага III. Итоговый balance_total: {final_balance}")

    def process_step_4(self):
        """Шаг IV: Обновление счетчиков"""
        curr_data = self.temp_df[self.temp_df['Dates'] == self.target_date]
        prev_data = self.temp_df[self.temp_df['Dates'] == self.prev_date][['serialno', 'sne', 'ppr', 'repair_days', 'Status', 'Status_P']]
        
        working_df = pd.merge(
            curr_data,
            prev_data,
            on='serialno',
            suffixes=('', '_prev')
        )

        # 1. Status = "Эксплуатация"
        exploitation_mask = working_df['Status'] == 'Эксплуатация'
        working_df.loc[exploitation_mask, 'sne'] = working_df.loc[exploitation_mask, 'sne_prev'] + \
                                                  working_df.loc[exploitation_mask, 'daily_flight_hours']
        working_df.loc[exploitation_mask, 'ppr'] = working_df.loc[exploitation_mask, 'ppr_prev'] + \
                                                  working_df.loc[exploitation_mask, 'daily_flight_hours']

        # 2. Status = "Исправен" и Status_P (t-1) = "Ремонт"
        ready_after_repair_mask = (working_df['Status'] == 'Исправен') & \
                                 (working_df['Status_P_prev'] == 'Ремонт')
        working_df.loc[ready_after_repair_mask, 'sne'] = working_df.loc[ready_after_repair_mask, 'sne_prev']
        working_df.loc[ready_after_repair_mask, 'ppr'] = 0
        working_df.loc[ready_after_repair_mask, 'repair_days'] = None

        # 3. Status = "Исправен" и Status_P (t-1) != "Ремонт"
        ready_not_from_repair_mask = (working_df['Status'] == 'Исправен') & \
                                    (working_df['Status_P_prev'] != 'Ремонт')
        working_df.loc[ready_not_from_repair_mask, 'sne'] = working_df.loc[ready_not_from_repair_mask, 'sne_prev']
        working_df.loc[ready_not_from_repair_mask, 'ppr'] = working_df.loc[ready_not_from_repair_mask, 'ppr_prev']

        # 4. Status = "Ремонт" и Status_P (t-1) = "Эксплуатация"
        repair_from_exploitation_mask = (working_df['Status'] == 'Ремонт') & \
                                      (working_df['Status_P_prev'] == 'Эксплуатация')
        working_df.loc[repair_from_exploitation_mask, 'sne'] = working_df.loc[repair_from_exploitation_mask, 'sne_prev']
        working_df.loc[repair_from_exploitation_mask, 'ppr'] = working_df.loc[repair_from_exploitation_mask, 'ppr_prev']
        working_df.loc[repair_from_exploitation_mask, 'repair_days'] = 1

        # 5. Status = "Ремонт" и Status_P (t-1) != "Эксплуатация"
        repair_not_from_exploitation_mask = (working_df['Status'] == 'Ремонт') & \
                                          (working_df['Status_P_prev'] != 'Эксплуатация')
        working_df.loc[repair_not_from_exploitation_mask, 'sne'] = working_df.loc[repair_not_from_exploitation_mask, 'sne_prev']
        working_df.loc[repair_not_from_exploitation_mask, 'ppr'] = working_df.loc[repair_not_from_exploitation_mask, 'ppr_prev']
        working_df.loc[repair_not_from_exploitation_mask, 'repair_days'] = \
            working_df.loc[repair_not_from_exploitation_mask, 'repair_days_prev'] + 1

        # 6. Status = "Хранение" или "Неактивно"
        storage_inactive_mask = working_df['Status'].isin(['Хранение', 'Неактивно'])
        working_df.loc[storage_inactive_mask, 'sne'] = working_df.loc[storage_inactive_mask, 'sne_prev']
        working_df.loc[storage_inactive_mask, 'ppr'] = working_df.loc[storage_inactive_mask, 'ppr_prev']

        # Обновляем значения в основной таблице
        update_columns = ['sne', 'ppr', 'repair_days']
        self.temp_df.loc[self.temp_df['Dates'] == self.target_date, update_columns] = working_df[update_columns]

        self.logger.info(f"Шаг IV завершен для даты {self.target_date.strftime('%Y-%m-%d')}")

    def save_results(self, date: datetime) -> None:
        """Обновление результатов в кубе OlapCube_VNV"""
        data_to_update = self.temp_df[self.temp_df['Dates'] == date].copy()
        
        # Подготовка данных для обновления
        updates = []
        for _, row in data_to_update.iterrows():
            updates.append({
                'serialno': row['serialno'],
                'Dates': row['Dates'],
                'Status_P': row['Status_P'],
                'sne': float(row['sne']) if pd.notnull(row['sne']) else None,
                'ppr': float(row['ppr']) if pd.notnull(row['ppr']) else None,
                'repair_days': float(row['repair_days']) if pd.notnull(row['repair_days']) else None,
                'balance_mi8t': float(row['balance_mi8t']) if pd.notnull(row['balance_mi8t']) else None,
                'balance_mi17': float(row['balance_mi17']) if pd.notnull(row['balance_mi17']) else None,
                'balance_empty': float(row['balance_empty']) if pd.notnull(row['balance_empty']) else None,
                'balance_total': float(row['balance_total']) if pd.notnull(row['balance_total']) else None,
                'stock_mi8t': float(row['stock_mi8t']) if pd.notnull(row['stock_mi8t']) else None,
                'stock_mi17': float(row['stock_mi17']) if pd.notnull(row['stock_mi17']) else None,
                'stock_empty': float(row['stock_empty']) if pd.notnull(row['stock_empty']) else None,
                'stock_total': float(row['stock_total']) if pd.notnull(row['stock_total']) else None
            })

        # Обновление данных в кубе
        for update in updates:
            query = """
                ALTER TABLE OlapCube_VNV
                UPDATE 
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

        self.logger.info(f"Обновлены данные в кубе для даты {date.strftime('%Y-%m-%d')}: {len(updates)} записей")

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

    def run_cycle(self):
        """Основной метод для запуска цикла обработки данных"""
        try:
            # Получаем первую дату из куба
            query = "SELECT MIN(Dates) as first_date FROM OlapCube_VNV"
            first_date = self.client.execute(query)[0][0]
            
            # Начинаем со следующего дня после первой даты
            current_date = first_date + timedelta(days=1)
            
            self.logger.info(f"Первая дата в кубе: {first_date.strftime('%Y-%m-%d')}")
            self.logger.info(f"Начинаем обработку с даты: {current_date.strftime('%Y-%m-%d')}")
            
            # Обрабатываем 2 дня
            for day in range(2):
                self.target_date = current_date
                self.prev_date = current_date - timedelta(days=1)
                
                self.logger.info(f"\n{'='*80}")
                self.logger.info(f"Обработка даты: {self.target_date.strftime('%Y-%m-%d')}")
                self.logger.info(f"Предыдущая дата: {self.prev_date.strftime('%Y-%m-%d')}")
                
                # Загружаем данные
                self.load_data()
                
                # Логируем начальное состояние
                prev_data = self.temp_df[self.temp_df['Dates'] == self.prev_date]
                curr_data = self.temp_df[self.temp_df['Dates'] == self.target_date]
                
                self.logger.info(f"\n--- Исходные данные ---")
                # Status на предыдущую дату
                self.logger.info(f"Status на {self.prev_date.strftime('%Y-%m-%d')}:")
                status_counts = prev_data['Status'].value_counts()
                for status, count in status_counts.items():
                    self.logger.info(f"  {status}: {count}")
                
                # Status_P на текущую дату (до обработки)
                self.logger.info(f"\nStatus_P на {self.target_date.strftime('%Y-%m-%d')} (до обработки):")
                status_p_counts = curr_data['Status_P'].value_counts()
                for status, count in status_p_counts.items():
                    self.logger.info(f"  {status}: {count}")
                
                # Шаг 1: Status -> Status_P
                self.logger.info(f"\n--- Шаг I: Status({self.prev_date.strftime('%Y-%m-%d')}) -> Status_P({self.target_date.strftime('%Y-%m-%d')}) ---")
                self.process_step_1()
                curr_data = self.temp_df[self.temp_df['Dates'] == self.target_date]
                self.logger.info("Status_P после Шага I:")
                status_p_counts = curr_data['Status_P'].value_counts()
                for status, count in status_p_counts.items():
                    self.logger.info(f"  {status}: {count}")
                
                # Шаг 2: Расчет балансов
                self.logger.info(f"\n--- Шаг II: Расчет balance_total ---")
                self.process_step_2()
                curr_data = self.temp_df[self.temp_df['Dates'] == self.target_date]
                self.logger.info("Значения счетчиков:")
                self.logger.info(f"  mi8t_count: {curr_data['mi8t_count'].iloc[0]}")
                self.logger.info(f"  mi17_count: {curr_data['mi17_count'].iloc[0]}")
                self.logger.info("Расчет балансов:")
                self.logger.info(f"  balance_mi8t: {curr_data['balance_mi8t'].iloc[0]}")
                self.logger.info(f"  balance_mi17: {curr_data['balance_mi17'].iloc[0]}")
                self.logger.info(f"  balance_empty: {curr_data['balance_empty'].iloc[0]}")
                self.logger.info(f"  balance_total: {curr_data['balance_total'].iloc[0]}")
                
                # Шаг 3: Корректировка Status_P
                self.logger.info(f"\n--- Шаг III: Корректировка Status_P на основе balance_total ---")
                self.process_step_3()
                curr_data = self.temp_df[self.temp_df['Dates'] == self.target_date]
                self.logger.info("Status_P после корректировки:")
                status_p_counts = curr_data['Status_P'].value_counts()
                for status, count in status_p_counts.items():
                    self.logger.info(f"  {status}: {count}")
                
                # Шаг 4: Расчет показателей
                self.logger.info(f"\n--- Шаг IV: Расчет sne, ppr, repair_days ---")
                self.process_step_4()
                curr_data = self.temp_df[self.temp_df['Dates'] == self.target_date]
                self.logger.info("Значения показателей (не NULL):")
                self.logger.info(f"  sne > 0: {len(curr_data[curr_data['sne'] > 0])}")
                self.logger.info(f"  ppr > 0: {len(curr_data[curr_data['ppr'] > 0])}")
                self.logger.info(f"  repair_days > 0: {len(curr_data[curr_data['repair_days'] > 0])}")
                
                # Сохранение результатов
                self.logger.info(f"\n--- Сохранение результатов ---")
                self.save_results(self.target_date)
                
                # Переходим к следующему дню
                current_date += timedelta(days=1)
                
            self.logger.info(f"\nОбработка данных завершена. Обработано дней: 2")
            
        except Exception as e:
            self.logger.error(f"Ошибка при обработке даты {self.target_date}: {str(e)}")
            raise

# Создание и запуск процессора
processor = CycleProcessor()
processor.run_cycle() 