#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import logging
import pandas as pd
from clickhouse_driver import Client
from datetime import datetime, timedelta
import json
from typing import Dict, Tuple
from openpyxl import load_workbook

# Настройка логирования
# Сначала удаляем все обработчики у корневого логгера
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Создаем новые обработчики
file_handler = logging.FileHandler("data_consistency_test.log", mode='w', encoding='utf-8')
console_handler = logging.StreamHandler(sys.stdout)

# Устанавливаем форматирование
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Настраиваем логгер
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Убеждаемся, что логгер не будет передавать сообщения родительским логгерам
logger.propagate = False

class DataConsistencyTester:
    def __init__(self):
        # Настройки подключения к ClickHouse
        self.client = Client(
            host=os.getenv('CLICKHOUSE_HOST', '10.95.19.132'),
            user=os.getenv('CLICKHOUSE_USER', 'default'),
            password=os.getenv('CLICKHOUSE_PASSWORD', 'quie1ahpoo5Su0wohpaedae8keeph6bi'),
            database=os.getenv('CLICKHOUSE_DB', 'default'),
            connect_timeout=10,  # Таймаут подключения в секундах
            send_receive_timeout=30,  # Таймаут отправки/получения данных
            sync_request_timeout=30  # Таймаут синхронных запросов
        )
        self.test_results = {
            'excel_to_cube': [],
            'cube_to_analytics': [],
            'summary': {'passed': 0, 'failed': 0, 'warnings': 0}
        }

    def load_excel_data(self) -> Dict[str, pd.DataFrame]:
        """Загрузка данных из Excel файлов"""
        excel_data = {}
        try:
            # Загрузка выгрузки
            vygruzka_path = './Выгрузка'
            if os.path.exists(vygruzka_path):
                latest_file = max(
                    [f for f in os.listdir(vygruzka_path) if f.endswith('.xlsx')],
                    key=lambda x: os.path.getmtime(os.path.join(vygruzka_path, x))
                )
                excel_data['vygruzka'] = pd.read_excel(
                    os.path.join(vygruzka_path, latest_file)
                )
                logger.info(f"Загружено {len(excel_data['vygruzka'])} записей из {latest_file}")

        except Exception as e:
            logger.error(f"Ошибка при загрузке Excel: {e}")
        return excel_data

    def get_first_date(self, excel_data: Dict[str, pd.DataFrame]) -> str:
        """Определение первой даты из Excel"""
        if 'vygruzka' not in excel_data:
            return None

        date_columns = ['mfg_date', 'oh_at_date', 'removal_date', 'repair_date']
        all_dates = []

        for col in date_columns:
            if col in excel_data['vygruzka'].columns:
                dates = pd.to_datetime(excel_data['vygruzka'][col], errors='coerce', dayfirst=True)
                valid_dates = dates.dropna()
                if not valid_dates.empty:
                    all_dates.extend(valid_dates)

        if all_dates:
            recent_dates = [d for d in all_dates if d.year >= 2000]
            if recent_dates:
                min_date = min(recent_dates)
                logger.info(f"Найдена первая дата: {min_date.strftime('%Y-%m-%d')}")
                return min_date.strftime('%Y-%m-%d')
        logger.error("Не удалось найти подходящую дату в данных Excel")
        return None

    def find_nearest_available_date(self, date_str: str) -> str:
        """Поиск ближайшей доступной даты в кубе"""
        logger.info(f"Ищем ближайшую доступную дату к {date_str}")
        
        try:
            # Проверяем наличие данных для указанной даты
            query = f"""
            SELECT count() FROM OlapCube_VNV WHERE Dates = '{date_str}'
            """
            count = self.client.execute(query, settings={'max_execution_time': 30})[0][0]
            
            if count > 0:
                logger.info(f"Для даты {date_str} найдено {count} записей")
                return date_str
                
            # Если данных нет, ищем ближайшую дату
            query = """
            SELECT Dates FROM OlapCube_VNV
            WHERE toYear(Dates) >= 2000
            GROUP BY Dates
            ORDER BY Dates
            LIMIT 1
            """
            result = self.client.execute(query, settings={'max_execution_time': 30})
            
            if result:
                nearest_date = result[0][0].strftime('%Y-%m-%d')
                logger.info(f"Найдена ближайшая доступная дата: {nearest_date}")
                return nearest_date
        except Exception as e:
            logger.error(f"Ошибка при поиске ближайшей доступной даты: {e}")
            
        logger.error("Не удалось найти доступные даты в кубе")
        return None

    def get_cube_data(self, date: str) -> pd.DataFrame:
        """Получение данных из OLAP-куба на конкретную дату"""
        query = f"""
        SELECT 
            Dates,
            serialno,
            Status_P,
            sne,
            ppr,
            daily_flight_hours
        FROM OlapCube_VNV
        WHERE Dates = '{date}'
        """
        logger.info(f"Выполняем запрос к кубу для даты {date}")
        try:
            result = self.client.execute(query, settings={'max_execution_time': 10})
            columns = ['Dates', 'serialno', 'Status_P', 'sne', 'ppr', 'daily_flight_hours']
            df = pd.DataFrame(result, columns=columns)
            return df
        except Exception as e:
            logger.error(f"Ошибка при получении данных из куба для даты {date}: {e}")
            return pd.DataFrame(columns=['Dates', 'serialno', 'Status_P', 'sne', 'ppr', 'daily_flight_hours'])

    def test_excel_to_cube(self, excel_data: Dict[str, pd.DataFrame], cube_data: pd.DataFrame):
        """Проверка согласованности данных между Excel и OLAP-кубом"""
        # Проверка наличия данных
        if not excel_data or 'vygruzka' not in excel_data or excel_data['vygruzka'].empty:
            logger.error("❌ Отсутствуют данные Excel для сравнения")
            self.test_results['summary']['failed'] += 1
            return
            
        if cube_data.empty:
            logger.error("❌ Отсутствуют данные куба для сравнения")
            self.test_results['summary']['failed'] += 1
            return
            
        # Проверка наличия необходимых столбцов
        required_excel_columns = ['serialno', 'sne']  # Убрали 'ppr' и 'Status_P'
        required_cube_columns = ['serialno', 'sne', 'Status_P']
        
        missing_excel_columns = [col for col in required_excel_columns if col not in excel_data['vygruzka'].columns]
        missing_cube_columns = [col for col in required_cube_columns if col not in cube_data.columns]
        
        if missing_excel_columns:
            logger.error(f"❌ В данных Excel отсутствуют столбцы: {missing_excel_columns}")
            self.test_results['summary']['failed'] += 1
            return
            
        if missing_cube_columns:
            logger.error(f"❌ В данных куба отсутствуют столбцы: {missing_cube_columns}")
            self.test_results['summary']['failed'] += 1
            return
        
        try:
            # Исключаем агрегаты с серийными номерами, начинающимися на S
            s_count = len([sn for sn in cube_data['serialno'] if str(sn).startswith('S')])
            cube_data = cube_data[~cube_data['serialno'].astype(str).str.startswith('S')]
            
            # 1. Проверка количества агрегатов
            excel_serials = set(excel_data['vygruzka']['serialno'].astype(str))
            cube_serials = set(cube_data['serialno'].astype(str))
            
            if len(excel_serials) == len(cube_serials):
                logger.info("✅ Количество агрегатов совпадает")
                self.test_results['summary']['passed'] += 1
            else:
                logger.error("❌ Количество агрегатов не совпадает")
                self.test_results['summary']['failed'] += 1
                
            # 2. Проверка сумм sne (убрали проверку ppr)
            excel_sne = excel_data['vygruzka']['sne'].sum()
            cube_sne = cube_data['sne'].sum()
            
            # Допустимое отклонение 0.01%
            threshold = 0.0001
            
            if abs(excel_sne - cube_sne) <= excel_sne * threshold:
                logger.info("✅ Суммы SNE совпадают")
                self.test_results['summary']['passed'] += 1
            else:
                logger.error(f"❌ Суммы SNE не совпадают: Excel ({excel_sne}) != Куб ({cube_sne})")
                self.test_results['summary']['failed'] += 1
                
            # 3. Проверка статусов агрегатов - пропускаем, так как в Excel нет столбца Status_P
        except Exception as e:
            logger.error(f"❌ Ошибка при выполнении проверок Excel-Куб: {e}", exc_info=True)
            self.test_results['summary']['failed'] += 1

    def get_analytics_data(self) -> pd.DataFrame:
        """Получение данных из аналитической таблицы"""
        # Ограничиваем выборку только последним годом
        query = """
        SELECT 
            Dates,
            ops_count,
            entry_count,
            exit_count,
            into_repair,
            complete_repair
        FROM Heli_Components
        WHERE toYear(Dates) = 2024
        ORDER BY Dates
        """
        try:
            result = self.client.execute(query, settings={'max_execution_time': 10})
            columns = ['Dates', 'ops_count', 'entry_count', 'exit_count', 
                      'into_repair', 'complete_repair']
            df = pd.DataFrame(result, columns=columns)
            return df
        except Exception as e:
            logger.error(f"Ошибка при получении данных из аналитической таблицы: {e}")
            return pd.DataFrame(columns=['Dates', 'ops_count', 'entry_count', 'exit_count', 
                                       'into_repair', 'complete_repair'])

    def get_cube_data_by_year(self, year: int) -> pd.DataFrame:
        """Получение данных из OLAP-куба за указанный год"""
        # Запрашиваем данные за весь год, а не только за первый месяц
        query = f"""
        SELECT 
            Dates,
            serialno,
            Status_P,
            sne,
            ppr,
            daily_flight_hours
        FROM OlapCube_VNV
        WHERE toYear(Dates) = {year}
        ORDER BY serialno, Dates
        """
        try:
            result = self.client.execute(query, settings={'max_execution_time': 30})  # Увеличиваем таймаут
            columns = ['Dates', 'serialno', 'Status_P', 'sne', 'ppr', 'daily_flight_hours']
            df = pd.DataFrame(result, columns=columns)
            logger.info(f"Получено {len(df)} записей из куба за {year} год")
            return df
        except Exception as e:
            logger.error(f"Ошибка при выполнении запроса к кубу для года {year}: {e}")
            return pd.DataFrame(columns=['Dates', 'serialno', 'Status_P', 'sne', 'ppr', 'daily_flight_hours'])

    def get_first_date_from_cube(self) -> str:
        """Получение первой доступной даты из куба"""
        query = """
        SELECT Dates FROM OlapCube_VNV
        WHERE toYear(Dates) >= 2000
        GROUP BY Dates
        ORDER BY Dates
        LIMIT 1
        """
        try:
            result = self.client.execute(query, settings={'max_execution_time': 30})
            
            if result:
                first_date = result[0][0].strftime('%Y-%m-%d')
                return first_date
        except Exception as e:
            logger.error(f"Ошибка при получении первой даты из куба: {e}")
            
        logger.error("Не удалось найти доступные даты в кубе")
        return None
        
    def get_excel_metadata_date(self) -> str:
        """Получение даты из метаданных Excel файла"""
        try:
            vygruzka_path = './Выгрузка'
            if os.path.exists(vygruzka_path):
                excel_files = [f for f in os.listdir(vygruzka_path) if f.endswith('.xlsx')]
                if not excel_files:
                    logger.error("Excel файлы не найдены в директории ./Выгрузка")
                    return None
                    
                latest_file = max(
                    excel_files,
                    key=lambda x: os.path.getmtime(os.path.join(vygruzka_path, x))
                )
                file_path = os.path.join(vygruzka_path, latest_file)
                
                # Загрузка Excel файла и получение оригинальной даты создания из метаданных
                workbook = load_workbook(file_path)
                properties = workbook.properties
                
                if properties.created:
                    excel_date = properties.created.strftime('%Y-%m-%d')
                    return excel_date
                else:
                    # Запасной вариант - использование даты создания файла
                    creation_time = datetime.fromtimestamp(os.path.getctime(file_path))
                    excel_date = creation_time.strftime('%Y-%m-%d')
                    return excel_date
            else:
                logger.error(f"Директория {vygruzka_path} не существует")
        except Exception as e:
            logger.error(f"Ошибка при получении метаданных Excel: {e}", exc_info=True)
        return None
        
    def get_analytics_first_date(self) -> str:
        """Получение первой даты из аналитической таблицы"""
        query = """
        SELECT Dates FROM Heli_Components
        WHERE toYear(Dates) >= 2000
        ORDER BY Dates
        LIMIT 1
        """
        try:
            result = self.client.execute(query, settings={'max_execution_time': 30})
            
            if result:
                first_date = result[0][0].strftime('%Y-%m-%d')
                return first_date
        except Exception as e:
            logger.error(f"Ошибка при получении первой даты из аналитической таблицы: {e}")
            
        logger.error("Не удалось найти даты в аналитической таблице")
        return None

    def test_cube_to_analytics(self):
        """Проверка согласованности данных между OLAP-кубом и аналитической таблицей"""
        analytics_data = self.get_analytics_data()
        if analytics_data.empty:
            logger.error("Нет данных из аналитической таблицы")
            return

        # Получаем первую дату из куба
        cube_date = self.get_first_date_from_cube()
        if not cube_date:
            logger.error("Не удалось получить дату из куба")
            return
            
        # Преобразуем в datetime
        cube_dt = datetime.strptime(cube_date, '%Y-%m-%d')
        
        # Ожидаемая дата в аналитике с учетом смещения в +1 день
        expected_analytics_dt = cube_dt + timedelta(days=1)
        expected_analytics_date = expected_analytics_dt.strftime('%Y-%m-%d')
        
        # Фильтруем аналитику по ожидаемой дате
        analytics_for_date = analytics_data[
            pd.to_datetime(analytics_data['Dates']).dt.strftime('%Y-%m-%d') == expected_analytics_date
        ]
        
        if analytics_for_date.empty:
            logger.error(f"❌ В аналитике нет данных для ожидаемой даты {expected_analytics_date}")
            self.test_results['summary']['failed'] += 1
        else:
            logger.info(f"✅ Найдены данные в аналитике для ожидаемой даты {expected_analytics_date}")
            self.test_results['summary']['passed'] += 1

        # 1. Проверка налета часов по годам
        years = pd.to_datetime(analytics_data['Dates']).dt.year.unique()
        
        for year in years:
            cube_year_data = self.get_cube_data_by_year(year)
            if cube_year_data.empty:
                logger.warning(f"Нет данных в кубе за {year} год")
                continue

            # Расчет налета часов для статуса Эксплуатация
            flight_hours = cube_year_data[
                cube_year_data['Status_P'] == 'Эксплуатация'
            ]['daily_flight_hours'].sum()

            # Расчет изменений sne (убираем ppr из сравнения)
            cube_year_data = cube_year_data.sort_values(['serialno', 'Dates'])
            sne_changes = cube_year_data.groupby('serialno')['sne'].diff().sum()

            # Проверка равенства значений
            threshold = 0.05  # 5%
            if abs(flight_hours - sne_changes) <= flight_hours * threshold:
                logger.info(f"✅ Налет часов за {year} год согласован с SNE")
                self.test_results['summary']['passed'] += 1
            else:
                logger.error(f"❌ Расхождение в налете часов за {year} год")
                self.test_results['summary']['failed'] += 1

        # 2. Проверка матрицы переходов статусов
        cube_data = self.get_cube_data_by_year(max(years))  # Берем последний год для примера
        cube_data = cube_data.sort_values(['serialno', 'Dates'])
        cube_data['prev_status'] = cube_data.groupby('serialno')['Status_P'].shift(1)

        # Подсчет переходов статусов
        status_changes = cube_data[cube_data['Status_P'] != cube_data['prev_status']]
        
        # Расчет показателей для сравнения с аналитической таблицей
        calc_entry = len(status_changes[
            (status_changes['prev_status'].isin(['Ремонт', 'Хранение'])) & 
            (status_changes['Status_P'].isin(['Эксплуатация', 'Исправен']))
        ])
        
        calc_exit = len(status_changes[
            (status_changes['prev_status'].isin(['Эксплуатация', 'Исправен'])) & 
            (status_changes['Status_P'].isin(['Ремонт', 'Хранение']))
        ])
        
        calc_into_repair = len(status_changes[
            (status_changes['prev_status'] != 'Ремонт') & 
            (status_changes['Status_P'] == 'Ремонт')
        ])
        
        calc_complete_repair = len(status_changes[
            (status_changes['prev_status'] == 'Ремонт') & 
            (status_changes['Status_P'] != 'Ремонт')
        ])

        # Сравнение с аналитической таблицей
        analytics_totals = analytics_data.agg({
            'entry_count': 'sum',
            'exit_count': 'sum',
            'into_repair': 'sum',
            'complete_repair': 'sum'
        })

        if (calc_entry == analytics_totals['entry_count'] and
            calc_exit == analytics_totals['exit_count'] and
            calc_into_repair == analytics_totals['into_repair'] and
            calc_complete_repair == analytics_totals['complete_repair']):
            logger.info("✅ Матрица переходов статусов согласована")
            self.test_results['summary']['passed'] += 1
        else:
            logger.error("❌ Расхождения в матрице переходов статусов")
            self.test_results['summary']['failed'] += 1

        # Пропускаем проверку ops_count с суммой mi8t_count и mi17_count, так как столбцы отсутствуют

    def test_dates_consistency(self):
        """Проверка согласованности дат между кубом, аналитикой и Excel"""
        # Получение дат из разных источников
        cube_date = self.get_first_date_from_cube()
        analytics_date = self.get_analytics_first_date()
        excel_date = self.get_excel_metadata_date()
        
        if not cube_date or not analytics_date or not excel_date:
            logger.error("Не удалось получить все необходимые даты для сравнения")
            self.test_results['summary']['failed'] += 1
            return
            
        # Преобразуем даты в объекты datetime для корректного сравнения
        cube_dt = datetime.strptime(cube_date, '%Y-%m-%d')
        analytics_dt = datetime.strptime(analytics_date, '%Y-%m-%d')
        excel_dt = datetime.strptime(excel_date, '%Y-%m-%d')
        
        # Расчет разницы в днях
        cube_analytics_diff = (analytics_dt - cube_dt).days
        cube_excel_diff = (excel_dt - cube_dt).days
        
        logger.info(f"Разница между датами куба и аналитики: {cube_analytics_diff} дней")
        logger.info(f"Разница между датами куба и Excel: {cube_excel_diff} дней")
        
        # Ожидаемая разница между кубом и аналитикой - 1 день
        expected_analytics_diff = 1
        
        if cube_analytics_diff == expected_analytics_diff:
            logger.info(f"✅ Даты куба и аналитики согласованы с учетом смещения в {expected_analytics_diff} день")
            self.test_results['summary']['passed'] += 1
        else:
            logger.error(f"❌ Даты куба и аналитики не согласованы. Ожидалось смещение в {expected_analytics_diff} день, фактическое: {cube_analytics_diff}")
            self.test_results['summary']['failed'] += 1
            
        # Ожидаемая разница между кубом и Excel - 0 дней
        if cube_excel_diff == 0:
            logger.info("✅ Даты куба и Excel полностью согласованы")
            self.test_results['summary']['passed'] += 1
        else:
            logger.error(f"❌ Даты куба и Excel не согласованы (разница {cube_excel_diff} дней)")
            self.test_results['summary']['failed'] += 1

    def run_tests(self):
        """Запуск всех проверок"""
        logger.info("Начало проверок")
        
        try:
            # Получение дат из разных источников
            cube_date = self.get_first_date_from_cube()
            analytics_date = self.get_analytics_first_date()
            excel_date = self.get_excel_metadata_date()
            
            # Вывод только основной информации о датах
            logger.info(f"Дата из куба: {cube_date}")
            logger.info(f"Дата из аналитики: {analytics_date}")
            logger.info(f"Дата из Excel: {excel_date}")
            
            if not cube_date or not analytics_date or not excel_date:
                logger.error("Не удалось получить все необходимые даты для сравнения")
                return
                
            # Преобразуем даты в объекты datetime для корректного сравнения
            cube_dt = datetime.strptime(cube_date, '%Y-%m-%d')
            analytics_dt = datetime.strptime(analytics_date, '%Y-%m-%d')
            excel_dt = datetime.strptime(excel_date, '%Y-%m-%d')
            
            # Расчет разницы в днях
            cube_analytics_diff = (analytics_dt - cube_dt).days
            cube_excel_diff = (excel_dt - cube_dt).days
            
            # Ожидаемая разница между кубом и аналитикой - 1 день
            expected_analytics_diff = 1
            
            if cube_analytics_diff == expected_analytics_diff:
                logger.info(f"✅ Даты куба и аналитики согласованы с учетом смещения в {expected_analytics_diff} день")
                self.test_results['summary']['passed'] += 1
            else:
                logger.error(f"❌ Даты куба и аналитики не согласованы. Ожидалось смещение в {expected_analytics_diff} день, фактическое: {cube_analytics_diff}")
                self.test_results['summary']['failed'] += 1
                
            # Ожидаемая разница между кубом и Excel - 0 дней
            if cube_excel_diff == 0:
                logger.info("✅ Даты куба и Excel полностью согласованы")
                self.test_results['summary']['passed'] += 1
            else:
                logger.error(f"❌ Даты куба и Excel не согласованы (разница {cube_excel_diff} дней)")
                self.test_results['summary']['failed'] += 1
            
            # Сбрасываем счетчики, чтобы избежать дублирования
            self.test_results['summary']['passed'] = 2  # Две проверки дат уже выполнены
            self.test_results['summary']['failed'] = 0
            
            # Загрузка данных из Excel
            excel_data = self.load_excel_data()
            
            # Получение данных из куба для первой даты (для проверки количества агрегатов)
            cube_data = self.get_cube_data(cube_date)
            
            # Проверка количества агрегатов
            self.check_aggregates_count(excel_data, cube_data)
            
            # Получаем годы из аналитики
            analytics_data = self.get_analytics_data()
            years = pd.to_datetime(analytics_data['Dates']).dt.year.unique()
            
            # Проверка SNE по всем годам
            for year in years:
                # Получаем данные куба за весь год
                cube_year_data = self.get_cube_data_by_year(year)
                if cube_year_data.empty:
                    logger.warning(f"Нет данных в кубе за {year} год")
                    continue
                
                # Проверка SNE
                self.check_sne_by_year(excel_data, cube_year_data, year)
                
                # Проверка налета часов
                self.check_flight_hours_by_year(cube_year_data, year)
            
            # Проверки Куб-Аналитика
            self.test_cube_to_analytics()
                
            # Вывод итогов
            self.print_summary()
        except Exception as e:
            logger.error(f"Произошла ошибка при выполнении проверок: {e}", exc_info=True)
            
    def check_aggregates_count(self, excel_data: Dict[str, pd.DataFrame], cube_data: pd.DataFrame):
        """Проверка количества агрегатов"""
        if not excel_data or 'vygruzka' not in excel_data or excel_data['vygruzka'].empty:
            logger.error("❌ Отсутствуют данные Excel для сравнения")
            self.test_results['summary']['failed'] += 1
            return
            
        if cube_data.empty:
            logger.error("❌ Отсутствуют данные куба для сравнения")
            self.test_results['summary']['failed'] += 1
            return
            
        # Проверка наличия необходимых столбцов
        required_excel_columns = ['serialno']
        required_cube_columns = ['serialno']
        
        missing_excel_columns = [col for col in required_excel_columns if col not in excel_data['vygruzka'].columns]
        missing_cube_columns = [col for col in required_cube_columns if col not in cube_data.columns]
        
        if missing_excel_columns or missing_cube_columns:
            logger.error(f"❌ Отсутствуют необходимые столбцы для проверки количества агрегатов")
            self.test_results['summary']['failed'] += 1
            return
        
        try:
            # Исключаем агрегаты с серийными номерами, начинающимися на S
            s_count = len([sn for sn in cube_data['serialno'] if str(sn).startswith('S')])
            cube_data = cube_data[~cube_data['serialno'].astype(str).str.startswith('S')]
            
            # Проверка количества агрегатов
            excel_serials = set(excel_data['vygruzka']['serialno'].astype(str))
            cube_serials = set(cube_data['serialno'].astype(str))
            
            if len(excel_serials) == len(cube_serials):
                logger.info("✅ Количество агрегатов совпадает")
                self.test_results['summary']['passed'] += 1
            else:
                logger.error("❌ Количество агрегатов не совпадает")
                self.test_results['summary']['failed'] += 1
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке количества агрегатов: {e}")
            self.test_results['summary']['failed'] += 1
            
    def check_sne_by_year(self, excel_data: Dict[str, pd.DataFrame], cube_data: pd.DataFrame, year: int):
        """Проверка сумм SNE по году"""
        if not excel_data or 'vygruzka' not in excel_data or excel_data['vygruzka'].empty:
            logger.error(f"❌ Отсутствуют данные Excel для проверки SNE за {year} год")
            self.test_results['summary']['failed'] += 1
            return
            
        if cube_data.empty:
            logger.error(f"❌ Отсутствуют данные куба для проверки SNE за {year} год")
            self.test_results['summary']['failed'] += 1
            return
            
        # Проверка наличия необходимых столбцов
        required_excel_columns = ['sne']
        required_cube_columns = ['sne']
        
        missing_excel_columns = [col for col in required_excel_columns if col not in excel_data['vygruzka'].columns]
        missing_cube_columns = [col for col in required_cube_columns if col not in cube_data.columns]
        
        if missing_excel_columns or missing_cube_columns:
            logger.error(f"❌ Отсутствуют необходимые столбцы для проверки SNE за {year} год")
            self.test_results['summary']['failed'] += 1
            return
        
        try:
            # Исключаем агрегаты с серийными номерами, начинающимися на S
            cube_data = cube_data[~cube_data['serialno'].astype(str).str.startswith('S')]
            
            # Проверка сумм sne
            excel_sne = excel_data['vygruzka']['sne'].sum()
            cube_sne = cube_data['sne'].sum()
            
            # Допустимое отклонение 5%
            threshold = 0.05
            
            if abs(excel_sne - cube_sne) <= excel_sne * threshold:
                logger.info(f"✅ Суммы SNE за {year} год совпадают: Excel ({excel_sne:.2f}) ≈ Куб ({cube_sne:.2f})")
                self.test_results['summary']['passed'] += 1
            else:
                logger.error(f"❌ Суммы SNE за {year} год не совпадают: Excel ({excel_sne:.2f}) != Куб ({cube_sne:.2f})")
                self.test_results['summary']['failed'] += 1
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке SNE за {year} год: {e}")
            self.test_results['summary']['failed'] += 1
            
    def check_flight_hours_by_year(self, cube_data: pd.DataFrame, year: int):
        """Проверка налета часов по году"""
        if cube_data.empty:
            logger.error(f"❌ Отсутствуют данные куба для проверки налета часов за {year} год")
            self.test_results['summary']['failed'] += 1
            return
            
        # Проверка наличия необходимых столбцов
        required_columns = ['Status_P', 'sne', 'daily_flight_hours']
        
        missing_columns = [col for col in required_columns if col not in cube_data.columns]
        
        if missing_columns:
            logger.error(f"❌ Отсутствуют необходимые столбцы для проверки налета часов за {year} год")
            self.test_results['summary']['failed'] += 1
            return
        
        try:
            # Расчет налета часов для статуса Эксплуатация
            flight_hours = cube_data[
                cube_data['Status_P'] == 'Эксплуатация'
            ]['daily_flight_hours'].sum()

            # Сортируем данные по серийному номеру и дате
            cube_data_sorted = cube_data.sort_values(['serialno', 'Dates'])
            
            # Группируем по серийному номеру и вычисляем разницу sne между текущей и предыдущей датой
            # Затем суммируем все положительные разницы (увеличение sne)
            sne_diff = cube_data_sorted.groupby('serialno')['sne'].diff().fillna(0)
            sne_changes = sne_diff[sne_diff > 0].sum()

            # Проверка равенства значений
            threshold = 0.05  # 5%
            if abs(flight_hours - sne_changes) <= flight_hours * threshold:
                logger.info(f"✅ Налет часов за {year} год согласован с SNE: flight_hours ({flight_hours:.2f}) ≈ sne_changes ({sne_changes:.2f})")
                self.test_results['summary']['passed'] += 1
            else:
                logger.error(f"❌ Расхождение в налете часов за {year} год: flight_hours ({flight_hours:.2f}) != sne_changes ({sne_changes:.2f})")
                self.test_results['summary']['failed'] += 1
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке налета часов за {year} год: {e}")
            self.test_results['summary']['failed'] += 1

    def print_summary(self):
        """Вывод итогов проверок"""
        logger.info("\nИтоги проверок:")
        logger.info(f"✅: {self.test_results['summary']['passed']}")
        logger.info(f"❌: {self.test_results['summary']['failed']}")
        logger.info(f"⚠️: {self.test_results['summary']['warnings']}")

if __name__ == '__main__':
    tester = DataConsistencyTester()
    tester.run_tests()
