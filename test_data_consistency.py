#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import logging
import pandas as pd
import numpy as np
from clickhouse_driver import Client
from datetime import datetime, timedelta, date
import argparse
import json
from typing import Dict, List, Tuple, Any, Optional, Union
import traceback

# Класс для сериализации дат в JSON
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_consistency_test.log", mode='w', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Настройки подключения к ClickHouse
clickhouse_host = os.getenv('CLICKHOUSE_HOST', '10.95.19.132')
clickhouse_user = os.getenv('CLICKHOUSE_USER', 'default')
clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', 'quie1ahpoo5Su0wohpaedae8keeph6bi')
database_name = os.getenv('CLICKHOUSE_DB', 'default')

# Пути к Excel-файлам (настраиваются через аргументы командной строки)
DEFAULT_EXCEL_PATHS = {
    'vygruzka': './Выгрузка/Standard Table Report 251124 ВНВ rev KDV.xlsx',
    'agregat': './Агрегат/Агрегат.xlsx',
    'monthly_flight_hours': './Программа/Program.xlsx'
}

class DataConsistencyTester:
    """
    Класс для проверки согласованности данных между Excel-файлами, 
    OLAP-кубом и аналитической таблицей
    """
    
    def __init__(self, excel_paths=None, version_id=None):
        """
        Инициализация тестера
        
        Args:
            excel_paths (dict): Пути к Excel-файлам
            version_id (str): ID версии для проверки в аналитической таблице
        """
        self.excel_paths = excel_paths or DEFAULT_EXCEL_PATHS
        self.version_id = version_id
        self.client = Client(
            host=clickhouse_host,
            user=clickhouse_user,
            password=clickhouse_password,
            port=9000,
            secure=False,
            settings={'strings_encoding': 'utf-8'}
        )
        self.test_results = {
            'excel_to_cube': [],
            'cube_to_analytics': [],
            'summary': {'passed': 0, 'failed': 0, 'warnings': 0}
        }
        
        # Проверяем подключение к БД
        try:
            self.client.execute("SELECT 1")
            logger.info("Подключение к ClickHouse успешно установлено")
        except Exception as e:
            logger.error(f"Ошибка подключения к ClickHouse: {e}")
            sys.exit(1)
            
        # Если version_id не указан, берем последний
        if not self.version_id:
            try:
                query = f"""
                SELECT version_id 
                FROM {database_name}.Heli_Components 
                ORDER BY created_at DESC 
                LIMIT 1
                """
                result = self.client.execute(query)
                if result:
                    self.version_id = result[0][0]
                    logger.info(f"Используем последнюю версию: {self.version_id}")
                else:
                    logger.error("Не найдено ни одной версии в аналитической таблице")
                    sys.exit(1)
            except Exception as e:
                logger.error(f"Ошибка при получении последней версии: {e}")
                sys.exit(1)
    
    def load_excel_data(self) -> Dict[str, pd.DataFrame]:
        """
        Загрузка данных из Excel-файлов
        
        Returns:
            Dict[str, pd.DataFrame]: Словарь с данными из Excel-файлов
        """
        excel_data = {}
        
        try:
            # Загрузка всех файлов из папки Выгрузка
            vygruzka_path = './Выгрузка'
            if os.path.exists(vygruzka_path):
                excel_files = [f for f in os.listdir(vygruzka_path) if f.endswith('.xlsx')]
                if excel_files:
                    latest_file = max(excel_files, key=lambda x: os.path.getmtime(os.path.join(vygruzka_path, x)))
                    excel_data['vygruzka'] = pd.read_excel(
                        os.path.join(vygruzka_path, latest_file),
                        engine='openpyxl'
                    )
                    logger.info(f"Загружено {len(excel_data['vygruzka'])} записей из {latest_file}")
                else:
                    logger.warning(f"В папке {vygruzka_path} не найдено файлов Excel")
            else:
                logger.warning(f"Папка {vygruzka_path} не найдена")
            
            # Загрузка всех файлов из папки Агрегат
            agregat_path = './Агрегат'
            if os.path.exists(agregat_path):
                excel_files = [f for f in os.listdir(agregat_path) if f.endswith('.xlsx')]
                if excel_files:
                    latest_file = max(excel_files, key=lambda x: os.path.getmtime(os.path.join(agregat_path, x)))
                    excel_data['agregat'] = pd.read_excel(
                        os.path.join(agregat_path, latest_file),
                        engine='openpyxl'
                    )
                    logger.info(f"Загружено {len(excel_data['agregat'])} записей из {latest_file}")
                else:
                    logger.warning(f"В папке {agregat_path} не найдено файлов Excel")
            else:
                logger.warning(f"Папка {agregat_path} не найдена")
            
            # Загрузка всех файлов из папки Программа
            program_path = './Программа'
            if os.path.exists(program_path):
                excel_files = [f for f in os.listdir(program_path) if f.endswith('.xlsx')]
                if excel_files:
                    latest_file = max(excel_files, key=lambda x: os.path.getmtime(os.path.join(program_path, x)))
                    excel_data['monthly_flight_hours'] = pd.read_excel(
                        os.path.join(program_path, latest_file),
                        engine='openpyxl'
                    )
                    logger.info(f"Загружено {len(excel_data['monthly_flight_hours'])} записей из {latest_file}")
                else:
                    logger.warning(f"В папке {program_path} не найдено файлов Excel")
            else:
                logger.warning(f"Папка {program_path} не найдена")
                
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных из Excel: {e}")
            traceback.print_exc()
            
        return excel_data
    
    def get_cube_data(self, date_range=None) -> pd.DataFrame:
        """
        Получение данных из OLAP-куба
        
        Args:
            date_range (tuple): Диапазон дат (min_date, max_date)
            
        Returns:
            pd.DataFrame: Данные из OLAP-куба
        """
        try:
            # Если диапазон дат не указан, берем последние 30 дней
            if not date_range:
                max_date_query = f"SELECT MAX(Dates) FROM {database_name}.OlapCube_VNV"
                max_date = self.client.execute(max_date_query)[0][0]
                
                # Вычисляем дату на 30 дней назад
                if isinstance(max_date, str):
                    min_date = (datetime.strptime(max_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
                else:  # Если max_date уже является объектом datetime.date
                    min_date = (max_date - timedelta(days=30)).strftime('%Y-%m-%d')
                    max_date = max_date.strftime('%Y-%m-%d')
                
                date_range = (min_date, max_date)
                logger.info(f"Ограничиваем выборку данных из OLAP-куба последними 30 днями: {min_date} - {max_date}")
            
            # Получаем данные из куба с ограничением по датам
            query = f"""
            SELECT 
                Dates,
                serialno,
                Status, 
                Status_P, 
                sne,
                ppr,
                oh,
                ll,
                daily_flight_hours,
                trigger_type,
                mi8t_count,
                mi17_count
            FROM {database_name}.OlapCube_VNV
            WHERE Dates BETWEEN '{date_range[0]}' AND '{date_range[1]}'
            ORDER BY Dates, serialno
            """
            
            result = self.client.execute(query)
            
            # Преобразуем результат в DataFrame
            columns = [
                'Dates', 'serialno', 'Status', 'Status_P', 'sne', 'ppr', 'oh', 'll', 
                'daily_flight_hours', 'trigger_type', 'mi8t_count', 'mi17_count'
            ]
            
            df = pd.DataFrame(result, columns=columns)
            
            logger.info(f"Загружено {len(df)} записей из OLAP-куба")
            
            return df
            
        except Exception as e:
            logger.error(f"Ошибка при получении данных из OLAP-куба: {e}")
            traceback.print_exc()
            return pd.DataFrame()
    
    def get_analytics_data(self) -> pd.DataFrame:
        """
        Получение данных из аналитической таблицы
        
        Returns:
            pd.DataFrame: Данные из аналитической таблицы
        """
        try:
            query = f"""
            SELECT 
                Dates,
                ops_count,
                hbs_count,
                repair_count,
                total_operable,
                entry_count,
                exit_count,
                into_repair,
                complete_repair,
                mi8t_count,
                mi17_count
            FROM {database_name}.Heli_Components
            WHERE version_id = '{self.version_id}'
            ORDER BY Dates
            """
            
            result = self.client.execute(query)
            
            # Преобразуем результат в DataFrame
            columns = [
                'Dates', 'ops_count', 'hbs_count', 'repair_count', 'total_operable',
                'entry_count', 'exit_count', 'into_repair', 'complete_repair',
                'mi8t_count', 'mi17_count'
            ]
            
            df = pd.DataFrame(result, columns=columns)
            
            logger.info(f"Загружено {len(df)} записей из аналитической таблицы")
            
            return df
            
        except Exception as e:
            logger.error(f"Ошибка при получении данных из аналитической таблицы: {e}")
            traceback.print_exc()
            return pd.DataFrame()
    
    def test_excel_to_cube_consistency(self, excel_data: Dict[str, pd.DataFrame], cube_data: pd.DataFrame) -> None:
        """
        Проверка согласованности данных между Excel-файлами и OLAP-кубом
        
        Args:
            excel_data (Dict[str, pd.DataFrame]): Данные из Excel-файлов
            cube_data (pd.DataFrame): Данные из OLAP-куба
        """
        logger.info("Проверка согласованности данных между Excel-файлами и OLAP-кубом")
        
        # Получаем первую дату в кубе
        first_date = cube_data['Dates'].min()
        logger.info(f"Проверка согласованности данных на дату: {first_date}")
        
        # Фильтруем данные куба по первой дате
        cube_first_date = cube_data[cube_data['Dates'] == first_date]
        
        # Фильтруем агрегаты, исключая те, чьи серийные номера начинаются на S
        # Эти агрегаты есть в кубе и аналитической таблице, но их нет в выгрузке Excel
        cube_filtered = cube_first_date.copy()
        s_serials = cube_filtered[cube_filtered['serialno'].astype(str).str.startswith('S', na=False)]
        if not s_serials.empty:
            logger.info(f"Исключаем {len(s_serials)} агрегатов с серийными номерами, начинающимися на S при сравнении с Excel")
            cube_filtered = cube_filtered[~cube_filtered['serialno'].astype(str).str.startswith('S', na=False)]
        
        # Проверка 1: Количество агрегатов по serialno должно совпадать, без дубликатов
        if 'vygruzka' in excel_data:
            # Проверяем наличие столбца с серийными номерами
            serial_column = 'serialno' if 'serialno' in excel_data['vygruzka'].columns else 'SerialNumber'
            
            if serial_column in excel_data['vygruzka'].columns:
                # Получаем уникальные серийные номера из выгрузки и куба
                vygruzka_serials = set(excel_data['vygruzka'][serial_column].dropna().unique())
                cube_serials = set(cube_filtered['serialno'].dropna().unique())
                
                # Находим пересечение, отсутствующие и лишние серийные номера
                common_serials = vygruzka_serials.intersection(cube_serials)
                missing_in_cube = vygruzka_serials - cube_serials
                extra_in_cube = cube_serials - vygruzka_serials
                
                test_result = {
                    'test_name': 'Проверка количества агрегатов по serialno',
                    'status': 'PASSED' if len(missing_in_cube) == 0 and len(extra_in_cube) == 0 else 'FAILED',
                    'details': {
                        'date_checked': first_date,
                        'vygruzka_serials_count': len(vygruzka_serials),
                        'cube_serials_count': len(cube_serials),
                        'common_serials_count': len(common_serials),
                        'missing_in_cube_count': len(missing_in_cube),
                        'missing_in_cube': list(missing_in_cube)[:20] if missing_in_cube else [],
                        'extra_in_cube_count': len(extra_in_cube),
                        'extra_in_cube': list(extra_in_cube)[:20] if extra_in_cube else []
                    }
                }
                
                self.test_results['excel_to_cube'].append(test_result)
                
                if test_result['status'] == 'PASSED':
                    logger.info(f"✅ Количество агрегатов по serialno совпадает: {len(common_serials)}")
                    self.test_results['summary']['passed'] += 1
                else:
                    logger.error(f"❌ Обнаружены расхождения в количестве агрегатов по serialno")
                    if missing_in_cube:
                        logger.error(f"   - {len(missing_in_cube)} серийных номеров отсутствуют в кубе")
                    if extra_in_cube:
                        logger.error(f"   - {len(extra_in_cube)} лишних серийных номеров в кубе")
                    self.test_results['summary']['failed'] += 1
            else:
                logger.warning(f"⚠️ В файле vygruzka отсутствует столбец с серийными номерами ({serial_column})")
                self.test_results['summary']['warnings'] += 1
        
        # Проверка 2: Сумма sne и ppr должна совпадать
        if 'vygruzka' in excel_data and 'sne' in excel_data['vygruzka'].columns and 'ppr' in excel_data['vygruzka'].columns:
            # Преобразуем столбцы в числовой формат
            excel_data['vygruzka']['sne'] = pd.to_numeric(excel_data['vygruzka']['sne'], errors='coerce')
            excel_data['vygruzka']['ppr'] = pd.to_numeric(excel_data['vygruzka']['ppr'], errors='coerce')
            
            # Вычисляем суммы в Excel и кубе
            excel_sne_sum = excel_data['vygruzka']['sne'].sum()
            excel_ppr_sum = excel_data['vygruzka']['ppr'].sum()
            
            cube_sne_sum = cube_filtered['sne'].sum()
            cube_ppr_sum = cube_filtered['ppr'].sum()
            
            # Вычисляем разницу
            sne_diff = excel_sne_sum - cube_sne_sum
            ppr_diff = excel_ppr_sum - cube_ppr_sum
            
            # Определяем допустимую погрешность (0.1%)
            sne_threshold = excel_sne_sum * 0.001
            ppr_threshold = excel_ppr_sum * 0.001
            
            test_result = {
                'test_name': 'Проверка суммы sne и ppr',
                'status': 'PASSED' if abs(sne_diff) <= sne_threshold and abs(ppr_diff) <= ppr_threshold else 'FAILED',
                'details': {
                    'date_checked': first_date,
                    'excel_sne_sum': excel_sne_sum,
                    'cube_sne_sum': cube_sne_sum,
                    'sne_diff': sne_diff,
                    'sne_diff_percent': (sne_diff / excel_sne_sum * 100) if excel_sne_sum != 0 else 0,
                    'excel_ppr_sum': excel_ppr_sum,
                    'cube_ppr_sum': cube_ppr_sum,
                    'ppr_diff': ppr_diff,
                    'ppr_diff_percent': (ppr_diff / excel_ppr_sum * 100) if excel_ppr_sum != 0 else 0
                }
            }
            
            self.test_results['excel_to_cube'].append(test_result)
            
            if test_result['status'] == 'PASSED':
                logger.info(f"✅ Суммы sne и ppr совпадают с допустимой погрешностью")
                self.test_results['summary']['passed'] += 1
            else:
                logger.error(f"❌ Обнаружены расхождения в суммах sne и ppr")
                if abs(sne_diff) > sne_threshold:
                    logger.error(f"   - Разница в sne: {sne_diff:.2f} ({(sne_diff / excel_sne_sum * 100):.2f}%)")
                if abs(ppr_diff) > ppr_threshold:
                    logger.error(f"   - Разница в ppr: {ppr_diff:.2f} ({(ppr_diff / excel_ppr_sum * 100):.2f}%)")
                self.test_results['summary']['failed'] += 1
        else:
            logger.warning("⚠️ В файле vygruzka отсутствуют столбцы sne и/или ppr")
            self.test_results['summary']['warnings'] += 1
        
        # Проверка 3 и 4: Проверка статусов
        if 'vygruzka' in excel_data and 'condition' in excel_data['vygruzka'].columns:
            # Проверка 3: Сумма condition = "Исправный" должна равняться сумме в кубе для Status_P = "Эксплуатация" + "Исправен"
            excel_operable = excel_data['vygruzka'][excel_data['vygruzka']['condition'] == 'Исправный']
            cube_operable = cube_filtered[cube_filtered['Status_P'].isin(['Эксплуатация', 'Исправен'])]
            
            # Проверка 4: Сумма condition = "Неисправный" должна равняться сумме в кубе для Status_P = "Ремонт" + "Хранение"
            excel_inoperable = excel_data['vygruzka'][excel_data['vygruzka']['condition'] == 'Неисправный']
            cube_inoperable = cube_filtered[cube_filtered['Status_P'].isin(['Ремонт', 'Хранение'])]
            
            test_result = {
                'test_name': 'Проверка статусов агрегатов',
                'status': 'PASSED',
                'details': {
                    'date_checked': first_date,
                    'excel_operable_count': len(excel_operable),
                    'cube_operable_count': len(cube_operable),
                    'excel_inoperable_count': len(excel_inoperable),
                    'cube_inoperable_count': len(cube_inoperable)
                }
            }
            
            # Проверяем исправные
            if len(excel_operable) != len(cube_operable):
                test_result['status'] = 'FAILED'
                test_result['details']['operable_diff'] = len(excel_operable) - len(cube_operable)
            
            # Проверяем неисправные
            if len(excel_inoperable) != len(cube_inoperable):
                test_result['status'] = 'FAILED'
                test_result['details']['inoperable_diff'] = len(excel_inoperable) - len(cube_inoperable)
            
            self.test_results['excel_to_cube'].append(test_result)
            
            if test_result['status'] == 'PASSED':
                logger.info("✅ Количество агрегатов по статусам совпадает")
                self.test_results['summary']['passed'] += 1
            else:
                logger.error("❌ Обнаружены расхождения в количестве агрегатов по статусам:")
                if 'operable_diff' in test_result['details']:
                    logger.error(f"   - Разница в количестве исправных: {test_result['details']['operable_diff']}")
                if 'inoperable_diff' in test_result['details']:
                    logger.error(f"   - Разница в количестве неисправных: {test_result['details']['inoperable_diff']}")
                self.test_results['summary']['failed'] += 1
        else:
            logger.warning("⚠️ В файле vygruzka отсутствует столбец condition")
            self.test_results['summary']['warnings'] += 1
    
    def test_cube_to_analytics_consistency(self, cube_data: pd.DataFrame, analytics_data: pd.DataFrame) -> None:
        """
        Проверка согласованности данных между OLAP-кубом и аналитической таблицей
        
        Args:
            cube_data (pd.DataFrame): Данные из OLAP-куба
            analytics_data (pd.DataFrame): Данные из аналитической таблицы
        """
        logger.info("Проверка согласованности данных между OLAP-кубом и аналитической таблицей")
        
        # Проверяем, что данные не пустые
        if cube_data.empty:
            logger.error("Данные из OLAP-куба пусты, пропускаем проверки")
            self.test_results['summary']['warnings'] += 1
            return
            
        if analytics_data.empty:
            logger.error("Данные из аналитической таблицы пусты, пропускаем проверки")
            self.test_results['summary']['warnings'] += 1
            return
            
        # Проверяем наличие необходимых столбцов
        required_cube_columns = ['Dates', 'serialno', 'Status_P', 'sne', 'ppr', 'daily_flight_hours', 'mi8t_count', 'mi17_count']
        required_analytics_columns = ['Dates', 'ops_count', 'entry_count', 'exit_count', 'into_repair', 'complete_repair', 'mi8t_count', 'mi17_count']
        
        missing_cube_columns = [col for col in required_cube_columns if col not in cube_data.columns]
        missing_analytics_columns = [col for col in required_analytics_columns if col not in analytics_data.columns]
        
        if missing_cube_columns:
            logger.error(f"В данных из OLAP-куба отсутствуют столбцы: {missing_cube_columns}")
            self.test_results['summary']['warnings'] += 1
            return
            
        if missing_analytics_columns:
            logger.error(f"В данных из аналитической таблицы отсутствуют столбцы: {missing_analytics_columns}")
            self.test_results['summary']['warnings'] += 1
            return
        
        # Проверка 1: Сравнение налета часов и изменений счетчиков по годам
        try:
            # Создаем копию данных куба
            cube_copy = cube_data.copy()
            
            # Преобразуем столбцы в числовой формат
            cube_copy['sne'] = pd.to_numeric(cube_copy['sne'], errors='coerce')
            cube_copy['ppr'] = pd.to_numeric(cube_copy['ppr'], errors='coerce')
            cube_copy['daily_flight_hours'] = pd.to_numeric(cube_copy['daily_flight_hours'], errors='coerce')
            
            # Добавляем столбец с годом
            cube_copy['year'] = pd.to_datetime(cube_copy['Dates']).dt.year
            
            # Вычисляем сумму daily_flight_hours для статуса "Эксплуатация" по годам
            flight_hours_by_year = cube_copy[cube_copy['Status_P'] == 'Эксплуатация'].groupby('year')['daily_flight_hours'].sum()

            # Вычисляем изменения sne и ppr по годам
            cube_copy = cube_copy.sort_values(['serialno', 'Dates'])
            cube_copy['sne_diff'] = cube_copy.groupby('serialno')['sne'].diff()
            cube_copy['ppr_diff'] = cube_copy.groupby('serialno')['ppr'].diff()

            sne_changes_by_year = cube_copy.groupby('year')['sne_diff'].sum()
            ppr_changes_by_year = cube_copy.groupby('year')['ppr_diff'].sum()

            # Проверяем соответствие для каждого года
            for year in flight_hours_by_year.index:
                flight_hours = flight_hours_by_year[year]
                sne_changes = sne_changes_by_year.get(year, 0)
                ppr_changes = ppr_changes_by_year.get(year, 0)

                # Вычисляем разницу в процентах
                sne_diff_percent = abs((flight_hours - sne_changes) / flight_hours * 100) if flight_hours != 0 else 0
                ppr_diff_percent = abs((flight_hours - ppr_changes) / flight_hours * 100) if flight_hours != 0 else 0

                # Допустимая погрешность - 1%
                threshold = 1.0

                test_result = {
                    'test_name': f'Проверка согласованности налета часов и изменений счетчиков за {year} год',
                    'status': 'PASSED' if sne_diff_percent <= threshold and ppr_diff_percent <= threshold else 'FAILED',
                    'details': {
                        'year': year,
                        'flight_hours': flight_hours,
                        'sne_changes': sne_changes,
                        'ppr_changes': ppr_changes,
                        'sne_diff_percent': sne_diff_percent,
                        'ppr_diff_percent': ppr_diff_percent,
                        'threshold': threshold
                    }
                }

                self.test_results['cube_to_analytics'].append(test_result)

                if test_result['status'] == 'PASSED':
                    logger.info(f"✅ Согласованность налета часов и изменений счетчиков за {year} год: OK")
                    self.test_results['summary']['passed'] += 1
                else:
                    logger.error(f"❌ Обнаружены расхождения в налете часов и изменениях счетчиков за {year} год:")
                    logger.error(f"   - Разница с SNE: {sne_diff_percent:.2f}%")
                    logger.error(f"   - Разница с PPR: {ppr_diff_percent:.2f}%")
                    self.test_results['summary']['failed'] += 1

        except Exception as e:
            logger.error(f"Ошибка при проверке согласованности налета часов и изменений счетчиков: {e}")
            traceback.print_exc()
            self.test_results['summary']['failed'] += 1

        # Проверка 2: Проверка матрицы переходов статусов
        try:
            # Создаем копии данных
            cube_copy = cube_data.copy()
            analytics_copy = analytics_data.copy()

            # Группируем данные по датам и вычисляем изменения статусов
            cube_copy = cube_copy.sort_values(['serialno', 'Dates'])
            cube_copy['prev_status'] = cube_copy.groupby('serialno')['Status_P'].shift(1)
            
            # Вычисляем переходы статусов
            status_changes = cube_copy[cube_copy['Status_P'] != cube_copy['prev_status']].copy()
            
            # Считаем количество переходов по типам
            entry_count = len(status_changes[
                (status_changes['prev_status'].isin(['Ремонт', 'Хранение'])) & 
                (status_changes['Status_P'].isin(['Эксплуатация', 'Исправен']))
            ])
            
            exit_count = len(status_changes[
                (status_changes['prev_status'].isin(['Эксплуатация', 'Исправен'])) & 
                (status_changes['Status_P'].isin(['Ремонт', 'Хранение']))
            ])
            
            into_repair = len(status_changes[
                (status_changes['prev_status'] != 'Ремонт') & 
                (status_changes['Status_P'] == 'Ремонт')
            ])
            
            complete_repair = len(status_changes[
                (status_changes['prev_status'] == 'Ремонт') & 
                (status_changes['Status_P'] != 'Ремонт')
            ])

            # Сравниваем с данными из аналитической таблицы
            analytics_totals = analytics_copy.agg({
                'entry_count': 'sum',
                'exit_count': 'sum',
                'into_repair': 'sum',
                'complete_repair': 'sum'
            })

            test_result = {
                'test_name': 'Проверка матрицы переходов статусов',
                'status': 'PASSED',
                'details': {
                    'cube_entry_count': entry_count,
                    'analytics_entry_count': analytics_totals['entry_count'],
                    'cube_exit_count': exit_count,
                    'analytics_exit_count': analytics_totals['exit_count'],
                    'cube_into_repair': into_repair,
                    'analytics_into_repair': analytics_totals['into_repair'],
                    'cube_complete_repair': complete_repair,
                    'analytics_complete_repair': analytics_totals['complete_repair']
                }
            }

            # Проверяем каждый показатель
            if entry_count != analytics_totals['entry_count']:
                test_result['status'] = 'FAILED'
                test_result['details']['entry_diff'] = entry_count - analytics_totals['entry_count']
            
            if exit_count != analytics_totals['exit_count']:
                test_result['status'] = 'FAILED'
                test_result['details']['exit_diff'] = exit_count - analytics_totals['exit_count']
            
            if into_repair != analytics_totals['into_repair']:
                test_result['status'] = 'FAILED'
                test_result['details']['into_repair_diff'] = into_repair - analytics_totals['into_repair']
            
            if complete_repair != analytics_totals['complete_repair']:
                test_result['status'] = 'FAILED'
                test_result['details']['complete_repair_diff'] = complete_repair - analytics_totals['complete_repair']

            self.test_results['cube_to_analytics'].append(test_result)

            if test_result['status'] == 'PASSED':
                logger.info("✅ Матрица переходов статусов соответствует аналитической таблице")
                self.test_results['summary']['passed'] += 1
            else:
                logger.error("❌ Обнаружены расхождения в матрице переходов статусов:")
                if 'entry_diff' in test_result['details']:
                    logger.error(f"   - Разница в entry_count: {test_result['details']['entry_diff']}")
                if 'exit_diff' in test_result['details']:
                    logger.error(f"   - Разница в exit_count: {test_result['details']['exit_diff']}")
                if 'into_repair_diff' in test_result['details']:
                    logger.error(f"   - Разница в into_repair: {test_result['details']['into_repair_diff']}")
                if 'complete_repair_diff' in test_result['details']:
                    logger.error(f"   - Разница в complete_repair: {test_result['details']['complete_repair_diff']}")
                self.test_results['summary']['failed'] += 1

        except Exception as e:
            logger.error(f"Ошибка при проверке матрицы переходов статусов: {e}")
            traceback.print_exc()
            self.test_results['summary']['failed'] += 1

        # Проверка 3: Сравнение ops_count с суммой mi8t_count и mi17_count
        try:
            # Создаем копии данных
            cube_copy = cube_data.copy()
            analytics_copy = analytics_data.copy()

            # Проверяем, что в аналитической таблице есть данные
            if len(analytics_copy) == 0:
                logger.warning("В аналитической таблице нет данных для проверки ops_count")
                self.test_results['summary']['warnings'] += 1
                return

            # Добавляем столбец с месяцем в обе таблицы
            cube_copy['month'] = pd.to_datetime(cube_copy['Dates']).dt.to_period('M')
            analytics_copy['month'] = pd.to_datetime(analytics_copy['Dates']).dt.to_period('M')

            # Группируем данные куба по месяцам
            cube_monthly = cube_copy.groupby('month').agg({
                'mi8t_count': 'sum',
                'mi17_count': 'sum'
            }).reset_index()
            
            # Проверяем, что в сгруппированных данных есть записи
            if len(cube_monthly) == 0:
                logger.warning("В сгруппированных данных куба нет записей для проверки")
                self.test_results['summary']['warnings'] += 1
                return
                
            cube_monthly['total_count'] = cube_monthly['mi8t_count'] + cube_monthly['mi17_count']

            # Проверяем соответствие для каждого дня в аналитической таблице
            for date in analytics_copy['Dates'].unique():
                month = pd.to_datetime(date).to_period('M')
                
                # Проверяем, что для данного месяца есть данные в кубе
                if not any(cube_monthly['month'] == month):
                    logger.warning(f"В кубе нет данных для месяца {month}, пропускаем проверку для даты {date}")
                    continue
                    
                analytics_row = analytics_copy[analytics_copy['Dates'] == date].iloc[0]
                cube_row = cube_monthly[cube_monthly['month'] == month].iloc[0]

                test_result = {
                    'test_name': f'Проверка соответствия ops_count сумме mi8t_count и mi17_count на {date}',
                    'status': 'PASSED',
                    'details': {
                        'date': date,
                        'ops_count': analytics_row['ops_count'],
                        'mi8t_count': cube_row['mi8t_count'],
                        'mi17_count': cube_row['mi17_count'],
                        'total_count': cube_row['total_count']
                    }
                }

                if analytics_row['ops_count'] != cube_row['total_count']:
                    test_result['status'] = 'FAILED'
                    test_result['details']['diff'] = analytics_row['ops_count'] - cube_row['total_count']

                self.test_results['cube_to_analytics'].append(test_result)

                if test_result['status'] == 'PASSED':
                    logger.info(f"✅ Соответствие ops_count сумме mi8t_count и mi17_count на {date}: OK")
                    self.test_results['summary']['passed'] += 1
                else:
                    logger.error(f"❌ Обнаружены расхождения в количестве агрегатов на {date}:")
                    logger.error(f"   - ops_count: {analytics_row['ops_count']}")
                    logger.error(f"   - mi8t_count + mi17_count: {cube_row['total_count']}")
                    logger.error(f"   - Разница: {test_result['details']['diff']}")
                    self.test_results['summary']['failed'] += 1

        except Exception as e:
            logger.error(f"Ошибка при проверке соответствия ops_count сумме mi8t_count и mi17_count: {e}")
            traceback.print_exc()
            self.test_results['summary']['failed'] += 1

    def get_min_date_and_version(self, excel_data: Dict[str, pd.DataFrame]) -> Tuple[str, str]:
        """
        Определение минимальной даты из Excel и соответствующей версии в аналитической таблице
        
        Args:
            excel_data (Dict[str, pd.DataFrame]): Данные из Excel-файлов
            
        Returns:
            Tuple[str, str]: (min_date, version_id)
        """
        try:
            # Получаем минимальную дату из Excel
            min_date = None
            if 'vygruzka' in excel_data:
                # Проверяем все столбцы с датами
                date_columns = ['date', 'Date', 'дата', 'Дата', 'mfg_date', 'oh_at_date', 'removal_date', 'repair_date']
                all_dates = []
                
                for col in date_columns:
                    if col in excel_data['vygruzka'].columns:
                        logger.info(f"Найден столбец с датой: {col}")
                        # Пробуем разные форматы даты
                        try:
                            # Сначала пробуем с явным указанием формата dd.mm.yyyy
                            dates = pd.to_datetime(excel_data['vygruzka'][col], format='%d.%m.%Y', dayfirst=True, errors='coerce')
                            if dates.isna().all():
                                # Если все даты оказались NaN, пробуем стандартный формат
                                dates = pd.to_datetime(excel_data['vygruzka'][col], dayfirst=True, errors='coerce')
                        except:
                            # Если не получилось, пробуем еще раз стандартный формат
                            dates = pd.to_datetime(excel_data['vygruzka'][col], dayfirst=True, errors='coerce')
                        
                        valid_dates = dates.dropna()
                        if not valid_dates.empty:
                            logger.info(f"В столбце {col} найдено {len(valid_dates)} валидных дат")
                            logger.info(f"Примеры дат из {col}: {sorted(set(valid_dates))[:3]}")
                            all_dates.extend(valid_dates)
                
                if all_dates:
                    # Фильтруем даты, исключая слишком старые (до 2000 года)
                    recent_dates = [d for d in all_dates if d.year >= 2000]
                    if recent_dates:
                        logger.info(f"Найдено {len(recent_dates)} дат после 2000 года")
                        min_date = min(recent_dates)
                    else:
                        logger.warning("Не найдено дат после 2000 года, используем самую последнюю дату")
                        min_date = max(all_dates)
                    
                    logger.info(f"Всего найдено {len(all_dates)} валидных дат")
                    logger.info(f"Выбранная дата для анализа: {min_date}")
            
            if not min_date:
                # Если дата не найдена в стандартных столбцах, выводим список всех столбцов
                if 'vygruzka' in excel_data:
                    logger.error("Столбец с датой не найден. Доступные столбцы:")
                    for col in excel_data['vygruzka'].columns:
                        logger.error(f"  - {col}")
                logger.error("Не удалось определить минимальную дату в Excel")
                sys.exit(1)
            
            # Форматируем дату
            min_date_str = min_date.strftime('%Y-%m-%d')
            logger.info(f"Форматированная дата для анализа: {min_date_str}")
            
            # Проверяем доступные таблицы в базе данных
            logger.info("Проверка доступных таблиц в базе данных...")
            tables_query = f"SHOW TABLES FROM {database_name}"
            try:
                tables = self.client.execute(tables_query)
                logger.info(f"Доступные таблицы в базе данных {database_name}:")
                for table in tables:
                    logger.info(f"  - {table[0]}")
                
                # Используем таблицу Heli_Components
                table_name = 'Heli_Components'
                logger.info(f"Используем таблицу {table_name}")
                
            except Exception as e:
                logger.error(f"Ошибка при получении списка таблиц: {e}")
                table_name = 'Heli_Components'  # Используем значение по умолчанию
            
            # Проверяем наличие версии для выбранной даты
            query = f"""
            SELECT version_id
            FROM {database_name}.{table_name}
            WHERE Dates = '{min_date_str}'
            ORDER BY created_at DESC
            LIMIT 1
            """
            
            logger.info(f"Выполняем запрос к БД: {query}")
            result = self.client.execute(query)
            
            if result:
                version_id = result[0][0]
                logger.info(f"Найдена версия {version_id} для даты {min_date_str}")
                return min_date_str, version_id
            else:
                # Если версия не найдена для выбранной даты, ищем ближайшую доступную дату
                logger.warning(f"Не найдена версия для даты {min_date_str}, ищем ближайшую доступную дату")
                
                # Получаем список всех доступных дат
                dates_query = f"""
                SELECT DISTINCT Dates
                FROM {database_name}.{table_name}
                ORDER BY Dates DESC
                LIMIT 30
                """
                
                available_dates = self.client.execute(dates_query)
                if available_dates:
                    # Берем самую последнюю доступную дату
                    latest_date = available_dates[0][0]
                    logger.info(f"Используем последнюю доступную дату: {latest_date}")
                    
                    # Получаем версию для этой даты
                    version_query = f"""
                    SELECT version_id
                    FROM {database_name}.{table_name}
                    WHERE Dates = '{latest_date}'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                    
                    version_result = self.client.execute(version_query)
                    if version_result:
                        version_id = version_result[0][0]
                        logger.info(f"Найдена версия {version_id} для даты {latest_date}")
                        return latest_date, version_id
                    else:
                        logger.error(f"Не найдена версия для даты {latest_date}")
                        sys.exit(1)
                else:
                    logger.error("Не найдено доступных дат в таблице")
                    sys.exit(1)
                
        except Exception as e:
            logger.error(f"Ошибка при определении минимальной даты и версии: {e}")
            traceback.print_exc()
            sys.exit(1)

    def run_tests(self, days: int = None) -> None:
        """
        Запуск всех проверок
        
        Args:
            days (int): Количество дней для проверки (опционально)
        """
        # Загружаем данные из Excel
        excel_data = self.load_excel_data()
        
        # Определяем минимальную дату и версию
        min_date, version_id = self.get_min_date_and_version(excel_data)
        self.version_id = version_id
        
        # Получаем данные из куба
        if days:
            # Проверяем тип min_date
            if isinstance(min_date, str):
                min_date_obj = datetime.strptime(min_date, '%Y-%m-%d')
            else:
                min_date_obj = min_date
                min_date = min_date_obj.strftime('%Y-%m-%d')
                
            max_date = (min_date_obj + timedelta(days=days)).strftime('%Y-%m-%d')
            date_range = (min_date, max_date)
        else:
            # Если min_date не строка, преобразуем его
            if not isinstance(min_date, str):
                min_date = min_date.strftime('%Y-%m-%d')
            date_range = (min_date, min_date)
            
        cube_data = self.get_cube_data(date_range)
        
        # Получаем данные из аналитической таблицы
        analytics_data = self.get_analytics_data()
        
        # Запускаем проверки
        self.test_excel_to_cube_consistency(excel_data, cube_data)
        self.test_cube_to_analytics_consistency(cube_data, analytics_data)
        
        # Выводим итоги
        logger.info("\nИтоги проверок:")
        logger.info(f"✅ Успешно: {self.test_results['summary']['passed']}")
        logger.info(f"❌ Ошибок: {self.test_results['summary']['failed']}")
        logger.info(f"⚠️ Предупреждений: {self.test_results['summary']['warnings']}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Проверка согласованности данных')
    parser.add_argument('--days', type=int, help='Количество дней для проверки')
    args = parser.parse_args()
    
    tester = DataConsistencyTester()
    tester.run_tests(args.days)