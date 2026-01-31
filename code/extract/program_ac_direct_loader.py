#!/usr/bin/env python3
"""
Program AC Direct Loader - Прямое создание тензора программы полетов ВС
======================================================================

Загружает данные из Program_heli.xlsx напрямую в таблицу flight_program_ac,
создавая тензор на 4000 дней для операций и новых поставок по типам ВС.

СТРУКТУРА EXCEL:
- Строка с "Год": годы по месяцам (1=2025, 2=2025, ...)
- Строки с ops_counter_*: операции ВС (распределить равномерно по дням месяца)
- Строки с new_counter_*: новые поставки (только в последний день месяца)

ЛОГИКА РАСПРЕДЕЛЕНИЯ:
- ops_counter_*: равномерно по всем дням месяца
- new_counter_*: полное значение в середину месяца (15-е число), остальные дни = 0
- Период: 4000 дней от базовой даты
- Группировка: по ac_type_mask (типы ВС)

РЕЗУЛЬТАТ: таблица flight_program_ac

Автор: AI Assistant
Дата: 2025-01-19
"""

import sys
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple, Any, Optional
import openpyxl
import calendar

# Добавляем путь к утилитам
sys.path.append(str(Path(__file__).parent / 'utils'))
from config_loader import get_clickhouse_client

class ProgramHeliAnalyzer:
    """Анализирует структуру Excel файла Program_heli.xlsx"""
    
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.logger = logging.getLogger(__name__)
        
    def analyze_excel_structure(self) -> Dict[str, Any]:
        """Анализирует структуру файла и извлекает данные"""
        try:
            if not self.file_path.exists():
                raise FileNotFoundError(f"Файл {self.file_path} не найден")
            
            # Загружаем Excel
            df = pd.read_excel(self.file_path, sheet_name='2025', header=0, engine='openpyxl')
            self.logger.info(f"📖 Загружен Excel: {len(df)} строк, {len(df.columns)} колонок")
            
            # Получаем все колонки с данными (включая многолетние)
            data_columns = []
            for col in df.columns:
                if col not in ['ac_type_mask', 'Месяц']:
                    data_columns.append(col)
            
            basic_columns = ['ac_type_mask', 'Месяц'] + data_columns
            df_filtered = df[basic_columns].copy()
            self.logger.info(f"📋 Найдено: {len(data_columns)} колонок с данными")
            
            # Анализируем структуру
            result = {
                'year_mapping': self.extract_year_mapping(df_filtered),
                'ops_data': self.parse_ops_data(df_filtered),
                'new_data': self.parse_new_data(df_filtered),
                'data_columns': data_columns,
                'raw_df': df_filtered
            }
            
            ops_count = len(result['ops_data'])
            new_count = len(result['new_data'])
            self.logger.info(f"✅ Структура проанализирована: {ops_count} ops_counter, {new_count} new_counter")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка анализа Excel: {e}")
            raise
    
    def extract_year_mapping(self, df: pd.DataFrame) -> Dict[str, Tuple[int, int]]:
        """Извлекает соответствие колонка→(месяц, год) из строки 'Год'"""
        try:
            # Ищем строку с годами
            year_rows = df[df['Месяц'] == 'Год']
            if year_rows.empty:
                self.logger.warning("⚠️ Не найдена строка с годами")
                return {}
            
            year_row = year_rows.iloc[0]
            column_mapping = {}
            
            # Обрабатываем все колонки с данными
            for col in df.columns:
                if col not in ['ac_type_mask', 'Месяц'] and pd.notna(year_row[col]):
                    year = int(year_row[col])
                    
                    # Определяем месяц из названия колонки
                    if isinstance(col, int):
                        # Колонки типа 1, 2, 3... это месяцы
                        month = col
                    elif isinstance(col, str) and '.' in col:
                        # Колонки типа '4.1', '4.2'... первая часть - месяц
                        month = int(col.split('.')[0])
                    else:
                        continue
                    
                    column_mapping[col] = (month, year)
            
            self.logger.info(f"📅 Найдено {len(column_mapping)} колонок с данными по годам")
            
            # Показываем примеры
            examples = list(column_mapping.items())[:5]
            for col, (month, year) in examples:
                self.logger.info(f"   Колонка {col} → месяц {month}, год {year}")
            
            return column_mapping
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка извлечения соответствия колонок: {e}")
            return {}
    
    def parse_ops_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Парсит данные ops_counter_* для равномерного распределения по дням"""
        try:
            ops_data = []
            
            # Фильтруем строки с ops_counter
            ops_rows = df[df['Месяц'].str.contains('ops_counter', na=False)]
            
            for idx, row in ops_rows.iterrows():
                ac_type_mask = row['ac_type_mask'] if pd.notna(row['ac_type_mask']) else None
                field_name = row['Месяц']
                
                # Извлекаем данные по всем колонкам
                column_data = {}
                for col in df.columns:
                    if col not in ['ac_type_mask', 'Месяц'] and pd.notna(row[col]) and row[col] != 0:
                        column_data[col] = float(row[col])
                
                if column_data and ac_type_mask is not None:
                    record = {
                        'ac_type_mask': int(ac_type_mask),
                        'field_name': field_name,
                        'column_data': column_data,
                        'distribution_type': 'daily_equal'  # Равномерно по дням
                    }
                    ops_data.append(record)
            
            self.logger.info(f"📊 ops_counter: {len(ops_data)} записей")
            for record in ops_data:
                cols_count = len(record['column_data'])
                self.logger.info(f"   {record['field_name']} (ac_type_mask={record['ac_type_mask']}, {cols_count} колонок)")
            
            return ops_data
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка парсинга ops_data: {e}")
            return []
    
    def parse_new_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Парсит данные new_counter_* для размещения на 15-е число месяца"""
        try:
            new_data = []
            
            # Фильтруем строки с new_counter
            new_rows = df[df['Месяц'].str.contains('new_counter', na=False)]
            
            for idx, row in new_rows.iterrows():
                ac_type_mask = row['ac_type_mask'] if pd.notna(row['ac_type_mask']) else None
                field_name = row['Месяц']
                
                # Извлекаем данные по всем колонкам
                column_data = {}
                for col in df.columns:
                    if col not in ['ac_type_mask', 'Месяц'] and pd.notna(row[col]) and row[col] != 0:
                        column_data[col] = float(row[col])
                
                if column_data and ac_type_mask is not None:
                    record = {
                        'ac_type_mask': int(ac_type_mask),
                        'field_name': field_name,
                        'column_data': column_data,
                        'distribution_type': 'last_day_only'  # Только последний день месяца
                    }
                    new_data.append(record)
            
            self.logger.info(f"📊 new_counter: {len(new_data)} записей")
            for record in new_data:
                cols_count = len(record['column_data'])
                self.logger.info(f"   {record['field_name']} (ac_type_mask={record['ac_type_mask']}, {cols_count} колонок)")
            
            return new_data
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка парсинга new_data: {e}")
            return []


class ACTensorEngine:
    """Движок создания тензора для операций и поставок ВС"""
    
    def __init__(self, year_mapping: Dict[int, int]):
        self.year_mapping = year_mapping
        self.logger = logging.getLogger(__name__)
        
    def generate_4000_day_calendar(self, base_date: date) -> List[Tuple[date, int, int, bool]]:
        """Создает календарь на 4000 дней: (дата, месяц, год, is_last_day_of_month)"""
        try:
            calendar_data = []
            current_date = base_date
            
            for day in range(4000):
                month_number = current_date.month
                year_number = current_date.year
                
                # Определяем, последний ли это день месяца
                next_day = current_date + timedelta(days=1)
                is_last_day = next_day.month != current_date.month
                
                calendar_data.append((current_date, month_number, year_number, is_last_day))
                current_date += timedelta(days=1)
            
            self.logger.info(f"📅 Создан календарь: 4000 дней ({base_date} - {current_date - timedelta(days=1)})")
            return calendar_data
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания календаря: {e}")
            raise
    
    def find_exact_column_value(self, target_month: int, target_year: int, 
                               column_data: Dict[str, float], 
                               year_mapping: Dict[str, Tuple[int, int]]) -> float:
        """
        Находит точное значение для конкретного месяца и года
        """
        try:
            for col, value in column_data.items():
                if col in year_mapping:
                    month, year = year_mapping[col]
                    if month == target_month and year == target_year:
                        return value
            return 0.0
        except Exception as e:
            self.logger.error(f"❌ Ошибка поиска точного значения: {e}")
            return 0.0

    def find_last_known_column_value(self, target_month: int, target_year: int,
                                   column_data: Dict[str, float], 
                                   year_mapping: Dict[str, Tuple[int, int]]) -> float:
        """
        Находит последнее известное значение для месяца используя логику размножения
        
        Логика:
        1. Ищем точное совпадение месяца/года
        2. Если нет - ищем последний доступный год для этого месяца
        3. Если нет - ищем последний доступный месяц/год
        """
        try:
            # 1. Точное совпадение месяца и года
            exact_value = self.find_exact_column_value(target_month, target_year, column_data, year_mapping)
            if exact_value != 0.0:
                return exact_value
            
            # 2. Ищем последний доступный год для этого месяца
            best_year = 0
            best_value = 0.0
            
            for col, value in column_data.items():
                if col in year_mapping:
                    month, year = year_mapping[col]
                    if month == target_month and year > best_year:
                        best_year = year
                        best_value = value
            
            if best_value != 0.0:
                return best_value
            
            # 3. Ищем последний доступный месяц (предыдущие месяцы)
            for check_month in range(target_month - 1, 0, -1):
                for col, value in column_data.items():
                    if col in year_mapping:
                        month, year = year_mapping[col]
                        if month == check_month and year >= best_year:
                            best_year = year
                            best_value = value
                if best_value != 0.0:
                    return best_value
            
            # 4. Ищем в следующих месяцах (декабрь → target_month)
            for check_month in range(12, target_month, -1):
                for col, value in column_data.items():
                    if col in year_mapping:
                        month, year = year_mapping[col]
                        if month == check_month and year >= best_year:
                            best_year = year
                            best_value = value
                if best_value != 0.0:
                    return best_value
            
            return 0.0
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка поиска последнего известного значения по колонкам: {e}")
            return 0.0

    def find_last_known_value(self, target_month: int, monthly_data: Dict[int, float]) -> float:
        """
        Находит последнее известное значение для месяца
        
        Логика:
        1. Если есть точное совпадение месяца - используем его
        2. Если нет - ищем последний заполненный месяц (по убыванию от target_month)
        3. Если ничего не найдено до января - ищем от декабря вниз
        """
        try:
            # 1. Точное совпадение
            if target_month in monthly_data and monthly_data[target_month] != 0:
                return monthly_data[target_month]
            
            # 2. Ищем последний заполненный месяц (от target_month вниз до 1)
            for month in range(target_month - 1, 0, -1):
                if month in monthly_data and monthly_data[month] != 0:
                    return monthly_data[month]
            
            # 3. Ищем от декабря вниз до target_month
            for month in range(12, target_month, -1):
                if month in monthly_data and monthly_data[month] != 0:
                    return monthly_data[month]
            
            # 4. Если ничего не найдено - возвращаем 0
            return 0.0
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка поиска последнего известного значения: {e}")
            return 0.0

    def distribute_column_value(self, target_date: date, target_month: int, target_year: int, 
                               column_data: Dict[str, float], distribution_type: str, 
                               year_mapping: Dict[str, Tuple[int, int]]) -> float:
        """
        Распределяет значения из колонок по дням с логикой размножения на все 4000 дней
        
        distribution_type:
        - 'daily_equal': ops_counter - размножаем на все 4000 дней используя последнее известное значение
        - 'last_day_only': new_counter - только для конкретного года/месяца в последний день
        
        year_mapping: соответствие колонка → (месяц, год)
        """
        try:
            # Проверяем что месяц совпадает с целевой датой
            if target_date.month != target_month:
                return 0.0
            
            if distribution_type == 'daily_equal':
                # ops_counter: размножаем на все 4000 дней используя последнее известное значение
                monthly_value = self.find_last_known_column_value(target_month, target_year, column_data, year_mapping)
                return monthly_value
                
            elif distribution_type == 'last_day_only':
                # new_counter: только для конкретного года/месяца в середину месяца (15-е число)
                monthly_value = self.find_exact_column_value(target_month, target_year, column_data, year_mapping)
                if monthly_value == 0.0:
                    return 0.0
                return monthly_value if target_date.day == 15 else 0.0
            
            else:
                self.logger.error(f"❌ Неизвестный тип распределения: {distribution_type}")
                return 0.0
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка распределения значения: {e}")
            return 0.0


class ProgramACDirectLoader:
    """Главный загрузчик - прямое создание тензора flight_program_ac"""
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.client = None
        self.days_count = 4000
        
    def _setup_logging(self) -> logging.Logger:
        """Настройка логирования"""
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def connect_to_database(self) -> bool:
        """Подключение к ClickHouse"""
        try:
            self.client = get_clickhouse_client()
            result = self.client.execute('SELECT 1 as test')
            self.logger.info("✅ Подключение к ClickHouse успешно!")
            return True
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения: {e}")
            return False
    
    def get_base_date(self) -> date:
        """Получение базовой даты - последняя version_date из heli_pandas"""
        try:
            query = """
            SELECT MAX(version_date) as last_version_date
            FROM heli_pandas
            WHERE version_date IS NOT NULL
            """
            result = self.client.execute(query)
            base_date = result[0][0]
            self.logger.info(f"📅 Базовая дата (последняя version_date): {base_date}")
            return base_date
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения базовой даты: {e}")
            return datetime.now().date()
    
    def get_ac_types(self) -> List[int]:
        """Получает список типов ВС из анализа Excel данных"""
        try:
            # Будет определено из данных Excel
            return [32, 64]  # По умолчанию Ми-8 и Ми-17
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения типов ВС: {e}")
            return []
    
    def create_flight_program_ac_table(self, version_date: date) -> bool:
        """Создание таблицы flight_program_ac (если не существует) и очистка данных для version_date"""
        try:
            # Создаем таблицу если не существует (не удаляем!)
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS flight_program_ac (
                dates Date,                        -- переименовано из flight_date
                ops_counter_mi8 UInt16,            -- счетчики операций: 0-65535 достаточно
                ops_counter_mi17 UInt16,           -- счетчики операций: 0-65535 достаточно
                ops_counter_total UInt16,          -- вычисляемое поле: сумма двух UInt16
                new_counter_mi17 UInt8,            -- новые поставки: 0-255 достаточно
                trigger_program_mi8 Int8,          -- триггеры: -128 до 127 достаточно
                trigger_program_mi17 Int8,         -- триггеры: -128 до 127 достаточно
                trigger_program Int8,              -- триггеры: -128 до 127 достаточно
                version_date Date DEFAULT today(),
                version_id UInt8 DEFAULT 1
            ) ENGINE = MergeTree()
            ORDER BY (version_date, dates)
            SETTINGS index_granularity = 8192
            """
            
            self.client.execute(create_table_sql)
            
            # Удаляем только записи с текущим version_date (rewrite policy)
            delete_sql = f"ALTER TABLE flight_program_ac DELETE WHERE version_date = '{version_date}'"
            self.client.execute(delete_sql)
            # Ждём завершения мутации
            self.client.execute("OPTIMIZE TABLE flight_program_ac FINAL")
            self.logger.info(f"✅ Таблица flight_program_ac: удалены записи для version_date={version_date}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания таблицы: {e}")
            return False
    
    def generate_tensor_data(self, ops_data: List[Dict], new_data: List[Dict], 
                           tensor_engine: ACTensorEngine, calendar: List[Tuple],
                           year_mapping: Dict[str, Tuple[int, int]], 
                           base_date: date, version_id: int = 1) -> List[List]:
        """Генерирует данные тензора в flat структуре (одна запись на дату)"""
        try:
            self.logger.info("🔄 Генерация оптимизированных данных тензора...")
            
            # Создаем индексы данных по полям для быстрого доступа
            ops_data_by_field = {}
            new_data_by_field = {}
            
            for record in ops_data:
                field_name = record['field_name']
                ops_data_by_field[field_name] = record
            
            for record in new_data:
                field_name = record['field_name'] 
                new_data_by_field[field_name] = record
            
            insert_data = []
            
            self.logger.info(f"📊 Генерация flat-структуры: {len(calendar):,} дат")
            
            # Генерируем одну запись на дату со всеми полями
            for flight_date, month_number, year_number, is_last_day in calendar:
                
                # Получаем значения для всех полей (приводим к целым числам)
                ops_mi8 = 0
                ops_mi17 = 0
                new_mi17 = 0
                
                # ops_counter_mi8 (приводим к UInt16)
                if 'ops_counter_mi8' in ops_data_by_field:
                    record = ops_data_by_field['ops_counter_mi8']
                    ops_mi8 = int(tensor_engine.distribute_column_value(
                        flight_date, month_number, year_number, record['column_data'],
                        record['distribution_type'], year_mapping
                    ))
                
                # ops_counter_mi17 (приводим к UInt16)
                if 'ops_counter_mi17' in ops_data_by_field:
                    record = ops_data_by_field['ops_counter_mi17']
                    ops_mi17 = int(tensor_engine.distribute_column_value(
                        flight_date, month_number, year_number, record['column_data'],
                        record['distribution_type'], year_mapping
                    ))
                
                # new_counter_mi17 (приводим к UInt8)
                if 'new_counter_mi17' in new_data_by_field:
                    record = new_data_by_field['new_counter_mi17']
                    new_mi17 = int(tensor_engine.distribute_column_value(
                        flight_date, month_number, year_number, record['column_data'],
                        record['distribution_type'], year_mapping
                    ))
                    
                # Вычисляемые поля (рассчитываются позже в add_calculated_fields)
                ops_total = 0  # будет рассчитано позже (UInt16)
                trigger_mi8 = 0  # будет рассчитано позже (Int8)
                trigger_mi17 = 0  # будет рассчитано позже (Int8) 
                trigger_total = 0  # будет рассчитано позже (Int8)
                
                # Добавляем запись с flat структурой
                insert_data.append([
                    flight_date,    # dates
                    ops_mi8,        # ops_counter_mi8
                    ops_mi17,       # ops_counter_mi17
                    ops_total,      # ops_counter_total (рассчитается позже)
                    new_mi17,       # new_counter_mi17
                    trigger_mi8,    # trigger_program_mi8 (рассчитается позже)
                    trigger_mi17,   # trigger_program_mi17 (рассчитается позже)
                    trigger_total,  # trigger_program (рассчитается позже)
                    base_date,      # version_date
                    version_id      # version_id
                ])
            
            self.logger.info(f"✅ Сгенерировано {len(insert_data):,} записей flat-тензора")
            return insert_data
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка генерации тензора: {e}")
            raise
    
    def insert_tensor_data(self, insert_data: List[List]) -> bool:
        """Массовая вставка оптимизированного тензора в ClickHouse"""
        try:
            self.logger.info(f"💾 Начинаем вставку {len(insert_data):,} записей в flat структуру...")
            
            # Новая структура колонок (flat)
            column_names = [
                'dates', 'ops_counter_mi8', 'ops_counter_mi17', 'ops_counter_total',
                'new_counter_mi17', 'trigger_program_mi8', 'trigger_program_mi17', 
                'trigger_program', 'version_date', 'version_id'
            ]
            
            # Вставляем батчами
            batch_size = 100000
            for i in range(0, len(insert_data), batch_size):
                batch = insert_data[i:i + batch_size]
                self.client.execute('INSERT INTO flight_program_ac VALUES', batch)
                self.logger.info(f"📦 Вставлено {i + len(batch):,} / {len(insert_data):,} записей")
            
            self.logger.info("✅ Оптимизированные данные успешно загружены в flight_program_ac")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка вставки данных: {e}")
            return False
    
    def validate_tensor(self) -> bool:
        """Валидация оптимизированного тензора flight_program_ac"""
        try:
            self.logger.info("🔍 === ВАЛИДАЦИЯ ОПТИМИЗИРОВАННОГО ТЕНЗОРА ===")
            
            # 1. Общая статистика
            stats_query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT dates) as unique_dates,
                MIN(dates) as min_date,
                MAX(dates) as max_date,
                SUM(ops_counter_mi8 + ops_counter_mi17 + new_counter_mi17) as total_sum,
                COUNT(CASE WHEN ops_counter_total > 0 THEN 1 END) as non_zero_records
            FROM flight_program_ac
            """
            stats_result = self.client.execute(stats_query)
            row = stats_result[0]
            
            self.logger.info(f"📊 Общая статистика:")
            self.logger.info(f"   Всего записей: {row[0]:,}")
            self.logger.info(f"   Уникальных дат: {row[1]:,}")
            self.logger.info(f"   Период: {row[2]} - {row[3]}")
            self.logger.info(f"   Общая сумма значений: {row[4]:.1f}")
            self.logger.info(f"   Записей с ops_counter_total > 0: {row[5]:,}")
            
            # 2. Проверка полей
            field_stats = self.client.execute("""
            SELECT 
                'ops_counter_mi8' as field_name,
                toInt64(SUM(ops_counter_mi8)) as total_sum,
                toInt64(COUNT(CASE WHEN ops_counter_mi8 > 0 THEN 1 END)) as non_zero
            FROM flight_program_ac
            UNION ALL
            SELECT 
                'ops_counter_mi17' as field_name,
                toInt64(SUM(ops_counter_mi17)) as total_sum,
                toInt64(COUNT(CASE WHEN ops_counter_mi17 > 0 THEN 1 END)) as non_zero
            FROM flight_program_ac
            UNION ALL
            SELECT 
                'ops_counter_total' as field_name,
                toInt64(SUM(ops_counter_total)) as total_sum,
                toInt64(COUNT(CASE WHEN ops_counter_total > 0 THEN 1 END)) as non_zero
            FROM flight_program_ac
            UNION ALL
            SELECT 
                'trigger_program' as field_name,
                toInt64(SUM(trigger_program)) as total_sum,
                toInt64(COUNT(CASE WHEN trigger_program != 0 THEN 1 END)) as non_zero
            FROM flight_program_ac
            """)
            
            self.logger.info(f"📋 Статистика по полям:")
            for field_name, total_sum, non_zero in field_stats:
                self.logger.info(f"   {field_name}: сумма={total_sum:.1f}, ненулевых={non_zero:,}")
            
            # 3. Простые проверки качества
            issues = []
            
            if row[0] == 0:
                issues.append("❌ Нет записей в таблице")
            
            if row[1] != 4000:
                issues.append(f"❌ Неправильное количество дней: {row[1]} вместо 4000")
            
            if row[4] == 0:
                issues.append("❌ Все значения равны нулю")
            
            # Результат валидации
            if issues:
                self.logger.warning(f"⚠️ Обнаружены проблемы:")
                for issue in issues:
                    self.logger.warning(f"  {issue}")
                return False
            else:
                self.logger.info(f"✅ Все проверки валидации пройдены успешно!")
                self.logger.info(f"✅ Оптимизированный тензор готов для использования!")
                return True
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка валидации: {e}")
            return False
    
    def add_calculated_fields(self, version_date: date) -> bool:
        """Обновляет вычисляемые поля для конкретного version_date в flight_program_ac"""
        try:
            self.logger.info(f"🔄 === ПОСТПРОЦЕССИНГ для version_date={version_date} ===")
            
            # 1. ops_counter_total = ops_counter_mi8 + ops_counter_mi17 (простое UPDATE)
            self.logger.info("📊 Расчёт ops_counter_total...")
            total_update = f"""
            ALTER TABLE flight_program_ac
            UPDATE ops_counter_total = ops_counter_mi8 + ops_counter_mi17
            WHERE version_date = '{version_date}'
            """
            self.client.execute(total_update)
            
            # 2. Trigger поля вычисляем через временную таблицу
            self.logger.info("📊 Расчёт trigger полей через временную таблицу...")
            
            # Удаляем временную таблицу если осталась
            self.client.execute("DROP TABLE IF EXISTS flight_program_ac_temp")
            
            # Создаем временную таблицу ТОЛЬКО для текущего version_date
            temp_calc_sql = f"""
            CREATE TABLE flight_program_ac_temp (
                dates Date,
                ops_counter_mi8 UInt16,
                ops_counter_mi17 UInt16,
                ops_counter_total UInt16,
                new_counter_mi17 UInt8,
                trigger_program_mi8 Int8,
                trigger_program_mi17 Int8,
                trigger_program Int8,
                version_date Date,
                version_id UInt8
            ) ENGINE = MergeTree()
            ORDER BY (version_date, dates)
            AS
                SELECT 
                dates,
                ops_counter_mi8,
                ops_counter_mi17, 
                ops_counter_total,
                new_counter_mi17,
                toInt8(ops_counter_mi8 - lagInFrame(ops_counter_mi8, 1, 0) 
                    OVER (PARTITION BY version_date ORDER BY dates ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as trigger_program_mi8,
                toInt8(ops_counter_mi17 - lagInFrame(ops_counter_mi17, 1, 0)
                    OVER (PARTITION BY version_date ORDER BY dates ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as trigger_program_mi17,
                toInt8(0) as trigger_program,
                version_date,
                version_id
            FROM flight_program_ac
            WHERE version_date = '{version_date}'
            ORDER BY dates
            """
            self.client.execute(temp_calc_sql)
            
            # Добавляем trigger_program = trigger_program_mi8 + trigger_program_mi17  
            update_trigger_total_sql = """
            ALTER TABLE flight_program_ac_temp
            UPDATE trigger_program = trigger_program_mi8 + trigger_program_mi17
            WHERE 1 = 1
            """
            self.client.execute(update_trigger_total_sql)
            self.client.execute("OPTIMIZE TABLE flight_program_ac_temp FINAL")
            
            # Удаляем данные текущего version_date из основной таблицы и вставляем обновленные
            self.logger.info("📊 Замена данных в основной таблице...")
            self.client.execute(f"ALTER TABLE flight_program_ac DELETE WHERE version_date = '{version_date}'")
            self.client.execute("OPTIMIZE TABLE flight_program_ac FINAL")
            self.client.execute("INSERT INTO flight_program_ac SELECT * FROM flight_program_ac_temp")
            self.client.execute("DROP TABLE flight_program_ac_temp")
            
            # Проверяем результаты расчётов
            self.logger.info("🔍 Проверка результатов постпроцессинга...")
            stats_query = f"""
            SELECT 
                COUNT(*) as total_records,
                toInt64(SUM(ops_counter_total)) as sum_total,
                toInt64(SUM(trigger_program)) as sum_trigger,
                COUNT(CASE WHEN ops_counter_total > 0 THEN 1 END) as non_zero_total,
                COUNT(CASE WHEN trigger_program != 0 THEN 1 END) as non_zero_trigger
            FROM flight_program_ac 
            WHERE version_date = '{version_date}'
            """
            
            stats_result = self.client.execute(stats_query)
            
            if stats_result:
                row = stats_result[0]
                self.logger.info("✅ Результаты постпроцессинга:")
                self.logger.info(f"   Всего записей: {row[0]:,}")
                self.logger.info(f"   Сумма ops_counter_total: {row[1]:.1f}")
                self.logger.info(f"   Сумма trigger_program: {row[2]:.1f}")
                self.logger.info(f"   Записей с ops_counter_total > 0: {row[3]:,}")
                self.logger.info(f"   Записей с trigger_program ≠ 0: {row[4]:,}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка постпроцессинга: {e}")
            return False
    
    def correct_first_trigger_values(self) -> bool:
        """Корректирует первые значения trigger полей ПОСЛЕ загрузки heli_pandas"""
        try:
            self.logger.info("🔄 === КОРРЕКТИРОВКА ПЕРВЫХ ЗНАЧЕНИЙ TRIGGER ПОЛЕЙ ===")
            
            # Получаем первую дату из новой структуры
            first_date_query = "SELECT MIN(dates) FROM flight_program_ac"
            first_date_result = self.client.execute(first_date_query)
            first_date = first_date_result[0][0]
            self.logger.info(f"📅 Первая дата в календаре: {first_date}")
            
            # 1. Корректировка trigger_program_mi8 (group_by=1)
            self.logger.info("🔧 Корректировка trigger_program_mi8 (МИ-8, group_by=1)...")
            
            # Подсчитываем компоненты МИ-8 в статусе 2
            mi8_count_query = """
            SELECT COUNT(*) as component_count
            FROM heli_pandas hp
            WHERE hp.partseqno_i IN (
                SELECT partno_comp 
                FROM md_components 
                WHERE group_by = 1
            )
            AND hp.status_id = 2
            """
            mi8_count_result = self.client.execute(mi8_count_query)
            mi8_component_count = mi8_count_result[0][0]
            self.logger.info(f"   МИ-8 компонентов в статусе 2: {mi8_component_count}")
            
            # Получаем текущее первое значение ops_counter_mi8 из новой структуры
            mi8_first_ops_query = f"""
            SELECT ops_counter_mi8 
            FROM flight_program_ac 
            WHERE dates = '{first_date}'
            """
            mi8_first_result = self.client.execute(mi8_first_ops_query)
            mi8_first_ops = mi8_first_result[0][0] if mi8_first_result else 0
            self.logger.info(f"   Текущее значение ops_counter_mi8 на первую дату: {mi8_first_ops}")
            
            # Вычисляем корректировку: компоненты в статусе 2 - ops_counter
            mi8_correction = mi8_component_count - mi8_first_ops
            self.logger.info(f"   Корректировка МИ-8: {mi8_component_count} - {mi8_first_ops} = {mi8_correction}")
            
            # Обновляем первое значение в новой структуре
            mi8_update_query = f"""
            ALTER TABLE flight_program_ac 
            UPDATE trigger_program_mi8 = {mi8_correction}
            WHERE dates = '{first_date}'
            """
            self.client.execute(mi8_update_query)
            
            # 2. Корректировка trigger_program_mi17 (group_by=2)
            self.logger.info("🔧 Корректировка trigger_program_mi17 (МИ-17, group_by=2)...")
            
            # Подсчитываем компоненты МИ-17 в статусе 2
            mi17_count_query = """
            SELECT COUNT(*) as component_count
            FROM heli_pandas hp
            WHERE hp.partseqno_i IN (
                SELECT partno_comp 
                FROM md_components 
                WHERE group_by = 2
            )
            AND hp.status_id = 2
            """
            mi17_count_result = self.client.execute(mi17_count_query)
            mi17_component_count = mi17_count_result[0][0]
            self.logger.info(f"   МИ-17 компонентов в статусе 2: {mi17_component_count}")
            
            # Получаем текущее первое значение ops_counter_mi17 из новой структуры
            mi17_first_ops_query = f"""
            SELECT ops_counter_mi17 
            FROM flight_program_ac 
            WHERE dates = '{first_date}'
            """
            mi17_first_result = self.client.execute(mi17_first_ops_query)
            mi17_first_ops = mi17_first_result[0][0] if mi17_first_result else 0
            self.logger.info(f"   Текущее значение ops_counter_mi17 на первую дату: {mi17_first_ops}")
            
            # Вычисляем корректировку: компоненты в статусе 2 - ops_counter
            mi17_correction = mi17_component_count - mi17_first_ops
            self.logger.info(f"   Корректировка МИ-17: {mi17_component_count} - {mi17_first_ops} = {mi17_correction}")
            
            # Обновляем первое значение в новой структуре
            mi17_update_query = f"""
            ALTER TABLE flight_program_ac 
            UPDATE trigger_program_mi17 = {mi17_correction}
            WHERE dates = '{first_date}'
            """
            self.client.execute(mi17_update_query)
            
            # 3. Пересчитываем trigger_program = trigger_program_mi8 + trigger_program_mi17 в новой структуре
            self.logger.info("🔧 Пересчет trigger_program...")
            trigger_total_update_query = f"""
            ALTER TABLE flight_program_ac 
            UPDATE trigger_program = trigger_program_mi8 + trigger_program_mi17
            WHERE dates = '{first_date}'
            """
            self.client.execute(trigger_total_update_query)
            
            # 4. Проверяем результаты в новой структуре
            verification_query = f"""
            SELECT 
                trigger_program_mi8,
                trigger_program_mi17,
                trigger_program
            FROM flight_program_ac 
            WHERE dates = '{first_date}'
            """
            verification_result = self.client.execute(verification_query)
            
            if verification_result:
                mi8_val, mi17_val, total_val = verification_result[0]
                self.logger.info("✅ Корректировка завершена:")
                self.logger.info(f"   trigger_program_mi8 (МИ-8): {mi8_val}")
                self.logger.info(f"   trigger_program_mi17 (МИ-17): {mi17_val}")
                self.logger.info(f"   trigger_program (Общий): {total_val}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка корректировки trigger полей: {e}")
            return False
    
    def create_final_tensor(self, excel_path: str = None,
                          version_date: Optional[date] = None, version_id: int = 1) -> bool:
        """Главная функция создания финального тензора"""
        try:
            # Определяем путь к Excel
            if excel_path is None:
                from utils.version_utils import get_dataset_path
                dataset_path = get_dataset_path()
                if dataset_path:
                    excel_path = str(dataset_path / 'Program_heli.xlsx')
                else:
                    excel_path = 'data_input/source_data/Program_heli.xlsx'
            
            self.logger.info("🚀 === PROGRAM AC DIRECT LOADER ===")
            self.logger.info(f"Создание тензора flight_program_ac из {excel_path}")
            self.logger.info("Размер: поля × типы ВС × 4000 дней")
            
            # 1. Анализ Excel структуры
            analyzer = ProgramHeliAnalyzer(excel_path)
            excel_data = analyzer.analyze_excel_structure()
            
            # 2. Получение базовой даты
            if version_date is None:
                version_date = self.get_base_date()
            
            # 3. Создание движка тензора
            tensor_engine = ACTensorEngine(excel_data['year_mapping'])
            calendar = tensor_engine.generate_4000_day_calendar(version_date)
            
            # 4. Создание таблицы (и очистка данных для текущей version_date)
            if not self.create_flight_program_ac_table(version_date):
                return False
            
            # 5. Генерация данных тензора
            insert_data = self.generate_tensor_data(
                excel_data['ops_data'], excel_data['new_data'],
                tensor_engine, calendar, excel_data['year_mapping'], 
                version_date, version_id
            )
            
            # 6. Вставка данных
            if not self.insert_tensor_data(insert_data):
                return False
            
            # 7. Постпроцессинг - добавление вычисляемых полей
            if not self.add_calculated_fields(version_date):
                self.logger.warning("⚠️ Ошибка постпроцессинга, но основные данные загружены")
            
            # 8. Корректировка первых значений trigger полей (ПОСЛЕ загрузки heli_pandas)
            if not self.correct_first_trigger_values():
                self.logger.warning("⚠️ Ошибка корректировки trigger полей, но основные данные загружены")
            
            # 9. Валидация
            validation_success = self.validate_tensor()
            
            if validation_success:
                self.logger.info("🎉 === ТЕНЗОР FLIGHT_PROGRAM_AC ГОТОВ ===")
                self.logger.info(f"📅 Версия данных: {version_date} (version_id={version_id})")
                self.logger.info(f"📊 Размер тензора: {len(insert_data):,} записей")
                self.logger.info(f"🔥 Готов для использования!")
            else:
                self.logger.warning("⚠️ Тензор создан, но есть проблемы с валидацией")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка создания тензора: {e}")
            return False


def main(version_date: Optional[str] = None, version_id: Optional[int] = None):
    """Главная функция с поддержкой версионирования"""
    print("🚀 === PROGRAM AC DIRECT LOADER ===")
    print("Прямое создание тензора flight_program_ac из Program_heli.xlsx")
    print()
    
    loader = ProgramACDirectLoader()
    
    # Подключение к БД
    if not loader.connect_to_database():
        print("❌ Не удалось подключиться к базе данных")
        return False
    
    # Парсинг параметров версионирования
    parsed_version_date = None
    parsed_version_id = 1
    
    if version_date and version_id:
        try:
            parsed_version_date = datetime.strptime(version_date, '%Y-%m-%d').date()
            parsed_version_id = int(version_id)
            print(f"🗓️ Версия данных (из параметров): {parsed_version_date}, version_id: {parsed_version_id}")
        except ValueError as e:
            print(f"❌ Ошибка парсинга параметров версии: {e}")
            return False
    
    # Создание тензора
    success = loader.create_final_tensor(
        version_date=parsed_version_date,
        version_id=parsed_version_id
    )
    
    if success:
        print("✅ Прямой загрузчик AC завершен успешно!")
        return True
    else:
        print("❌ Ошибка в работе прямого загрузчика AC!")
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Program AC Direct Loader для Helicopter Component Lifecycle')
    parser.add_argument('--version-date', type=str, help='Дата версии (YYYY-MM-DD)')
    parser.add_argument('--version-id', type=int, help='ID версии')
    parser.add_argument('--dataset-path', type=str, help='Путь к папке датасета (v_YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Устанавливаем путь к датасету если передан
    if args.dataset_path:
        from utils.version_utils import set_dataset_path
        set_dataset_path(args.dataset_path)
    
    success = main(version_date=args.version_date, version_id=args.version_id)
    sys.exit(0 if success else 1) 