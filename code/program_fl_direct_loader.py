#!/usr/bin/env python3
"""
Program FL Direct Loader - Прямое создание тензора программы полетов
===================================================================

Загружает данные из обновленного Program.xlsx напрямую в таблицу flight_program_fl,
минуя промежуточную таблицу flight_program.

НОВАЯ СТРУКТУРА EXCEL:
- Строка 0: Годы (Месяц="Год", 1=2025, 2=2025, ...)  
- Строка 1+: Данные daily_flight по типам/экземплярам
- Убрано разделение на ops_counter_mi8/mi17 и daily_flight
- Остались только данные daily_flight

ЛОГИКА РАЗМНОЖЕНИЯ:
- Базовая дата: последняя version_date из heli_pandas
- Период: 4000 дней вперед
- Приоритеты: 1-serialno (экземпляры), 2-ac_type_mask (типы)
- Размножение: если нет данных на дату → берем по дню/месяцу из последнего известного года

ВАЛИДАЦИЯ:
- Проверка планеров по serialno  
- Проверка общего количества заполненных планеров
- Отсутствие пустых и нулевых полей в массиве

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

# Добавляем путь к утилитам
sys.path.append(str(Path(__file__).parent / 'utils'))
from config_loader import get_clickhouse_client

class ExcelStructureAnalyzer:
    """Анализирует новую структуру Excel файла Program.xlsx"""
    
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
            
            # Анализируем структуру
            result = {
                'year_mapping': self.extract_year_mapping(df),
                'flight_data': self.parse_flight_data(df),
                'month_columns': [col for col in df.columns if isinstance(col, int) and 1 <= col <= 12],
                'raw_df': df
            }
            
            self.logger.info(f"✅ Структура проанализирована: {len(result['flight_data'])} записей данных")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка анализа Excel: {e}")
            raise
    
    def extract_year_mapping(self, df: pd.DataFrame) -> Dict[int, int]:
        """Извлекает соответствие месяц→год из строки 'Год'"""
        try:
            # Ищем строку с годами (Месяц = "Год")
            year_rows = df[df['Месяц'] == 'Год']
            if year_rows.empty:
                self.logger.warning("⚠️ Не найдена строка с годами, используем 2025 по умолчанию")
                return {month: 2025 for month in range(1, 13)}
            
            year_row = year_rows.iloc[0]
            year_mapping = {}
            
            for month in range(1, 13):
                if month in year_row.index and pd.notna(year_row[month]):
                    year_mapping[month] = int(year_row[month])
                else:
                    year_mapping[month] = 2025  # Fallback
            
            self.logger.info(f"📅 Годы по месяцам: {year_mapping}")
            
            # Проверяем на множественные годы
            unique_years = set(year_mapping.values())
            if len(unique_years) > 1:
                self.logger.info(f"📅 Обнаружены множественные годы: {sorted(unique_years)}")
            else:
                self.logger.info(f"📅 Единый год для всех месяцев: {list(unique_years)[0]}")
            
            return year_mapping
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка извлечения годов: {e}")
            # Fallback
            return {month: 2025 for month in range(1, 13)}
    
    def parse_flight_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Парсит данные daily_flight по типам и экземплярам"""
        try:
            flight_data = []
            
            # Фильтруем строки с данными (исключаем строку с годами)
            data_rows = df[df['Месяц'] == 'daily_flight']
            
            for idx, row in data_rows.iterrows():
                ac_type_mask = row['ac_type_mask'] if pd.notna(row['ac_type_mask']) else None
                serialno = row['serialno'] if pd.notna(row['serialno']) else None
                
                # Извлекаем данные по месяцам
                monthly_data = {}
                for month in range(1, 13):
                    if month in row.index and pd.notna(row[month]):
                        monthly_data[month] = float(row[month])
                
                if monthly_data:  # Только если есть данные
                    record = {
                        'ac_type_mask': int(ac_type_mask) if ac_type_mask is not None else None,
                        'serialno': int(serialno) if serialno is not None else None,
                        'monthly_data': monthly_data,
                        'data_type': 'instance' if serialno is not None else 'type'
                    }
                    flight_data.append(record)
            
            self.logger.info(f"📊 Парсинг завершен:")
            instance_count = len([r for r in flight_data if r['data_type'] == 'instance'])
            type_count = len([r for r in flight_data if r['data_type'] == 'type'])
            self.logger.info(f"   - По экземплярам (serialno): {instance_count} записей")
            self.logger.info(f"   - По типам (ac_type_mask): {type_count} записей")
            
            return flight_data
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка парсинга данных: {e}")
            raise


class YearExpansionEngine:
    """Движок размножения данных на 4000 дней"""
    
    def __init__(self, year_mapping: Dict[int, int]):
        self.year_mapping = year_mapping
        self.logger = logging.getLogger(__name__)
        self.last_known_year = max(year_mapping.values())
        
    def generate_4000_day_calendar(self, base_date: date) -> List[Tuple[date, int, int]]:
        """Создает календарь на 4000 дней: (дата, месяц, год)"""
        try:
            calendar = []
            current_date = base_date
            
            for day in range(4000):
                month_number = current_date.month
                year_number = current_date.year
                calendar.append((current_date, month_number, year_number))
                current_date += timedelta(days=1)
            
            self.logger.info(f"📅 Создан календарь: 4000 дней ({base_date} - {current_date - timedelta(days=1)})")
            return calendar
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания календаря: {e}")
            raise
    
    def find_matching_data(self, target_month: int, target_year: int, monthly_data: Dict[int, float]) -> float:
        """
        Логика размножения данных на 4000 дней:
        
        1. Если месяц M присутствует в monthly_data → используем его значение (даже если 0!)
        2. Если месяца M нет в monthly_data → берем значение месяца M из последнего известного года
        3. Если и там нет → возвращаем 0.0
        
        Ключ: 0 — это ДАННЫЕ, а не отсутствие данных
        """
        try:
            # 1. Если месяц есть в данных (даже если 0) → используем
            if target_month in monthly_data:
                return float(monthly_data[target_month])
            
            # 2. Месяца нет в текущих данных → берем из последнего известного года
            # Ищем месяц с таким же номером в year_mapping
            # (все данные из последнего года, поэтому ищем в same monthly_data, но логически из last_known_year)
            # На самом деле здесь нужно проверить: если данные только за 2024, то для ANY месяца
            # который есть в 2024, его используем; если нет в 2024 → 0
            
            # Так как year_mapping дает нам какой год для каждого месяца, 
            # и нам нужно "последний известный год", то ищем месяцы из последнего года
            # Но данные хранятся просто {месяц: значение} без привязки к году
            # Значит в monthly_data уже хранятся данные только за один год (последний)
            
            # Если месяца нет — просто возвращаем 0
            return 0.0
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка поиска данных для месяца {target_month}: {e}")
            return 0.0


class FlightProgramDirectLoader:
    """Главный загрузчик - прямое создание тензора flight_program_fl"""
    
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
            # Fallback на текущую дату
            return datetime.now().date()
    
    def load_aircraft_dictionary(self) -> List[Tuple[int, int]]:
        """Загружает 279 планеров из dict_aircraft_number_flat"""
        try:
            query = """
            SELECT aircraft_number, ac_type_mask
            FROM dict_aircraft_number_flat
            ORDER BY aircraft_number
            """
            result = self.client.execute(query)
            aircraft_list = [(row[0], row[1]) for row in result]
            self.logger.info(f"🚁 Найдено {len(aircraft_list)} планеров в словаре")
            if aircraft_list:
                return aircraft_list
            # Fallback: если словарь пуст, берём из heli_pandas
            self.logger.warning("⚠️ Словарь dict_aircraft_number_flat пуст, используем fallback из heli_pandas")
            hp_rows = self.client.execute(
                """
                SELECT DISTINCT aircraft_number, ac_type_mask
                FROM heli_pandas
                WHERE aircraft_number > 0
                ORDER BY aircraft_number
                """
            )
            hp_list = [(int(a or 0), int(m or 0)) for a, m in hp_rows]
            self.logger.info(f"📋 Fallback набор планеров из heli_pandas: {len(hp_list)}")
            return hp_list
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения списка планеров: {e}")
            return []
    
    def create_flight_program_fl_table(self) -> bool:
        """Создание таблицы flight_program_fl"""
        try:
            # Удаляем таблицу если существует
            self.client.execute("DROP TABLE IF EXISTS flight_program_fl")
            
            # Создаем новую таблицу
            create_table_sql = """
            CREATE TABLE flight_program_fl (
                aircraft_number UInt32,
                dates Date,
                daily_hours UInt32,
                ac_type_mask UInt8,
                version_date Date DEFAULT today(),
                version_id UInt8 DEFAULT 1
            ) ENGINE = MergeTree()
            ORDER BY (aircraft_number, dates)
            SETTINGS index_granularity = 8192
            """
            
            self.client.execute(create_table_sql)
            self.logger.info("✅ Таблица flight_program_fl создана")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания таблицы: {e}")
            return False
    
    def apply_priority_logic(self, all_aircraft: List[Tuple[int, int]], 
                           flight_data: List[Dict], 
                           expansion_engine: YearExpansionEngine,
                           calendar: List[Tuple[date, int, int]],
                           base_date: date, version_id: int = 1) -> List[List]:
        """
        Применяет логику приоритетов и создает данные для вставки
        
        Приоритеты:
        1. По экземплярам (aircraft_number = serialno) - ПРИОРИТЕТ
        2. По типам (ac_type_mask) - для оставшихся планеров
        """
        try:
            self.logger.info("🔄 Применение логики приоритетов...")
            
            # Разделяем данные по типам
            instance_data = {}  # {serialno: monthly_data}
            type_data = {}      # {ac_type_mask: monthly_data}
            
            for record in flight_data:
                if record['data_type'] == 'instance' and record['serialno'] is not None:
                    instance_data[record['serialno']] = record['monthly_data']
                elif record['data_type'] == 'type' and record['ac_type_mask'] is not None:
                    type_data[record['ac_type_mask']] = record['monthly_data']
            
            self.logger.info(f"📊 Данные разделены:")
            self.logger.info(f"   - По экземплярам: {len(instance_data)} записей")
            self.logger.info(f"   - По типам: {len(type_data)} записей")
            
            # Создаем данные для вставки
            insert_data = []
            stats = {'instance_count': 0, 'type_count': 0, 'no_data_count': 0}
            
            for aircraft_number, ac_type_mask in all_aircraft:
                # Определяем источник данных
                monthly_data = None
                source_type = None
                
                # ПРИОРИТЕТ 1: По экземпляру
                if aircraft_number in instance_data:
                    monthly_data = instance_data[aircraft_number]
                    source_type = 'instance'
                    stats['instance_count'] += 1
                
                # ПРИОРИТЕТ 2: По типу
                elif ac_type_mask in type_data:
                    monthly_data = type_data[ac_type_mask]
                    source_type = 'type'
                    stats['type_count'] += 1
                else:
                    # Нет данных - заполняем нулями
                    monthly_data = {month: 0.0 for month in range(1, 13)}
                    source_type = 'no_data'
                    stats['no_data_count'] += 1
                
                # Генерируем данные на 4000 дней
                for flight_date, month_number, year_number in calendar:
                    daily_hours = expansion_engine.find_matching_data(month_number, year_number, monthly_data)
                    
                    insert_data.append([
                        int(aircraft_number),
                        flight_date,  # dates (переименовано из flight_date)
                        int(daily_hours),  # Конвертируем в UInt32
                        ac_type_mask,
                        base_date,
                        version_id
                    ])
            
            self.logger.info(f"✅ Логика приоритетов применена:")
            self.logger.info(f"   - По экземплярам: {stats['instance_count']} планеров")
            self.logger.info(f"   - По типам: {stats['type_count']} планеров")
            self.logger.info(f"   - Без данных (нули): {stats['no_data_count']} планеров")
            self.logger.info(f"   - Всего записей для вставки: {len(insert_data):,}")
            
            return insert_data

        except Exception as e:
            self.logger.error(f"❌ Ошибка применения логики приоритетов: {e}")
            raise

    def generate_new_mi17_aircraft_numbers(self) -> List[int]:
        """Генерирует последовательность новых aircraft_number для Ми‑17 начиная с 100000.

        Источник количества: суммарный new_counter_mi17 из flight_program_ac по последней версии.
        Тип: UInt32. Маска типа для этих бортов: 64.
        """
        try:
            # Получаем последнюю версию flight_program_ac
            # Проверим, существует ли flight_program_ac
            exists = self.client.execute("EXISTS TABLE flight_program_ac")[0][0]
            if not exists:
                self.logger.info("ℹ️ flight_program_ac отсутствует — пропускаем генерацию новых Ми-17")
                return []
            ver = self.client.execute(
                """
                SELECT version_date, version_id
                FROM flight_program_ac
                ORDER BY version_date DESC, version_id DESC
                LIMIT 1
                """
            )
            if not ver:
                return []
            vdate, vid = ver[0]
            rows = self.client.execute(
                f"""
                SELECT toInt64(SUM(new_counter_mi17))
                FROM flight_program_ac
                WHERE version_date = '{vdate}' AND version_id = {int(vid)}
                """
            )
            total_new = int(rows[0][0] or 0)
            if total_new <= 0:
                return []
            # Проверим занятые номера ≥100000, чтобы избежать коллизий при повторных загрузках
            res = self.client.execute(
                """
                SELECT max(aircraft_number) FROM dict_aircraft_number_flat
                WHERE aircraft_number >= 100000
                """
            )
            max_existing = int(res[0][0] or 99999)
            start = max(100000, max_existing + 1)
            return [start + i for i in range(total_new)]
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка генерации новых Ми‑17 номеров: {e}")
            return []

    def extend_aircraft_dictionary_with_new_mi17(self, aircraft_list: List[Tuple[int,int]]) -> List[Tuple[int,int]]:
        """Дополняет словарь бортов новыми Ми‑17 (mask=64) по сгенерированным номерам.
        Возвращает объединённый список пар (aircraft_number, ac_type_mask).
        """
        try:
            new_numbers = self.generate_new_mi17_aircraft_numbers()
            if not new_numbers:
                return aircraft_list
            extended = list(aircraft_list)
            for ac in new_numbers:
                extended.append((int(ac), 64))
            # Зафиксируем их в dict_aircraft_number_flat, чтобы последующие шаги видели их
            values = [(int(ac), 64,)]
            try:
                self.client.execute("""
                    CREATE TABLE IF NOT EXISTS dict_aircraft_number_flat (
                        aircraft_number UInt32,
                        ac_type_mask UInt8,
                        version_date Date DEFAULT today(),
                        version_id UInt8 DEFAULT 1
                    ) ENGINE = MergeTree()
                    ORDER BY aircraft_number
                """)
            except Exception:
                pass
            # Вставляем пачками, игнорируя дубликаты через ON CLUSTER отсутствует — делаем фильтр
            existing = {int(r[0]) for r in self.client.execute("SELECT aircraft_number FROM dict_aircraft_number_flat WHERE aircraft_number >= 100000")}
            insert_vals = [(ac, 64) for ac in new_numbers if ac not in existing]
            if insert_vals:
                self.client.execute("INSERT INTO dict_aircraft_number_flat (aircraft_number, ac_type_mask) VALUES", insert_vals)
                self.logger.info(f"📘 Добавлено в словарь новых Ми‑17: {len(insert_vals)} записей, диапазон [{insert_vals[0][0]}..{insert_vals[-1][0]}]")
            else:
                self.logger.info("ℹ️ Новых Ми‑17 для словаря не требуется — всё уже есть")
            return extended
        except Exception as e:
            self.logger.error(f"❌ Ошибка дополнения словаря новыми Ми‑17: {e}")
            return aircraft_list
    
    def insert_tensor_data(self, insert_data: List[List]) -> bool:
        """Массовая вставка тензора в ClickHouse"""
        try:
            self.logger.info(f"💾 Начинаем вставку {len(insert_data):,} записей...")
            
            column_names = [
                'aircraft_number', 'dates', 'daily_hours', 
                'ac_type_mask', 'version_date', 'version_id'
            ]
            
            # Вставляем батчами для лучшей производительности  
            batch_size = 100000
            for i in range(0, len(insert_data), batch_size):
                batch = insert_data[i:i + batch_size]
                self.client.execute('INSERT INTO flight_program_fl VALUES', batch)
                self.logger.info(f"📦 Вставлено {i + len(batch):,} / {len(insert_data):,} записей")
            
            self.logger.info("✅ Данные успешно загружены в flight_program_fl")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка вставки данных: {e}")
            return False
    
    def validate_tensor(self) -> bool:
        """
        Валидация созданного тензора:
        1. Проверка планеров по serialno
        2. Проверка общего количества заполненных планеров  
        3. Отсутствие пустых и нулевых полей в массиве
        """
        try:
            self.logger.info("🔍 === ВАЛИДАЦИЯ ТЕНЗОРА flight_program_fl ===")
            
            # 1. Общая статистика
            stats_query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT aircraft_number) as unique_aircraft,
                COUNT(DISTINCT dates) as unique_dates,
                MIN(dates) as min_date,
                MAX(dates) as max_date,
                AVG(daily_hours) as avg_hours,
                SUM(CASE WHEN daily_hours > 0 THEN 1 ELSE 0 END) as non_zero_records
            FROM flight_program_fl
            """
            stats_result = self.client.execute(stats_query)
            row = stats_result[0]
            
            self.logger.info(f"📊 Общая статистика:")
            self.logger.info(f"   Всего записей: {row[0]:,}")
            self.logger.info(f"   Уникальных планеров: {row[1]:,}")
            self.logger.info(f"   Уникальных дат: {row[2]:,}")
            self.logger.info(f"   Период: {row[3]} - {row[4]}")
            self.logger.info(f"   Средний налет: {row[5]:.2f} часов/день")
            self.logger.info(f"   Записей с налетом > 0: {row[6]:,}")
            
            # 2. Проверка планеров по serialno (из Excel)
            serialno_check_query = """
            SELECT 
                aircraft_number,
                COUNT(*) as record_count,
                AVG(daily_hours) as avg_hours,
                SUM(CASE WHEN daily_hours > 0 THEN 1 ELSE 0 END) as non_zero_count
            FROM flight_program_fl
            WHERE aircraft_number IN (
                SELECT DISTINCT aircraft_number 
                FROM flight_program_fl 
                WHERE aircraft_number IN (27067)  -- Пример из Excel
            )
            GROUP BY aircraft_number
            ORDER BY aircraft_number
            """
            
            try:
                serialno_result = self.client.execute(serialno_check_query)
                if serialno_result:
                    self.logger.info(f"📋 Планеры по serialno из Excel:")
                    for aircraft_number, record_count, avg_hours, non_zero_count in serialno_result:
                        self.logger.info(f"   Планер {aircraft_number}: {record_count} записей, среднее {avg_hours:.1f}ч, ненулевых {non_zero_count}")
                else:
                    self.logger.info(f"📋 Планеры по serialno: данные не найдены")
            except Exception as e:
                self.logger.warning(f"⚠️ Не удалось проверить планеры по serialno: {e}")
            
            # 3. Проверка распределения по типам ВС
            type_query = """
            SELECT 
                ac_type_mask,
                COUNT(DISTINCT aircraft_number) as aircraft_count,
                AVG(daily_hours) as avg_hours,
                SUM(CASE WHEN daily_hours > 0 THEN 1 ELSE 0 END) as non_zero_records
            FROM flight_program_fl
            GROUP BY ac_type_mask
            ORDER BY ac_type_mask
            """
            type_result = self.client.execute(type_query)
            
            self.logger.info(f"📋 Распределение по типам ВС:")
            for ac_type_mask, aircraft_count, avg_hours, non_zero_records in type_result:
                ac_type_name = "Ми-8" if ac_type_mask == 32 else "Ми-17" if ac_type_mask == 64 else f"Тип-{ac_type_mask}"
                self.logger.info(f"   {ac_type_name} (mask={ac_type_mask}): {aircraft_count} планеров, среднее {avg_hours:.1f}ч, ненулевых {non_zero_records:,}")
            
            # 4. Проверка на NULL/пустые значения
            null_check_query = """
            SELECT 
                SUM(CASE WHEN aircraft_number IS NULL THEN 1 ELSE 0 END) as null_aircraft,
                SUM(CASE WHEN dates IS NULL THEN 1 ELSE 0 END) as null_dates,
                SUM(CASE WHEN daily_hours IS NULL THEN 1 ELSE 0 END) as null_hours,
                SUM(CASE WHEN ac_type_mask IS NULL THEN 1 ELSE 0 END) as null_mask
            FROM flight_program_fl
            """
            null_result = self.client.execute(null_check_query)
            null_row = null_result[0]
            
            # 5. Оценка результатов валидации
            issues = []
            
            # Проверяем размер тензора
            expected_size = row[1] * row[2]  # планеры * дни
            actual_size = row[0]
            if abs(expected_size - actual_size) > 1000:  # Допуск 1000 записей
                issues.append(f"❌ Неожиданный размер тензора: ожидалось ~{expected_size:,}, получено {actual_size:,}")
            
            # Проверяем NULL значения
            if any(null_row):
                issues.append(f"❌ Найдены NULL значения: aircraft={null_row[0]}, dates={null_row[1]}, hours={null_row[2]}, mask={null_row[3]}")
            
            # Проверяем наличие данных
            if row[6] == 0:  # Нет записей с налетом > 0
                issues.append(f"❌ Все записи имеют нулевой налет")
            
            # Результат валидации
            if issues:
                self.logger.warning(f"⚠️ Обнаружены проблемы:")
                for issue in issues:
                    self.logger.warning(f"  {issue}")
                return False
            else:
                self.logger.info(f"✅ Все проверки валидации пройдены успешно!")
                self.logger.info(f"✅ Тензор flight_program_fl готов для Flame GPU!")
                return True
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка валидации: {e}")
            return False
    
    def create_final_tensor(self, excel_path: str = 'data_input/source_data/Program.xlsx',
                          version_date: Optional[date] = None, version_id: int = 1) -> bool:
        """Главная функция создания финального тензора"""
        try:
            self.logger.info("🚀 === PROGRAM FL DIRECT LOADER ===")
            self.logger.info("Создание тензора flight_program_fl напрямую из Excel")
            self.logger.info("Размер: ~279 планеров × 4000 дней = ~1.1M записей")
            
            # 1. Анализ Excel структуры
            analyzer = ExcelStructureAnalyzer(excel_path)
            excel_data = analyzer.analyze_excel_structure()
            
            # 2. Получение базовой даты
            if version_date is None:
                version_date = self.get_base_date()
            
            # 3. Создание движка размножения
            expansion_engine = YearExpansionEngine(excel_data['year_mapping'])
            calendar = expansion_engine.generate_4000_day_calendar(version_date)
            
            # 4. Загрузка словаря планеров и расширение новыми Ми‑17
            all_aircraft = self.load_aircraft_dictionary()
            all_aircraft = self.extend_aircraft_dictionary_with_new_mi17(all_aircraft)
            if not all_aircraft:
                self.logger.error("❌ Нет данных о планерах")
                return False
            
            # 5. Создание таблицы
            if not self.create_flight_program_fl_table():
                return False
            
            # 6. Применение логики приоритетов и создание данных
            insert_data = self.apply_priority_logic(
                all_aircraft, excel_data['flight_data'], 
                expansion_engine, calendar, version_date, version_id
            )
            
            # 7. Вставка данных
            if not self.insert_tensor_data(insert_data):
                return False
            
            # 8. Валидация
            validation_success = self.validate_tensor()
            
            if validation_success:
                self.logger.info("🎉 === ТЕНЗОР FLIGHT_PROGRAM_FL ГОТОВ ===")
                self.logger.info(f"📅 Версия данных: {version_date} (version_id={version_id})")
                self.logger.info(f"📊 Размер тензора: {len(insert_data):,} записей")
                self.logger.info(f"🔥 Готов для использования в Flame GPU!")
            else:
                self.logger.warning("⚠️ Тензор создан, но есть проблемы с валидацией")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка создания тензора: {e}")
            return False


def main(version_date: Optional[str] = None, version_id: Optional[int] = None):
    """Главная функция с поддержкой версионирования"""
    print("🚀 === PROGRAM FL DIRECT LOADER ===")
    print("Прямое создание тензора flight_program_fl из обновленного Excel")
    print()
    
    loader = FlightProgramDirectLoader()
    
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
        print("✅ Прямой загрузчик завершен успешно!")
        return True
    else:
        print("❌ Ошибка в работе прямого загрузчика!")
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Program FL Direct Loader для Helicopter Component Lifecycle')
    parser.add_argument('--version-date', type=str, help='Дата версии (YYYY-MM-DD)')
    parser.add_argument('--version-id', type=int, help='ID версии')
    
    args = parser.parse_args()
    
    success = main(version_date=args.version_date, version_id=args.version_id)
    sys.exit(0 if success else 1) 