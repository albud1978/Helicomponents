#!/usr/bin/env python3
"""
Flight Program Flat Loader - Создание ежедневного тензора налетов планеров
========================================================================

Создает таблицу flight_program_fl с ежедневными нормативными налетами для всех планеров.

ЛОГИКА ПРИОРИТЕТОВ:
1. По экземплярам (aircraft_number = serialno в flight_program) - ПРИОРИТЕТ
2. По типам (ac_type_mask) - для оставшихся планеров

РАЗМЕР ТЕНЗОРА: ~279 планеров × 4000 дней = ~1.1M записей

Автор: AI Assistant
Дата: 2025-01-18
"""

import sys
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any

# Добавляем путь к утилитам
sys.path.append(str(Path(__file__).parent / 'utils'))
from config_loader import load_clickhouse_config
import clickhouse_connect

class FlightProgramFlatLoader:
    """Загрузчик ежедневного тензора программы полетов для всех планеров"""
    
    def __init__(self):
        """Инициализация загрузчика"""
        self.logger = self._setup_logging()
        self.config = load_clickhouse_config()
        
        # Исправляем конфигурацию для HTTP порта
        self.config['port'] = 8123
        if 'settings' in self.config:
            self.config['settings'] = {k: v for k, v in self.config['settings'].items() if k != 'use_numpy'}
        
        self.client = None
        self.days_count = 4000  # Количество дней для прогноза
        
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
            self.client = clickhouse_connect.get_client(**self.config)
            result = self.client.query('SELECT 1 as test')
            self.logger.info("✅ Подключение к ClickHouse успешно!")
            return True
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения: {e}")
            return False
    
    def get_base_date(self) -> datetime:
        """Получение базовой даты - последняя version_date из heli_pandas"""
        try:
            query = """
            SELECT MAX(version_date) as last_version_date
            FROM heli_pandas
            WHERE version_date IS NOT NULL
            """
            result = self.client.query(query)
            base_date = result.result_rows[0][0]
            self.logger.info(f"📅 Базовая дата (последняя version_date): {base_date}")
            return base_date
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения базовой даты: {e}")
            # Fallback на текущую дату
            return datetime.now().date()
    
    def get_all_aircraft(self) -> List[Tuple[int, int]]:
        """Получение всех планеров из словаря aircraft_number"""
        try:
            query = """
            SELECT aircraft_number, ac_type_mask
            FROM dict_aircraft_number_flat
            ORDER BY aircraft_number
            """
            result = self.client.query(query)
            aircraft_list = [(row[0], row[1]) for row in result.result_rows]
            self.logger.info(f"🚁 Найдено {len(aircraft_list)} планеров в словаре")
            return aircraft_list
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения списка планеров: {e}")
            return []
    
    def get_flight_program_by_instance(self) -> Dict[int, Dict[int, float]]:
        """Получение программы полетов по экземплярам (aircraft_number = serialno)"""
        try:
            # ИСПРАВЛЕНО: используем serialno вместо partno для связи с aircraft_number
            query = """
            SELECT 
                serialno,
                month_number,
                value
            FROM flight_program
            WHERE field_type = 'daily_flight' 
                AND serialno IS NOT NULL
            ORDER BY serialno, month_number
            """
            result = self.client.query(query)
            
            # Структура: {aircraft_number: {month: daily_value}}
            flight_data = {}
            for row in result.result_rows:
                aircraft_number = row[0]  # serialno теперь соответствует aircraft_number
                month = row[1]
                daily_value = row[2]
                
                if aircraft_number not in flight_data:
                    flight_data[aircraft_number] = {}
                flight_data[aircraft_number][month] = daily_value
            
            self.logger.info(f"📋 Программа по экземплярам: {len(flight_data)} планеров")
            return flight_data
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения программы по экземплярам: {e}")
            return {}
    
    def get_flight_program_by_type(self) -> Dict[int, Dict[int, float]]:
        """Получение программы полетов по типам ВС (ac_type_mask)"""
        try:
            query = """
            SELECT 
                ac_type_mask,
                month_number,
                value
            FROM flight_program
            WHERE field_type = 'daily_flight' 
                AND serialno IS NULL
                AND ac_type_mask IS NOT NULL
            ORDER BY ac_type_mask, month_number
            """
            result = self.client.query(query)
            
            # Структура: {ac_type_mask: {month: daily_value}}
            flight_data = {}
            for row in result.result_rows:
                ac_type_mask = row[0]
                month = row[1]
                daily_value = row[2]
                
                if ac_type_mask not in flight_data:
                    flight_data[ac_type_mask] = {}
                flight_data[ac_type_mask][month] = daily_value
            
            self.logger.info(f"📋 Программа по типам: {len(flight_data)} типов ВС")
            return flight_data
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения программы по типам: {e}")
            return {}
    
    def generate_daily_calendar(self, base_date: datetime) -> List[Tuple[datetime, int]]:
        """Генерация календаря на 4000 дней с номерами месяцев"""
        calendar = []
        current_date = base_date
        
        for day in range(self.days_count):
            month_number = current_date.month
            calendar.append((current_date, month_number))
            current_date += timedelta(days=1)
        
        self.logger.info(f"📅 Создан календарь: {len(calendar)} дней ({base_date} - {current_date - timedelta(days=1)})")
        return calendar
    
    def create_flight_program_fl_table(self) -> bool:
        """Создание таблицы flight_program_fl"""
        try:
            # Удаляем таблицу если существует
            self.client.command("DROP TABLE IF EXISTS flight_program_fl")
            
            # Создаем новую таблицу
            create_table_sql = """
            CREATE TABLE flight_program_fl (
                aircraft_number UInt16,
                flight_date Date,
                daily_hours Float32,
                ac_type_mask UInt8,
                version_date Date DEFAULT today(),
                version_id UInt8 DEFAULT 1
            ) ENGINE = MergeTree()
            ORDER BY (aircraft_number, flight_date)
            SETTINGS index_granularity = 8192
            """
            
            self.client.command(create_table_sql)
            self.logger.info("✅ Таблица flight_program_fl создана")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания таблицы: {e}")
            return False
    
    def populate_flight_program_fl(self) -> bool:
        """Заполнение таблицы flight_program_fl с приоритетной логикой"""
        try:
            # Получаем исходные данные
            base_date = self.get_base_date()
            all_aircraft = self.get_all_aircraft()
            flight_by_instance = self.get_flight_program_by_instance()
            flight_by_type = self.get_flight_program_by_type()
            daily_calendar = self.generate_daily_calendar(base_date)
            
            if not all_aircraft:
                self.logger.error("❌ Нет данных о планерах")
                return False
            
            # Подготавливаем данные для вставки
            insert_data = []
            version_id = 1  # Версия данных
            
            instance_count = 0
            type_count = 0
            
            self.logger.info("🔄 Генерация ежедневных данных...")
            
            for aircraft_number, ac_type_mask in all_aircraft:
                for flight_date, month_number in daily_calendar:
                    daily_hours = 0.0
                    
                    # ПРИОРИТЕТ 1: По экземплярам
                    if aircraft_number in flight_by_instance:
                        if month_number in flight_by_instance[aircraft_number]:
                            daily_hours = float(flight_by_instance[aircraft_number][month_number])
                            if flight_date == daily_calendar[0][0]:  # Только для первого дня для статистики
                                instance_count += 1
                    
                    # ПРИОРИТЕТ 2: По типам (если нет данных по экземпляру)
                    elif ac_type_mask in flight_by_type:
                        if month_number in flight_by_type[ac_type_mask]:
                            daily_hours = float(flight_by_type[ac_type_mask][month_number])
                            if flight_date == daily_calendar[0][0]:  # Только для первого дня для статистики
                                type_count += 1
                    
                    # Добавляем запись
                    insert_data.append([
                        aircraft_number,
                        flight_date,
                        daily_hours,
                        ac_type_mask,
                        base_date,  # version_date
                        version_id  # version_id
                    ])
            
            # Массовая вставка данных
            self.logger.info(f"💾 Вставка {len(insert_data):,} записей...")
            
            column_names = [
                'aircraft_number', 'flight_date', 'daily_hours', 
                'ac_type_mask', 'version_date', 'version_id'
            ]
            
            # Вставляем батчами для лучшей производительности
            batch_size = 50000
            for i in range(0, len(insert_data), batch_size):
                batch = insert_data[i:i + batch_size]
                self.client.insert('flight_program_fl', batch, column_names=column_names)
                self.logger.info(f"📦 Вставлено {i + len(batch):,} / {len(insert_data):,} записей")
            
            self.logger.info("✅ Данные успешно загружены в flight_program_fl")
            self.logger.info(f"📊 Статистика приоритетов:")
            self.logger.info(f"   - По экземплярам: {instance_count} планеров")
            self.logger.info(f"   - По типам: {type_count} планеров")
            self.logger.info(f"   - Всего планеров: {len(all_aircraft)}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка заполнения таблицы: {e}")
            return False
    
    def validate_flight_program_fl(self) -> bool:
        """Валидация созданной таблицы"""
        try:
            # Проверяем общую статистику
            stats_query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT aircraft_number) as unique_aircraft,
                COUNT(DISTINCT flight_date) as unique_dates,
                MIN(flight_date) as min_date,
                MAX(flight_date) as max_date,
                AVG(daily_hours) as avg_hours
            FROM flight_program_fl
            """
            stats_result = self.client.query(stats_query)
            row = stats_result.result_rows[0]
            
            self.logger.info("📊 === ВАЛИДАЦИЯ flight_program_fl ===")
            self.logger.info(f"   Всего записей: {row[0]:,}")
            self.logger.info(f"   Уникальных планеров: {row[1]:,}")
            self.logger.info(f"   Уникальных дат: {row[2]:,}")
            self.logger.info(f"   Период: {row[3]} - {row[4]}")
            self.logger.info(f"   Средний налет: {row[5]:.2f} часов/день")
            
            # Проверяем распределение по типам ВС
            type_query = """
            SELECT 
                ac_type_mask,
                COUNT(DISTINCT aircraft_number) as aircraft_count,
                AVG(daily_hours) as avg_hours
            FROM flight_program_fl
            GROUP BY ac_type_mask
            ORDER BY ac_type_mask
            """
            type_result = self.client.query(type_query)
            
            self.logger.info("📋 Распределение по типам ВС:")
            for row in type_result.result_rows:
                type_name = "Ми-8 семейство" if row[0] == 32 else "Ми-17 семейство" if row[0] == 64 else "Другой"
                self.logger.info(f"   ac_type_mask {row[0]} ({type_name}): {row[1]} планеров, {row[2]:.2f} ч/день")
            
            # Ожидаемое количество записей
            expected_records = len(self.get_all_aircraft()) * self.days_count
            actual_records = stats_result.result_rows[0][0]
            
            if actual_records == expected_records:
                self.logger.info("✅ Количество записей соответствует ожидаемому")
                return True
            else:
                self.logger.warning(f"⚠️ Ожидалось {expected_records:,} записей, получено {actual_records:,}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка валидации: {e}")
            return False

def main():
    """Главная функция"""
    print("🚁 FLIGHT PROGRAM FLAT LOADER")
    print("=" * 50)
    print("Создание ежедневного тензора налетов для всех планеров")
    print("Размер: ~279 планеров × 4000 дней = ~1.1M записей")
    print()
    
    loader = FlightProgramFlatLoader()
    
    if not loader.connect_to_database():
        print("❌ Не удалось подключиться к базе данных")
        return
    
    print("🔄 Этап 1: Создание таблицы flight_program_fl...")
    if not loader.create_flight_program_fl_table():
        print("❌ Ошибка создания таблицы")
        return
    
    print("🔄 Этап 2: Заполнение данных с приоритетной логикой...")
    if not loader.populate_flight_program_fl():
        print("❌ Ошибка заполнения данных")
        return
    
    print("🔄 Этап 3: Валидация результатов...")
    if loader.validate_flight_program_fl():
        print("✅ Тензор flight_program_fl готов для использования!")
    else:
        print("⚠️ Есть проблемы с данными, но таблица создана")
    
    loader.client.close()

if __name__ == "__main__":
    main() 