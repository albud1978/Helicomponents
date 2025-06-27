#!/usr/bin/env python3
"""
Обработка поля location для выделения номеров вертолетов RA-XXXXX
Создает новое поле aircraft_number (UInt16) с номерами вертолетов
"""

import sys
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Any

# Добавляем путь к утилитам
sys.path.append(str(Path(__file__).parent / 'utils'))
from config_loader import load_clickhouse_config
import clickhouse_connect

class LocationProcessor:
    """Обработчик поля location для извлечения номеров вертолетов"""
    
    def __init__(self):
        """Инициализация обработчика"""
        self.logger = self._setup_logging()
        self.config = load_clickhouse_config()
        
        # Исправляем конфигурацию для работы с ClickHouse
        self.config['port'] = 8123  # HTTP порт
        if 'settings' in self.config:
            self.config['settings'] = {k: v for k, v in self.config['settings'].items() if k != 'use_numpy'}
        
        self.client = None
    
    def _setup_logging(self) -> logging.Logger:
        """Настройка логирования"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def connect_to_database(self) -> bool:
        """Подключение к ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(**self.config)
            result = self.client.query('SELECT 1 as test')
            self.logger.info(f"✅ Подключение к ClickHouse успешно!")
            return True
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения: {e}")
            return False
    
    def add_aircraft_number_column(self) -> bool:
        """Добавление колонки aircraft_number в heli_pandas"""
        self.logger.info("🔧 Добавление колонки aircraft_number...")
        
        try:
            # Добавляем колонку если её нет
            alter_query = "ALTER TABLE heli_pandas ADD COLUMN IF NOT EXISTS aircraft_number UInt16 DEFAULT 0"
            self.client.query(alter_query)
            
            self.logger.info("✅ Колонка aircraft_number добавлена в heli_pandas")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка добавления колонки: {e}")
            return False
    
    def extract_aircraft_numbers(self) -> Dict[str, int]:
        """Извлечение номеров вертолетов из значений RA-"""
        self.logger.info("🔍 Извлечение номеров вертолетов из RA- значений...")
        
        try:
            # Получаем все уникальные RA- значения
            ra_result = self.client.query("""
                SELECT DISTINCT location
                FROM heli_pandas 
                WHERE location LIKE 'RA-%'
                ORDER BY location
            """)
            
            aircraft_mapping = {}
            invalid_count = 0
            
            for row in ra_result.result_rows:
                location = row[0]
                # Убираем префикс 'RA-'
                digits_part = location[3:]
                
                # Проверяем что это 5 цифр
                if len(digits_part) == 5 and digits_part.isdigit():
                    aircraft_number = int(digits_part)
                    aircraft_mapping[location] = aircraft_number
                else:
                    invalid_count += 1
                    self.logger.warning(f"⚠️ Неправильный формат: {location}")
            
            self.logger.info(f"✅ Извлечено {len(aircraft_mapping)} номеров вертолетов")
            if invalid_count > 0:
                self.logger.warning(f"⚠️ Найдено {invalid_count} значений неправильного формата")
            
            # Показываем примеры
            self.logger.info("📋 Примеры извлеченных номеров:")
            for i, (location, number) in enumerate(list(aircraft_mapping.items())[:5]):
                self.logger.info(f"   {location} → {number}")
            
            return aircraft_mapping
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка извлечения номеров: {e}")
            return {}
    
    def update_aircraft_numbers(self, aircraft_mapping: Dict[str, int]) -> bool:
        """Обновление поля aircraft_number в heli_pandas"""
        self.logger.info("🔢 Обновление поля aircraft_number...")
        
        try:
            # Сначала очищаем поле
            self.client.query("ALTER TABLE heli_pandas UPDATE aircraft_number = 0 WHERE 1=1")
            
            # Обновляем батчами для производительности
            batch_size = 50
            mapping_items = list(aircraft_mapping.items())
            
            for i in range(0, len(mapping_items), batch_size):
                batch = mapping_items[i:i + batch_size]
                
                # Строим CASE WHEN для батча
                case_when_parts = []
                locations_list = []
                
                for location, aircraft_number in batch:
                    case_when_parts.append(f"WHEN location = '{location}' THEN {aircraft_number}")
                    locations_list.append(f"'{location}'")
                
                case_when_sql = " ".join(case_when_parts)
                locations_sql = ",".join(locations_list)
                
                update_query = f"""
                ALTER TABLE heli_pandas 
                UPDATE aircraft_number = CASE {case_when_sql} ELSE 0 END
                WHERE location IN ({locations_sql})
                """
                
                self.client.query(update_query)
                self.logger.info(f"  📊 Обработано {min(i + batch_size, len(mapping_items))}/{len(mapping_items)} значений")
            
            # Проверяем результат
            result = self.client.query("SELECT COUNT(*) FROM heli_pandas WHERE aircraft_number > 0")
            enriched_count = result.result_rows[0][0]
            self.logger.info(f"✅ Обогащено {enriched_count} записей номерами вертолетов")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления aircraft_number: {e}")
            return False
    
    def clear_non_aircraft_locations(self) -> bool:
        """Очистка location для записей не являющихся вертолетами"""
        self.logger.info("🧹 Очистка location для не-вертолетов...")
        
        try:
            # Получаем статистику до очистки
            before_result = self.client.query("SELECT COUNT(*) FROM heli_pandas WHERE location IS NOT NULL AND location != ''")
            before_count = before_result.result_rows[0][0]
            
            # Очищаем location для всех записей, которые НЕ начинаются с 'RA-'
            clear_query = """
            ALTER TABLE heli_pandas 
            UPDATE location = ''
            WHERE location IS NOT NULL 
              AND location != '' 
              AND NOT (location LIKE 'RA-%')
            """
            
            self.client.query(clear_query)
            
            # Получаем статистику после очистки
            after_result = self.client.query("SELECT COUNT(*) FROM heli_pandas WHERE location IS NOT NULL AND location != ''")
            after_count = after_result.result_rows[0][0]
            
            cleared_count = before_count - after_count
            self.logger.info(f"✅ Очищено {cleared_count} записей не-вертолетов")
            self.logger.info(f"📊 Осталось {after_count} записей с location (только RA- значения)")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка очистки location: {e}")
            return False
    
    def verify_processing(self) -> bool:
        """Проверка качества обработки"""
        self.logger.info("🔍 Проверка качества обработки location...")
        
        try:
            # Общая статистика
            total_result = self.client.query("SELECT COUNT(*) FROM heli_pandas")
            total_count = total_result.result_rows[0][0]
            
            # Статистика aircraft_number
            aircraft_result = self.client.query("SELECT COUNT(*) FROM heli_pandas WHERE aircraft_number > 0")
            aircraft_count = aircraft_result.result_rows[0][0]
            aircraft_coverage = (aircraft_count / total_count) * 100
            
            # Статистика location после очистки
            location_result = self.client.query("SELECT COUNT(*) FROM heli_pandas WHERE location IS NOT NULL AND location != ''")
            location_count = location_result.result_rows[0][0]
            
            self.logger.info(f"📊 Статистика обработки (всего записей: {total_count}):")
            self.logger.info(f"   aircraft_number: {aircraft_count} записей ({aircraft_coverage:.1f}%)")
            self.logger.info(f"   location (RA- только): {location_count} записей")
            
            # Проверяем соответствие
            consistency_result = self.client.query("""
                SELECT COUNT(*) 
                FROM heli_pandas 
                WHERE aircraft_number > 0 
                  AND (location IS NULL OR location = '' OR NOT location LIKE 'RA-%')
            """)
            inconsistent_count = consistency_result.result_rows[0][0]
            
            if inconsistent_count > 0:
                self.logger.warning(f"⚠️ Найдено {inconsistent_count} несогласованных записей")
            else:
                self.logger.info("✅ Все записи согласованы")
            
            # Показываем примеры
            sample_result = self.client.query("""
                SELECT location, aircraft_number 
                FROM heli_pandas 
                WHERE aircraft_number > 0 
                ORDER BY aircraft_number 
                LIMIT 5
            """)
            
            self.logger.info("📋 Примеры обработанных записей:")
            for row in sample_result.result_rows:
                self.logger.info(f"   location: '{row[0]}' → aircraft_number: {row[1]}")
            
            # Диапазон номеров
            range_result = self.client.query("""
                SELECT MIN(aircraft_number), MAX(aircraft_number), COUNT(DISTINCT aircraft_number)
                FROM heli_pandas 
                WHERE aircraft_number > 0
            """)
            
            if range_result.result_rows:
                min_num, max_num, unique_count = range_result.result_rows[0]
                self.logger.info(f"📈 Диапазон номеров: {min_num} - {max_num} ({unique_count} уникальных)")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки: {e}")
            return False
    
    def run_processing(self) -> bool:
        """Запуск полной обработки поля location"""
        self.logger.info("🚀 Запуск обработки поля location")
        
        try:
            # 1. Подключение
            if not self.connect_to_database():
                return False
            
            # 2. Добавление колонки aircraft_number
            if not self.add_aircraft_number_column():
                return False
            
            # 3. Извлечение номеров вертолетов
            aircraft_mapping = self.extract_aircraft_numbers()
            if not aircraft_mapping:
                self.logger.error("❌ Не удалось извлечь номера вертолетов")
                return False
            
            # 4. Обновление aircraft_number
            if not self.update_aircraft_numbers(aircraft_mapping):
                return False
            
            # 5. Очистка не-вертолетных location
            if not self.clear_non_aircraft_locations():
                return False
            
            # 6. Проверка качества
            if not self.verify_processing():
                return False
            
            self.logger.info("🎯 ОБРАБОТКА ПОЛЯ LOCATION ЗАВЕРШЕНА!")
            self.logger.info("🚀 Создано поле aircraft_number с номерами вертолетов")
            self.logger.info("✅ Поле location очищено от не-вертолетных значений")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обработки: {e}")
            return False

def main():
    """Основная функция"""
    processor = LocationProcessor()
    return 0 if processor.run_processing() else 1

if __name__ == "__main__":
    exit(main()) 