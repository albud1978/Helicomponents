#!/usr/bin/env python3
"""
FLAME GPU MacroProperty4 Loader для Transform этапа
Загружает данные flight_program_ac в настоящий FLAME GPU MacroProperty4
Использует pyflamegpu для создания реальных GPU структур данных
"""

import logging
import sys
import os
from typing import Dict, Any, List, Tuple, Optional
import json
from datetime import datetime

# Добавляем путь к модулям проекта
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

try:
    import pyflamegpu
except ImportError as e:
    print(f"❌ Ошибка импорта pyflamegpu: {e}")
    print("💡 Убедитесь что FLAME GPU установлен: pip install pyflamegpu")
    sys.exit(1)

from utils.config_loader import get_clickhouse_client

def setup_logging() -> logging.Logger:
    """Настройка логирования"""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, f"flame_macroproperty4_loader_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"🔥 Запуск FLAME GPU MacroProperty4 Loader")
    logger.info(f"📋 Лог файл: {log_file}")
    
    return logger

class FlameMacroProperty4Loader:
    """Загрузчик MacroProperty4 в настоящий FLAME GPU Environment"""
    
    def __init__(self, client=None, version_date=None, version_id=None):
        self.client = client or get_clickhouse_client()
        self.logger = setup_logging()
        self.version_date = version_date
        self.version_id = version_id
        
        # FLAME GPU компоненты
        self.model = None
        self.environment = None
        self.simulation = None
        
        # Метаданные загрузки
        self.field_mapping = {}
        self.data_stats = {}
        self.stats = {
            'total_records': 0,
            'loaded_macroproperties': 0,
            'field_mappings': 0,
            'conversion_time': 0,
            'load_time': 0
        }
        
        self.logger.info("🚀 FlameMacroProperty4Loader инициализирован")
        
    def get_latest_version(self) -> Tuple[str, int]:
        """Получение последней версии данных из flight_program_ac"""
        try:
            result = self.client.execute("""
                SELECT version_date, version_id 
                FROM flight_program_ac 
                ORDER BY version_date DESC, version_id DESC 
                LIMIT 1
            """)
            
            if result:
                version_date, version_id = result[0]
                self.logger.info(f"📅 Последняя версия flight_program_ac: {version_date}, v{version_id}")
                return str(version_date), version_id
            else:
                raise ValueError("❌ Нет данных в flight_program_ac")
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения версии: {e}")
            raise

    def load_flight_program_ac_from_clickhouse(self) -> Tuple[List[Tuple], List[str]]:
        """Загрузка данных flight_program_ac из ClickHouse"""
        try:
            # Автоматическое определение версии если не указана
            if not self.version_date or not self.version_id:
                self.version_date, self.version_id = self.get_latest_version()
            
            self.logger.info(f"🔍 Загрузка flight_program_ac для версии {self.version_date}, v{self.version_id}")
            
            # Определяем поля для загрузки (исключаем служебные поля)
            analytics_fields = [
                'dates', 'ops_counter_mi8', 'ops_counter_mi17', 'ops_counter_total',
                'new_counter_mi17', 'trigger_program_mi8', 'trigger_program_mi17', 'trigger_program'
            ]
            
            fields_str = ', '.join(analytics_fields)
            
            query = f"""
                SELECT {fields_str}
                FROM flight_program_ac 
                WHERE version_date = '{self.version_date}' AND version_id = {self.version_id}
                ORDER BY dates
            """
            
            self.logger.info(f"📊 SQL: {query}")
            
            data = self.client.execute(query)
            self.stats['total_records'] = len(data)
            
            self.logger.info(f"✅ Загружено {len(data):,} записей flight_program_ac")
            self.logger.info(f"📋 Поля: {analytics_fields}")
            
            return data, analytics_fields
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки flight_program_ac: {e}")
            raise

    def create_field_id_mapping(self, field_names: List[str]) -> Dict[str, int]:
        """Создание маппинга field_name → field_id из dict_digital_values_flat"""
        try:
            self.logger.info("🔢 Создание field_id маппинга...")
            
            field_mapping = {}
            
            for field_name in field_names:
                result = self.client.execute(f"""
                    SELECT field_id 
                    FROM dict_digital_values_flat 
                    WHERE primary_table = 'flight_program_ac' AND field_name = '{field_name}'
                """)
                
                if result:
                    field_id = result[0][0]
                    field_mapping[field_name] = field_id
                    self.logger.info(f"📋 {field_name} → field_id: {field_id}")
                else:
                    self.logger.error(f"❌ field_id не найден для {field_name}")
                    raise ValueError(f"field_id не найден для поля {field_name}")
            
            self.field_mapping = field_mapping
            self.stats['field_mappings'] = len(field_mapping)
            
            self.logger.info(f"✅ Создан field_id маппинг: {len(field_mapping)} полей")
            return field_mapping
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания field_id маппинга: {e}")
            raise

    def _convert_to_flame_gpu_type(self, value: Any, target_type: str, field_name: str) -> Any:
        """Конвертация значения в совместимый с FLAME GPU тип"""
        try:
            if value is None:
                return 0  # FLAME GPU не поддерживает NULL
                
            if target_type in ['UInt8', 'UInt16', 'UInt32']:
                return int(value) if value >= 0 else 0
            elif target_type in ['Int8', 'Int16', 'Int32']:
                return int(value)
            elif target_type == 'Float32':
                return float(value)
            elif target_type == 'Date':
                # Правильная конвертация datetime.date объектов
                if hasattr(value, 'year'):  # это datetime.date объект
                    epoch_date = datetime(1970, 1, 1).date()
                    return (value - epoch_date).days
                else:
                    return int(value)
            else:
                return value
                
        except Exception as e:
            self.logger.warning(f"⚠️ Ошибка конвертации {field_name}={value} в {target_type}: {e}")
            return 0

    def convert_data_to_flame_gpu_types(self, data: List[Tuple], field_names: List[str]) -> List[List]:
        """Конвертация данных в типы совместимые с FLAME GPU"""
        try:
            self.logger.info("🔄 Конвертация данных в FLAME GPU типы...")
            
            # Получаем типы данных из словаря
            type_mapping = {}
            for field_name in field_names:
                result = self.client.execute(f"""
                    SELECT data_type 
                    FROM dict_digital_values_flat 
                    WHERE primary_table = 'flight_program_ac' AND field_name = '{field_name}'
                """)
                
                if result:
                    type_mapping[field_name] = result[0][0]
                else:
                    type_mapping[field_name] = 'String'  # Default
            
            converted_data = []
            for row in data:
                converted_row = []
                for i, (field_name, value) in enumerate(zip(field_names, row)):
                    target_type = type_mapping[field_name]
                    converted_value = self._convert_to_flame_gpu_type(value, target_type, field_name)
                    converted_row.append(converted_value)
                
                converted_data.append(converted_row)
            
            self.logger.info(f"✅ Конвертировано {len(converted_data):,} записей")
            return converted_data
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка конвертации данных: {e}")
            raise

    def initialize_flame_gpu(self) -> bool:
        """Инициализация FLAME GPU модели и environment"""
        try:
            self.logger.info("🔥 Инициализация FLAME GPU...")
            
            # Создание модели
            self.model = pyflamegpu.ModelDescription("HelicopterSimulation")
            self.environment = self.model.Environment()
            
            self.logger.info("✅ FLAME GPU модель создана")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации FLAME GPU: {e}")
            return False

    def load_into_flame_gpu_macroproperty(self, data: List[List], field_names: List[str]) -> bool:
        """Загрузка данных в FLAME GPU MacroProperty4 Environment Property Array"""
        try:
            self.logger.info("⬆️ Загрузка MacroProperty4 в FLAME GPU Environment...")
            
            if not self.model:
                if not self.initialize_flame_gpu():
                    return False
            
            # Маппинг типов ClickHouse → FLAME GPU методы
            type_mapping = {
                'UInt32': 'newPropertyArrayUInt32',
                'UInt16': 'newPropertyArrayUInt16', 
                'UInt8': 'newPropertyArrayUInt8',
                'Int8': 'newPropertyArrayInt8',
                'Float32': 'newPropertyArrayFloat',
                'Date': 'newPropertyArrayUInt16',  # Date как UInt16 (дни с эпохи)
            }
            
            # Получаем типы данных для каждого поля
            field_types = {}
            for field_name in field_names:
                result = self.client.execute(f"""
                    SELECT data_type 
                    FROM dict_digital_values_flat 
                    WHERE primary_table = 'flight_program_ac' AND field_name = '{field_name}'
                """)
                if result:
                    field_types[field_name] = result[0][0]
                else:
                    field_types[field_name] = 'UInt32'  # Default
            
            # Загружаем каждое поле как отдельный Environment Property Array
            for field_idx, field_name in enumerate(field_names):
                field_id = self.field_mapping[field_name]
                field_type = field_types[field_name]
                
                # Извлекаем данные для этого поля из всех записей
                field_data = [row[field_idx] for row in data]
                
                # Создаем Environment Property Array с данными (как в MacroProperty1)
                property_name = f"macroproperty4_field_{field_id}"
                
                # Получаем правильный метод для создания массива
                create_method = type_mapping.get(field_type, 'newPropertyArrayUInt32')
                method_func = getattr(self.environment, create_method)
                
                # FLAME GPU требует данные при создании Property Array
                method_func(property_name, field_data)
                
                self.logger.info(f"📊 {field_name} (field_id: {field_id}, {field_type}) → {len(field_data):,} значений")
            
            self.stats['loaded_macroproperties'] = len(field_names)
            
            self.logger.info(f"✅ MacroProperty4 загружен: {len(field_names)} полей, {len(data):,} записей")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки MacroProperty4: {e}")
            return False

    def run_loading_process(self) -> bool:
        """Запуск полного процесса загрузки MacroProperty4"""
        try:
            start_time = datetime.now()
            self.logger.info("🚀 === ЗАПУСК ЗАГРУЗКИ MACROPROPERTY4 ===")
            
            # 1. Загрузка данных из ClickHouse
            data, field_names = self.load_flight_program_ac_from_clickhouse()
            
            # 2. Создание field_id маппинга
            field_mapping = self.create_field_id_mapping(field_names)
            
            # 3. Конвертация данных
            converted_data = self.convert_data_to_flame_gpu_types(data, field_names)
            
            # 4. Загрузка в FLAME GPU
            success = self.load_into_flame_gpu_macroproperty(converted_data, field_names)
            
            # 5. Финальная статистика
            end_time = datetime.now()
            total_time = (end_time - start_time).total_seconds()
            
            self.logger.info("📊 === ИТОГОВАЯ СТАТИСТИКА ===")
            self.logger.info(f"📅 Версия данных: {self.version_date}, v{self.version_id}")
            self.logger.info(f"📊 Записей обработано: {self.stats['total_records']:,}")
            self.logger.info(f"🔢 MacroProperty полей: {self.stats['loaded_macroproperties']}")
            self.logger.info(f"🎯 Field_id маппингов: {self.stats['field_mappings']}")
            self.logger.info(f"⏱️ Общее время: {total_time:.2f}с")
            self.logger.info(f"✅ Статус: {'УСПЕШНО' if success else 'ОШИБКА'}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка загрузки: {e}")
            return False

def main():
    """Главная функция загрузчика"""
    import argparse
    
    parser = argparse.ArgumentParser(description='FLAME GPU MacroProperty4 Loader')
    parser.add_argument('--version-date', help='Дата версии данных (YYYY-MM-DD)')
    parser.add_argument('--version-id', type=int, help='ID версии данных')
    
    args = parser.parse_args()
    
    # Создание и запуск загрузчика
    loader = FlameMacroProperty4Loader(
        version_date=args.version_date,
        version_id=args.version_id
    )
    
    success = loader.run_loading_process()
    
    if success:
        print("🎯 MacroProperty4 загружен успешно!")
        return 0
    else:
        print("❌ Ошибка загрузки MacroProperty4")
        return 1

if __name__ == "__main__":
    exit(main()) 