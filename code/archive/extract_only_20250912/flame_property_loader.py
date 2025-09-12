#!/usr/bin/env python3
"""
FLAME GPU Property Loader для Transform этапа
Загружает данные версионности (version_date, version_id) из heli_pandas в настоящий FLAME GPU Property
Использует pyflamegpu для создания реальных GPU структур данных
"""

import logging
import sys
import os
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime

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
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f'logs/flame_property_loader_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger = logging.getLogger(__name__)
    logger.info(f"📋 Лог файл: {log_filename}")
    return logger

class FlamePropertyLoader:
    """Загрузчик Property в настоящий FLAME GPU Environment"""
    
    def __init__(self, client=None, version_date=None, version_id=None):
        self.client = client or get_clickhouse_client()
        self.logger = setup_logging()
        self.version_date = version_date
        self.version_id = version_id
        
        self.model = None
        self.environment = None
        self.simulation = None
        
        self.field_mapping = {}
        self.data_stats = {}
        self.stats = {
            'total_records': 0,
            'loaded_properties': 0,
            'field_mappings': 0,
            'conversion_time': 0,
            'load_time': 0
        }
        
        self.logger.info("🚀 FlamePropertyLoader инициализирован")
        
    def get_latest_version(self) -> Tuple[str, int]:
        """Получение последней версии данных из heli_pandas"""
        try:
            result = self.client.execute("""
                SELECT version_date, version_id 
                FROM heli_pandas 
                ORDER BY version_date DESC, version_id DESC 
                LIMIT 1
            """)
            
            if result:
                version_date = result[0][0]
                version_id = result[0][1]
                self.logger.info(f"📅 Последняя версия heli_pandas: {version_date}, v{version_id}")
                return str(version_date), version_id
            else:
                raise Exception("Нет данных в heli_pandas")
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения версии: {e}")
            raise

    def load_heli_pandas_from_clickhouse(self) -> Tuple[List[Tuple], List[str]]:
        """Загрузка данных version_date, version_id из heli_pandas"""
        try:
            if not self.version_date or not self.version_id:
                self.version_date, self.version_id = self.get_latest_version()
            
            # Property содержит только 2 поля согласно документации
            analytics_fields = [
                'version_date', 'version_id'
            ]
            
            fields_str = ', '.join(analytics_fields)
            query = f"""
                SELECT DISTINCT {fields_str}
                FROM heli_pandas 
                WHERE version_date = '{self.version_date}' AND version_id = {self.version_id}
                ORDER BY version_date, version_id
            """
            
            self.logger.info(f"🔍 Загрузка Property из heli_pandas для версии {self.version_date}, v{self.version_id}")
            self.logger.info(f"📊 SQL: {query}")
            
            data = self.client.execute(query)
            self.stats['total_records'] = len(data)
            
            self.logger.info(f"✅ Загружено {len(data):,} записей Property")
            self.logger.info(f"📋 Поля: {analytics_fields}")
            
            return data, analytics_fields
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки Property: {e}")
            raise

    def create_field_id_mapping(self, field_names: List[str]) -> Dict[str, int]:
        """Создание маппинга field_name → field_id из dict_digital_values_flat"""
        try:
            self.logger.info("🔢 Создание field_id маппинга...")
            
            # Property имеет фиксированные field_id согласно документации
            predefined_mapping = {
                'version_date': 71,
                'version_id': 72
            }
            
            field_mapping = {}
            for field_name in field_names:
                if field_name in predefined_mapping:
                    field_id = predefined_mapping[field_name]
                    field_mapping[field_name] = field_id
                    self.logger.info(f"📋 {field_name} → field_id: {field_id}")
                else:
                    # Если нет в предопределенных, проверяем словарь
                    result = self.client.execute(f"""
                        SELECT field_id 
                        FROM dict_digital_values_flat 
                        WHERE primary_table = 'heli_pandas' AND field_name = '{field_name}'
                    """)
                    
                    if result:
                        field_id = result[0][0]
                        field_mapping[field_name] = field_id
                        self.logger.info(f"📋 {field_name} → field_id: {field_id}")
                    else:
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
            
            # Предопределенные типы для Property полей
            type_mapping = {
                'version_date': 'Date',
                'version_id': 'UInt8'
            }
            
            # Дополняем из словаря если нужно
            for field_name in field_names:
                if field_name not in type_mapping:
                    result = self.client.execute(f"""
                        SELECT data_type 
                        FROM dict_digital_values_flat 
                        WHERE primary_table = 'heli_pandas' AND field_name = '{field_name}'
                    """)
                    if result:
                        type_mapping[field_name] = result[0][0]
                    else:
                        type_mapping[field_name] = 'String'
            
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
            
            self.model = pyflamegpu.ModelDescription("HelicopterSimulation")
            self.environment = self.model.Environment()
            
            self.logger.info("✅ FLAME GPU модель создана")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации FLAME GPU: {e}")
            return False

    def load_into_flame_gpu_property(self, data: List[List], field_names: List[str]) -> bool:
        """Загрузка данных в FLAME GPU Property (простые скалярные значения)"""
        try:
            self.logger.info("⬆️ Загрузка Property в FLAME GPU Environment...")
            
            if not self.model:
                if not self.initialize_flame_gpu():
                    return False
            
            # Property содержит скалярные значения, не массивы
            # Берем первую запись (должна быть единственная для DISTINCT)
            if len(data) > 0:
                record = data[0]  # Первая (и единственная) запись
                
                # Загружаем каждое поле как отдельное Environment Property (скаляр)
                for field_idx, field_name in enumerate(field_names):
                    field_id = self.field_mapping[field_name]
                    value = record[field_idx]
                    
                    # Создаем простое Environment Property
                    property_name = f"property_field_{field_id}"
                    
                    # Определяем тип и создаем скалярное свойство
                    if field_name == 'version_date':
                        # Date как UInt16 (дни с эпохи)
                        self.environment.newPropertyUInt16(property_name, value)
                        data_type = 'UInt16'
                    elif field_name == 'version_id':
                        # version_id как UInt8
                        self.environment.newPropertyUInt8(property_name, value)
                        data_type = 'UInt8'
                    else:
                        # Fallback - UInt32
                        self.environment.newPropertyUInt32(property_name, value)
                        data_type = 'UInt32'
                    
                    self.logger.info(f"📊 {field_name} (field_id: {field_id}, {data_type}) → значение: {value}")
                
                self.stats['loaded_properties'] = len(field_names)
                
                self.logger.info(f"✅ Property загружен: {len(field_names)} полей, скалярные значения")
                return True
            else:
                self.logger.error("❌ Нет данных для загрузки в Property")
                return False
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки Property: {e}")
            return False

    def run_loading_process(self) -> bool:
        """Запуск полного процесса загрузки Property"""
        self.logger.info("🚀 === ЗАПУСК ЗАГРУЗКИ PROPERTY ===")
        start_time = datetime.now()
        
        try:
            # 1. Загрузка данных из ClickHouse
            data, field_names = self.load_heli_pandas_from_clickhouse()
            
            # 2. Создание field_id маппинга
            field_mapping = self.create_field_id_mapping(field_names)
            
            # 3. Конвертация данных в FLAME GPU типы
            converted_data = self.convert_data_to_flame_gpu_types(data, field_names)
            
            # 4. Загрузка в FLAME GPU Property
            success = self.load_into_flame_gpu_property(converted_data, field_names)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Статистика
            self.logger.info("📊 === ИТОГОВАЯ СТАТИСТИКА ===")
            self.logger.info(f"📅 Версия данных: {self.version_date}, v{self.version_id}")
            self.logger.info(f"📊 Записей обработано: {self.stats['total_records']:,}")
            self.logger.info(f"🔢 Property полей: {self.stats['loaded_properties']}")
            self.logger.info(f"🎯 Field_id маппингов: {self.stats['field_mappings']}")
            self.logger.info(f"⏱️ Общее время: {duration:.2f}с")
            self.logger.info(f"✅ Статус: {'УСПЕШНО' if success else 'ОШИБКА'}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка загрузки Property: {e}")
            return False

def main():
    """Главная функция"""
    import argparse
    
    parser = argparse.ArgumentParser(description='FLAME GPU Property Loader')
    parser.add_argument('--version-date', type=str, help='Дата версии данных (YYYY-MM-DD)')
    parser.add_argument('--version-id', type=int, help='ID версии данных')
    
    args = parser.parse_args()
    
    loader = FlamePropertyLoader(
        version_date=args.version_date,
        version_id=args.version_id
    )
    
    success = loader.run_loading_process()
    
    if success:
        print("🎯 Property загружен успешно!")
        return 0
    else:
        print("❌ Ошибка загрузки Property")
        return 1

if __name__ == "__main__":
    exit(main()) 