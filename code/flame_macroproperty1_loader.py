#!/usr/bin/env python3
"""
FLAME GPU MacroProperty1 Loader для Transform этапа
Загружает данные md_components в настоящий FLAME GPU MacroProperty1
Использует pyflamegpu для создания реальных GPU структур данных
"""

import logging
import sys
import os
from typing import Dict, Any, List, Tuple, Optional
import json
from datetime import datetime

# Добавляем путь к utils
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))

from config_loader import get_clickhouse_client
from version_utils import extract_unified_version_date

# Импорт настоящего FLAME GPU
import pyflamegpu

def setup_logging() -> logging.Logger:
    """Настройка логирования"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/flame_macroproperty1_loader.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

class FlameMacroProperty1Loader:
    """Загрузчик MacroProperty1 в настоящий FLAME GPU Environment"""
    
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
        self.field_types = {}
        self.component_count = 0
        
        # Статистика
        self.stats = {
            'total_records': 0,
            'loaded_macroproperties': 0,
            'field_mapping': {},
            'flame_gpu_version': pyflamegpu.VERSION_FULL,
            'start_time': None,
            'end_time': None
        }
    
    def get_field_mapping_from_clickhouse(self) -> Dict[str, int]:
        """Получение field_id маппинга из dict_digital_values_flat"""
        self.logger.info("🔍 Получение field_id маппинга для md_components...")
        
        try:
            query = """
                SELECT field_name, field_id 
                FROM dict_digital_values_flat 
                WHERE primary_table = 'md_components'
                ORDER BY field_id
            """
            
            result = self.client.execute(query)
            
            field_mapping = {}
            for field_name, field_id in result:
                field_mapping[field_name] = field_id
                self.logger.info(f"   📋 {field_name} -> field_id: {field_id}")
            
            self.field_mapping = field_mapping
            self.stats['field_mapping'] = field_mapping
            self.logger.info(f"✅ Загружено {len(field_mapping)} field_id маппингов из md_components (из них будет использовано 20 для MacroProperty1)")
            
            return field_mapping
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения field_id маппинга: {e}")
            raise
    
    def get_field_types_from_clickhouse(self) -> Dict[str, str]:
        """Получение типов данных полей из ClickHouse"""
        self.logger.info("🔍 Анализ типов данных md_components...")
        
        try:
            describe_query = "DESCRIBE TABLE md_components"
            table_structure = self.client.execute(describe_query)
            
            field_types = {}
            for row in table_structure:
                field_name = row[0]
                field_type = row[1]
                field_types[field_name] = field_type
                self.logger.info(f"   🎯 {field_name}: {field_type}")
            
            self.field_types = field_types
            return field_types
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка анализа типов данных: {e}")
            raise
    
    def load_md_components_from_clickhouse(self) -> Tuple[List[Tuple], List[str]]:
        """Загрузка данных md_components из ClickHouse с фильтрацией по analytics_fields"""
        self.logger.info("📊 Загрузка данных md_components из ClickHouse...")
        
        # Определяем поля для аналитики MacroProperty1 (20 полей - расширенная аналитика)
        analytics_fields = [
            # Основные поля компонентов (14 полей из базовой аналитики)
            'partno_comp',         # field_id: 44
            'type_restricted',     # field_id: 68
            'group_by',           # field_id: 20
            'comp_number',        # field_id: 13
            'll_mi8',             # field_id: 27
            'll_mi17',            # field_id: 26
            'oh_mi8',             # field_id: 35
            'oh_mi17',            # field_id: 34
            'oh_threshold_mi8',   # field_id: 37
            'repair_time',        # field_id: 54
            'partout_time',       # field_id: 45
            'assembly_time',      # field_id: 9
            'br_mi8',             # новый BR для МИ-8
            'br_mi17',            # новый BR для МИ-17
            'restrictions_mask',  # field_id: 55
            
            # Дополнительные поля для симуляции (6 полей)
            'common_restricted1', # field_id: 11 - нужен для restrictions_mask
            'common_restricted2', # field_id: 12 - нужен для restrictions_mask  
            'trigger_interval',   # field_id: 64 - нужен для restrictions_mask
            'ac_type_mask',       # field_id: 4  - нужен для симуляции МИ-8/МИ-17
            'sne_new',            # field_id: 93 - оптимизированное поле
            'ppr_new',            # field_id: 88 - оптимизированное поле
            # Версионные поля для согласованности с MP и Property
            'version_date',       # Date → UInt16 (days since epoch)
            'version_id'          # UInt8
        ]
        
        # Исключаем лишние поля
        excluded_fields = ['partno', 'repair_price', 'purchase_price']
        
        self.logger.info(f"🔍 Фильтрация MacroProperty1: {len(analytics_fields)} полей аналитики")
        self.logger.info(f"🗑️ Исключаем поля: {excluded_fields}")
        
        try:
            # Формируем SELECT с фильтрацией полей
            fields_str = ', '.join(analytics_fields)
            
            # Определяем версионные параметры
            if self.version_date and self.version_id:
                # Преобразуем datetime в строку даты для ClickHouse
                version_date_str = self.version_date.strftime('%Y-%m-%d') if hasattr(self.version_date, 'strftime') else str(self.version_date)
                query = f"""
                    SELECT {fields_str} FROM md_components 
                    WHERE version_date = '{version_date_str}' 
                    AND version_id = {self.version_id}
                    ORDER BY partno_comp
                """
                result = self.client.execute(query)
            else:
                # Загружаем последнюю версию
                query = f"""
                    SELECT {fields_str} FROM md_components 
                    WHERE (version_date, version_id) = (
                        SELECT version_date, version_id 
                        FROM md_components 
                        ORDER BY version_date DESC, version_id DESC 
                        LIMIT 1
                    )
                    ORDER BY partno_comp
                """
                result = self.client.execute(query)
            
            # Порядок полей соответствует analytics_fields
            field_order = analytics_fields
            
            self.component_count = len(result)
            self.stats['total_records'] = len(result)
            
            self.logger.info(f"✅ Загружено {len(result)} записей md_components")
            self.logger.info(f"📋 Порядок полей: {field_order}")
            
            return result, field_order
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки md_components: {e}")
            raise
    
    def create_flame_gpu_model(self) -> pyflamegpu.ModelDescription:
        """Создание FLAME GPU ModelDescription"""
        self.logger.info("🏗️ Создание FLAME GPU ModelDescription для Heli_Rotable...")
        
        try:
            # Создаем модель
            model = pyflamegpu.ModelDescription("Heli_Rotable")
            self.model = model
            
            self.logger.info(f"✅ FLAME GPU модель 'Heli_Rotable' создана")
            self.logger.info(f"🎯 FLAME GPU версия: {pyflamegpu.VERSION_FULL}")
            
            return model
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания FLAME GPU модели: {e}")
            raise
    
    def create_macroproperty1_environment(self, field_mapping: Dict[str, int], 
                                        field_types: Dict[str, str]) -> pyflamegpu.EnvironmentDescription:
        """Создание Environment с Property Arrays для MacroProperty1"""
        self.logger.info("🌍 Создание FLAME GPU Environment с Property Arrays...")
        
        try:
            env = self.model.Environment()
            self.environment = env
            
            # Маппинг ClickHouse типов в FLAME GPU Property Array методы
            type_mapping = {
                'UInt32': 'newPropertyArrayUInt32',
                'UInt16': 'newPropertyArrayUInt16', 
                'UInt8': 'newPropertyArrayUInt8',
                'Float32': 'newPropertyArrayFloat',
                'Float64': 'newPropertyArrayDouble',
                'Date': 'newPropertyArrayUInt16',  # Date как UInt16
                'String': 'newPropertyArrayUInt32'  # String как UInt32 (если это ID)
            }
            
            created_properties = 0
            
            # Создаем Property Array для каждого поля
            for field_name, field_id in sorted(field_mapping.items(), key=lambda x: x[1]):
                if field_name in field_types:
                    ch_type = field_types[field_name]
                    
                    # Определяем метод FLAME GPU
                    flame_method = None
                    for ch_pattern, method in type_mapping.items():
                        if ch_pattern in ch_type:
                            flame_method = method
                            break
                    
                    if flame_method:
                        # Создаем Property Array с пустыми значениями
                        property_name = f"field_{field_id}"
                        default_array = [0] * self.component_count  # Пустые значения
                        
                        method_func = getattr(env, flame_method)
                        method_func(property_name, default_array)
                        
                        created_properties += 1
                        self.logger.info(f"   ✅ Property Array {property_name} ({field_name}): "
                                       f"{flame_method} size={self.component_count}")
                    else:
                        self.logger.warning(f"   ⚠️ Неизвестный тип {ch_type} для поля {field_name}")
            
            self.stats['created_properties'] = created_properties
            self.logger.info(f"🎯 Создано {created_properties} Property Arrays в FLAME GPU Environment")
            
            return env
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания Environment: {e}")
            raise
    
    def create_flame_gpu_simulation(self) -> pyflamegpu.CUDASimulation:
        """Создание FLAME GPU CUDASimulation"""
        self.logger.info("🚀 Создание FLAME GPU CUDASimulation...")
        
        try:
            simulation = pyflamegpu.CUDASimulation(self.model)
            self.simulation = simulation
            
            self.logger.info("✅ FLAME GPU CUDASimulation создана")
            return simulation
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания CUDASimulation: {e}")
            raise
    
    def load_data_into_macroproperty1(self, data: List[Tuple], field_order: List[str], 
                                    field_mapping: Dict[str, int], field_types: Dict[str, str]) -> None:
        """Загрузка данных в FLAME GPU MacroProperty1"""
        self.logger.info("📥 Загрузка данных в FLAME GPU MacroProperty1...")
        
        try:
            # Маппинг ClickHouse типов в методы установки данных Environment
            set_methods = {
                'UInt32': 'setEnvironmentPropertyArrayUInt32',
                'UInt16': 'setEnvironmentPropertyArrayUInt16',
                'UInt8': 'setEnvironmentPropertyArrayUInt8', 
                'Float32': 'setEnvironmentPropertyArrayFloat',
                'Float64': 'setEnvironmentPropertyArrayDouble',
                'Date': 'setEnvironmentPropertyArrayUInt16',  # Date как UInt16
                'String': 'setEnvironmentPropertyArrayUInt32'  # String как UInt32 (если это ID)
            }
            
            loaded_properties = 0
            
            # Загружаем данные для каждого поля
            for field_name in field_order:
                if field_name in field_mapping and field_name in field_types:
                    field_id = field_mapping[field_name]
                    ch_type = field_types[field_name]
                    column_index = field_order.index(field_name)
                    
                    # Извлекаем данные столбца с обработкой NULL значений
                    raw_column_data = [row[column_index] for row in data]
                    
                    # Обрабатываем NULL значения в зависимости от типа
                    default_values = {
                        'UInt32': 0,
                        'UInt16': 0,
                        'UInt8': 0,
                        'Float32': 0.0,
                        'Float64': 0.0,
                        'Date': 0,
                        'String': 0
                    }
                    
                    # Определяем дефолтное значение
                    default_val = 0
                    for ch_pattern, default in default_values.items():
                        if ch_pattern in ch_type:
                            default_val = default
                            break
                    
                    # Заменяем None на дефолтные значения и обрабатываем Date
                    column_data = []
                    for val in raw_column_data:
                        if val is None:
                            column_data.append(default_val)
                        elif 'Date' in ch_type and hasattr(val, 'year'):
                            # Конвертируем Date в days since epoch (для UInt16)
                            epoch_date = datetime(1970, 1, 1).date()
                            date_val = val if hasattr(val, 'year') else val.date()
                            days_since_epoch = (date_val - epoch_date).days
                            column_data.append(days_since_epoch)
                        else:
                            column_data.append(val)
                    
                    # Определяем метод установки
                    set_method = None
                    for ch_pattern, method in set_methods.items():
                        if ch_pattern in ch_type:
                            set_method = method
                            break
                    
                    if set_method and hasattr(self.simulation, set_method):
                        property_name = f"field_{field_id}"
                        
                        # Устанавливаем данные в Environment Property Array (MacroProperty)
                        method_func = getattr(self.simulation, set_method)
                        method_func(property_name, column_data)
                        
                        loaded_properties += 1
                        self.logger.info(f"   ✅ Загружено {len(column_data)} значений в "
                                       f"{property_name} ({field_name}) методом {set_method}")
                    else:
                        self.logger.warning(f"   ⚠️ Метод {set_method} не найден для поля {field_name}")
            
            self.stats['loaded_macroproperties'] = loaded_properties
            self.logger.info(f"🎯 Загружено {loaded_properties} Property Arrays с данными")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки данных в MacroProperty1: {e}")
            raise
    
    def save_flame_metadata(self) -> None:
        """Сохранение метаданных FLAME GPU загрузки"""
        self.logger.info("💾 Сохранение метаданных FLAME GPU загрузки...")
        
        try:
            metadata = {
                'creation_date': datetime.now().isoformat(),
                'flame_gpu_version': pyflamegpu.VERSION_FULL,
                'model_name': 'Heli_Rotable',
                'version_date': self.version_date.isoformat() if self.version_date else None,
                'version_id': self.version_id,
                'total_records': self.stats['total_records'],
                'loaded_macroproperties': self.stats['loaded_macroproperties'],
                'field_mapping': self.field_mapping,
                'field_types': self.field_types,
                'component_count': self.component_count
            }
            
            # Сохраняем в файл
            os.makedirs('temp_data', exist_ok=True)
            metadata_file = 'temp_data/flame_macroproperty1_metadata.json'
            
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)
            
            self.logger.info(f"✅ FLAME GPU метаданные сохранены: {metadata_file}")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения метаданных: {e}")
            raise
    
    def load_macroproperty1_to_flame_gpu(self) -> pyflamegpu.CUDASimulation:
        """Основной метод загрузки MacroProperty1 в FLAME GPU"""
        self.logger.info("🚀 Начало загрузки MacroProperty1 в FLAME GPU")
        self.stats['start_time'] = datetime.now()
        
        try:
            # 1. Получение маппинга полей
            field_mapping = self.get_field_mapping_from_clickhouse()
            
            # 2. Получение типов данных
            field_types = self.get_field_types_from_clickhouse()
            
            # 3. Загрузка данных из ClickHouse
            data, field_order = self.load_md_components_from_clickhouse()
            
            # 4. Создание FLAME GPU модели
            model = self.create_flame_gpu_model()
            
            # 5. Создание Environment с MacroProperty1
            env = self.create_macroproperty1_environment(field_mapping, field_types)
            
            # 6. Создание CUDASimulation
            simulation = self.create_flame_gpu_simulation()
            
            # 7. Загрузка данных в MacroProperty1
            self.load_data_into_macroproperty1(data, field_order, field_mapping, field_types)
            
            # 8. Сохранение метаданных
            self.save_flame_metadata()
            
            self.stats['end_time'] = datetime.now()
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            
            self.logger.info("✅ MacroProperty1 успешно загружено в FLAME GPU!")
            self.logger.info(f"📊 Статистика загрузки:")
            self.logger.info(f"   • FLAME GPU версия: {pyflamegpu.VERSION_FULL}")
            self.logger.info(f"   • Записей: {self.stats['total_records']}")
            self.logger.info(f"   • MacroProperty полей: {self.stats['loaded_macroproperties']}")
            self.logger.info(f"   • Время загрузки: {duration:.2f} сек")
            
            return simulation
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки MacroProperty1 в FLAME GPU: {e}")
            raise

    def load_macroproperty1_complete(self) -> Tuple[pyflamegpu.CUDASimulation, Dict[str, int], Dict[str, str], int]:
        """Полная загрузка MacroProperty1 с возвратом всех необходимых данных для валидации"""
        self.logger.info("🚀 Полная загрузка MacroProperty1 для валидации")
        
        try:
            # Загружаем все как обычно
            simulation = self.load_macroproperty1_to_flame_gpu()
            
            # Возвращаем simulation и все необходимые данные для валидации
            return simulation, self.field_mapping, self.field_types, self.stats['total_records']
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка полной загрузки MacroProperty1: {e}")
            raise

def main():
    """Основная функция"""
    logger = setup_logging()
    
    try:
        # Проверяем версию FLAME GPU
        logger.info(f"🎮 FLAME GPU версия: {pyflamegpu.VERSION_FULL}")
        
        # Проверяем версионные параметры
        version_date = None
        version_id = None
        
        if len(sys.argv) >= 3:
            if '--version-date' in sys.argv:
                idx = sys.argv.index('--version-date')
                if idx + 1 < len(sys.argv):
                    version_date = datetime.fromisoformat(sys.argv[idx + 1])
            
            if '--version-id' in sys.argv:
                idx = sys.argv.index('--version-id')
                if idx + 1 < len(sys.argv):
                    version_id = int(sys.argv[idx + 1])
        
        # Создаем FLAME GPU загрузчик
        loader = FlameMacroProperty1Loader(
            version_date=version_date,
            version_id=version_id
        )
        
        # Загружаем MacroProperty1 в FLAME GPU
        simulation = loader.load_macroproperty1_to_flame_gpu()
        
        logger.info("🎯 MacroProperty1 готово в FLAME GPU для дальнейшей работы!")
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 