#!/usr/bin/env python3
"""
FLAME GPU MacroProperty3 Loader для Transform этапа
Загружает данные heli_pandas в настоящий FLAME GPU MacroProperty3
Использует pyflamegpu для создания реальных GPU структур данных
psn является agent_id для агентов в FLAME GPU
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
    
    log_file = os.path.join(log_dir, f"flame_macroproperty3_loader_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"🔥 Запуск FLAME GPU MacroProperty3 Loader")
    logger.info(f"📋 Лог файл: {log_file}")
    
    return logger

class FlameMacroProperty3Loader:
    """Загрузчик MacroProperty3 (heli_pandas) в FLAME GPU Environment Property Arrays"""
    
    def __init__(self):
        self.logger = setup_logging()
        self.client = None
        self.model = None
        self.environment = None
        self.simulation = None
        self.agent_count = 0
        self.field_mapping = {}
        self.field_types = {}
        self.stats = {
            'total_records': 0,
            'loaded_macroproperties': 0,
            'agents_with_psn': 0,
            'null_conversions': {}
        }
        
        # Устанавливаем соединение с ClickHouse
        try:
            self.client = get_clickhouse_client()
            self.logger.info("✅ Соединение с ClickHouse установлено")
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения к ClickHouse: {e}")
            raise

    def get_field_mapping_from_clickhouse(self) -> Dict[str, int]:
        """Получение field_id маппинга из dict_digital_values_flat"""
        self.logger.info("🔍 Получение field_id маппинга для heli_pandas...")
        
        try:
            result = self.client.execute("""
                SELECT field_name, field_id 
                FROM dict_digital_values_flat 
                WHERE primary_table = 'heli_pandas'
                ORDER BY field_id
            """)
            
            self.field_mapping = {row[0]: row[1] for row in result}
            self.logger.info(f"✅ Получен field_id маппинг для {len(self.field_mapping)} полей")
            
            for field_name, field_id in sorted(self.field_mapping.items(), key=lambda x: x[1]):
                self.logger.info(f"   📋 {field_name} → field_id: {field_id}")
            
            return self.field_mapping
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения field_id маппинга: {e}")
            raise

    def get_field_types_from_clickhouse(self) -> Dict[str, str]:
        """Получение типов полей из ClickHouse"""
        self.logger.info("🔍 Анализ типов полей heli_pandas...")
        
        try:
            result = self.client.execute("DESCRIBE TABLE heli_pandas")
            self.field_types = {row[0]: row[1] for row in result}
            
            self.logger.info(f"✅ Проанализировано {len(self.field_types)} типов полей")
            return self.field_types
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка анализа типов полей: {e}")
            raise

    def load_heli_pandas_from_clickhouse(self) -> Tuple[List[Tuple], List[str]]:
        """Загрузка данных heli_pandas из ClickHouse"""
        self.logger.info("📥 Загрузка данных heli_pandas из ClickHouse...")
        
        try:
            # ТОЛЬКО поля из аналитики MacroProperty3 - цифровые поля для GPU
            analytics_fields = [
                'partseqno_i',     # field_id: 46 - uint32
                'psn',             # field_id: 48 - uint32 (agent_id)
                'address_i',       # field_id: 7  - uint16
                'lease_restricted', # field_id: 24 - uint8
                'group_by',        # NEW: группировка взаимозаменяемости
                'status_id',       # field_id: 61 - uint8
                'status_change',   # NEW: метка перехода на D
                'aircraft_number', # field_id: 8  - uint32
                'ac_type_mask',    # field_id: 4  - multihot[u8]
                'll',              # field_id: 25 - uint32
                'oh',              # field_id: 33 - uint32
                'oh_threshold',    # field_id: 36 - uint32
                'sne',             # field_id: 59 - uint32
                'ppr',             # field_id: 47 - uint32
                'repair_days',     # field_id: 52 - uint16
                'mfg_date'         # field_id: 30 - Date
            ]
            
            # Фильтруем только те поля, которые есть в field_mapping и в аналитике
            mapped_fields = [field for field in analytics_fields if field in self.field_mapping]
            field_list = ", ".join(mapped_fields)
            
            self.logger.info(f"🎯 Загружаем ТОЛЬКО поля из аналитики MacroProperty3:")
            for field in mapped_fields:
                field_id = self.field_mapping.get(field, 'N/A')
                self.logger.info(f"   📋 {field} (field_id: {field_id})")
            
            # Исключаем текстовые поля (partno, serialno, ac_typ, location, condition, owner)
            
            # ИСПРАВЛЯЕМ: Загружаем ВСЕ поля как в MacroProperty1, потом фильтруем при обработке
            query = """
                SELECT * FROM heli_pandas 
                WHERE (version_date, version_id) = (
                    SELECT version_date, version_id 
                    FROM heli_pandas 
                    ORDER BY version_date DESC, version_id DESC 
                    LIMIT 1
                )
                ORDER BY psn
            """
            
            data = self.client.execute(query)
            self.agent_count = len(data)
            self.stats['total_records'] = self.agent_count
            
            # Получаем порядок полей - КАК В MACROPROPERTY1
            field_order_query = """
                SELECT name, position 
                FROM system.columns 
                WHERE database = 'default' AND table = 'heli_pandas'
                ORDER BY position
            """
            column_info = self.client.execute(field_order_query)
            field_order = [col_name for col_name, pos in column_info]
            
            # Подсчет агентов с валидным psn (для agent_id)
            psn_index = field_order.index('psn') if 'psn' in field_order else -1
            agents_with_psn = 0
            if psn_index >= 0:
                agents_with_psn = sum(1 for row in data if row[psn_index] is not None)
                self.stats['agents_with_psn'] = agents_with_psn
            
            self.logger.info(f"✅ Загружено {self.agent_count} записей heli_pandas")
            self.logger.info(f"🎯 Агентов с валидным psn (agent_id): {agents_with_psn}")
            
            return data, field_order
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки данных heli_pandas: {e}")
            raise

    def analyze_data_types(self, field_order: List[str]) -> Dict[str, str]:
        """Анализ соответствия типов ClickHouse и FLAME GPU"""
        self.logger.info("🔍 Анализ соответствия типов данных...")
        
        gpu_types = {}
        for field_name in field_order:
            ch_type = self.field_types.get(field_name, 'String')
            
            # Маппинг типов ClickHouse → FLAME GPU
            if 'UInt32' in ch_type or 'Nullable(UInt32)' in ch_type:
                gpu_types[field_name] = 'UInt32'
            elif 'UInt16' in ch_type or 'Nullable(UInt16)' in ch_type:
                gpu_types[field_name] = 'UInt16'
            elif 'UInt8' in ch_type:
                gpu_types[field_name] = 'UInt8'
            elif 'Float64' in ch_type or 'Nullable(Float64)' in ch_type:
                gpu_types[field_name] = 'Float64'
            elif 'Float32' in ch_type or 'Nullable(Float32)' in ch_type:
                gpu_types[field_name] = 'Float32'
            elif 'Date' in ch_type or 'Nullable(Date)' in ch_type:
                gpu_types[field_name] = 'UInt16'  # Date как UInt16 для FLAME GPU
            else:
                gpu_types[field_name] = 'UInt32'  # String как UInt32 hash
                
            self.logger.info(f"   🔄 {field_name}: {ch_type} → {gpu_types[field_name]}")
        
        return gpu_types

    def create_flame_gpu_model(self) -> pyflamegpu.ModelDescription:
        """Создание FLAME GPU ModelDescription"""
        self.logger.info("🏗️ Создание FLAME GPU ModelDescription для Heli_Rotable...")
        
        self.model = pyflamegpu.ModelDescription('Heli_Rotable')
        self.logger.info(f"✅ FLAME GPU модель '{self.model.getName()}' создана")
        self.logger.info(f"🎯 FLAME GPU версия: {pyflamegpu.VERSION_FULL}")
        
        return self.model

    def create_macroproperty3_environment(self, field_mapping: Dict[str, int], 
                                        field_types: Dict[str, str]) -> pyflamegpu.EnvironmentDescription:
        """Создание Environment с Property Arrays для MacroProperty3"""
        self.logger.info("🌍 Создание FLAME GPU Environment с Property Arrays...")
        
        try:
            env = self.model.Environment()
            self.environment = env
            
            # ТОЧНАЯ КОПИЯ ИЗ MACROPROPERTY1
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
            
            # Поля из аналитики MacroProperty3 (исключаем текстовые)
            # Добавлены group_by и status_change для поддержки RTC фильтров и меток переходов
            analytics_fields = [
                'partseqno_i', 'psn', 'address_i', 'lease_restricted',
                'group_by', 'status_id', 'status_change',
                'aircraft_number', 'ac_type_mask', 'll', 'oh', 'oh_threshold',
                'sne', 'ppr', 'repair_days', 'mfg_date'
            ]
            
            created_properties = 0
            
            # Создаем Property Array для каждого поля - ТОЧНАЯ КОПИЯ ИЗ MACROPROPERTY1
            for field_name, field_id in sorted(field_mapping.items(), key=lambda x: x[1]):
                # Фильтруем только аналитические поля
                if field_name not in analytics_fields:
                    self.logger.info(f"   ⏭️ Пропускаем {field_name} (не в аналитике MacroProperty3)")
                    continue
                    
                if field_name in field_types:
                    ch_type = field_types[field_name]
                    
                    # Определяем метод FLAME GPU - ТОЧНАЯ КОПИЯ
                    flame_method = None
                    for ch_pattern, method in type_mapping.items():
                        if ch_pattern in ch_type:
                            flame_method = method
                            break
                    
                    if flame_method:
                        # Создаем Property Array с пустыми значениями - ТОЧНАЯ КОПИЯ
                        property_name = f"field_{field_id}"
                        default_array = [0] * self.agent_count  # Пустые значения
                        
                        method_func = getattr(env, flame_method)
                        method_func(property_name, default_array)
                        
                        created_properties += 1
                        self.logger.info(f"   ✅ Property Array {property_name} ({field_name}): "
                                       f"{flame_method} size={self.agent_count}")
                    else:
                        self.logger.warning(f"   ⚠️ Неизвестный тип {ch_type} для поля {field_name}")
            
            self.stats['loaded_macroproperties'] = created_properties
            self.logger.info(f"🎯 Создано {created_properties} Property Arrays в FLAME GPU Environment")
            
            return env
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания Environment: {e}")
            raise

    def create_flame_gpu_simulation(self) -> pyflamegpu.CUDASimulation:
        """Создание CUDASimulation"""
        self.logger.info("🚀 Создание FLAME GPU CUDASimulation...")
        
        self.simulation = pyflamegpu.CUDASimulation(self.model)
        self.logger.info("✅ FLAME GPU CUDASimulation создана")
        
        return self.simulation

    def load_data_into_macroproperty3(self, data: List[Tuple], field_order: List[str], 
                                    field_mapping: Dict[str, int], field_types: Dict[str, str]) -> None:
        """Загрузка данных в FLAME GPU MacroProperty3"""
        self.logger.info("📥 Загрузка данных в FLAME GPU MacroProperty3...")
        
        try:
            # ТОЧНАЯ КОПИЯ ЛОГИКИ ИЗ MACROPROPERTY1
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
            
            # Поля из аналитики MacroProperty3 (исключаем текстовые)
            # Добавлены group_by и status_change для согласованности с Environment
            analytics_fields = [
                'partseqno_i', 'psn', 'address_i', 'lease_restricted',
                'group_by', 'status_id', 'status_change',
                'aircraft_number', 'ac_type_mask', 'll', 'oh', 'oh_threshold',
                'sne', 'ppr', 'repair_days', 'mfg_date'
            ]
            
            loaded_properties = 0
            
            # ДИАГНОСТИКА field_order
            self.logger.info(f"🔍 field_order длина: {len(field_order)}")
            self.logger.info(f"🔍 field_order[:5]: {field_order[:5]}")
            if 'partseqno_i' in field_order:
                idx = field_order.index('partseqno_i')
                self.logger.info(f"🔍 partseqno_i индекс в field_order: {idx}")
            
            # Загружаем данные для каждого поля - ТОЧНО КАК В MACROPROPERTY1
            for field_name in field_order:
                # Фильтруем только аналитические поля
                if field_name not in analytics_fields:
                    self.logger.info(f"   ⏭️ Пропускаем {field_name} (не в аналитике MacroProperty3)")
                    continue
                    
                if field_name in field_mapping and field_name in field_types:
                    field_id = field_mapping[field_name]
                    ch_type = field_types[field_name]
                    column_index = field_order.index(field_name)
                    
                    # Извлекаем данные столбца с обработкой NULL значений - КАК В MACROPROPERTY1
                    raw_column_data = [row[column_index] for row in data]
                    
                    # МИНИМАЛЬНАЯ ДИАГНОСТИКА ТОЛЬКО ДЛЯ ПЕРВОГО ПОЛЯ
                    if field_name == 'partseqno_i':
                        self.logger.info(f"   🔍 {field_name}: column_index={column_index}, raw_data[:3]={raw_column_data[:3]}, типы={[type(v) for v in raw_column_data[:3]]}")
                    
                    # Обрабатываем NULL значения в зависимости от типа - ТОЧНАЯ КОПИЯ
                    default_values = {
                        'UInt32': 0,
                        'UInt16': 0,
                        'UInt8': 0,
                        'Float32': 0.0,
                        'Float64': 0.0,
                        'Date': 0,
                        'String': 0
                    }
                    
                    # Определяем дефолтное значение - ТОЧНАЯ КОПИЯ ЛОГИКИ
                    default_val = 0
                    for ch_pattern, default in default_values.items():
                        if ch_pattern in ch_type:
                            default_val = default
                            break
                    
                    # Заменяем None на дефолтные значения - ТОЧНАЯ КОПИЯ
                    column_data = [default_val if val is None else val for val in raw_column_data]
                    
                    # СПЕЦИАЛЬНАЯ ОБРАБОТКА ДЛЯ DATE ПОЛЕЙ - КАК В MACROPROPERTY1
                    if 'Date' in ch_type:
                        from datetime import date
                        epoch = date(1970, 1, 1)
                        processed_data = []
                        for val in column_data:
                            if val == default_val or val is None:
                                processed_data.append(0)
                            elif hasattr(val, 'toordinal'):
                                # Дата объект
                                days_since_epoch = val.toordinal() - epoch.toordinal()
                                processed_data.append(max(0, days_since_epoch))
                            else:
                                processed_data.append(0)
                        column_data = processed_data
                    
                    # Определяем метод установки - ТОЧНАЯ КОПИЯ
                    set_method = None
                    for ch_pattern, method in set_methods.items():
                        if ch_pattern in ch_type:
                            set_method = method
                            break
                    
                    if set_method and hasattr(self.simulation, set_method):
                        property_name = f"field_{field_id}"
                        
                        # Устанавливаем данные в Environment Property Array - ТОЧНАЯ КОПИЯ
                        method_func = getattr(self.simulation, set_method)
                        method_func(property_name, column_data)
                        
                        loaded_properties += 1
                        self.logger.info(f"   ✅ Загружено {len(column_data)} значений в "
                                       f"{property_name} ({field_name}) методом {set_method}")
                    else:
                        self.logger.warning(f"   ⚠️ Метод {set_method} не найден для поля {field_name}")
            
            self.logger.info(f"🎯 Загружено {loaded_properties} Property Arrays с данными")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки данных в MacroProperty3: {e}")
            raise

    def save_flame_metadata(self) -> str:
        """Сохранение метаданных загрузки"""
        metadata_dir = "temp_data"
        os.makedirs(metadata_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        metadata_file = os.path.join(metadata_dir, f"flame_macroproperty3_metadata_{timestamp}.json")
        
        metadata = {
            'timestamp': timestamp,
            'model_name': self.model.getName() if self.model else None,
            'flame_gpu_version': pyflamegpu.VERSION_FULL,
            'agent_count': self.agent_count,
            'field_mapping': self.field_mapping,
            'field_types': self.field_types,
            'stats': self.stats
        }
        
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"💾 Метаданные сохранены: {metadata_file}")
        return metadata_file

    def load_macroproperty3_to_flame_gpu(self) -> pyflamegpu.CUDASimulation:
        """Основной метод загрузки MacroProperty3 в FLAME GPU"""
        self.logger.info("🚀 Начало загрузки MacroProperty3 в FLAME GPU")
        
        try:
            # 1. Получение field_id маппинга
            self.logger.info("📋 Шаг 1: Получение field_id маппинга...")
            field_mapping = self.get_field_mapping_from_clickhouse()
            
            # 2. Анализ типов данных
            self.logger.info("📊 Шаг 2: Анализ типов данных...")
            field_types = self.get_field_types_from_clickhouse()
            
            # 3. Загрузка данных из ClickHouse
            self.logger.info("📥 Шаг 3: Загрузка данных из ClickHouse...")
            data, field_order = self.load_heli_pandas_from_clickhouse()
            
            # 4. Создание FLAME GPU модели
            self.logger.info("🏗️ Шаг 4: Создание FLAME GPU модели...")
            model = self.create_flame_gpu_model()
            
            # 5. Создание Environment
            self.logger.info("🌍 Шаг 5: Создание Environment...")
            env = self.create_macroproperty3_environment(field_mapping, field_types)
            
            # 6. Создание CUDASimulation
            self.logger.info("🚀 Шаг 6: Создание CUDASimulation...")
            sim = self.create_flame_gpu_simulation()
            
            # 7. Загрузка данных в FLAME GPU
            self.logger.info("📥 Шаг 7: Загрузка данных в FLAME GPU...")
            self.load_data_into_macroproperty3(data, field_order, field_mapping, field_types)
            
            # 8. Сохранение метаданных
            self.logger.info("💾 Шаг 8: Сохранение метаданных...")
            metadata_file = self.save_flame_metadata()
            
            self.logger.info("✅ MacroProperty3 успешно загружен в FLAME GPU")
            self.logger.info(f"📊 Статистика загрузки:")
            self.logger.info(f"   • Всего агентов: {self.stats['total_records']}")
            self.logger.info(f"   • Агентов с psn (agent_id): {self.stats['agents_with_psn']}")
            self.logger.info(f"   • Загружено Property Arrays: {self.stats['loaded_macroproperties']}")
            self.logger.info(f"   • NULL конвертаций: {sum(self.stats['null_conversions'].values())}")
            
            return sim
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки MacroProperty3: {e}")
            raise

    def load_macroproperty3_complete(self) -> Tuple[pyflamegpu.CUDASimulation, Dict[str, int], Dict[str, str], int]:
        """Полная загрузка MacroProperty3 с возвратом всех необходимых данных для валидации"""
        self.logger.info("🚀 Полная загрузка MacroProperty3 для валидации")
        
        try:
            simulation = self.load_macroproperty3_to_flame_gpu()
            return simulation, self.field_mapping, self.field_types, self.stats['total_records']
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка полной загрузки MacroProperty3: {e}")
            raise

def main():
    """Основная функция для тестирования загрузчика"""
    print("🔥 FLAME GPU MacroProperty3 Loader")
    print("=" * 50)
    
    try:
        loader = FlameMacroProperty3Loader()
        simulation = loader.load_macroproperty3_to_flame_gpu()
        
        print("\n" + "=" * 50)
        print("✅ ЗАГРУЗКА MACROPROPERTY3 ЗАВЕРШЕНА УСПЕШНО!")
        print(f"🎯 Модель: {loader.model.getName()}")
        print(f"📊 Агентов загружено: {loader.stats['total_records']}")
        print(f"🔑 Агентов с psn (agent_id): {loader.stats['agents_with_psn']}")
        print(f"🌍 Property Arrays: {loader.stats['loaded_macroproperties']}")
        
        if loader.stats['null_conversions']:
            print(f"🔄 NULL конвертации:")
            for field, count in loader.stats['null_conversions'].items():
                print(f"   • {field}: {count} значений")
        
        print("=" * 50)
        
    except Exception as e:
        print(f"\n❌ ОШИБКА: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 