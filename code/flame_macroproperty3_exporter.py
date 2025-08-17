#!/usr/bin/env python3
"""
FLAME GPU MacroProperty3 Exporter - экспорт для визуального контроля
Экспортирует MacroProperty3 из FLAME GPU в постоянную таблицу ClickHouse
для визуального контроля и анализа пользователем
"""

import logging
import sys
import os
import json
from typing import Dict, List, Tuple, Any
from datetime import datetime

# Добавляем путь к utils
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))

from config_loader import get_clickhouse_client
from flame_macroproperty3_loader import FlameMacroProperty3Loader

# Импорт настоящего FLAME GPU
import pyflamegpu

def setup_logging() -> logging.Logger:
    """Настройка логирования"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f'logs/flame_macroproperty3_exporter_{timestamp}.log'
    
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

class FlameMacroProperty3Exporter:
    """Экспортер MacroProperty3 из FLAME GPU в ClickHouse для визуального контроля"""
    
    def __init__(self, client=None):
        self.client = client or get_clickhouse_client()
        self.logger = setup_logging()
        self.export_table = "flame_macroproperty3_export"
        
    def create_export_table(self, field_mapping: Dict[str, int], field_types: Dict[str, str]) -> None:
        """Создание постоянной таблицы для экспорта MacroProperty3"""
        self.logger.info("🏗️ Создание постоянной таблицы для экспорта MacroProperty3...")
        
        try:
            # Удаляем старую таблицу если есть
            self.client.execute(f"DROP TABLE IF EXISTS {self.export_table}")
            
            # Получаем аналитические поля MacroProperty3
            analytics_fields = [
                'partseqno_i', 'psn', 'address_i', 'lease_restricted', 'group_by', 'status_id',
                'status_change', 'aircraft_number', 'ac_type_mask', 'll', 'oh', 'oh_threshold',
                'sne', 'ppr', 'repair_days', 'mfg_date'
            ]
            
            # Создаем DDL с field_id комментариями
            fields_ddl = ['record_id UInt32 COMMENT \'Порядковый номер записи\'']
            
            for field_name in analytics_fields:
                if field_name in field_mapping:
                    field_id = field_mapping[field_name]
                    ch_type = field_types.get(field_name, 'UInt32')
                    
                    # Убираем Nullable для экспортной таблицы
                    if 'Nullable(' in ch_type:
                        ch_type = ch_type.replace('Nullable(', '').replace(')', '')
                    
                    # Преобразуем Date в UInt16 для FLAME GPU совместимости
                    if ch_type == 'Date':
                        ch_type = 'UInt16'
                    
                    field_ddl = f"{field_name} {ch_type} COMMENT 'field_id: {field_id}'"
                    fields_ddl.append(field_ddl)
                    self.logger.info(f"   📋 {field_name} (field_id: {field_id}): {ch_type}")
            
            # Создаем таблицу с ENGINE = Memory для легкого перезаписывания
            create_table_sql = f"""
            CREATE TABLE {self.export_table} (
                {', '.join(fields_ddl)}
            ) ENGINE = Memory
            COMMENT 'Экспорт MacroProperty3 из FLAME GPU для визуального контроля'
            """
            
            self.client.execute(create_table_sql)
            self.logger.info(f"✅ Постоянная таблица {self.export_table} создана")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания таблицы экспорта: {e}")
            raise

    def export_macroproperty3_for_review(self) -> bool:
        """
        Экспорт MacroProperty3 для визуального контроля
        Создает собственный процесс загрузки для обеспечения персистентности данных
        """
        self.logger.info("🔥 Запуск FLAME GPU MacroProperty3 Exporter")
        
        sim = None
        loader = None
        
        try:
            # Создаем собственный экземпляр загрузчика
            loader = FlameMacroProperty3Loader()
            loader.client = self.client  # Устанавливаем клиент
            
            # Выполняем полную загрузку MacroProperty3
            self.logger.info("📥 Полная загрузка MacroProperty3 для экспорта...")
            
            # Шаг 1: Получение field_id маппинга
            field_mapping = loader.get_field_mapping_from_clickhouse()
            
            # Шаг 2: Анализ типов данных
            field_types = loader.get_field_types_from_clickhouse()
            
            # Шаг 3: Загрузка данных из ClickHouse
            heli_data, field_order = loader.load_heli_pandas_from_clickhouse()  # ИСПРАВЛЕНО: правильно распаковываем tuple
            
            # Шаг 4: Создание FLAME GPU модели
            model = loader.create_flame_gpu_model()
            loader.model = model  # Устанавливаем модель в загрузчик
            
            # Шаг 5: Создание Environment
            env = loader.create_macroproperty3_environment(field_mapping, field_types)
            
            # Шаг 6: Создание CUDASimulation
            sim = loader.create_flame_gpu_simulation()
            loader.simulation = sim  # Устанавливаем simulation в загрузчик
            
            # Шаг 7: Загрузка данных в FLAME GPU - ИСПОЛЬЗУЕМ ПРАВИЛЬНЫЙ field_order
            metadata = loader.load_data_into_macroproperty3(heli_data, field_order, field_mapping, field_types)
            
            self.logger.info("🔒 Simulation объект создан для экспорта")
            
            # Создаем экспортную таблицу
            self.create_export_table(field_mapping, field_types)
            
            # Экспорт данных из FLAME GPU
            self.export_flame_environment_to_clickhouse(sim, field_mapping, len(heli_data))
            
            self.logger.info("✅ Экспорт MacroProperty3 для визуального контроля завершен")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка экспорта MacroProperty3: {e}")
            return False
            
        finally:
            # Освобождение ресурсов
            if sim:
                sim = None
            if loader:
                loader = None
            self.logger.info("🗑️ Освобождение FLAME GPU ресурсов...")

    def export_flame_environment_to_clickhouse(self, sim, field_mapping: Dict[str, int], total_records: int) -> None:
        """Экспорт Environment Property Arrays из FLAME GPU в ClickHouse - РАБОЧАЯ СХЕМА из MacroProperty1"""
        self.logger.info("📤 Экспорт MacroProperty3 из FLAME GPU в ClickHouse...")
        
        try:
            # Получаем аналитические поля MacroProperty3 в правильном порядке
            analytics_fields = [
                'partseqno_i', 'psn', 'address_i', 'lease_restricted', 'group_by', 'status_id',
                'status_change', 'aircraft_number', 'ac_type_mask', 'll', 'oh', 'oh_threshold',
                'sne', 'ppr', 'repair_days', 'mfg_date'
            ]
            # Ограничиваемся только полями, которые реально существуют в heli_pandas
            try:
                ch_schema = self.client.execute("DESCRIBE TABLE heli_pandas")
                existing_fields = {row[0] for row in ch_schema}
            except Exception:
                existing_fields = set()
            available_fields = [f for f in analytics_fields if (f in field_mapping and f in existing_fields)]
            
            # Подготавливаем данные для экспорта по схеме MacroProperty1
            export_data = []
            flame_version = pyflamegpu.VERSION_FULL
            
            self.logger.info(f"📊 Экспортируем {total_records} записей из FLAME GPU...")
            self.logger.info(f"🔥 FLAME GPU версия: {flame_version}")
            
            for record_id in range(total_records):
                row = [record_id]  # record_id
                
                # Экспортируем каждое поле в аналитическом порядке
                for field_name in available_fields:
                    if field_name in field_mapping:
                        field_id = field_mapping[field_name]
                        property_name = f"field_{field_id}"
                        
                        try:
                            # Сначала пробуем UInt32 - основной тип
                            property_array = sim.getEnvironmentPropertyArrayUInt32(property_name)
                            value = property_array[record_id] if record_id < len(property_array) else 0
                            row.append(value)
                            
                        except Exception as prop_e:
                            # Пробуем другие типы данных по схеме MacroProperty1
                            try:
                                property_array = sim.getEnvironmentPropertyArrayUInt16(property_name)
                                value = property_array[record_id] if record_id < len(property_array) else 0
                                row.append(value)
                            except:
                                try:
                                    property_array = sim.getEnvironmentPropertyArrayUInt8(property_name)
                                    value = property_array[record_id] if record_id < len(property_array) else 0
                                    row.append(value)
                                except:
                                    try:
                                        property_array = sim.getEnvironmentPropertyArrayFloat(property_name)
                                        value = property_array[record_id] if record_id < len(property_array) else 0.0
                                        row.append(value)
                                    except:
                                        # Если не удалось получить, ставим 0
                                        self.logger.warning(f"⚠️ Не удалось получить {property_name} для {field_name}: {prop_e}")
                                        row.append(0)
                    else:
                        # Поле не найдено в маппинге
                        row.append(0)
                
                export_data.append(row)
            
            # Подготавливаем список полей для INSERT
            field_list = ["record_id"] + available_fields
            
            # Вставляем данные в ClickHouse
            insert_query = f"INSERT INTO {self.export_table} ({', '.join(field_list)}) VALUES"
            
            self.logger.info(f"💾 Вставляем {len(export_data)} записей в постоянную таблицу...")
            self.client.execute(insert_query, export_data)
            self.logger.info(f"✅ Экспорт завершен: {len(export_data)} записей в таблицу {self.export_table}")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка экспорта MacroProperty3: {e}")
            raise

def main():
    """Основная функция экспортера"""
    print("🔥 FLAME GPU MacroProperty3 Exporter")
    print("=" * 50)
    
    try:
        exporter = FlameMacroProperty3Exporter()
        success = exporter.export_macroproperty3_for_review()
        
        if success:
            print("\n" + "=" * 50)
            print("✅ ЭКСПОРТ MACROPROPERTY3 ЗАВЕРШЕН!")
            print(f"📊 Таблица для визуального контроля: flame_macroproperty3_export")
            print("=" * 50)
        else:
            print("\n❌ Экспорт завершился с ошибками")
            
    except Exception as e:
        print(f"\n❌ ОШИБКА: {e}")

if __name__ == "__main__":
    main() 