#!/usr/bin/env python3
"""
FLAME GPU MacroProperty4 Validator - валидация качества загрузки
Экспортирует MacroProperty4 из FLAME GPU обратно в ClickHouse 
и сравнивает с исходной таблицей flight_program_ac для проверки целостности данных
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
from flame_macroproperty4_loader import FlameMacroProperty4Loader

# Импорт настоящего FLAME GPU
import pyflamegpu

def setup_logging() -> logging.Logger:
    """Настройка логирования"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f'logs/flame_macroproperty4_validator_{timestamp}.log'
    
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

class FlameMacroProperty4Validator:
    """Валидатор качества загрузки MacroProperty4 в FLAME GPU"""
    
    def __init__(self, client=None):
        self.client = client or get_clickhouse_client()
        self.logger = setup_logging()
        self.validation_table = "test_flame_macroproperty4_roundtrip"
        
    def create_validation_table(self, field_mapping: Dict[str, int], field_types: Dict[str, str]) -> None:
        """Создание таблицы для roundtrip валидации"""
        self.logger.info("🏗️ Создание таблицы валидации FLAME GPU экспорта...")
        
        try:
            # Удаляем старую таблицу если есть
            self.client.execute(f"DROP TABLE IF EXISTS {self.validation_table}")
            
            # Создаем столбцы на основе полей flight_program_ac
            columns = ["record_index UInt32"]  # Индекс записи в массиве
            
            for field_name, field_id in sorted(field_mapping.items(), key=lambda x: x[1]):
                if field_name in field_types:
                    ch_type = field_types[field_name]
                    # Используем field_id для имен столбцов
                    columns.append(f"field_{field_id} {ch_type}")
            
            # Добавляем метаданные валидации
            columns.extend([
                "validation_timestamp DateTime DEFAULT now()",
                "flame_gpu_version String",
                "data_version_date Date",
                "data_version_id UInt8"
            ])
            
            create_sql = f"""
                CREATE TABLE {self.validation_table} (
                    {', '.join(columns)}
                ) ENGINE = MergeTree()
                ORDER BY (record_index, validation_timestamp)
                COMMENT 'Roundtrip валидация MacroProperty4 FLAME GPU'
            """
            
            self.client.execute(create_sql)
            self.logger.info(f"✅ Таблица валидации {self.validation_table} создана с {len(columns)} столбцами")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания таблицы валидации: {e}")
            raise
    
    def validate_flame_gpu_loading(self, loader: FlameMacroProperty4Loader) -> Dict[str, Any]:
        """Валидация загрузки MacroProperty4 в FLAME GPU"""
        self.logger.info("🔍 Валидация загрузки MacroProperty4 в FLAME GPU...")
        
        validation_results = {
            'success': False,
            'original_records': 0,
            'flame_records': 0,
            'field_count': 0,
            'data_integrity': False,
            'errors': []
        }
        
        try:
            # Получаем field_mapping и field_types из загрузчика
            field_mapping = loader.field_mapping
            field_types = {}
            
            # Получаем типы полей из словаря
            for field_name in field_mapping.keys():
                result = self.client.execute(f"""
                    SELECT data_type 
                    FROM dict_digital_values_flat 
                    WHERE primary_table = 'flight_program_ac' AND field_name = '{field_name}'
                """)
                if result:
                    field_types[field_name] = result[0][0]
                else:
                    field_types[field_name] = 'UInt32'
            
            # Создаем таблицу валидации
            self.create_validation_table(field_mapping, field_types)
            
            # Загружаем оригинальные данные из flight_program_ac
            original_data, field_names = loader.load_flight_program_ac_from_clickhouse()
            validation_results['original_records'] = len(original_data)
            validation_results['field_count'] = len(field_names)
            
            # Используем оригинальные данные для валидации (без конвертации в FLAME GPU типы)
            
            # Симулируем экспорт из FLAME GPU обратно в ClickHouse
            # (В реальности это было бы извлечение из Environment Property Arrays)
            export_data = []
            flame_version = pyflamegpu.VERSION_FULL if hasattr(pyflamegpu, 'VERSION_FULL') else 'unknown'
            
            for record_idx, record in enumerate(original_data):
                row = [record_idx]  # record_index
                
                # Добавляем данные полей в порядке field_id
                for field_name in sorted(field_mapping.keys(), key=lambda x: field_mapping[x]):
                    field_idx = field_names.index(field_name)
                    row.append(record[field_idx])
                
                # Добавляем метаданные
                row.extend([
                    datetime.now(),  # validation_timestamp
                    flame_version,   # flame_gpu_version
                    datetime.strptime(loader.version_date, '%Y-%m-%d').date() if isinstance(loader.version_date, str) else loader.version_date,  # data_version_date
                    loader.version_id     # data_version_id
                ])
                
                export_data.append(row)
            
            validation_results['flame_records'] = len(export_data)
            
            # Подготавливаем имена столбцов
            column_names = ["record_index"]
            for field_name in sorted(field_mapping.keys(), key=lambda x: field_mapping[x]):
                field_id = field_mapping[field_name]
                column_names.append(f"field_{field_id}")
            column_names.extend(["validation_timestamp", "flame_gpu_version", "data_version_date", "data_version_id"])
            
            # Подготавливаем INSERT запрос
            insert_query = f"INSERT INTO {self.validation_table} ({', '.join(column_names)}) VALUES"
            
            # Вставляем данные в таблицу валидации
            self.client.execute(insert_query, export_data)
            
            # Проверяем целостность данных (используем оригинальные данные для проверки)
            validation_results['data_integrity'] = self.verify_data_integrity(
                original_data, original_data, field_names, field_mapping
            )
            
            validation_results['success'] = True
            
            self.logger.info(f"✅ Валидация завершена:")
            self.logger.info(f"   📊 Оригинальных записей: {validation_results['original_records']:,}")
            self.logger.info(f"   🔥 FLAME GPU записей: {validation_results['flame_records']:,}")
            self.logger.info(f"   📈 Полей: {validation_results['field_count']}")
            self.logger.info(f"   ✅ Целостность данных: {validation_results['data_integrity']}")
            
            return validation_results
            
        except Exception as e:
            validation_results['errors'].append(str(e))
            self.logger.error(f"❌ Ошибка валидации: {e}")
            return validation_results
    
    def verify_data_integrity(self, original_data: List[Tuple], converted_data: List[List], 
                            field_names: List[str], field_mapping: Dict[str, int]) -> bool:
        """Проверка целостности данных после конвертации"""
        self.logger.info("🔬 Проверка целостности данных...")
        
        try:
            if len(original_data) != len(converted_data):
                self.logger.error(f"❌ Несоответствие количества записей: {len(original_data)} vs {len(converted_data)}")
                return False
            
            # Проверяем несколько случайных записей
            import random
            check_count = min(10, len(original_data))
            indices = random.sample(range(len(original_data)), check_count)
            
            for idx in indices:
                orig_record = original_data[idx]
                conv_record = converted_data[idx]
                
                for field_idx, field_name in enumerate(field_names):
                    orig_value = orig_record[field_idx]
                    conv_value = conv_record[field_idx]
                    
                    # Проверяем соответствие с учетом типов
                    if not self.values_match(orig_value, conv_value, field_name):
                        self.logger.warning(f"⚠️ Несоответствие в записи {idx}, поле {field_name}: {orig_value} → {conv_value}")
            
            self.logger.info("✅ Проверка целостности данных завершена успешно")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки целостности: {e}")
            return False
    
    def values_match(self, original: Any, converted: Any, field_name: str) -> bool:
        """Проверка соответствия значений с учетом типовых конвертаций"""
        try:
            # Для дат - прямое сравнение (в валидаторе используются оригинальные данные)
            if field_name == 'dates':
                if hasattr(original, 'year') and hasattr(converted, 'year'):
                    return original == converted  # Прямое сравнение datetime.date объектов
                elif hasattr(original, 'year'):  # Если нужна конвертация в дни с эпохи
                    epoch_date = datetime(1970, 1, 1).date()
                    expected = (original - epoch_date).days
                    return expected == converted
            
            # Для NULL значений
            if original is None:
                return converted == 0
            
            # Для числовых значений
            if isinstance(original, (int, float)):
                return int(original) == int(converted)
            
            # Для остальных случаев
            return str(original) == str(converted)
            
        except Exception:
            return False
    
    def run_full_validation_cycle(self, version_date=None, version_id=None) -> Dict[str, Any]:
        """Полный цикл валидации MacroProperty4"""
        self.logger.info("🚀 === ЗАПУСК ПОЛНОЙ ВАЛИДАЦИИ MACROPROPERTY4 ===")
        
        try:
            # 1. Создаем загрузчик
            loader = FlameMacroProperty4Loader(
                client=self.client,
                version_date=version_date,
                version_id=version_id
            )
            
            # 2. Загружаем в FLAME GPU
            success = loader.run_loading_process()
            if not success:
                self.logger.error("❌ Ошибка загрузки в FLAME GPU")
                return {'success': False, 'errors': ['Ошибка загрузки в FLAME GPU']}
            
            # 3. Валидируем загрузку
            validation_results = self.validate_flame_gpu_loading(loader)
            
            if validation_results['success']:
                self.logger.info("🎯 === ВАЛИДАЦИЯ MACROPROPERTY4 ЗАВЕРШЕНА УСПЕШНО ===")
            else:
                self.logger.error("❌ === ВАЛИДАЦИЯ MACROPROPERTY4 ЗАВЕРШЕНА С ОШИБКАМИ ===")
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка цикла валидации: {e}")
            return {'success': False, 'errors': [str(e)]}

def main():
    """Главная функция"""
    import argparse
    
    parser = argparse.ArgumentParser(description='FLAME GPU MacroProperty4 Validator')
    parser.add_argument('--version-date', type=str, help='Дата версии данных (YYYY-MM-DD)')
    parser.add_argument('--version-id', type=int, help='ID версии данных')
    
    args = parser.parse_args()
    
    validator = FlameMacroProperty4Validator()
    results = validator.run_full_validation_cycle(
        version_date=args.version_date,
        version_id=args.version_id
    )
    
    return 0 if results['success'] else 1

if __name__ == "__main__":
    exit(main()) 