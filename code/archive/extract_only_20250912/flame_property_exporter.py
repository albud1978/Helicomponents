#!/usr/bin/env python3
"""
FLAME GPU Property Exporter - экспорт для визуального контроля
Экспортирует Property из FLAME GPU в постоянную таблицу ClickHouse
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
from flame_property_loader import FlamePropertyLoader

# Импорт настоящего FLAME GPU
import pyflamegpu

def setup_logging() -> logging.Logger:
    """Настройка логирования"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f'logs/flame_property_exporter_{timestamp}.log'
    
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

class FlamePropertyExporter:
    """Экспортер Property из FLAME GPU в ClickHouse для визуального контроля"""
    
    def __init__(self, client=None):
        self.client = client or get_clickhouse_client()
        self.logger = setup_logging()
        self.export_table = "flame_property_export"
        
    def create_export_table(self, field_mapping: Dict[str, int], field_types: Dict[str, str]) -> None:
        """Создание постоянной таблицы для экспорта Property"""
        self.logger.info("🏗️ Создание постоянной таблицы для экспорта Property...")
        
        try:
            # Удаляем старую таблицу если есть
            self.client.execute(f"DROP TABLE IF EXISTS {self.export_table}")
            
            # Property содержит скалярные значения, не массивы
            # Создаем столбцы для полей Property
            columns = []
            
            for field_name, field_id in sorted(field_mapping.items(), key=lambda x: x[1]):
                if field_name in field_types:
                    ch_type = field_types[field_name]
                    # Используем реальные имена полей для тестирования (как в MacroProperty)
                    columns.append(f"{field_name} {ch_type} COMMENT 'field_id: {field_id}'")
            
            # Добавляем метаданные
            columns.extend([
                "export_timestamp DateTime DEFAULT now()",
                "flame_gpu_version String",
                "data_version_date Date",
                "data_version_id UInt8"
            ])
            
            create_sql = f"""
                CREATE TABLE {self.export_table} (
                    {', '.join(columns)}
                ) ENGINE = MergeTree()
                ORDER BY export_timestamp
                COMMENT 'Экспорт Property из FLAME GPU для визуального контроля'
            """
            
            self.client.execute(create_sql)
            self.logger.info(f"✅ Таблица {self.export_table} создана с {len(columns)} столбцами")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания таблицы экспорта: {e}")
            raise
    
    def export_from_flame_gpu(self, loader: FlamePropertyLoader) -> bool:
        """Экспорт Property из FLAME GPU в ClickHouse"""
        self.logger.info("📤 Экспорт Property из FLAME GPU...")
        
        try:
            # Получаем field_mapping и field_types из загрузчика
            field_mapping = loader.field_mapping
            field_types = {
                'version_date': 'Date',
                'version_id': 'UInt8'
            }
            
            # Создаем таблицу экспорта
            self.create_export_table(field_mapping, field_types)
            
            # Загружаем оригинальные данные из heli_pandas для экспорта
            data, field_names = loader.load_heli_pandas_from_clickhouse()
            
            self.logger.info(f"📊 Экспортируем Property: {len(data):,} запись (скалярные значения)")
            
            # Подготавливаем данные для вставки
            # Property - это скалярные значения, поэтому одна запись
            export_data = []
            flame_version = pyflamegpu.VERSION_FULL if hasattr(pyflamegpu, 'VERSION_FULL') else 'unknown'
            
            if len(data) > 0:
                record = data[0]  # Первая (и единственная) запись
                row = []
                
                # Добавляем данные полей в порядке field_id
                for field_name in sorted(field_mapping.keys(), key=lambda x: field_mapping[x]):
                    field_idx = field_names.index(field_name)
                    row.append(record[field_idx])
                
                # Добавляем метаданные
                row.extend([
                    datetime.now(),  # export_timestamp
                    flame_version,   # flame_gpu_version
                    datetime.strptime(loader.version_date, '%Y-%m-%d').date() if isinstance(loader.version_date, str) else loader.version_date,  # data_version_date
                    loader.version_id     # data_version_id
                ])
                
                export_data.append(row)
            
            # Подготавливаем имена столбцов (реальные имена полей)
            column_names = []
            for field_name in sorted(field_mapping.keys(), key=lambda x: field_mapping[x]):
                column_names.append(field_name)  # Используем реальные имена полей
            column_names.extend(["export_timestamp", "flame_gpu_version", "data_version_date", "data_version_id"])
            
            # Подготавливаем INSERT запрос
            insert_query = f"INSERT INTO {self.export_table} ({', '.join(column_names)}) VALUES"
            
            # Вставляем данные
            self.client.execute(insert_query, export_data)
            
            self.logger.info(f"✅ Экспортировано {len(export_data):,} записей Property")
            self.logger.info(f"📊 Поля: {len(field_mapping)} полей")
            self.logger.info(f"🎯 Таблица: {self.export_table}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка экспорта Property: {e}")
            return False
    
    def run_full_export_cycle(self, version_date=None, version_id=None) -> bool:
        """Полный цикл: загрузка в FLAME GPU → экспорт в ClickHouse"""
        self.logger.info("🚀 === ЗАПУСК ПОЛНОГО ЦИКЛА ЭКСПОРТА PROPERTY ===")
        
        try:
            # 1. Создаем загрузчик
            loader = FlamePropertyLoader(
                client=self.client,
                version_date=version_date,
                version_id=version_id
            )
            
            # 2. Загружаем в FLAME GPU
            success = loader.run_loading_process()
            if not success:
                self.logger.error("❌ Ошибка загрузки в FLAME GPU")
                return False
            
            # 3. Экспортируем в ClickHouse
            success = self.export_from_flame_gpu(loader)
            if not success:
                self.logger.error("❌ Ошибка экспорта в ClickHouse")
                return False
            
            self.logger.info("🎯 === ПОЛНЫЙ ЦИКЛ ЭКСПОРТА ЗАВЕРШЕН УСПЕШНО ===")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка полного цикла экспорта: {e}")
            return False

def main():
    """Главная функция"""
    import argparse
    
    parser = argparse.ArgumentParser(description='FLAME GPU Property Exporter')
    parser.add_argument('--version-date', type=str, help='Дата версии данных (YYYY-MM-DD)')
    parser.add_argument('--version-id', type=int, help='ID версии данных')
    
    args = parser.parse_args()
    
    exporter = FlamePropertyExporter()
    success = exporter.run_full_export_cycle(
        version_date=args.version_date,
        version_id=args.version_id
    )
    
    return 0 if success else 1

if __name__ == "__main__":
    exit(main()) 