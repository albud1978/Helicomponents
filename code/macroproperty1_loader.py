#!/usr/bin/env python3
"""
MacroProperty1 Loader для Transform этапа
Загружает данные md_components в MacroProperty1 с field_id маппингом
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

def setup_logging() -> logging.Logger:
    """Настройка логирования"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/macroproperty1_loader.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

class MacroProperty1Loader:
    """Загрузчик MacroProperty1 (md_components) с field_id маппингом"""
    
    def __init__(self, client=None, version_date=None, version_id=None):
        self.client = client or get_clickhouse_client()
        self.logger = setup_logging()
        self.version_date = version_date
        self.version_id = version_id
        
        # Статистика загрузки
        self.stats = {
            'total_records': 0,
            'loaded_fields': 0,
            'field_mapping': {},
            'data_types': {},
            'start_time': None,
            'end_time': None
        }
    
    def get_field_mapping(self) -> Dict[str, int]:
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
            
            self.stats['field_mapping'] = field_mapping
            self.logger.info(f"✅ Загружено {len(field_mapping)} field_id маппингов")
            
            return field_mapping
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения field_id маппинга: {e}")
            raise
    
    def load_md_components_data(self) -> List[Tuple]:
        """Загрузка данных из md_components"""
        self.logger.info("📊 Загрузка данных из md_components...")
        
        try:
            # Если указаны версионные параметры, используем их
            if self.version_date and self.version_id:
                query = """
                    SELECT * FROM md_components 
                    WHERE version_date = %(version_date)s 
                    AND version_id = %(version_id)s
                    ORDER BY partno_comp
                """
                params = {
                    'version_date': self.version_date, 
                    'version_id': self.version_id
                }
                result = self.client.execute(query, params)
            else:
                # Загружаем последнюю версию
                query = """
                    SELECT * FROM md_components 
                    WHERE (version_date, version_id) = (
                        SELECT version_date, version_id 
                        FROM md_components 
                        ORDER BY version_date DESC, version_id DESC 
                        LIMIT 1
                    )
                    ORDER BY partno_comp
                """
                result = self.client.execute(query)
            
            self.stats['total_records'] = len(result)
            self.logger.info(f"✅ Загружено {len(result)} записей из md_components")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки md_components: {e}")
            raise
    
    def analyze_data_types(self, data: List[Tuple], field_mapping: Dict[str, int]) -> Dict[int, str]:
        """Анализ типов данных для каждого field_id"""
        self.logger.info("🔍 Анализ типов данных...")
        
        # Получаем информацию о структуре таблицы
        try:
            describe_query = "DESCRIBE TABLE md_components"
            table_structure = self.client.execute(describe_query)
            
            # Создаем маппинг имя_поля -> тип
            field_types = {}
            for row in table_structure:
                field_name = row[0]
                field_type = row[1]
                field_types[field_name] = field_type
            
            # Маппим field_id -> тип данных
            field_id_types = {}
            for field_name, field_id in field_mapping.items():
                if field_name in field_types:
                    field_id_types[field_id] = field_types[field_name]
                    self.logger.info(f"   🎯 field_id {field_id} ({field_name}): {field_types[field_name]}")
            
            self.stats['data_types'] = field_id_types
            return field_id_types
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка анализа типов данных: {e}")
            raise
    
    def create_macroproperty1(self, data: List[Tuple], field_mapping: Dict[str, int], 
                            field_types: Dict[int, str]) -> Dict[int, List]:
        """Создание MacroProperty1 структуры"""
        self.logger.info("🏗️ Создание MacroProperty1 структуры...")
        
        try:
            # Получаем названия полей из первой строки (если используем column names)
            # Альтернативно, берем из field_mapping ключи в правильном порядке
            
            # Сначала определяем порядок полей в результате запроса
            field_order_query = """
                SELECT name, position 
                FROM system.columns 
                WHERE database = 'default' AND table = 'md_components'
                ORDER BY position
            """
            
            column_info = self.client.execute(field_order_query)
            field_order = [col_name for col_name, pos in column_info]
            
            self.logger.info(f"📋 Порядок полей в таблице: {field_order}")
            
            # Создаем MacroProperty1 как словарь field_id -> данные
            macroproperty1 = {}
            
            for field_name in field_order:
                if field_name in field_mapping:
                    field_id = field_mapping[field_name]
                    column_index = field_order.index(field_name)
                    
                    # Извлекаем данные по столбцу
                    column_data = [row[column_index] for row in data]
                    
                    macroproperty1[field_id] = column_data
                    
                    self.logger.info(f"   ✅ field_id {field_id} ({field_name}): {len(column_data)} значений")
            
            self.stats['loaded_fields'] = len(macroproperty1)
            self.logger.info(f"🎯 MacroProperty1 создано с {len(macroproperty1)} полями")
            
            return macroproperty1
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания MacroProperty1: {e}")
            raise
    
    def save_macroproperty1_metadata(self, macroproperty1: Dict[int, List], 
                                   field_mapping: Dict[str, int], 
                                   field_types: Dict[int, str]) -> None:
        """Сохранение метаданных MacroProperty1"""
        self.logger.info("💾 Сохранение метаданных MacroProperty1...")
        
        try:
            metadata = {
                'creation_date': datetime.now().isoformat(),
                'version_date': self.version_date.isoformat() if self.version_date else None,
                'version_id': self.version_id,
                'total_records': self.stats['total_records'],
                'loaded_fields': self.stats['loaded_fields'],
                'field_mapping': field_mapping,
                'field_types': field_types,
                'field_statistics': {}
            }
            
            # Добавляем статистику по каждому полю
            for field_id, data in macroproperty1.items():
                field_name = next((name for name, fid in field_mapping.items() if fid == field_id), f"field_{field_id}")
                
                metadata['field_statistics'][field_id] = {
                    'field_name': field_name,
                    'data_type': field_types.get(field_id, 'unknown'),
                    'record_count': len(data),
                    'null_count': sum(1 for x in data if x is None),
                    'sample_values': data[:5] if data else []
                }
            
            # Сохраняем в файл
            os.makedirs('temp_data', exist_ok=True)
            metadata_file = 'temp_data/macroproperty1_metadata.json'
            
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)
            
            self.logger.info(f"✅ Метаданные сохранены: {metadata_file}")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения метаданных: {e}")
            raise
    
    def load_macroproperty1(self) -> Dict[int, List]:
        """Основной метод загрузки MacroProperty1"""
        self.logger.info("🚀 Начало загрузки MacroProperty1 (md_components)")
        self.stats['start_time'] = datetime.now()
        
        try:
            # 1. Получение field_id маппинга
            field_mapping = self.get_field_mapping()
            
            # 2. Загрузка данных md_components
            data = self.load_md_components_data()
            
            # 3. Анализ типов данных
            field_types = self.analyze_data_types(data, field_mapping)
            
            # 4. Создание MacroProperty1
            macroproperty1 = self.create_macroproperty1(data, field_mapping, field_types)
            
            # 5. Сохранение метаданных
            self.save_macroproperty1_metadata(macroproperty1, field_mapping, field_types)
            
            self.stats['end_time'] = datetime.now()
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            
            self.logger.info("✅ MacroProperty1 успешно загружено!")
            self.logger.info(f"📊 Статистика загрузки:")
            self.logger.info(f"   • Записей: {self.stats['total_records']}")
            self.logger.info(f"   • Полей: {self.stats['loaded_fields']}")
            self.logger.info(f"   • Время загрузки: {duration:.2f} сек")
            
            return macroproperty1
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки MacroProperty1: {e}")
            raise

def main():
    """Основная функция"""
    logger = setup_logging()
    
    try:
        # Проверяем наличие версионных параметров
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
        
        # Создаем загрузчик
        loader = MacroProperty1Loader(
            version_date=version_date,
            version_id=version_id
        )
        
        # Загружаем MacroProperty1
        macroproperty1 = loader.load_macroproperty1()
        
        logger.info("🎯 MacroProperty1 готово для использования в FLAME GPU!")
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 