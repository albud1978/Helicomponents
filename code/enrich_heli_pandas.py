#!/usr/bin/env python3
"""
Обогащение таблицы heli_pandas числовыми полями для Flame GPU
Добавляет поля _id/_mask соответствующие созданным ClickHouse Dictionary
"""

import sys
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Any

# Добавляем путь к утилитам
sys.path.append(str(Path(__file__).parent / 'utils'))
from config_loader import load_clickhouse_config
import clickhouse_connect

class HeliPandasEnricher:
    """Обогащение heli_pandas числовыми полями для GPU"""
    
    def __init__(self):
        """Инициализация обогатителя"""
        self.logger = self._setup_logging()
        self.config = load_clickhouse_config()
        
        # Исправляем конфигурацию для работы с ClickHouse
        self.config['port'] = 8123  # HTTP порт
        if 'settings' in self.config:
            self.config['settings'] = {k: v for k, v in self.config['settings'].items() if k != 'use_numpy'}
        
        self.client = None
        
        # Битовые маски для типов ВС (из созданных словарей)
        self.ac_type_masks = {
            'Ми-26': 128,    # 0b10000000
            'Ми-17': 64,     # 0b01000000  
            'Ми-8Т': 32,     # 0b00100000
        }
    
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
    
    def add_numeric_columns(self) -> bool:
        """Добавление числовых колонок в heli_pandas"""
        self.logger.info("🔧 Добавление числовых колонок в heli_pandas...")
        
        try:
            # Добавляем колонки если их нет
            alter_queries = [
                "ALTER TABLE heli_pandas ADD COLUMN IF NOT EXISTS partno_id UInt16 DEFAULT 0",
                "ALTER TABLE heli_pandas ADD COLUMN IF NOT EXISTS serialno_id UInt16 DEFAULT 0", 
                "ALTER TABLE heli_pandas ADD COLUMN IF NOT EXISTS ac_type_mask UInt8 DEFAULT 0",
                "ALTER TABLE heli_pandas ADD COLUMN IF NOT EXISTS owner_id UInt8 DEFAULT 0"
            ]
            
            for query in alter_queries:
                self.client.query(query)
            
            self.logger.info("✅ Числовые колонки добавлены в heli_pandas")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка добавления колонок: {e}")
            return False
    
    def load_dictionary_mappings(self) -> Dict[str, Dict]:
        """Загрузка маппингов из Dictionary таблиц"""
        self.logger.info("📖 Загрузка маппингов из Dictionary таблиц...")
        
        mappings = {}
        
        try:
            # 1. Загрузка партномеров
            partno_result = self.client.query("SELECT partno_id, partno FROM dict_partno_flat")
            mappings['partno'] = {row[1]: row[0] for row in partno_result.result_rows}
            
            # 2. Загрузка серийных номеров  
            serialno_result = self.client.query("SELECT serialno_id, serialno FROM dict_serialno_flat")
            mappings['serialno'] = {row[1]: row[0] for row in serialno_result.result_rows}
            
            # 3. Загрузка типов ВС
            ac_type_result = self.client.query("SELECT ac_type_mask, ac_typ FROM dict_ac_type_flat")
            mappings['ac_typ'] = {row[1]: row[0] for row in ac_type_result.result_rows}
            
            # 4. Загрузка владельцев
            owner_result = self.client.query("SELECT owner_id, owner FROM dict_owner_flat")
            mappings['owner'] = {row[1]: row[0] for row in owner_result.result_rows}
            
            self.logger.info(f"✅ Загружено маппингов:")
            self.logger.info(f"  📋 partno: {len(mappings['partno'])} значений")
            self.logger.info(f"  📋 serialno: {len(mappings['serialno'])} значений") 
            self.logger.info(f"  📋 ac_typ: {len(mappings['ac_typ'])} значений")
            self.logger.info(f"  📋 owner: {len(mappings['owner'])} значений")
            
            return mappings
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки маппингов: {e}")
            return {}
    
    def enrich_partno_ids(self, mappings: Dict[str, Dict]) -> bool:
        """Обогащение партномеров"""
        self.logger.info("🔢 Обогащение partno_id...")
        
        try:
            for partno, partno_id in mappings['partno'].items():
                update_query = f"""
                ALTER TABLE heli_pandas 
                UPDATE partno_id = {partno_id}
                WHERE partno = '{partno}'
                """
                self.client.query(update_query)
            
            # Проверяем результат
            result = self.client.query("SELECT COUNT(*) FROM heli_pandas WHERE partno_id > 0")
            enriched_count = result.result_rows[0][0]
            self.logger.info(f"✅ Обогащено {enriched_count} записей партномерами")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обогащения partno: {e}")
            return False
    
    def enrich_serialno_ids(self, mappings: Dict[str, Dict]) -> bool:
        """Обогащение серийных номеров"""
        self.logger.info("🔢 Обогащение serialno_id...")
        
        try:
            # Обновляем батчами для производительности
            batch_size = 1000
            serialno_items = list(mappings['serialno'].items())
            
            for i in range(0, len(serialno_items), batch_size):
                batch = serialno_items[i:i + batch_size]
                
                # Строим CASE WHEN для батча
                case_when_parts = []
                for serialno, serialno_id in batch:
                    case_when_parts.append(f"WHEN serialno = '{serialno}' THEN {serialno_id}")
                
                case_when_sql = " ".join(case_when_parts)
                
                update_query = f"""
                ALTER TABLE heli_pandas 
                UPDATE serialno_id = CASE {case_when_sql} ELSE 0 END
                WHERE serialno IN ({','.join([f"'{s}'" for s, _ in batch])})
                """
                
                self.client.query(update_query)
                self.logger.info(f"  📊 Обработано {min(i + batch_size, len(serialno_items))}/{len(serialno_items)} серийных номеров")
            
            # Проверяем результат
            result = self.client.query("SELECT COUNT(*) FROM heli_pandas WHERE serialno_id > 0")
            enriched_count = result.result_rows[0][0]
            self.logger.info(f"✅ Обогащено {enriched_count} записей серийными номерами")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обогащения serialno: {e}")
            return False
    
    def enrich_ac_type_masks(self, mappings: Dict[str, Dict]) -> bool:
        """Обогащение типов ВС битовыми масками"""
        self.logger.info("🔢 Обогащение ac_type_mask...")
        
        try:
            for ac_typ, ac_type_mask in mappings['ac_typ'].items():
                update_query = f"""
                ALTER TABLE heli_pandas 
                UPDATE ac_type_mask = {ac_type_mask}
                WHERE ac_typ = '{ac_typ}'
                """
                self.client.query(update_query)
            
            # Проверяем результат
            result = self.client.query("SELECT COUNT(*) FROM heli_pandas WHERE ac_type_mask > 0")
            enriched_count = result.result_rows[0][0]
            self.logger.info(f"✅ Обогащено {enriched_count} записей типами ВС")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обогащения ac_typ: {e}")
            return False
    
    def enrich_owner_ids(self, mappings: Dict[str, Dict]) -> bool:
        """Обогащение владельцев"""
        self.logger.info("🔢 Обогащение owner_id...")
        
        try:
            for owner, owner_id in mappings['owner'].items():
                update_query = f"""
                ALTER TABLE heli_pandas 
                UPDATE owner_id = {owner_id}
                WHERE owner = '{owner}'
                """
                self.client.query(update_query)
            
            # Проверяем результат
            result = self.client.query("SELECT COUNT(*) FROM heli_pandas WHERE owner_id > 0")
            enriched_count = result.result_rows[0][0]
            self.logger.info(f"✅ Обогащено {enriched_count} записей владельцами")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обогащения owner: {e}")
            return False
    
    def verify_enrichment(self) -> bool:
        """Проверка качества обогащения"""
        self.logger.info("🔍 Проверка качества обогащения...")
        
        try:
            # Общая статистика
            total_result = self.client.query("SELECT COUNT(*) FROM heli_pandas")
            total_count = total_result.result_rows[0][0]
            
            # Статистика по полям
            fields_stats = {}
            fields = ['partno_id', 'serialno_id', 'ac_type_mask', 'owner_id']
            
            for field in fields:
                result = self.client.query(f"SELECT COUNT(*) FROM heli_pandas WHERE {field} > 0")
                enriched_count = result.result_rows[0][0]
                coverage = (enriched_count / total_count) * 100
                fields_stats[field] = {'count': enriched_count, 'coverage': coverage}
            
            self.logger.info(f"📊 Статистика обогащения (всего записей: {total_count}):")
            for field, stats in fields_stats.items():
                self.logger.info(f"  {field}: {stats['count']} записей ({stats['coverage']:.1f}%)")
            
            # Проверяем примеры
            sample_result = self.client.query("""
                SELECT partno, partno_id, serialno, serialno_id, ac_typ, ac_type_mask, owner, owner_id 
                FROM heli_pandas 
                WHERE partno_id > 0 AND serialno_id > 0 
                LIMIT 3
            """)
            
            self.logger.info("📋 Примеры обогащенных записей:")
            for row in sample_result.result_rows:
                self.logger.info(f"  partno: '{row[0]}' → {row[1]}, serialno: '{row[2]}' → {row[3]}")
                self.logger.info(f"  ac_typ: '{row[4]}' → {row[5]}, owner: '{row[6]}' → {row[7]}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки: {e}")
            return False
    
    def run_enrichment(self) -> bool:
        """Запуск полного обогащения"""
        self.logger.info("🚀 Запуск обогащения heli_pandas числовыми полями")
        
        try:
            # 1. Подключение
            if not self.connect_to_database():
                return False
            
            # 2. Добавление колонок
            if not self.add_numeric_columns():
                return False
            
            # 3. Загрузка маппингов
            mappings = self.load_dictionary_mappings()
            if not mappings:
                return False
            
            # 4. Обогащение по полям
            if not self.enrich_partno_ids(mappings):
                return False
            
            if not self.enrich_serialno_ids(mappings):
                return False
                
            if not self.enrich_ac_type_masks(mappings):
                return False
                
            if not self.enrich_owner_ids(mappings):
                return False
            
            # 5. Проверка качества
            if not self.verify_enrichment():
                return False
            
            self.logger.info("🎯 ОБОГАЩЕНИЕ HELI_PANDAS ЗАВЕРШЕНО!")
            self.logger.info("🚀 Готово для загрузки в Flame GPU с числовыми полями")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обогащения: {e}")
            return False

def main():
    """Основная функция"""
    enricher = HeliPandasEnricher()
    return 0 if enricher.run_enrichment() else 1

if __name__ == "__main__":
    exit(main()) 