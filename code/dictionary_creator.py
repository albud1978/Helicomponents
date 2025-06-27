#!/usr/bin/env python3
"""
Создатель ClickHouse Dictionary для преобразования текстовых полей в числовые ID
Анализирует таблицу heli_pandas (~6,677 агрегатов для Flame GPU) 
и создает ClickHouse Dictionary таблицы и объекты для Direct Join с результатами Flame GPU.

Архитектура ETL:
1. Анализ данных в heli_pandas
2. Создание маппингов (текст → числовой ID) 
3. Создание ClickHouse Dictionary таблиц (FLAT layout)
4. Заполнение Dictionary таблиц данными
5. Создание ClickHouse Dictionary объектов для O(1) lookup

Назначение: Обогащение результатов Flame GPU "человеческими" названиями в Superset BI
"""

import sys
import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Any
from datetime import datetime

# Добавляем путь к утилитам
sys.path.append(str(Path(__file__).parent / 'utils'))
from config_loader import load_clickhouse_config
import clickhouse_connect

class DictionaryCreator:
    """Создатель словарей для преобразования текстовых полей в числовые ID"""
    
    def __init__(self):
        """Инициализация создателя словарей"""
        self.logger = self._setup_logging()
        self.config = load_clickhouse_config()
        
        # Исправляем конфигурацию для работы с ClickHouse
        self.config['port'] = 8123  # HTTP порт
        if 'settings' in self.config:
            self.config['settings'] = {k: v for k, v in self.config['settings'].items() if k != 'use_numpy'}
        
        self.client = None
        
        # Битовые маски для типов ВС (из OLAP MultiBOM)
        self.ac_type_masks = {
            'Ми-26': 128,    # 0b10000000
            'Ми-17': 64,     # 0b01000000  
            'Ми-8Т': 32,     # 0b00100000
        }
        
        # ПРИМЕЧАНИЕ: поля location и condition обрабатываются отдельно по своей логике
    
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
    
    def analyze_source_data(self) -> Dict[str, Dict]:
        """Анализ исходных данных в heli_pandas для создания словарей"""
        self.logger.info("🔍 Анализ данных в heli_pandas...")
        
        # Проверяем таблицу
        count_result = self.client.query("""
            SELECT count(*) as total_records, max(version_date) as latest_date
            FROM heli_pandas
        """)
        
        total_records, latest_date = count_result.result_rows[0]
        self.logger.info(f"📊 Анализируем {total_records:,} записей от {latest_date}")
        
        # Основные поля для словарей (исключаем location и condition - они обрабатываются отдельно)
        text_fields = ['partno', 'serialno', 'ac_typ', 'owner']
        analysis = {}
        
        for field in text_fields:
            try:
                # Получаем все уникальные значения с частотой
                values_result = self.client.query(f"""
                    SELECT {field}, count(*) as cnt
                    FROM heli_pandas 
                    WHERE {field} IS NOT NULL AND {field} != '' AND version_date = '{latest_date}'
                    GROUP BY {field}
                    ORDER BY cnt DESC
                """)
                
                values_data = [(row[0], row[1]) for row in values_result.result_rows]
                analysis[field] = {'values': values_data}
                
                self.logger.info(f"  📋 {field}: {len(values_data)} уникальных значений")
                
            except Exception as e:
                self.logger.error(f"❌ Ошибка анализа поля {field}: {e}")
        
        return analysis
    
    def create_dictionaries(self, analysis: Dict[str, Dict]) -> Dict[str, Dict]:
        """Создание словарей маппинга на основе анализа данных"""
        self.logger.info("🔢 Создание словарей маппинга...")
        
        dictionaries = {}
        
        # 1. PARTNO DICTIONARY
        if 'partno' in analysis:
            partno_values = analysis['partno']['values']
            partno_dict = {partno: idx for idx, (partno, _) in enumerate(partno_values, 1)}
            
            dictionaries['partno'] = {
                'mapping': partno_dict,
                'target_type': 'UInt16',
                'description': f"Партномера: {len(partno_dict)} значений"
            }
            self.logger.info(f"  ✅ partno: {len(partno_dict)} значений → UInt16")
        
        # 2. AC_TYP DICTIONARY (битовые маски)
        if 'ac_typ' in analysis:
            ac_typ_values = [val for val, _ in analysis['ac_typ']['values']]
            ac_typ_dict = {}
            
            for ac_type in ac_typ_values:
                if ac_type in self.ac_type_masks:
                    ac_typ_dict[ac_type] = self.ac_type_masks[ac_type]
            
            dictionaries['ac_typ'] = {
                'mapping': ac_typ_dict,
                'target_type': 'UInt8',
                'description': f"Типы ВС: битовые маски"
            }
            self.logger.info(f"  ✅ ac_typ: {len(ac_typ_dict)} типов ВС → UInt8")
        
        # 3. OWNER DICTIONARY
        if 'owner' in analysis:
            owner_values = analysis['owner']['values']
            owner_dict = {owner: idx for idx, (owner, _) in enumerate(owner_values, 1)}
            
            dictionaries['owner'] = {
                'mapping': owner_dict,
                'target_type': 'UInt8',
                'description': f"Владельцы: {len(owner_dict)} значений"
            }
            self.logger.info(f"  ✅ owner: {len(owner_dict)} владельцев → UInt8")
        
        # 4. SERIALNO DICTIONARY
        if 'serialno' in analysis:
            serialno_values = analysis['serialno']['values']
            serialno_dict = {serialno: idx for idx, (serialno, _) in enumerate(serialno_values, 1)}
            
            dictionaries['serialno'] = {
                'mapping': serialno_dict,
                'target_type': 'UInt16',
                'description': f"Серийные номера: {len(serialno_dict)} значений"
            }
            self.logger.info(f"  ✅ serialno: {len(serialno_dict)} серийных номеров → UInt16")
        
        return dictionaries
    
    def create_dictionary_tables(self, dictionaries: Dict[str, Dict]) -> bool:
        """Создание ClickHouse Dictionary таблиц согласно ETL архитектуре"""
        self.logger.info("🏗️ Создание ClickHouse Dictionary таблиц для Direct Join...")
        
        try:
            # 1. Таблица партномеров (FLAT layout)
            partno_dict_sql = """
            CREATE TABLE IF NOT EXISTS dict_partno_flat (
                partno_id UInt16,          -- FLAT layout key (плотные значения 1,2,3...)
                partno String
            ) ENGINE = Memory
            """
            
            # 2. Таблица типов ВС (битовые маски)
            ac_type_dict_sql = """
            CREATE TABLE IF NOT EXISTS dict_ac_type_flat (
                ac_type_mask UInt8,        -- FLAT layout key (32, 64, 96, 128)
                ac_typ String,
                description String
            ) ENGINE = Memory
            """
            
            # 3. Таблица владельцев
            owner_dict_sql = """
            CREATE TABLE IF NOT EXISTS dict_owner_flat (
                owner_id UInt8,            -- FLAT layout key (1,2,3...)
                owner String
            ) ENGINE = Memory
            """
            
            # 4. Таблица серийных номеров
            serialno_dict_sql = """
            CREATE TABLE IF NOT EXISTS dict_serialno_flat (
                serialno_id UInt16,        -- FLAT layout key (1,2,3...)
                serialno String
            ) ENGINE = Memory
            """
            
            # Создание таблиц
            self.client.query(partno_dict_sql)
            self.client.query(ac_type_dict_sql)
            self.client.query(owner_dict_sql)
            self.client.query(serialno_dict_sql)
            
            self.logger.info("✅ Dictionary таблицы созданы для FLAT layout")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания Dictionary таблиц: {e}")
            return False
    
    def populate_dictionary_tables(self, dictionaries: Dict[str, Dict]) -> bool:
        """Заполнение Dictionary таблиц данными"""
        self.logger.info("📊 Заполнение Dictionary таблиц данными...")
        
        try:
            # 1. Заполнение партномеров
            if 'partno' in dictionaries:
                partno_data = []
                for partno, partno_id in dictionaries['partno']['mapping'].items():
                    partno_data.append([partno_id, partno])
                
                if partno_data:
                    # Очищаем таблицу
                    self.client.query("TRUNCATE TABLE dict_partno_flat")
                    # Вставляем данные
                    self.client.insert('dict_partno_flat', partno_data,
                                     column_names=['partno_id', 'partno'])
                    self.logger.info(f"✅ Загружено {len(partno_data)} партномеров в Dictionary")
            
            # 2. Заполнение типов ВС
            if 'ac_typ' in dictionaries:
                ac_type_data = []
                ac_type_descriptions = {
                    'Ми-26': 'Тяжелый многоцелевой вертолет',
                    'Ми-17': 'Средний многоцелевой вертолет', 
                    'Ми-8Т': 'Средний транспортный вертолет'
                }
                
                for ac_typ, ac_type_mask in dictionaries['ac_typ']['mapping'].items():
                    description = ac_type_descriptions.get(ac_typ, 'Неизвестный тип')
                    ac_type_data.append([ac_type_mask, ac_typ, description])
                
                if ac_type_data:
                    self.client.query("TRUNCATE TABLE dict_ac_type_flat")
                    self.client.insert('dict_ac_type_flat', ac_type_data,
                                     column_names=['ac_type_mask', 'ac_typ', 'description'])
                    self.logger.info(f"✅ Загружено {len(ac_type_data)} типов ВС в Dictionary")
            
            # 3. Заполнение владельцев
            if 'owner' in dictionaries:
                owner_data = []
                for owner, owner_id in dictionaries['owner']['mapping'].items():
                    owner_data.append([owner_id, owner])
                
                if owner_data:
                    self.client.query("TRUNCATE TABLE dict_owner_flat")
                    self.client.insert('dict_owner_flat', owner_data,
                                     column_names=['owner_id', 'owner'])
                    self.logger.info(f"✅ Загружено {len(owner_data)} владельцев в Dictionary")
            
            # 4. Заполнение серийных номеров
            if 'serialno' in dictionaries:
                serialno_data = []
                for serialno, serialno_id in dictionaries['serialno']['mapping'].items():
                    serialno_data.append([serialno_id, serialno])
                
                if serialno_data:
                    self.client.query("TRUNCATE TABLE dict_serialno_flat")
                    self.client.insert('dict_serialno_flat', serialno_data,
                                     column_names=['serialno_id', 'serialno'])
                    self.logger.info(f"✅ Загружено {len(serialno_data)} серийных номеров в Dictionary")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка заполнения Dictionary таблиц: {e}")
            return False
    
    def create_clickhouse_dictionaries(self) -> bool:
        """Создание ClickHouse Dictionary объектов с FLAT layout для Direct Join"""
        self.logger.info("📚 Создание ClickHouse Dictionary объектов с FLAT layout...")
        
        try:
            # 1. Dictionary для партномеров (FLAT layout)
            partno_dict_ddl = f"""
            CREATE OR REPLACE DICTIONARY partno_dict_flat (
                partno_id UInt16,
                partno String
            )
            PRIMARY KEY partno_id
            SOURCE(CLICKHOUSE(
                HOST '{self.config['host']}'
                PORT {self.config['port']}
                TABLE 'dict_partno_flat'
                DB '{self.config['database']}'
            ))
            LAYOUT(FLAT(INITIAL_ARRAY_SIZE 5000 MAX_ARRAY_SIZE 5000))
            LIFETIME(MIN 0 MAX 3600)
            """
            
            # 2. Dictionary для типов ВС (FLAT layout)
            ac_type_dict_ddl = f"""
            CREATE OR REPLACE DICTIONARY ac_type_dict_flat (
                ac_type_mask UInt8,
                ac_typ String,
                description String
            )
            PRIMARY KEY ac_type_mask
            SOURCE(CLICKHOUSE(
                HOST '{self.config['host']}'
                PORT {self.config['port']}
                TABLE 'dict_ac_type_flat'
                DB '{self.config['database']}'
            ))
            LAYOUT(FLAT(INITIAL_ARRAY_SIZE 256 MAX_ARRAY_SIZE 256))
            LIFETIME(MIN 0 MAX 3600)
            """
            
            # 3. Dictionary для владельцев (FLAT layout)
            owner_dict_ddl = f"""
            CREATE OR REPLACE DICTIONARY owner_dict_flat (
                owner_id UInt8,
                owner String
            )
            PRIMARY KEY owner_id
            SOURCE(CLICKHOUSE(
                HOST '{self.config['host']}'
                PORT {self.config['port']}
                TABLE 'dict_owner_flat'
                DB '{self.config['database']}'
            ))
            LAYOUT(FLAT(INITIAL_ARRAY_SIZE 256 MAX_ARRAY_SIZE 256))
            LIFETIME(MIN 0 MAX 3600)
            """
            
            # 4. Dictionary для серийных номеров (FLAT layout)
            serialno_dict_ddl = f"""
            CREATE OR REPLACE DICTIONARY serialno_dict_flat (
                serialno_id UInt16,
                serialno String
            )
            PRIMARY KEY serialno_id
            SOURCE(CLICKHOUSE(
                HOST '{self.config['host']}'
                PORT {self.config['port']}
                TABLE 'dict_serialno_flat'
                DB '{self.config['database']}'
            ))
            LAYOUT(FLAT(INITIAL_ARRAY_SIZE 10000 MAX_ARRAY_SIZE 10000))
            LIFETIME(MIN 0 MAX 3600)
            """
            
            # Создание Dictionary объектов
            self.client.query(partno_dict_ddl)
            self.client.query(ac_type_dict_ddl)
            self.client.query(owner_dict_ddl)
            self.client.query(serialno_dict_ddl)
            
            self.logger.info("✅ ClickHouse Dictionary объекты созданы с FLAT layout")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания ClickHouse Dictionary объектов: {e}")
            return False
    
    def run_full_analysis(self) -> bool:
        """Запуск полного анализа и создания словарей"""
        self.logger.info("🚀 Запуск создания словарей для Flame GPU")
        
        try:
            # 1. Подключение
            if not self.connect_to_database():
                return False
            
            # 2. Анализ данных
            analysis = self.analyze_source_data()
            
            # 3. Создание словарей
            dictionaries = self.create_dictionaries(analysis)
            
            # 4. Создание ClickHouse Dictionary таблиц
            if not self.create_dictionary_tables(dictionaries):
                return False
            
            # 5. Заполнение Dictionary таблиц данными
            if not self.populate_dictionary_tables(dictionaries):
                return False
            
            # 6. Создание ClickHouse Dictionary объектов
            if not self.create_clickhouse_dictionaries():
                return False
            
            self.logger.info("🎯 СОЗДАНИЕ CLICKHOUSE DICTIONARY ЗАВЕРШЕНО!")
            self.logger.info(f"📊 Создано {len(dictionaries)} Dictionary объектов для Direct Join")
            self.logger.info("🚀 Готово для интеграции с Flame GPU → ClickHouse → Superset BI")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка: {e}")
            return False

def main():
    """Основная функция"""
    creator = DictionaryCreator()
    return 0 if creator.run_full_analysis() else 1

if __name__ == "__main__":
    exit(main()) 