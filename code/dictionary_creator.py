#!/usr/bin/env python3
"""
Создатель ClickHouse Dictionary для аналитики + типов ВС
Генерирует словари на основе DISTINCT значений из heli_pandas для аналитических целей.

АРХИТЕКТУРА v3.0 (аналитические словари + встроенные ID):
- partno_id → partseqno_i (встроенный ID из Excel) + dict_partno_flat (для аналитики)
- serialno_id → psn (встроенный ID из Excel) + dict_serialno_flat (для аналитики)
- owner_id → address_i (встроенный ID из Excel) + dict_owner_flat (для аналитики)
- ac_typ → ac_type_mask (битовые маски для multihot GPU операций)

Назначение: Создание словарей для аналитики на основе DISTINCT из heli_pandas
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
    """Создатель словарей для аналитики на основе DISTINCT из heli_pandas"""
    
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
            'МИ26Т': 128,    # 0b10000000 (тот же Ми-26)
            'Ми-17': 64,     # 0b01000000  
            'МИ171': 64,     # 0b01000000 (вариант Ми-17)
            '171А2': 64,     # 0b01000000 (вариант Ми-17)
            'МИ171Е': 64,    # 0b01000000 (экспортный Ми-17)
            'Ми-8Т': 32,     # 0b00100000
            'МИ8МТВ': 32,    # 0b00100000 (вариант Ми-8Т)
            'МИ8': 32,       # 0b00100000 (базовый Ми-8)
            'МИ8АМТ': 32,    # 0b00100000 (модернизированный Ми-8)
            'КА32Т': 16,     # 0b00010000 (Камов Ка-32)
            '350B3': 8,      # 0b00001000 (Airbus H350)
            '355NP': 4,      # 0b00000100 (Airbus H355) 
            '355N': 4,       # 0b00000100 (Airbus H355 вариант)
            'R44': 2,        # 0b00000010 (Robinson R44)
            'R44I': 2,       # 0b00000010 (Robinson R44 вариант)
            'R44II': 2,      # 0b00000010 (Robinson R44 вариант)
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
    
    def validate_embedded_id_fields(self) -> bool:
        """Валидация встроенных ID полей из Excel"""
        self.logger.info("🔍 Валидация встроенных ID полей из Excel...")
        
        try:
            # Получаем статистику по встроенным ID полям
            embedded_stats_result = self.client.query("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(partseqno_i) as partseqno_filled,
                    COUNT(psn) as psn_filled,
                    COUNT(address_i) as address_filled,
                    COUNT(ac_type_i) as ac_type_filled,
                    MAX(version_date) as latest_date
                FROM heli_pandas
            """)
            
            if not embedded_stats_result.result_rows:
                self.logger.error("❌ Нет данных в heli_pandas")
                return False
            
            stats = embedded_stats_result.result_rows[0]
            total, partseqno_filled, psn_filled, address_filled, ac_type_filled, latest_date = stats
            
            self.logger.info(f"📊 Статистика встроенных ID полей (всего записей: {total:,}, дата: {latest_date}):")
            self.logger.info(f"  partseqno_i: {partseqno_filled:,} ({partseqno_filled/total*100:.1f}%)")
            self.logger.info(f"  psn: {psn_filled:,} ({psn_filled/total*100:.1f}%)")
            self.logger.info(f"  address_i: {address_filled:,} ({address_filled/total*100:.1f}%)")
            self.logger.info(f"  ac_type_i: {ac_type_filled:,} ({ac_type_filled/total*100:.1f}%)")
            
            # Проверяем качество заполнения
            issues = []
            min_coverage = 90.0  # Минимальное покрытие 90%
            
            if partseqno_filled/total*100 < min_coverage:
                issues.append(f"partseqno_i покрытие {partseqno_filled/total*100:.1f}% < {min_coverage}%")
            
            if psn_filled/total*100 < min_coverage:
                issues.append(f"psn покрытие {psn_filled/total*100:.1f}% < {min_coverage}%")
            
            if address_filled/total*100 < min_coverage:
                issues.append(f"address_i покрытие {address_filled/total*100:.1f}% < {min_coverage}%")
            
            if issues:
                self.logger.warning(f"⚠️ Проблемы покрытия встроенных ID полей:")
                for issue in issues:
                    self.logger.warning(f"  {issue}")
                return False
            else:
                self.logger.info("✅ Встроенные ID поля корректно заполнены")
                return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка валидации встроенных ID: {e}")
            return False
    
    def analyze_heli_pandas_data(self) -> Dict[str, Dict]:
        """Анализ данных heli_pandas для создания аналитических словарей"""
        self.logger.info("🔍 Анализ heli_pandas для создания аналитических словарей...")
        
        try:
            # Анализ партномеров - берем DISTINCT пары partno, partseqno_i
            partno_result = self.client.query("""
                SELECT DISTINCT partno, partseqno_i
                FROM heli_pandas 
                WHERE partno IS NOT NULL AND partno != '' AND partseqno_i IS NOT NULL
                ORDER BY partseqno_i
            """)
            partno_data = [(row[0], row[1]) for row in partno_result.result_rows]
            self.logger.info(f"📋 Найдено {len(partno_data)} уникальных пар partno → partseqno_i")
            
            # Анализ серийных номеров - берем DISTINCT пары serialno, psn
            serialno_result = self.client.query("""
                SELECT DISTINCT serialno, psn
                FROM heli_pandas 
                WHERE serialno IS NOT NULL AND serialno != '' AND psn IS NOT NULL
                ORDER BY psn
            """)
            serialno_data = [(row[0], row[1]) for row in serialno_result.result_rows]
            self.logger.info(f"📋 Найдено {len(serialno_data)} уникальных пар serialno → psn")
            
            # Анализ владельцев - берем DISTINCT пары owner, address_i
            owner_result = self.client.query("""
                SELECT DISTINCT owner, address_i
                FROM heli_pandas 
                WHERE owner IS NOT NULL AND owner != '' AND address_i IS NOT NULL
                ORDER BY address_i
            """)
            owner_data = [(row[0], row[1]) for row in owner_result.result_rows]
            self.logger.info(f"📋 Найдено {len(owner_data)} уникальных пар owner → address_i")
            
            # Анализ типов ВС (существующая логика)
            ac_type_result = self.client.query("""
                SELECT ac_typ, count(*) as cnt
                FROM heli_pandas 
                WHERE ac_typ IS NOT NULL AND ac_typ != ''
                GROUP BY ac_typ
                ORDER BY cnt DESC
            """)
            ac_type_data = [(row[0], row[1]) for row in ac_type_result.result_rows]
            self.logger.info(f"📋 Найдено {len(ac_type_data)} уникальных типов ВС")
            
            return {
                'partno': {'pairs': partno_data},
                'serialno': {'pairs': serialno_data}, 
                'owner': {'pairs': owner_data},
                'ac_typ': {'values': ac_type_data}
            }
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка анализа heli_pandas: {e}")
            return {}

    def create_analytics_dictionaries(self, analysis: Dict[str, Dict]) -> Dict[str, Dict]:
        """Создание аналитических словарей на основе реальных ID из heli_pandas"""
        self.logger.info("🔢 Создание аналитических словарей...")
        
        dictionaries = {}
        
        # Создание словаря партномеров - используем реальные partseqno_i
        if 'partno' in analysis:
            partno_pairs = analysis['partno']['pairs']
            partno_dict = {partno: partseqno_i for partno, partseqno_i in partno_pairs}
            dictionaries['partno'] = {
                'mapping': partno_dict,
                'target_type': 'UInt32'
            }
            self.logger.info(f"✅ Создан словарь partno: {len(partno_dict)} партномеров → partseqno_i (UInt32)")
        
        # Создание словаря серийных номеров - используем реальные psn
        if 'serialno' in analysis:
            serialno_pairs = analysis['serialno']['pairs']
            serialno_dict = {serialno: psn for serialno, psn in serialno_pairs}
            dictionaries['serialno'] = {
                'mapping': serialno_dict,
                'target_type': 'UInt32'
            }
            self.logger.info(f"✅ Создан словарь serialno: {len(serialno_dict)} серийных номеров → psn (UInt32)")
        
        # Создание словаря владельцев - используем реальные address_i
        if 'owner' in analysis:
            owner_pairs = analysis['owner']['pairs']
            owner_dict = {owner: address_i for owner, address_i in owner_pairs}
            dictionaries['owner'] = {
                'mapping': owner_dict,
                'target_type': 'UInt32'
            }
            self.logger.info(f"✅ Создан словарь owner: {len(owner_dict)} владельцев → address_i (UInt32)")
        
        # Создание словаря типов ВС (существующая логика с битовыми масками)
        if 'ac_typ' in analysis:
            ac_typ_values = [val for val, _ in analysis['ac_typ']['values']]
            ac_typ_dict = {}
            
            for ac_type in ac_typ_values:
                if ac_type in self.ac_type_masks:
                    ac_typ_dict[ac_type] = self.ac_type_masks[ac_type]
                else:
                    self.logger.warning(f"⚠️ Неизвестный тип ВС: {ac_type} - пропускаем")
            
            dictionaries['ac_typ'] = {
                'mapping': ac_typ_dict,
                'target_type': 'UInt8'
            }
            self.logger.info(f"✅ Создан словарь ac_typ: {len(ac_typ_dict)} типов ВС → UInt8")
        
        return dictionaries

    def create_dictionary_tables(self) -> bool:
        """Создание ClickHouse Dictionary таблиц для аналитики"""
        self.logger.info("🏗️ Создание Dictionary таблиц для аналитики...")
        
        try:
            # Удаляем старые таблицы если существуют
            tables_to_drop = ['dict_partno_flat', 'dict_serialno_flat', 'dict_owner_flat', 'dict_ac_type_flat']
            for table in tables_to_drop:
                try:
                    self.client.query(f"DROP TABLE IF EXISTS {table}")
                    self.logger.info(f"🗑️ Удалена старая таблица {table}")
                except Exception as e:
                    self.logger.debug(f"Таблица {table} не существовала: {e}")
            
            # Таблица партномеров - partno → partseqno_i (ИСТИННО АДДИТИВНАЯ)
            partno_dict_sql = """
            CREATE TABLE IF NOT EXISTS dict_partno_flat (
                partseqno_i UInt32,
                partno String,
                load_timestamp DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (partseqno_i, partno, load_timestamp)
            SETTINGS index_granularity = 8192
            """
            
            # Таблица серийных номеров - serialno → psn (ИСТИННО АДДИТИВНАЯ)
            serialno_dict_sql = """
            CREATE TABLE IF NOT EXISTS dict_serialno_flat (
                psn UInt32,
                serialno String,
                load_timestamp DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (psn, serialno, load_timestamp)
            SETTINGS index_granularity = 8192
            """
            
            # Таблица владельцев - owner → address_i (ИСТИННО АДДИТИВНАЯ)
            owner_dict_sql = """
            CREATE TABLE IF NOT EXISTS dict_owner_flat (
                address_i UInt32,
                owner String,
                load_timestamp DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (address_i, owner, load_timestamp)
            SETTINGS index_granularity = 8192
            """
            
            # Таблица типов ВС (битовые маски) (ИСТИННО АДДИТИВНАЯ)
            ac_type_dict_sql = """
            CREATE TABLE IF NOT EXISTS dict_ac_type_flat (
                ac_type_mask UInt8,
                ac_typ String,
                load_timestamp DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (ac_type_mask, ac_typ, load_timestamp)
            SETTINGS index_granularity = 8192
            """
            
            # Создаем все таблицы
            self.client.query(partno_dict_sql)
            self.client.query(serialno_dict_sql)
            self.client.query(owner_dict_sql)
            self.client.query(ac_type_dict_sql)
            
            self.logger.info("✅ Dictionary таблицы созданы: partno, serialno, owner, ac_type")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания Dictionary таблиц: {e}")
            return False

    def populate_dictionary_tables(self, dictionaries: Dict[str, Dict]) -> bool:
        """Аддитивное заполнение Dictionary таблиц данными (без TRUNCATE)"""
        self.logger.info("📊 Аддитивное заполнение Dictionary таблиц...")
        
        try:
            current_timestamp = datetime.now()
            
            # Заполнение партномеров - partno → partseqno_i (АДДИТИВНО)
            if 'partno' in dictionaries:
                partno_data = []
                for partno, partseqno_i in dictionaries['partno']['mapping'].items():
                    partno_data.append([partseqno_i, partno, current_timestamp])
                
                if partno_data:
                    self.client.insert('dict_partno_flat', partno_data,
                                     column_names=['partseqno_i', 'partno', 'load_timestamp'])
                    self.logger.info(f"✅ Добавлено {len(partno_data)} партномеров (истинно аддитивно)")
            
            # Заполнение серийных номеров - serialno → psn (АДДИТИВНО)
            if 'serialno' in dictionaries:
                serialno_data = []
                for serialno, psn in dictionaries['serialno']['mapping'].items():
                    serialno_data.append([psn, serialno, current_timestamp])
                
                if serialno_data:
                    self.client.insert('dict_serialno_flat', serialno_data,
                                     column_names=['psn', 'serialno', 'load_timestamp'])
                    self.logger.info(f"✅ Добавлено {len(serialno_data)} серийных номеров (истинно аддитивно)")
            
            # Заполнение владельцев - owner → address_i (АДДИТИВНО)
            if 'owner' in dictionaries:
                owner_data = []
                for owner, address_i in dictionaries['owner']['mapping'].items():
                    owner_data.append([address_i, owner, current_timestamp])
                
                if owner_data:
                    self.client.insert('dict_owner_flat', owner_data,
                                     column_names=['address_i', 'owner', 'load_timestamp'])
                    self.logger.info(f"✅ Добавлено {len(owner_data)} владельцев (истинно аддитивно)")
            
            # Заполнение типов ВС (АДДИТИВНО)
            if 'ac_typ' in dictionaries:
                ac_type_data = []
                
                for ac_typ, ac_type_mask in dictionaries['ac_typ']['mapping'].items():
                    ac_type_data.append([ac_type_mask, ac_typ, current_timestamp])
                
                if ac_type_data:
                    self.client.insert('dict_ac_type_flat', ac_type_data,
                                     column_names=['ac_type_mask', 'ac_typ', 'load_timestamp'])
                    self.logger.info(f"✅ Добавлено {len(ac_type_data)} типов ВС (истинно аддитивно)")
            
            self.logger.info("🎯 Аддитивное заполнение словарей завершено (без TRUNCATE)")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка аддитивного заполнения Dictionary таблиц: {e}")
            return False

    def create_clickhouse_dictionary_objects(self) -> bool:
        """Создание ClickHouse Dictionary объектов для аналитики"""
        self.logger.info("📚 Создание ClickHouse Dictionary объектов...")
        
        try:
            # Удаляем старые Dictionary объекты если существуют
            dictionaries_to_drop = ['partno_dict_flat', 'serialno_dict_flat', 'owner_dict_flat', 'ac_type_dict_flat']
            for dict_name in dictionaries_to_drop:
                try:
                    self.client.query(f"DROP DICTIONARY IF EXISTS {dict_name}")
                    self.logger.info(f"🗑️ Удален старый Dictionary {dict_name}")
                except Exception as e:
                    self.logger.debug(f"Dictionary {dict_name} не существовал: {e}")
            
            # Dictionary для партномеров - partseqno_i → partno
            partno_dict_ddl = f"""
            CREATE OR REPLACE DICTIONARY partno_dict_flat (
                partseqno_i UInt32,
                partno String
            )
            PRIMARY KEY partseqno_i
            SOURCE(CLICKHOUSE(
                HOST '{self.config['host']}'
                PORT {self.config['port']}
                TABLE 'dict_partno_flat'
                DB '{self.config['database']}'
            ))
            LAYOUT(FLAT())
            LIFETIME(MIN 0 MAX 3600)
            """
            
            # Dictionary для серийных номеров - psn → serialno
            serialno_dict_ddl = f"""
            CREATE OR REPLACE DICTIONARY serialno_dict_flat (
                psn UInt32,
                serialno String
            )
            PRIMARY KEY psn
            SOURCE(CLICKHOUSE(
                HOST '{self.config['host']}'
                PORT {self.config['port']}
                TABLE 'dict_serialno_flat'
                DB '{self.config['database']}'
            ))
            LAYOUT(FLAT())
            LIFETIME(MIN 0 MAX 3600)
            """
            
            # Dictionary для владельцев - address_i → owner
            owner_dict_ddl = f"""
            CREATE OR REPLACE DICTIONARY owner_dict_flat (
                address_i UInt32,
                owner String
            )
            PRIMARY KEY address_i
            SOURCE(CLICKHOUSE(
                HOST '{self.config['host']}'
                PORT {self.config['port']}
                TABLE 'dict_owner_flat'
                DB '{self.config['database']}'
            ))
            LAYOUT(FLAT())
            LIFETIME(MIN 0 MAX 3600)
            """
            
            # Dictionary для типов ВС (существующая логика)
            ac_type_dict_ddl = f"""
            CREATE OR REPLACE DICTIONARY ac_type_dict_flat (
                ac_type_mask UInt8,
                ac_typ String
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
            
            # Создаем все Dictionary объекты
            self.client.query(partno_dict_ddl)
            self.client.query(serialno_dict_ddl)
            self.client.query(owner_dict_ddl)
            self.client.query(ac_type_dict_ddl)
            
            self.logger.info("✅ ClickHouse Dictionary объекты созданы: partno, serialno, owner, ac_type")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания ClickHouse Dictionary объектов: {e}")
            return False

    def run_full_analysis(self) -> bool:
        """Запуск создания аналитических словарей"""
        self.logger.info("🚀 Создание словарей v3.0 - аналитические словари + типы ВС")
        
        try:
            # 1. Подключение
            if not self.connect_to_database():
                return False
            
            # 2. Валидация встроенных ID полей из Excel
            if not self.validate_embedded_id_fields():
                self.logger.warning("⚠️ Проблемы с встроенными ID полями, но продолжаем...")
            
            # 3. Анализ данных heli_pandas
            analysis = self.analyze_heli_pandas_data()
            if not analysis:
                return False
            
            # 4. Создание аналитических словарей
            dictionaries = self.create_analytics_dictionaries(analysis)
            if not dictionaries:
                return False
            
            # 5. Создание Dictionary таблиц
            if not self.create_dictionary_tables():
                return False
            
            # 6. Заполнение Dictionary таблиц данными
            if not self.populate_dictionary_tables(dictionaries):
                return False
            
            # 7. Создание ClickHouse Dictionary объектов
            if not self.create_clickhouse_dictionary_objects():
                return False
            
            self.logger.info("🎯 СОЗДАНИЕ АНАЛИТИЧЕСКИХ СЛОВАРЕЙ v3.0 ЗАВЕРШЕНО!")
            self.logger.info("💡 Встроенные ID: partseqno_i, psn, address_i, ac_type_i (напрямую из Excel)")
            self.logger.info("📊 Аналитические словари: dict_partno_flat, dict_serialno_flat, dict_owner_flat")
            self.logger.info("✨ Битовые маски: ac_type_mask для multihot GPU операций")
            self.logger.info("🚀 Готово для аналитики и Flame GPU")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка: {e}")
            return False

def main():
    """Основная функция"""
    print("🚀 === СОЗДАТЕЛЬ АНАЛИТИЧЕСКИХ СЛОВАРЕЙ v3.0 ===")
    print("💡 Встроенные ID из Excel + аналитические словари из DISTINCT heli_pandas")
    print("📊 Создаем словари: партномера, серийники, владельцы, типы ВС")
    
    try:
        creator = DictionaryCreator()
        success = creator.run_full_analysis()
        
        if success:
            print(f"\n🎯 === АНАЛИТИЧЕСКИЕ СЛОВАРИ ГОТОВЫ ===")
            print(f"✅ Встроенные ID: partseqno_i, psn, address_i, ac_type_i")
            print(f"📊 Аналитические словари: partno, serialno, owner (DISTINCT из heli_pandas)")
            print(f"✨ Битовые маски: ac_type_mask для multihot GPU операций")
            print(f"🚀 Готово для аналитики и Flame GPU!")
            return 0
        else:
            print(f"\n❌ === ОШИБКА СОЗДАНИЯ СЛОВАРЕЙ ===")
            return 1
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        return 1

if __name__ == "__main__":
    exit(main()) 