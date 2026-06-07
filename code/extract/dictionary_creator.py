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

# Добавляем пути к utils и общему коду
code_root = Path(__file__).resolve().parents[1]
sys.path.append(str(code_root / 'utils'))
sys.path.append(str(code_root))
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
        
        # Версионность (устанавливается в main)
        self.version_date = None
        self.version_id = None
        
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
            self.logger.error(f"❌ Ошибка подключения к ClickHouse: {e}")
            return False
    
    def get_version_from_heli_pandas(self) -> Tuple[str, int]:
        """Получает актуальные версионные параметры из таблицы heli_pandas"""
        try:
            self.logger.info("📅 Получение версионных параметров из heli_pandas...")
            
            # Проверяем существование таблицы heli_pandas
            table_exists = self.client.query("EXISTS TABLE heli_pandas").result_rows[0][0]
            if not table_exists:
                self.logger.error("❌ Таблица heli_pandas не существует!")
                self.logger.error("💡 Словари создаются ПОСЛЕ загрузки данных в heli_pandas")
                self.logger.error("🔄 Запустите сначала dual_loader.py или полный ETL цикл")
                return None, None
            
            # Получаем актуальные версионные параметры
            version_query = """
                SELECT 
                    MAX(version_date) as latest_version_date,
                    MAX(version_id) as latest_version_id
                FROM heli_pandas 
                WHERE version_date = (SELECT MAX(version_date) FROM heli_pandas)
            """
            
            version_result = self.client.query(version_query)
            if not version_result.result_rows:
                self.logger.error("❌ Нет данных в heli_pandas")
                return None, None
            
            latest_version_date, latest_version_id = version_result.result_rows[0]
            
            self.logger.info(f"✅ Версионные параметры из heli_pandas:")
            self.logger.info(f"   version_date: {latest_version_date}")
            self.logger.info(f"   version_id: {latest_version_id}")
            
            return latest_version_date, latest_version_id
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения версионных параметров: {e}")
            return None, None
    
    def validate_embedded_id_fields(self) -> bool:
        """Валидация встроенных ID полей из Excel"""
        self.logger.info("🔍 Валидация встроенных ID полей из Excel...")
        
        try:
            # Сначала проверяем существование таблицы heli_pandas
            table_exists = self.client.query("EXISTS TABLE heli_pandas").result_rows[0][0]
            if not table_exists:
                self.logger.error("❌ Таблица heli_pandas не существует!")
                self.logger.error("💡 Словари создаются ПОСЛЕ загрузки данных в heli_pandas")
                self.logger.error("🔄 Запустите сначала dual_loader.py или полный ETL цикл")
                return False
            
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
            
            # Анализ серийных номеров - берем DISTINCT пары (partno, serialno), psn
            serialno_result = self.client.query("""
                SELECT DISTINCT partno, serialno, psn
                FROM heli_pandas 
                WHERE partno IS NOT NULL AND partno != '' 
                  AND serialno IS NOT NULL AND serialno != '' 
                  AND psn IS NOT NULL
                ORDER BY psn
            """)
            serialno_data = [(row[0], row[1], row[2]) for row in serialno_result.result_rows]
            self.logger.info(f"📋 Найдено {len(serialno_data)} уникальных троек (partno, serialno) → psn")
            
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
        
        # Создание словаря серийных номеров - используем пары (partno, serialno) → psn
        if 'serialno' in analysis:
            serialno_pairs = analysis['serialno']['pairs']
            serialno_dict = {(partno, serialno): psn for partno, serialno, psn in serialno_pairs}
            dictionaries['serialno'] = {
                'mapping': serialno_dict,
                'target_type': 'UInt32'
            }
            self.logger.info(f"✅ Создан словарь serialno: {len(serialno_dict)} пар (partno, serialno) → psn (UInt32)")
        
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
            
            # Таблица партномеров - partno → partseqno_i (АДДИТИВНАЯ + ВЕРСИОННАЯ)
            partno_dict_sql = """
            CREATE TABLE IF NOT EXISTS dict_partno_flat (
                partseqno_i UInt32,
                partno String,
                version_date Date DEFAULT today(),
                version_id UInt8 DEFAULT 1,
                load_timestamp DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (partseqno_i, partno, version_date, version_id, load_timestamp)
            PARTITION BY toYYYYMM(version_date)
            SETTINGS index_granularity = 8192
            """
            
            # Таблица серийных номеров - (partno, serialno) → psn (АДДИТИВНАЯ + ВЕРСИОННАЯ)
            serialno_dict_sql = """
            CREATE TABLE IF NOT EXISTS dict_serialno_flat (
                psn UInt32,
                partno String,
                serialno String,
                version_date Date DEFAULT today(),
                version_id UInt8 DEFAULT 1,
                load_timestamp DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (psn, partno, serialno, version_date, version_id, load_timestamp)
            PARTITION BY toYYYYMM(version_date)
            SETTINGS index_granularity = 8192
            """
            
            # Таблица владельцев - owner → address_i (АДДИТИВНАЯ + ВЕРСИОННАЯ)
            owner_dict_sql = """
            CREATE TABLE IF NOT EXISTS dict_owner_flat (
                address_i UInt32,
                owner String,
                version_date Date DEFAULT today(),
                version_id UInt8 DEFAULT 1,
                load_timestamp DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (address_i, owner, version_date, version_id, load_timestamp)
            PARTITION BY toYYYYMM(version_date)
            SETTINGS index_granularity = 8192
            """
            
            # Таблица типов ВС (битовые маски) (АДДИТИВНАЯ + ВЕРСИОННАЯ)
            ac_type_dict_sql = """
            CREATE TABLE IF NOT EXISTS dict_ac_type_flat (
                ac_type_mask UInt8,
                ac_typ String,
                version_date Date DEFAULT today(),
                version_id UInt8 DEFAULT 1,
                load_timestamp DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (ac_type_mask, ac_typ, version_date, version_id, load_timestamp)
            PARTITION BY toYYYYMM(version_date)
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
        """Заполнение Dictionary таблиц данными (АДДИТИВНО + ВЕРСИОННО)"""
        try:
            self.logger.info("📊 Аддитивное заполнение Dictionary таблиц...")
            
            # Получаем актуальные версионные параметры из heli_pandas
            version_date, version_id = self.get_version_from_heli_pandas()
            if version_date is None or version_id is None:
                self.logger.error("❌ Не удалось получить версионные параметры из heli_pandas")
                return False
            
            # Устанавливаем версионные параметры для использования в остальной логике
            self.version_date = version_date
            self.version_id = version_id
            
            current_timestamp = datetime.now()
            
            # Заполнение партномеров - partno → partseqno_i (АДДИТИВНО + ВЕРСИОННО)
            if 'partno' in dictionaries:
                partno_data = []
                for partno, partseqno_i in dictionaries['partno']['mapping'].items():
                    partno_data.append([partseqno_i, partno, self.version_date, self.version_id, current_timestamp])
                
                if partno_data:
                    self.client.insert('dict_partno_flat', partno_data,
                                     column_names=['partseqno_i', 'partno', 'version_date', 'version_id', 'load_timestamp'])
                    self.logger.info(f"✅ Добавлено {len(partno_data)} партномеров (аддитивно + версионно)")
            
            # Заполнение серийных номеров - (partno, serialno) → psn (АДДИТИВНО + ВЕРСИОННО)
            if 'serialno' in dictionaries:
                serialno_data = []
                for (partno, serialno), psn in dictionaries['serialno']['mapping'].items():
                    serialno_data.append([psn, partno, serialno, self.version_date, self.version_id, current_timestamp])
                
                if serialno_data:
                    self.client.insert('dict_serialno_flat', serialno_data,
                                     column_names=['psn', 'partno', 'serialno', 'version_date', 'version_id', 'load_timestamp'])
                    self.logger.info(f"✅ Добавлено {len(serialno_data)} пар (partno, serialno) (аддитивно + версионно)")
            
            # Заполнение владельцев - owner → address_i (АДДИТИВНО + ВЕРСИОННО)
            if 'owner' in dictionaries:
                owner_data = []
                for owner, address_i in dictionaries['owner']['mapping'].items():
                    owner_data.append([address_i, owner, self.version_date, self.version_id, current_timestamp])
                
                if owner_data:
                    self.client.insert('dict_owner_flat', owner_data,
                                     column_names=['address_i', 'owner', 'version_date', 'version_id', 'load_timestamp'])
                    self.logger.info(f"✅ Добавлено {len(owner_data)} владельцев (аддитивно + версионно)")
            
            # Заполнение типов ВС (АДДИТИВНО + ВЕРСИОННО)
            if 'ac_typ' in dictionaries:
                ac_type_data = []
                
                for ac_typ, ac_type_mask in dictionaries['ac_typ']['mapping'].items():
                    ac_type_data.append([ac_type_mask, ac_typ, self.version_date, self.version_id, current_timestamp])
                
                if ac_type_data:
                    self.client.insert('dict_ac_type_flat', ac_type_data,
                                     column_names=['ac_type_mask', 'ac_typ', 'version_date', 'version_id', 'load_timestamp'])
                    self.logger.info(f"✅ Добавлено {len(ac_type_data)} типов ВС (аддитивно + версионно)")
            
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
            
            # Dictionary для серийных номеров - psn → (partno, serialno)
            serialno_dict_ddl = f"""
            CREATE OR REPLACE DICTIONARY serialno_dict_flat (
                psn UInt32,
                partno String,
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

    def create_status_dictionary(self) -> bool:
        """Создание словаря статусов dict_status_flat (НЕ аддитивный)"""
        self.logger.info("📋 Создание словаря статусов...")
        
        try:
            # Импортируем словарь статусов из процессора
            from extract.overhaul_status_processor import load_dict_status_flat
            
            # Удаляем старые таблицы/словари если существуют
            try:
                self.client.query("DROP DICTIONARY IF EXISTS status_dict_flat")
                self.client.query("DROP TABLE IF EXISTS dict_status_flat")
                self.logger.info("🗑️ Удалены старые объекты dict_status_flat")
            except Exception as e:
                self.logger.debug(f"Старые объекты не существовали: {e}")
            
            # Создаем таблицу словаря статусов
            status_table_sql = """
            CREATE TABLE dict_status_flat (
                status_id UInt8,
                status_name String,
                load_timestamp DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (status_id, load_timestamp)
            SETTINGS index_granularity = 8192
            """
            
            self.client.query(status_table_sql)
            
            # Получаем словарь статусов
            status_mapping = load_dict_status_flat()
            
            # Заполняем данными
            status_data = []
            current_timestamp = datetime.now()
            
            for status_id, status_name in status_mapping.items():
                status_data.append([status_id, status_name, current_timestamp])
            
            self.client.insert('dict_status_flat', status_data,
                             column_names=['status_id', 'status_name', 'load_timestamp'])
            
            # Создаем ClickHouse Dictionary объект
            status_dict_ddl = f"""
            CREATE OR REPLACE DICTIONARY status_dict_flat (
                status_id UInt8,
                status_name String
            )
            PRIMARY KEY status_id
            SOURCE(CLICKHOUSE(
                HOST '{self.config['host']}'
                PORT {self.config['port']}
                TABLE 'dict_status_flat'
                DB '{self.config['database']}'
            ))
            LAYOUT(FLAT())
            LIFETIME(MIN 0 MAX 3600)
            """
            
            self.client.query(status_dict_ddl)
            
            self.logger.info(f"✅ Словарь статусов создан: {len(status_data)} записей")
            self.logger.info("📋 Статусы:")
            for status_id, status_name in sorted(status_mapping.items()):
                self.logger.info(f"   {status_id}: {status_name}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания словаря статусов: {e}")
            return False
    
    def create_aircraft_number_dictionary(self) -> bool:
        """Создание аддитивного словаря номеров ВС dict_aircraft_number_flat с ac_type_mask"""
        self.logger.info("🚁 Создание аддитивного словаря номеров ВС с ac_type_mask...")
        
        try:
            # Проверяем существование таблицы heli_pandas
            table_exists = self.client.query("EXISTS TABLE heli_pandas").result_rows[0][0]
            if not table_exists:
                self.logger.error("❌ Таблица heli_pandas не существует!")
                self.logger.error("💡 Словарь номеров ВС создается ПОСЛЕ загрузки данных в heli_pandas")
                return False
            
            # Получаем уникальные номера ВС с их ac_type_mask из heli_pandas
            # ЛОГИКА: 
            # 1. Берем ТОЛЬКО планеры используя md_components.group_by IN (1, 2)
            # 2. ac_type_mask берем от этих записей
            aircraft_query = """
            SELECT 
                h.aircraft_number,
                any(h.ac_type_mask) as ac_type_mask
            FROM heli_pandas h
            JOIN md_components m ON h.partseqno_i = m.partseqno_i
            WHERE h.aircraft_number IS NOT NULL AND h.aircraft_number > 0
                AND h.ac_type_mask IS NOT NULL AND h.ac_type_mask > 0
                AND m.group_by IN (1, 2)
            GROUP BY h.aircraft_number
            ORDER BY h.aircraft_number
            """
            
            result = self.client.query(aircraft_query)
            if not result.result_rows:
                self.logger.warning("⚠️ Нет данных о номерах ВС в heli_pandas — создаём пустую таблицу и Dictionary")
                # Создаём таблицу, если её нет
                aircraft_table_sql = """
                CREATE TABLE IF NOT EXISTS dict_aircraft_number_flat (
                    aircraft_number UInt32,
                    formatted_number String,
                    registration_code String,
                    is_leading_zero UInt8 DEFAULT 0,
                    ac_type_mask UInt8 DEFAULT 0,
                    version_date Date DEFAULT today(),
                    version_id UInt8 DEFAULT 1,
                    load_timestamp DateTime DEFAULT now()
                ) ENGINE = MergeTree()
                ORDER BY (aircraft_number, version_date, version_id, load_timestamp)
                PARTITION BY toYYYYMM(version_date)
                SETTINGS index_granularity = 8192
                """
                self.client.query(aircraft_table_sql)
                # Создаём/обновляем Dictionary объект
                aircraft_dict_ddl = f"""
                CREATE OR REPLACE DICTIONARY aircraft_number_dict_flat (
                    aircraft_number UInt32,
                    formatted_number String,
                    registration_code String,
                    is_leading_zero UInt8,
                    ac_type_mask UInt8
                )
                PRIMARY KEY aircraft_number
                SOURCE(CLICKHOUSE(
                    HOST '{self.config['host']}'
                    PORT {self.config['port']}
                    TABLE 'dict_aircraft_number_flat'
                    DB '{self.config['database']}'
                ))
                LAYOUT(FLAT())
                LIFETIME(MIN 0 MAX 3600)
                """
                self.client.query(aircraft_dict_ddl)
                self.logger.info("✅ Пустая dict_aircraft_number_flat создана и Dictionary определён")
                return True
            
            # Создаем словарь aircraft_number -> ac_type_mask
            aircraft_data_map = {}
            
            for row in result.result_rows:
                aircraft_number, ac_type_mask = row
                aircraft_data_map[aircraft_number] = ac_type_mask
            
            self.logger.info(f"📋 Найдено {len(aircraft_data_map)} уникальных номеров ВС с ac_type_mask")
            
            # Создаем таблицу если не существует (АДДИТИВНАЯ + ВЕРСИОННАЯ) с новым полем ac_type_mask
            aircraft_table_sql = """
            CREATE TABLE IF NOT EXISTS dict_aircraft_number_flat (
                aircraft_number UInt32,
                formatted_number String,
                registration_code String,
                is_leading_zero UInt8 DEFAULT 0,
                ac_type_mask UInt8 DEFAULT 0,
                version_date Date DEFAULT today(),
                version_id UInt8 DEFAULT 1,
                load_timestamp DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (aircraft_number, version_date, version_id, load_timestamp)
            PARTITION BY toYYYYMM(version_date)
            SETTINGS index_granularity = 8192
            """
            
            self.client.query(aircraft_table_sql)
            
            # Проверяем и добавляем поле ac_type_mask если его нет в существующей таблице
            try:
                structure_result = self.client.query("DESCRIBE dict_aircraft_number_flat")
                columns = [row[0] for row in structure_result.result_rows]
                
                if 'ac_type_mask' not in columns:
                    self.logger.info("🔧 Добавляем поле ac_type_mask к существующей таблице...")
                    alter_sql = "ALTER TABLE dict_aircraft_number_flat ADD COLUMN ac_type_mask UInt8 DEFAULT 0"
                    self.client.query(alter_sql)
                    self.logger.info("✅ Поле ac_type_mask добавлено к существующей таблице")
                else:
                    self.logger.info("💡 Поле ac_type_mask уже существует в таблице")
                
                # Заполняем ac_type_mask для существующих записей если они пустые
                empty_count_result = self.client.query("SELECT COUNT(*) FROM dict_aircraft_number_flat WHERE ac_type_mask = 0")
                empty_count = empty_count_result.result_rows[0][0]
                
                if empty_count > 0:
                    self.logger.info(f"🔧 Заполняем ac_type_mask для {empty_count} существующих записей...")
                    
                    # Заполняем ac_type_mask на основе данных из heli_pandas
                    for aircraft_number, ac_type_mask in aircraft_data_map.items():
                        update_sql = f"""
                        ALTER TABLE dict_aircraft_number_flat 
                        UPDATE ac_type_mask = {ac_type_mask}
                        WHERE aircraft_number = {aircraft_number} AND ac_type_mask = 0
                        """
                        self.client.query(update_sql)
                    
                    self.logger.info(f"✅ ac_type_mask заполнен для существующих записей")
                else:
                    self.logger.info("💡 ac_type_mask уже заполнен для всех записей")
                    
            except Exception as alter_error:
                self.logger.warning(f"⚠️ Не удалось проверить/добавить поле ac_type_mask: {alter_error}")
                # Продолжаем выполнение
            
            # Получаем существующие номера для аддитивности
            existing_query = "SELECT DISTINCT aircraft_number FROM dict_aircraft_number_flat"
            try:
                existing_result = self.client.query(existing_query)
                existing_numbers = {row[0] for row in existing_result.result_rows}
                self.logger.info(f"📋 Найдено {len(existing_numbers)} существующих номеров ВС")
            except:
                existing_numbers = set()
                self.logger.info("📋 Словарь номеров ВС пуст")
            
            # Определяем новые номера для добавления
            new_numbers = set(aircraft_data_map.keys()) - existing_numbers
            
            if not new_numbers:
                self.logger.info("✅ Все номера ВС уже существуют в словаре")
            else:
                # Подготавливаем данные только для новых номеров
                aircraft_data = []
                current_timestamp = datetime.now()
                
                for aircraft_number in sorted(new_numbers):
                    formatted_number = f"{aircraft_number:05d}"
                    registration_code = f"RA-{formatted_number}"
                    is_leading_zero = 1 if aircraft_number < 10000 else 0
                    ac_type_mask = aircraft_data_map[aircraft_number]
                    
                    aircraft_data.append([
                        aircraft_number, formatted_number, registration_code, 
                        is_leading_zero, ac_type_mask, self.version_date, self.version_id, current_timestamp
                    ])
                
                # Аддитивная загрузка с новым полем ac_type_mask + версионность
                self.client.insert('dict_aircraft_number_flat', aircraft_data,
                                 column_names=['aircraft_number', 'formatted_number', 
                                             'registration_code', 'is_leading_zero', 'ac_type_mask', 
                                             'version_date', 'version_id', 'load_timestamp'])
                
                self.logger.info(f"✅ Добавлено {len(aircraft_data)} новых номеров ВС с ac_type_mask (аддитивно)")
                
                # Показываем примеры обогащенных данных
                self.logger.info("📋 Примеры обогащенных записей:")
                for i, aircraft_record in enumerate(aircraft_data[:3]):
                    aircraft_number, formatted_number, registration_code, is_leading_zero, ac_type_mask = aircraft_record[:5]
                    self.logger.info(f"  {aircraft_number} → {registration_code} (ac_type_mask: {ac_type_mask})")
            
            # Создаем/обновляем ClickHouse Dictionary объект с полем ac_type_mask
            aircraft_dict_ddl = f"""
            CREATE OR REPLACE DICTIONARY aircraft_number_dict_flat (
                aircraft_number UInt32,
                formatted_number String,
                registration_code String,
                is_leading_zero UInt8,
                ac_type_mask UInt8
            )
            PRIMARY KEY aircraft_number
            SOURCE(CLICKHOUSE(
                HOST '{self.config['host']}'
                PORT {self.config['port']}
                TABLE 'dict_aircraft_number_flat'
                DB '{self.config['database']}'
            ))
            LAYOUT(FLAT())
            LIFETIME(MIN 0 MAX 3600)
            """
            
            self.client.query(aircraft_dict_ddl)
            
            total_count = len(existing_numbers) + len(new_numbers if new_numbers else [])
            self.logger.info(f"✅ Словарь номеров ВС с ac_type_mask готов: {total_count} записей")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания словаря номеров ВС: {e}")
            return False
    
    def create_all_dictionaries_with_dictget(self) -> bool:
        """Создание ВСЕХ словарей с полной поддержкой dictGet"""
        self.logger.info("🚀 === СОЗДАНИЕ ВСЕХ СЛОВАРЕЙ С ПОДДЕРЖКОЙ DICTGET ===")
        
        try:
            # 1. Подключение
            if not self.connect_to_database():
                return False
            
            # 2. Создание основных аналитических словарей (АДДИТИВНЫЕ)
            if not self.run_full_analysis():
                self.logger.error("❌ Ошибка создания основных аналитических словарей")
                return False
            
            # 3. Создание словаря статусов (НЕ АДДИТИВНЫЙ)
            if not self.create_status_dictionary():
                self.logger.error("❌ Ошибка создания словаря статусов")
                return False
            
            # 4. Создание словаря номеров ВС (АДДИТИВНЫЙ)  
            if not self.create_aircraft_number_dictionary():
                self.logger.error("❌ Ошибка создания словаря номеров ВС")
                return False
            
            # 5. Проверка всех Dictionary объектов
            self.verify_all_dictionaries()
            
            self.logger.info("🎯 === ВСЕ СЛОВАРИ СОЗДАНЫ И ГОТОВЫ К РАБОТЕ ===")
            self.logger.info("✅ АДДИТИВНЫЕ словари:")
            self.logger.info("   - dict_partno_flat → partno_dict_flat")
            self.logger.info("   - dict_serialno_flat → serialno_dict_flat") 
            self.logger.info("   - dict_owner_flat → owner_dict_flat")
            self.logger.info("   - dict_ac_type_flat → ac_type_dict_flat")
            self.logger.info("   - dict_aircraft_number_flat → aircraft_number_dict_flat (с ac_type_mask)")
            self.logger.info("✅ НЕ АДДИТИВНЫЙ словарь:")
            self.logger.info("   - dict_status_flat → status_dict_flat")
            self.logger.info("🔥 Поддержка dictGet: ПОЛНАЯ для всех словарей")
            self.logger.info("🚁 aircraft_number_dict_flat теперь содержит ac_type_mask для Flame GPU")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка создания всех словарей: {e}")
            return False
    
    def verify_all_dictionaries(self) -> None:
        """Проверка всех созданных словарей и Dictionary объектов"""
        self.logger.info("🔍 Проверка всех словарей...")
        
        # Список всех словарей для проверки
        dictionaries_to_check = [
            ('dict_partno_flat', 'partno_dict_flat'),
            ('dict_serialno_flat', 'serialno_dict_flat'),
            ('dict_owner_flat', 'owner_dict_flat'),
            ('dict_ac_type_flat', 'ac_type_dict_flat'),
            ('dict_status_flat', 'status_dict_flat'),
            ('dict_aircraft_number_flat', 'aircraft_number_dict_flat')
        ]
        
        for table_name, dict_name in dictionaries_to_check:
            try:
                # Проверка таблицы
                table_count = self.client.query(f"SELECT COUNT(*) FROM {table_name}").result_rows[0][0]
                
                # Проверка Dictionary объекта
                dict_check = self.client.query(f"SELECT COUNT(*) FROM system.dictionaries WHERE name = '{dict_name}'").result_rows[0][0]
                
                status = "✅" if dict_check > 0 else "❌"
                self.logger.info(f"   {status} {table_name} ({table_count} записей) → {dict_name}")
                
            except Exception as e:
                self.logger.warning(f"   ⚠️ Ошибка проверки {table_name}: {e}")


def main(version_date=None, version_id=None):
    """Основная функция с поддержкой версионирования"""
    print("🚀 === ЗАГРУЗЧИК DICTIONARY_CREATOR ===")
    
    try:
        creator = DictionaryCreator()
        
        # Устанавливаем версионные параметры если переданы
        if version_date is not None and version_id is not None:
            creator.version_date = version_date
            creator.version_id = version_id
            print(f"🗓️ Версия данных (из параметров ETL): {version_date}, version_id: {version_id}")
        else:
            print("📅 Версионные параметры будут получены из heli_pandas автоматически")
        
        # Создание ВСЕХ словарей с версионностью из heli_pandas
        success = creator.create_all_dictionaries_with_dictget()
        
        if success:
            print("🎯 Словари созданы успешно!")
            return True
        else:
            print("❌ Ошибка создания словарей!")
            return False
            
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        return False


if __name__ == "__main__":
    """Точка входа скрипта"""
    import sys
    import argparse
    from datetime import datetime
    
    parser = argparse.ArgumentParser(description='Dictionary Creator - Создание всех словарей')
    parser.add_argument('--version-date', type=str, help='Дата версии данных (YYYY-MM-DD)')
    parser.add_argument('--version-id', type=int, help='ID версии данных')
    parser.add_argument('--legacy', action='store_true', help='Legacy режим без версионности')
    
    args = parser.parse_args()
    
    if args.legacy:
        # Legacy режим без версионности
        creator = DictionaryCreator()
        success = creator.run_full_analysis()
        print("⚠️ LEGACY режим: созданы только аналитические словари")
    else:
        # Парсинг версионных параметров
        version_date_parsed = None
        version_id_parsed = None
        
        if args.version_date:
            try:
                version_date_parsed = datetime.strptime(args.version_date, '%Y-%m-%d').date()
            except ValueError:
                print(f"❌ Неверный формат даты: {args.version_date}. Используйте YYYY-MM-DD")
                sys.exit(1)
        
        if args.version_id:
            version_id_parsed = args.version_id
        
        # Новый режим с версионностью
        success = main(version_date=version_date_parsed, version_id=version_id_parsed)
    
    sys.exit(0 if success else 1) 