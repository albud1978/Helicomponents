#!/usr/bin/env python3
"""
Оптимизированный pipeline v2.0 со словарями в ClickHouse
Сравнение подходов: pandas dict lookup vs ClickHouse Dictionary lookup
"""

import os
import sys
import pandas as pd
import numpy as np
import cudf
import pyarrow as pa
import clickhouse_connect
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
import time
from concurrent.futures import ThreadPoolExecutor
import yaml

# Добавляем путь к утилитам
sys.path.append(str(Path(__file__).parent / 'utils'))
from config_loader import load_database_config

class OptimizedPipelineV2WithDictionaries:
    """
    Оптимизированный pipeline v2.0 со словарями в ClickHouse
    
    Архитектура:
    1. RAW таблица: исходные данные + ссылки на словари
    2. Dictionary таблицы: партномеры, типы ВС, владельцы, состояния
    3. Results таблица: результаты GPU + обогащение через ClickHouse Dictionary lookup
    
    Поток данных:
    Excel → pandas → Dictionaries → RAW table → cuDF → Flame GPU → cuDF → CH Dictionary lookup → Results table
    """
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.config = load_database_config()
        self.client = None
        
        # Маппинги будут загружаться из ClickHouse Dictionary
        self.partno_mapping = {}
        self.ac_type_mapping = {}
        self.component_type_mapping = {}
        self.owner_mapping = {}
        self.condition_mapping = {}
        
    def _setup_logging(self) -> logging.Logger:
        """Настройка логирования"""
        os.makedirs('test_output', exist_ok=True)
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        logger = logging.getLogger('optimized_pipeline_v2_dict')
        logger.setLevel(logging.INFO)
        
        # Убираем существующие обработчики
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Файловый обработчик
        file_handler = logging.FileHandler('test_output/optimized_pipeline_v2_dict.log', mode='w')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        
        # Консольный обработчик
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def connect_clickhouse(self) -> bool:
        """Подключение к ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                username=self.config.get('user'),
                password=self.config.get('password'),
                settings={'max_threads': 8}
            )
            
            # Тест подключения
            self.client.query("SELECT 1")
            self.logger.info(f"✅ ClickHouse подключен: {self.config['host']}:{self.config['port']}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения к ClickHouse: {e}")
            return False
    
    def create_dictionary_tables(self) -> bool:
        """Создание Dictionary таблиц"""
        self.logger.info("🏗️ Создание Dictionary таблиц...")
        
        # 1. Словарь партномеров
        partno_dict_sql = """
        CREATE TABLE IF NOT EXISTS dict_partno (
            partno_id UInt16,
            partno String,
            component_name String,
            component_type String
        ) ENGINE = Memory
        """
        
        # 2. Словарь типов ВС (битовые маски)
        ac_type_dict_sql = """
        CREATE TABLE IF NOT EXISTS dict_ac_type (
            ac_typ String,
            ac_type_mask UInt8,
            description String
        ) ENGINE = Memory
        """
        
        # 3. Словарь типов компонентов
        component_type_dict_sql = """
        CREATE TABLE IF NOT EXISTS dict_component_type (
            component_type_id UInt8,
            component_type String,
            description String
        ) ENGINE = Memory
        """
        
        # 4. Словарь владельцев
        owner_dict_sql = """
        CREATE TABLE IF NOT EXISTS dict_owner (
            owner_id UInt8,
            owner String,
            owner_type String
        ) ENGINE = Memory
        """
        
        # 5. Словарь состояний (битовые маски)
        condition_dict_sql = """
        CREATE TABLE IF NOT EXISTS dict_condition (
            condition String,
            condition_mask UInt8,
            description String
        ) ENGINE = Memory
        """
        
        try:
            self.client.command(partno_dict_sql)
            self.client.command(ac_type_dict_sql)
            self.client.command(component_type_dict_sql)
            self.client.command(owner_dict_sql)
            self.client.command(condition_dict_sql)
            
            self.logger.info("✅ Dictionary таблицы созданы")
            return True
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания Dictionary таблиц: {e}")
            return False
    
    def create_clickhouse_dictionaries(self) -> bool:
        """Создание ClickHouse Dictionary объектов"""
        self.logger.info("📚 Создание ClickHouse Dictionary объектов...")
        
        # 1. Dictionary для партномеров
        partno_dict_ddl = """
        CREATE OR REPLACE DICTIONARY partno_dictionary (
            partno String,
            partno_id UInt16,
            component_name String,
            component_type String
        )
        PRIMARY KEY partno
        SOURCE(CLICKHOUSE(
            HOST 'localhost'
            PORT 9000
            TABLE 'dict_partno'
            DB 'helicopter_analytics'
        ))
        LAYOUT(HASHED())
        LIFETIME(MIN 0 MAX 3600)
        """
        
        # 2. Dictionary для типов ВС
        ac_type_dict_ddl = """
        CREATE OR REPLACE DICTIONARY ac_type_dictionary (
            ac_typ String,
            ac_type_mask UInt8,
            description String
        )
        PRIMARY KEY ac_typ
        SOURCE(CLICKHOUSE(
            HOST 'localhost'
            PORT 9000
            TABLE 'dict_ac_type'
            DB 'helicopter_analytics'
        ))
        LAYOUT(HASHED())
        LIFETIME(MIN 0 MAX 3600)
        """
        
        # 3. Dictionary для владельцев
        owner_dict_ddl = """
        CREATE OR REPLACE DICTIONARY owner_dictionary (
            owner String,
            owner_id UInt8,
            owner_type String
        )
        PRIMARY KEY owner
        SOURCE(CLICKHOUSE(
            HOST 'localhost'
            PORT 9000
            TABLE 'dict_owner'
            DB 'helicopter_analytics'
        ))
        LAYOUT(HASHED())
        LIFETIME(MIN 0 MAX 3600)
        """
        
        # 4. Dictionary для состояний
        condition_dict_ddl = """
        CREATE OR REPLACE DICTIONARY condition_dictionary (
            condition String,
            condition_mask UInt8,
            description String
        )
        PRIMARY KEY condition
        SOURCE(CLICKHOUSE(
            HOST 'localhost'
            PORT 9000
            TABLE 'dict_condition'
            DB 'helicopter_analytics'
        ))
        LAYOUT(HASHED())
        LIFETIME(MIN 0 MAX 3600)
        """
        
        try:
            self.client.command(partno_dict_ddl)
            self.client.command(ac_type_dict_ddl)
            self.client.command(owner_dict_ddl)
            self.client.command(condition_dict_ddl)
            
            self.logger.info("✅ ClickHouse Dictionary объекты созданы")
            return True
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания ClickHouse Dictionary: {e}")
            return False
    
    def populate_dictionaries(self, df: pd.DataFrame, md_dict: pd.DataFrame, md_comp: pd.DataFrame) -> bool:
        """Заполнение Dictionary таблиц данными"""
        self.logger.info("📥 Заполнение Dictionary таблиц...")
        
        start_time = time.time()
        
        try:
            # 1. Словарь партномеров
            unique_partnos = df['partno'].dropna().unique()
            partno_data = []
            for idx, partno in enumerate(unique_partnos, 1):
                # Ищем компонент в справочнике
                component_info = md_comp[md_comp['partno'] == partno]
                component_name = component_info['component_name'].iloc[0] if not component_info.empty else partno
                component_type = component_info['component_type'].iloc[0] if not component_info.empty else 'Unknown'
                
                partno_data.append([idx, partno, component_name, component_type])
                self.partno_mapping[partno] = idx
            
            self.client.insert('dict_partno', partno_data,
                             column_names=['partno_id', 'partno', 'component_name', 'component_type'])
            
            # 2. Словарь типов ВС
            ac_type_data = [
                ['Ми-26', 128, 'Тяжелый вертолет'],
                ['Ми-17', 64, 'Средний вертолет Ми-17'],
                ['Ми-8Т', 32, 'Средний вертолет Ми-8Т'],
                ['Ка-32', 16, 'Корабельный вертолет'],
                ['AS-350', 8, 'Легкий вертолет AS-350'],
                ['AS-355', 4, 'Легкий вертолет AS-355'],
                ['R-44', 2, 'Сверхлегкий вертолет']
            ]
            
            self.client.insert('dict_ac_type', ac_type_data,
                             column_names=['ac_typ', 'ac_type_mask', 'description'])
            
            # 3. Словарь типов компонентов
            unique_components = df['component_type'].dropna().unique()
            component_type_data = []
            for idx, comp_type in enumerate(unique_components, 1):
                component_type_data.append([idx, comp_type, f'Тип компонента: {comp_type}'])
                self.component_type_mapping[comp_type] = idx
            
            self.client.insert('dict_component_type', component_type_data,
                             column_names=['component_type_id', 'component_type', 'description'])
            
            # 4. Словарь владельцев
            owner_data = [
                [1, 'ЮТ-ВУ', 'Operator'],
                [2, 'UTE', 'Operator'],
                [3, 'ГТЛК', 'Leasing'],
                [4, 'СБЕР ЛИЗИНГ', 'Leasing'],
                [5, 'ГПМ', 'Maintenance'],
                [6, 'АО ГПМ', 'Maintenance'],
                [7, 'ИП', 'Individual'],
                [8, 'АРВ', 'Operator'],
                [9, 'И', 'Other']
            ]
            
            self.client.insert('dict_owner', owner_data,
                             column_names=['owner_id', 'owner', 'owner_type'])
            
            # 5. Словарь состояний
            condition_data = [
                ['ИСПРАВНЫЙ', 7, 'Исправен, в эксплуатации'],
                ['НЕИСПРАВНЫЙ', 4, 'Неисправен, требует ремонта'],
                ['ДОНОР', 1, 'Донор запчастей'],
                ['СНЯТ', 0, 'Снят с эксплуатации'],
                ['НЕ УСТАНОВЛЕН', 6, 'Исправен, не установлен'],
                ['ПОСТАВКА', 3, 'Поставка, резерв']
            ]
            
            self.client.insert('dict_condition', condition_data,
                             column_names=['condition', 'condition_mask', 'description'])
            
            duration = time.time() - start_time
            self.logger.info(f"✅ Dictionary таблицы заполнены за {duration:.2f} сек")
            self.logger.info(f"📊 Загружено: {len(partno_data)} партномеров, {len(ac_type_data)} типов ВС")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка заполнения Dictionary: {e}")
            return False
    
    def create_raw_table(self) -> bool:
        """Создание RAW таблицы (без числовых ID, они будут из Dictionary)"""
        raw_table_sql = """
        CREATE TABLE IF NOT EXISTS helicopter_components_raw_dict (
            -- Основные идентификаторы (строковые - для lookup в Dictionary)
            partno String,
            serialno String,  -- ключ связи
            ac_typ String,
            component_type String,
            location String,
            owner String,
            condition String,
            
            -- Ресурсные данные
            ll Float32,
            oh Float32,
            oh_threshold Float32,
            sne Float32,
            ppr Float32,
            
            -- Даты
            mfg_date Nullable(Date),
            removal_date Nullable(Date),
            target_date Nullable(Date),
            
            -- Метаданные
            version_date Date DEFAULT today(),
            load_timestamp DateTime DEFAULT now()
            
        ) ENGINE = MergeTree()
        PARTITION BY (toYYYYMM(version_date))
        ORDER BY (serialno, partno, version_date)
        SETTINGS index_granularity = 8192
        """
        
        try:
            self.client.command(raw_table_sql)
            self.logger.info("✅ RAW таблица создана/проверена")
            return True
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания RAW таблицы: {e}")
            return False
    
    def create_results_table(self) -> bool:
        """Создание таблицы результатов"""
        results_table_sql = """
        CREATE TABLE IF NOT EXISTS helicopter_simulation_results_dict (
            -- Ключ связи
            serialno String,
            
            -- Результаты GPU симуляции
            predicted_failure_days UInt16,
            maintenance_priority UInt8,
            replacement_recommended UInt8,  -- 0/1
            remaining_resource_pct Float32,
            risk_score Float32,
            
            -- Денормализованные поля (обогащенные через ClickHouse Dictionary)
            partno String,
            component_name String,
            component_type String,
            ac_typ String,
            ac_type_description String,
            location String,
            owner String,
            owner_type String,
            condition String,
            condition_description String,
            
            -- Исходные ресурсы
            current_ll Float32,
            current_oh Float32,
            oh_threshold Float32,
            
            -- Метаданные симуляции
            simulation_id String,
            simulation_date DateTime DEFAULT now(),
            model_version String,
            input_version_date Date,
            processing_time_ms UInt32
            
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(simulation_date)
        ORDER BY (serialno, simulation_id)
        SETTINGS index_granularity = 8192
        """
        
        try:
            self.client.command(results_table_sql)
            self.logger.info("✅ Results таблица создана/проверена")
            return True
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания Results таблицы: {e}")
            return False
    
    def load_excel_data(self, excel_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Загрузка данных из Excel"""
        self.logger.info(f"📂 Загрузка данных из {excel_path}")
        
        try:
            # Загрузка основных данных
            df = pd.read_excel(excel_path, sheet_name='Status_Components')
            
            # Загрузка справочников
            md_dict = pd.read_excel('data_input/master_data/MD_Dictionary.xlsx')
            md_comp = pd.read_excel('data_input/master_data/MD_Сomponents.xlsx')
            
            self.logger.info(f"✅ Загружено {len(df)} записей из Excel")
            return df, md_dict, md_comp
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки Excel: {e}")
            raise
    
    def load_raw_data(self, df: pd.DataFrame) -> bool:
        """Загрузка RAW данных (без обогащения - все строковые поля)"""
        self.logger.info("📤 Загрузка RAW данных...")
        
        start_time = time.time()
        
        try:
            # Подготавливаем данные как есть (все строковые)
            columns_to_load = [
                'partno', 'serialno', 'ac_typ', 'component_type', 'location', 'owner', 'condition',
                'll', 'oh', 'oh_threshold', 'sne', 'ppr',
                'mfg_date', 'removal_date', 'target_date'
            ]
            
            raw_data = df[columns_to_load].copy()
            raw_data['version_date'] = pd.Timestamp.today().date()
            raw_data['load_timestamp'] = pd.Timestamp.now()
            
            # Загрузка через Arrow
            table = pa.Table.from_pandas(raw_data)
            
            self.client.insert_arrow('helicopter_components_raw_dict', table)
            
            duration = time.time() - start_time
            self.logger.info(f"✅ RAW данные загружены за {duration:.2f} сек")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки RAW данных: {e}")
            return False
    
    def prepare_gpu_data_from_clickhouse(self) -> cudf.DataFrame:
        """Подготовка GPU данных через ClickHouse Dictionary lookup"""
        self.logger.info("🚀 Подготовка GPU данных через ClickHouse Dictionary...")
        
        start_time = time.time()
        
        try:
            # Запрос с Dictionary lookup для получения числовых ID
            gpu_query = """
            SELECT 
                serialno,
                dictGet('partno_dictionary', 'partno_id', partno) as partno_id,
                dictGet('ac_type_dictionary', 'ac_type_mask', ac_typ) as ac_type_mask,
                dictGet('owner_dictionary', 'owner_id', owner) as owner_id,
                dictGet('condition_dictionary', 'condition_mask', condition) as condition_mask,
                ll, oh, oh_threshold, sne, ppr
            FROM helicopter_components_raw_dict
            WHERE version_date = today()
            """
            
            # Загружаем через pandas, затем в cuDF
            pandas_df = self.client.query_df(gpu_query)
            cudf_data = cudf.from_pandas(pandas_df)
            
            duration = time.time() - start_time
            self.logger.info(f"✅ GPU данные подготовлены за {duration:.2f} сек")
            self.logger.info(f"📊 GPU DataFrame: {len(cudf_data)} записей, {cudf_data.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
            
            return cudf_data
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка подготовки GPU данных: {e}")
            raise
    
    def simulate_flame_gpu(self, cudf_data: cudf.DataFrame) -> cudf.DataFrame:
        """Симуляция Flame GPU (заглушка)"""
        self.logger.info("🔥 Запуск Flame GPU симуляции...")
        
        start_time = time.time()
        
        # Имитация GPU расчетов
        n_records = len(cudf_data)
        
        results = cudf.DataFrame({
            'serialno': cudf_data['serialno'].copy(),
            'predicted_failure_days': cudf.Series(np.random.randint(30, 1000, n_records), dtype='uint16'),
            'maintenance_priority': cudf.Series(np.random.randint(1, 6, n_records), dtype='uint8'),
            'replacement_recommended': cudf.Series(np.random.randint(0, 2, n_records), dtype='uint8'),
            'remaining_resource_pct': cudf.Series(np.random.uniform(0, 100, n_records), dtype='float32'),
            'risk_score': cudf.Series(np.random.uniform(0, 1, n_records), dtype='float32')
        })
        
        # Дедупликация
        results = results.drop_duplicates(subset=['serialno'])
        
        duration = time.time() - start_time
        self.logger.info(f"✅ Flame GPU симуляция завершена за {duration:.2f} сек")
        self.logger.info(f"📊 Результатов: {len(results)} уникальных записей")
        
        return results
    
    def load_results_with_dictionary_enrichment(self, gpu_results: cudf.DataFrame, simulation_id: str) -> bool:
        """Загрузка результатов с обогащением через ClickHouse Dictionary"""
        self.logger.info("📤 Загрузка результатов с Dictionary обогащением...")
        
        start_time = time.time()
        
        try:
            # Конвертируем GPU результаты в pandas для загрузки
            results_df = gpu_results.to_pandas()
            
            # Создаем временную таблицу для результатов GPU
            temp_table_sql = """
            CREATE TEMPORARY TABLE temp_gpu_results (
                serialno String,
                predicted_failure_days UInt16,
                maintenance_priority UInt8,
                replacement_recommended UInt8,
                remaining_resource_pct Float32,
                risk_score Float32
            )
            """
            
            self.client.command(temp_table_sql)
            
            # Загружаем GPU результаты во временную таблицу
            gpu_table = pa.Table.from_pandas(results_df)
            self.client.insert_arrow('temp_gpu_results', gpu_table)
            
            # Обогащенная вставка через Dictionary lookup
            enriched_insert_sql = f"""
            INSERT INTO helicopter_simulation_results_dict
            SELECT 
                -- GPU результаты
                g.serialno,
                g.predicted_failure_days,
                g.maintenance_priority,
                g.replacement_recommended,
                g.remaining_resource_pct,
                g.risk_score,
                
                -- Обогащение через Dictionary
                r.partno,
                dictGet('partno_dictionary', 'component_name', r.partno) as component_name,
                r.component_type,
                r.ac_typ,
                dictGet('ac_type_dictionary', 'description', r.ac_typ) as ac_type_description,
                r.location,
                r.owner,
                dictGet('owner_dictionary', 'owner_type', r.owner) as owner_type,
                r.condition,
                dictGet('condition_dictionary', 'description', r.condition) as condition_description,
                
                -- Исходные ресурсы
                r.ll as current_ll,
                r.oh as current_oh,
                r.oh_threshold,
                
                -- Метаданные
                '{simulation_id}' as simulation_id,
                now() as simulation_date,
                'FlameGPU_v2.0_Dict' as model_version,
                r.version_date as input_version_date,
                0 as processing_time_ms
                
            FROM temp_gpu_results g
            LEFT JOIN helicopter_components_raw_dict r ON g.serialno = r.serialno
            WHERE r.version_date = today()
            """
            
            self.client.command(enriched_insert_sql)
            
            duration = time.time() - start_time
            self.logger.info(f"✅ Результаты загружены с Dictionary обогащением за {duration:.2f} сек")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки результатов: {e}")
            return False
    
    def run_full_pipeline(self, excel_path: str) -> bool:
        """Запуск полного пайплайна с ClickHouse Dictionary"""
        self.logger.info("🚀 Запуск оптимизированного пайплайна v2.0 с ClickHouse Dictionary")
        
        total_start = time.time()
        
        try:
            # 1. Подключение к ClickHouse
            if not self.connect_clickhouse():
                return False
            
            # 2. Создание Dictionary структуры
            if not self.create_dictionary_tables():
                return False
            
            # 3. Создание основных таблиц
            if not self.create_raw_table() or not self.create_results_table():
                return False
            
            # 4. Загрузка данных из Excel
            df, md_dict, md_comp = self.load_excel_data(excel_path)
            
            # 5. Заполнение Dictionary таблиц
            if not self.populate_dictionaries(df, md_dict, md_comp):
                return False
            
            # 6. Создание ClickHouse Dictionary объектов
            if not self.create_clickhouse_dictionaries():
                return False
            
            # 7. Загрузка RAW данных (без обогащения)
            if not self.load_raw_data(df):
                return False
            
            # 8. Подготовка GPU данных через Dictionary lookup
            cudf_data = self.prepare_gpu_data_from_clickhouse()
            
            # 9. GPU симуляция
            gpu_results = self.simulate_flame_gpu(cudf_data)
            
            # 10. Загрузка результатов с Dictionary обогащением
            simulation_id = f"sim_dict_{int(time.time())}"
            success = self.load_results_with_dictionary_enrichment(gpu_results, simulation_id)
            
            total_duration = time.time() - total_start
            
            if success:
                self.logger.info("=" * 60)
                self.logger.info("✅ ПАЙПЛАЙН С DICTIONARY ЗАВЕРШЕН УСПЕШНО!")
                self.logger.info(f"⏱️ Общее время: {total_duration:.2f} сек")
                self.logger.info(f"📊 Обработано записей: {len(df)}")
                self.logger.info(f"📈 Результатов симуляции: {len(gpu_results)}")
                self.logger.info("=" * 60)
                return True
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка пайплайна: {e}")
            return False

def main():
    """Главная функция"""
    pipeline = OptimizedPipelineV2WithDictionaries()
    
    excel_path = "data_input/source_data/Status_Components.xlsx"
    
    if not os.path.exists(excel_path):
        pipeline.logger.error(f"❌ Файл не найден: {excel_path}")
        return False
    
    return pipeline.run_full_pipeline(excel_path)

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)