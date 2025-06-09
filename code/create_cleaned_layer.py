#!/usr/bin/env python3
"""
🧹 Создание CLEANED слоя в ClickHouse
=================================

Создает очищенную таблицу Status_Components_cleaned из RAW данных с применением:
1. Фильтрации по справочнику MD_Components.xlsx (только валидные partno)
2. Битовых масок применимости (effectivity_type)
3. Обогащения данными из справочника

Автор: AI Assistant
Дата: 2025-01-09
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
import sys
import os

# Используем clickhouse_driver напрямую, как в архивном проекте
from clickhouse_driver import Client

# Добавляем путь к утилитам
sys.path.append(str(Path(__file__).parent / 'utils'))
from config_loader import load_database_config

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cleaned_layer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CleanedLayerCreator:
    """Создатель CLEANED слоя в ClickHouse"""
    
    def __init__(self):
        """Инициализация создателя CLEANED слоя"""
        # Загружаем конфигурацию через универсальную функцию
        config = load_database_config()
        
        # Подключение к ClickHouse как в архивном проекте
        self.client = Client(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
        self.md_components_path = Path('data_input/master_data/MD_Сomponents.xlsx')
        self.logger = logger
        
        # Маппинг битовых масок из анализа
        self.effectivity_mapping = {
            0b01100000: {'id': 1, 'types': ['Ми-8Т', 'Ми-17'], 'desc': 'Универсальные'},
            0b01000000: {'id': 2, 'types': ['Ми-17'], 'desc': 'Только Ми-17'},
            0b00100000: {'id': 3, 'types': ['Ми-8Т'], 'desc': 'Только Ми-8Т'}
        }
        
    def load_md_components(self) -> pd.DataFrame:
        """
        Загружает справочник MD_Components.xlsx
        
        Returns:
            DataFrame с компонентами и их характеристиками
        """
        try:
            self.logger.info(f"📂 Загружаю справочник {self.md_components_path}")
            
            # Загружаем с русскими заголовками (строка 8 = индекс 7)
            df = pd.read_excel(
                self.md_components_path,
                sheet_name='Агрегаты',
                header=7,  # Русские заголовки
                engine='openpyxl'
            )
            
            # Маппинг русских названий на английские
            column_mapping = {
                'Агрегат': 'component_name',
                'Чертежный номер': 'partno',
                'Количество установленных на ВС': 'qty_per_aircraft',
                'Группировка оборота': 'group_by',
                'Применимость к типу ВС': 'effectivity_type',
                'Запрет на смену типа ВС': 'type_restricted'
            }
            
            # Переименовываем столбцы
            df_renamed = df.rename(columns=column_mapping)
            
            # Фильтруем только нужные столбцы
            required_columns = [
                'partno', 'component_name', 'qty_per_aircraft', 
                'group_by', 'effectivity_type', 'type_restricted'
            ]
            
            available_columns = [col for col in required_columns if col in df_renamed.columns]
            df_filtered = df_renamed[available_columns].copy()
            
            # Очищаем от пустых строк
            df_filtered = df_filtered.dropna(subset=['partno'])
            
            self.logger.info(f"✅ Загружено {len(df_filtered)} компонентов из справочника")
            self.logger.info(f"📋 Столбцы: {list(df_filtered.columns)}")
            
            # Выводим несколько примеров для проверки
            self.logger.info("📋 Примеры данных:")
            for _, row in df_filtered.head(3).iterrows():
                self.logger.info(f"   {row['component_name']} | {row['partno']} | {row['effectivity_type']}")
            
            return df_filtered
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка при загрузке MD_Components: {e}")
            raise
    
    def create_effectivity_mapping_table(self):
        """Создает справочную таблицу битовых масок"""
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS dim_effectivity_type (
            effectivity_id UInt8,
            binary_mask String,
            decimal_value UInt16,
            description String,
            mi8t_compatible UInt8,
            mi17_compatible UInt8
        ) ENGINE = Memory
        """
        
        self.client.execute(create_table_sql)
        
        # Очищаем и заполняем данными
        self.client.execute("TRUNCATE TABLE dim_effectivity_type")
        
        for mask, info in self.effectivity_mapping.items():
            insert_sql = """
            INSERT INTO dim_effectivity_type VALUES 
            ({}, '{}', {}, '{}', {}, {})
            """.format(
                info['id'],
                bin(mask),
                mask,
                info['desc'],
                1 if 'Ми-8Т' in info['types'] else 0,
                1 if 'Ми-17' in info['types'] else 0
            )
            self.client.execute(insert_sql)
        
        self.logger.info("✅ Создана справочная таблица dim_effectivity_type")
    
    def create_cleaned_table(self):
        """Создает структуру CLEANED таблицы"""
        
        drop_table_sql = "DROP TABLE IF EXISTS Status_Components_cleaned"
        self.client.execute(drop_table_sql)
        
        create_table_sql = """
        CREATE TABLE Status_Components_cleaned (
            -- Основные поля из RAW
            partno LowCardinality(String),
            serialno LowCardinality(String),
            ac_typ LowCardinality(String),
            location Nullable(String),
            mfg_date Nullable(Date),
            removal_date Nullable(Date),
            target_date Nullable(Date),
            condition LowCardinality(String),
            owner LowCardinality(String),
            lease_restricted LowCardinality(String),
            oh Nullable(UInt32),
            oh_threshold Nullable(UInt32),
            ll Nullable(UInt32),
            sne Nullable(UInt32),
            ppr Nullable(UInt32),
            
            -- Обогащение из MD_Components
            component_name LowCardinality(String),
            qty_per_aircraft Nullable(UInt8),
            group_by Nullable(UInt8),
            effectivity_type Nullable(UInt16),
            effectivity_id Nullable(UInt8),
            type_restricted Nullable(UInt8),
            
            -- Метаданные
            version_date Date DEFAULT today(),
            cleaned_timestamp DateTime DEFAULT now()
            
        ) ENGINE = MergeTree()
        ORDER BY (partno, serialno, version_date)
        PARTITION BY toYYYYMM(version_date)
        """
        
        self.client.execute(create_table_sql)
        self.logger.info("✅ Создана таблица Status_Components_cleaned")
        
        # Добавляем комментарии (убираем из-за проблем с кодировкой в ClickHouse)
        # comment_sql = """
        # ALTER TABLE Status_Components_cleaned 
        # COMMENT 'CLEANED layer for component statuses with MD_Components filtering and bitmasks'
        # """
        # self.client.execute(comment_sql)
    
    def get_compatibility_filter(self, ac_typ: str, effectivity_type: int) -> bool:
        """
        Проверяет совместимость компонента с типом ВС
        
        Args:
            ac_typ: Тип воздушного судна
            effectivity_type: Битовая маска применимости
            
        Returns:
            True если совместим, False иначе
        """
        for mask, info in self.effectivity_mapping.items():
            if mask == effectivity_type:
                return ac_typ in info['types']
        
        # Если маска не найдена, считаем универсальной
        return True
    
    def process_and_insert_data(self, md_components: pd.DataFrame):
        """
        Обрабатывает и вставляет данные в CLEANED таблицу
        
        Args:
            md_components: DataFrame с компонентами из справочника
        """
        # Получаем список валидных partno из справочника
        valid_partnos = set(md_components['partno'].dropna().astype(str))
        self.logger.info(f"📋 Валидных партномеров в справочнике: {len(valid_partnos)}")
        
        # Создаем маппинг для быстрого поиска
        md_dict = {}
        for _, row in md_components.iterrows():
            partno = str(row['partno'])
            
            # Конвертируем битовую маску из строки в число
            effectivity_type = row.get('effectivity_type')
            if isinstance(effectivity_type, str) and effectivity_type.startswith('0b'):
                try:
                    effectivity_type = int(effectivity_type, 2)  # Конвертируем из двоичной строки
                except ValueError:
                    effectivity_type = None
            elif pd.isna(effectivity_type):
                effectivity_type = None
            
            md_dict[partno] = {
                'component_name': row.get('component_name', ''),
                'qty_per_aircraft': row.get('qty_per_aircraft'),
                'group_by': row.get('group_by'),
                'effectivity_type': effectivity_type,
                'type_restricted': row.get('type_restricted')
            }
        
        # Читаем данные из RAW таблицы с фильтрацией
        partno_list = "', '".join(valid_partnos)
        
        select_sql = f"""
        SELECT 
            partno, serialno, ac_typ, location,
            mfg_date, removal_date, target_date,
            condition, owner, lease_restricted,
            oh, oh_threshold, ll, sne, ppr,
            version_date
        FROM Status_Components_raw 
        WHERE partno IN ('{partno_list}')
        ORDER BY partno, serialno
        """
        
        self.logger.info("🔍 Читаю данные из RAW таблицы...")
        raw_data = self.client.execute(select_sql)
        
        if not raw_data:
            self.logger.warning("⚠️ Нет данных в RAW таблице для фильтрации")
            return
        
        self.logger.info(f"📊 Получено {len(raw_data)} записей из RAW")
        
        # Обрабатываем данные порциями
        batch_size = 5000
        inserted_count = 0
        filtered_count = 0
        
        for i in range(0, len(raw_data), batch_size):
            batch = raw_data[i:i + batch_size]
            batch_data = []
            
            for row in batch:
                partno = row[0]  # partno
                ac_typ = row[2]  # ac_typ
                
                # Получаем данные из справочника
                md_info = md_dict.get(str(partno), {})
                effectivity_type = md_info.get('effectivity_type')
                
                # Применяем битовые маски - фильтруем несовместимые
                if effectivity_type is not None and ac_typ:
                    if not self.get_compatibility_filter(ac_typ, effectivity_type):
                        filtered_count += 1
                        continue
                
                # Определяем effectivity_id
                effectivity_id = None
                if effectivity_type is not None:
                    for mask, info in self.effectivity_mapping.items():
                        if mask == effectivity_type:
                            effectivity_id = info['id']
                            break
                
                # Подготавливаем запись для вставки
                clean_row = list(row) + [
                    md_info.get('component_name', ''),
                    md_info.get('qty_per_aircraft'),
                    md_info.get('group_by'),
                    effectivity_type,
                    effectivity_id,
                    md_info.get('type_restricted'),
                    datetime.now()  # cleaned_timestamp
                ]
                
                batch_data.append(clean_row)
            
            # Вставляем батч
            if batch_data:
                insert_sql = """
                INSERT INTO Status_Components_cleaned (
                    partno, serialno, ac_typ, location,
                    mfg_date, removal_date, target_date,
                    condition, owner, lease_restricted,
                    oh, oh_threshold, ll, sne, ppr, version_date,
                    component_name, qty_per_aircraft, group_by,
                    effectivity_type, effectivity_id, type_restricted,
                    cleaned_timestamp
                ) VALUES
                """
                
                self.client.execute(insert_sql, batch_data)
                inserted_count += len(batch_data)
                
                self.logger.info(f"📝 Вставлено {len(batch_data)} записей (батч {i//batch_size + 1})")
        
        self.logger.info(f"✅ Обработка завершена:")
        self.logger.info(f"   📊 Вставлено записей: {inserted_count:,}")
        self.logger.info(f"   🚫 Отфильтровано по маскам: {filtered_count:,}")
    
    def validate_cleaned_data(self):
        """Валидация данных в CLEANED таблице"""
        
        validation_queries = [
            ("Общее количество записей", "SELECT count() FROM Status_Components_cleaned"),
            ("Уникальных partno", "SELECT count(DISTINCT partno) FROM Status_Components_cleaned"),
            ("Записей с component_name", "SELECT count() FROM Status_Components_cleaned WHERE component_name != ''"),
            ("Распределение по effectivity_id", "SELECT effectivity_id, count() FROM Status_Components_cleaned GROUP BY effectivity_id ORDER BY effectivity_id"),
            ("Распределение по ac_typ", "SELECT ac_typ, count() FROM Status_Components_cleaned WHERE ac_typ != '' GROUP BY ac_typ ORDER BY count() DESC LIMIT 5")
        ]
        
        self.logger.info("🔍 Валидация CLEANED данных:")
        
        for description, query in validation_queries:
            try:
                result = self.client.execute(query)
                if result and isinstance(result[0], tuple):
                    self.logger.info(f"   {description}:")
                    for row in result:
                        self.logger.info(f"     {row}")
                else:
                    self.logger.info(f"   {description}: {result[0]:,}")
            except Exception as e:
                self.logger.error(f"   ❌ Ошибка в валидации '{description}': {e}")
    
    def create_cleaned_layer(self):
        """Основной метод создания CLEANED слоя"""
        
        try:
            self.logger.info("🚀 Начинаю создание CLEANED слоя...")
            
            # 1. Загружаем справочник MD_Components
            md_components = self.load_md_components()
            
            # 2. Создаем справочную таблицу битовых масок
            self.create_effectivity_mapping_table()
            
            # 3. Создаем структуру CLEANED таблицы
            self.create_cleaned_table()
            
            # 4. Обрабатываем и вставляем данные
            self.process_and_insert_data(md_components)
            
            # 5. Валидируем результат
            self.validate_cleaned_data()
            
            self.logger.info("🎉 CLEANED слой успешно создан!")
            
        except Exception as e:
            self.logger.error(f"💥 Ошибка при создании CLEANED слоя: {e}")
            raise
        finally:
            self.client.disconnect()

def main():
    """Главная функция"""
    
    print("🧹 Создание CLEANED слоя в ClickHouse")
    print("=" * 50)
    
    try:
        creator = CleanedLayerCreator()
        creator.create_cleaned_layer()
        
        print("\n✅ Успешно создан CLEANED слой!")
        print("📊 Данные отфильтрованы по справочнику MD_Components")
        print("🎯 Применены битовые маски применимости")
        
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 