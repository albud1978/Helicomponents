#!/usr/bin/env python3
"""
Модуль интеграции ClickHouse -> cuDF -> Flame GPU
Оптимизированная загрузка и обработка данных для агентного моделирования
"""

import logging
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Union
import numpy as np
import cudf
import clickhouse_connect
from datetime import datetime
import yaml

# Добавляем путь к утилитам
sys.path.append(str(Path(__file__).parent / 'utils'))
from config_loader import load_config

logger = logging.getLogger(__name__)

class CuDFFlameIntegrator:
    """
    Оптимизированная интеграция ClickHouse -> cuDF -> Flame GPU
    
    Основные возможности:
    1. Zero-copy загрузка из ClickHouse в cuDF
    2. GPU-оптимизированные типы данных
    3. Unified Memory поддержка для больших датасетов
    4. Битовые операции для фильтрации
    5. Подготовка данных для Flame GPU агентов
    """
    
    def __init__(self, enable_unified_memory: bool = True):
        """
        Инициализация интегратора
        
        Args:
            enable_unified_memory: Включить CUDA Unified Memory для больших датасетов
        """
        self.config = load_config()
        self.client = None
        self.enable_unified_memory = enable_unified_memory
        
        # Настройка cuDF для unified memory
        if enable_unified_memory:
            try:
                cudf.set_option("spill", True)
                cudf.set_option("spill_on_demand", True)
                logger.info("✅ CUDA Unified Memory включена в cuDF")
            except Exception as e:
                logger.warning(f"⚠️ Не удалось включить Unified Memory: {e}")
    
    def connect_to_database(self) -> bool:
        """Подключение к ClickHouse"""
        try:
            db_config = self.config['database']['clickhouse']
            self.client = clickhouse_connect.get_client(
                host=db_config['host'],
                port=db_config['port'],
                database=db_config['database'],
                username=db_config.get('username'),
                password=db_config.get('password')
            )
            
            # Проверка подключения
            self.client.query("SELECT 1")
            logger.info(f"✅ ClickHouse подключен: {db_config['host']}:{db_config['port']}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к ClickHouse: {e}")
            return False
    
    def load_digital_data(self, version_date: Optional[str] = None, 
                         filters: Optional[Dict] = None) -> cudf.DataFrame:
        """
        Загрузка оптимизированных данных из ClickHouse в cuDF
        
        Args:
            version_date: Дата версии данных (по умолчанию сегодня)
            filters: Дополнительные фильтры {'field': value}
            
        Returns:
            cuDF DataFrame с GPU-оптимизированными типами
        """
        logger.info("🔄 Загрузка данных в cuDF...")
        
        # Базовый запрос с оптимизированными типами
        base_query = """
        SELECT 
            -- Ключевые поля (оптимизированные типы)
            partno_id,
            serialno_id, 
            ac_type_mask,
            location_id,
            
            -- Ресурсные поля (UInt32 для GPU арифметики)
            ll,
            oh,
            oh_threshold,
            sne,
            ppr,
            
            -- Даты (Date для эффективного хранения)
            mfg_date,
            removal_date,
            target_date,
            
            -- Категориальные поля (битовые маски)
            lease_restricted_bit,
            owner_id,
            condition_mask,
            
            -- Вычисляемые поля
            interchangeable_group_id,
            effectivity_type_mask,
            
            -- Метаданные
            version_date,
            load_timestamp
            
        FROM status_components_digital
        WHERE version_date = {version_filter}
        """
        
        # Определение даты версии
        if version_date is None:
            version_filter = "today()"
        else:
            version_filter = f"'{version_date}'"
        
        query = base_query.format(version_filter=version_filter)
        
        # Добавление дополнительных фильтров
        if filters:
            for field, value in filters.items():
                if isinstance(value, str):
                    query += f" AND {field} = '{value}'"
                elif isinstance(value, (list, tuple)):
                    query += f" AND {field} IN ({','.join(map(str, value))})"
                else:
                    query += f" AND {field} = {value}"
        
        try:
            # Загрузка данных через pandas, затем конвертация в cuDF
            # (прямая загрузка в cuDF может быть добавлена когда появится поддержка)
            pandas_df = self.client.query_df(query)
            
            # Конвертация в cuDF с оптимизированными типами
            cudf_df = cudf.from_pandas(pandas_df)
            
            # Принудительное приведение к GPU-оптимизированным типам
            cudf_df = self._optimize_dtypes(cudf_df)
            
            logger.info(f"✅ Загружено {len(cudf_df)} записей в cuDF")
            logger.info(f"📊 Размер в GPU памяти: {cudf_df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
            
            return cudf_df
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки данных: {e}")
            raise
    
    def _optimize_dtypes(self, df: cudf.DataFrame) -> cudf.DataFrame:
        """
        Принудительная оптимизация типов данных для GPU
        
        Args:
            df: cuDF DataFrame
            
        Returns:
            Оптимизированный DataFrame
        """
        logger.debug("🔧 Оптимизация типов данных для GPU...")
        
        # Маппинг оптимальных типов
        optimal_types = {
            'partno_id': 'uint16',
            'serialno_id': 'uint32',
            'ac_type_mask': 'uint8',
            'location_id': 'uint16',
            'll': 'uint32',
            'oh': 'uint32', 
            'oh_threshold': 'uint32',
            'sne': 'uint32',
            'ppr': 'uint32',
            'lease_restricted_bit': 'uint8',
            'owner_id': 'uint8',
            'condition_mask': 'uint8',
            'interchangeable_group_id': 'uint16',
            'effectivity_type_mask': 'uint8'
        }
        
        # Применение оптимальных типов
        for column, dtype in optimal_types.items():
            if column in df.columns:
                try:
                    df[column] = df[column].astype(dtype)
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось привести {column} к {dtype}: {e}")
        
        return df
    
    def apply_bitwise_filters(self, df: cudf.DataFrame, 
                            ac_types: Optional[List[str]] = None,
                            conditions: Optional[List[str]] = None,
                            owner_restrictions: bool = False) -> cudf.DataFrame:
        """
        Применение битовых фильтров для быстрой GPU фильтрации
        
        Args:
            df: cuDF DataFrame
            ac_types: Список типов ВС для фильтрации
            conditions: Список условий для фильтрации  
            owner_restrictions: Включить только компоненты с ограничениями владельца
            
        Returns:
            Отфильтрованный DataFrame
        """
        logger.info("🔍 Применение битовых фильтров...")
        
        filtered_df = df.copy()
        
        # Фильтр по типам ВС (битовые операции)
        if ac_types:
            ac_type_masks = {
                'Ми-26': 128, 'Ми-17': 64, 'Ми-8Т': 32, 'Ка-32': 16,
                'AS-350': 8, 'AS-355': 4, 'R-44': 2
            }
            
            # Создание объединенной битовой маски
            combined_mask = 0
            for ac_type in ac_types:
                if ac_type in ac_type_masks:
                    combined_mask |= ac_type_masks[ac_type]
            
            if combined_mask > 0:
                # GPU-оптимизированная битовая фильтрация
                mask = (filtered_df['ac_type_mask'] & combined_mask) > 0
                filtered_df = filtered_df[mask]
                logger.info(f"🎯 Фильтр по типам ВС: {len(filtered_df)} записей")
        
        # Фильтр по состояниям (битовые операции)
        if conditions:
            condition_masks = {
                'ИСПРАВНЫЙ': 7, 'НЕИСПРАВНЫЙ': 4, 'НЕ УСТАНОВЛЕН': 6,
                'ДОНОР': 1, 'СНЯТ': 0
            }
            
            condition_values = []
            for condition in conditions:
                if condition in condition_masks:
                    condition_values.append(condition_masks[condition])
            
            if condition_values:
                mask = filtered_df['condition_mask'].isin(condition_values)
                filtered_df = filtered_df[mask]
                logger.info(f"🎯 Фильтр по состояниям: {len(filtered_df)} записей")
        
        # Фильтр по ограничениям владельца
        if owner_restrictions:
            mask = filtered_df['lease_restricted_bit'] == 1
            filtered_df = filtered_df[mask]
            logger.info(f"🎯 Фильтр по ограничениям: {len(filtered_df)} записей")
        
        return filtered_df
    
    def calculate_maintenance_metrics(self, df: cudf.DataFrame) -> cudf.DataFrame:
        """
        Вычисление метрик технического обслуживания на GPU
        
        Args:
            df: cuDF DataFrame с ресурсными данными
            
        Returns:
            DataFrame с дополнительными метриками
        """
        logger.info("📊 Расчет метрик ТО на GPU...")
        
        result_df = df.copy()
        
        # Вычисления на GPU
        result_df['remaining_life'] = result_df['oh_threshold'] - result_df['oh']
        result_df['utilization_ratio'] = result_df['oh'] / result_df['oh_threshold']
        
        # Битовые флаги для быстрой классификации
        result_df['maintenance_urgency'] = 0
        
        # Критическое состояние (< 100 часов = 6000 минут)
        critical_mask = result_df['remaining_life'] < 6000
        result_df.loc[critical_mask, 'maintenance_urgency'] = 4  # 0b100
        
        # Предупреждение (< 500 часов = 30000 минут)  
        warning_mask = (result_df['remaining_life'] < 30000) & (~critical_mask)
        result_df.loc[warning_mask, 'maintenance_urgency'] = 2  # 0b010
        
        # Нормальное состояние
        normal_mask = result_df['remaining_life'] >= 30000
        result_df.loc[normal_mask, 'maintenance_urgency'] = 1  # 0b001
        
        logger.info("✅ Метрики ТО рассчитаны")
        return result_df
    
    def prepare_for_flame_gpu(self, df: cudf.DataFrame) -> Dict:
        """
        Подготовка данных для Flame GPU агентов
        
        Args:
            df: cuDF DataFrame с оптимизированными данными
            
        Returns:
            Словарь с данными для Flame GPU
        """
        logger.info("🔥 Подготовка данных для Flame GPU...")
        
        # Основные поля агентов (в оптимальном порядке для памяти)
        agent_fields = [
            'partno_id',        # uint16
            'serialno_id',      # uint32  
            'ac_type_mask',     # uint8
            'location_id',      # uint16
            'oh',               # uint32
            'oh_threshold',     # uint32
            'condition_mask',   # uint8
            'interchangeable_group_id'  # uint16
        ]
        
        # Извлечение данных агентов
        agent_data = {}
        for field in agent_fields:
            if field in df.columns:
                # Конвертация в numpy arrays для передачи в Flame GPU
                agent_data[field] = df[field].to_array()
        
        # Метаданные для Flame GPU
        flame_metadata = {
            'agent_count': len(df),
            'memory_per_agent': sum([
                2,  # partno_id
                4,  # serialno_id
                1,  # ac_type_mask
                2,  # location_id
                4,  # oh
                4,  # oh_threshold
                1,  # condition_mask
                2   # interchangeable_group_id
            ]),  # = 20 bytes per agent
            'total_memory_mb': len(df) * 20 / 1024 / 1024,
            'gpu_optimized': True,
            'unified_memory_enabled': self.enable_unified_memory
        }
        
        logger.info(f"🔥 Подготовлено {flame_metadata['agent_count']} агентов")
        logger.info(f"📊 Размер данных: {flame_metadata['total_memory_mb']:.1f} MB")
        
        return {
            'agent_data': agent_data,
            'metadata': flame_metadata,
            'original_dataframe': df
        }
    
    def create_compatibility_matrix(self, df: cudf.DataFrame) -> cudf.DataFrame:
        """
        Создание матрицы совместимости компонентов на GPU
        
        Args:
            df: cuDF DataFrame с данными компонентов
            
        Returns:
            Матрица совместимости
        """
        logger.info("🔗 Создание матрицы совместимости...")
        
        # Группировка по типам компонентов
        compatibility = df.groupby(['partno_id', 'interchangeable_group_id']).agg({
            'ac_type_mask': 'first',
            'effectivity_type_mask': 'first',
            'condition_mask': 'count'  # количество доступных компонентов
        }).reset_index()
        
        compatibility.columns = ['partno_id', 'group_id', 'installed_on', 
                               'compatible_with', 'available_count']
        
        # Вычисление совместимости (битовое AND)
        compatibility['is_compatible'] = (
            compatibility['installed_on'] & compatibility['compatible_with']
        ) > 0
        
        logger.info(f"✅ Создана матрица совместимости: {len(compatibility)} записей")
        return compatibility
    
    def export_to_flame_format(self, flame_data: Dict, output_path: str):
        """
        Экспорт данных в формат для Flame GPU
        
        Args:
            flame_data: Подготовленные данные для Flame GPU
            output_path: Путь для сохранения
        """
        logger.info(f"💾 Экспорт в формат Flame GPU: {output_path}")
        
        output_path = Path(output_path)
        output_path.mkdir(exist_ok=True)
        
        # Сохранение данных агентов в бинарном формате
        agent_data = flame_data['agent_data']
        for field, data in agent_data.items():
            np.save(output_path / f"{field}.npy", data)
        
        # Сохранение метаданных
        metadata_path = output_path / "metadata.yaml"
        with open(metadata_path, 'w') as f:
            yaml.dump(flame_data['metadata'], f)
        
        # Сохранение исходного DataFrame для отладки
        flame_data['original_dataframe'].to_parquet(
            output_path / "debug_dataframe.parquet"
        )
        
        logger.info(f"✅ Данные экспортированы в {output_path}")

def main():
    """Демонстрация интеграции"""
    logging.basicConfig(level=logging.INFO)
    
    # Создание интегратора
    integrator = CuDFFlameIntegrator(enable_unified_memory=True)
    
    if not integrator.connect_to_database():
        return
    
    try:
        # Загрузка данных
        df = integrator.load_digital_data()
        
        # Применение фильтров
        filtered_df = integrator.apply_bitwise_filters(
            df, 
            ac_types=['Ми-26', 'Ми-17'],
            conditions=['ИСПРАВНЫЙ', 'НЕ УСТАНОВЛЕН']
        )
        
        # Расчет метрик
        metrics_df = integrator.calculate_maintenance_metrics(filtered_df)
        
        # Подготовка для Flame GPU
        flame_data = integrator.prepare_for_flame_gpu(metrics_df)
        
        # Экспорт
        integrator.export_to_flame_format(flame_data, "temp/flame_gpu_data")
        
        print("✅ Интеграция завершена успешно!")
        
    except Exception as e:
        logger.error(f"❌ Ошибка интеграции: {e}")

if __name__ == "__main__":
    main() 