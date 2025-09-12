#!/usr/bin/env python3
"""
FLAME GPU MacroProperty1 Exporter - экспорт для визуального контроля
Экспортирует MacroProperty1 из FLAME GPU в постоянную таблицу ClickHouse
для визуального контроля и анализа пользователем
"""

import logging
import sys
import os
import json
from typing import Dict, List, Tuple, Any
from datetime import datetime, timedelta, date

# Добавляем путь к utils
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))

from config_loader import get_clickhouse_client
from flame_macroproperty1_loader import FlameMacroProperty1Loader

# Импорт настоящего FLAME GPU
import pyflamegpu

def setup_logging() -> logging.Logger:
    """Настройка логирования"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/flame_macroproperty1_exporter.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

class FlameMacroProperty1Exporter:
    """Экспортер MacroProperty1 из FLAME GPU в ClickHouse для визуального контроля"""
    
    def __init__(self, client=None):
        self.client = client or get_clickhouse_client()
        self.logger = setup_logging()
        self.export_table = "flame_macroproperty1_export"
        
    def create_export_table(self, field_mapping: Dict[str, int], field_types: Dict[str, str]) -> None:
        """Создание постоянной таблицы для экспорта MacroProperty1"""
        self.logger.info("🏗️ Создание постоянной таблицы для экспорта MacroProperty1...")
        
        try:
            # Удаляем старую таблицу если есть
            self.client.execute(f"DROP TABLE IF EXISTS {self.export_table}")
            
            # Создаем поля для таблицы (простая структура для перезаписи)
            fields_ddl = ["record_id UInt32"]
            
            # Добавляем поля с именами и field_id комментариями
            for field_name, field_id in sorted(field_mapping.items(), key=lambda x: x[1]):
                ch_type = field_types.get(field_name, 'String')
                field_ddl = f"{field_name} {ch_type} COMMENT 'field_id: {field_id}'"
                fields_ddl.append(field_ddl)
                self.logger.info(f"   📋 {field_name} (field_id: {field_id}): {ch_type}")
            
            # Создаем простую таблицу для перезаписи
            create_query = f"""
                CREATE TABLE {self.export_table} (
                    {', '.join(fields_ddl)}
                ) ENGINE = Memory
                COMMENT 'Экспорт MacroProperty1 из FLAME GPU (перезаписываемая таблица)'
            """
            
            self.client.execute(create_query)
            self.logger.info(f"✅ Постоянная таблица {self.export_table} создана")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания таблицы экспорта: {e}")
            raise

    def export_flame_environment_to_table(self, sim: pyflamegpu.CUDASimulation, 
                                        field_mapping: Dict[str, int], 
                                        total_records: int) -> None:
        """Экспорт данных из FLAME GPU Environment в постоянную таблицу ClickHouse"""
        self.logger.info("📤 Экспорт MacroProperty1 из FLAME GPU в постоянную таблицу...")
        
        try:
            # Подготавливаем данные для экспорта
            export_data = []
            flame_version = pyflamegpu.VERSION_FULL
            export_timestamp = datetime.now()
            
            self.logger.info(f"📊 Экспортируем {total_records} записей из FLAME GPU...")
            self.logger.info(f"🔥 FLAME GPU версия: {flame_version}")
            
            for record_id in range(total_records):
                row = [record_id]  # Только record_id
                
                # Экспортируем каждое поле по оригинальному имени (не field_id)
                for field_name, field_id in sorted(field_mapping.items(), key=lambda x: x[1]):
                    property_name = f"field_{field_id}"
                    
                    try:
                        # Получаем Property Array из FLAME GPU Environment
                        property_array = sim.getEnvironmentPropertyArrayUInt32(property_name)
                        
                        # Получаем значение для конкретной записи
                        if record_id < len(property_array):
                            value = property_array[record_id]
                        else:
                            value = 0  # Дефолтное значение если вышли за границы
                        
                        row.append(value)
                        
                    except Exception as prop_e:
                        # Пробуем другие типы данных
                        try:
                            property_array = sim.getEnvironmentPropertyArrayUInt16(property_name)
                            value = property_array[record_id] if record_id < len(property_array) else 0
                            row.append(value)
                        except:
                            try:
                                property_array = sim.getEnvironmentPropertyArrayUInt8(property_name)
                                value = property_array[record_id] if record_id < len(property_array) else 0
                                row.append(value)
                            except:
                                try:
                                    property_array = sim.getEnvironmentPropertyArrayFloat(property_name)
                                    value = property_array[record_id] if record_id < len(property_array) else 0.0
                                    row.append(value)
                                except:
                                    # Если не удалось получить, ставим 0
                                    self.logger.warning(f"⚠️ Не удалось получить {property_name} для {field_name}: {prop_e}")
                                    row.append(0)
                
                export_data.append(row)
            
            # Подготавливаем список полей для INSERT
            field_list = ["record_id"]
            for field_name, field_id in sorted(field_mapping.items(), key=lambda x: x[1]):
                field_list.append(field_name)  # Используем оригинальные имена полей

            # Приведение типов под схему таблицы (String/Nullable(String), Date/Nullable(Date))
            try:
                schema = self.client.execute(
                    f"SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = '{self.export_table}'"
                )
                type_by_name = {name: ctype for name, ctype in schema}
                string_indices = [i for i, col in enumerate(field_list) if 'String' in (type_by_name.get(col, '') or '')]
                date_indices = [i for i, col in enumerate(field_list) if 'Date' in (type_by_name.get(col, '') or '')]
                if string_indices or date_indices:
                    epoch = date(1970, 1, 1)
                    for row in export_data:
                        # String: приводим к строкам
                        for idx in string_indices:
                            if idx < len(row):
                                val = row[idx]
                                row[idx] = '' if val is None else str(val)
                        # Date: конвертируем из дней с эпохи в date
                        for idx in date_indices:
                            if idx < len(row):
                                val = row[idx]
                                if val is None:
                                    row[idx] = None
                                elif isinstance(val, date):
                                    row[idx] = val
                                elif isinstance(val, datetime):
                                    row[idx] = val.date()
                                else:
                                    try:
                                        row[idx] = epoch + timedelta(days=int(val))
                                    except Exception:
                                        row[idx] = None
            except Exception as type_e:
                self.logger.warning(f"⚠️ Не удалось привести типы по схеме export-таблицы: {type_e}. Пробуем вставку как есть")

            # Вставляем данные в ClickHouse
            insert_query = f"INSERT INTO {self.export_table} ({', '.join(field_list)}) VALUES"

            self.logger.info(f"💾 Вставляем {len(export_data)} записей в постоянную таблицу...")
            self.client.execute(insert_query, export_data)
            self.logger.info(f"✅ Экспорт завершен: {len(export_data)} записей в таблицу {self.export_table}")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка экспорта из FLAME GPU: {e}")
            raise

    def generate_export_summary(self, field_mapping: Dict[str, int], 
                               total_records: int,
                               loader_stats: Dict[str, Any] = None) -> str:
        """Генерация сводки экспорта MacroProperty1"""
        self.logger.info("📋 Генерация сводки экспорта MacroProperty1...")
        
        try:
            report_lines = []
            report_lines.append("=" * 60)
            report_lines.append("📤 СВОДКА ЭКСПОРТА FLAME GPU MACROPROPERTY1")
            report_lines.append("=" * 60)
            report_lines.append("")
            
            # Общая информация
            report_lines.append("📊 ОБЩАЯ ИНФОРМАЦИЯ:")
            report_lines.append(f"   • Дата экспорта: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            report_lines.append(f"   • Таблица ClickHouse: {self.export_table}")
            report_lines.append(f"   • Записей экспортировано: {total_records:,}")
            report_lines.append(f"   • Полей MacroProperty1: {len(field_mapping)}")
            report_lines.append("")
            
            # FLAME GPU информация
            if loader_stats:
                report_lines.append("🔥 FLAME GPU ИНФОРМАЦИЯ:")
                report_lines.append(f"   • Версия FLAME GPU: {pyflamegpu.VERSION_FULL}")
                report_lines.append(f"   • Модель: Heli_Rotable")
                report_lines.append(f"   • Property Arrays: {loader_stats.get('loaded_macroproperties', 'N/A')}")
                report_lines.append(f"   • Время загрузки: {loader_stats.get('loading_time', 'N/A')} сек")
                report_lines.append("")
            
            # Структура полей
            report_lines.append("🎯 СТРУКТУРА ЭКСПОРТИРОВАННЫХ ПОЛЕЙ:")
            report_lines.append(f"   {'Поле':<20} {'field_id':<10} {'Тип':<15} {'Описание'}")
            report_lines.append(f"   {'-'*20} {'-'*10} {'-'*15} {'-'*30}")
            
            field_descriptions = {
                'comp_number': 'Номер компонента',
                'group_by': 'Группа агрегата', 
                'type_restricted': 'Ограничение по типу',
                'partno_comp': 'ID номера детали',
                'assembly_time': 'Время сборки (дни)',
                'repair_time': 'Время ремонта (дни)',
                'partout_time': 'Время списания (дни)',
                'br_mi8': 'BR Ми‑8 (минуты)',
                'br_mi17': 'BR Ми‑17 (минуты)',
                'll_mi8': 'LL МИ-8 (часы)',
                'll_mi17': 'LL МИ-17 (часы)', 
                'oh_mi8': 'OH МИ-8 (часы)',
                'oh_mi17': 'OH МИ-17 (часы)',
                'oh_threshold_mi8': 'OH порог МИ-8 (часы)',
                'repair_price': 'Стоимость ремонта',
                'purchase_price': 'Стоимость закупки',
                'restrictions_mask': 'Маска ограничений',
                'trigger_interval': 'Интервал триггера',
                'common_restricted1': 'Общее ограничение 1',
                'common_restricted2': 'Общее ограничение 2'
            }
            
            for field_name, field_id in sorted(field_mapping.items(), key=lambda x: x[1]):
                description = field_descriptions.get(field_name, 'Поле MacroProperty1')
                # Получаем тип из таблицы
                try:
                    type_query = f"""
                        SELECT type FROM system.columns 
                        WHERE database = 'default' 
                        AND table = '{self.export_table}' 
                        AND name = '{field_name}'
                    """
                    type_result = self.client.execute(type_query)
                    field_type = type_result[0][0] if type_result else 'Unknown'
                except:
                    field_type = 'Unknown'
                
                report_lines.append(f"   {field_name:<20} {field_id:<10} {field_type:<15} {description}")
            
            report_lines.append("")
            
            # SQL запросы для анализа
            report_lines.append("💡 РЕКОМЕНДУЕМЫЕ SQL ЗАПРОСЫ ДЛЯ АНАЛИЗА:")
            report_lines.append("")
            report_lines.append("1. Просмотр всех данных:")
            report_lines.append(f"   SELECT * FROM {self.export_table} ORDER BY record_id;")
            report_lines.append("")
            report_lines.append("2. Сравнение с исходной таблицей:")
            report_lines.append(f"   SELECT orig.partno_comp, orig.comp_number, exp.comp_number")
            report_lines.append(f"   FROM md_components orig")
            report_lines.append(f"   JOIN {self.export_table} exp ON orig.partno_comp = exp.partno_comp")
            report_lines.append(f"   ORDER BY orig.partno_comp;")
            report_lines.append("")
            report_lines.append("3. Проверка field_id маппинга:")
            report_lines.append(f"   SELECT name, comment FROM system.columns")
            report_lines.append(f"   WHERE table = '{self.export_table}' AND comment LIKE '%field_id%';")
            report_lines.append("")
            
            report_lines.append("=" * 60)
            
            # Сохраняем сводку
            report_content = "\n".join(report_lines)
            
            os.makedirs('temp_data', exist_ok=True)
            report_file = f'temp_data/flame_macroproperty1_export_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report_content)
            
            self.logger.info(f"✅ Сводка экспорта сохранена: {report_file}")
            
            # Выводим сводку в лог
            for line in report_lines:
                self.logger.info(line)
            
            return report_file
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка генерации сводки: {e}")
            raise

    def export_macroproperty1_for_review(self) -> str:
        """Полный экспорт MacroProperty1 для визуального контроля"""
        self.logger.info("🚀 Начало экспорта MacroProperty1 для визуального контроля")
        
        # Инициализируем переменные для управления ресурсами
        loader = None
        sim = None
        field_mapping = {}
        field_types = {}
        total_records = 0
        
        try:
            # Шаг 1: Создаем загрузчик и загружаем данные в FLAME GPU
            self.logger.info("📥 Шаг 1: Создание загрузчика и загрузка данных в FLAME GPU...")
            loader = FlameMacroProperty1Loader()
            
            # КРИТИЧНО: Загружаем данные и СРАЗУ экспортируем, пока они в памяти
            # Получаем все компоненты по отдельности без вызова complete() который освобождает ресурсы
            self.logger.info("🔍 Получение field_id маппинга...")
            field_mapping = loader.get_field_mapping_from_clickhouse()
            
            self.logger.info("📊 Анализ типов данных...")
            field_types = loader.get_field_types_from_clickhouse()
            
            self.logger.info("📥 Загрузка данных из ClickHouse...")
            data, field_order = loader.load_md_components_from_clickhouse()
            total_records = len(data)
            
            self.logger.info("🏗️ Создание FLAME GPU модели...")
            model = loader.create_flame_gpu_model()
            
            self.logger.info("🌍 Создание Environment...")
            env = loader.create_macroproperty1_environment(field_mapping, field_types)
            
            self.logger.info("🚀 Создание CUDASimulation...")
            sim = loader.create_flame_gpu_simulation()
            
            self.logger.info("📥 Загрузка данных в FLAME GPU...")
            loader.load_data_into_macroproperty1(data, field_order, field_mapping, field_types)
            
            self.logger.info(f"✅ Данные загружены в FLAME GPU: {total_records} записей, {len(field_mapping)} полей")
            self.logger.info("🔒 СРАЗУ экспортируем, пока данные в памяти FLAME GPU")
            
            # Шаг 2: Создаем постоянную таблицу
            self.logger.info("🏗️ Шаг 2: Создание постоянной таблицы экспорта...")
            self.create_export_table(field_mapping, field_types)
            
            # Шаг 3: НЕМЕДЛЕННО экспортируем из FLAME GPU в постоянную таблицу
            self.logger.info("📤 Шаг 3: НЕМЕДЛЕННЫЙ экспорт из FLAME GPU в постоянную таблицу...")
            self.export_flame_environment_to_table(sim, field_mapping, total_records)
            
            # Шаг 4: Генерируем сводку
            self.logger.info("📋 Шаг 4: Генерация сводки экспорта...")
            loader_stats = loader.stats if loader else {}
            report_file = self.generate_export_summary(field_mapping, total_records, loader_stats)
            
            self.logger.info("✅ Экспорт MacroProperty1 для визуального контроля завершен")
            self.logger.info(f"📊 Данные доступны в таблице: {self.export_table}")
            
            return report_file
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка экспорта MacroProperty1: {e}")
            raise
            
        finally:
            # Освобождаем FLAME GPU ресурсы в конце
            self.logger.info("🗑️ Освобождение FLAME GPU ресурсов...")
            sim = None
            loader = None

def main():
    """Основная функция"""
    logger = setup_logging()
    
    try:
        logger.info("📤 Запуск экспорта FLAME GPU MacroProperty1 для визуального контроля...")
        
        exporter = FlameMacroProperty1Exporter()
        report_file = exporter.export_macroproperty1_for_review()
        
        logger.info(f"🎉 Экспорт завершен! Сводка: {report_file}")
        logger.info(f"📊 Данные доступны в ClickHouse таблице: {exporter.export_table}")
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 