#!/usr/bin/env python3
"""
FLAME GPU MacroProperty1 Validator - валидация качества загрузки
Экспортирует MacroProperty1 из FLAME GPU обратно в ClickHouse 
и сравнивает с исходной таблицей md_components для проверки целостности данных
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
            logging.FileHandler('logs/flame_macroproperty1_validator.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

class FlameMacroProperty1Validator:
    """Валидатор качества загрузки MacroProperty1 в FLAME GPU"""
    
    def __init__(self, client=None):
        self.client = client or get_clickhouse_client()
        self.logger = setup_logging()
        self.validation_table = "test_flame_macroproperty1_roundtrip"
        
    def create_validation_table(self, field_mapping: Dict[str, int], field_types: Dict[str, str]) -> None:
        """Создание таблицы для roundtrip валидации"""
        self.logger.info("🏗️ Создание таблицы валидации FLAME GPU экспорта...")
        
        try:
            # Удаляем старую таблицу если есть
            self.client.execute(f"DROP TABLE IF EXISTS {self.validation_table}")
            
            # Создаем поля для таблицы
            fields_ddl = ["record_id UInt32"]
            
            for field_name, field_id in sorted(field_mapping.items(), key=lambda x: x[1]):
                ch_type = field_types.get(field_name, 'String')
                field_ddl = f"field_{field_id} {ch_type}"
                fields_ddl.append(field_ddl)
                self.logger.info(f"   📋 field_{field_id} ({field_name}): {ch_type}")
            
            # Создаем таблицу
            create_query = f"""
                CREATE TABLE {self.validation_table} (
                    {', '.join(fields_ddl)}
                ) ENGINE = Memory
            """
            
            self.client.execute(create_query)
            self.logger.info(f"✅ Таблица валидации {self.validation_table} создана")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания таблицы валидации: {e}")
            raise

    def export_flame_environment_to_clickhouse(self, sim: pyflamegpu.CUDASimulation, 
                                             field_mapping: Dict[str, int], 
                                             total_records: int) -> None:
        """Экспорт данных из FLAME GPU Environment обратно в ClickHouse"""
        self.logger.info("📤 Экспорт данных из FLAME GPU Environment в ClickHouse...")
        
        try:
            # Подготавливаем данные для экспорта
            export_data = []
            
            self.logger.info(f"📊 Экспортируем {total_records} записей из FLAME GPU...")
            
            for record_id in range(total_records):
                row = [record_id]  # record_id
                
                # Экспортируем каждое поле по field_id
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
                                    self.logger.warning(f"⚠️ Не удалось получить {property_name}: {prop_e}")
                                    row.append(0)
                
                export_data.append(row)
            
            # Подготавливаем список полей для INSERT
            field_list = ["record_id"]
            for field_id in sorted(field_mapping.values()):
                field_list.append(f"field_{field_id}")
            
            # Приведение типов под схему таблицы (String/Nullable(String), Date/Nullable(Date))
            try:
                schema = self.client.execute(
                    f"SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = '{self.validation_table}'"
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
                self.logger.warning(f"⚠️ Не удалось привести типы по схеме валидационной таблицы: {type_e}. Пробуем вставку как есть")
            
            # Вставляем данные в ClickHouse
            insert_query = f"INSERT INTO {self.validation_table} ({', '.join(field_list)}) VALUES"
            
            self.logger.info(f"💾 Вставляем {len(export_data)} записей в ClickHouse...")
            self.client.execute(insert_query, export_data)
            self.logger.info(f"✅ Экспорт завершен: {len(export_data)} записей")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка экспорта из FLAME GPU: {e}")
            raise

    def compare_original_vs_exported(self, field_mapping: Dict[str, int]) -> Dict[str, Any]:
        """Сравнение исходных данных md_components с экспортированными из FLAME GPU"""
        self.logger.info("🔍 Сравнение исходных данных с экспортированными из FLAME GPU...")
        
        try:
            # Загружаем исходные данные
            original_query = """
                SELECT * FROM md_components 
                WHERE (version_date, version_id) = (
                    SELECT version_date, version_id 
                    FROM md_components 
                    ORDER BY version_date DESC, version_id DESC 
                    LIMIT 1
                )
                ORDER BY partno_comp
            """
            original_data = self.client.execute(original_query)
            
            # Загружаем экспортированные данные
            exported_query = f"SELECT * FROM {self.validation_table} ORDER BY record_id"
            exported_data = self.client.execute(exported_query)
            
            self.logger.info(f"📊 Исходных записей: {len(original_data)}")
            self.logger.info(f"📊 Экспортированных записей: {len(exported_data)}")
            
            # Получаем порядок полей
            original_fields_query = """
                SELECT name, position 
                FROM system.columns 
                WHERE database = 'default' AND table = 'md_components'
                ORDER BY position
            """
            original_field_order = [name for name, pos in self.client.execute(original_fields_query)]
            
            exported_fields_query = f"""
                SELECT name, position 
                FROM system.columns 
                WHERE database = 'default' AND table = '{self.validation_table}'
                ORDER BY position
            """
            exported_field_order = [name for name, pos in self.client.execute(exported_fields_query)]
            
            # Результаты сравнения
            comparison_results = {
                'total_original_records': len(original_data),
                'total_exported_records': len(exported_data),
                'record_count_match': len(original_data) == len(exported_data),
                'field_comparisons': {},
                'overall_success': True,
                'mismatches': [],
                'flame_gpu_test': 'PASSED' if len(original_data) == len(exported_data) else 'FAILED'
            }
            
            # Сравниваем поле за полем
            for field_name, field_id in field_mapping.items():
                if field_name in original_field_order:
                    original_col_idx = original_field_order.index(field_name)
                    exported_field_name = f"field_{field_id}"
                    
                    if exported_field_name in exported_field_order:
                        exported_col_idx = exported_field_order.index(exported_field_name)
                        
                        matches = 0
                        mismatches = 0
                        sample_mismatches = []
                        null_count_original = 0
                        null_count_exported = 0
                        
                        min_records = min(len(original_data), len(exported_data))
                        
                        for i in range(min_records):
                            original_val = original_data[i][original_col_idx]
                            exported_val = exported_data[i][exported_col_idx]
                            
                            # Считаем NULL значения
                            if original_val is None:
                                null_count_original += 1
                                original_val = 0  # NULL в исходных данных = 0 в FLAME GPU
                            
                            if exported_val is None:
                                null_count_exported += 1
                            
                            if original_val == exported_val:
                                matches += 1
                            else:
                                mismatches += 1
                                if len(sample_mismatches) < 5:
                                    sample_mismatches.append({
                                        'record_id': i,
                                        'original': original_val,
                                        'exported': exported_val
                                    })
                        
                        field_success = mismatches == 0
                        comparison_results['field_comparisons'][field_name] = {
                            'field_id': field_id,
                            'matches': matches,
                            'mismatches': mismatches,
                            'success': field_success,
                            'null_handling': {
                                'original_nulls': null_count_original,
                                'exported_nulls': null_count_exported,
                                'null_conversion_ok': null_count_exported == 0
                            },
                            'sample_mismatches': sample_mismatches
                        }
                        
                        if not field_success:
                            comparison_results['overall_success'] = False
                            comparison_results['mismatches'].append(field_name)
                        
                        status = "✅" if field_success else "❌"
                        null_info = f"(NULL: {null_count_original}→0)" if null_count_original > 0 else ""
                        self.logger.info(f"   {status} {field_name} (field_{field_id}): "
                                       f"{matches} совпадений, {mismatches} расхождений {null_info}")
            
            return comparison_results
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка сравнения данных: {e}")
            raise

    def generate_validation_report(self, comparison_results: Dict[str, Any], 
                                 loader_stats: Dict[str, Any] = None) -> str:
        """Генерация отчета валидации FLAME GPU"""
        self.logger.info("📋 Генерация отчета валидации FLAME GPU...")
        
        try:
            report_lines = []
            report_lines.append("=" * 70)
            report_lines.append("🔥 ОТЧЕТ ВАЛИДАЦИИ FLAME GPU MACROPROPERTY1")
            report_lines.append("=" * 70)
            report_lines.append("")
            
            # Общая статистика
            report_lines.append("📊 ОБЩАЯ СТАТИСТИКА:")
            report_lines.append(f"   • Исходных записей в md_components: {comparison_results['total_original_records']:,}")
            report_lines.append(f"   • Загружено в FLAME GPU: {comparison_results['total_exported_records']:,}")
            report_lines.append(f"   • Соответствие количества записей: {'✅' if comparison_results['record_count_match'] else '❌'}")
            report_lines.append(f"   • FLAME GPU тест: {comparison_results['flame_gpu_test']} 🔥")
            report_lines.append(f"   • Общий результат валидации: {'✅ УСПЕШНО' if comparison_results['overall_success'] else '❌ ОШИБКИ'}")
            report_lines.append("")
            
            # Статистика FLAME GPU из загрузчика
            if loader_stats:
                report_lines.append("🔥 FLAME GPU СТАТИСТИКА:")
                report_lines.append(f"   • Версия FLAME GPU: {loader_stats.get('flame_version', 'N/A')}")
                report_lines.append(f"   • Property Arrays создано: {loader_stats.get('loaded_macroproperties', 'N/A')}")
                report_lines.append(f"   • Время загрузки: {loader_stats.get('loading_time', 'N/A')} сек")
                report_lines.append("")
            
            # Результаты по полям
            report_lines.append("🎯 РЕЗУЛЬТАТЫ ПО ПОЛЯМ:")
            
            success_count = 0
            total_fields = len(comparison_results['field_comparisons'])
            null_conversions = 0
            
            for field_name, result in comparison_results['field_comparisons'].items():
                status = "✅" if result['success'] else "❌"
                field_id = result['field_id']
                matches = result['matches']
                mismatches = result['mismatches']
                
                # Информация о NULL конверсиях
                null_info = ""
                if result['null_handling']['original_nulls'] > 0:
                    original_nulls = result['null_handling']['original_nulls']
                    null_info = f" (NULL→0: {original_nulls})"
                    null_conversions += 1
                
                report_lines.append(f"   {status} field_{field_id} ({field_name}): "
                                   f"{matches:,} совпадений, {mismatches:,} расхождений{null_info}")
                
                if not result['success'] and result['sample_mismatches']:
                    report_lines.append(f"      💥 Примеры расхождений:")
                    for sample in result['sample_mismatches'][:3]:
                        report_lines.append(f"         Запись {sample['record_id']}: "
                                           f"исходное='{sample['original']}', "
                                           f"FLAME GPU='{sample['exported']}'")
                
                if result['success']:
                    success_count += 1
            
            report_lines.append("")
            report_lines.append(f"📈 ИТОГО: {success_count}/{total_fields} полей прошли валидацию")
            
            if null_conversions > 0:
                report_lines.append(f"🔄 NULL КОНВЕРСИИ: {null_conversions} полей с NULL→0 преобразованием")
            
            # Проблемы или успех
            if comparison_results['mismatches']:
                report_lines.append("")
                report_lines.append("❌ ОБНАРУЖЕННЫЕ ПРОБЛЕМЫ:")
                for field_name in comparison_results['mismatches']:
                    report_lines.append(f"   • Расхождения в поле: {field_name}")
                
                report_lines.append("")
                report_lines.append("💡 РЕКОМЕНДАЦИИ:")
                report_lines.append("   • Проверить обработку NULL значений в FLAME GPU")
                report_lines.append("   • Исследовать расхождения в указанных полях")
                report_lines.append("   • Проверить корректность типов данных и конверсий")
            else:
                report_lines.append("")
                report_lines.append("🎉 ОТЛИЧНО! Все поля прошли FLAME GPU валидацию успешно! 🔥")
                report_lines.append("✅ MacroProperty1 корректно загружен и готов для использования")
            
            report_lines.append("")
            report_lines.append("=" * 70)
            
            # Сохраняем отчет
            report_content = "\n".join(report_lines)
            
            os.makedirs('temp_data', exist_ok=True)
            report_file = f'temp_data/flame_macroproperty1_validation_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report_content)
            
            self.logger.info(f"✅ Отчет валидации FLAME GPU сохранен: {report_file}")
            
            # Выводим отчет в лог
            for line in report_lines:
                self.logger.info(line)
            
            return report_file
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка генерации отчета: {e}")
            raise

    def cleanup_validation_table(self) -> None:
        """Очистка таблицы валидации"""
        try:
            self.client.execute(f"DROP TABLE IF EXISTS {self.validation_table}")
            self.logger.info(f"🗑️ Таблица валидации {self.validation_table} удалена")
        except Exception as e:
            self.logger.warning(f"⚠️ Ошибка удаления таблицы валидации: {e}")

    def validate_flame_macroproperty1(self, cleanup: bool = True) -> str:
        """Полная валидация FLAME GPU MacroProperty1 через roundtrip тест"""
        self.logger.info("🚀 Начало полной валидации FLAME GPU MacroProperty1")
        
        # Инициализируем переменные
        loader = None
        sim = None
        field_mapping = {}
        field_types = {}
        total_records = 0
        
        try:
            # Шаг 1: Создаем загрузчик и загружаем данные в FLAME GPU
            self.logger.info("📥 Шаг 1: Создание загрузчика и загрузка данных в FLAME GPU...")
            loader = FlameMacroProperty1Loader()
            
            # КРИТИЧНО: Сохраняем ссылку на simulation чтобы данные не освободились
            sim, field_mapping, field_types, total_records = loader.load_macroproperty1_complete()
            
            self.logger.info(f"✅ Данные загружены в FLAME GPU: {total_records} записей, {len(field_mapping)} полей")
            self.logger.info("🔒 Simulation объект сохранен в памяти для валидации")
            
            # Шаг 2: Создаем таблицу валидации
            self.logger.info("🏗️ Шаг 2: Создание таблицы валидации...")
            self.create_validation_table(field_mapping, field_types)
            
            # Шаг 3: Экспортируем из FLAME GPU в ClickHouse (сразу, пока данные в памяти)
            self.logger.info("📤 Шаг 3: Экспорт из FLAME GPU в ClickHouse...")
            self.export_flame_environment_to_clickhouse(sim, field_mapping, total_records)
            
            # Шаг 4: Сравниваем данные
            self.logger.info("🔍 Шаг 4: Сравнение исходных и экспортированных данных...")
            comparison_results = self.compare_original_vs_exported(field_mapping)
            
            # Шаг 5: Генерируем отчет
            self.logger.info("📋 Шаг 5: Генерация отчета валидации...")
            loader_stats = loader.stats if loader else {}
            report_file = self.generate_validation_report(comparison_results, loader_stats)
            
            # Шаг 6: Очистка
            if cleanup:
                self.cleanup_validation_table()
            
            # Явно освобождаем FLAME GPU ресурсы
            self.logger.info("🗑️ Освобождение FLAME GPU ресурсов...")
            sim = None  # Освобождаем ссылку на simulation
            loader = None  # Освобождаем загрузчик
            
            self.logger.info("✅ Полная валидация FLAME GPU MacroProperty1 завершена")
            return report_file
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка валидации FLAME GPU MacroProperty1: {e}")
            
            # Очистка при ошибке
            if cleanup:
                self.cleanup_validation_table()
            
            # Освобождаем ресурсы при ошибке
            sim = None
            loader = None
            
            raise

def main():
    """Основная функция"""
    logger = setup_logging()
    
    try:
        logger.info("🔥 Запуск FLAME GPU MacroProperty1 валидации...")
        
        validator = FlameMacroProperty1Validator()
        report_file = validator.validate_flame_macroproperty1(cleanup=True)
        
        logger.info(f"🎉 FLAME GPU валидация завершена! Отчет: {report_file}")
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 