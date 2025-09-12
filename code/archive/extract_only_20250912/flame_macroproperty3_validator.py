#!/usr/bin/env python3
"""
FLAME GPU MacroProperty3 Validator - валидация качества загрузки
Экспортирует MacroProperty3 из FLAME GPU обратно в ClickHouse 
и сравнивает с исходной таблицей heli_pandas для проверки целостности данных
Работает с 7113 агентами и 14 полями из аналитики
"""

import logging
import sys
import os
import json
from typing import Dict, List, Tuple, Any
from datetime import datetime

# Добавляем путь к модулям проекта
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

try:
    import pyflamegpu
except ImportError as e:
    print(f"❌ Ошибка импорта pyflamegpu: {e}")
    print("💡 Убедитесь что FLAME GPU установлен: pip install pyflamegpu")
    sys.exit(1)

from utils.config_loader import get_clickhouse_client
from flame_macroproperty3_loader import FlameMacroProperty3Loader

def setup_logging() -> logging.Logger:
    """Настройка логирования"""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, f"flame_macroproperty3_validator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"🔍 Запуск FLAME GPU MacroProperty3 Validator")
    logger.info(f"📋 Лог файл: {log_file}")
    
    return logger

class FlameMacroProperty3Validator:
    """Валидатор качества загрузки MacroProperty3 в FLAME GPU через roundtrip тест"""
    
    def __init__(self):
        self.logger = setup_logging()
        self.client = None
        self.original_table = "heli_pandas"
        self.validation_table = "flame_macroproperty3_validation"
        
        # Устанавливаем соединение с ClickHouse
        try:
            self.client = get_clickhouse_client()
            self.logger.info("✅ Соединение с ClickHouse установлено")
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения к ClickHouse: {e}")
            raise

    def create_validation_table(self, field_mapping: Dict[str, int], field_types: Dict[str, str]) -> None:
        """Создание временной таблицы для валидации"""
        self.logger.info("🏗️ Создание временной таблицы для валидации MacroProperty3...")
        
        try:
            # Удаляем существующую таблицу
            self.client.execute(f"DROP TABLE IF EXISTS {self.validation_table}")
            
            # Создаем DDL только для полей из аналитики MacroProperty3
            analytics_fields = [
                'partseqno_i', 'psn', 'address_i', 'lease_restricted', 'group_by', 'status_id',
                'aircraft_number', 'ac_type_mask', 'll', 'oh', 'oh_threshold',
                'sne', 'ppr', 'repair_days', 'mfg_date'
            ]
            # Ограничиваемся только реально существующими в heli_pandas и имеющимися в маппинге
            try:
                ch_schema = self.client.execute("DESCRIBE TABLE heli_pandas")
                existing_fields = {row[0] for row in ch_schema}
            except Exception:
                existing_fields = set()
            available_fields = [f for f in analytics_fields if (f in field_mapping and f in existing_fields)]
            
            fields_ddl = ["record_id UInt32"]
            
            for field_name in available_fields:
                if field_name in field_mapping:
                    field_id = field_mapping[field_name]
                    ch_type = field_types.get(field_name, 'String')
                    
                    # Убираем Nullable для валидационной таблицы
                    if 'Nullable(' in ch_type:
                        ch_type = ch_type.replace('Nullable(', '').replace(')', '')
                    
                    # Преобразуем Date в UInt16 для FLAME GPU совместимости
                    if ch_type == 'Date':
                        ch_type = 'UInt16'
                    
                    field_ddl = f"{field_name} {ch_type} COMMENT 'field_id: {field_id}'"
                    fields_ddl.append(field_ddl)
                    self.logger.info(f"   📋 {field_name} (field_id: {field_id}): {ch_type}")
            
            create_query = f"""
                CREATE TABLE {self.validation_table} (
                    {', '.join(fields_ddl)}
                ) ENGINE = MergeTree()
                ORDER BY record_id
                COMMENT 'Временная таблица для валидации MacroProperty3 из FLAME GPU'
            """
            
            self.client.execute(create_query)
            self.logger.info(f"✅ Временная таблица {self.validation_table} создана")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания таблицы валидации: {e}")
            raise

    def export_flame_environment_to_clickhouse(self, sim: pyflamegpu.CUDASimulation, 
                                             field_mapping: Dict[str, int], total_records: int) -> None:
        """Экспорт MacroProperty3 из FLAME GPU Environment в ClickHouse"""
        self.logger.info("📤 Экспорт MacroProperty3 из FLAME GPU в ClickHouse...")
        
        try:
            # Поля из аналитики MacroProperty3
            analytics_fields = [
                'partseqno_i', 'psn', 'address_i', 'lease_restricted', 'group_by', 'status_id',
                'aircraft_number', 'ac_type_mask', 'll', 'oh', 'oh_threshold',
                'sne', 'ppr', 'repair_days', 'mfg_date'
            ]
            # Ограничиваемся реально доступными полями
            try:
                ch_schema = self.client.execute("DESCRIBE TABLE heli_pandas")
                existing_fields = {row[0] for row in ch_schema}
            except Exception:
                existing_fields = set()
            available_fields = [f for f in analytics_fields if (f in field_mapping and f in existing_fields)]
            
            # Получение методов экспорта
            get_methods = {
                'UInt32': 'getEnvironmentPropertyArrayUInt32',
                'UInt16': 'getEnvironmentPropertyArrayUInt16',
                'UInt8': 'getEnvironmentPropertyArrayUInt8',
                'Float32': 'getEnvironmentPropertyArrayFloat',
                'Float64': 'getEnvironmentPropertyArrayDouble'
            }
            
            # Сбор данных из FLAME GPU Environment
            exported_data = {}
            
            for field_name in available_fields:
                if field_name in field_mapping:
                    field_id = field_mapping[field_name]
                    property_name = f"field_{field_id}"
                    
                    # Определение метода получения на основе типа FLAME GPU
                    get_method = None
                    if field_name in ['partseqno_i', 'psn', 'aircraft_number', 'll', 'oh', 'oh_threshold', 'sne', 'ppr']:
                        get_method = 'getEnvironmentPropertyArrayUInt32'
                    elif field_name in ['address_i', 'repair_days', 'mfg_date']:
                        get_method = 'getEnvironmentPropertyArrayUInt16'
                    elif field_name in ['lease_restricted', 'status_id', 'ac_type_mask', 'group_by']:
                        get_method = 'getEnvironmentPropertyArrayUInt8'
                    else:
                        get_method = 'getEnvironmentPropertyArrayUInt32'  # По умолчанию
                    
                    if get_method and hasattr(sim, get_method):
                        method_func = getattr(sim, get_method)
                        data_array = method_func(property_name)
                        exported_data[field_name] = list(data_array)
                        self.logger.info(f"   ✅ Экспортировано {len(data_array)} значений из {property_name} ({field_name})")
                    else:
                        self.logger.warning(f"   ⚠️ Метод {get_method} не найден для {field_name}")
            
            # Подготовка данных для вставки
            records = []
            for i in range(total_records):
                record = [i]  # record_id
                for field_name in available_fields:
                    if field_name in exported_data:
                        value = exported_data[field_name][i] if i < len(exported_data[field_name]) else 0
                        # Все значения из FLAME GPU уже являются числами (UInt16/UInt32)
                        record.append(value)
                    else:
                        record.append(0)  # Значение по умолчанию
                records.append(record)
            
            # Вставка в ClickHouse
            field_names = ['record_id'] + available_fields
            field_list = ", ".join(field_names)
            
            # Отладочная информация для первой записи
            if records:
                self.logger.info(f"🔍 Первая запись для отладки: {records[0][:5]}...")
                self.logger.info(f"🔍 Типы данных в первой записи: {[type(val).__name__ for val in records[0][:5]]}")
            
            insert_query = f"INSERT INTO {self.validation_table} ({field_list}) VALUES"
            self.client.execute(insert_query, records)
            
            self.logger.info(f"✅ Экспортировано {len(records)} записей MacroProperty3 в ClickHouse")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка экспорта MacroProperty3: {e}")
            raise

    def compare_original_vs_exported(self, field_mapping: Dict[str, int]) -> Dict[str, Any]:
        """Сравнение исходной heli_pandas с экспортированной из FLAME GPU"""
        self.logger.info("🔍 Сравнение исходной heli_pandas с экспортированной...")
        
        try:
            # Поля из аналитики MacroProperty3 (ограничиваемся реально доступными)
            analytics_fields_all = [
                'partseqno_i', 'psn', 'address_i', 'lease_restricted', 'group_by', 'status_id',
                'status_change', 'aircraft_number', 'ac_type_mask', 'll', 'oh', 'oh_threshold',
                'sne', 'ppr', 'repair_days', 'mfg_date'
            ]
            try:
                ch_schema = self.client.execute("DESCRIBE TABLE heli_pandas")
                existing_fields = {row[0] for row in ch_schema}
            except Exception:
                existing_fields = set()
            analytics_fields = [f for f in analytics_fields_all if (f in field_mapping and f in existing_fields)]
            
            # Загрузка исходных данных (последняя версия)
            original_select = ", ".join(analytics_fields)
            original_query = f"""
                SELECT {original_select}
                FROM {self.original_table}
                WHERE (version_date, version_id) = (
                    SELECT version_date, version_id 
                    FROM {self.original_table} 
                    ORDER BY version_date DESC, version_id DESC 
                    LIMIT 1
                )
                ORDER BY psn
            """
            original_data = self.client.execute(original_query)
            
            # Загрузка экспортированных данных
            exported_select = ", ".join(analytics_fields)
            exported_query = f"""
                SELECT {exported_select}
                FROM {self.validation_table}
                ORDER BY record_id
            """
            exported_data = self.client.execute(exported_query)
            
            self.logger.info(f"📊 Исходных записей: {len(original_data)}")
            self.logger.info(f"📊 Экспортированных записей: {len(exported_data)}")
            
            # Сравнение поле за полем
            comparison_results = {
                'total_original_records': len(original_data),
                'total_exported_records': len(exported_data),
                'record_count_match': len(original_data) == len(exported_data),
                'field_comparisons': {},
                'overall_success': True,
                'mismatches': []
            }
            
            min_records = min(len(original_data), len(exported_data))
            
            for field_idx, field_name in enumerate(analytics_fields):
                matches = 0
                mismatches = 0
                null_conversions = 0
                sample_mismatches = []
                
                self.logger.info(f"🔍 Сравнение поля {field_name}...")
                
                for record_idx in range(min_records):
                    original_val = original_data[record_idx][field_idx]
                    exported_val = exported_data[record_idx][field_idx]
                    
                    # Обработка NULL значений (FLAME GPU конвертирует NULL в 0)
                    original_val_processed = original_val
                    if original_val is None:
                        original_val_processed = 0
                        null_conversions += 1
                    
                    # Специальная обработка для Date типов
                    if field_name == 'mfg_date' and original_val_processed != 0:
                        if hasattr(original_val_processed, 'toordinal'):
                            from datetime import date
                            epoch = date(1970, 1, 1)
                            days_since_epoch = original_val_processed.toordinal() - epoch.toordinal()
                            original_val_processed = max(0, days_since_epoch)
                    
                    # Сравнение значений
                    if original_val_processed == exported_val:
                        matches += 1
                    else:
                        mismatches += 1
                        if len(sample_mismatches) < 5:
                            sample_mismatches.append({
                                'record_idx': record_idx,
                                'original': original_val,
                                'original_processed': original_val_processed,
                                'exported': exported_val,
                                'was_null': original_val is None
                            })
                
                field_success = mismatches == 0
                field_id = field_mapping.get(field_name, 'N/A')
                
                comparison_results['field_comparisons'][field_name] = {
                    'field_id': field_id,
                    'matches': matches,
                    'mismatches': mismatches,
                    'success': field_success,
                    'null_conversions': null_conversions,
                    'sample_mismatches': sample_mismatches
                }
                
                if not field_success:
                    comparison_results['overall_success'] = False
                    comparison_results['mismatches'].append(field_name)
                
                status = "✅" if field_success else "❌"
                null_info = f" (NULL: {null_conversions}→0)" if null_conversions > 0 else ""
                self.logger.info(f"   {status} {field_name}: {matches} совпадений, {mismatches} расхождений{null_info}")
            
            return comparison_results
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка сравнения данных: {e}")
            raise

    def generate_validation_report(self, comparison_results: Dict[str, Any], 
                                 loader_stats: Dict[str, Any]) -> str:
        """Генерация отчета валидации"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_dir = "temp_data"
        os.makedirs(report_dir, exist_ok=True)
        
        report_file = os.path.join(report_dir, f"flame_macroproperty3_validation_report_{timestamp}.txt")
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("🔍 ОТЧЕТ ВАЛИДАЦИИ FLAME GPU MACROPROPERTY3\n")
            f.write("=" * 80 + "\n\n")
            
            # Общая статистика
            f.write("📊 ОБЩАЯ СТАТИСТИКА:\n")
            f.write(f"   • Исходная таблица: {self.original_table}\n")
            f.write(f"   • Валидационная таблица: {self.validation_table}\n")
            f.write(f"   • Исходных записей: {comparison_results['total_original_records']}\n")
            f.write(f"   • Экспортированных записей: {comparison_results['total_exported_records']}\n")
            f.write(f"   • Соответствие количества: {'✅' if comparison_results['record_count_match'] else '❌'}\n")
            f.write(f"   • Общий результат: {'✅ УСПЕШНО' if comparison_results['overall_success'] else '❌ ОШИБКИ'}\n\n")
            
            # Статистика загрузчика
            f.write("🔥 СТАТИСТИКА ЗАГРУЗЧИКА:\n")
            f.write(f"   • Всего агентов: {loader_stats.get('total_records', 'N/A')}\n")
            f.write(f"   • Агентов с psn (agent_id): {loader_stats.get('agents_with_psn', 'N/A')}\n")
            f.write(f"   • Загружено Property Arrays: {loader_stats.get('loaded_macroproperties', 'N/A')}\n")
            
            null_conversions = loader_stats.get('null_conversions', {})
            if null_conversions:
                f.write(f"   • NULL конвертации:\n")
                for field, count in null_conversions.items():
                    f.write(f"     - {field}: {count} значений\n")
            f.write("\n")
            
            # Результаты сравнения по полям
            f.write("🎯 РЕЗУЛЬТАТЫ СРАВНЕНИЯ ПО ПОЛЯМ:\n")
            f.write(f"{'Поле':<20} {'field_id':<8} {'Совпадения':<10} {'Расхождения':<12} {'NULL→0':<8} {'Статус':<6}\n")
            f.write("-" * 70 + "\n")
            
            for field_name, results in comparison_results['field_comparisons'].items():
                field_id = results['field_id']
                matches = results['matches']
                mismatches = results['mismatches']
                null_conv = results['null_conversions']
                status = "✅ ОК" if results['success'] else "❌ ERR"
                
                f.write(f"{field_name:<20} {field_id:<8} {matches:<10} {mismatches:<12} {null_conv:<8} {status:<6}\n")
            
            # Итоговая статистика
            total_fields = len(comparison_results['field_comparisons'])
            successful_fields = sum(1 for r in comparison_results['field_comparisons'].values() if r['success'])
            total_null_conversions = sum(r['null_conversions'] for r in comparison_results['field_comparisons'].values())
            
            f.write(f"\n📈 ИТОГО: {successful_fields}/{total_fields} полей прошли сравнение\n")
            f.write(f"🔄 NULL КОНВЕРСИИ: {total_null_conversions} значений преобразовано\n\n")
            
            if comparison_results['overall_success']:
                f.write("🎉 ПРЕВОСХОДНО! Все поля идентичны между таблицами! 🔥\n")
                f.write("✅ FLAME GPU MacroProperty3 загружено корректно\n\n")
                f.write("💡 РЕКОМЕНДАЦИИ:\n")
                f.write("   • Данные успешно загружены в FLAME GPU\n")
                f.write("   • MacroProperty3 готово для использования в симуляции\n")
                f.write("   • Можно переходить к следующему этапу Transform\n")
            else:
                f.write("⚠️ ОБНАРУЖЕНЫ РАСХОЖДЕНИЯ В ДАННЫХ!\n")
                f.write(f"❌ Поля с ошибками: {', '.join(comparison_results['mismatches'])}\n\n")
                f.write("🔧 РЕКОМЕНДАЦИИ:\n")
                f.write("   • Проверить логику обработки NULL значений\n")
                f.write("   • Проверить соответствие типов данных\n")
                f.write("   • Проанализировать примеры расхождений\n")
            
            f.write("\n" + "=" * 80 + "\n")
        
        self.logger.info(f"✅ Отчет валидации сохранен: {report_file}")
        return report_file

    def cleanup_validation_table(self) -> None:
        """Очистка временной таблицы валидации"""
        try:
            self.client.execute(f"DROP TABLE IF EXISTS {self.validation_table}")
            self.logger.info(f"🗑️ Временная таблица {self.validation_table} удалена")
        except Exception as e:
            self.logger.warning(f"⚠️ Ошибка удаления временной таблицы: {e}")

    def validate_flame_macroproperty3(self, cleanup: bool = True) -> str:
        """Полная валидация FLAME GPU MacroProperty3 через roundtrip тест"""
        self.logger.info("🚀 Начало полной валидации FLAME GPU MacroProperty3")
        
        loader = None
        sim = None
        field_mapping = {}
        field_types = {}
        total_records = 0
        
        try:
            self.logger.info("📥 Шаг 1: Создание загрузчика и загрузка данных в FLAME GPU...")
            loader = FlameMacroProperty3Loader()
            sim, field_mapping, field_types, total_records = loader.load_macroproperty3_complete()
            
            self.logger.info(f"✅ Данные загружены в FLAME GPU: {total_records} записей, {len(field_mapping)} полей")
            self.logger.info("🔒 Simulation объект сохранен в памяти для валидации")
            
            self.logger.info("🏗️ Шаг 2: Создание таблицы валидации...")
            self.create_validation_table(field_mapping, field_types)
            
            self.logger.info("📤 Шаг 3: Экспорт из FLAME GPU в ClickHouse...")
            self.export_flame_environment_to_clickhouse(sim, field_mapping, total_records)
            
            self.logger.info("🔍 Шаг 4: Сравнение исходных и экспортированных данных...")
            comparison_results = self.compare_original_vs_exported(field_mapping)
            
            self.logger.info("📋 Шаг 5: Генерация отчета валидации...")
            loader_stats = loader.stats if loader else {}
            report_file = self.generate_validation_report(comparison_results, loader_stats)
            
            if cleanup:
                self.cleanup_validation_table()
                
            self.logger.info("🗑️ Освобождение FLAME GPU ресурсов...")
            sim = None
            loader = None
            
            self.logger.info("✅ Полная валидация FLAME GPU MacroProperty3 завершена")
            return report_file
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка валидации FLAME GPU MacroProperty3: {e}")
            if cleanup:
                self.cleanup_validation_table()
            sim = None
            loader = None
            raise
        finally:
            self.logger.info("🗑️ Освобождение FLAME GPU ресурсов...")
            sim = None
            loader = None

def main():
    """Основная функция для тестирования валидатора"""
    print("🔍 FLAME GPU MacroProperty3 Validator")
    print("=" * 50)
    
    try:
        validator = FlameMacroProperty3Validator()
        report_file = validator.validate_flame_macroproperty3()
        
        print("\n" + "=" * 50)
        print("✅ ВАЛИДАЦИЯ MACROPROPERTY3 ЗАВЕРШЕНА!")
        print(f"📋 Отчет валидации: {report_file}")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n❌ ОШИБКА: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 