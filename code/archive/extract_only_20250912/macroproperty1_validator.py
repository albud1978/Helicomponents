#!/usr/bin/env python3
"""
MacroProperty1 Validator - валидация качества загрузки
Проверяет корректность загрузки MacroProperty1 через roundtrip тестирование
"""

import logging
import sys
import os
import json
from typing import Dict, List, Tuple, Any
from datetime import datetime

# Добавляем путь к utils
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))

from config_loader import get_clickhouse_client
from macroproperty1_loader import MacroProperty1Loader

def setup_logging() -> logging.Logger:
    """Настройка логирования"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/macroproperty1_validator.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

class MacroProperty1Validator:
    """Валидатор качества загрузки MacroProperty1"""
    
    def __init__(self, client=None):
        self.client = client or get_clickhouse_client()
        self.logger = setup_logging()
        self.validation_table = "test_macroproperty1_roundtrip"
        
    def create_validation_table(self, field_mapping: Dict[str, int], field_types: Dict[int, str]) -> None:
        """Создание таблицы для roundtrip валидации"""
        self.logger.info("🏗️ Создание таблицы валидации...")
        
        try:
            # Сначала удаляем таблицу если существует
            self.client.execute(f"DROP TABLE IF EXISTS {self.validation_table}")
            
            # Создаем DDL для таблицы
            fields_ddl = ["record_id UInt32"]
            
            for field_name, field_id in sorted(field_mapping.items(), key=lambda x: x[1]):
                ch_type = field_types.get(field_id, 'String')
                field_ddl = f"field_{field_id} {ch_type}"
                fields_ddl.append(field_ddl)
                self.logger.info(f"   📋 field_{field_id} ({field_name}): {ch_type}")
            
            create_query = f"""
                CREATE TABLE {self.validation_table} (
                    {', '.join(fields_ddl)}
                ) ENGINE = Memory
            """
            
            self.client.execute(create_query)
            self.logger.info(f"✅ Таблица {self.validation_table} создана")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания таблицы валидации: {e}")
            raise
    
    def export_macroproperty1_to_table(self, macroproperty1: Dict[int, List], 
                                     field_mapping: Dict[str, int]) -> None:
        """Экспорт MacroProperty1 в таблицу ClickHouse"""
        self.logger.info("📤 Экспорт MacroProperty1 в таблицу валидации...")
        
        try:
            # Определяем количество записей
            record_count = len(next(iter(macroproperty1.values())))
            self.logger.info(f"📊 Экспортируем {record_count} записей")
            
            # Подготавливаем данные для вставки
            insert_data = []
            
            for record_id in range(record_count):
                row = [record_id]  # Начинаем с record_id
                
                # Добавляем данные по field_id в порядке возрастания
                for field_id in sorted(macroproperty1.keys()):
                    value = macroproperty1[field_id][record_id]
                    row.append(value)
                
                insert_data.append(row)
            
            # Формируем список полей для INSERT
            field_list = ["record_id"]
            for field_id in sorted(macroproperty1.keys()):
                field_list.append(f"field_{field_id}")
            
            # Выполняем вставку
            insert_query = f"INSERT INTO {self.validation_table} ({', '.join(field_list)}) VALUES"
            
            self.client.execute(insert_query, insert_data)
            self.logger.info(f"✅ Экспортировано {len(insert_data)} записей")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка экспорта в таблицу: {e}")
            raise
    
    def compare_original_vs_exported(self, field_mapping: Dict[str, int]) -> Dict[str, Any]:
        """Сравнение исходных данных md_components с экспортированными"""
        self.logger.info("🔍 Сравнение исходных данных с экспортированными...")
        
        try:
            # Загружаем исходные данные (последняя версия)
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
            
            # Получаем порядок полей в исходной таблице
            original_fields_query = """
                SELECT name, position 
                FROM system.columns 
                WHERE database = 'default' AND table = 'md_components'
                ORDER BY position
            """
            original_field_order = [name for name, pos in self.client.execute(original_fields_query)]
            
            # Получаем порядок полей в экспортированной таблице  
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
                'mismatches': []
            }
            
            # Сравниваем поле в поле
            for field_name, field_id in field_mapping.items():
                if field_name in original_field_order:
                    original_col_idx = original_field_order.index(field_name)
                    exported_field_name = f"field_{field_id}"
                    
                    if exported_field_name in exported_field_order:
                        exported_col_idx = exported_field_order.index(exported_field_name)
                        
                        # Сравниваем значения
                        matches = 0
                        mismatches = 0
                        sample_mismatches = []
                        
                        min_records = min(len(original_data), len(exported_data))
                        
                        for i in range(min_records):
                            original_val = original_data[i][original_col_idx]
                            exported_val = exported_data[i][exported_col_idx]
                            
                            if original_val == exported_val:
                                matches += 1
                            else:
                                mismatches += 1
                                if len(sample_mismatches) < 5:  # Собираем образцы несоответствий
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
                            'sample_mismatches': sample_mismatches
                        }
                        
                        if not field_success:
                            comparison_results['overall_success'] = False
                            comparison_results['mismatches'].append(field_name)
                        
                        self.logger.info(f"   {'✅' if field_success else '❌'} {field_name} (field_{field_id}): "
                                       f"{matches} совпадений, {mismatches} расхождений")
            
            return comparison_results
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка сравнения данных: {e}")
            raise
    
    def generate_validation_report(self, comparison_results: Dict[str, Any], 
                                 metadata_file: str = None) -> str:
        """Генерация отчета валидации"""
        self.logger.info("📋 Генерация отчета валидации...")
        
        try:
            report_lines = []
            report_lines.append("=" * 60)
            report_lines.append("🎯 ОТЧЕТ ВАЛИДАЦИИ MACROPROPERTY1")
            report_lines.append("=" * 60)
            report_lines.append("")
            
            # Общая статистика
            report_lines.append("📊 ОБЩАЯ СТАТИСТИКА:")
            report_lines.append(f"   • Исходных записей в md_components: {comparison_results['total_original_records']:,}")
            report_lines.append(f"   • Загружено в MacroProperty1: {comparison_results['total_exported_records']:,}")
            report_lines.append(f"   • Соответствие количества записей: {'✅' if comparison_results['record_count_match'] else '❌'}")
            report_lines.append(f"   • Общий результат валидации: {'✅ УСПЕШНО' if comparison_results['overall_success'] else '❌ ОШИБКИ'}")
            report_lines.append("")
            
            # Статистика по полям
            report_lines.append("🎯 РЕЗУЛЬТАТЫ ПО ПОЛЯМ:")
            
            success_count = 0
            total_fields = len(comparison_results['field_comparisons'])
            
            for field_name, result in comparison_results['field_comparisons'].items():
                status = "✅" if result['success'] else "❌"
                field_id = result['field_id']
                matches = result['matches']
                mismatches = result['mismatches']
                
                report_lines.append(f"   {status} field_{field_id} ({field_name}): "
                                  f"{matches:,} совпадений, {mismatches:,} расхождений")
                
                if not result['success'] and result['sample_mismatches']:
                    report_lines.append(f"      💥 Примеры расхождений:")
                    for sample in result['sample_mismatches'][:3]:
                        report_lines.append(f"         Запись {sample['record_id']}: "
                                          f"исходное='{sample['original']}', "
                                          f"экспортированное='{sample['exported']}'")
                
                if result['success']:
                    success_count += 1
            
            report_lines.append("")
            report_lines.append(f"📈 ИТОГО: {success_count}/{total_fields} полей прошли валидацию")
            
            # Проблемы
            if comparison_results['mismatches']:
                report_lines.append("")
                report_lines.append("❌ ОБНАРУЖЕННЫЕ ПРОБЛЕМЫ:")
                for field_name in comparison_results['mismatches']:
                    report_lines.append(f"   • Расхождения в поле: {field_name}")
                
                report_lines.append("")
                report_lines.append("💡 РЕКОМЕНДАЦИИ:")
                report_lines.append("   • Проверить обработку NULL значений")
                report_lines.append("   • Исследовать расхождения в указанных полях")
                report_lines.append("   • Проверить корректность типов данных")
            else:
                report_lines.append("")
                report_lines.append("🎉 ОТЛИЧНО! Все поля прошли валидацию успешно")
            
            # Добавляем метаданные если доступны
            if metadata_file and os.path.exists(metadata_file):
                try:
                    with open(metadata_file, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                    
                    report_lines.append("")
                    report_lines.append("📋 МЕТАДАННЫЕ ЗАГРУЗКИ:")
                    report_lines.append(f"   • Дата создания: {metadata.get('creation_date', 'N/A')}")
                    report_lines.append(f"   • Версия данных: {metadata.get('version_date', 'N/A')}")
                    report_lines.append(f"   • ID версии: {metadata.get('version_id', 'N/A')}")
                    report_lines.append(f"   • Загружено полей: {metadata.get('loaded_fields', 'N/A')}")
                except:
                    pass
            
            report_lines.append("")
            report_lines.append("=" * 60)
            
            report_content = "\n".join(report_lines)
            
            # Сохраняем отчет
            os.makedirs('temp_data', exist_ok=True)
            report_file = f'temp_data/macroproperty1_validation_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report_content)
            
            self.logger.info(f"✅ Отчет валидации сохранен: {report_file}")
            
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
    
    def validate_macroproperty1(self, macroproperty1: Dict[int, List], 
                              field_mapping: Dict[str, int], 
                              field_types: Dict[int, str],
                              cleanup: bool = True) -> str:
        """Полная валидация MacroProperty1"""
        self.logger.info("🚀 Начало валидации MacroProperty1")
        
        try:
            # 1. Создание таблицы валидации
            self.create_validation_table(field_mapping, field_types)
            
            # 2. Экспорт MacroProperty1 в таблицу
            self.export_macroproperty1_to_table(macroproperty1, field_mapping)
            
            # 3. Сравнение данных
            comparison_results = self.compare_original_vs_exported(field_mapping)
            
            # 4. Генерация отчета
            report_file = self.generate_validation_report(comparison_results, 
                                                        'temp_data/macroproperty1_metadata.json')
            
            # 5. Очистка (опционально)
            if cleanup:
                self.cleanup_validation_table()
            
            self.logger.info("✅ Валидация MacroProperty1 завершена")
            return report_file
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка валидации MacroProperty1: {e}")
            if cleanup:
                self.cleanup_validation_table()
            raise

def main():
    """Основная функция"""
    logger = setup_logging()
    
    try:
        logger.info("🎯 Запуск полного цикла: загрузка + валидация MacroProperty1")
        
        # 1. Загружаем MacroProperty1
        loader = MacroProperty1Loader()
        macroproperty1 = loader.load_macroproperty1()
        
        # 2. Валидируем результаты
        validator = MacroProperty1Validator()
        
        # Получаем метаданные из загрузчика
        field_mapping = loader.stats['field_mapping']
        field_types = loader.stats['data_types']
        
        # Выполняем валидацию
        report_file = validator.validate_macroproperty1(
            macroproperty1, field_mapping, field_types, cleanup=True
        )
        
        logger.info(f"🎉 Полный цикл завершен! Отчет: {report_file}")
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 