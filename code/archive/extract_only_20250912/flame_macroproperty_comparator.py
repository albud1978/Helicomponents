#!/usr/bin/env python3
"""
FLAME GPU MacroProperty1 Comparator - сравнение данных
Сравнивает экспортированную таблицу flame_macroproperty1_export 
с исходной md_components для проверки целостности FLAME GPU загрузки
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

def setup_logging() -> logging.Logger:
    """Настройка логирования"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/flame_macroproperty_comparator.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

class FlameMacroPropertyComparator:
    """Компаратор данных между исходной и экспортированной таблицами"""
    
    def __init__(self, client=None):
        self.client = client or get_clickhouse_client()
        self.logger = setup_logging()
        self.original_table = "md_components"
        self.export_table = "flame_macroproperty1_export"
        
    def analyze_table_structures(self) -> Dict[str, Any]:
        """Анализ структур обеих таблиц"""
        self.logger.info("🔍 Анализ структур таблиц...")
        
        try:
            # Структура исходной таблицы
            original_query = """
                SELECT name, type, comment 
                FROM system.columns 
                WHERE database = 'default' AND table = 'md_components'
                ORDER BY position
            """
            original_columns = self.client.execute(original_query)
            
            # Структура экспортированной таблицы
            export_query = """
                SELECT name, type, comment 
                FROM system.columns 
                WHERE database = 'default' AND table = 'flame_macroproperty1_export'
                ORDER BY position
            """
            export_columns = self.client.execute(export_query)
            
            self.logger.info(f"📊 Исходная таблица: {len(original_columns)} полей")
            self.logger.info(f"📊 Экспортированная таблица: {len(export_columns)} полей")
            
            # Создаем маппинг field_id из комментариев
            field_id_mapping = {}
            for name, type_, comment in export_columns:
                if comment and 'field_id:' in comment:
                    field_id = int(comment.split('field_id:')[1].strip())
                    field_id_mapping[name] = field_id
            
            return {
                'original_columns': original_columns,
                'export_columns': export_columns,
                'field_id_mapping': field_id_mapping
            }
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка анализа структур: {e}")
            raise

    def get_matching_fields(self, structures: Dict[str, Any]) -> List[Tuple[str, str, int]]:
        """Получение полей, присутствующих в обеих таблицах"""
        self.logger.info("🎯 Поиск совпадающих полей...")
        
        try:
            original_fields = {name for name, type_, comment in structures['original_columns']}
            export_fields = {name for name, type_, comment in structures['export_columns'] if name != 'record_id'}
            field_id_mapping = structures['field_id_mapping']
            
            # Находим пересечение полей
            matching_fields = []
            for field_name in original_fields.intersection(export_fields):
                field_id = field_id_mapping.get(field_name, 0)
                matching_fields.append((field_name, field_name, field_id))
            
            self.logger.info(f"✅ Найдено {len(matching_fields)} совпадающих полей:")
            for orig_field, exp_field, field_id in matching_fields:
                self.logger.info(f"   📋 {orig_field} -> field_id: {field_id}")
            
            return matching_fields
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка поиска совпадающих полей: {e}")
            raise

    def load_comparison_data(self, matching_fields: List[Tuple[str, str, int]]) -> Dict[str, Any]:
        """Загрузка данных для сравнения (учитывая строковый формат export таблицы)"""
        self.logger.info("📥 Загрузка данных для сравнения (строковый формат)...")
        
        try:
            # Формируем список полей для запросов
            original_fields = [field_name for field_name, _, _ in matching_fields]
            export_fields = [field_name for _, field_name, _ in matching_fields]
            
            # Загружаем данные из исходной таблицы (последняя версия)
            # КРИТИЧНО: Сортируем по partno_comp для соответствия порядку экспорта
            original_select = ", ".join(original_fields)
            original_query = f"""
                SELECT {original_select}
                FROM {self.original_table} 
                WHERE (version_date, version_id) = (
                    SELECT version_date, version_id 
                    FROM {self.original_table} 
                    ORDER BY version_date DESC, version_id DESC 
                    LIMIT 1
                )
                ORDER BY partno_comp
            """
            original_data = self.client.execute(original_query)
            
            # Загружаем данные из экспортированной таблицы (строковый формат)
            # ENGINE = Memory хранит данные построчно, поэтому порядок записей сохраняется
            export_select = ", ".join(export_fields)
            export_query = f"""
                SELECT {export_select}
                FROM {self.export_table} 
                ORDER BY record_id
            """
            export_data = self.client.execute(export_query)
            
            self.logger.info(f"📊 Исходных записей: {len(original_data)}")
            self.logger.info(f"📊 Экспортированных записей: {len(export_data)}")
            self.logger.info("🔍 Строковый формат: данные построчно, порядок сохранен")
            
            # Проверяем, что количество записей совпадает
            if len(original_data) != len(export_data):
                self.logger.warning(f"⚠️ Различие в количестве записей: {len(original_data)} vs {len(export_data)}")
            
            return {
                'original_data': original_data,
                'export_data': export_data,
                'original_fields': original_fields,
                'export_fields': export_fields
            }
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки данных: {e}")
            raise

    def compare_field_by_field(self, comparison_data: Dict[str, Any], 
                              matching_fields: List[Tuple[str, str, int]]) -> Dict[str, Any]:
        """Детальное сравнение поле за полем (строковый формат)"""
        self.logger.info("🔍 Детальное сравнение поле за полем (строковый формат)...")
        
        try:
            original_data = comparison_data['original_data']
            export_data = comparison_data['export_data']
            original_fields = comparison_data['original_fields']
            
            comparison_results = {
                'total_original_records': len(original_data),
                'total_export_records': len(export_data),
                'record_count_match': len(original_data) == len(export_data),
                'field_comparisons': {},
                'overall_success': True,
                'mismatches': [],
                'storage_format': 'строковый (ENGINE = Memory)'
            }
            
            min_records = min(len(original_data), len(export_data))
            self.logger.info(f"🎯 Сравниваем {min_records} записей по {len(matching_fields)} полям")
            
            # Сравниваем каждое поле (построчно в строковом формате)
            for field_idx, (orig_field, exp_field, field_id) in enumerate(matching_fields):
                matches = 0
                mismatches = 0
                null_original = 0
                null_export = 0
                sample_mismatches = []
                type_conversions = 0
                
                self.logger.info(f"🔍 Сравнение поля {orig_field} (field_id: {field_id})...")
                
                # Построчное сравнение (подходит для строкового формата ENGINE = Memory)
                for record_idx in range(min_records):
                    # Значения из обеих таблиц (одинаковый индекс = одинаковая запись)
                    original_val = original_data[record_idx][field_idx]
                    export_val = export_data[record_idx][field_idx]
                    
                    # Обработка NULL значений (FLAME GPU конвертирует NULL в 0)
                    original_val_processed = original_val
                    if original_val is None:
                        null_original += 1
                        original_val_processed = 0  # NULL в FLAME GPU преобразуется в 0
                        type_conversions += 1
                    
                    if export_val is None:
                        null_export += 1
                    
                    # Проверка типов для строкового формата
                    if isinstance(original_val_processed, float) and isinstance(export_val, (int, float)):
                        # Приводим float к int если нет дробной части
                        if original_val_processed == int(original_val_processed):
                            original_val_processed = int(original_val_processed)
                    
                    # Сравнение значений
                    if original_val_processed == export_val:
                        matches += 1
                    else:
                        mismatches += 1
                        if len(sample_mismatches) < 5:
                            sample_mismatches.append({
                                'record_idx': record_idx,
                                'original': original_val,
                                'original_processed': original_val_processed,
                                'export': export_val,
                                'was_null': original_val is None
                            })
                
                field_success = mismatches == 0
                comparison_results['field_comparisons'][orig_field] = {
                    'field_id': field_id,
                    'matches': matches,
                    'mismatches': mismatches,
                    'success': field_success,
                    'null_handling': {
                        'original_nulls': null_original,
                        'export_nulls': null_export,
                        'type_conversions': type_conversions
                    },
                    'sample_mismatches': sample_mismatches
                }
                
                if not field_success:
                    comparison_results['overall_success'] = False
                    comparison_results['mismatches'].append(orig_field)
                
                status = "✅" if field_success else "❌"
                null_info = f" (NULL: {null_original}→0)" if null_original > 0 else ""
                self.logger.info(f"   {status} {orig_field}: {matches} совпадений, {mismatches} расхождений{null_info}")
            
            return comparison_results
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка сравнения полей: {e}")
            raise

    def generate_comparison_report(self, comparison_results: Dict[str, Any], 
                                 structures: Dict[str, Any],
                                 matching_fields: List[Tuple[str, str, int]]) -> str:
        """Генерация подробного отчета сравнения"""
        self.logger.info("📋 Генерация отчета сравнения...")
        
        try:
            report_lines = []
            report_lines.append("=" * 80)
            report_lines.append("🔍 ОТЧЕТ СРАВНЕНИЯ FLAME GPU MACROPROPERTY1 С ИСХОДНЫМИ ДАННЫМИ")
            report_lines.append("=" * 80)
            report_lines.append("")
            
            # Общая статистика
            report_lines.append("📊 ОБЩАЯ СТАТИСТИКА:")
            report_lines.append(f"   • Исходная таблица: {self.original_table}")
            report_lines.append(f"   • Экспортированная таблица: {self.export_table}")
            report_lines.append(f"   • Исходных записей: {comparison_results['total_original_records']:,}")
            report_lines.append(f"   • Экспортированных записей: {comparison_results['total_export_records']:,}")
            report_lines.append(f"   • Соответствие количества: {'✅' if comparison_results['record_count_match'] else '❌'}")
            report_lines.append(f"   • Общий результат: {'✅ УСПЕШНО' if comparison_results['overall_success'] else '❌ ПРОБЛЕМЫ'}")
            report_lines.append("")
            
            # Структурные различия
            original_count = len(structures['original_columns'])
            export_count = len(structures['export_columns']) - 1  # -1 для record_id
            matched_count = len(matching_fields)
            storage_format = comparison_results.get('storage_format', 'неизвестный')
            
            report_lines.append("🏗️ СТРУКТУРНЫЙ АНАЛИЗ:")
            report_lines.append(f"   • Исходная таблица: колоночный формат (MergeTree)")
            report_lines.append(f"   • Экспортированная таблица: {storage_format}")
            report_lines.append(f"   • Полей в исходной таблице: {original_count}")
            report_lines.append(f"   • Полей в экспортированной таблице: {export_count}")
            report_lines.append(f"   • Совпадающих полей: {matched_count}")
            report_lines.append(f"   • Полей только в исходной: {original_count - matched_count}")
            report_lines.append(f"   • Полей только в экспортированной: {export_count - matched_count}")
            report_lines.append(f"   • Порядок записей: построчный (record_id соответствует порядку)")
            report_lines.append("")
            
            # Результаты по полям
            report_lines.append("🎯 РЕЗУЛЬТАТЫ СРАВНЕНИЯ ПО ПОЛЯМ:")
            report_lines.append(f"   {'Поле':<20} {'field_id':<8} {'Совпадения':<10} {'Расхождения':<12} {'NULL→0':<8} {'Статус'}")
            report_lines.append(f"   {'-'*20} {'-'*8} {'-'*10} {'-'*12} {'-'*8} {'-'*6}")
            
            success_count = 0
            null_conversions = 0
            
            for orig_field, _, field_id in matching_fields:
                if orig_field in comparison_results['field_comparisons']:
                    result = comparison_results['field_comparisons'][orig_field]
                    
                    status = "✅ ОК" if result['success'] else "❌ FAIL"
                    matches = result['matches']
                    mismatches = result['mismatches']
                    nulls = result['null_handling']['original_nulls']
                    
                    report_lines.append(f"   {orig_field:<20} {field_id:<8} {matches:<10} {mismatches:<12} {nulls:<8} {status}")
                    
                    if result['success']:
                        success_count += 1
                    
                    if nulls > 0:
                        null_conversions += 1
            
            report_lines.append("")
            report_lines.append(f"📈 ИТОГО: {success_count}/{len(matching_fields)} полей прошли сравнение")
            
            if null_conversions > 0:
                report_lines.append(f"🔄 NULL КОНВЕРСИИ: {null_conversions} полей с NULL→0 преобразованием")
            
            # Детали проблем
            if comparison_results['mismatches']:
                report_lines.append("")
                report_lines.append("❌ ОБНАРУЖЕННЫЕ ПРОБЛЕМЫ:")
                for field_name in comparison_results['mismatches']:
                    result = comparison_results['field_comparisons'][field_name]
                    report_lines.append(f"   • {field_name}: {result['mismatches']} расхождений")
                    
                    if result['sample_mismatches']:
                        report_lines.append(f"     Примеры расхождений:")
                        for sample in result['sample_mismatches'][:3]:
                            report_lines.append(f"       Запись {sample['record_idx']}: "
                                               f"исходное={sample['original']}, "
                                               f"экспорт={sample['export']}")
            else:
                report_lines.append("")
                report_lines.append("🎉 ПРЕВОСХОДНО! Все поля идентичны между таблицами! 🔥")
                report_lines.append("✅ FLAME GPU MacroProperty1 загружено корректно")
            
            # Рекомендации
            report_lines.append("")
            report_lines.append("💡 РЕКОМЕНДАЦИИ:")
            if comparison_results['overall_success']:
                report_lines.append("   • Данные успешно загружены в FLAME GPU")
                report_lines.append("   • MacroProperty1 готово для использования в симуляции")
                report_lines.append("   • Можно переходить к следующему этапу Transform")
            else:
                report_lines.append("   • Исследовать причины расхождений в указанных полях")
                report_lines.append("   • Проверить обработку NULL значений")
                report_lines.append("   • Повторить загрузку с корректировками")
            
            report_lines.append("")
            report_lines.append("=" * 80)
            
            # Сохраняем отчет
            report_content = "\n".join(report_lines)
            
            os.makedirs('temp_data', exist_ok=True)
            report_file = f'temp_data/flame_macroproperty_comparison_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report_content)
            
            self.logger.info(f"✅ Отчет сравнения сохранен: {report_file}")
            
            # Выводим отчет в лог
            for line in report_lines:
                self.logger.info(line)
            
            return report_file
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка генерации отчета: {e}")
            raise

    def compare_tables(self) -> str:
        """Полное сравнение таблиц"""
        self.logger.info("🚀 Начало сравнения таблиц FLAME GPU MacroProperty1")
        
        try:
            # Шаг 1: Анализ структур
            self.logger.info("📋 Шаг 1: Анализ структур таблиц...")
            structures = self.analyze_table_structures()
            
            # Шаг 2: Поиск совпадающих полей
            self.logger.info("🎯 Шаг 2: Поиск совпадающих полей...")
            matching_fields = self.get_matching_fields(structures)
            
            if not matching_fields:
                self.logger.error("❌ Не найдено совпадающих полей для сравнения!")
                return ""
            
            # Шаг 3: Загрузка данных
            self.logger.info("📥 Шаг 3: Загрузка данных для сравнения...")
            comparison_data = self.load_comparison_data(matching_fields)
            
            # Шаг 4: Сравнение поле за полем
            self.logger.info("🔍 Шаг 4: Детальное сравнение полей...")
            comparison_results = self.compare_field_by_field(comparison_data, matching_fields)
            
            # Шаг 5: Генерация отчета
            self.logger.info("📋 Шаг 5: Генерация отчета сравнения...")
            report_file = self.generate_comparison_report(comparison_results, structures, matching_fields)
            
            self.logger.info("✅ Сравнение таблиц завершено")
            return report_file
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка сравнения таблиц: {e}")
            raise

def main():
    """Основная функция"""
    logger = setup_logging()
    
    try:
        logger.info("🔍 Запуск сравнения FLAME GPU MacroProperty1 с исходными данными...")
        
        comparator = FlameMacroPropertyComparator()
        report_file = comparator.compare_tables()
        
        if report_file:
            logger.info(f"🎉 Сравнение завершено! Отчет: {report_file}")
        else:
            logger.error("❌ Не удалось создать отчет сравнения")
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 