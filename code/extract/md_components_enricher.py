#!/usr/bin/env python3
"""
Обогатитель справочника md_components полем partno_comp

Функционал:
1. Добавляет цифровое поле partno_comp в md_components 
2. Обогащает его через dict_partno_flat с реальными ID из AMOS (partseqno_i)
3. Валидирует соответствие партномеров между справочником и словарем
4. Предоставляет статистику покрытия и несовпадений

АРХИТЕКТУРА v3.0 (словари с реальными ID):
- md_components.partno → dict_partno_flat → partno_comp (partseqno_i из AMOS)
- Разные названия полей для разных тензоров в Flame GPU macro property:
  * heli_pandas.partseqno_i → один тензор
  * md_components.partno_comp → другой тензор (аналогичные значения)

Место в ETL Pipeline:
- ПОСЛЕ: dictionary_creator.py (использует созданные словари)
- ПЕРЕД: calculate_beyond_repair.py (чтобы br и partno_comp в одном этапе)
"""

import sys
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime

# Добавляем путь к утилитам
sys.path.append(str(Path(__file__).parent / 'utils'))
from config_loader import load_clickhouse_config
import clickhouse_connect

class MDComponentsEnricher:
    """Обогатитель md_components полем partno_comp через словарь с реальными ID"""
    
    def __init__(self):
        """Инициализация обогатителя"""
        self.logger = self._setup_logging()
        self.config = load_clickhouse_config()
        
        # Исправляем конфигурацию для работы с ClickHouse
        self.config['port'] = 8123  # HTTP порт
        if 'settings' in self.config:
            self.config['settings'] = {k: v for k, v in self.config['settings'].items() if k != 'use_numpy'}
        
        self.client = None
    
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
    
    def check_prerequisites(self) -> bool:
        """Проверка предварительных условий"""
        self.logger.info("🔍 Проверка предварительных условий...")
        
        try:
            # 1. Проверяем наличие основных таблиц
            tables_result = self.client.query("""
                SELECT name 
                FROM system.tables 
                WHERE database = currentDatabase() 
                  AND name IN ('md_components')
                ORDER BY name
            """)
            
            existing_tables = [row[0] for row in tables_result.result_rows]
            if 'md_components' not in existing_tables:
                self.logger.error("❌ Отсутствует таблица md_components")
                return False
            
            self.logger.info("✅ Основная таблица md_components найдена")
            
            # 2. Проверяем наличие словарной таблицы
            dict_tables_result = self.client.query("""
                SELECT name 
                FROM system.tables 
                WHERE database = currentDatabase() 
                  AND name = 'dict_partno_flat'
            """)
            
            if not dict_tables_result.result_rows:
                self.logger.error("❌ Отсутствует словарная таблица dict_partno_flat")
                return False
            
            self.logger.info("✅ Словарная таблица dict_partno_flat найдена")
            
            # 3. Проверяем наличие данных в md_components
            md_count_result = self.client.query("SELECT COUNT(*) FROM md_components")
            md_count = md_count_result.result_rows[0][0]
            
            if md_count == 0:
                self.logger.error("❌ Нет данных в md_components")
                return False
            
            self.logger.info(f"📊 md_components: {md_count} записей")
            
            # 4. Проверяем наличие данных в словаре
            dict_count_result = self.client.query("SELECT COUNT(*) FROM dict_partno_flat")
            dict_count = dict_count_result.result_rows[0][0]
            
            if dict_count == 0:
                self.logger.error("❌ Нет данных в dict_partno_flat")
                return False
            
            self.logger.info(f"📊 dict_partno_flat: {dict_count} записей")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки предварительных условий: {e}")
            return False
    
    def add_partno_comp_column(self) -> bool:
        """Добавление колонки partno_comp в md_components"""
        self.logger.info("🔧 Добавление колонки partno_comp в md_components...")
        
        try:
            # Проверяем существование колонки
            structure_result = self.client.query("DESCRIBE md_components")
            columns = [row[0] for row in structure_result.result_rows]
            
            if 'partno_comp' in columns:
                self.logger.info("ℹ️ Колонка partno_comp уже существует")
            else:
                alter_query = "ALTER TABLE md_components ADD COLUMN partno_comp Nullable(UInt32) DEFAULT NULL"
                self.client.command(alter_query)
                self.logger.info("✅ Колонка partno_comp (UInt32) добавлена в md_components")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка добавления колонки partno_comp: {e}")
            return False
    
    def load_partno_dictionary(self) -> Dict[str, int]:
        """Загрузка словаря партномеров из dict_partno_flat"""
        self.logger.info("📚 Загрузка словаря партномеров из dict_partno_flat...")
        
        partno_dict = {}
        
        try:
            # Загружаем словарь: partno → partseqno_i (реальные ID из AMOS)
            partno_result = self.client.query("SELECT partno, partseqno_i FROM dict_partno_flat")
            for row in partno_result.result_rows:
                partno, partseqno_i = row
                partno_dict[partno] = partseqno_i
            
            self.logger.info(f"📦 Загружен dict_partno_flat: {len(partno_dict)} партномеров")
            
            # Показываем примеры
            if partno_dict:
                self.logger.info("📋 Примеры из словаря:")
                for i, (partno, partseqno_i) in enumerate(list(partno_dict.items())[:5]):
                    self.logger.info(f"  '{partno}' → {partseqno_i}")
                if len(partno_dict) > 5:
                    self.logger.info(f"  ... и еще {len(partno_dict)-5} партномеров")
            
            return partno_dict
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки словаря партномеров: {e}")
            return {}
    
    def analyze_partno_coverage(self, partno_dict: Dict[str, int]) -> Tuple[int, int, float]:
        """Анализ покрытия партномеров"""
        self.logger.info("🔍 Анализ покрытия партномеров...")
        
        try:
            # Получаем уникальные партномера из md_components
            md_partno_result = self.client.query("""
                SELECT DISTINCT partno, COUNT(*) as cnt
                FROM md_components 
                WHERE partno IS NOT NULL AND partno != ''
                GROUP BY partno
            """)
            
            found_count = 0
            missing_count = 0
            missing_examples = []
            
            for row in md_partno_result.result_rows:
                partno = row[0]
                if partno in partno_dict:
                    found_count += 1
                else:
                    missing_count += 1
                    if len(missing_examples) < 5:
                        missing_examples.append(partno)
            
            total_count = found_count + missing_count
            coverage = (found_count / total_count) * 100 if total_count > 0 else 0
            
            self.logger.info(f"📊 Покрытие партномеров:")
            self.logger.info(f"  ✅ Найдены в словаре: {found_count}/{total_count} ({coverage:.1f}%)")
            self.logger.info(f"  ❌ Отсутствуют в словаре: {missing_count}/{total_count} ({100-coverage:.1f}%)")
            
            if missing_examples:
                self.logger.warning("⚠️ Примеры отсутствующих партномеров:")
                for partno in missing_examples:
                    self.logger.warning(f"  '{partno}'")
                if missing_count > len(missing_examples):
                    self.logger.warning(f"  ... и еще {missing_count - len(missing_examples)} партномеров")
            
            return found_count, missing_count, coverage
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка анализа покрытия: {e}")
            return 0, 0, 0.0
    
    def update_partno_comp_field(self, partno_dict: Dict[str, int]) -> bool:
        """Обновление поля partno_comp в md_components"""
        self.logger.info("💾 Обновление поля partno_comp...")
        
        try:
            if not partno_dict:
                self.logger.warning("⚠️ Нет данных для обновления partno_comp")
                return False
            
            # Очищаем поле
            self.client.command("ALTER TABLE md_components UPDATE partno_comp = NULL WHERE 1=1")
            self.logger.info("🧹 Поле partno_comp очищено")
            
            # Создаем CASE WHEN выражение для обновления
            partno_cases = []
            for partno, partseqno_i in partno_dict.items():
                escaped_partno = partno.replace("'", "''")
                partno_cases.append(f"WHEN partno = '{escaped_partno}' THEN {partseqno_i}")
            
            if partno_cases:
                partno_case_expr = " ".join(partno_cases)
                update_query = f"""
                ALTER TABLE md_components 
                UPDATE partno_comp = CASE {partno_case_expr} ELSE NULL END
                WHERE partno IS NOT NULL AND partno != ''
                """
                
                self.client.command(update_query)
                self.logger.info(f"✅ partno_comp обновлено для {len(partno_dict)} партномеров")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления partno_comp: {e}")
            return False
    
    def validate_enrichment(self) -> bool:
        """Валидация результатов обогащения"""
        self.logger.info("🔍 Валидация результатов обогащения...")
        
        try:
            # Общая статистика
            total_result = self.client.query("SELECT COUNT(*) FROM md_components")
            total_count = total_result.result_rows[0][0]
            
            # Статистика заполнения partno_comp
            filled_result = self.client.query("SELECT COUNT(*) FROM md_components WHERE partno_comp IS NOT NULL")
            filled_count = filled_result.result_rows[0][0]
            
            coverage = (filled_count / total_count) * 100 if total_count > 0 else 0
            
            self.logger.info(f"📊 Результаты обогащения (всего записей: {total_count}):")
            self.logger.info(f"  partno_comp заполнено: {filled_count} ({coverage:.1f}%)")
            self.logger.info(f"  partno_comp пустое: {total_count - filled_count} ({100-coverage:.1f}%)")
            
            # Статистика диапазонов значений
            if filled_count > 0:
                range_result = self.client.query("""
                    SELECT 
                        MIN(partno_comp) as min_id,
                        MAX(partno_comp) as max_id,
                        COUNT(DISTINCT partno_comp) as unique_ids
                    FROM md_components 
                    WHERE partno_comp IS NOT NULL
                """)
                
                min_id, max_id, unique_ids = range_result.result_rows[0]
                self.logger.info(f"📈 Диапазон partno_comp: {min_id} - {max_id} ({unique_ids} уникальных ID)")
            
            # Примеры обогащенных записей
            examples_result = self.client.query("""
                SELECT partno, partno_comp
                FROM md_components 
                WHERE partno_comp IS NOT NULL
                ORDER BY partno_comp
                LIMIT 5
            """)
            
            if examples_result.result_rows:
                self.logger.info("📋 Примеры обогащенных записей:")
                for row in examples_result.result_rows:
                    partno, partno_comp = row
                    self.logger.info(f"  '{partno}' → partno_comp: {partno_comp}")
            
            # Считаем обогащение успешным если покрытие > 80%
            success_threshold = 80.0
            if coverage >= success_threshold:
                self.logger.info(f"✅ Обогащение успешно: покрытие {coverage:.1f}% >= {success_threshold}%")
                return True
            else:
                self.logger.warning(f"⚠️ Низкое покрытие: {coverage:.1f}% < {success_threshold}%")
                return False
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка валидации: {e}")
            return False
    
    def run_enrichment(self) -> bool:
        """Запуск полного процесса обогащения"""
        self.logger.info("🚀 Запуск обогащения md_components полем partno_comp")
        self.logger.info("📚 Используем словарь dict_partno_flat с реальными ID из AMOS")
        self.logger.info("🎯 Цель: разные тензоры в Flame GPU (partseqno_i vs partno_comp)")
        
        try:
            # 1. Подключение
            if not self.connect_to_database():
                return False
            
            # 2. Проверка предварительных условий
            if not self.check_prerequisites():
                return False
            
            # 3. Добавление колонки partno_comp
            if not self.add_partno_comp_column():
                return False
            
            # 4. Загрузка словаря партномеров
            partno_dict = self.load_partno_dictionary()
            if not partno_dict:
                self.logger.error("❌ Не удалось загрузить словарь партномеров")
                return False
            
            # 5. Анализ покрытия
            found_count, missing_count, coverage = self.analyze_partno_coverage(partno_dict)
            
            # 6. Обновление поля partno_comp
            if not self.update_partno_comp_field(partno_dict):
                return False
            
            # 7. Валидация результатов
            if not self.validate_enrichment():
                self.logger.warning("⚠️ Валидация показала проблемы, но обогащение выполнено")
            
            self.logger.info("🎯 ОБОГАЩЕНИЕ MD_COMPONENTS ЗАВЕРШЕНО!")
            self.logger.info(f"✅ Добавлено поле partno_comp с реальными ID из AMOS")
            self.logger.info(f"📊 Покрытие: {found_count} партномеров ({coverage:.1f}%)")
            self.logger.info("🚀 Готово для разных тензоров в Flame GPU")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обогащения: {e}")
            return False

def main():
    """Основная функция"""
    print("🚀 === ОБОГАТИТЕЛЬ MD_COMPONENTS v3.0 ===")
    print("📚 Добавляем partno_comp через словарь с реальными ID из AMOS")
    print("🎯 Цель: разные тензоры в Flame GPU (partseqno_i vs partno_comp)")
    
    try:
        enricher = MDComponentsEnricher()
        success = enricher.run_enrichment()
        
        if success:
            print(f"\n🎯 === ОБОГАЩЕНИЕ ЗАВЕРШЕНО ===")
            print(f"✅ Поле partno_comp добавлено в md_components")
            print(f"📚 Использованы реальные ID из dict_partno_flat")
            print(f"🚀 Готово для разных тензоров в Flame GPU!")
            return 0
        else:
            print(f"\n❌ === ОШИБКА ОБОГАЩЕНИЯ ===")
            return 1
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        return 1

if __name__ == "__main__":
    exit(main()) 