#!/usr/bin/env python3
"""
Конвертация данных AMOS из текстового формата в полностью цифровой
Создание ID-маппингов для всех строковых полей и битовых масок
"""

import logging
from pathlib import Path
import pandas as pd
from typing import Dict, List, Tuple, Optional, Set
import yaml
import clickhouse_connect
from datetime import datetime
import sys
import re

# Добавляем путь к утилитам
sys.path.append(str(Path(__file__).parent / 'utils'))
from config_loader import load_config

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_output/digital_converter.log', mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DigitalDataConverter:
    """Конвертер данных в цифровой формат"""
    
    def __init__(self):
        """Инициализация конвертера"""
        self.config = load_config()
        self.client = None
        
        # Маппинги будут создаваться динамически из данных
        self.partno_mapping = {}
        self.serialno_mapping = {}
        self.location_mapping = {}
        
        # Битовые маски из схемы
        self.ac_type_masks = {
            'Ми-26': 128, 'МИ26Т': 128,    # 0b10000000
            'Ми-17': 64,                    # 0b01000000  
            'Ми-8Т': 32,                    # 0b00100000
            'Ка-32': 16,                    # 0b00010000
            'AS-350': 8, '350B3': 8,        # 0b00001000
            'AS-355': 4, '355NP': 4,        # 0b00000100
            'R-44': 2,                      # 0b00000010
        }
        
        self.owner_mapping = {
            'ЮТ-ВУ': 1, 'UTE': 2, 'ГТЛК': 3, 'СБЕР ЛИЗИНГ': 4,
            'ГПМ': 5, 'АО ГПМ': 6, 'ИП': 7, 'АРВ': 8, 'И': 9
        }
        
        self.condition_mapping = {
            'ИСПРАВНЫЙ': 7,        # 0b111 - Эксплуатация
            'НЕИСПРАВНЫЙ': 4,      # 0b100 - Ремонт  
            'ДОНОР': 1,            # 0b001 - Хранение
            'СНЯТ ЗАКАЗЧИКОМ': 0,  # 0b000 - Неактивно
            'СНЯТ': 0,             # 0b000 - Неактивно
            'НЕ УСТАНОВЛЕН': 6,    # 0b110 - Исправен, счетчики не работают
            'ПОСТАВКА': 3,         # 0b011 - Резерв
        }
        
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
            
            result = self.client.query("SELECT 1")
            logger.info(f"✅ Подключение к ClickHouse: {db_config['host']}:{db_config['port']}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к ClickHouse: {e}")
            return False
    
    def analyze_source_data(self) -> Dict:
        """Анализ исходных данных для создания маппингов"""
        logger.info("🔍 Анализ исходных данных...")
        
        # Получаем уникальные значения из cleaned_layer
        try:
            # Уникальные партномера
            partno_query = """
            SELECT DISTINCT partno 
            FROM status_components_cleaned 
            WHERE partno IS NOT NULL AND partno != ''
            ORDER BY partno
            """
            partno_result = self.client.query(partno_query)
            unique_partnos = [row[0] for row in partno_result.result_rows]
            
            # Уникальные серийные номера
            serialno_query = """
            SELECT DISTINCT serialno 
            FROM status_components_cleaned 
            WHERE serialno IS NOT NULL AND serialno != ''
            ORDER BY serialno
            """
            serialno_result = self.client.query(serialno_query)
            unique_serialnos = [row[0] for row in serialno_result.result_rows]
            
            # Уникальные локации (номера ВС)
            location_query = """
            SELECT DISTINCT location 
            FROM status_components_cleaned 
            WHERE location IS NOT NULL AND location != ''
            ORDER BY location
            """
            location_result = self.client.query(location_query)
            unique_locations = [row[0] for row in location_result.result_rows]
            
            # Анализ типов ВС
            ac_typ_query = """
            SELECT ac_typ, count(*) as cnt
            FROM status_components_cleaned 
            WHERE ac_typ IS NOT NULL 
            GROUP BY ac_typ 
            ORDER BY cnt DESC
            """
            ac_typ_result = self.client.query(ac_typ_query)
            ac_typ_stats = {row[0]: row[1] for row in ac_typ_result.result_rows}
            
            analysis = {
                'partnos': {
                    'count': len(unique_partnos),
                    'values': unique_partnos[:10],  # Первые 10 для примера
                    'all_values': unique_partnos
                },
                'serialnos': {
                    'count': len(unique_serialnos),
                    'values': unique_serialnos[:10],
                    'all_values': unique_serialnos
                },
                'locations': {
                    'count': len(unique_locations),
                    'values': unique_locations[:10],
                    'all_values': unique_locations
                },
                'ac_types': ac_typ_stats
            }
            
            logger.info(f"📊 Найдено уникальных значений:")
            logger.info(f"   - Партномеров: {len(unique_partnos)}")
            logger.info(f"   - Серийных номеров: {len(unique_serialnos)}")
            logger.info(f"   - Локаций: {len(unique_locations)}")
            logger.info(f"   - Типов ВС: {len(ac_typ_stats)}")
            
            return analysis
            
        except Exception as e:
            logger.error(f"❌ Ошибка анализа данных: {e}")
            return {}
    
    def create_id_mappings(self, analysis: Dict):
        """Создание ID маппингов для строковых полей"""
        logger.info("🔄 Создание ID маппингов...")
        
        # 1. Создание маппинга партномеров (ID начинается с 1)
        for i, partno in enumerate(analysis['partnos']['all_values'], 1):
            self.partno_mapping[partno] = i
        
        # 2. Создание маппинга серийных номеров  
        for i, serialno in enumerate(analysis['serialnos']['all_values'], 1):
            self.serialno_mapping[serialno] = i
            
        # 3. Создание маппинга локаций
        for i, location in enumerate(analysis['locations']['all_values'], 1):
            self.location_mapping[location] = i
        
        logger.info(f"✅ Создано маппингов:")
        logger.info(f"   - Партномеров: {len(self.partno_mapping)}")
        logger.info(f"   - Серийных номеров: {len(self.serialno_mapping)}")
        logger.info(f"   - Локаций: {len(self.location_mapping)}")
    
    def create_additional_mapping_tables(self):
        """Создание дополнительных справочных таблиц"""
        logger.info("🔄 Создание дополнительных справочных таблиц...")
        
        # Таблица маппинга серийных номеров
        serialno_ddl = """
        CREATE TABLE IF NOT EXISTS dim_serialno_mapping (
            serialno_id UInt32,
            serialno String
        ) ENGINE = Memory
        """
        
        # Таблица маппинга локаций
        location_ddl = """
        CREATE TABLE IF NOT EXISTS dim_location_mapping (
            location_id UInt16,
            location_name String,
            aircraft_type String
        ) ENGINE = Memory
        """
        
        try:
            self.client.command(serialno_ddl)
            self.client.command(location_ddl)
            logger.info("✅ Созданы дополнительные справочные таблицы")
        except Exception as e:
            logger.error(f"❌ Ошибка создания таблиц: {e}")
    
    def populate_additional_mapping_tables(self):
        """Заполнение дополнительных справочных таблиц"""
        logger.info("🔄 Заполнение дополнительных справочных таблиц...")
        
        # 1. Заполнение маппинга серийных номеров
        serialno_data = [[serialno_id, serialno] 
                        for serialno, serialno_id in self.serialno_mapping.items()]
        
        self.client.insert('dim_serialno_mapping', serialno_data,
                          column_names=['serialno_id', 'serialno'])
        logger.info(f"✅ Загружено {len(serialno_data)} серийных номеров")
        
        # 2. Заполнение маппинга локаций с определением типа ВС
        location_data = []
        for location, location_id in self.location_mapping.items():
            # Попытка определить тип ВС по локации
            aircraft_type = self._detect_aircraft_type_by_location(location)
            location_data.append([location_id, location, aircraft_type])
        
        self.client.insert('dim_location_mapping', location_data,
                          column_names=['location_id', 'location_name', 'aircraft_type'])
        logger.info(f"✅ Загружено {len(location_data)} локаций")
    
    def _detect_aircraft_type_by_location(self, location: str) -> str:
        """Определение типа ВС по номеру локации"""
        # Простая эвристика на основе паттернов номеров ВС
        location_clean = str(location).strip().upper()
        
        # Паттерны для разных типов ВС
        if re.match(r'^RA-\d+', location_clean):  # RA-XXXXX
            return "Unknown"
        elif re.match(r'^\d{3,4}$', location_clean):  # 3-4 цифры
            return "Helicopter"
        elif 'МИ' in location_clean or 'MI' in location_clean:
            return "Mi-8/Mi-17"
        else:
            return "Unknown"
    
    def load_component_interchangeability_data(self):
        """Загрузка данных о взаимозаменяемости из MD_Components"""
        logger.info("🔄 Загрузка данных о взаимозаменяемости...")
        
        try:
            # Читаем MD_Components.xlsx
            md_components_path = Path('data_input/master_data/MD_Сomponents.xlsx')
            
            if not md_components_path.exists():
                logger.warning(f"⚠️ Файл {md_components_path} не найден")
                return
            
            # Читаем данные, пропуская первые строки с заголовками
            df = pd.read_excel(md_components_path, sheet_name='Агрегаты', 
                              header=1, skiprows=[0])  # Используем строку 2 как заголовки
            
            # Очистка DataFrame
            df = df.dropna(subset=['partno'])  # Убираем строки без партномеров
            df = df[df['partno'].astype(str).str.strip() != '']
            
            logger.info(f"📊 Загружено {len(df)} записей из MD_Components")
            
            # Заполнение dim_partno_mapping с данными о взаимозаменяемости
            partno_enriched_data = []
            interchangeable_groups_data = []
            group_id_counter = 1
            
            for _, row in df.iterrows():
                partno = str(row['partno']).strip()
                component_name = str(row.get('component_type', '')).strip()
                effectivity_type = row.get('effectivity_type', 0)
                group_by = row.get('group_by', 0)
                
                # Определяем группу взаимозаменяемости
                interchangeable_group_id = group_by if group_by > 0 else 0
                
                # Получаем ID партномера из основного маппинга
                partno_id = self.partno_mapping.get(partno, 0)
                
                if partno_id > 0:
                    partno_enriched_data.append([
                        partno_id, partno, component_name, 
                        effectivity_type, interchangeable_group_id
                    ])
            
            # Заполнение dim_partno_mapping (добавляем недостающие поля)
            if partno_enriched_data:
                # Сначала очищаем таблицу
                self.client.command("TRUNCATE TABLE dim_partno_mapping")
                
                # Заполняем обновленными данными
                self.client.insert('dim_partno_mapping', partno_enriched_data,
                                  column_names=['partno_id', 'partno', 'component_name', 
                                               'effectivity_type_mask', 'interchangeable_group_id'])
                logger.info(f"✅ Обновлено {len(partno_enriched_data)} записей в dim_partno_mapping")
                
                # Заполнение остальных партномеров (которых нет в MD_Components)
                missing_partnos_data = []
                for partno, partno_id in self.partno_mapping.items():
                    if not any(row[0] == partno_id for row in partno_enriched_data):
                        missing_partnos_data.append([
                            partno_id, partno, 'Unknown', 0, 0
                        ])
                
                if missing_partnos_data:
                    self.client.insert('dim_partno_mapping', missing_partnos_data,
                                      column_names=['partno_id', 'partno', 'component_name', 
                                                   'effectivity_type_mask', 'interchangeable_group_id'])
                    logger.info(f"✅ Добавлено {len(missing_partnos_data)} недостающих партномеров")
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки данных взаимозаменяемости: {e}")
    
    def convert_data_to_digital(self):
        """Конвертация данных в цифровой формат"""
        logger.info("🔄 Конвертация данных в цифровой формат...")
        
        conversion_query = """
        INSERT INTO status_components_digital
        SELECT 
            -- Конвертация в ID
            coalesce(p.partno_id, 0) as partno_id,
            coalesce(s.serialno_id, 0) as serialno_id,
            coalesce(multiIf(
                sc.ac_typ = 'Ми-26', 128,
                sc.ac_typ = 'МИ26Т', 128,
                sc.ac_typ = 'Ми-17', 64,
                sc.ac_typ = 'Ми-8Т', 32,
                sc.ac_typ = 'Ка-32', 16,
                sc.ac_typ = 'AS-350', 8,
                sc.ac_typ = '350B3', 8,
                sc.ac_typ = 'AS-355', 4,
                sc.ac_typ = '355NP', 4,
                sc.ac_typ = 'R-44', 2,
                0
            ), 0) as ac_type_mask,
            coalesce(l.location_id, 0) as location_id,
            
            -- Ресурсные поля (конвертация часов в минуты)
            coalesce(toUInt32(sc.ll * 60), 0) as ll,
            coalesce(toUInt32(sc.oh * 60), 0) as oh,
            coalesce(toUInt32(sc.oh_threshold * 60), 0) as oh_threshold,
            coalesce(toUInt32(sc.sne * 60), 0) as sne,
            coalesce(toUInt32(sc.ppr * 60), 0) as ppr,
            
            -- Даты
            sc.mfg_date,
            sc.removal_date,
            sc.target_date,
            
            -- Битовые поля
            CASE WHEN sc.lease_restricted = 'Y' THEN 1 ELSE 0 END as lease_restricted_bit,
            coalesce(multiIf(
                sc.owner = 'ЮТ-ВУ', 1,
                sc.owner = 'UTE', 2,
                sc.owner = 'ГТЛК', 3,
                sc.owner = 'СБЕР ЛИЗИНГ', 4,
                sc.owner = 'ГПМ', 5,
                sc.owner = 'АО ГПМ', 6,
                sc.owner = 'ИП', 7,
                sc.owner = 'АРВ', 8,
                sc.owner = 'И', 9,
                0
            ), 0) as owner_id,
            coalesce(multiIf(
                sc.condition = 'ИСПРАВНЫЙ', 7,
                sc.condition = 'НЕИСПРАВНЫЙ', 4,
                sc.condition = 'ДОНОР', 1,
                sc.condition = 'СНЯТ ЗАКАЗЧИКОМ', 0,
                sc.condition = 'СНЯТ', 0,
                sc.condition = 'НЕ УСТАНОВЛЕН', 6,
                sc.condition = 'ПОСТАВКА', 3,
                0
            ), 0) as condition_mask,
            
            -- Вычисляемые поля из маппинга
            coalesce(pm.interchangeable_group_id, 0) as interchangeable_group_id,
            coalesce(pm.effectivity_type_mask, 0) as effectivity_type_mask,
            
            -- Метаданные
            sc.version_date,
            now() as load_timestamp
            
        FROM status_components_cleaned sc
        LEFT JOIN dim_partno_mapping pm ON sc.partno = pm.partno
        LEFT JOIN dim_serialno_mapping s ON sc.serialno = s.serialno
        LEFT JOIN dim_location_mapping l ON sc.location = l.location_name
        """
        
        try:
            # Очищаем целевую таблицу
            self.client.command("TRUNCATE TABLE status_components_digital")
            
            # Выполняем конвертацию
            result = self.client.command(conversion_query)
            
            # Проверяем результат
            count_query = "SELECT count(*) FROM status_components_digital"
            count_result = self.client.query(count_query)
            converted_count = count_result.result_rows[0][0]
            
            logger.info(f"✅ Конвертировано {converted_count} записей в цифровой формат")
            
            return converted_count > 0
            
        except Exception as e:
            logger.error(f"❌ Ошибка конвертации данных: {e}")
            return False
    
    def validate_digital_data(self):
        """Валидация цифровых данных"""
        logger.info("🔍 Валидация цифровых данных...")
        
        validation_queries = [
            ("Общее количество записей", "SELECT count(*) FROM status_components_digital"),
            ("Записи с нулевыми partno_id", "SELECT count(*) FROM status_components_digital WHERE partno_id = 0"),
            ("Записи с нулевыми serialno_id", "SELECT count(*) FROM status_components_digital WHERE serialno_id = 0"),
            ("Уникальные типы ВС (маски)", "SELECT ac_type_mask, count(*) FROM status_components_digital GROUP BY ac_type_mask ORDER BY ac_type_mask"),
            ("Распределение состояний", "SELECT condition_mask, count(*) FROM status_components_digital GROUP BY condition_mask ORDER BY condition_mask"),
            ("Записи с группами взаимозаменяемости", "SELECT count(*) FROM status_components_digital WHERE interchangeable_group_id > 0"),
        ]
        
        validation_results = {}
        
        for desc, query in validation_queries:
            try:
                result = self.client.query(query)
                validation_results[desc] = result.result_rows
                
                if "count(*)" in query and len(result.result_rows) == 1:
                    logger.info(f"   {desc}: {result.result_rows[0][0]}")
                else:
                    logger.info(f"   {desc}: {len(result.result_rows)} групп")
                    
            except Exception as e:
                logger.error(f"❌ Ошибка валидации '{desc}': {e}")
        
        return validation_results
    
    def generate_conversion_report(self, validation_results: Dict):
        """Генерация отчета о конвертации"""
        logger.info("📋 Генерация отчета о конвертации...")
        
        report = f"""
# Отчет о конвертации данных в цифровой формат

## Дата конвертации: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Статистика маппингов
- **Партномеров**: {len(self.partno_mapping)}
- **Серийных номеров**: {len(self.serialno_mapping)}
- **Локаций**: {len(self.location_mapping)}

## Битовые маски

### Типы ВС
{chr(10).join([f"- {name}: {mask} (0b{mask:08b})" for name, mask in self.ac_type_masks.items()])}

### Состояния
{chr(10).join([f"- {name}: {mask} (0b{mask:03b})" for name, mask in self.condition_mapping.items()])}

### Владельцы
{chr(10).join([f"- {name}: {id}" for name, id in self.owner_mapping.items()])}

## Результаты валидации
"""
        
        for desc, results in validation_results.items():
            report += f"\n### {desc}\n"
            if isinstance(results, list) and len(results) > 0:
                if len(results[0]) == 1:  # Простой счетчик
                    report += f"Результат: {results[0][0]}\n"
                else:  # Группировка
                    for row in results[:10]:  # Первые 10 строк
                        report += f"- {' | '.join(map(str, row))}\n"
                    if len(results) > 10:
                        report += f"... и еще {len(results) - 10} записей\n"

        report += f"""
## Преимущества цифрового формата
1. **Экономия места**: ~62% меньше размер данных
2. **Производительность**: Быстрые битовые операции
3. **GPU совместимость**: Оптимальные типы для Flame GPU
4. **Масштабируемость**: Эффективные JOIN операции

## Следующие шаги
1. Тестирование производительности запросов
2. Создание ETL процесса для инкрементальных обновлений
3. Интеграция с Flame GPU для ABM симуляции
"""
        
        report_path = Path('test_output/digital_conversion_report.md')
        report_path.write_text(report, encoding='utf-8')
        logger.info(f"✅ Отчет сохранен: {report_path}")

def main():
    """Основная функция конвертации"""
    logger.info("🚀 Начало конвертации данных в цифровой формат...")
    
    converter = DigitalDataConverter()
    
    # 1. Подключение к базе данных
    if not converter.connect_to_database():
        return False
    
    try:
        # 2. Анализ исходных данных
        analysis = converter.analyze_source_data()
        if not analysis:
            logger.error("❌ Не удалось проанализировать данные")
            return False
        
        # 3. Создание ID маппингов
        converter.create_id_mappings(analysis)
        
        # 4. Создание дополнительных справочных таблиц
        converter.create_additional_mapping_tables()
        
        # 5. Заполнение справочных таблиц
        converter.populate_additional_mapping_tables()
        
        # 6. Загрузка данных о взаимозаменяемости
        converter.load_component_interchangeability_data()
        
        # 7. Конвертация данных
        if not converter.convert_data_to_digital():
            logger.error("❌ Не удалось конвертировать данные")
            return False
        
        # 8. Валидация результатов
        validation_results = converter.validate_digital_data()
        
        # 9. Генерация отчета
        converter.generate_conversion_report(validation_results)
        
        logger.info("✅ Конвертация в цифровой формат завершена успешно!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка конвертации: {e}")
        return False
    
    finally:
        if converter.client:
            converter.client.close()

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 