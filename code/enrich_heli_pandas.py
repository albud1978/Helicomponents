#!/usr/bin/env python3
"""
Обогащение таблицы heli_pandas для Flame GPU v2.0
Заполняет ТОЛЬКО поле ac_type_mask для multihot битовых масок типов ВС

АРХИТЕКТУРА v2.0 (встроенные ID поля):
- partno_id → partseqno_i (встроенный ID из Excel) ✅ НЕ ОБРАБАТЫВАЕТСЯ
- serialno_id → psn (встроенный ID из Excel) ✅ НЕ ОБРАБАТЫВАЕТСЯ
- owner_id → address_i (встроенный ID из Excel) ✅ НЕ ОБРАБАТЫВАЕТСЯ
- ac_type_i → ac_type_i (встроенный ID из Excel) ✅ НЕ ОБРАБАТЫВАЕТСЯ
- ac_typ → ac_type_mask (битовые маски для multihot) ✅ ОБРАБАТЫВАЕТСЯ

Назначение: Заполнение ТОЛЬКО ac_type_mask через встроенные маски
"""

import sys
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Any

# Добавляем путь к утилитам
sys.path.append(str(Path(__file__).parent / 'utils'))
from config_loader import load_clickhouse_config
import clickhouse_connect

class HeliPandasEnricher:
    """Обогащение heli_pandas ТОЛЬКО полем ac_type_mask для GPU"""
    
    def __init__(self):
        """Инициализация обогатителя"""
        self.logger = self._setup_logging()
        self.config = load_clickhouse_config()
        
        # Исправляем конфигурацию для работы с ClickHouse
        self.config['port'] = 8123  # HTTP порт
        if 'settings' in self.config:
            self.config['settings'] = {k: v for k, v in self.config['settings'].items() if k != 'use_numpy'}
        
        self.client = None
        
        # Битовые маски для типов ВС (расширенный список)
        self.ac_type_masks = {
            'Ми-26': 128,    # 0b10000000
            'МИ26Т': 128,    # 0b10000000 (тот же Ми-26)
            'Ми-17': 64,     # 0b01000000  
            'МИ171': 64,     # 0b01000000 (вариант Ми-17)
            '171А2': 64,     # 0b01000000 (вариант Ми-17)
            'МИ171Е': 64,    # 0b01000000 (экспортный Ми-17)
            'Ми-8Т': 32,     # 0b00100000
            'МИ8МТВ': 32,    # 0b00100000 (вариант Ми-8Т)
            'МИ8': 32,       # 0b00100000 (базовый Ми-8)
            'МИ8АМТ': 32,    # 0b00100000 (модернизированный Ми-8)
            'КА32Т': 16,     # 0b00010000 (Камов Ка-32)
            '350B3': 8,      # 0b00001000 (Airbus H350)
            '355NP': 4,      # 0b00000100 (Airbus H355) 
            '355N': 4,       # 0b00000100 (Airbus H355 вариант)
            'R44': 2,        # 0b00000010 (Robinson R44)
            'R44I': 2,       # 0b00000010 (Robinson R44 вариант)
            'R44II': 2,      # 0b00000010 (Robinson R44 вариант)
        }
    
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
    
    def check_table_structure(self) -> bool:
        """Проверка структуры таблицы heli_pandas"""
        self.logger.info("🔍 Проверка структуры таблицы heli_pandas...")
        
        try:
            # Проверяем что ac_type_mask уже есть в схеме таблицы
            structure_result = self.client.query("DESCRIBE heli_pandas")
            columns = [row[0] for row in structure_result.result_rows]
            
            if 'ac_type_mask' in columns:
                self.logger.info("✅ Колонка ac_type_mask найдена в схеме heli_pandas")
            else:
                self.logger.error("❌ Колонка ac_type_mask отсутствует в схеме!")
                return False
            
            # Проверяем встроенные ID поля (информационно)
            embedded_fields = ['partseqno_i', 'psn', 'address_i', 'ac_type_i']
            missing_embedded = [field for field in embedded_fields if field not in columns]
            present_embedded = [field for field in embedded_fields if field in columns]
            
            if missing_embedded:
                self.logger.warning(f"⚠️ Отсутствующие встроенные ID поля: {missing_embedded}")
            
            if present_embedded:
                self.logger.info(f"💡 Встроенные ID поля в схеме: {present_embedded}")
                self.logger.info("💡 Встроенные ID поля НЕ обрабатываются (уже заполнены из Excel)")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки структуры: {e}")
            return False
    
    def validate_embedded_id_coverage(self) -> bool:
        """Валидация покрытия встроенных ID полей (информационно)"""
        self.logger.info("📊 Валидация встроенных ID полей из Excel...")
        
        try:
            # Проверяем покрытие встроенных ID полей
            coverage_result = self.client.query("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(partseqno_i) as partseqno_filled,
                    COUNT(psn) as psn_filled,
                    COUNT(address_i) as address_filled,
                    COUNT(ac_type_i) as ac_type_filled
                FROM heli_pandas
            """)
            
            if not coverage_result.result_rows:
                self.logger.warning("⚠️ Нет данных в heli_pandas")
                return False
            
            total, partseqno_filled, psn_filled, address_filled, ac_type_filled = coverage_result.result_rows[0]
            
            self.logger.info(f"📊 Покрытие встроенных ID полей (всего записей: {total:,}):")
            self.logger.info(f"  partseqno_i: {partseqno_filled:,} ({partseqno_filled/total*100:.1f}%)")
            self.logger.info(f"  psn: {psn_filled:,} ({psn_filled/total*100:.1f}%)")
            self.logger.info(f"  address_i: {address_filled:,} ({address_filled/total*100:.1f}%)")
            self.logger.info(f"  ac_type_i: {ac_type_filled:,} ({ac_type_filled/total*100:.1f}%)")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка валидации встроенных ID: {e}")
            return False
    
    def load_ac_type_mappings(self) -> Dict[str, int]:
        """Загрузка маппингов для типов ВС (встроенные маски или из словаря)"""
        self.logger.info("📖 Загрузка маппингов для типов ВС...")
        
        try:
            # Проверяем существование таблицы словаря типов ВС
            check_table = self.client.query("SELECT COUNT(*) FROM system.tables WHERE name = 'dict_ac_type_flat'")
            
            if check_table.result_rows[0][0] == 0:
                self.logger.info("💡 Таблица dict_ac_type_flat не найдена, используем встроенные маски")
                ac_type_mapping = self.ac_type_masks.copy()
            else:
                # Загружаем из таблицы словаря
                self.logger.info("📚 Загружаем маппинги из таблицы dict_ac_type_flat")
                ac_type_result = self.client.query("SELECT ac_type_mask, ac_typ FROM dict_ac_type_flat")
                ac_type_mapping = {row[1]: row[0] for row in ac_type_result.result_rows}
                
                # Дополняем встроенными масками (fallback)
                for ac_type, mask in self.ac_type_masks.items():
                    if ac_type not in ac_type_mapping:
                        ac_type_mapping[ac_type] = mask
            
            self.logger.info(f"✅ Загружено маппингов типов ВС: {len(ac_type_mapping)}")
            
            # Показываем примеры маппингов
            self.logger.info("📋 Примеры маппингов типов ВС:")
            for i, (ac_type, mask) in enumerate(list(ac_type_mapping.items())[:5]):
                self.logger.info(f"  {ac_type} → {mask} (0b{mask:08b})")
            if len(ac_type_mapping) > 5:
                self.logger.info(f"  ... и еще {len(ac_type_mapping)-5} маппингов")
            
            return ac_type_mapping
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки маппингов: {e}")
            return {}
    
    def enrich_ac_type_masks(self, ac_type_mapping: Dict[str, int]) -> bool:
        """Обогащение ac_type_mask для multihot битовых операций"""
        self.logger.info("🔧 Обогащение ac_type_mask для multihot операций...")
        
        try:
            if not ac_type_mapping:
                self.logger.error("❌ Нет маппингов для обогащения ac_type_mask")
                return False
            
            # Сначала очищаем поле
            self.client.execute("ALTER TABLE heli_pandas UPDATE ac_type_mask = 0 WHERE 1=1")
            self.logger.info("🧹 Поле ac_type_mask очищено")
            
            # Обновляем значения ac_type_mask для каждого типа ВС
            updated_count = 0
            
            for ac_type, mask in ac_type_mapping.items():
                # Экранируем кавычки в типе ВС
                escaped_ac_type = ac_type.replace("'", "''")
                
                update_query = f"""
                ALTER TABLE heli_pandas 
                UPDATE ac_type_mask = {mask}
                WHERE ac_typ = '{escaped_ac_type}'
                """
                
                self.client.execute(update_query)
                
                # Проверяем сколько записей обновилось
                count_result = self.client.query(f"SELECT COUNT(*) FROM heli_pandas WHERE ac_typ = '{escaped_ac_type}'")
                type_count = count_result.result_rows[0][0]
                
                if type_count > 0:
                    updated_count += type_count
                    self.logger.info(f"  ✅ {ac_type}: {type_count:,} записей → маска {mask}")
                else:
                    self.logger.debug(f"  ⚪ {ac_type}: 0 записей")
            
            self.logger.info(f"✅ Обогащено {updated_count:,} записей с ac_type_mask")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обогащения ac_type_mask: {e}")
            return False
    
    def verify_enrichment(self) -> bool:
        """Проверка качества обогащения"""
        self.logger.info("🔍 Проверка качества обогащения...")
        
        try:
            # Общая статистика
            total_result = self.client.query("SELECT COUNT(*) FROM heli_pandas")
            total_count = total_result.result_rows[0][0]
            
            # Проверяем ac_type_mask
            mask_result = self.client.query("SELECT COUNT(*) FROM heli_pandas WHERE ac_type_mask > 0")
            mask_count = mask_result.result_rows[0][0]
            mask_coverage = (mask_count / total_count) * 100 if total_count > 0 else 0
            
            self.logger.info(f"📊 Результаты обогащения (всего записей: {total_count:,}):")
            self.logger.info(f"  ac_type_mask > 0: {mask_count:,} ({mask_coverage:.1f}%)")
            
            # Статистика встроенных ID полей (информационно)
            self.logger.info(f"💡 Встроенные ID поля (из Excel, не обрабатываются):")
            embedded_stats = self.client.query("""
                SELECT 
                    COUNT(partseqno_i) as partseqno_filled,
                    COUNT(psn) as psn_filled,
                    COUNT(address_i) as address_filled,
                    COUNT(ac_type_i) as ac_type_filled
                FROM heli_pandas
            """)
            
            if embedded_stats.result_rows:
                partseqno_filled, psn_filled, address_filled, ac_type_filled = embedded_stats.result_rows[0]
                self.logger.info(f"  partseqno_i: {partseqno_filled:,} ({partseqno_filled/total_count*100:.1f}%)")
                self.logger.info(f"  psn: {psn_filled:,} ({psn_filled/total_count*100:.1f}%)")
                self.logger.info(f"  address_i: {address_filled:,} ({address_filled/total_count*100:.1f}%)")
                self.logger.info(f"  ac_type_i: {ac_type_filled:,} ({ac_type_filled/total_count*100:.1f}%)")
            
            # Статистика по типам ВС
            types_result = self.client.query("""
                SELECT ac_typ, ac_type_mask, COUNT(*) as count
                FROM heli_pandas 
                WHERE ac_type_mask > 0
                GROUP BY ac_typ, ac_type_mask
                ORDER BY count DESC
                LIMIT 10
            """)
            
            if types_result.result_rows:
                self.logger.info("📋 Статистика по типам ВС (топ-10):")
                for row in types_result.result_rows:
                    ac_typ, mask, count = row
                    self.logger.info(f"  {ac_typ}: маска {mask} (0b{mask:08b}) → {count:,} записей")
            
            # Проверяем примеры обогащения
            examples_result = self.client.query("""
                SELECT ac_typ, ac_type_mask, partseqno_i, psn, address_i
                FROM heli_pandas 
                WHERE ac_type_mask > 0 
                LIMIT 3
            """)
            
            if examples_result.result_rows:
                self.logger.info("📋 Примеры обогащенных записей:")
                for row in examples_result.result_rows:
                    ac_typ, mask, partseqno_i, psn, address_i = row
                    self.logger.info(f"  ac_typ: '{ac_typ}' → ac_type_mask: {mask}")
                    self.logger.info(f"    встроенные ID: partseqno_i={partseqno_i}, psn={psn}, address_i={address_i}")
            
            # Считаем обогащение успешным если покрытие ac_type_mask > 80%
            success_threshold = 80.0
            if mask_coverage >= success_threshold:
                self.logger.info(f"✅ Обогащение успешно: покрытие ac_type_mask {mask_coverage:.1f}% >= {success_threshold}%")
                return True
            else:
                self.logger.warning(f"⚠️ Низкое покрытие ac_type_mask: {mask_coverage:.1f}% < {success_threshold}%")
                return False
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки: {e}")
            return False
    
    def run_enrichment(self) -> bool:
        """Запуск обогащения ТОЛЬКО ac_type_mask"""
        self.logger.info("🚀 Запуск обогащения heli_pandas v2.0")
        self.logger.info("💡 Встроенные ID поля (partseqno_i, psn, address_i, ac_type_i) уже из Excel")
        self.logger.info("✨ Обрабатываем ТОЛЬКО ac_type_mask для multihot битовых операций")
        
        try:
            # 1. Подключение
            if not self.connect_to_database():
                return False
            
            # 2. Проверка структуры таблицы
            if not self.check_table_structure():
                return False
            
            # 3. Валидация встроенных ID полей (информационно)
            self.validate_embedded_id_coverage()
            
            # 4. Загрузка маппингов для типов ВС
            ac_type_mapping = self.load_ac_type_mappings()
            if not ac_type_mapping:
                return False
            
            # 5. Обогащение ac_type_mask для multihot битовых масок
            if not self.enrich_ac_type_masks(ac_type_mapping):
                return False
            
            # 6. Проверка качества
            if not self.verify_enrichment():
                self.logger.warning("⚠️ Проверка показала проблемы, но обогащение выполнено")
            
            self.logger.info("🎯 ОБОГАЩЕНИЕ HELI_PANDAS v2.0 ЗАВЕРШЕНО!")
            self.logger.info("💡 Встроенные ID поля из Excel (или NULL) - НЕ обрабатываются")
            self.logger.info("✨ ac_type_mask заполнен для multihot битовых операций")
            self.logger.info("🚀 Готово для загрузки в Flame GPU")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обогащения: {e}")
            return False

def main():
    """Основная функция"""
    print("🚀 === ОБОГАТИТЕЛЬ HELI_PANDAS v2.0 ===")
    print("💡 Встроенные ID поля (partseqno_i, psn, address_i, ac_type_i) уже из Excel")
    print("✨ Обрабатываем ТОЛЬКО ac_type_mask для multihot битовых операций")
    
    try:
        enricher = HeliPandasEnricher()
        success = enricher.run_enrichment()
        
        if success:
            print(f"\n🎯 === ОБОГАЩЕНИЕ ЗАВЕРШЕНО ===")
            print(f"✅ ac_type_mask заполнен для multihot операций")
            print(f"💡 Встроенные ID поля НЕ изменялись (уже из Excel)")
            print(f"🚀 Flame GPU integration готов!")
            return 0
        else:
            print(f"\n❌ === ОШИБКА ОБОГАЩЕНИЯ ===")
            return 1
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        return 1

if __name__ == "__main__":
    exit(main()) 