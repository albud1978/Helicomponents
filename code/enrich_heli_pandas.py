#!/usr/bin/env python3
"""
Упрощенное обогащение таблицы heli_pandas для Flame GPU
Заполняет поле ac_type_mask для multihot битовых масок типов ВС

Встроенные ID поля (partseqno_i, psn, address_i, ac_type_i) теперь поступают 
напрямую из Excel файла, поэтому генерация словарей больше не нужна.
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
    """Обогащение heli_pandas числовыми полями для GPU"""
    
    def __init__(self):
        """Инициализация обогатителя"""
        self.logger = self._setup_logging()
        self.config = load_clickhouse_config()
        
        # Исправляем конфигурацию для работы с ClickHouse
        self.config['port'] = 8123  # HTTP порт
        if 'settings' in self.config:
            self.config['settings'] = {k: v for k, v in self.config['settings'].items() if k != 'use_numpy'}
        
        self.client = None
        
        # Битовые маски для типов ВС (из созданных словарей)
        self.ac_type_masks = {
            'Ми-26': 128,    # 0b10000000
            'МИ26Т': 128,    # 0b10000000 (тот же Ми-26)
            'Ми-17': 64,     # 0b01000000  
            'МИ171': 64,     # 0b01000000 (вариант Ми-17)
            '171А2': 64,     # 0b01000000 (вариант Ми-17)
            'МИ171Е': 64,    # 0b01000000 (экспортный Ми-17)
            'Ми-8Т': 32,     # 0b00100000
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
        """Проверка структуры таблицы heli_pandas (ac_type_mask уже должен быть в схеме)"""
        self.logger.info("🔍 Проверка структуры таблицы heli_pandas...")
        
        try:
            # Проверяем что ac_type_mask уже есть в схеме таблицы
            structure_result = self.client.query("DESCRIBE heli_pandas")
            columns = [row[0] for row in structure_result.result_rows]
            
            if 'ac_type_mask' in columns:
                self.logger.info("✅ Колонка ac_type_mask найдена в схеме heli_pandas")
            else:
                self.logger.warning("⚠️ Колонка ac_type_mask отсутствует в схеме!")
                return False
            
            # Проверяем встроенные ID поля
            embedded_fields = ['partseqno_i', 'psn', 'address_i', 'ac_type_i']
            missing_embedded = [field for field in embedded_fields if field not in columns]
            present_embedded = [field for field in embedded_fields if field in columns]
            
            if missing_embedded:
                self.logger.warning(f"⚠️ Отсутствующие встроенные ID поля: {missing_embedded}")
            
            if present_embedded:
                self.logger.info(f"💡 Встроенные ID поля в схеме: {present_embedded}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки структуры: {e}")
            return False
    
    def load_dictionary_mappings(self) -> Dict[str, Dict]:
        """Загрузка маппингов только для ac_type_mask (остальные ID уже в Excel)"""
        self.logger.info("📖 Загрузка маппингов для типов ВС...")
        
        mappings = {}
        
        try:
            # Загружаем только маппинги типов ВС для битовых масок (multihot)
            # partno_id, serialno_id, owner_id теперь поступают напрямую из Excel как partseqno_i, psn, address_i
            
            # Проверяем существование таблицы словаря типов ВС
            check_table = self.client.query("SELECT COUNT(*) FROM system.tables WHERE name = 'dict_ac_type_flat'")
            if check_table.result_rows[0][0] == 0:
                self.logger.warning("⚠️ Таблица dict_ac_type_flat не найдена, используем встроенные маски")
                # Fallback на встроенные битовые маски
                mappings['ac_typ'] = self.ac_type_masks
            else:
                # Загружаем из таблицы
                ac_type_result = self.client.query("SELECT ac_type_mask, ac_typ FROM dict_ac_type_flat")
                mappings['ac_typ'] = {row[1]: row[0] for row in ac_type_result.result_rows}
            
            self.logger.info(f"✅ Загружено маппингов:")
            self.logger.info(f"  📋 ac_typ (битовые маски): {len(mappings['ac_typ'])} значений")
            self.logger.info(f"💡 ID поля партномеров, серийников, владельцев берутся из Excel")
            
            return mappings
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки маппингов: {e}")
            # Fallback на встроенные маски
            return {'ac_typ': self.ac_type_masks}
    

    
    def enrich_ac_type_masks(self, mappings: Dict[str, Dict]) -> bool:
        """Обогащение типов ВС битовыми масками"""
        self.logger.info("🔢 Обогащение ac_type_mask...")
        
        try:
            total_enriched = 0
            for ac_typ, ac_type_mask in mappings['ac_typ'].items():
                # Правильный синтаксис ClickHouse для UPDATE
                update_query = f"""
                ALTER TABLE heli_pandas 
                UPDATE ac_type_mask = {ac_type_mask}
                WHERE ac_typ = '{ac_typ}'
                """
                
                # Проверяем сколько записей будет обновлено
                count_query = f"SELECT COUNT(*) FROM heli_pandas WHERE ac_typ = '{ac_typ}'"
                count_result = self.client.query(count_query)
                records_to_update = count_result.result_rows[0][0]
                
                if records_to_update > 0:
                    self.client.query(update_query)
                    total_enriched += records_to_update
                    self.logger.info(f"  ✅ {ac_typ} → {ac_type_mask}: обновлено {records_to_update} записей")
                else:
                    self.logger.warning(f"  ⚠️ {ac_typ}: не найдено записей для обновления")
            
            # Финальная проверка результата
            result = self.client.query("SELECT COUNT(*) FROM heli_pandas WHERE ac_type_mask > 0")
            final_enriched_count = result.result_rows[0][0]
            
            self.logger.info(f"✅ Обогащено {final_enriched_count} записей типами ВС")
            
            # Дополнительная диагностика если ничего не обогащено
            if final_enriched_count == 0:
                self.logger.warning("⚠️ ac_type_mask не обогащен! Проверяем диагностику...")
                
                # Проверяем какие типы ВС есть в данных
                ac_types_result = self.client.query("SELECT DISTINCT ac_typ, COUNT(*) FROM heli_pandas GROUP BY ac_typ ORDER BY COUNT(*) DESC")
                self.logger.info("📋 Типы ВС в данных:")
                for ac_typ, count in ac_types_result.result_rows:
                    mask_status = "✅" if ac_typ in mappings['ac_typ'] else "❌"
                    self.logger.info(f"  {mask_status} '{ac_typ}': {count} записей")
            
            return final_enriched_count > 0
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обогащения ac_typ: {e}")
            import traceback
            traceback.print_exc()
            return False
    

    
    def verify_enrichment(self) -> bool:
        """Проверка качества обогащения"""
        self.logger.info("🔍 Проверка качества обогащения...")
        
        try:
            # Общая статистика
            total_result = self.client.query("SELECT COUNT(*) FROM heli_pandas")
            total_count = total_result.result_rows[0][0]
            
            # Проверяем только ac_type_mask (встроенные ID поля уже из Excel)
            result = self.client.query("SELECT COUNT(*) FROM heli_pandas WHERE ac_type_mask > 0")
            enriched_count = result.result_rows[0][0]
            coverage = (enriched_count / total_count) * 100 if total_count > 0 else 0
            
            self.logger.info(f"📊 Статистика обогащения (всего записей: {total_count}):")
            self.logger.info(f"  ac_type_mask: {enriched_count} записей ({coverage:.1f}%)")
            
            # Статистика встроенных ID полей из Excel
            embedded_fields = ['partseqno_i', 'psn', 'address_i', 'ac_type_i']
            self.logger.info(f"💡 Статистика встроенных ID полей из Excel:")
            
            embedded_stats = {}
            for field in embedded_fields:
                try:
                    result = self.client.query(f"SELECT COUNT(*) FROM heli_pandas WHERE {field} IS NOT NULL")
                    count = result.result_rows[0][0]
                    coverage = (count / total_count) * 100 if total_count > 0 else 0
                    embedded_stats[field] = count
                    
                    if count == 0:
                        self.logger.warning(f"  {field}: 0 записей (0.0%) - поле отсутствует в Excel")
                    else:
                        self.logger.info(f"  {field}: {count} записей ({coverage:.1f}%)")
                except Exception as e:
                    self.logger.warning(f"  {field}: ошибка проверки - {e}")
                    embedded_stats[field] = -1
            
            # Общий анализ встроенных ID полей
            total_embedded = sum(1 for count in embedded_stats.values() if count > 0)
            if total_embedded == 0:
                self.logger.warning("⚠️ Все встроенные ID поля пусты - вероятно используется старый Excel без встроенных ID")
            elif total_embedded == len(embedded_fields):
                self.logger.info("✅ Все встроенные ID поля заполнены - используется новый Excel")
            else:
                self.logger.warning(f"⚠️ Частично заполнены встроенные ID поля: {total_embedded}/{len(embedded_fields)}")
            
            # Проверяем примеры обогащения ac_type_mask
            try:
                sample_result = self.client.query("""
                    SELECT ac_typ, ac_type_mask, partseqno_i, psn, address_i, ac_type_i
                    FROM heli_pandas 
                    WHERE ac_type_mask > 0 
                    LIMIT 3
                """)
                
                self.logger.info("📋 Примеры обогащенных записей:")
                for row in sample_result.result_rows:
                    self.logger.info(f"  ac_typ: '{row[0]}' → ac_type_mask: {row[1]}")
                    self.logger.info(f"  встроенные ID: partseqno_i={row[2]}, psn={row[3]}, address_i={row[4]}, ac_type_i={row[5]}")
            except Exception as sample_error:
                self.logger.warning(f"⚠️ Не удалось получить примеры: {sample_error}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки: {e}")
            return False
    
    def run_enrichment(self) -> bool:
        """Запуск упрощенного обогащения (только ac_type_mask)"""
        self.logger.info("🚀 Запуск упрощенного обогащения heli_pandas")
        self.logger.info("💡 ID поля (partseqno_i, psn, address_i, ac_type_i) загружены из Excel или заполнены NULL")
        
        try:
            # 1. Подключение
            if not self.connect_to_database():
                return False
            
            # 2. Проверка структуры таблицы (ac_type_mask уже должен быть в схеме)
            if not self.check_table_structure():
                return False
            
            # 3. Загрузка маппингов (только для типов ВС)
            mappings = self.load_dictionary_mappings()
            if not mappings:
                return False
            
            # 4. Обогащение ac_type_mask для multihot битовых масок
            if not self.enrich_ac_type_masks(mappings):
                return False
            
            # 5. Проверка качества
            if not self.verify_enrichment():
                return False
            
            self.logger.info("🎯 УПРОЩЕННОЕ ОБОГАЩЕНИЕ HELI_PANDAS ЗАВЕРШЕНО!")
            self.logger.info("✨ Встроенные ID поля из Excel (или NULL) + ac_type_mask для multihot")
            self.logger.info("🚀 Готово для загрузки в Flame GPU")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обогащения: {e}")
            return False

def main():
    """Основная функция"""
    enricher = HeliPandasEnricher()
    return 0 if enricher.run_enrichment() else 1

if __name__ == "__main__":
    exit(main()) 