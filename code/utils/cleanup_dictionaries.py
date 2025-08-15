#!/usr/bin/env python3
"""
Принудительная очистка всех словарей
===================================

Удаляет ВСЕ словарные таблицы и Dictionary объекты для чистой перезагрузки.
Используется когда аддитивные словари накопили некорректные данные.

Автор: AI Assistant  
Дата: 2025-01-18
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

# Добавляем путь к утилитам
sys.path.append(str(Path(__file__).parent))
from config_loader import load_clickhouse_config
import clickhouse_connect

class DictionaryCleanup:
    """Очистка всех словарей проекта"""
    
    def __init__(self):
        """Инициализация"""
        self.logger = self._setup_logging()
        self.config = load_clickhouse_config()
        
        # Исправляем конфигурацию для HTTP порта
        self.config['port'] = 8123
        if 'settings' in self.config:
            self.config['settings'] = {k: v for k, v in self.config['settings'].items() if k != 'use_numpy'}
        
        self.client = None
        
        # Список всех словарей проекта (таблицы + Dictionary объекты)
        self.dictionary_objects = [
            'aircraft_number_dict_flat',
            'status_dict_flat', 
            'partno_dict_flat',
            'serialno_dict_flat',
            'owner_dict_flat',
            'ac_type_dict_flat'
        ]
        
        self.dictionary_tables = [
            'dict_aircraft_number_flat',
            'dict_status_flat',
            'dict_partno_flat', 
            'dict_serialno_flat',
            'dict_owner_flat',
            'dict_ac_type_flat'
        ]
    
    def _setup_logging(self) -> logging.Logger:
        """Настройка логирования"""
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def connect_to_database(self) -> bool:
        """Подключение к ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(**self.config)
            result = self.client.query('SELECT 1 as test')
            self.logger.info("✅ Подключение к ClickHouse успешно!")
            return True
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения: {e}")
            return False
    
    def drop_dictionary_objects(self) -> int:
        """Удаление Dictionary объектов"""
        self.logger.info("🗑️ Удаление Dictionary объектов...")
        dropped_count = 0
        
        for dict_name in self.dictionary_objects:
            try:
                # Проверяем существование Dictionary
                check_query = f"EXISTS DICTIONARY {dict_name}"
                exists = self.client.query(check_query).result_rows[0][0]
                
                if exists:
                    drop_query = f"DROP DICTIONARY {dict_name}"
                    self.client.command(drop_query)
                    self.logger.info(f"✅ Удален Dictionary: {dict_name}")
                    dropped_count += 1
                else:
                    self.logger.info(f"ℹ️ Dictionary не существует: {dict_name}")
                    
            except Exception as e:
                self.logger.warning(f"⚠️ Ошибка удаления Dictionary {dict_name}: {e}")
        
        return dropped_count
    
    def drop_dictionary_tables(self) -> int:
        """Удаление словарных таблиц"""
        self.logger.info("🗑️ Удаление словарных таблиц...")
        dropped_count = 0
        
        for table_name in self.dictionary_tables:
            try:
                # Проверяем существование таблицы
                check_query = f"EXISTS TABLE {table_name}"
                exists = self.client.query(check_query).result_rows[0][0]
                
                if exists:
                    drop_query = f"DROP TABLE {table_name}"
                    self.client.command(drop_query)
                    self.logger.info(f"✅ Удалена таблица: {table_name}")
                    dropped_count += 1
                else:
                    self.logger.info(f"ℹ️ Таблица не существует: {table_name}")
                    
            except Exception as e:
                self.logger.warning(f"⚠️ Ошибка удаления таблицы {table_name}: {e}")
        
        return dropped_count
    
    def cleanup_all_dictionaries(self) -> bool:
        """Полная очистка всех словарей"""
        self.logger.info("🧹 === ПРИНУДИТЕЛЬНАЯ ОЧИСТКА СЛОВАРЕЙ ===")
        
        try:
            # 1. Удаляем Dictionary объекты
            dict_dropped = self.drop_dictionary_objects()
            
            # 2. Удаляем словарные таблицы  
            table_dropped = self.drop_dictionary_tables()
            
            total_dropped = dict_dropped + table_dropped
            self.logger.info(f"✅ Очистка завершена: удалено {total_dropped} объектов")
            self.logger.info(f"   - Dictionary объектов: {dict_dropped}")
            self.logger.info(f"   - Словарных таблиц: {table_dropped}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка очистки словарей: {e}")
            return False

def main():
    """Главная функция"""
    print("🧹 ПРИНУДИТЕЛЬНАЯ ОЧИСТКА СЛОВАРЕЙ")
    print("=" * 50)
    print("ВНИМАНИЕ: Этот скрипт удалит ВСЕ словари проекта!")
    print("Используйте только для чистой перезагрузки.")
    print()
    
    # Подтверждение
    confirm = input("Продолжить? (yes/нет): ").strip().lower()
    if confirm not in ['yes', 'y', 'да']:
        print("❌ Операция отменена")
        return
    
    # Выполняем очистку
    cleanup = DictionaryCleanup()
    
    if not cleanup.connect_to_database():
        print("❌ Не удалось подключиться к базе данных")
        return
    
    success = cleanup.cleanup_all_dictionaries()
    
    cleanup.client.close()
    
    if success:
        print("\n✅ Словари очищены! Теперь можно запускать Extract.")
        print("💡 Запустите: python3 code/extract_master.py")
    else:
        print("\n❌ Очистка завершилась с ошибками")

if __name__ == "__main__":
    main() 