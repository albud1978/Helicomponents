#!/usr/bin/env python3
"""
ETL Version Manager - централизованное управление версиями данных

Обеспечивает синхронное управление version_id для всех ETL таблиц:
- heli_pandas, heli_raw, md_components, status_overhaul, program_ac, flight_program_ac, flight_program_fl

Логика версионирования:
- Новая дата = новый счетчик version_id с 1
- Та же дата = инкремент version_id
- Глобальный выбор политики для всех таблиц синхронно
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import date

logger = logging.getLogger(__name__)

class ETLVersionManager:
    """Централизованный менеджер версий для ETL системы"""
    
    # Список всех ETL таблиц для версионирования
    ETL_TABLES = [
        'heli_pandas',
        'heli_raw', 
        'md_components',
        'status_overhaul',
        'program_ac',
        'flight_program_ac',
        'flight_program_fl'
    ]
    
    def __init__(self, client):
        """
        Инициализация менеджера версий
        
        Args:
            client: ClickHouse client
        """
        self.client = client
        
    def get_existing_versions(self, version_date: date, tables: List[str] = None) -> Dict[str, List[int]]:
        """
        Получает существующие version_id для указанной даты
        
        Args:
            version_date: Дата для проверки
            tables: Список таблиц (по умолчанию все ETL таблицы)
            
        Returns:
            Dict[table_name, [version_ids]]
        """
        if tables is None:
            tables = self.ETL_TABLES
            
        existing_versions = {}
        
        for table_name in tables:
            try:
                # Проверяем существование таблицы
                check_table_sql = f"EXISTS TABLE {table_name}"
                table_exists = self.client.execute(check_table_sql)[0][0]
                
                if not table_exists:
                    logger.debug(f"Таблица {table_name} не существует")
                    existing_versions[table_name] = []
                    continue
                
                # Проверяем наличие поля version_id
                check_column_sql = f"""
                SELECT count() 
                FROM system.columns 
                WHERE table = '{table_name}' AND name = 'version_id'
                """
                
                has_version_id = self.client.execute(check_column_sql)[0][0] > 0
                
                if not has_version_id:
                    logger.debug(f"Таблица {table_name} не имеет поля version_id")
                    existing_versions[table_name] = []
                    continue
                
                # Получаем существующие version_id для даты
                query_sql = f"""
                SELECT DISTINCT version_id 
                FROM {table_name} 
                WHERE version_date = '{version_date}'
                ORDER BY version_id
                """
                
                result = self.client.execute(query_sql)
                version_ids = [row[0] for row in result] if result else []
                existing_versions[table_name] = version_ids
                
                logger.debug(f"Таблица {table_name}: найдены version_id {version_ids} для даты {version_date}")
                
            except Exception as e:
                logger.warning(f"Ошибка при проверке версий в таблице {table_name}: {e}")
                existing_versions[table_name] = []
        
        return existing_versions
    
    def get_next_version_id(self, version_date: date, tables: List[str] = None) -> int:
        """
        Определяет следующий version_id для указанной даты
        
        Args:
            version_date: Дата версии
            tables: Список таблиц для проверки
            
        Returns:
            Следующий version_id (начиная с 1)
        """
        existing_versions = self.get_existing_versions(version_date, tables)
        
        # Собираем все существующие version_id из всех таблиц
        all_version_ids = set()
        for table_versions in existing_versions.values():
            all_version_ids.update(table_versions)
        
        if not all_version_ids:
            return 1
        
        return max(all_version_ids) + 1
    
    def check_version_conflicts(self, version_date: date, tables: List[str] = None) -> Tuple[bool, int, Dict[str, List[int]]]:
        """
        Проверяет конфликты версий и определяет политику загрузки
        
        Args:
            version_date: Дата версии
            tables: Список таблиц для проверки
            
        Returns:
            Tuple[has_conflicts, next_version_id, existing_versions]
        """
        if tables is None:
            tables = self.ETL_TABLES
            
        existing_versions = self.get_existing_versions(version_date, tables)
        
        # Проверяем наличие данных в любой из таблиц
        has_conflicts = any(version_ids for version_ids in existing_versions.values())
        
        next_version_id = self.get_next_version_id(version_date, tables)
        
        return has_conflicts, next_version_id, existing_versions
    
    def handle_version_policy(self, version_date: date, tables: List[str] = None) -> Tuple[str, int]:
        """
        Обрабатывает политику версионирования с интерактивным выбором
        
        Args:
            version_date: Дата версии
            tables: Список таблиц
            
        Returns:
            Tuple[policy, version_id] где policy in ['rewrite', 'append', 'cancel']
        """
        if tables is None:
            tables = self.ETL_TABLES
            
        has_conflicts, next_version_id, existing_versions = self.check_version_conflicts(version_date, tables)
        
        if not has_conflicts:
            logger.info(f"✅ Новая дата {version_date}, начинаем с version_id = 1")
            return 'append', 1
        
        # Показываем информацию о существующих версиях
        print(f"\n⚠️ Найдены данные за дату {version_date}:")
        print("="*60)
        
        for table_name, version_ids in existing_versions.items():
            if version_ids:
                print(f"📊 {table_name}: version_id {version_ids}")
            else:
                print(f"📊 {table_name}: нет данных")
        
        print(f"\n🔄 Следующий доступный version_id: {next_version_id}")
        print("="*60)
        
        # Интерактивный выбор политики
        while True:
            print("\n🎯 Выберите действие:")
            print("1. 🔄 ПЕРЕЗАПИСАТЬ - удалить ВСЕ данные за эту дату из ВСЕХ таблиц")
            print("2. ➕ ДОПОЛНИТЬ - добавить как новую версию")
            print("3. ❌ ОТКАЗАТЬСЯ - прервать загрузку")
            
            choice = input("\nВаш выбор (1/2/3): ").strip()
            
            if choice == '1':
                logger.info("🔄 Выбрана политика: ПЕРЕЗАПИСАТЬ")
                return 'rewrite', 1
            elif choice == '2':
                logger.info(f"➕ Выбрана политика: ДОПОЛНИТЬ (version_id = {next_version_id})")
                return 'append', next_version_id
            elif choice == '3':
                logger.info("❌ Загрузка отменена пользователем")
                return 'cancel', 0
            else:
                print("❌ Неверный выбор. Попробуйте снова.")
    
    def execute_rewrite_policy(self, version_date: date, tables: List[str] = None) -> bool:
        """
        Выполняет политику перезаписи - удаляет все данные за указанную дату
        
        Args:
            version_date: Дата для удаления
            tables: Список таблиц
            
        Returns:
            True если успешно, False при ошибке
        """
        if tables is None:
            tables = self.ETL_TABLES
            
        logger.info(f"🔄 Выполнение политики ПЕРЕЗАПИСАТЬ для даты {version_date}")
        
        deleted_counts = {}
        
        for table_name in tables:
            try:
                # Проверяем существование таблицы
                check_table_sql = f"EXISTS TABLE {table_name}"
                table_exists = self.client.execute(check_table_sql)[0][0]
                
                if not table_exists:
                    logger.debug(f"Таблица {table_name} не существует, пропускаем")
                    continue
                
                # Считаем записи для удаления
                count_sql = f"SELECT count() FROM {table_name} WHERE version_date = '{version_date}'"
                count_to_delete = self.client.execute(count_sql)[0][0]
                
                if count_to_delete == 0:
                    logger.debug(f"В таблице {table_name} нет данных за дату {version_date}")
                    continue
                
                # Удаляем данные
                delete_sql = f"DELETE FROM {table_name} WHERE version_date = '{version_date}'"
                self.client.execute(delete_sql)
                
                deleted_counts[table_name] = count_to_delete
                logger.info(f"🗑️ Удалено {count_to_delete} записей из {table_name}")
                
            except Exception as e:
                logger.error(f"❌ Ошибка при удалении данных из {table_name}: {e}")
                return False
        
        total_deleted = sum(deleted_counts.values())
        logger.info(f"✅ Политика ПЕРЕЗАПИСАТЬ выполнена: удалено {total_deleted} записей из {len(deleted_counts)} таблиц")
        
        return True
    
    def add_version_id_fields(self, tables: List[str] = None) -> bool:
        """
        Добавляет поле version_id во все ETL таблицы
        
        Args:
            tables: Список таблиц (по умолчанию все ETL таблицы)
            
        Returns:
            True если успешно
        """
        if tables is None:
            tables = self.ETL_TABLES
            
        logger.info("🔧 Добавление поля version_id в ETL таблицы...")
        
        for table_name in tables:
            try:
                # Проверяем существование таблицы
                check_table_sql = f"EXISTS TABLE {table_name}"
                exists = self.client.execute(check_table_sql)[0][0]
                
                if not exists:
                    logger.warning(f"⚠️ Таблица {table_name} не существует, пропускаем")
                    continue
                
                # Проверяем наличие поля version_id
                check_column_sql = f"""
                SELECT count() 
                FROM system.columns 
                WHERE table = '{table_name}' AND name = 'version_id'
                """
                
                column_exists = self.client.execute(check_column_sql)[0][0] > 0
                
                if column_exists:
                    logger.info(f"✅ Поле version_id уже существует в {table_name}")
                    continue
                
                # Добавляем поле version_id
                alter_sql = f"""
                ALTER TABLE {table_name} 
                ADD COLUMN `version_id` UInt8 DEFAULT 1
                """
                
                self.client.execute(alter_sql)
                logger.info(f"✅ Поле version_id добавлено в {table_name}")
                
            except Exception as e:
                logger.error(f"❌ Ошибка при обработке таблицы {table_name}: {e}")
                return False
        
        logger.info("✅ Добавление полей version_id завершено")
        return True
    
    def validate_version_consistency(self, version_date: date, version_id: int, tables: List[str] = None) -> Dict[str, int]:
        """
        Проверяет консистентность версий между таблицами
        
        Args:
            version_date: Дата версии
            version_id: ID версии
            tables: Список таблиц
            
        Returns:
            Dict[table_name, record_count] для указанной версии
        """
        if tables is None:
            tables = self.ETL_TABLES
            
        version_counts = {}
        
        for table_name in tables:
            try:
                # Проверяем существование таблицы
                check_table_sql = f"EXISTS TABLE {table_name}"
                table_exists = self.client.execute(check_table_sql)[0][0]
                
                if not table_exists:
                    version_counts[table_name] = 0
                    continue
                
                # Считаем записи для версии
                count_sql = f"""
                SELECT count() 
                FROM {table_name} 
                WHERE version_date = '{version_date}' AND version_id = {version_id}
                """
                
                count = self.client.execute(count_sql)[0][0]
                version_counts[table_name] = count
                
            except Exception as e:
                logger.warning(f"Ошибка при проверке консистентности {table_name}: {e}")
                version_counts[table_name] = -1  # Ошибка
        
        return version_counts 