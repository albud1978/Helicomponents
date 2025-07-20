#!/usr/bin/env python3
"""
ETL Master - главный оркестратор для системы Helicopter Component Lifecycle

Микросервисная архитектура:
- Централизованное управление версиями и политиками
- Координация всех ETL загрузчиков
- Единая точка входа с выбором режима (тест/прод)
- Быстрое тестирование с полной перезагрузкой
"""

import subprocess
import sys
import time
import logging
from pathlib import Path
from datetime import date, datetime
from typing import List, Dict, Optional

# Добавляем путь к утилитам
sys.path.append(str(Path(__file__).parent / 'utils'))
from config_loader import get_clickhouse_client
from etl_version_manager import ETLVersionManager
import openpyxl
import os

# Настройка логгирования
Path('logs').mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl_master.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def extract_unified_version_date():
    """
    КОСТЫЛЬ: Извлекает единую version_date из Status_Components.xlsx
    для использования всеми загрузчиками (чтобы избежать разброса дат)
    """
    try:
        status_path = Path('data_input/source_data/Status_Components.xlsx')
        logger.info(f"📅 Извлечение единой version_date из {status_path.name}...")
        
        # Открываем Excel файл для чтения метаданных
        workbook = openpyxl.load_workbook(status_path, read_only=True)
        props = workbook.properties
        
        current_year = datetime.now().year
        version_source = "unknown"
        version_date = date.today()
        
        # Приоритет 1: дата создания файла (с проверкой года)
        if props.created:
            created_date = props.created
            if abs(created_date.year - current_year) <= 1:
                version_date = created_date.date()
                version_source = "Excel created"
                logger.info(f"📅 Дата создания Excel: {created_date}")
            else:
                logger.warning(f"⚠️ Дата создания {created_date} отличается от текущего года более чем на год")
        
        # Приоритет 2: дата модификации
        if props.modified and version_source == "unknown":
            version_date = props.modified.date()
            version_source = "Excel modified"
            logger.info(f"📅 Дата модификации Excel: {props.modified}")
        elif props.modified:
            logger.info(f"📅 Дата модификации Excel: {props.modified}")
        
        # Приоритет 3: время модификации файла в ОС
        if version_source == "unknown":
            mtime = os.path.getmtime(status_path)
            version_date = datetime.fromtimestamp(mtime).date()
            version_source = "OS modified"
        
        # Дополнительная информация
        file_stats = os.stat(status_path)
        logger.info(f"📋 Файл: {status_path.name}")
        logger.info(f"📏 Размер: {file_stats.st_size:,} байт")
        logger.info(f"🕐 Модификация ОС: {datetime.fromtimestamp(file_stats.st_mtime)}")
        logger.info(f"🎯 Источник версии: {version_source}")
        
        workbook.close()
        
        logger.info(f"✅ Единая version_date для всех загрузчиков: {version_date}")
        return version_date
        
    except Exception as e:
        logger.error(f"❌ Ошибка извлечения версии из Status_Components.xlsx: {e}")
        fallback_date = date.today()
        logger.warning(f"🚨 Используем fallback дату: {fallback_date}")
        return fallback_date

class ETLMaster:
    """Главный оркестратор ETL системы"""
    
    # Конфигурация ETL пайплайна в правильном порядке
    ETL_PIPELINE = [
        {
            'script': 'md_components_loader.py',
            'description': 'MD Components - мастер-данные компонентов',
            'dependencies': [],
            'result_table': 'md_components',
            'critical': True  # Критичен для фильтрации
        },
        {
            'script': 'status_overhaul_loader.py', 
            'description': 'Status & Overhaul - статусы и капремонт',
            'dependencies': [],
            'result_table': 'status_overhaul',
            'critical': True
        },
        {
            'script': 'program_ac_loader.py',
            'description': 'Program AC - связка программ и ВС',
            'dependencies': [],
            'result_table': 'program_ac', 
            'critical': True
        },
        {
            'script': 'dual_loader.py',
            'description': 'Status Components - основные данные + процессинг',
            'dependencies': ['md_components', 'status_overhaul', 'program_ac'],
            'result_table': 'heli_pandas',
            'critical': True
        },
        {
            'script': 'enrich_heli_pandas.py',
            'description': 'Обогащение ac_type_mask',
            'dependencies': ['heli_pandas'],
            'result_table': 'heli_pandas',
            'critical': False
        },
        {
            'script': 'dictionary_creator.py',
            'description': 'Все справочники (статусы, партномера, серийники, владельцы, типы ВС, номера ВС)',
            'dependencies': ['heli_pandas'],
            'result_table': 'dict_status_flat',
            'critical': False
        },
        {
            'script': 'calculate_beyond_repair.py',
            'description': 'Расчет Beyond Repair (br)',
            'dependencies': ['md_components'],
            'result_table': 'md_components', 
            'critical': False
        },
        {
            'script': 'md_components_enricher.py',
            'description': 'Обогащение MD Components',
            'dependencies': ['md_components', 'heli_pandas'],
            'result_table': 'md_components',
            'critical': False
        },
        # === ТЕНЗОРЫ (в самом конце, когда все данные готовы) ===
        {
            'script': 'program_fl_direct_loader.py',
            'description': 'Flight Program FL Direct - прямой тензор программ полетов на 4000 дней',
            'dependencies': ['dict_aircraft_number_flat'],
            'result_table': 'flight_program_fl',
            'critical': False
        },
        {
            'script': 'program_ac_direct_loader.py',
            'description': 'Flight Program AC Direct - прямой тензор операций ВС на 4000 дней с постпроцессингом',
            'dependencies': ['heli_pandas', 'md_components'],
            'result_table': 'flight_program_ac',
            'critical': False
        },
        # === МЕТА-СЛОВАРЬ (финальный этап после всех таблиц) ===
        {
            'script': 'digital_values_dictionary_creator.py',
            'description': 'Digital Values Dictionary - аддитивный словарь всех полей для Flame GPU macroproperty',
            'dependencies': ['heli_pandas', 'md_components', 'flight_program_ac', 'flight_program_fl'],
            'result_table': 'dict_digital_values_flat',
            'critical': False
        }
    ]
    
    def __init__(self):
        """Инициализация ETL Master"""
        self.client = None
        self.version_manager = None
        self.version_date = None
        self.version_id = None
        self.mode = None  # 'test' или 'prod'
        
    def initialize(self) -> bool:
        """Инициализация подключений и менеджеров"""
        try:
            # Подключение к ClickHouse
            self.client = get_clickhouse_client()
            if not self.client:
                logger.error("❌ Не удалось подключиться к ClickHouse")
                return False
                
            # Инициализация версионного менеджера
            self.version_manager = ETLVersionManager(self.client)
            
            logger.info("✅ ETL Master инициализирован")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации: {e}")
            return False
    
    def select_mode(self) -> bool:
        """Выбор режима работы: тест или прод"""
        print("\n" + "="*70)
        print("🎯 ETL MASTER - HELICOPTER COMPONENT LIFECYCLE")
        print("="*70)
        print("\n🔧 Выберите режим работы:")
        print("1. 🧪 ТЕСТ - удалить ВСЕ таблицы и создать заново (быстро)")
        print("2. 🏭 ПРОД - дополнить существующие данные (версионирование)")
        print("3. ❌ ОТМЕНА")
        
        while True:
            try:
                choice = input("\nВаш выбор (1/2/3): ").strip()
                
                if choice == '1':
                    self.mode = 'test'
                    logger.info("🧪 Выбран ТЕСТОВЫЙ режим - полная перезагрузка")
                    return True
                elif choice == '2':
                    self.mode = 'prod'
                    logger.info("🏭 Выбран ПРОДОВЫЙ режим - версионирование")
                    return True
                elif choice == '3':
                    logger.info("❌ Операция отменена пользователем")
                    return False
                else:
                    print("❌ Неверный выбор. Введите 1, 2 или 3.")
                    
            except KeyboardInterrupt:
                print("\n❌ Операция отменена пользователем")
                return False
    
    def prepare_test_mode(self) -> bool:
        """Подготовка тестового режима - удаление всех таблиц"""
        try:
            logger.info("🧪 === РЕЖИМ ТЕСТ: ПОЛНАЯ ОЧИСТКА ===")
            
            # Список ТОЛЬКО таблиц которые создаются текущим ETL пайплайном
            # ЗАЩИЩЕНЫ ОТ УДАЛЕНИЯ: 
            # - OlapCube_VNV (cycle_full9.py), Heli_Components (analytic_CPU.py), Helicopter_Components, OlapCube_Analytics (демо-стенд)
            # - ИСТИННО АДДИТИВНЫЕ СЛОВАРИ: dict_partno_flat, dict_serialno_flat, dict_owner_flat, dict_ac_type_flat, aircraft_number_dict (MergeTree)
            tables_to_drop = [
                # Dictionary объекты (создаются dictionary_creator.py)
                'aircraft_number_dictionary',      # legacy Dictionary объект
                'status_dict_flat',                # Dictionary объект для статусов
                'partno_dict_flat',               # Dictionary объект для партномеров  
                'serialno_dict_flat',             # Dictionary объект для серийников
                'owner_dict_flat',                # Dictionary объект для владельцев
                'ac_type_dict_flat',              # Dictionary объект для типов ВС
                'aircraft_number_dict_flat',      # Dictionary объект для номеров ВС
                'digital_values_dict_flat',       # Dictionary объект для цифровых значений полей
                
                # Основные таблицы ETL пайплайна
                'heli_pandas', 'heli_raw',           # создается dual_loader.py  
                'md_components',                     # создается md_components_loader.py
                'status_overhaul',                   # создается status_overhaul_loader.py
                'program_ac',                        # создается program_ac_loader.py
                'flight_program_fl',                 # создается program_fl_direct_loader.py
                'flight_program_ac',                 # создается program_ac_direct_loader.py
                
                # ИСКЛЮЧЕНЫ ИЗ УДАЛЕНИЯ - ИСТИННО АДДИТИВНЫЕ СЛОВАРНЫЕ ТАБЛИЦЫ (MergeTree):
                # 'dict_partno_flat', 'dict_serialno_flat', 'dict_owner_flat',   # создается dictionary_creator.py (ИСТИННО АДДИТИВНЫЕ)
                # 'dict_ac_type_flat', 'dict_aircraft_number_flat'               # создается dictionary_creator.py (ИСТИННО АДДИТИВНЫЕ)
                # 'dict_digital_values_flat'                                     # создается digital_values_dictionary_creator.py (ИСТИННО АДДИТИВНЫЙ)
                
                # Не-аддитивная таблица статуса (пересоздается каждый раз)
                'dict_status_flat'  # создается dictionary_creator.py (единственная не-аддитивная)
            ]
            
            print(f"\n🗑️ Удаление {len(tables_to_drop)} таблиц проекта...")
            print("🛡️ ЗАЩИЩЕНЫ от удаления: истинно аддитивные словари (dict_partno_flat, dict_serialno_flat, dict_owner_flat, dict_ac_type_flat, aircraft_number_dict)")
            deleted_count = 0
            
            for table in tables_to_drop:
                try:
                    # Специальная обработка для Dictionary объектов
                    dictionary_objects = [
                        'aircraft_number_dictionary', 'status_dict_flat', 'partno_dict_flat',
                        'serialno_dict_flat', 'owner_dict_flat', 'ac_type_dict_flat', 'aircraft_number_dict_flat',
                        'digital_values_dict_flat'
                    ]
                    
                    if table in dictionary_objects:
                        # Проверяем существование Dictionary
                        dict_exists = self.client.execute(f"""
                            SELECT COUNT(*) FROM system.dictionaries 
                            WHERE database = 'default' AND name = '{table}'
                        """)[0][0] > 0
                        
                        if dict_exists:
                            self.client.execute(f"DROP DICTIONARY {table}")
                            logger.info(f"✅ Удален Dictionary: {table}")
                            deleted_count += 1
                        else:
                            logger.debug(f"⏭️ Dictionary {table} не существует")
                    else:
                        # Обычные таблицы
                        exists = self.client.execute(f"EXISTS TABLE {table}")[0][0]
                        if exists:
                            self.client.execute(f"DROP TABLE {table}")
                            logger.info(f"✅ Удалена таблица: {table}")
                            deleted_count += 1
                        else:
                            logger.debug(f"⏭️ Таблица {table} не существует")
                        
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка удаления {table}: {e}")
            
            # В тестовом режиме всегда version_id = 1
            # КОСТЫЛЬ: используем единую дату из Status_Components.xlsx для всех загрузчиков
            self.version_date = extract_unified_version_date()
            self.version_id = 1
            
            logger.info(f"✅ Тестовый режим подготовлен: удалено {deleted_count} таблиц")
            logger.info(f"🎯 Единая версия для всех загрузчиков: {self.version_date} (version_id=1)")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка подготовки тестового режима: {e}")
            return False
    
    def prepare_prod_mode(self) -> bool:
        """Подготовка продового режима - версионирование"""
        try:
            logger.info("🏭 === РЕЖИМ ПРОД: ВЕРСИОНИРОВАНИЕ ===")
            
            # Добавляем поля version_id в существующие таблицы
            if not self.version_manager.add_version_id_fields():
                logger.error("❌ Ошибка добавления полей version_id")
                return False
            
            # Определяем дату версии (можно задать параметром или взять текущую)
            self.version_date = date.today()
            
            # Обрабатываем политику версионирования
            policy, version_id = self.version_manager.handle_version_policy(self.version_date)
            
            if policy == 'cancel':
                logger.info("❌ Загрузка отменена пользователем")
                return False
            
            self.version_id = version_id
            
            # Выполняем политику перезаписи если выбрана
            if policy == 'rewrite':
                if not self.version_manager.execute_rewrite_policy(self.version_date):
                    logger.error("❌ Ошибка выполнения политики перезаписи")
                    return False
            
            logger.info(f"✅ Продовый режим подготовлен")
            logger.info(f"🎯 Версия: {self.version_date} (version_id={self.version_id})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка подготовки продового режима: {e}")
            return False
    
    def run_microservice(self, step: Dict) -> bool:
        """Запуск отдельного ETL микросервиса"""
        script_name = step['script']
        description = step['description']
        
        logger.info(f"🚀 Запуск микросервиса: {script_name}")
        logger.info(f"📋 Описание: {description}")
        
        script_path = Path('code') / script_name
        
        if not script_path.exists():
            logger.error(f"❌ Скрипт не найден: {script_path}")
            return False
        
        try:
            start_time = time.time()
            
            # Формируем команду с параметрами версионирования
            cmd_with_params = [
                sys.executable, str(script_path),
                '--version-date', str(self.version_date),
                '--version-id', str(self.version_id)
            ]
            
            # Сначала пробуем с параметрами версионирования
            result = subprocess.run(
                cmd_with_params,
                capture_output=True,
                text=True,
                timeout=1800,  # 30 минут максимум
                cwd=Path.cwd()  # Запускаем из корневой директории
            )
            
            # Если скрипт не поддерживает версионирование, пробуем без параметров
            if result.returncode != 0 and ("unrecognized arguments" in result.stderr or "unknown option" in result.stderr):
                logger.warning(f"⚠️ Скрипт {script_name} не поддерживает версионирование, запускаем без параметров")
                
                cmd_without_params = [sys.executable, str(script_path)]
                
                result = subprocess.run(
                    cmd_without_params,
                    capture_output=True,
                    text=True,
                    timeout=1800,
                    cwd=Path.cwd()
                )
            
            execution_time = time.time() - start_time
            
            if result.returncode == 0:
                logger.info(f"✅ Микросервис {script_name} завершен успешно за {execution_time:.1f}с")
                
                # Показываем последние строки вывода
                if result.stdout:
                    stdout_lines = result.stdout.strip().split('\n')
                    logger.info("📊 Последние строки вывода:")
                    for line in stdout_lines[-3:]:
                        logger.info(f"   {line}")
                
                return True
            else:
                logger.error(f"❌ Микросервис {script_name} завершился с ошибкой (код: {result.returncode})")
                
                if result.stderr:
                    logger.error("❌ STDERR:")
                    for line in result.stderr.strip().split('\n'):
                        logger.error(f"   {line}")
                
                if result.stdout:
                    logger.error("❌ STDOUT:")
                    for line in result.stdout.strip().split('\n'):
                        logger.error(f"   {line}")
                
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"❌ Микросервис {script_name} превысил время выполнения (30 минут)")
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка запуска микросервиса {script_name}: {e}")
            return False
    
    def validate_dependencies(self, step: Dict) -> bool:
        """Проверка зависимостей для этапа"""
        dependencies = step.get('dependencies', [])
        if not dependencies:
            return True
        
        logger.debug(f"🔍 Проверка зависимостей для {step['script']}: {dependencies}")
        
        for table_name in dependencies:
            try:
                exists = self.client.execute(f"EXISTS TABLE {table_name}")[0][0]
                if not exists:
                    logger.warning(f"⚠️ Зависимость {table_name} не найдена, но продолжаем")
                    return True  # Продолжаем даже если зависимость отсутствует
                
                count = self.client.execute(f"SELECT count() FROM {table_name}")[0][0]
                logger.debug(f"✅ Зависимость {table_name}: {count:,} записей")
                
            except Exception as e:
                logger.warning(f"⚠️ Ошибка проверки зависимости {table_name}: {e}")
        
        return True
    
    def validate_result(self, step: Dict) -> Dict:
        """Валидация результата этапа"""
        result_table = step.get('result_table')
        if not result_table:
            return {'success': True, 'message': 'Нет таблицы для проверки'}
        
        try:
            # Проверяем существование таблицы
            exists = self.client.execute(f"EXISTS TABLE {result_table}")[0][0]
            if not exists:
                return {'success': False, 'message': f'Таблица {result_table} не создана'}
            
            # Проверяем наличие поля version_id
            has_version_id = self.client.execute(f"""
                SELECT count() 
                FROM system.columns 
                WHERE table = '{result_table}' AND name = 'version_id'
            """)[0][0] > 0
            
            if has_version_id:
                # Таблица поддерживает версионирование - считаем для конкретной версии
                count_sql = f"""
                SELECT count() 
                FROM {result_table} 
                WHERE version_date = '{self.version_date}' AND version_id = {self.version_id}
                """
                
                count = self.client.execute(count_sql)[0][0]
                
                if count == 0:
                    return {'success': False, 'message': f'Нет данных в {result_table} для версии {self.version_date}v{self.version_id}'}
                
                return {'success': True, 'message': f'{result_table}: {count:,} записей (версия {self.version_id})'}
            else:
                # Таблица еще не поддерживает версионирование - считаем общее количество
                count_sql = f"SELECT count() FROM {result_table}"
                count = self.client.execute(count_sql)[0][0]
                
                if count == 0:
                    return {'success': False, 'message': f'Нет данных в {result_table}'}
                
                return {'success': True, 'message': f'{result_table}: {count:,} записей (без версионирования)'}
            
        except Exception as e:
            return {'success': False, 'message': f'Ошибка проверки {result_table}: {e}'}
    
    def run_pipeline(self) -> bool:
        """Запуск полного ETL пайплайна"""
        logger.info("🚀 === ЗАПУСК ETL ПАЙПЛАЙНА ===")
        
        total_steps = len(self.ETL_PIPELINE)
        success_count = 0
        failed_steps = []
        
        for i, step in enumerate(self.ETL_PIPELINE, 1):
            logger.info(f"\n📋 ЭТАП {i}/{total_steps}: {step['script']}")
            
            # Проверка зависимостей
            if not self.validate_dependencies(step):
                logger.warning(f"⚠️ Проблемы с зависимостями для {step['script']}, но продолжаем")
            
            # Запуск микросервиса
            success = self.run_microservice(step)
            
            if success:
                success_count += 1
                
                # Валидация результата
                validation = self.validate_result(step)
                if validation['success']:
                    logger.info(f"✅ ЭТАП {i} завершен: {validation['message']}")
                else:
                    logger.warning(f"⚠️ ЭТАП {i} завершен с предупреждениями: {validation['message']}")
            else:
                failed_steps.append(step['script'])
                
                if step['critical']:
                    logger.error(f"❌ КРИТИЧЕСКИЙ ЭТАП {i} провален: {step['script']}")
                    logger.error("🛑 Останавливаем пайплайн из-за критической ошибки")
                    break
                else:
                    logger.warning(f"⚠️ НЕКРИТИЧЕСКИЙ ЭТАП {i} провален: {step['script']}, продолжаем")
        
        # Итоговая статистика
        logger.info(f"\n📊 === ИТОГИ ПАЙПЛАЙНА ===")
        logger.info(f"✅ Успешно: {success_count}/{total_steps} этапов")
        logger.info(f"🎯 Версия данных: {self.version_date} (version_id={self.version_id})")
        logger.info(f"🔧 Режим: {self.mode.upper()}")
        
        if failed_steps:
            logger.warning(f"⚠️ Проваленные этапы: {', '.join(failed_steps)}")
        
        # Финальная проверка системы
        self.final_validation()
        
        return success_count == total_steps
    
    def final_validation(self):
        """Финальная валидация готовности системы"""
        logger.info("\n🔍 === ФИНАЛЬНАЯ ВАЛИДАЦИЯ ===")
        
        # Ключевые таблицы для GPU
        critical_tables = ['heli_pandas', 'md_components', 'status_overhaul', 'program_ac']
        
        all_ready = True
        total_records = 0
        
        for table in critical_tables:
            try:
                exists = self.client.execute(f"EXISTS TABLE {table}")[0][0]
                if exists:
                    count = self.client.execute(f"SELECT count() FROM {table}")[0][0]
                    total_records += count
                    
                    # Проверяем наличие поля version_id
                    has_version_id = self.client.execute(f"""
                        SELECT count() 
                        FROM system.columns 
                        WHERE table = '{table}' AND name = 'version_id'
                    """)[0][0] > 0
                    
                    if has_version_id:
                        version_count = self.client.execute(
                            f"SELECT count() FROM {table} WHERE version_date = '{self.version_date}' AND version_id = {self.version_id}"
                        )[0][0]
                        
                        logger.info(f"✅ {table}: {count:,} записей всего, {version_count:,} для версии {self.version_id}")
                        
                        if version_count == 0:
                            all_ready = False
                    else:
                        logger.info(f"✅ {table}: {count:,} записей (без версионирования)")
                        
                        # Для таблиц без версионирования считаем готовными если есть данные
                        if count == 0:
                            all_ready = False
                else:
                    logger.error(f"❌ Критическая таблица {table} отсутствует")
                    all_ready = False
                    
            except Exception as e:
                logger.error(f"❌ Ошибка проверки {table}: {e}")
                all_ready = False
        
        if all_ready:
            logger.info(f"\n🎉 СИСТЕМА ГОТОВА ДЛЯ FLAME GPU!")
            logger.info(f"📊 Общий объем данных: {total_records:,} записей")
            logger.info(f"🚀 Можно запускать Agent-Based моделирование")
        else:
            logger.warning(f"\n⚠️ Система требует дополнительной настройки")

def main():
    """Главная функция ETL Master"""
    master = ETLMaster()
    
    try:
        # Инициализация
        if not master.initialize():
            sys.exit(1)
        
        # Выбор режима
        if not master.select_mode():
            sys.exit(0)
        
        # Подготовка в зависимости от режима
        if master.mode == 'test':
            if not master.prepare_test_mode():
                sys.exit(1)
        elif master.mode == 'prod':
            if not master.prepare_prod_mode():
                sys.exit(1)
        
        # Запуск пайплайна
        start_time = time.time()
        success = master.run_pipeline()
        total_time = time.time() - start_time
        
        logger.info(f"\n⏱️ Общее время выполнения: {total_time:.1f} секунд")
        
        if success:
            logger.info("🎉 ETL ПАЙПЛАЙН ЗАВЕРШЕН УСПЕШНО!")
            sys.exit(0)
        else:
            logger.warning("⚠️ ETL ПАЙПЛАЙН ЗАВЕРШЕН С ОШИБКАМИ")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("\n❌ ETL Master прерван пользователем")
        sys.exit(130)
    except Exception as e:
        logger.error(f"💥 Критическая ошибка ETL Master: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 