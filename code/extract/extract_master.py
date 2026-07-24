#!/usr/bin/env python3
"""
Extract Master - Оркестратор Extract этапа
Микросервисная архитектура: Extract → Transform → Load (этап Extract)

Дата создания: 19-07-2025  
Последнее обновление: 01-01-2026

Роль: Координация всех Extract процессов
- Загрузка Excel данных  
- Обогащение справочниками
- Создание словарей
- Обработка статусов
- Подготовка данных для Transform

Мультизагрузка v1.0:
- Поддержка выбора датасета из папок v_YYYY-MM-DD
- Передача пути датасета загрузчикам через --dataset-path
- md_components универсальна для всех датасетов
"""

import argparse
import subprocess
import sys
import time
import logging
from pathlib import Path
from datetime import date, datetime
from typing import List, Dict, Optional

# Добавляем пути для utils и общего кода (code/)
code_root = Path(__file__).resolve().parents[1]
repo_root = code_root.parent
sys.path.append(str(code_root / 'utils'))
sys.path.append(str(code_root))
from config_loader import get_clickhouse_client
from etl_version_manager import ETLVersionManager
from dataset_manager import DatasetManager, DatasetInfo
import openpyxl
import os

# Настройка логгирования
Path('logs').mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/extract_master.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def extract_unified_version_date(dataset_path: str = None):
    """
    Извлекает единую version_date для датасета.
    
    Приоритет:
    0. Дата из имени папки датасета (v_YYYY-MM-DD) — ГЛАВНЫЙ ИСТОЧНИК
    1. Дата создания Excel файла
    2. Дата модификации Excel файла
    3. Время модификации файла в ОС
    
    Args:
        dataset_path: Путь к папке датасета (v_YYYY-MM-DD)
    """
    import re
    
    try:
        version_source = "unknown"
        version_date = date.today()
        
        # Приоритет 0: Дата из имени папки датасета (v_YYYY-MM-DD)
        if dataset_path:
            folder_name = Path(dataset_path).name
            match = re.match(r'v_(\d{4}-\d{2}-\d{2})', folder_name)
            if match:
                version_date = datetime.strptime(match.group(1), '%Y-%m-%d').date()
                version_source = "folder name"
                logger.info(f"📅 Версия из имени папки: {version_date}")
                logger.info(f"✅ Единая version_date для всех загрузчиков: {version_date}")
                return version_date
        
        # Fallback: метаданные Excel
        if dataset_path:
            status_path = Path(dataset_path) / 'Status_Components.xlsx'
        else:
            status_path = Path('data_input/source_data/Status_Components.xlsx')
        logger.info(f"📅 Извлечение единой version_date из {status_path}...")
        
        # Открываем Excel файл для чтения метаданных
        workbook = openpyxl.load_workbook(status_path, read_only=True)
        props = workbook.properties
        
        current_year = datetime.now().year
        
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

class ExtractMaster:
    """Главный оркестратор Extract этапа"""
    
    # Конфигурация Extract пайплайна в правильном порядке
    EXTRACT_PIPELINE = [
        {
            'script': 'md_components_loader.py',
            'description': 'MD Components - мастер-данные компонентов',
            'dependencies': [],
            'result_table': 'md_components',
            'critical': True  # Критичен для фильтрации
        },
        {
            'script': 'calculate_beyond_repair.py',
            'description': 'Расчет Beyond Repair (br_mi8/br_mi17)',
            'dependencies': ['md_components'],
            'result_table': 'md_components',
            'critical': False
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
            'dependencies': ['heli_pandas', 'md_components'],
            'result_table': 'dict_aircraft_number_flat',
            'critical': False,
            # На этом шаге словарь может быть временно пуст (до генерации новых ВС в AC/FL)
            # Предупреждение валидации для пустой таблицы по текущей версии подавляем осознанно
            'allow_empty': True
        },
        {
            'script': 'md_components_enricher.py',
            'description': 'Валидация MD Components partseqno_i (Excel SSoT vs dict_partno_flat)',
            'dependencies': ['md_components', 'dict_partno_flat'],
            'result_table': 'md_components',
            'critical': True
        },
        {
            'script': 'md_components_psn_reserve.py',
            'description': 'Валидация резервирования psn для симуляционных рождений агрегатов',
            'dependencies': ['md_components', 'heli_pandas'],
            'result_table': 'md_components',
            'critical': True
        },
        # === ТЕНЗОРЫ (в самом конце, когда все данные готовы) ===
        {
            'script': 'program_ac_direct_loader.py',
            'description': 'Flight Program AC Direct - тензор без первой trigger-коррекции (--skip-trigger-correction)',
            'dependencies': ['heli_pandas', 'md_components'],
            'result_table': 'flight_program_ac',
            'critical': False,
            'args': ['--skip-trigger-correction'],
        },
        {
            'script': 'program_fl_direct_loader.py',
            'description': 'Flight Program FL Direct - прямой тензор программ полетов на 4000 дней',
            'dependencies': ['dict_aircraft_number_flat'],
            'result_table': 'flight_program_fl',
            'critical': False
        },
        {
            'script': 'heli_pandas_group_by_enricher.py',
            'description': 'Обогащение heli_pandas.group_by из md_components.partseqno_i (идемпотентно)',
            'dependencies': ['md_components', 'heli_pandas'],
            'result_table': 'heli_pandas',
            'critical': False,
            'args': ['--apply']
        },
        {
            'script': 'program_ac_precheck_runner.py',
            'description': 'Program AC Precheck D1 - корректировка status_id для D1',
            'dependencies': ['heli_pandas', 'md_components', 'flight_program_fl'],
            'result_table': 'heli_pandas',
            'critical': False
        },
        {
            'script': 'aggregate_status_block.py',
            'description': 'Классификация агрегатов: component → serviceable → repair → component resync → storage',
            'dependencies': ['heli_pandas', 'md_components'],
            'result_table': 'heli_pandas',
            'critical': True
        },
        # DISABLED 2026-07-22: heli_pandas_economics_status.py сохранён на диске,
        # но исключён из исполняемого pipeline (former ferry/resource→7 gate).
        # === STATUS/REPAIR TAIL (после основных словарей и тензоров) ===
        {
            'script': 'repair_days_calculator.py',
            'description': 'Расчет repair_days для ВС в ремонте',
            'dependencies': ['md_components', 'heli_pandas', 'status_overhaul'],
            'result_table': 'heli_pandas',
            'critical': True
        },
        {
            'script': 'heli_pandas_terminal_br_gate.py',
            'description': 'Финальный BR-gate: status_id=1/7 → terminal (6) по br_effective',
            'dependencies': ['md_components', 'heli_pandas'],
            'result_table': 'heli_pandas',
            'critical': True
        },
        {
            'script': 'day0_ops_deficit_demote_runner.py',
            'description': 'Day0 OPS demote: excess status=2 vs MP4 until OPS==target',
            'dependencies': ['heli_pandas', 'flight_program_ac'],
            'result_table': 'heli_pandas',
            'critical': True,
        },
        {
            'script': 'program_ac_trigger_correction',
            'script_path': 'code/extract/program_ac_direct_loader.py',
            'description': 'Корректировка первых trigger полей AC по финальному OPS после demote',
            'dependencies': ['heli_pandas', 'flight_program_ac'],
            'result_table': 'flight_program_ac',
            'critical': True,
            'args': ['--trigger-correction-only'],
        },
        # === МЕТА-СЛОВАРЬ (последний executable после status tail и demote) ===
        {
            'script': 'digital_values_dictionary_creator.py',
            'description': 'Digital Values Dictionary - аддитивный словарь всех полей для Flame GPU macroproperty',
            'dependencies': ['heli_pandas', 'md_components', 'flight_program_ac', 'flight_program_fl'],
            'result_table': 'dict_digital_values_flat',
            'critical': False
        },
        # === PRE-SIMULATION РАЗМЕТКА (инициализация status_change на D0) ===
        # Удалено: pre_simulation_status_change (status_change более не используется)
    ]
    DWH_EXCEL_REPLACED_STEPS = {
        'status_overhaul_loader.py',
        'program_ac_loader.py',
        'dual_loader.py',
    }
    DWH_POST_CASCADE_STEPS = {
        'program_ac_precheck_runner.py',
        'aggregate_status_block.py',
        'repair_days_calculator.py',
        'heli_pandas_terminal_br_gate.py',
    }
    
    def __init__(self):
        """Инициализация Extract Master"""
        self.client = None
        self.version_manager = None
        self.version_date = None
        self.version_id = None
        self.mode = None  # 'test' или 'prod'
        self.source = 'excel'  # 'excel' или 'dwh'
        self.replace_slice = False
        self.dataset: DatasetInfo = None  # Выбранный датасет
        self.dataset_path: str = None  # Путь к папке датасета
        
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
            
            logger.info("✅ Extract Master инициализирован")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации: {e}")
            return False
    
    def select_dataset(self) -> bool:
        """Выбор датасета для загрузки"""
        print("\n" + "="*70)
        print("📂 ВЫБОР ДАТАСЕТА")
        print("="*70)
        
        # Обнаруживаем датасеты
        manager = DatasetManager()
        datasets = manager.discover_datasets()
        
        if not datasets:
            logger.error("❌ Не найдено ни одного комплектного датасета")
            logger.info("💡 Создайте папку v_YYYY-MM-DD в data_input/source_data/")
            logger.info("   с файлами: Status_Components.xlsx, Status_Overhaul.xlsx, Program_AC.xlsx")
            return False
        
        # Интерактивный выбор
        selected = manager.select_dataset_interactive()
        
        if not selected:
            return False
        
        self.dataset = selected
        self.dataset_path = str(selected.path)
        
        # Устанавливаем глобальный путь для всех микросервисов
        from utils.version_utils import set_dataset_path
        set_dataset_path(self.dataset_path)
        
        logger.info(f"✅ Выбран датасет: {selected.name}")
        logger.info(f"📁 Путь: {self.dataset_path}")
        
        return True
    
    def select_mode(self) -> bool:
        """Выбор режима работы: тест или прод"""
        print("\n" + "="*70)
        print("🎯 EXTRACT MASTER - HELICOPTER COMPONENT LIFECYCLE")
        print("="*70)
        print(f"\n📂 Датасет: {self.dataset.name if self.dataset else 'не выбран'}")
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
            
            # Список ТОЛЬКО таблиц которые создаются текущим Extract пайплайном
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
                
                # Основные таблицы Extract пайплайна
                'heli_pandas', 'heli_raw',           # создается dual_loader.py  
                'md_components',                     # создается md_components_loader.py
                'status_overhaul',                   # создается status_overhaul_loader.py
                'program_ac',                        # создается program_ac_loader.py
                'flight_program_fl',                 # создается program_fl_direct_loader.py
                'flight_program_ac',                 # создается program_ac_direct_loader.py
                'dict_aircraft_number_flat',         # создается dictionary_creator.py (ЗАВИСИТ от heli_pandas!)
                
                # ИСКЛЮЧЕНЫ ИЗ УДАЛЕНИЯ - ИСТИННО АДДИТИВНЫЕ СЛОВАРНЫЕ ТАБЛИЦЫ (MergeTree):
                # 'dict_partno_flat', 'dict_serialno_flat', 'dict_owner_flat',   # создается dictionary_creator.py (ИСТИННО АДДИТИВНЫЕ)
                # 'dict_ac_type_flat'                                            # создается dictionary_creator.py (ИСТИННО АДДИТИВНЫЕ)
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
            # Используем единую дату из Status_Components.xlsx выбранного датасета
            self.version_date = extract_unified_version_date(self.dataset_path)
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
            
            # Определяем дату версии из датасета
            self.version_date = extract_unified_version_date(self.dataset_path)
            
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

    def configure_non_interactive(
        self,
        *,
        source: str,
        mode: str,
        dataset_path: str,
        version_date: Optional[str],
        version_id: int,
        replace_slice: bool,
    ) -> bool:
        """Настройка CLI-режима без интерактивных вопросов."""
        self.source = source
        self.mode = mode
        self.dataset_path = str(Path(dataset_path))
        self.version_id = int(version_id)
        self.replace_slice = bool(replace_slice)

        from utils.version_utils import set_dataset_path
        set_dataset_path(self.dataset_path)

        if version_date:
            self.version_date = datetime.strptime(version_date, '%Y-%m-%d').date()
        elif source == 'excel':
            self.version_date = extract_unified_version_date(self.dataset_path)
        else:
            logger.error("❌ Для --source dwh обязателен --version-date")
            return False

        logger.info(
            "✅ CLI режим: source=%s, mode=%s, version=%s v%s, dataset=%s",
            self.source,
            self.mode,
            self.version_date,
            self.version_id,
            self.dataset_path,
        )
        return True

    def build_pipeline(self) -> List[Dict]:
        """Собирает фактический pipeline под источник данных."""
        if self.source == 'excel':
            return [dict(step) for step in self.EXTRACT_PIPELINE]

        pipeline: List[Dict] = []
        dwh_step_added = False
        for step in self.EXTRACT_PIPELINE:
            script = step['script']
            if script in self.DWH_EXCEL_REPLACED_STEPS:
                if not dwh_step_added:
                    args = [
                        '--report-date', str(self.version_date),
                        '--version-id', str(self.version_id),
                        '--step', 'all',
                        '--skip-existing',
                        '--no-enrich',
                    ]
                    if self.replace_slice:
                        args.append('--replace-slice')
                    pipeline.append({
                        'script': 'dwh_loader.py',
                        'script_path': 'code/utils/dwh_loader.py',
                        'description': 'DWH load-only: program_ac/status_overhaul/status_components без status cascade',
                        'dependencies': ['md_components'],
                        'result_table': 'heli_pandas',
                        'critical': True,
                        'args': args,
                        'pass_version_args': False,
                    })
                    dwh_step_added = True
                continue
            if script in self.DWH_POST_CASCADE_STEPS:
                continue
            if script == 'program_ac_direct_loader.py':
                pipeline.append({
                    'script': 'dwh_post_enrichment.py',
                    'script_path': 'code/utils/dwh_post_enrichment.py',
                    'description': 'DWH planner cascade: reset → overhaul → program_ac → 3b (до AC tensor)',
                    'dependencies': ['heli_pandas', 'program_ac', 'status_overhaul'],
                    'result_table': 'heli_pandas',
                    'critical': True,
                    'args': ['--phase', 'planner'],
                })
            pipeline.append(dict(step))
            if script == 'heli_pandas_group_by_enricher.py':
                pipeline.append({
                    'script': 'dwh_post_enrichment.py',
                    'script_path': 'code/utils/dwh_post_enrichment.py',
                    'description': 'DWH post cascade: precheck → aggregate status block → repair_days → terminal BR',
                    'dependencies': ['heli_pandas', 'flight_program_ac', 'flight_program_fl'],
                    'result_table': 'heli_pandas',
                    'critical': True,
                    'args': ['--phase', 'post'],
                })
        return pipeline

    def resolve_script_path(self, step: Dict) -> Path:
        """Возвращает путь скрипта: extract-local или относительно repo root."""
        raw_script = step.get('script_path') or step['script']
        raw_path = Path(raw_script)
        if raw_path.is_absolute():
            return raw_path
        if step.get('script_path') or raw_script.startswith('code/'):
            return repo_root / raw_path
        return Path(__file__).parent / raw_path
    
    def run_microservice(self, step: Dict) -> bool:
        """Запуск отдельного Extract микросервиса"""
        script_name = step['script']
        description = step['description']
        
        logger.info(f"🚀 Запуск микросервиса: {script_name}")
        logger.info(f"📋 Описание: {description}")
        
        script_path = self.resolve_script_path(step)
        
        if not script_path.exists():
            logger.error(f"❌ Скрипт не найден: {script_path}")
            return False
        
        try:
            start_time = time.time()
            
            # Формируем команду с параметрами версионирования и доп. аргументами шага (если есть)
            extra_args = step.get('args', [])
            
            # Базовые параметры
            cmd_with_params = [sys.executable, str(script_path)]
            if step.get('pass_version_args', True):
                cmd_with_params.extend([
                    '--version-date', str(self.version_date),
                    '--version-id', str(self.version_id),
                ])
            
            # Добавляем путь к датасету для скриптов которые его поддерживают
            # md_components_loader НЕ использует датасет (мастер-данные универсальны)
            if self.dataset_path and script_name not in ['md_components_loader.py', 'calculate_beyond_repair.py', 
                                                         'md_components_enricher.py', 'md_components_psn_reserve.py', 'enrich_heli_pandas.py',
                                                         'dictionary_creator.py', 'digital_values_dictionary_creator.py',
                                                         'heli_pandas_group_by_enricher.py',
                                                         'aggregate_status_block.py',
                                                         'heli_pandas_economics_status.py',
                                                         'repair_days_calculator.py', 'heli_pandas_terminal_br_gate.py',
                                                         'day0_ops_deficit_demote_runner.py', 'dwh_loader.py',
                                                         ]:
                cmd_with_params.extend(['--dataset-path', self.dataset_path])
            
            # Добавляем дополнительные аргументы шага
            cmd_with_params.extend(extra_args)
            
            # Поддержка импорта utils при запуске скрипта из code/extract
            env = os.environ.copy()
            env["PYTHONPATH"] = f"{str(code_root)}{os.pathsep}{env.get('PYTHONPATH', '')}"

            # Сначала пробуем с параметрами версионирования
            result = subprocess.run(
                cmd_with_params,
                capture_output=True,
                text=True,
                timeout=1800,  # 30 минут максимум
                cwd=Path.cwd(),  # Запускаем из корневой директории
                env=env
            )
            
            # Если скрипт не поддерживает версионирование, пробуем без параметров
            if result.returncode != 0 and ("unrecognized arguments" in result.stderr or "unknown option" in result.stderr):
                logger.warning(f"⚠️ Скрипт {script_name} не поддерживает версионирование, запускаем без параметров")
                
                cmd_without_params = [sys.executable, str(script_path), *extra_args]
                
                result = subprocess.run(
                    cmd_without_params,
                    capture_output=True,
                    text=True,
                    timeout=1800,
                    cwd=Path.cwd(),
                    env=env
                )
            
            execution_time = time.time() - start_time
            
            if result.returncode == 0:
                logger.info(f"✅ Микросервис {script_name} завершен успешно за {execution_time:.1f}с")
                
                # Показываем последние строки вывода
                if result.stdout:
                    stdout_lines = result.stdout.strip().split('\n')
                    for line in stdout_lines:
                        if line.startswith("aggregate_status_block/"):
                            logger.info(f"   {line}")
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
            
            # Аддитивные таблицы (единые справочники) - считаем общее количество, не по версии
            ADDITIVE_TABLES = ['md_components']
            
            if has_version_id and result_table not in ADDITIVE_TABLES:
                # Таблица поддерживает версионирование - считаем для конкретной версии
                count_sql = f"""
                SELECT count() 
                FROM {result_table} 
                WHERE version_date = '{self.version_date}' AND version_id = {self.version_id}
                """
                
                count = self.client.execute(count_sql)[0][0]
                
                if count == 0:
                    # Разрешаем пустую таблицу, если шаг помечен allow_empty
                    if step.get('allow_empty'):
                        return {'success': True, 'message': f'{result_table}: 0 записей (допустимо для этого шага)'}
                    return {'success': False, 'message': f'Нет данных в {result_table} для версии {self.version_date}v{self.version_id}'}
                
                return {'success': True, 'message': f'{result_table}: {count:,} записей (версия {self.version_id})'}
            elif result_table in ADDITIVE_TABLES:
                # Аддитивная таблица — единый справочник, считаем ВСЁ
                count_sql = f"SELECT count() FROM {result_table}"
                count = self.client.execute(count_sql)[0][0]
                
                if count == 0:
                    if step.get('allow_empty'):
                        return {'success': True, 'message': f'{result_table}: 0 записей (допустимо для этого шага)'}
                    return {'success': False, 'message': f'Нет данных в {result_table}'}
                
                return {'success': True, 'message': f'{result_table}: {count:,} записей (единый справочник)'}
            else:
                # Таблица еще не поддерживает версионирование - считаем общее количество
                count_sql = f"SELECT count() FROM {result_table}"
                count = self.client.execute(count_sql)[0][0]
                
                if count == 0:
                    if step.get('allow_empty'):
                        return {'success': True, 'message': f'{result_table}: 0 записей (допустимо для этого шага)'}
                    return {'success': False, 'message': f'Нет данных в {result_table}'}
                
                return {'success': True, 'message': f'{result_table}: {count:,} записей (без версионирования)'}
            
        except Exception as e:
            return {'success': False, 'message': f'Ошибка проверки {result_table}: {e}'}

    def validate_zero_statuses(self) -> bool:
        """Проверка нулевых статусов в heli_pandas для текущей версии."""
        if self.version_date is None or self.version_id is None:
            logger.error("❌ Невозможно проверить status_id=0: version_date/version_id не определены")
            return False

        zero_stats_sql = f"""
        SELECT
            countIf(group_by IN (1, 2) AND status_id = 0) AS planers_zero,
            countIf(group_by > 2 AND status_id = 0) AS aggregates_zero,
            countIf(status_id = 0) AS total_zero
        FROM heli_pandas
        WHERE version_date = '{self.version_date}' AND version_id = {self.version_id}
        """

        try:
            planers_zero, aggregates_zero, total_zero = self.client.execute(zero_stats_sql)[0]
        except Exception as e:
            logger.error(f"❌ Ошибка проверки status_id=0 в heli_pandas: {e}")
            return False

        if total_zero > 0:
            logger.error(
                "❌ heli_pandas: обнаружены status_id=0 "
                f"(planers_zero={planers_zero}, aggregates_zero={aggregates_zero}, total_zero={total_zero})"
            )
            return False

        logger.info(
            "✅ heli_pandas: проверка status_id=0 пройдена "
            f"(planers_zero={planers_zero}, aggregates_zero={aggregates_zero}, total_zero={total_zero})"
        )
        return True

    def validate_ops_matches_mp4(self) -> bool:
        """Проверка acceptance: OPS Mi-8/Mi-17 после demote совпадают с MP4 day0."""
        if self.version_date is None or self.version_id is None:
            logger.error("❌ Невозможно проверить OPS==MP4: version_date/version_id не определены")
            return False

        try:
            from extract.ops_target_comparator import compare_ops_to_target
            comparison = compare_ops_to_target(
                self.client,
                self.version_date,
                int(self.version_id),
            )
        except Exception as e:
            logger.error(f"❌ Ошибка проверки OPS==MP4: {e}")
            return False

        mismatches = comparison[
            (comparison['excess'] != 0)
            | (comparison['ops_count'] != comparison['target'])
        ]
        for row in comparison.itertuples(index=False):
            logger.info(
                "OPS==MP4 %s (group_by=%s): ops=%s target=%s excess=%s target_date=%s",
                row.type,
                row.group_by,
                row.ops_count,
                row.target,
                row.excess,
                row.target_date,
            )

        if not mismatches.empty:
            logger.error("❌ OPS после demote не совпадает с MP4 day0")
            return False

        logger.info("✅ OPS Mi-8/Mi-17 после demote совпадает с MP4 day0")
        return True
    
    def run_pipeline(self) -> bool:
        """Запуск полного Extract пайплайна"""
        logger.info("🚀 === ЗАПУСК EXTRACT ПАЙПЛАЙНА ===")
        
        pipeline = self.build_pipeline()
        total_steps = len(pipeline)
        success_count = 0
        failed_steps = []
        
        for i, step in enumerate(pipeline, 1):
            logger.info(f"\n📋 ЭТАП {i}/{total_steps}: {step['script']}")
            
            if step.get('skip'):
                reason = step.get('skip_reason', 'skip requested')
                logger.warning(f"⏭️ ЭТАП {i}/{total_steps} пропущен: {step['script']}")
                logger.warning(f"⚠️ Причина: {reason}")
                success_count += 1
                continue
            
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
        final_ok = self.final_validation()
        
        return success_count == total_steps and final_ok
    
    def final_validation(self):
        """Финальная валидация готовности системы"""
        logger.info("\n🔍 === ФИНАЛЬНАЯ ВАЛИДАЦИЯ ===")
        
        # Ключевые таблицы для GPU
        critical_tables = ['heli_pandas', 'md_components', 'status_overhaul', 'program_ac']
        
        all_ready = True
        total_records = 0
        
        non_versioned_tables = {'md_components'}
        
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
                    
                    if table in non_versioned_tables:
                        logger.info(f"✅ {table}: {count:,} записей (единый справочник)")
                        
                        if count == 0:
                            all_ready = False
                    elif has_version_id:
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

        # Проверка нулевых статусов в heli_pandas (текущая версия)
        if not self.validate_zero_statuses():
            all_ready = False
        if not self.validate_ops_matches_mp4():
            all_ready = False
        
        if all_ready:
            logger.info(f"\n🎉 СИСТЕМА ГОТОВА ДЛЯ FLAME GPU!")
            logger.info(f"📊 Общий объем данных: {total_records:,} записей")
            logger.info(f"🚀 Можно запускать Agent-Based моделирование")
        else:
            logger.warning(f"\n⚠️ Система требует дополнительной настройки")
        return all_ready

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Extract Master - оркестратор Extract этапа')
    parser.add_argument('--source', choices=('excel', 'dwh'), default='excel', help='Источник данных')
    parser.add_argument('--mode', choices=('test', 'prod'), help='Режим запуска')
    parser.add_argument('--dataset-path', type=str, help='Путь к папке датасета (v_YYYY-MM-DD)')
    parser.add_argument('--version-date', type=str, help='Дата версии (YYYY-MM-DD)')
    parser.add_argument('--version-id', type=int, default=1, help='ID версии')
    parser.add_argument('--replace-slice', action='store_true', help='Передать --replace-slice в dwh_loader')
    return parser.parse_args()

def main():
    """Главная функция Extract Master"""
    args = parse_args()
    non_interactive = any([
        args.mode,
        args.dataset_path,
        args.version_date,
        args.source != 'excel',
        args.replace_slice,
    ])
    master = ExtractMaster()
    
    try:
        if non_interactive:
            if not args.mode:
                logger.error("❌ В CLI-режиме обязателен --mode")
                sys.exit(2)
            if not args.dataset_path:
                logger.error("❌ В CLI-режиме обязателен --dataset-path")
                sys.exit(2)

            if not master.configure_non_interactive(
                source=args.source,
                mode=args.mode,
                dataset_path=args.dataset_path,
                version_date=args.version_date,
                version_id=args.version_id,
                replace_slice=args.replace_slice,
            ):
                sys.exit(1)

            if not master.initialize():
                sys.exit(1)
        else:
            # Выбор датасета
            if not master.select_dataset():
                sys.exit(0)

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
            logger.info("🎉 EXTRACT ПАЙПЛАЙН ЗАВЕРШЕН УСПЕШНО!")
            sys.exit(0)
        else:
            logger.warning("⚠️ EXTRACT ПАЙПЛАЙН ЗАВЕРШЕН С ОШИБКАМИ")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("\n❌ Extract Master прерван пользователем")
        sys.exit(130)
    except Exception as e:
        logger.error(f"💥 Критическая ошибка Extract Master: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 