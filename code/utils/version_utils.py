"""
Утилиты для единой версионности в ETL пайплайне
Все загрузчики используют один источник версии - Status_Components.xlsx

Поддержка мультизагрузки:
- Если передан dataset_path, используется Status_Components.xlsx из этой папки
- Если не передан, используется путь по умолчанию
"""

import os
import openpyxl
from datetime import datetime, date
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

# Глобальная переменная для пути к датасету (устанавливается extract_master.py)
_current_dataset_path: Path = None


def set_dataset_path(dataset_path: str) -> None:
    """
    Устанавливает путь к текущему датасету для всех загрузчиков
    
    Args:
        dataset_path: Путь к папке датасета (v_YYYY-MM-DD)
    """
    global _current_dataset_path
    _current_dataset_path = Path(dataset_path) if dataset_path else None
    logger.info(f"📂 Установлен путь к датасету: {_current_dataset_path}")


def get_dataset_path() -> Path:
    """
    Возвращает текущий путь к датасету
    
    Returns:
        Path к папке датасета или None
    """
    return _current_dataset_path


def extract_unified_version_date(dataset_path: str = None) -> date:
    """
    Извлекает единую version_date из Status_Components.xlsx
    для использования всеми загрузчиками (единый источник версионности)
    
    Args:
        dataset_path: Путь к папке датасета (опционально)
        
    Returns:
        date: Версия данных из Status_Components.xlsx
    """
    try:
        # Определяем путь к Status_Components.xlsx
        if dataset_path:
            status_path = Path(dataset_path) / 'Status_Components.xlsx'
        elif _current_dataset_path:
            status_path = _current_dataset_path / 'Status_Components.xlsx'
        else:
            # Обратная совместимость: старый путь по умолчанию
            status_path = Path('data_input/source_data/Status_Components.xlsx')
        
        logger.info(f"📅 Извлечение единой version_date из {status_path}...")
        
        # Проверяем существование файла
        if not status_path.exists():
            logger.warning(f"⚠️ Файл {status_path} не найден, используем сегодняшнюю дату")
            return date.today()
        
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
        
        logger.info(f"✅ Единая version_date: {version_date}")
        return version_date
        
    except Exception as e:
        logger.error(f"❌ Ошибка извлечения версии из Status_Components.xlsx: {e}")
        fallback_date = date.today()
        logger.warning(f"🚨 Используем fallback дату: {fallback_date}")
        return fallback_date 