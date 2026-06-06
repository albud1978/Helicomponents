#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (code utils cleanup): orphan/устаревшая утилита, 0 импортёров; перекрыта живыми модулями или старым workflow.
"""
Universal Init - Универсальная инициализация для ETL проекта
===========================================================

Обеспечивает универсальную инициализацию всех ETL скриптов:
- Автоматическая настройка .env файла
- Универсальный логгер с поддержкой кодировок
- Загрузка конфигурации базы данных
- Проверка готовности окружения

Использование:
    from code.utils.universal_init import init_etl_script
    
    logger = init_etl_script(__name__)
    logger.info("✅ Скрипт готов к работе")

Автор: AI Assistant
Дата: 2025-07-19
"""

import sys
import os
from pathlib import Path
from typing import Optional
import logging

# Добавляем путь к утилитам
sys.path.append(str(Path(__file__).parent))

try:
    from auto_config import ensure_project_ready, get_auto_config
    from display_manager import get_universal_logger, print_display_info
    from config_loader import get_clickhouse_client
    # Пытаемся импортировать load_env_variables, но это не критично
    try:
        from config_loader import load_env_variables
        LOAD_ENV_AVAILABLE = True
    except ImportError:
        LOAD_ENV_AVAILABLE = False
    AUTO_CONFIG_AVAILABLE = True
except ImportError as e:
    # Fallback если модули недоступны
    AUTO_CONFIG_AVAILABLE = False
    LOAD_ENV_AVAILABLE = False
    print(f"⚠️ Автоконфигурация недоступна: {e}")


def init_etl_script(script_name: str, 
                   auto_setup: bool = True,
                   show_info: bool = None) -> logging.Logger:
    """
    Универсальная инициализация ETL скрипта
    
    Args:
        script_name: Имя модуля (__name__)
        auto_setup: Автоматически настраивать окружение
        show_info: Показывать информацию о системе (None = auto)
    
    Returns:
        Универсальный логгер с поддержкой кодировок
    """
    
    # 1. Автоматическая настройка окружения
    if auto_setup and AUTO_CONFIG_AVAILABLE:
        try:
            ensure_project_ready()
        except Exception as e:
            print(f"⚠️ Ошибка автонастройки: {e}")
    
    # 2. Загрузка переменных окружения
    try:
        if AUTO_CONFIG_AVAILABLE and LOAD_ENV_AVAILABLE:
            load_env_variables()
        else:
            # Fallback - попытка загрузить .env напрямую
            _fallback_load_env()
    except Exception as e:
        print(f"⚠️ Ошибка загрузки .env: {e}")
    
    # 3. Создание универсального логгера
    if AUTO_CONFIG_AVAILABLE:
        logger = get_universal_logger(script_name)
    else:
        # Fallback - стандартный логгер
        logger = _fallback_get_logger(script_name)
    
    # 4. Показ информации о системе (если нужно)
    if show_info is None:
        # Автоматически показываем при первом запуске
        show_info = os.getenv('SHOW_DISPLAY_INFO', 'false').lower() == 'true'
    
    if show_info and AUTO_CONFIG_AVAILABLE:
        try:
            print_display_info()
        except Exception:
            pass
    
    return logger


def check_database_connection(logger: Optional[logging.Logger] = None) -> bool:
    """Проверяет подключение к базе данных"""
    if not logger:
        logger = logging.getLogger(__name__)
    
    try:
        if AUTO_CONFIG_AVAILABLE:
            client = get_clickhouse_client()
            result = client.execute('SELECT 1 as test')
            logger.info("✅ Подключение к ClickHouse успешно!")
            return True
        else:
            logger.warning("⚠️ Автоконфигурация недоступна, подключение не проверено")
            return False
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к ClickHouse: {e}")
        return False


def get_script_info(script_name: str) -> dict:
    """Получает информацию о скрипте для логирования"""
    script_path = Path(script_name) if '/' in script_name else Path.cwd()
    
    return {
        'script_name': script_name,
        'script_path': str(script_path),
        'python_version': f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        'platform': sys.platform,
        'working_directory': str(Path.cwd()),
        'auto_config_available': AUTO_CONFIG_AVAILABLE
    }


def _fallback_load_env():
    """Fallback загрузка .env файла без внешних зависимостей"""
    env_file = Path.cwd() / '.env'
    if env_file.exists():
        try:
            with open(env_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip()
            print(f"✅ Environment variables загружены из {env_file}")
        except Exception as e:
            print(f"⚠️ Ошибка загрузки .env: {e}")


def _fallback_get_logger(name: str) -> logging.Logger:
    """Fallback создание логгера без универсального display manager"""
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    
    return logger


# Удобные функции для быстрого использования
def quick_init(script_name: str) -> logging.Logger:
    """Быстрая инициализация с минимальными настройками"""
    return init_etl_script(script_name, auto_setup=True, show_info=False)


def debug_init(script_name: str) -> logging.Logger:
    """Инициализация с отладочной информацией"""
    return init_etl_script(script_name, auto_setup=True, show_info=True)


def safe_init(script_name: str) -> logging.Logger:
    """Безопасная инициализация без автонастройки"""
    return init_etl_script(script_name, auto_setup=False, show_info=False)


if __name__ == "__main__":
    """Тестирование универсальной инициализации"""
    print("🧪 Тестирование Universal Init...")
    
    # Тест инициализации
    logger = debug_init(__name__)
    
    # Тест информации о скрипте
    info = get_script_info(__name__)
    logger.info("📋 Информация о скрипте:")
    for key, value in info.items():
        logger.info(f"   {key}: {value}")
    
    # Тест подключения к базе данных
    logger.info("🔍 Проверка подключения к базе данных...")
    db_ok = check_database_connection(logger)
    
    if db_ok:
        logger.info("✅ Все компоненты работают корректно!")
    else:
        logger.warning("⚠️ Некоторые компоненты могут работать неправильно")
    
    print("✅ Тестирование завершено!") 