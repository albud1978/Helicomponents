#!/usr/bin/env python3
"""
Автоматический конфигуратор для ETL проекта Helicopter Component Lifecycle
=========================================================================

Обеспечивает универсальную настройку проекта "из коробки":
- Автоматическое создание .env файла при первом запуске
- Умная детекция оптимальных настроек для конкретной системы
- Встроенные defaults без зависимости от внешних файлов
- Автоматическая миграция существующих конфигураций

Автор: AI Assistant
Дата: 2025-07-19
"""

import os
import sys
import platform
import shutil
from pathlib import Path
from typing import Dict, Optional, Any
import yaml


class UniversalAutoConfig:
    """Автоматический конфигуратор проекта с умными defaults"""
    
    def __init__(self, project_root: Optional[Path] = None):
        """Инициализация с автодетекцией корня проекта"""
        if project_root is None:
            # Автодетекция корня проекта (ищем .cursor/rules или .git)
            current = Path(__file__).resolve()
            while current.parent != current:
                if (current / '.cursor' / 'rules' / 'project.mdc').exists() or (current / '.git').exists():
                    project_root = current
                    break
                current = current.parent
            
            if project_root is None:
                project_root = Path.cwd()
        
        self.project_root = Path(project_root)
        self.env_file = self.project_root / '.env'
        self.config_dir = self.project_root / 'config'
        
        # Системная информация
        self.os_type = platform.system()
        self.is_windows = self.os_type == 'Windows'
        self.python_version = sys.version_info
        
    def detect_optimal_settings(self) -> Dict[str, Any]:
        """Умная детекция оптимальных настроек для текущей системы"""
        settings = {}
        
        # === ДЕТЕКЦИЯ DISPLAY НАСТРОЕК ===
        
        # Unicode поддержка
        unicode_support = self._detect_unicode_support()
        settings['UNICODE_MODE'] = 'true' if unicode_support else 'false'
        
        # Эмодзи поддержка
        emoji_support = unicode_support and self._detect_emoji_support()
        settings['EMOJI_MODE'] = 'unicode' if emoji_support else 'text'
        
        # Кодировка консоли
        console_encoding = self._detect_console_encoding()
        settings['CONSOLE_ENCODING'] = console_encoding
        
        # Display info (показывать только при первой настройке)
        settings['SHOW_DISPLAY_INFO'] = 'true'
        
        # === ДЕТЕКЦИЯ БАЗЫ ДАННЫХ ===
        
        # Пароль ClickHouse (пытаемся найти в существующих файлах)
        clickhouse_password = self._detect_clickhouse_password()
        settings['CLICKHOUSE_PASSWORD'] = clickhouse_password
        
        # === ДЕТЕКЦИЯ ОКРУЖЕНИЯ ===
        
        # Рабочий режим (dev/prod)
        work_mode = self._detect_work_mode()
        settings['WORK_MODE'] = work_mode
        
        # Уровень логирования
        log_level = 'DEBUG' if work_mode == 'dev' else 'INFO'
        settings['LOG_LEVEL'] = log_level
        
        return settings
    
    def _detect_unicode_support(self) -> bool:
        """Детекция поддержки Unicode в терминале"""
        try:
            # Тест 1: Проверяем кодировку stdout
            if hasattr(sys.stdout, 'encoding') and sys.stdout.encoding:
                encoding = sys.stdout.encoding.lower()
                if 'utf' in encoding:
                    return True
            
            # Тест 2: Пробуем вывести Unicode символ
            test_char = '✓'
            test_char.encode(sys.stdout.encoding or 'utf-8')
            return True
            
        except (UnicodeEncodeError, AttributeError, LookupError):
            return False
    
    def _detect_emoji_support(self) -> bool:
        """Детекция поддержки эмодзи"""
        try:
            # В Windows эмодзи поддерживаются хуже
            if self.is_windows:
                # Проверяем версию Windows и терминал
                if hasattr(sys.stdout, 'encoding'):
                    encoding = sys.stdout.encoding.lower()
                    # Windows Terminal и PowerShell 7+ поддерживают эмодзи лучше
                    return 'utf' in encoding
                return False
            else:
                # В Unix-like системах обычно хорошая поддержка
                return True
                
        except Exception:
            return False
    
    def _detect_console_encoding(self) -> str:
        """Детекция кодировки консоли"""
        try:
            if hasattr(sys.stdout, 'encoding') and sys.stdout.encoding:
                return sys.stdout.encoding
            elif self.is_windows:
                return 'cp1251'
            else:
                return 'utf-8'
        except Exception:
            return 'utf-8'
    
    def _detect_clickhouse_password(self) -> str:
        """Пытается найти существующий пароль ClickHouse"""
        # Проверяем существующий .env
        if self.env_file.exists():
            try:
                with open(self.env_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.startswith('CLICKHOUSE_PASSWORD='):
                            password = line.split('=', 1)[1].strip()
                            if password and password != 'your_password_here':
                                return password
            except Exception:
                pass
        
        # Проверяем переменные окружения
        env_password = os.getenv('CLICKHOUSE_PASSWORD')
        if env_password and env_password != 'your_password_here':
            return env_password
        
        # Placeholder для ручной настройки
        return 'your_password_here'
    
    def _detect_work_mode(self) -> str:
        """Детекция режима работы (dev/prod)"""
        # Проверяем индикаторы dev окружения
        dev_indicators = [
            os.getenv('DEVELOPMENT') == 'true',
            os.getenv('DEBUG') == 'true',
            '.git' in str(self.project_root),
            'dev' in str(self.project_root).lower(),
            'test' in str(self.project_root).lower()
        ]
        
        return 'dev' if any(dev_indicators) else 'prod'
    
    def create_env_file(self, force: bool = False) -> bool:
        """Создает .env файл с оптимальными настройками"""
        if self.env_file.exists() and not force:
            print(f"⚠️ Файл {self.env_file} уже существует. Используйте force=True для перезаписи.")
            return False
        
        # Получаем оптимальные настройки
        settings = self.detect_optimal_settings()
        
        # Создаем содержимое .env файла
        env_content = self._generate_env_content(settings)
        
        try:
            # Создаем резервную копию если файл существует
            if self.env_file.exists():
                backup_file = self.env_file.with_suffix('.env.backup')
                shutil.copy2(self.env_file, backup_file)
                print(f"📋 Создана резервная копия: {backup_file}")
            
            # Записываем новый файл
            with open(self.env_file, 'w', encoding='utf-8') as f:
                f.write(env_content)
            
            print(f"✅ Создан файл {self.env_file} с оптимальными настройками")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка создания .env файла: {e}")
            return False
    
    def _generate_env_content(self, settings: Dict[str, Any]) -> str:
        """Генерирует содержимое .env файла"""
        return f"""# Environment Variables для ETL проекта Helicopter Component Lifecycle
# ========================================================================
# Автоматически сгенерировано Universal Auto Config для {self.os_type}
# Дата создания: {self._get_current_datetime()}

# === НАСТРОЙКИ БАЗЫ ДАННЫХ ===
CLICKHOUSE_PASSWORD={settings['CLICKHOUSE_PASSWORD']}

# === НАСТРОЙКИ ОТОБРАЖЕНИЯ (автодетекция для {self.os_type}) ===
# Оптимально настроено для вашей системы и терминала

# Режим Unicode ({settings['UNICODE_MODE']})
UNICODE_MODE={settings['UNICODE_MODE']}

# Режим эмодзи ({settings['EMOJI_MODE']})
EMOJI_MODE={settings['EMOJI_MODE']}

# Кодировка консоли ({settings['CONSOLE_ENCODING']})
CONSOLE_ENCODING={settings['CONSOLE_ENCODING']}

# Показывать информацию о display manager при запуске
SHOW_DISPLAY_INFO={settings['SHOW_DISPLAY_INFO']}

# === НАСТРОЙКИ ОКРУЖЕНИЯ ===
# Рабочий режим (dev/prod)
WORK_MODE={settings['WORK_MODE']}

# Уровень логирования
LOG_LEVEL={settings['LOG_LEVEL']}

# === ИНСТРУКЦИИ ПО ИЗМЕНЕНИЮ ===
# 
# Если отображение работает неправильно, попробуйте:
#
# Для проблем с эмодзи в Windows:
# EMOJI_MODE=text
#
# Для проблем с кодировкой:
# CONSOLE_ENCODING=utf-8    # или cp1251 для старых Windows
#
# Для отладки отображения:
# SHOW_DISPLAY_INFO=true
#
# Для смены режима:
# WORK_MODE=dev             # или prod

# === СИСТЕМНАЯ ИНФОРМАЦИЯ ===
# OS: {self.os_type}
# Python: {self.python_version.major}.{self.python_version.minor}.{self.python_version.micro}
# Terminal encoding: {sys.stdout.encoding if hasattr(sys.stdout, 'encoding') else 'Unknown'}
# Unicode support: {settings['UNICODE_MODE']}
# Emoji support: {settings['EMOJI_MODE']}
"""
    
    def _get_current_datetime(self) -> str:
        """Получает текущую дату и время"""
        from datetime import datetime
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def ensure_config_ready(self) -> bool:
        """Обеспечивает готовность конфигурации проекта"""
        success = True
        
        print("🔧 Проверка конфигурации проекта...")
        
        # 1. Проверяем .env файл
        if not self.env_file.exists():
            print("📝 Файл .env не найден, создаем автоматически...")
            success = self.create_env_file() and success
        else:
            print(f"✅ Файл .env существует: {self.env_file}")
        
        # 2. Проверяем папку config
        if not self.config_dir.exists():
            print(f"📁 Создаем папку config: {self.config_dir}")
            self.config_dir.mkdir(exist_ok=True)
        
        # 3. Проверяем критичные настройки
        self._check_critical_settings()
        
        if success:
            print("✅ Конфигурация проекта готова!")
        else:
            print("⚠️ Конфигурация имеет проблемы, но проект может работать")
        
        return success
    
    def _check_critical_settings(self):
        """Проверяет критичные настройки"""
        # Проверяем пароль базы данных
        password = os.getenv('CLICKHOUSE_PASSWORD', '')
        if password in ('', 'your_password_here'):
            print("⚠️ ВНИМАНИЕ: Не установлен пароль CLICKHOUSE_PASSWORD в .env файле")
        
        # Проверяем права на запись
        if not os.access(self.project_root, os.W_OK):
            print(f"⚠️ ВНИМАНИЕ: Нет прав на запись в {self.project_root}")
    
    def print_system_info(self):
        """Выводит информацию о системе и настройках"""
        settings = self.detect_optimal_settings()
        
        print("=" * 60)
        print("🔧 UNIVERSAL AUTO CONFIG - СИСТЕМНАЯ ИНФОРМАЦИЯ")
        print("=" * 60)
        print(f"🖥️ Операционная система: {self.os_type}")
        print(f"🐍 Python версия: {self.python_version.major}.{self.python_version.minor}.{self.python_version.micro}")
        print(f"📁 Корень проекта: {self.project_root}")
        print(f"📝 Файл .env: {'✅ Существует' if self.env_file.exists() else '❌ Отсутствует'}")
        print()
        print("📊 ДЕТЕКТИРОВАННЫЕ НАСТРОЙКИ:")
        for key, value in settings.items():
            print(f"   {key}={value}")
        print("=" * 60)


# Глобальная переменная для singleton
_auto_config: Optional[UniversalAutoConfig] = None


def get_auto_config() -> UniversalAutoConfig:
    """Получает глобальный экземпляр автоконфигуратора"""
    global _auto_config
    
    if _auto_config is None:
        _auto_config = UniversalAutoConfig()
    
    return _auto_config


def ensure_project_ready() -> bool:
    """Обеспечивает готовность проекта к работе"""
    config = get_auto_config()
    return config.ensure_config_ready()


def print_system_info():
    """Выводит информацию о системе"""
    config = get_auto_config()
    config.print_system_info()


def auto_setup_env(force: bool = False) -> bool:
    """Автоматическая настройка .env файла"""
    config = get_auto_config()
    return config.create_env_file(force=force)


if __name__ == "__main__":
    """Утилита командной строки для настройки проекта"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Universal Auto Config для ETL проекта')
    parser.add_argument('--setup', action='store_true', help='Создать .env файл')
    parser.add_argument('--force', action='store_true', help='Принудительно перезаписать .env')
    parser.add_argument('--info', action='store_true', help='Показать системную информацию')
    parser.add_argument('--check', action='store_true', help='Проверить готовность конфигурации')
    
    args = parser.parse_args()
    
    if args.info:
        print_system_info()
    elif args.setup:
        auto_setup_env(force=args.force)
    elif args.check:
        ensure_project_ready()
    else:
        # По умолчанию - полная проверка и настройка
        print("🚀 Universal Auto Config - Автоматическая настройка проекта")
        print()
        ensure_project_ready()
        print()
        print_system_info() 