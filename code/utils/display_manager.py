#!/usr/bin/env python3
"""
Universal Display Manager для ETL проекта Helicopter Component Lifecycle
========================================================================

Обеспечивает универсальную поддержку отображения эмодзи и Unicode символов
для разных операционных систем и терминалов.

Автодетекция:
- Операционная система (Windows/Linux/macOS)
- Кодировка терминала (UTF-8/CP1251/etc)
- Поддержка Unicode символов

Конфигурация:
- Environment переменная UNICODE_MODE (true/false/auto)
- Environment переменная EMOJI_MODE (unicode/text/auto)
- Автоматические fallback для проблемных терминалов

Автор: AI Assistant  
Дата: 2025-07-19
"""

import os
import sys
import platform
import logging
from typing import Dict, Optional, Union
from pathlib import Path

# Автоматическая инициализация конфигурации при импорте
try:
    from .auto_config import ensure_project_ready
    # Обеспечиваем готовность проекта при первом импорте
    ensure_project_ready()
except (ImportError, Exception):
    # Если auto_config недоступен, продолжаем без автонастройки
    pass


class UniversalDisplayManager:
    """Универсальный менеджер отображения для ETL проекта"""
    
    # Словарь замен эмодзи на текстовые метки
    EMOJI_REPLACEMENTS = {
        # Статусы
        '✅': '[OK]',
        '❌': '[ERROR]', 
        '⚠️': '[WARNING]',
        'ℹ️': '[INFO]',
        
        # Процессы
        '🚀': '[START]',
        '🔄': '[PROCESS]',
        '💾': '[SAVE]',
        '🔍': '[CHECK]',
        '🔧': '[CONFIG]',
        '⏭️': '[SKIP]',
        '🛡️': '[PROTECT]',
        
        # Данные
        '📊': '[DATA]',
        '📋': '[LIST]',
        '📅': '[DATE]',
        '📖': '[LOAD]',
        '📦': '[BATCH]',
        '🗑️': '[DELETE]',
        '💿': '[DISK]',
        
        # Целевые
        '🎯': '[TARGET]',
        '🎉': '[COMPLETE]',
        '🏭': '[PROD]',
        '🧪': '[TEST]',
        
        # Объекты
        '🚁': '[AIRCRAFT]',
        '🔥': '[GPU]',
        '🔒': '[SECURE]',
        '🗝️': '[KEY]',
        
        # Прочие
        '👤': '[USER]',
        '🌟': '[STAR]',
        '💡': '[IDEA]',
        '📝': '[NOTE]',
        '🔧': '[TOOL]'
    }
    
    def __init__(self):
        """Инициализация с автодетекцией возможностей терминала"""
        self.os_type = platform.system()
        self.is_windows = self.os_type == 'Windows'
        
        # Определяем режимы работы
        self.unicode_mode = self._determine_unicode_mode()
        self.emoji_mode = self._determine_emoji_mode()
        self.encoding = self._determine_encoding()
        
        # Настраиваем логирование
        self._setup_safe_logging()
        
    def _determine_unicode_mode(self) -> bool:
        """Определяет поддержку Unicode в терминале"""
        # Проверяем environment переменную
        env_mode = os.getenv('UNICODE_MODE', 'auto').lower()
        
        if env_mode == 'true':
            return True
        elif env_mode == 'false':
            return False
        elif env_mode == 'auto':
            # Автодетекция
            if self.is_windows:
                # В Windows проверяем кодировку stdout
                return hasattr(sys.stdout, 'encoding') and \
                       sys.stdout.encoding and \
                       sys.stdout.encoding.lower() in ('utf-8', 'utf8')
            else:
                # В Unix-like системах обычно UTF-8 по умолчанию
                return True
        
        return False
    
    def _determine_emoji_mode(self) -> str:
        """Определяет режим отображения эмодзи"""
        env_mode = os.getenv('EMOJI_MODE', 'auto').lower()
        
        if env_mode in ('unicode', 'text'):
            return env_mode
        elif env_mode == 'auto':
            # Автодетекция: если Unicode поддерживается, используем эмодзи
            return 'unicode' if self.unicode_mode else 'text'
        
        return 'text'  # Безопасный fallback
    
    def _determine_encoding(self) -> str:
        """Определяет кодировку для вывода"""
        env_encoding = os.getenv('CONSOLE_ENCODING', 'auto').lower()
        
        if env_encoding != 'auto':
            return env_encoding
        
        # Автодетекция
        if hasattr(sys.stdout, 'encoding') and sys.stdout.encoding:
            return sys.stdout.encoding
        elif self.is_windows:
            return 'cp1251'  # Fallback для старых Windows
        else:
            return 'utf-8'   # Fallback для Unix
    
    def _setup_safe_logging(self):
        """Настраивает безопасное логирование"""
        # Создаем кастомный formatter
        self.log_formatter = SafeUnicodeFormatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            display_manager=self
        )
    
    def safe_format(self, message: str) -> str:
        """Безопасное форматирование сообщения для вывода"""
        if self.emoji_mode == 'unicode' and self.unicode_mode:
            # Возвращаем исходное сообщение с эмодзи
            return message
        
        # Заменяем эмодзи на текстовые метки
        result = message
        for emoji, replacement in self.EMOJI_REPLACEMENTS.items():
            result = result.replace(emoji, replacement)
        
        return result
    
    def safe_print(self, message: str, **kwargs):
        """Безопасный вывод в консоль"""
        formatted_message = self.safe_format(message)
        
        try:
            print(formatted_message, **kwargs)
        except UnicodeEncodeError:
            # Дополнительная защита: принудительное преобразование в ASCII
            ascii_message = formatted_message.encode('ascii', 'replace').decode('ascii')
            print(ascii_message, **kwargs)
    
    def get_logger(self, name: str) -> logging.Logger:
        """Создает безопасный логгер с поддержкой Unicode"""
        logger = logging.getLogger(name)
        
        # Удаляем существующие handlers чтобы избежать дублирования
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Создаем новый handler с безопасным форматированием
        handler = SafeStreamHandler(display_manager=self)
        handler.setFormatter(self.log_formatter)
        
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        
        return logger
    
    def print_system_info(self):
        """Выводит информацию о настройках отображения"""
        info_lines = [
            f"🖥️ OS: {self.os_type}",
            f"🔤 Unicode Mode: {self.unicode_mode}",
            f"😀 Emoji Mode: {self.emoji_mode}",
            f"📝 Encoding: {self.encoding}",
            f"🎯 Terminal: {sys.stdout.encoding if hasattr(sys.stdout, 'encoding') else 'Unknown'}"
        ]
        
        print("=" * 50)
        print("🔧 DISPLAY MANAGER SETTINGS")
        print("=" * 50)
        for line in info_lines:
            self.safe_print(line)
        print("=" * 50)


class SafeUnicodeFormatter(logging.Formatter):
    """Безопасный форматтер для логов с поддержкой Unicode"""
    
    def __init__(self, fmt: str, display_manager: UniversalDisplayManager):
        super().__init__(fmt)
        self.display_manager = display_manager
    
    def format(self, record: logging.LogRecord) -> str:
        """Форматирует запись лога с безопасной обработкой Unicode"""
        try:
            # Стандартное форматирование
            formatted = super().format(record)
            # Применяем безопасное форматирование
            return self.display_manager.safe_format(formatted)
            
        except (UnicodeEncodeError, UnicodeDecodeError):
            # Fallback: принудительное преобразование в ASCII
            try:
                raw_formatted = super().format(record)
                ascii_formatted = raw_formatted.encode('ascii', 'replace').decode('ascii')
                return self.display_manager.safe_format(ascii_formatted)
            except Exception:
                # Последний fallback: минимальная информация
                return f"[{record.levelname}] Message encoding error"


class SafeStreamHandler(logging.StreamHandler):
    """Безопасный обработчик потока с поддержкой разных кодировок"""
    
    def __init__(self, display_manager: UniversalDisplayManager, stream=None):
        super().__init__(stream)
        self.display_manager = display_manager
    
    def emit(self, record: logging.LogRecord):
        """Безопасный вывод записи лога"""
        try:
            msg = self.format(record)
            stream = self.stream
            
            # Пытаемся вывести как есть
            stream.write(msg + self.terminator)
            stream.flush()
            
        except UnicodeEncodeError:
            try:
                # Fallback 1: принудительная ASCII конвертация
                msg = self.format(record)
                ascii_msg = msg.encode('ascii', 'replace').decode('ascii')
                stream.write(ascii_msg + self.terminator)
                stream.flush()
                
            except Exception:
                # Fallback 2: минимальная информация
                try:
                    stream.write(f"[{record.levelname}] Encoding error in log message\n")
                    stream.flush()
                except Exception:
                    pass  # Последний fallback: молча пропускаем
        
        except Exception:
            # Для всех остальных ошибок используем стандартную обработку
            self.handleError(record)


# Глобальный экземпляр менеджера
_display_manager: Optional[UniversalDisplayManager] = None


def get_display_manager() -> UniversalDisplayManager:
    """Получает глобальный экземпляр менеджера отображения (singleton)"""
    global _display_manager
    
    if _display_manager is None:
        _display_manager = UniversalDisplayManager()
    
    return _display_manager


def get_universal_logger(name: str) -> logging.Logger:
    """Удобная функция для получения универсального логгера"""
    manager = get_display_manager()
    return manager.get_logger(name)


def safe_print(message: str, **kwargs):
    """Удобная функция для безопасного вывода"""
    manager = get_display_manager()
    manager.safe_print(message, **kwargs)


def print_display_info():
    """Удобная функция для вывода информации о настройках"""
    manager = get_display_manager()
    manager.print_system_info()


# Для обратной совместимости и удобства
def setup_universal_logging(name: str = __name__) -> logging.Logger:
    """Настраивает универсальное логирование для модуля"""
    return get_universal_logger(name)


if __name__ == "__main__":
    """Тестирование функциональности"""
    print("🧪 Тестирование Universal Display Manager...")
    
    # Выводим информацию о системе
    print_display_info()
    
    # Тестируем безопасный вывод
    test_messages = [
        "✅ Тест успешного сообщения",
        "❌ Тест ошибки с эмодзи",
        "🚀 Запуск процесса тестирования",
        "📊 Обработка данных: 1,234,567 записей",
        "🎉 Тестирование завершено успешно!"
    ]
    
    print("\n📋 Тестирование safe_print:")
    for msg in test_messages:
        safe_print(msg)
    
    # Тестируем логгер
    print("\n📋 Тестирование logger:")
    logger = get_universal_logger(__name__)
    for msg in test_messages:
        logger.info(msg)
    
    print("\n✅ Тестирование завершено!") 