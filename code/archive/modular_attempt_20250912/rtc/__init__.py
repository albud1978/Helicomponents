"""
RTC функции для FLAME GPU симуляции
Каждая функция в отдельном файле для модульности
Дата: 2025-09-12
"""

# Базовый класс для всех RTC функций
class BaseRTC:
    """Базовый класс для RTC функций"""
    NAME = ""
    DEPENDENCIES = []
    
    @staticmethod
    def get_source(frames: int, days: int, **kwargs) -> str:
        """Возвращает исходный код RTC функции"""
        raise NotImplementedError("Subclasses must implement get_source")
    
    @classmethod
    def get_dependencies(cls) -> list:
        """Возвращает список зависимостей"""
        return cls.DEPENDENCIES.copy()

__all__ = ['BaseRTC']


