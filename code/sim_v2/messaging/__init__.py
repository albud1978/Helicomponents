#!/usr/bin/env python3
"""
Messaging архитектура для симуляции планеров

Альтернативный подход с использованием нативных сообщений FLAME GPU
вместо MacroProperty буферов для квотирования.

Структура:
- base_model_messaging.py: Базовая модель с Message типами и QuotaManager агентом
- rtc_publish_report.py: Публикация PlanerReport сообщений
- rtc_quota_manager.py: QuotaManager логика (демоут + каскадный промоут)
- rtc_apply_decisions.py: Применение QuotaDecision планерами
- orchestrator_messaging.py: Оркестратор для messaging пайплайна

Таблицы СУБД:
- sim_masterv2_msg: Результаты симуляции (отдельно от основной ветки)

Дата создания: 07-01-2026
"""

from .base_model_messaging import V2BaseModelMessaging
from .orchestrator_messaging import MessagingOrchestrator

__all__ = [
    'V2BaseModelMessaging',
    'MessagingOrchestrator',
]


