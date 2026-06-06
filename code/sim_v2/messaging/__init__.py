#!/usr/bin/env python3
"""
Messaging архитектура для симуляции планеров

Активный V8-стек использует нативные сообщения FLAME GPU для
квотирования и RepairLine, сохраняя базовые типы модели в этом пакете.

Актуальная структура:
- orchestrator_limiter_v8.py: активный оркестратор LIMITER V8
- base_model_messaging.py: базовая модель с Message типами, QuotaManager и RepairLine агентами

Таблицы СУБД:
- sim_masterv2_msg: Результаты симуляции (отдельно от основной ветки)

Дата создания: 07-01-2026
"""

from .base_model_messaging import V2BaseModelMessaging
__all__ = [
    'V2BaseModelMessaging',
]


