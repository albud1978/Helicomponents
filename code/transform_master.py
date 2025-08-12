#!/usr/bin/env python3
"""
Transform Master Orchestrator

Запускает подготовку окружения, симуляцию по дням (будет добавлено) и экспорт LoggingLayer Planes (MP2) в ClickHouse.
Пока симуляция заглушена, создаётся пустая/демо запись в MP2 для проверки пайплайна экспорта.

Дата: 2025-08-10
"""

import sys
import os
from datetime import date

# Локальные импорты
from flame_macroproperty2_exporter import FlameMacroProperty2Exporter


def main():
    print("🔥 Transform Master — оркестратор")

    # MP2 Export table ensure
    exporter = FlameMacroProperty2Exporter()
    exporter.ensure_table()

    # TODO: здесь будет цикл симуляции с FLAME GPU
    # Временная демо‑вставка одной строки для контроля таблицы
    demo_row = {
        'dates': date(2025, 7, 4),
        'aircraft_number': 0,
        'ac_type_mask': 32,
        'status_id': 3,
        'daily_flight': 0,
        'trigger_pr_final_mi8': 0,
        'trigger_pr_final_mi17': 0,
        'partout_trigger': 0,
        'assembly_trigger': 0,
        'aircraft_age_years': 0,
        'mfg_date_final': date(1970,1,1),
        'simulation_metadata': 'demo: no-sim yet'
    }
    exporter.insert_rows([demo_row])
    print("✅ MP2 таблица подготовлена, демо‑строка вставлена")

if __name__ == '__main__':
    main()