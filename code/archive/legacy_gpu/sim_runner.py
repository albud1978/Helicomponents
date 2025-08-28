#!/usr/bin/env python3
"""
Оркестратор симуляции (первый инкремент):
- Загрузка MP/Property в память (без дублирования в БД)
- Создание модели и агентов
- Подготовка окружения на сутки D0 и выполнение слоёв 4/6
- Единый батч‑экспорт MP2 за D0
Дата: 2025-08-21
"""

from __future__ import annotations
from typing import Dict
from datetime import date, timedelta

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from config_loader import get_clickhouse_client

from flame_gpu_helicopter_model import HelicopterFlameModel
from sim_env_setup import (
    fetch_versions, fetch_mp1_br_rt, fetch_mp3, preload_mp4_by_day, preload_mp5_maps, build_daily_arrays
)
from sim_agent_factory import build_agents
from sim_logging_mp2 import export_day

try:
    import pyflamegpu
except Exception:
    pyflamegpu = None


def run(days: int = 1, host_only: bool = False, probe: str | None = None):
    if pyflamegpu is None:
        raise RuntimeError("pyflamegpu не установлен")
    # CUDA_PATH fallback для JIT (FLAME GPU ищет стандартные инклюды)
    if not os.environ.get('CUDA_PATH'):
        # Популярные пути CUDA в WSL/Ubuntu
        candidates = [
            "/usr/local/cuda",
            "/usr/local/cuda-12.4",
            "/usr/local/cuda-12.3",
            "/usr/local/cuda-12.2",
            "/usr/local/cuda-12.1",
            "/usr/local/cuda-12.0",
        ]
        for p in candidates:
            if os.path.isdir(p) and os.path.isdir(os.path.join(p, 'include')):
                os.environ['CUDA_PATH'] = p
                break
    # Мини-проверка наличия include
    cuda_path = os.environ.get('CUDA_PATH')
    if not cuda_path or not os.path.isdir(os.path.join(cuda_path, 'include')):
        raise RuntimeError("CUDA_PATH не установлен или не содержит include. Установите CUDA и задайте CUDA_PATH.")

    client = get_clickhouse_client()
    vdate, vid = fetch_versions(client)
    mp1 = fetch_mp1_br_rt(client)
    mp3_rows, mp3_fields = fetch_mp3(client, vdate, vid)
    n_agents = len(mp3_rows)

    # Опции режима
    # Сброс потенциальных флагов минимального режима
    os.environ.pop('RTC_MINIMAL', None)
    if host_only:
        os.environ['HOST_ONLY_SIM'] = '1'
    else:
        os.environ.pop('HOST_ONLY_SIM', None)
    if probe:
        os.environ['FLAMEGPU_PROBE'] = probe
    else:
        os.environ.pop('FLAMEGPU_PROBE', None)
    model = HelicopterFlameModel()
    model.build_model(num_agents=n_agents)
    sim = model.build_simulation()
    agent_desc = model.model.getAgent("component")

    # Агенты: probe-режим минимальный, иначе полный
    if probe:
        agent_desc = model.model.getAgent("component")
        # Мини‑популяция для RTC‑проб
        n_probe = 3
        av = pyflamegpu.AgentVector(agent_desc, n_probe)
        for i in range(n_probe):
            av[i].setVariableUInt('idx', i)
            # Базовые статус/ремонт
            av[i].setVariableUInt('status_id', 4)
            av[i].setVariableUInt('repair_days', 0)
            av[i].setVariableUInt('repair_time', 1)
            av[i].setVariableUInt('ppr', 0)
            # Ресурсы для ops_check (нулевые допустимы для компиляции/шага)
            av[i].setVariableUInt('sne', 0)
            av[i].setVariableUInt('ll', 0)
            av[i].setVariableUInt('oh', 0)
            av[i].setVariableUInt('br', 0)
        sim.setPopulationData(av)
    else:
        build_agents(sim, model, mp3_rows, mp3_fields, mp1)

    # Предзагрузка MP4/MP5
    mp4_by_day = preload_mp4_by_day(client)
    mp5_maps = preload_mp5_maps(client)

    # Бежим дни (пока первый инкремент — 1 день)
    days_list = sorted(mp4_by_day.keys())
    if not days_list:
        raise RuntimeError("Нет дат в MP4")
    if days:
        days_list = days_list[:days]

    epoch = date(1970,1,1)
    for D in days_list:
        # Подготовка окружения на день D
        today_map = mp5_maps.get(D, {})
        next_map = mp5_maps.get(D + timedelta(days=1), {})
        if not probe:
            daily_today, daily_next, partout_arr, asm_arr = build_daily_arrays(mp3_rows, mp3_fields, mp1, today_map, next_map)
            sim.setEnvironmentPropertyArrayUInt32("daily_today", daily_today)
            sim.setEnvironmentPropertyArrayUInt32("daily_next", daily_next)
            sim.setEnvironmentPropertyArrayUInt32("partout_time_arr", partout_arr)
            sim.setEnvironmentPropertyArrayUInt32("assembly_time_arr", asm_arr)
            sim.setEnvironmentPropertyUInt("current_day_ordinal", (D - epoch).days)
        else:
            # Для probe‑режимов RTC (например, ops_check) заполняем окружение минимальными массивами
            n_probe = 3
            sim.setEnvironmentPropertyArrayUInt32("daily_today", [0] * n_probe)
            sim.setEnvironmentPropertyArrayUInt32("daily_next", [0] * n_probe)
            sim.setEnvironmentPropertyArrayUInt32("partout_time_arr", [0] * n_probe)
            sim.setEnvironmentPropertyArrayUInt32("assembly_time_arr", [0] * n_probe)
            sim.setEnvironmentPropertyUInt("current_day_ordinal", (D - epoch).days)

        # Выполнить слои (первый инкремент — используем уже зарегистрированные rtc функции)
        sim.step()  # включает rtc_repair → rtc_ops_check → rtc_main → rtc_change → rtc_pass_through

        if not probe:
            # Экспорт MP2 (единый батч)
            ops_targets = mp4_by_day.get(D, {"ops_counter_mi8":0, "ops_counter_mi17":0})
            export_day(sim, agent_desc, D, daily_today, ops_targets, vdate, vid)

        break  # один день в первом инкременте


def main():
    import argparse
    p = argparse.ArgumentParser(description='Simulation Runner (increment 1)')
    p.add_argument('--days', type=int, default=1)
    p.add_argument('--host-only', action='store_true', help='Запуск без RTC (HostFunction)')
    p.add_argument('--probe', type=str, default=None, help='FLAMEGPU_PROBE: repair|ops_check|main|change|pass ...')
    a = p.parse_args()
    run(days=a.days, host_only=a.host_only, probe=a.probe)


if __name__ == '__main__':
    main()


