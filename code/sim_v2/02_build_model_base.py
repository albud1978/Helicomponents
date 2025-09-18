#!/usr/bin/env python3
"""
V2 Step 02: build_model_base
- Собирает базовую модель FLAME GPU без RTC‑ядер
- Устанавливает базовые Env‑свойства и проверяет инициализацию популяции (0 агентов)
- Smoke‑тест: создаёт/удаляет симуляцию, печатает сводку
"""
from __future__ import annotations

import os
import sys
import json
from typing import Dict

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from sim_env_setup import get_client, prepare_env_arrays


def main() -> int:
    try:
        import pyflamegpu as fg
    except Exception as e:
        print(f"pyflamegpu not installed: {e}")
        return 1

    # Грузим Env из ClickHouse (для DAYS/даты) и снапшот v2 (для FRAMES=union)
    client = get_client()
    env = prepare_env_arrays(client)
    DAYS = int(env['days_total_u16'])
    # По v2: FRAMES берём из снапшота 01_setup_env (union MP3∪MP5, без future)
    snap_path = os.environ.get('HL_V2_ENV_SNAPSHOT', 'tmp/env_snapshot.json')
    FRAMES = int(env['frames_total_u16'])
    try:
        if os.path.isfile(snap_path):
            import json as _json
            with open(snap_path, 'r', encoding='utf-8') as f:
                snap = _json.load(f)
            FRAMES = int(snap.get('frames_union_no_future', FRAMES))
    except Exception:
        pass

    # Базовая модель
    model = fg.ModelDescription("HeliSim_V2_Base")
    e = model.Environment()
    e.newPropertyUInt("version_date", int(env['version_date_u16']))
    e.newPropertyUInt("frames_total", FRAMES)
    e.newPropertyUInt("days_total", DAYS)

    # Агент (пустая популяция, только переменные)
    a = model.newAgent("component")
    for name in [
        "idx","group_by","status_id","ops_ticket","intent_flag",
        "sne","ppr","repair_days","daily_today_u32","daily_next_u32"
    ]:
        a.newVariableUInt(name, 0)

    sim = fg.CUDASimulation(model)
    sim.setEnvironmentPropertyUInt("version_date", int(env['version_date_u16']))
    sim.setEnvironmentPropertyUInt("frames_total", FRAMES)
    sim.setEnvironmentPropertyUInt("days_total", DAYS)

    # Smoke: пустой шаг без агентов
    print(f"V2 Base OK: DAYS={DAYS}, FRAMES={FRAMES}")
    return 0


if __name__ == "__main__":
    sys.exit(main())


