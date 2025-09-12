#!/usr/bin/env python3
"""
I/O слой окружения: загрузка MP1/MP3/MP4/MP5 из ClickHouse, подготовка Env, экспорт MP2
Дата: 2025-08-28
"""

from __future__ import annotations
from typing import Dict, List, Tuple
from datetime import date, timedelta

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from config_loader import get_clickhouse_client

from sim_env_setup import (
    fetch_versions,
    fetch_mp1_br_rt,
    fetch_mp3,
    preload_mp4_by_day,
    preload_mp5_maps,
    build_daily_arrays,
    prepare_env_arrays,
    apply_env_to_sim,
)


def load_env(cfg=None) -> Dict[str, object]:
    client = get_clickhouse_client()
    env = prepare_env_arrays(client)
    # Дополнительно вернём исходные MP3 строки для инициализации агентов
    vdate, vid = fetch_versions(client)
    mp3_rows, mp3_fields = fetch_mp3(client, vdate, vid)
    env['mp3_rows'] = mp3_rows
    env['mp3_fields'] = mp3_fields
    return env


def apply_env(sim, env_data: Dict[str, object]):
    return apply_env_to_sim(sim, env_data)


def init_agents(sim, env_data: Dict[str, object]):
    import pyflamegpu
    from repair_only_model import RepairOnlyModel
    # Используем уже собранную модель в симуляции
    model = sim.getModelDescription()
    agent_desc = model.getAgent("component")
    mp3_rows = env_data['mp3_rows']
    mp3_fields = env_data['mp3_fields']
    frames_index = env_data.get('frames_index', {})
    av = pyflamegpu.AgentVector(agent_desc, len(mp3_rows))
    idx = {name: i for i, name in enumerate(mp3_fields)}
    for i, r in enumerate(mp3_rows):
        ai = av[i]
        ac_num = int(r[idx.get('aircraft_number', -1)] or 0)
        ai.setVariableUInt("idx", int(frames_index.get(ac_num, 0)))
        ai.setVariableUInt("psn", int(r[idx['psn']] or 0))
        ai.setVariableUInt("partseqno_i", int(r[idx['partseqno_i']] or 0))
        ai.setVariableUInt("group_by", int(r[idx.get('group_by', -1)] or 0))
        ai.setVariableUInt("aircraft_number", ac_num)
        ai.setVariableUInt("ac_type_mask", int(r[idx.get('ac_type_mask', -1)] or 0))
        # Остальные переменные остаются по текущей логике в sim_master (не дублируем здесь)
    sim.setPopulationData(av)


def export_mp2(sim, cfg=None, tag: str = "main"):
    # Плейсхолдер: экспорт реализован в sim_master сейчас; перенесём после согласования
    pass


