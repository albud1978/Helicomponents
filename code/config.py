#!/usr/bin/env python3
"""
Конфигурация запуска симуляций (CLI/ENV)
Дата: 2025-08-28
"""

from __future__ import annotations
from dataclasses import dataclass
import os


@dataclass
class Config:
    days: int = 1
    smoke_days: int = 1
    use_gpu_logger: bool = False
    seatbelts: bool = True
    # Имена таблиц ClickHouse
    table_mp1: str = "md_components"
    table_mp3: str = "heli_pandas"
    table_mp4: str = "flight_program_ac"
    table_mp5: str = "flight_program_fl"
    table_mp2_export: str = "flame_macroproperty2_export"


def load_config() -> Config:
    cfg = Config()
    # ENV overrides (минимум для автономности, CLI остаётся в оркестраторе)
    cfg.days = int(os.getenv("FGPU_DAYS", cfg.days))
    cfg.smoke_days = int(os.getenv("FGPU_SMOKE_DAYS", cfg.smoke_days))
    cfg.use_gpu_logger = os.getenv("FGPU_USE_GPU_LOGGER", "0") in ("1", "true", "True")
    cfg.seatbelts = os.getenv("FGPU_SEATBELTS", "1") in ("1", "true", "True")
    # Таблицы (оставляем дефолты, можно переопределить через ENV)
    cfg.table_mp1 = os.getenv("FGPU_TBL_MP1", cfg.table_mp1)
    cfg.table_mp3 = os.getenv("FGPU_TBL_MP3", cfg.table_mp3)
    cfg.table_mp4 = os.getenv("FGPU_TBL_MP4", cfg.table_mp4)
    cfg.table_mp5 = os.getenv("FGPU_TBL_MP5", cfg.table_mp5)
    cfg.table_mp2_export = os.getenv("FGPU_TBL_MP2", cfg.table_mp2_export)
    return cfg


