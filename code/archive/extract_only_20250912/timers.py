#!/usr/bin/env python3
"""
Простой таймер для этапов пайплайна: t_host_fill_env / t_sim_step / t_export
Дата: 2025-08-28
"""

from __future__ import annotations
import time


class Timer:
    def __init__(self, label: str):
        self.label = label
        self.t0 = 0.0

    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        t1 = time.perf_counter()
        dt_ms = (t1 - self.t0) * 1000.0
        print(f"{self.label}={dt_ms:.2f} ms")


