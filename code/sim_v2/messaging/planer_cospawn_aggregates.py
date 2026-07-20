#!/usr/bin/env python3
"""Co-spawn агрегатов при рождении планера (MVP multiBOM L2).

HostFunction читает факты spawn текущего шага (det + dynamic) и пишет
записи рождения агрегатов в sim_l2_birth_v1. Наработки агрегатов не симулируются.
"""
from __future__ import annotations

import time
from typing import Any

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from sim_v2.components.planer_spawn_bom import (
    PsnSpawnAllocator,
    ReferenceBom,
    build_co_spawn_birth_rows,
)

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}") from e

from model_build import MAX_DAYS


def ensure_l2_birth_table(client) -> None:
    client.execute(
        """
        CREATE TABLE IF NOT EXISTS sim_l2_birth_v1 (
            version_date UInt32,
            version_id UInt32,
            birth_day UInt16,
            aircraft_number UInt32,
            planer_group_by UInt8,
            spawn_kind LowCardinality(String),
            partno String,
            partseqno_i UInt32,
            group_by UInt8,
            psn UInt32,
            state UInt8,
            pre_status_id UInt8,
            sne UInt32,
            ppr UInt32
        ) ENGINE = MergeTree()
        ORDER BY (version_date, version_id, birth_day, aircraft_number, psn)
        """
    )


def export_l2_births_to_ch(
    client,
    rows: list[dict[str, Any]],
    version_date_int: int,
    version_id: int,
) -> int:
    if not rows:
        return 0

    ensure_l2_birth_table(client)
    client.execute(
        """
        ALTER TABLE sim_l2_birth_v1 DELETE
        WHERE version_date = %(vd)s AND version_id = %(vi)s
        """,
        {"vd": int(version_date_int), "vi": int(version_id)},
        settings={"mutations_sync": 2},
    )

    columns = [
        "version_date",
        "version_id",
        "birth_day",
        "aircraft_number",
        "planer_group_by",
        "spawn_kind",
        "partno",
        "partseqno_i",
        "group_by",
        "psn",
        "state",
        "pre_status_id",
        "sne",
        "ppr",
    ]
    data = [[row[col] for col in columns] for row in rows]
    client.execute(
        f"INSERT INTO sim_l2_birth_v1 ({', '.join(columns)}) VALUES",
        data,
        settings={"max_partitions_per_insert_block": 300},
    )
    return len(rows)


class HF_PlanerCoSpawnAggregates(fg.HostFunction):
    """После spawn-слоёв: фиксирует рождение полного BOM агрегатов на планер."""

    def __init__(
        self,
        bom_by_gb: dict[int, ReferenceBom],
        allocator: PsnSpawnAllocator,
        birth_records: list[dict[str, Any]],
        version_date_int: int,
        version_id: int,
        days_total: int,
    ):
        super().__init__()
        self.bom_by_gb = bom_by_gb
        self.allocator = allocator
        self.birth_records = birth_records
        self.version_date_int = int(version_date_int)
        self.version_id = int(version_id)
        self.days_total = int(days_total)
        self._processed: set[tuple[int, str]] = set()

    def _emit_batch(
        self,
        spawn_kind: str,
        planner_group_by: int,
        planer_state: int,
        birth_day: int,
        base_acn: int,
        count: int,
    ) -> None:
        if count <= 0:
            return
        bom = self.bom_by_gb.get(planner_group_by)
        if bom is None:
            raise RuntimeError(f"Нет BOM для planner_group_by={planner_group_by}")

        for offset in range(int(count)):
            acn = int(base_acn) + offset
            self.birth_records.extend(
                build_co_spawn_birth_rows(
                    bom,
                    self.allocator,
                    version_date_int=self.version_date_int,
                    version_id=self.version_id,
                    birth_day=int(birth_day),
                    aircraft_number=acn,
                    planer_group_by=int(planner_group_by),
                    planer_state=int(planer_state),
                    spawn_kind=spawn_kind,
                )
            )

    def run(self, FLAMEGPU):
        env = FLAMEGPU.environment
        current_day_mp = env.getMacroPropertyUInt32("current_day_mp")
        day = int(current_day_mp[0])
        if self.days_total <= 0:
            return
        safe_day = day if day < self.days_total else self.days_total - 1

        batches = (
            ("det", 2, 3, "det_spawn_need", "det_spawn_base_acn"),
            ("dynamic_mi17", 2, 2, "spawn_dynamic_need", "spawn_dynamic_base_acn"),
            ("dynamic_mi8", 1, 2, "spawn_dynamic_need_mi8", "spawn_dynamic_base_acn_mi8"),
        )
        for spawn_kind, planner_gb, planer_state, need_name, acn_name in batches:
            key = (safe_day, spawn_kind)
            if key in self._processed:
                continue
            need_mp = env.getMacroPropertyUInt32(need_name)
            acn_mp = env.getMacroPropertyUInt32(acn_name)
            need = int(need_mp[safe_day])
            if need <= 0:
                continue
            base_acn = int(acn_mp[safe_day])
            self._emit_batch(spawn_kind, planner_gb, planer_state, safe_day, base_acn, need)
            self._processed.add(key)


def register_planer_cospawn_hf(
    model: fg.ModelDescription,
    *,
    bom_by_gb: dict[int, ReferenceBom],
    allocator: PsnSpawnAllocator,
    birth_records: list[dict[str, Any]],
    version_date_int: int,
    version_id: int,
    days_total: int,
) -> fg.HostFunction:
    hf = HF_PlanerCoSpawnAggregates(
        bom_by_gb=bom_by_gb,
        allocator=allocator,
        birth_records=birth_records,
        version_date_int=version_date_int,
        version_id=version_id,
        days_total=days_total,
    )
    layer = model.newLayer("layer_planer_cospawn_aggregates")
    layer.addHostFunction(hf)

    total_units = {gb: bom.total_units for gb, bom in bom_by_gb.items()}
    print(
        "  ✅ Co-spawn агрегатов: layer_planer_cospawn_aggregates "
        f"(gb1 units={total_units.get(1, 0)}, gb2 units={total_units.get(2, 0)})"
    )
    return hf
