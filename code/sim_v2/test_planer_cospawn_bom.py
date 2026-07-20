#!/usr/bin/env python3
"""Офлайн-тесты co-spawn BOM (без ClickHouse)."""

from datetime import date

from sim_v2.components.planer_spawn_bom import (
    BomLine,
    PsnSpawnAllocator,
    ReferenceBom,
    build_co_spawn_birth_rows,
)


def _sample_bom(planner_group_by: int = 2) -> ReferenceBom:
    return ReferenceBom(
        planner_group_by=planner_group_by,
        ref_aircraft_number=25100,
        ref_mfg_date=date(2024, 1, 1),
        engine_group_by=4 if planner_group_by == 2 else 3,
        lines=(
            BomLine("TV3-117", 1001, 4, 2, 11_000_000),
            BomLine("VR-14", 2002, 5, 1, 12_000_000),
        ),
    )


def test_allocator_unique_per_partseqno():
    alloc = PsnSpawnAllocator()
    assert alloc.allocate(1001, 11_000_000) == 11_000_000
    assert alloc.allocate(1001, 11_000_000) == 11_000_001
    assert alloc.allocate(2002, 12_000_000) == 12_000_000


def test_build_co_spawn_birth_rows_qty_and_binding():
    bom = _sample_bom(2)
    alloc = PsnSpawnAllocator()
    rows = build_co_spawn_birth_rows(
        bom,
        alloc,
        version_date_int=20260712,
        version_id=1,
        birth_day=42,
        aircraft_number=100005,
        planer_group_by=2,
        planer_state=2,
        spawn_kind="dynamic_mi17",
    )
    assert len(rows) == 3
    assert all(r["aircraft_number"] == 100005 for r in rows)
    assert all(r["birth_day"] == 42 for r in rows)
    assert all(r["pre_status_id"] == 0 for r in rows)
    assert all(r["sne"] == 0 and r["ppr"] == 0 for r in rows)
    engines = [r for r in rows if r["group_by"] == 4]
    assert len(engines) == 2
    assert engines[0]["psn"] != engines[1]["psn"]


def test_group_by_mismatch_raises():
    bom = _sample_bom(2)
    alloc = PsnSpawnAllocator()
    try:
        build_co_spawn_birth_rows(
            bom,
            alloc,
            version_date_int=20260712,
            version_id=1,
            birth_day=1,
            aircraft_number=100000,
            planer_group_by=1,
            planer_state=2,
            spawn_kind="dynamic_mi8",
        )
        raised = False
    except ValueError:
        raised = True
    assert raised


if __name__ == "__main__":
    test_allocator_unique_per_partseqno()
    test_build_co_spawn_birth_rows_qty_and_binding()
    test_group_by_mismatch_raises()
    print("OK: planer cospawn bom tests")
