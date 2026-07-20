#!/usr/bin/env python3
"""Эталонный BOM агрегатов для co-spawn вместе с рождением планера.

Контракт MVP multiBOM L2:
- эталонный планер: max(mfg_date) среди OPS с ровно 2 двигателями (=2, не >=2);
- BOM: все агрегаты (group_by NOT IN 1,2) на борту эталона, qty по partseqno_i;
- PSN: psn_spawn_start(partseqno_i) + per-номенклатурный счётчик (блок md_components).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any


@dataclass(frozen=True)
class BomLine:
    partno: str
    partseqno_i: int
    group_by: int
    qty: int
    psn_spawn_start: int


@dataclass(frozen=True)
class ReferenceBom:
    planner_group_by: int
    ref_aircraft_number: int
    ref_mfg_date: date | None
    engine_group_by: int
    lines: tuple[BomLine, ...]

    @property
    def total_units(self) -> int:
        return sum(line.qty for line in self.lines)


def _engine_group_by(planner_group_by: int) -> int:
    if planner_group_by == 1:
        return 3
    if planner_group_by == 2:
        return 4
    raise ValueError(f"Неподдерживаемый planner_group_by={planner_group_by}")


def resolve_reference_bom(
    client,
    version_date: date,
    version_id: int,
    planner_group_by: int,
) -> ReferenceBom:
    """Эталонный BOM для co-spawn планеров заданного group_by."""
    if planner_group_by not in (1, 2):
        raise ValueError(f"planner_group_by должен быть 1 или 2, получено {planner_group_by}")

    engine_gb = _engine_group_by(planner_group_by)
    ref_rows = client.execute(
        """
        WITH planner_ops AS (
            SELECT
                aircraft_number,
                group_by,
                mfg_date,
                if(group_by = 1, 3, 4) AS engine_gb
            FROM heli_pandas
            WHERE version_date = %(vd)s
              AND version_id = %(vi)s
              AND status_id = 2
              AND group_by = %(pgb)s
              AND aircraft_number > 0
        ),
        engine_cnt AS (
            SELECT aircraft_number, group_by, count() AS e_ops
            FROM heli_pandas
            WHERE version_date = %(vd)s
              AND version_id = %(vi)s
              AND status_id = 2
              AND group_by IN (3, 4)
              AND aircraft_number > 0
            GROUP BY aircraft_number, group_by
        ),
        fullkit AS (
            SELECT
                p.aircraft_number,
                p.group_by,
                p.mfg_date,
                ifNull(e.e_ops, 0) AS e_ops
            FROM planner_ops AS p
            LEFT JOIN engine_cnt AS e
                ON e.aircraft_number = p.aircraft_number
               AND e.group_by = p.engine_gb
            WHERE ifNull(e.e_ops, 0) = 2
        )
        SELECT aircraft_number, group_by, mfg_date
        FROM fullkit
        ORDER BY mfg_date DESC NULLS LAST, aircraft_number DESC
        LIMIT 1
        """,
        {"vd": version_date, "vi": version_id, "pgb": planner_group_by},
    )
    if not ref_rows:
        raise RuntimeError(
            f"Нет эталонного планера gb={planner_group_by} с fullkit (=2 двигателя) "
            f"для {version_date} v{version_id}"
        )

    ref_acn, ref_gb, ref_mfg = ref_rows[0]
    ref_acn = int(ref_acn)
    ref_gb = int(ref_gb)
    ref_mfg_date = ref_mfg if isinstance(ref_mfg, date) else None

    bom_rows = client.execute(
        """
        SELECT
            h.partno,
            toUInt32(h.partseqno_i) AS partseqno_i,
            toUInt8(h.group_by) AS group_by,
            count() AS qty,
            toUInt32(ifNull(m.psn_spawn_start, 0)) AS psn_spawn_start
        FROM heli_pandas AS h
        LEFT JOIN md_components AS m ON m.partseqno_i = h.partseqno_i
        WHERE h.version_date = %(vd)s
          AND h.version_id = %(vi)s
          AND h.aircraft_number = %(acn)s
          AND h.group_by NOT IN (1, 2)
          AND h.partseqno_i IS NOT NULL
          AND h.partseqno_i > 0
        GROUP BY h.partno, h.partseqno_i, h.group_by, m.psn_spawn_start
        ORDER BY h.group_by, h.partseqno_i
        """,
        {"vd": version_date, "vi": version_id, "acn": ref_acn},
    )
    if not bom_rows:
        raise RuntimeError(
            f"Эталонный борт acn={ref_acn} не содержит агрегатов (group_by>2) "
            f"для {version_date} v{version_id}"
        )

    lines: list[BomLine] = []
    for partno, partseqno_i, gb, qty, psn_start in bom_rows:
        psn_start = int(psn_start or 0)
        if psn_start <= 0:
            raise RuntimeError(
                f"partseqno_i={partseqno_i} ({partno}) без psn_spawn_start в md_components"
            )
        lines.append(
            BomLine(
                partno=str(partno),
                partseqno_i=int(partseqno_i),
                group_by=int(gb),
                qty=int(qty),
                psn_spawn_start=psn_start,
            )
        )

    return ReferenceBom(
        planner_group_by=ref_gb,
        ref_aircraft_number=ref_acn,
        ref_mfg_date=ref_mfg_date,
        engine_group_by=engine_gb,
        lines=tuple(lines),
    )


class PsnSpawnAllocator:
    """Per-номенклатурный счётчик PSN внутри блока psn_spawn_start."""

    def __init__(self) -> None:
        self._counters: dict[int, int] = {}

    def allocate(self, partseqno_i: int, psn_spawn_start: int) -> int:
        counter = self._counters.get(partseqno_i, 0)
        psn = int(psn_spawn_start) + counter
        self._counters[partseqno_i] = counter + 1
        return psn

    def snapshot(self) -> dict[int, int]:
        return dict(self._counters)


def build_co_spawn_birth_rows(
    bom: ReferenceBom,
    allocator: PsnSpawnAllocator,
    *,
    version_date_int: int,
    version_id: int,
    birth_day: int,
    aircraft_number: int,
    planer_group_by: int,
    planer_state: int,
    spawn_kind: str,
) -> list[dict[str, Any]]:
    """Строки рождения агрегатов для одного новорождённого планера."""
    if planer_group_by != bom.planner_group_by:
        raise ValueError(
            f"spawn planer gb={planer_group_by} не совпадает с BOM gb={bom.planner_group_by}"
        )

    rows: list[dict[str, Any]] = []
    for line in bom.lines:
        for _ in range(line.qty):
            psn = allocator.allocate(line.partseqno_i, line.psn_spawn_start)
            rows.append(
                {
                    "version_date": int(version_date_int),
                    "version_id": int(version_id),
                    "birth_day": int(birth_day),
                    "aircraft_number": int(aircraft_number),
                    "planer_group_by": int(planer_group_by),
                    "spawn_kind": str(spawn_kind),
                    "partno": line.partno,
                    "partseqno_i": int(line.partseqno_i),
                    "group_by": int(line.group_by),
                    "psn": int(psn),
                    "state": int(planer_state),
                    "pre_status_id": 0,
                    "sne": 0,
                    "ppr": 0,
                }
            )
    return rows


def load_reference_boms(
    client,
    version_date: date,
    version_id: int,
) -> dict[int, ReferenceBom]:
    """Загрузить эталоны для Mi-8 (gb1) и Mi-17 (gb2)."""
    return {
        1: resolve_reference_bom(client, version_date, version_id, planner_group_by=1),
        2: resolve_reference_bom(client, version_date, version_id, planner_group_by=2),
    }
