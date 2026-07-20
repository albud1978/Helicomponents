#!/usr/bin/env python3
"""Compare day-0 OPS counts in heli_pandas with MP4 targets."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

code_root = Path(__file__).resolve().parents[1]
sys.path.append(str(code_root / "utils"))


@dataclass(frozen=True)
class OpsTargetRow:
    group_by: int
    type: str
    ops_count: int
    target: int
    excess: int
    target_date: date


def ensure_columns(client) -> None:
    required_by_table = {
        "heli_pandas": {"version_date", "version_id", "group_by", "status_id", "aircraft_number"},
        "flight_program_ac": {
            "version_date",
            "version_id",
            "dates",
            "ops_counter_mi8",
            "ops_counter_mi17",
        },
    }
    for table, required in required_by_table.items():
        rows = client.execute(
            """
            SELECT name
            FROM system.columns
            WHERE database = currentDatabase()
              AND table = %(table)s
            """,
            {"table": table},
        )
        existing = {name for (name,) in rows}
        missing = required - existing
        if missing:
            raise RuntimeError(f"{table} missing required columns: {sorted(missing)}")


def load_day0_targets(client, version_date: date, version_id: int) -> tuple[date, int, int]:
    rows = client.execute(
        """
        SELECT dates, ops_counter_mi8, ops_counter_mi17
        FROM flight_program_ac
        WHERE version_date = %(vd)s
          AND version_id = %(vi)s
        ORDER BY dates
        """,
        {"vd": version_date, "vi": version_id},
    )
    if not rows:
        raise RuntimeError(
            f"flight_program_ac is empty for version_date={version_date}, "
            f"version_id={version_id}; MP4 targets are required"
        )

    selected = None
    for row in rows:
        if row[0] == version_date:
            selected = row
            break
    if selected is None:
        selected = rows[0]

    target_date, mi8, mi17 = selected
    return target_date, int(mi8 or 0), int(mi17 or 0)


def load_ops_counts(client, version_date: date, version_id: int) -> dict[int, int]:
    rows = client.execute(
        """
        SELECT
            toUInt8(group_by) AS group_by_value,
            countDistinct(aircraft_number) AS ops_count
        FROM heli_pandas
        WHERE version_date = %(vd)s
          AND version_id = %(vi)s
          AND toUInt8(ifNull(status_id, 0)) = 2
          AND toUInt8(ifNull(group_by, 0)) IN (1, 2)
          AND toUInt32(ifNull(aircraft_number, 0)) > 0
        GROUP BY group_by
        """,
        {"vd": version_date, "vi": version_id},
    )
    counts = {1: 0, 2: 0}
    for group_by, ops_count in rows:
        counts[int(group_by)] = int(ops_count)
    return counts


def compare_ops_to_target(client, version_date: date, version_id: int) -> pd.DataFrame:
    ensure_columns(client)
    target_date, mi8_target, mi17_target = load_day0_targets(
        client, version_date, version_id
    )
    ops = load_ops_counts(client, version_date, version_id)
    rows = [
        OpsTargetRow(
            group_by=1,
            type="Mi-8",
            ops_count=ops[1],
            target=mi8_target,
            excess=max(0, ops[1] - mi8_target),
            target_date=target_date,
        ),
        OpsTargetRow(
            group_by=2,
            type="Mi-17",
            ops_count=ops[2],
            target=mi17_target,
            excess=max(0, ops[2] - mi17_target),
            target_date=target_date,
        ),
    ]
    return pd.DataFrame([row.__dict__ for row in rows])
