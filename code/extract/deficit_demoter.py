#!/usr/bin/env python3
"""Apply day-0 OPS deficit demotions to heli_pandas."""

from __future__ import annotations

from datetime import date
from typing import Sequence

import pandas as pd


SERVICEABLE_CONDITION_SQL = """
upperUTF8(replaceRegexpAll(ifNull(condition, ''), '^\\s+|\\s+$', '')) = 'ИСПРАВНЫЙ'
"""


def select_demotions(ranking: pd.DataFrame, comparison: pd.DataFrame) -> pd.DataFrame:
    selected: list[pd.DataFrame] = []
    for _, row in comparison.iterrows():
        group_by = int(row["group_by"])
        excess = int(row["excess"])
        if excess <= 0:
            continue
        ranked_group = ranking[ranking["group_by"].astype(int) == group_by]
        if len(ranked_group) < excess:
            raise RuntimeError(
                f"Not enough OPS planers for group_by={group_by}: "
                f"need {excess}, have {len(ranked_group)}"
            )
        selected.append(ranked_group.head(excess).copy())

    if not selected:
        return pd.DataFrame(columns=list(ranking.columns))

    demoted = pd.concat(selected, ignore_index=True)
    demoted.insert(0, "demote_order", range(1, len(demoted) + 1))
    return demoted


def _format_uint_in(values: Sequence[int]) -> str:
    unique = sorted({int(value) for value in values})
    if not unique:
        raise ValueError("IN list is empty")
    if any(value <= 0 for value in unique):
        raise ValueError(f"Aircraft numbers must be positive UInt values: {unique}")
    return ", ".join(str(value) for value in unique)


def ensure_columns(client) -> None:
    required = {
        "version_date",
        "version_id",
        "aircraft_number",
        "group_by",
        "status_id",
        "condition",
    }
    rows = client.execute(
        """
        SELECT name
        FROM system.columns
        WHERE database = currentDatabase()
          AND table = 'heli_pandas'
        """
    )
    existing = {name for (name,) in rows}
    missing = required - existing
    if missing:
        raise RuntimeError(f"heli_pandas missing required columns: {sorted(missing)}")


def apply_demotions(
    client,
    version_date: date,
    version_id: int,
    demoted: pd.DataFrame,
    *,
    dry_run: bool,
) -> dict[str, int]:
    ensure_columns(client)
    if demoted.empty:
        return {"planers": 0, "aggregates_to_3": 0, "aggregates_to_7": 0}

    aircraft_numbers = [int(value) for value in demoted["aircraft_number"].tolist()]
    acn_in = _format_uint_in(aircraft_numbers)
    params = {"vd": version_date, "vi": version_id}

    planers = int(
        client.execute(
            f"""
            SELECT count()
            FROM heli_pandas
            WHERE version_date = %(vd)s
              AND version_id = %(vi)s
              AND toUInt8(ifNull(status_id, 0)) = 2
              AND toUInt8(ifNull(group_by, 0)) IN (1, 2)
              AND toUInt32(ifNull(aircraft_number, 0)) IN ({acn_in})
            """,
            params,
        )[0][0]
    )
    aggregates_to_3 = int(
        client.execute(
            f"""
            SELECT count()
            FROM heli_pandas
            WHERE version_date = %(vd)s
              AND version_id = %(vi)s
              AND toUInt8(ifNull(status_id, 0)) = 2
              AND toUInt32(ifNull(group_by, 0)) > 2
              AND toUInt32(ifNull(aircraft_number, 0)) IN ({acn_in})
              AND {SERVICEABLE_CONDITION_SQL}
            """,
            params,
        )[0][0]
    )
    aggregates_to_7 = int(
        client.execute(
            f"""
            SELECT count()
            FROM heli_pandas
            WHERE version_date = %(vd)s
              AND version_id = %(vi)s
              AND toUInt8(ifNull(status_id, 0)) = 2
              AND toUInt32(ifNull(group_by, 0)) > 2
              AND toUInt32(ifNull(aircraft_number, 0)) IN ({acn_in})
              AND NOT ({SERVICEABLE_CONDITION_SQL})
            """,
            params,
        )[0][0]
    )
    stats = {
        "planers": planers,
        "aggregates_to_3": aggregates_to_3,
        "aggregates_to_7": aggregates_to_7,
    }

    if dry_run:
        return stats

    client.execute("SET mutations_sync = 1")
    client.execute(
        f"""
        ALTER TABLE heli_pandas
        UPDATE status_id = 3
        WHERE version_date = %(vd)s
          AND version_id = %(vi)s
          AND toUInt8(ifNull(status_id, 0)) = 2
          AND toUInt8(ifNull(group_by, 0)) IN (1, 2)
          AND toUInt32(ifNull(aircraft_number, 0)) IN ({acn_in})
        """,
        params,
    )
    client.execute(
        f"""
        ALTER TABLE heli_pandas
        UPDATE status_id = 3
        WHERE version_date = %(vd)s
          AND version_id = %(vi)s
          AND toUInt8(ifNull(status_id, 0)) = 2
          AND toUInt32(ifNull(group_by, 0)) > 2
          AND toUInt32(ifNull(aircraft_number, 0)) IN ({acn_in})
          AND {SERVICEABLE_CONDITION_SQL}
        """,
        params,
    )
    client.execute(
        f"""
        ALTER TABLE heli_pandas
        UPDATE status_id = 7
        WHERE version_date = %(vd)s
          AND version_id = %(vi)s
          AND toUInt8(ifNull(status_id, 0)) = 2
          AND toUInt32(ifNull(group_by, 0)) > 2
          AND toUInt32(ifNull(aircraft_number, 0)) IN ({acn_in})
          AND NOT ({SERVICEABLE_CONDITION_SQL})
        """,
        params,
    )
    return stats
