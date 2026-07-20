#!/usr/bin/env python3
"""Rank day-0 OPS planers by aggregate deficit severity."""

from __future__ import annotations

import re
import sys
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

code_root = Path(__file__).resolve().parents[1]
sys.path.append(str(code_root))
sys.path.append(str(code_root / "utils"))

from heli_pandas_ops_other_groups import VersionInfo, fetch_aggregations  # type: ignore
from static_data_resolver import resolve_latest_md_slice  # type: ignore


SHORTAGE_RE = re.compile(r":(\d+)/(\d+)")


def parse_shortage(entry: str) -> tuple[int, int]:
    match = SHORTAGE_RE.search(entry)
    if not match:
        return (0, 0)
    return int(match.group(1)), int(match.group(2))


def _deficit_from_shortages(shortages: list[str]) -> tuple[int, int, str]:
    deficit_units = 0
    missing: list[str] = []
    for entry in shortages:
        installed, required = parse_shortage(entry)
        gap = required - installed
        if gap > 0:
            deficit_units += gap
            missing.append(entry.strip("*"))
    return deficit_units, len(missing), "; ".join(missing)


def _fetch_ops_plane_groups(client, version_date: date, version_id: int) -> dict[int, int]:
    rows = client.execute(
        """
        SELECT
            toUInt32(aircraft_number) AS aircraft_number,
            any(toUInt8(group_by)) AS group_by_value
        FROM heli_pandas
        WHERE version_date = %(vd)s
          AND version_id = %(vi)s
          AND toUInt8(ifNull(status_id, 0)) = 2
          AND toUInt8(ifNull(group_by, 0)) IN (1, 2)
          AND toUInt32(ifNull(aircraft_number, 0)) > 0
        GROUP BY aircraft_number
        """,
        {"vd": version_date, "vi": version_id},
    )
    return {int(aircraft_number): int(group_by) for aircraft_number, group_by in rows}


def calculate_deficit_ranking(
    client,
    version_date: date,
    version_id: int,
    *,
    output_path: Optional[Path] = None,
) -> pd.DataFrame:
    md_slice = resolve_latest_md_slice(client)
    version = VersionInfo(version_date=version_date.isoformat(), version_id=version_id)
    md_version = VersionInfo(
        version_date=md_slice.version_date,
        version_id=md_slice.version_id,
    )

    group_by_by_acn = _fetch_ops_plane_groups(client, version_date, version_id)
    aggregations = fetch_aggregations(client, version, md_version)
    rows: list[dict[str, object]] = []
    for item in aggregations:
        aircraft_number = int(item.aircraft_number)
        group_by = group_by_by_acn.get(aircraft_number)
        if group_by is None:
            raise RuntimeError(
                f"OPS planer {aircraft_number} has no status_id=2 group_by in heli_pandas"
            )
        deficit_units, deficit_positions, missing = _deficit_from_shortages(
            list(item.shortage_groups)
        )
        rows.append(
            {
                "aircraft_number": aircraft_number,
                "group_by": group_by,
                "type": "Mi-8" if group_by == 1 else "Mi-17",
                "variant": item.variant or "",
                "installed_total": int(item.total_components),
                "required_components": int(item.required_components),
                "deficit_units": int(deficit_units),
                "deficit_positions": int(deficit_positions),
                "missing_groups": missing,
            }
        )

    ranking = pd.DataFrame(rows)
    if ranking.empty:
        ranking = pd.DataFrame(
            columns=[
                "rank_in_group",
                "aircraft_number",
                "group_by",
                "type",
                "variant",
                "installed_total",
                "required_components",
                "deficit_units",
                "deficit_positions",
                "missing_groups",
            ]
        )
    else:
        ranking = ranking.sort_values(
            ["group_by", "deficit_units", "deficit_positions", "aircraft_number"],
            ascending=[True, False, False, False],
            kind="mergesort",
        ).reset_index(drop=True)
        ranking.insert(0, "rank_in_group", ranking.groupby("group_by").cumcount() + 1)

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        ranking.to_excel(output_path, index=False)

    return ranking
