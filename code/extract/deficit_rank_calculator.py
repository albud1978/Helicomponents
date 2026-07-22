#!/usr/bin/env python3
"""Rank day-0 OPS planers by aggregate deficit severity.

Rank modes (extract/day0 only; simulation demote is untouched):
- flat: sum of all shortage gaps (legacy)
- tiered: critical → powertrain → other; АГБ (35–37) ignored in sort keys

Critical (tier-1) is a compound signal only:
both engines missing (gap ≥ 2 on group 3 or 4) AND main VR missing (group 12 or 13).
VR-only or engines-only do not raise deficit_crit.
"""

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
GROUP_PREFIX_RE = re.compile(r"^(\d+(?:\|\d+)*)")

# Engines (Mi-8=3, Mi-17=4); main VR (Mi-8=12, Mi-17=13)
ENGINE_GROUPS = frozenset({3, 4})
MAIN_VR_GROUPS = frozenset({12, 13})
# Kept for callers/tests that still import the old name
CRITICAL_GROUPS = ENGINE_GROUPS | MAIN_VR_GROUPS
# Tier-2: tail/intermediate gearbox + APU
POWERTRAIN_GROUPS = frozenset({7, 8, 14, 15, 16})
# Soft instruments excluded from tiered sort keys (day-to-day churn)
SOFT_AGB_GROUPS = frozenset({35, 36, 37})
# Mislabelled Mi-17 APU OR shortage "12|13|14" — not main VR
APU_OR_LABEL_GROUPS = frozenset({12, 13, 14})

RANK_MODES = ("flat", "tiered")


def parse_shortage(entry: str) -> tuple[int, int]:
    match = SHORTAGE_RE.search(entry)
    if not match:
        return (0, 0)
    return int(match.group(1)), int(match.group(2))


def parse_shortage_groups(entry: str) -> set[int]:
    # fetch_aggregations may mark shortages with leading/trailing '**'
    cleaned = entry.strip().strip("*")
    match = GROUP_PREFIX_RE.match(cleaned)
    if not match:
        return set()
    return {int(token) for token in match.group(1).split("|") if token.isdigit()}


def _is_engine_shortage(groups: set[int]) -> bool:
    return bool(groups) and groups <= ENGINE_GROUPS


def _is_main_vr_shortage(groups: set[int]) -> bool:
    # exact main VR only; reject APU OR label {12,13,14}
    if not groups or groups == APU_OR_LABEL_GROUPS:
        return False
    return groups <= MAIN_VR_GROUPS


def _classify_non_crit_gap(groups: set[int], gap: int) -> tuple[int, int, int]:
    """Return (powertrain, other_ranked, soft) for gaps outside compound-crit."""
    if gap <= 0 or not groups:
        return (0, 0, 0)
    if groups & POWERTRAIN_GROUPS:
        return (gap, 0, 0)
    if groups & SOFT_AGB_GROUPS:
        return (0, 0, gap)
    return (0, gap, 0)


def _deficit_from_shortages(shortages: list[str]) -> dict[str, object]:
    deficit_units = 0
    deficit_powertrain = 0
    deficit_other = 0
    deficit_soft = 0
    engine_gap = 0
    vr_gap = 0
    missing: list[str] = []

    for entry in shortages:
        installed, required = parse_shortage(entry)
        gap = required - installed
        if gap <= 0:
            continue
        deficit_units += gap
        missing.append(entry.strip("*"))
        groups = parse_shortage_groups(entry)
        if _is_engine_shortage(groups):
            engine_gap += gap
            continue
        if _is_main_vr_shortage(groups):
            vr_gap += gap
            continue
        pt, other, soft = _classify_non_crit_gap(groups, gap)
        deficit_powertrain += pt
        deficit_other += other
        deficit_soft += soft

    # Compound crit only: both engines missing (need=2 → gap≥2) AND main VR missing
    if engine_gap >= 2 and vr_gap >= 1:
        deficit_crit = engine_gap + vr_gap
    else:
        # engines-only or VR-only: not tier-1; count in other
        deficit_crit = 0
        deficit_other += engine_gap + vr_gap

    return {
        "deficit_units": int(deficit_units),
        "deficit_positions": int(len(missing)),
        "deficit_crit": int(deficit_crit),
        "deficit_powertrain": int(deficit_powertrain),
        "deficit_other": int(deficit_other),
        "deficit_soft": int(deficit_soft),
        "missing_groups": "; ".join(missing),
    }


def apply_rank_sort(ranking: pd.DataFrame, rank_mode: str) -> pd.DataFrame:
    """Sort and assign rank_in_group. Does not mutate caller if copy is returned."""
    if rank_mode not in RANK_MODES:
        raise ValueError(f"Unknown rank_mode={rank_mode!r}; expected one of {RANK_MODES}")
    if ranking.empty:
        out = ranking.copy()
        if "rank_in_group" not in out.columns:
            out.insert(0, "rank_in_group", pd.Series(dtype="int64"))
        return out

    out = ranking.copy()
    if rank_mode == "flat":
        out = out.sort_values(
            ["group_by", "deficit_units", "deficit_positions", "aircraft_number"],
            ascending=[True, False, False, False],
            kind="mergesort",
        )
    else:
        # tiered: compound crit (2 engines+VR) → powertrain → other; АГБ ignored
        out = out.sort_values(
            [
                "group_by",
                "deficit_crit",
                "deficit_powertrain",
                "deficit_other",
                "deficit_units",
                "deficit_positions",
                "aircraft_number",
            ],
            ascending=[True, False, False, False, False, False, False],
            kind="mergesort",
        )
    out = out.reset_index(drop=True)
    if "rank_in_group" in out.columns:
        out = out.drop(columns=["rank_in_group"])
    out.insert(0, "rank_in_group", out.groupby("group_by").cumcount() + 1)
    out.insert(1, "rank_mode", rank_mode)
    return out


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
    rank_mode: str = "tiered",
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
        deficits = _deficit_from_shortages(list(item.shortage_groups))
        rows.append(
            {
                "aircraft_number": aircraft_number,
                "group_by": group_by,
                "type": "Mi-8" if group_by == 1 else "Mi-17",
                "variant": item.variant or "",
                "installed_total": int(item.total_components),
                "required_components": int(item.required_components),
                **deficits,
            }
        )

    ranking = pd.DataFrame(rows)
    empty_cols = [
        "rank_in_group",
        "rank_mode",
        "aircraft_number",
        "group_by",
        "type",
        "variant",
        "installed_total",
        "required_components",
        "deficit_units",
        "deficit_positions",
        "deficit_crit",
        "deficit_powertrain",
        "deficit_other",
        "deficit_soft",
        "missing_groups",
    ]
    if ranking.empty:
        ranking = pd.DataFrame(columns=empty_cols)
    else:
        ranking = apply_rank_sort(ranking, rank_mode)

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        ranking.to_excel(output_path, index=False)

    return ranking


def ranking_from_missing_groups_frame(
    frame: pd.DataFrame,
    *,
    rank_mode: str = "tiered",
) -> pd.DataFrame:
    """Re-score an existing ranking/audit frame by missing_groups (for dry counterfactuals)."""
    rows: list[dict[str, object]] = []
    for _, row in frame.iterrows():
        miss = str(row.get("missing_groups") or "")
        entries = [e.strip() for e in miss.split(";") if e.strip() and e.strip() != "nan"]
        deficits = _deficit_from_shortages(entries)
        rows.append(
            {
                "aircraft_number": int(row["aircraft_number"]),
                "group_by": int(row["group_by"]),
                "type": row.get("type")
                or ("Mi-8" if int(row["group_by"]) == 1 else "Mi-17"),
                "variant": row.get("variant") or "",
                "installed_total": int(row.get("installed_total") or 0),
                "required_components": int(row.get("required_components") or 0),
                **deficits,
            }
        )
    ranking = pd.DataFrame(rows)
    if ranking.empty:
        return ranking
    return apply_rank_sort(ranking, rank_mode)
