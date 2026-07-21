#!/usr/bin/env python3
"""Apply day-0 OPS deficit demotions to heli_pandas.

Destination gates (симметрия Mi-8/Mi-17), только для demoted OPS:
- сначала program_ac с 2025-07-04; без истории → planer 1, agg 7
- в программе + remain_calendar_days > 0 → planer 3, agg 3
- в программе + нет положительного календаря → planer 1, agg 3
Fallback нет treq OH(D) → due=base+10y−1д: только здесь (и только при истории в программе).
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Sequence

import pandas as pd

from extract.planer_calendar_remain import (
    destination_for_remain,
    fetch_calendar_remain_by_psn,
    normalize_registr,
    open_dwh_client,
    program_history_serials,
)


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
        "psn",
        "serialno",
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


def _fetch_ops_planers(
    client,
    version_date: date,
    version_id: int,
    aircraft_numbers: Sequence[int],
) -> pd.DataFrame:
    acn_in = _format_uint_in(aircraft_numbers)
    rows = client.execute(
        f"""
        SELECT
            toUInt32(ifNull(aircraft_number, 0)) AS aircraft_number,
            toUInt64(ifNull(psn, 0)) AS psn,
            serialno,
            toUInt8(ifNull(group_by, 0)) AS group_by
        FROM heli_pandas
        WHERE version_date = %(vd)s
          AND version_id = %(vi)s
          AND toUInt8(ifNull(status_id, 0)) = 2
          AND toUInt8(ifNull(group_by, 0)) IN (1, 2)
          AND toUInt32(ifNull(aircraft_number, 0)) IN ({acn_in})
        ORDER BY aircraft_number, serialno
        """,
        {"vd": version_date, "vi": version_id},
    )
    return pd.DataFrame(
        rows, columns=["aircraft_number", "psn", "serialno", "group_by"]
    )


def enrich_demotion_destinations(
    client,
    version_date: date,
    version_id: int,
    demoted: pd.DataFrame,
    *,
    dwh=None,
) -> pd.DataFrame:
    """Добавляет remain_d / oh_due / program_history / destination_* к demoted."""
    audit_cols = [
        "psn",
        "serialno",
        "remain_d",
        "oh_due",
        "oh_at_date",
        "program_history",
        "planer_status_id",
        "aggregates_status_id",
        "destination_reason",
    ]
    if demoted.empty:
        out = demoted.copy()
        for col in audit_cols:
            out[col] = pd.Series(dtype="object")
        return out

    aircraft_numbers = [int(v) for v in demoted["aircraft_number"].tolist()]
    planers = _fetch_ops_planers(client, version_date, version_id, aircraft_numbers)
    if planers.empty:
        raise RuntimeError(
            "enrich_demotion_destinations: нет OPS-планеров status_id=2 "
            f"для demoted acn={aircraft_numbers}"
        )

    if dwh is None:
        dwh = open_dwh_client()
    history = program_history_serials(client)
    fallback_psns = {
        int(row.psn)
        for row in planers.itertuples(index=False)
        if normalize_registr(row.serialno) in history
    }
    cal = fetch_calendar_remain_by_psn(
        dwh,
        version_date,
        [int(p) for p in planers["psn"].tolist()],
        fallback_10y_psns=fallback_psns,
    )

    # Один планер на aircraft_number (берём первую строку).
    by_acn: Dict[int, dict] = {}
    for row in planers.itertuples(index=False):
        acn = int(row.aircraft_number)
        if acn in by_acn:
            continue
        psn = int(row.psn)
        serial = normalize_registr(row.serialno)
        rem = cal.get(psn)
        remain_d = rem.remain_d if rem else None
        in_hist = serial in history
        planer_st, agg_st, reason = destination_for_remain(remain_d, in_hist)
        by_acn[acn] = {
            "psn": psn,
            "serialno": str(row.serialno),
            "remain_d": remain_d,
            "oh_due": rem.oh_due if rem else None,
            "oh_at_date": rem.oh_at_date if rem else None,
            "program_history": int(in_hist),
            "planer_status_id": planer_st,
            "aggregates_status_id": agg_st,
            "destination_reason": reason,
        }

    out = demoted.copy()
    for col in (
        "psn",
        "serialno",
        "remain_d",
        "oh_due",
        "oh_at_date",
        "program_history",
        "planer_status_id",
        "aggregates_status_id",
        "destination_reason",
    ):
        out[col] = [
            by_acn[int(acn)][col] if int(acn) in by_acn else None
            for acn in out["aircraft_number"].tolist()
        ]
    missing = [int(a) for a in out["aircraft_number"] if int(a) not in by_acn]
    if missing:
        raise RuntimeError(
            f"enrich_demotion_destinations: нет OPS-планера для acn={missing}"
        )
    return out


def _count_ops_rows(
    client,
    version_date: date,
    version_id: int,
    *,
    aircraft_numbers: Sequence[int],
    planers: bool,
) -> int:
    if not aircraft_numbers:
        return 0
    acn_in = _format_uint_in(aircraft_numbers)
    gb = "IN (1, 2)" if planers else "> 2"
    return int(
        client.execute(
            f"""
            SELECT count()
            FROM heli_pandas
            WHERE version_date = %(vd)s
              AND version_id = %(vi)s
              AND toUInt8(ifNull(status_id, 0)) = 2
              AND toUInt8(ifNull(group_by, 0)) {gb}
              AND toUInt32(ifNull(aircraft_number, 0)) IN ({acn_in})
            """,
            {"vd": version_date, "vi": version_id},
        )[0][0]
    )


def _update_status(
    client,
    version_date: date,
    version_id: int,
    *,
    aircraft_numbers: Sequence[int],
    status_id: int,
    planers: bool,
) -> None:
    if not aircraft_numbers:
        return
    acn_in = _format_uint_in(aircraft_numbers)
    gb = "IN (1, 2)" if planers else "> 2"
    client.execute(
        f"""
        ALTER TABLE heli_pandas
        UPDATE status_id = %(st)s
        WHERE version_date = %(vd)s
          AND version_id = %(vi)s
          AND toUInt8(ifNull(status_id, 0)) = 2
          AND toUInt8(ifNull(group_by, 0)) {gb}
          AND toUInt32(ifNull(aircraft_number, 0)) IN ({acn_in})
        """,
        {"vd": version_date, "vi": version_id, "st": int(status_id)},
    )


def apply_demotions(
    client,
    version_date: date,
    version_id: int,
    demoted: pd.DataFrame,
    *,
    dry_run: bool,
) -> dict[str, Any]:
    ensure_columns(client)
    empty_stats: dict[str, Any] = {
        "planers": 0,
        "planers_to_3": 0,
        "planers_to_1": 0,
        "aggregates_to_3": 0,
        "aggregates_to_7": 0,
        "by_reason": {},
        "destinations": [],
    }
    if demoted.empty:
        return empty_stats

    enriched = (
        demoted
        if "planer_status_id" in demoted.columns
        else enrich_demotion_destinations(client, version_date, version_id, demoted)
    )

    buckets: Dict[str, List[int]] = {
        "planer_3": [],
        "planer_1": [],
        "agg_3": [],
        "agg_7": [],
    }
    by_reason: Dict[str, int] = {}
    destinations: List[dict] = []

    def _clean(value):
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        return value

    for row in enriched.itertuples(index=False):
        acn = int(row.aircraft_number)
        planer_st = int(row.planer_status_id)
        agg_st = int(row.aggregates_status_id)
        reason = str(row.destination_reason)
        by_reason[reason] = by_reason.get(reason, 0) + 1
        remain_raw = _clean(getattr(row, "remain_d", None))
        destinations.append(
            {
                "aircraft_number": acn,
                "serialno": getattr(row, "serialno", None),
                "remain_d": int(remain_raw) if remain_raw is not None else None,
                "oh_due": _clean(getattr(row, "oh_due", None)),
                "program_history": int(getattr(row, "program_history", 0) or 0),
                "planer_status_id": planer_st,
                "aggregates_status_id": agg_st,
                "destination_reason": reason,
            }
        )
        if planer_st == 3:
            buckets["planer_3"].append(acn)
        else:
            buckets["planer_1"].append(acn)
        if agg_st == 3:
            buckets["agg_3"].append(acn)
        else:
            buckets["agg_7"].append(acn)

    all_acn = [int(v) for v in enriched["aircraft_number"].tolist()]
    stats = {
        "planers": _count_ops_rows(
            client, version_date, version_id, aircraft_numbers=all_acn, planers=True
        ),
        "planers_to_3": len(buckets["planer_3"]),
        "planers_to_1": len(buckets["planer_1"]),
        "aggregates_to_3": _count_ops_rows(
            client,
            version_date,
            version_id,
            aircraft_numbers=buckets["agg_3"],
            planers=False,
        ),
        "aggregates_to_7": _count_ops_rows(
            client,
            version_date,
            version_id,
            aircraft_numbers=buckets["agg_7"],
            planers=False,
        ),
        "by_reason": by_reason,
        "destinations": destinations,
    }

    if dry_run:
        return stats

    client.execute("SET mutations_sync = 1")
    _update_status(
        client,
        version_date,
        version_id,
        aircraft_numbers=buckets["planer_3"],
        status_id=3,
        planers=True,
    )
    _update_status(
        client,
        version_date,
        version_id,
        aircraft_numbers=buckets["planer_1"],
        status_id=1,
        planers=True,
    )
    _update_status(
        client,
        version_date,
        version_id,
        aircraft_numbers=buckets["agg_3"],
        status_id=3,
        planers=False,
    )
    _update_status(
        client,
        version_date,
        version_id,
        aircraft_numbers=buckets["agg_7"],
        status_id=7,
        planers=False,
    )
    return stats
