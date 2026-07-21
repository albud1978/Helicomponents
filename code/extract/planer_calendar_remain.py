#!/usr/bin/env python3
"""Календарный остаток OH планеров (дни) из DWH + история program_ac.

Единые destination gates для:
- day0 OPS deficit demote (`deficit_demoter`)
- OOR inactive/serviceable classifier этап 3b (`inactive_serviceable_classifier`)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, Iterable, Optional, Sequence, Set

PROGRAM_AC_HISTORY_START = date(2025, 7, 4)
_SENTINEL_OH = date(1972, 1, 1)
# Нет treq OH(D): due = base+10y−1д (inclusive 10y).
# Включать только через fallback_10y_psns; в 3b не передавать (demote-only).
_FALLBACK_OH_YEARS = 10


@dataclass(frozen=True)
class CalendarRemain:
    psn: int
    oh_at_date: Optional[date]
    mfg_date: Optional[date]
    raw_interval: Optional[int]
    int_days: Optional[int]
    base_date: Optional[date]
    oh_due: Optional[date]
    remain_d: Optional[int]
    used_fallback_10y: bool = False


def normalize_registr(value) -> str:
    return str(value).strip().zfill(5)


def program_history_serials(client) -> Set[str]:
    """serial/ac_registr из project program_ac с version_date >= 2025-07-04."""
    rows = client.execute(
        """
        SELECT DISTINCT ac_registr
        FROM program_ac
        WHERE version_date >= %(start)s
        """,
        {"start": PROGRAM_AC_HISTORY_START},
    )
    return {normalize_registr(r[0]) for r in rows if r[0] is not None}


def _int_days(raw: int) -> int:
    # В AMOS treq календарный интервал бывает в годах (5/8/10/11) или в днях.
    return raw * 365 if raw < 100 else raw


def _base_date(oh_at: Optional[date], mfg_d: Optional[date]) -> Optional[date]:
    if oh_at is None or oh_at <= _SENTINEL_OH:
        return mfg_d
    return oh_at


def _add_years(base: date, years: int) -> date:
    try:
        return base.replace(year=base.year + years)
    except ValueError:
        # 29.02 → 28.02 в невисокосном году
        return base.replace(year=base.year + years, day=28)


def _due_fallback_10y(base: date) -> date:
    """due = base + 10y − 1 день (последний день 10-летнего периода)."""
    return _add_years(base, _FALLBACK_OH_YEARS) - timedelta(days=1)


def _resolve_report_date(dwh, day0: date) -> date:
    rows = dwh.query(
        """
        SELECT max(report_date)
        FROM reports.amos_heli_rotables_components_status
        WHERE report_date <= toDate(%(vd)s)
        """,
        parameters={"vd": day0.isoformat()},
    ).result_rows
    if rows and rows[0][0] is not None:
        return rows[0][0]
    rows = dwh.query(
        "SELECT max(report_date) FROM reports.amos_heli_rotables_components_status"
    ).result_rows
    if not rows or rows[0][0] is None:
        raise RuntimeError(
            "DWH: пустая reports.amos_heli_rotables_components_status — "
            "нельзя вычислить календарный остаток OH"
        )
    return rows[0][0]


def fetch_calendar_remain_by_psn(
    dwh,
    day0: date,
    psns: Sequence[int],
    *,
    fallback_10y_psns: Optional[Set[int]] = None,
) -> Dict[int, CalendarRemain]:
    """remain_d по psn.

    Нет treq OH(D): fallback due=base+10y−1д только если psn ∈ fallback_10y_psns
    (борта с историей program_ac с 2025-07-04). Иначе remain_d=None.
    """
    unique = sorted({int(p) for p in psns if int(p) > 0})
    if not unique:
        return {}
    allowed_fallback = {int(p) for p in (fallback_10y_psns or set())}

    report_date = _resolve_report_date(dwh, day0)
    psn_in = ", ".join(str(p) for p in unique)

    vit_rows = dwh.query(
        f"""
        SELECT
            psn,
            oh_at_date,
            toDateOrNull(mfg_date) AS mfg_d
        FROM reports.amos_heli_rotables_components_status
        WHERE report_date = toDate(%(rd)s)
          AND psn IN ({psn_in})
        """,
        parameters={"rd": report_date.isoformat()},
    ).result_rows
    vit = {int(r[0]): (r[1], r[2]) for r in vit_rows}

    int_rows = dwh.query(
        f"""
        SELECT
            f.psn AS psn,
            max(i.amount_interval) AS raw_int
        FROM source.amos_heli_forecast f
        JOIN source.amos_heli_treq_time_requirement tr
            ON tr.event_key = f.event_perfno_i AND tr.valid_to IS NULL
        JOIN source.amos_heli_treq_interval_group ig
            ON ig.timerequirementno_i = tr.timerequirementno_i
        JOIN source.amos_heli_treq_dimension_group dg
            ON dg.interval_groupno_i = ig.interval_groupno_i
        JOIN source.amos_heli_treq_interval i
            ON i.dimension_groupno_i = dg.dimension_groupno_i
        WHERE f.valid_to IS NULL
          AND f.event LIKE 'OH%%'
          AND ig.threshold = 'N'
          AND i.dimension_type = 'I'
          AND i.counter_defno_i = 3
          AND f.psn IN ({psn_in})
        GROUP BY f.psn
        """,
    ).result_rows
    raw_by_psn = {int(r[0]): int(r[1]) for r in int_rows if r[1] is not None}

    out: Dict[int, CalendarRemain] = {}
    for psn in unique:
        oh_at, mfg_d = vit.get(psn, (None, None))
        raw = raw_by_psn.get(psn)
        base = _base_date(oh_at, mfg_d)

        if raw is None:
            if base is not None and psn in allowed_fallback:
                due = _due_fallback_10y(base)
                int_d = (due - base).days
                out[psn] = CalendarRemain(
                    psn=psn,
                    oh_at_date=oh_at,
                    mfg_date=mfg_d,
                    raw_interval=None,
                    int_days=int_d,
                    base_date=base,
                    oh_due=due,
                    remain_d=(due - day0).days,
                    used_fallback_10y=True,
                )
            else:
                out[psn] = CalendarRemain(
                    psn=psn,
                    oh_at_date=oh_at,
                    mfg_date=mfg_d,
                    raw_interval=None,
                    int_days=None,
                    base_date=base,
                    oh_due=None,
                    remain_d=None,
                    used_fallback_10y=False,
                )
            continue

        int_d = _int_days(raw)
        if base is None:
            out[psn] = CalendarRemain(
                psn=psn,
                oh_at_date=oh_at,
                mfg_date=mfg_d,
                raw_interval=raw,
                int_days=int_d,
                base_date=None,
                oh_due=None,
                remain_d=None,
                used_fallback_10y=False,
            )
            continue
        due = base + timedelta(days=int_d)
        out[psn] = CalendarRemain(
            psn=psn,
            oh_at_date=oh_at,
            mfg_date=mfg_d,
            raw_interval=raw,
            int_days=int_d,
            base_date=base,
            oh_due=due,
            remain_d=(due - day0).days,
            used_fallback_10y=False,
        )
    return out


def open_dwh_client():
    """Fail-fast DWH client; без silent fallback."""
    try:
        from utils.dwh_golden_replay_export import dwh_client
    except Exception:
        from dwh_golden_replay_export import dwh_client  # type: ignore
    return dwh_client()


def destination_for_remain(
    remain_d: Optional[int],
    in_program_history: bool,
) -> tuple[int, int, str]:
    """
    Returns (planer_status_id, aggregates_status_id, reason).

    Порядок: сначала program_ac (с 2025-07-04), потом календарный OH.
    - не в программе → planer 1, agg 7
    - в программе + remain_d > 0 → planer 3, agg 3
    - в программе + нет положительного календаря → planer 1, agg 3
    """
    if not in_program_history:
        return 1, 7, "not_in_program"
    if remain_d is not None and remain_d > 0:
        return 3, 3, "in_program_calendar_positive"
    return 1, 3, "in_program_no_calendar"
