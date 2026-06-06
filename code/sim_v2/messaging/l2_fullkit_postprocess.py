#!/usr/bin/env python3
# EXPERIMENTAL / REFERENCE (2026-06-06): пробный L2-контур (group_by=3/4) в messaging. Не production. Боевой L2 — code/sim_v2/units/orchestrator_units.py. Оставлен как справочный черновик.
"""
L2 fullkit postprocess:
Fill missing engine ops (>=2) for planner ops rows by inserting synthetic units.
"""
from __future__ import annotations

from typing import Iterable, List, Tuple

from utils.config_loader import get_clickhouse_client

SENTINEL_PARTSEQNO_I = 999999


def _delete_previous_synthetic_rows(
    client, table_units: str, units_vd: int, version_id: int, sentinel: int
) -> None:
    delete_sql = f"""
    ALTER TABLE {table_units} DELETE
    WHERE version_date = %(units_vd)s
      AND version_id = %(vid)s
      AND partseqno_i = %(sentinel)s
    SETTINGS mutations_sync = 1
    """
    client.execute(
        delete_sql,
        {"units_vd": units_vd, "vid": version_id, "sentinel": sentinel},
    )


def _fetch_missing_engine_ops(
    client,
    table_main: str,
    table_units: str,
    planner_vd: int,
    units_vd: int,
    version_id: int,
) -> List[Tuple[int, int, int, int]]:
    query = f"""
    WITH planner_ops AS (
        SELECT
            day_u16,
            aircraft_number,
            group_by
        FROM {table_main}
        WHERE version_date = %(planner_vd)s
          AND version_id = %(vid)s
          AND group_by IN (1, 2)
          AND status_id = 2
          AND aircraft_number > 0
    ),
    engine_ops AS (
        SELECT
            day_u16,
            aircraft_number,
            group_by,
            count() AS engines
        FROM {table_units}
        WHERE version_date = %(units_vd)s
          AND version_id = %(vid)s
          AND group_by IN (3, 4)
          AND state = 2
          AND aircraft_number > 0
        GROUP BY day_u16, aircraft_number, group_by
    )
    SELECT
        p.day_u16,
        p.aircraft_number,
        if(p.group_by = 1, 3, 4) AS target_group,
        ifNull(e.engines, 0) AS engines
    FROM planner_ops AS p
    LEFT JOIN engine_ops AS e
      ON p.day_u16 = e.day_u16
     AND p.aircraft_number = e.aircraft_number
     AND e.group_by = if(p.group_by = 1, 3, 4)
    WHERE ifNull(e.engines, 0) < 2
    ORDER BY p.day_u16, p.aircraft_number
    """
    rows = client.execute(
        query,
        {"planner_vd": planner_vd, "units_vd": units_vd, "vid": version_id},
    )
    return [(int(d), int(acn), int(gb), int(cnt)) for d, acn, gb, cnt in rows]


def _fetch_max_ids(client, table_units: str, units_vd: int, version_id: int) -> Tuple[int, int]:
    query = f"""
    SELECT
        ifNull(max(psn), 0) AS max_psn,
        ifNull(max(idx), 0) AS max_idx
    FROM {table_units}
    WHERE version_date = %(units_vd)s
      AND version_id = %(vid)s
    """
    max_psn, max_idx = client.execute(
        query, {"units_vd": units_vd, "vid": version_id}
    )[0]
    return int(max_psn), int(max_idx)


def _insert_rows(
    client, table_units: str, rows: Iterable[Tuple[int, int, int, int, int, int, int, int, int, int, int, int, int, int]]
) -> None:
    client.execute(
        f"""
        INSERT INTO {table_units} (
            version_date,
            version_id,
            day_u16,
            idx,
            psn,
            group_by,
            partseqno_i,
            aircraft_number,
            sne,
            ppr,
            state,
            repair_days,
            queue_position,
            active
        ) VALUES
        """,
        list(rows),
    )


def apply_l2_fullkit_postprocess(
    planner_version_date: int,
    units_version_date_int: int,
    version_id: int,
    table_main: str = "sim_masterv2_v9",
    table_units: str = "sim_units_v2",
    sentinel_partseqno: int = SENTINEL_PARTSEQNO_I,
    batch_size: int = 50000,
) -> int:
    """
    Ensures >=2 engines in ops per planner ops row by inserting synthetic rows.
    Returns number of inserted rows.
    """
    client = get_clickhouse_client()

    _delete_previous_synthetic_rows(
        client, table_units, units_version_date_int, version_id, sentinel_partseqno
    )

    missing = _fetch_missing_engine_ops(
        client,
        table_main,
        table_units,
        planner_version_date,
        units_version_date_int,
        version_id,
    )
    if not missing:
        print("   OK L2 fullkit postprocess: no missing engines found")
        return 0

    max_psn, max_idx = _fetch_max_ids(
        client, table_units, units_version_date_int, version_id
    )
    next_psn = max_psn + 1
    next_idx = max_idx + 1

    rows: List[Tuple[int, int, int, int, int, int, int, int, int, int, int, int, int, int]] = []
    inserted = 0

    for day_u16, acn, target_group, engines in missing:
        gap = 2 - int(engines)
        if gap <= 0:
            continue
        for _ in range(gap):
            rows.append(
                (
                    units_version_date_int,
                    version_id,
                    int(day_u16),
                    int(next_idx),
                    int(next_psn),
                    int(target_group),
                    int(sentinel_partseqno),
                    int(acn),
                    0,  # sne
                    0,  # ppr
                    2,  # state
                    0,  # repair_days
                    0,  # queue_position
                    1,  # active
                )
            )
            next_psn += 1
            next_idx += 1
            inserted += 1

            if len(rows) >= batch_size:
                _insert_rows(client, table_units, rows)
                rows = []

    if rows:
        _insert_rows(client, table_units, rows)

    print(f"   OK L2 fullkit postprocess: inserted {inserted:,} synthetic rows")
    return inserted


__all__ = ["apply_l2_fullkit_postprocess", "SENTINEL_PARTSEQNO_I"]
