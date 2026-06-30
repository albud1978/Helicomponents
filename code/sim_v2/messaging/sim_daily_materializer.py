#!/usr/bin/env python3
"""Materialize daily BI views for sim_masterv2_v9."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from sim_env_setup import get_client


TABLE_NAME = "default.sim_masterv2_v9_daily"
DEFICIT_TABLE_NAME = "default.sim_deficit_v9_daily"
SOURCE_TABLE = "default.sim_masterv2_v9"


DDL_DAILY = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    version_date UInt32,
    version_id UInt32,
    group_by UInt8,
    status_id UInt8,
    day_date Date,
    day_d Date,
    status_count_ffill UInt16,
    group_by_label String,
    version_date_ddmmyyyy String
) ENGINE = MergeTree()
PARTITION BY (version_date, version_id)
ORDER BY (version_date, version_id, group_by, status_id, day_date)
"""


DDL_DEFICIT = f"""
CREATE TABLE IF NOT EXISTS {DEFICIT_TABLE_NAME} (
    version_date UInt32,
    version_id UInt32,
    day_date Date,
    group_by UInt8,
    target UInt32,
    ops_count UInt32,
    deficit Int32
) ENGINE = MergeTree()
PARTITION BY (version_date, version_id)
ORDER BY (version_date, version_id, group_by, day_date)
"""


# clickhouse_driver uses pyformat placeholders, so literal percent signs must be escaped as %%.
INSERT_DAILY = f"""
INSERT INTO {TABLE_NAME}
(
    version_date,
    version_id,
    group_by,
    status_id,
    day_date,
    day_d,
    status_count_ffill,
    group_by_label,
    version_date_ddmmyyyy
)
WITH events_daily AS (
    SELECT
        version_date AS version_date,
        version_id AS version_id,
        group_by AS group_by,
        aircraft_number AS aircraft_number,
        day_date AS day_date,
        argMax(status_id, idx) AS status_id
    FROM {SOURCE_TABLE}
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND group_by IN (1, 2)
    GROUP BY version_date, version_id, group_by, aircraft_number, day_date
),
version_bounds AS (
    SELECT
        version_date AS version_date,
        version_id AS version_id,
        max(day_date) AS version_max_day
    FROM events_daily
    GROUP BY version_date, version_id
),
aircraft_span AS (
    SELECT
        e.version_date AS version_date,
        e.version_id AS version_id,
        e.group_by AS group_by,
        e.aircraft_number AS aircraft_number,
        min(e.day_date) AS aircraft_min_day,
        vb.version_max_day AS aircraft_max_day
    FROM events_daily e
    INNER JOIN version_bounds vb
      ON vb.version_date = e.version_date
     AND vb.version_id = e.version_id
    GROUP BY e.version_date, e.version_id, e.group_by, e.aircraft_number, vb.version_max_day
),
events_packed AS (
    SELECT
        version_date AS version_date,
        version_id AS version_id,
        group_by AS group_by,
        aircraft_number AS aircraft_number,
        arraySort(x -> x.1, groupArray((day_date, status_id))) AS day_status_pairs
    FROM events_daily
    GROUP BY version_date, version_id, group_by, aircraft_number
),
expanded_days AS (
    SELECT
        a.version_date AS version_date,
        a.version_id AS version_id,
        a.group_by AS group_by,
        a.aircraft_number AS aircraft_number,
        addDays(a.aircraft_min_day, n) AS day_date
    FROM aircraft_span a
    ARRAY JOIN range(dateDiff('day', a.aircraft_min_day, a.aircraft_max_day) + 1) AS n
),
daily_aircraft_status AS (
    SELECT
        d.version_date AS version_date,
        d.version_id AS version_id,
        d.group_by AS group_by,
        d.aircraft_number AS aircraft_number,
        d.day_date AS day_date,
        if(
            arrayLastIndex(x -> x <= d.day_date, arrayMap(p -> p.1, p.day_status_pairs)) = 0,
            0,
            arrayMap(p -> p.2, p.day_status_pairs)[arrayLastIndex(x -> x <= d.day_date, arrayMap(p -> p.1, p.day_status_pairs))]
        ) AS status_id
    FROM expanded_days d
    INNER JOIN events_packed p
      ON p.version_date = d.version_date
     AND p.version_id = d.version_id
     AND p.group_by = d.group_by
     AND p.aircraft_number = d.aircraft_number
),
daily_status_counts AS (
    SELECT
        version_date AS version_date,
        version_id AS version_id,
        group_by AS group_by,
        day_date AS day_date,
        status_id AS status_id,
        countDistinct(aircraft_number) AS status_count_ffill
    FROM daily_aircraft_status
    GROUP BY version_date, version_id, group_by, day_date, status_id
),
daily_grid AS (
    SELECT
        d.version_date AS version_date,
        d.version_id AS version_id,
        d.group_by AS group_by,
        d.day_date AS day_date,
        s.status_id AS status_id
    FROM (
        SELECT DISTINCT
            version_date AS version_date,
            version_id AS version_id,
            group_by AS group_by,
            day_date AS day_date
        FROM daily_aircraft_status
    ) d
    CROSS JOIN (
        SELECT arrayJoin([1, 2, 3, 4, 6, 7]) AS status_id
    ) s
)
SELECT
    g.version_date,
    g.version_id,
    g.group_by,
    toUInt8(g.status_id) AS status_id,
    g.day_date,
    -- day_d duplicates day_date so the BI mean-by-days metric can divide by
    -- countDistinct(day_d) without colliding with the time-grain alias of day_date
    -- (which Superset aliases as toStartOf<grain>(day_date) AS day_date).
    g.day_date AS day_d,
    toUInt16(ifNull(c.status_count_ffill, 0)) AS status_count_ffill,
    multiIf(g.group_by = 1, 'Ми-8', g.group_by = 2, 'Ми-17', toString(g.group_by)) AS group_by_label,
    formatDateTime(parseDateTimeBestEffort(toString(g.version_date)), '%%d-%%m-%%Y') AS version_date_ddmmyyyy
FROM daily_grid g
LEFT JOIN daily_status_counts c
  ON c.version_date = g.version_date
 AND c.version_id = g.version_id
 AND c.group_by = g.group_by
 AND c.day_date = g.day_date
 AND c.status_id = g.status_id
"""


INSERT_DEFICIT = f"""
INSERT INTO {DEFICIT_TABLE_NAME}
(
    version_date,
    version_id,
    day_date,
    group_by,
    target,
    ops_count,
    deficit
)
WITH daily_grid AS (
    SELECT DISTINCT
        version_date AS version_date,
        version_id AS version_id,
        group_by AS group_by,
        day_date AS day_date
    FROM {TABLE_NAME}
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND group_by IN (1, 2)
),
ops_counts AS (
    SELECT
        version_date AS version_date,
        version_id AS version_id,
        group_by AS group_by,
        day_date AS day_date,
        status_count_ffill AS ops_count
    FROM {TABLE_NAME}
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND group_by IN (1, 2)
      AND status_id = 2
),
program_targets AS (
    SELECT
        toUInt32(formatDateTime(version_date, '%%Y%%m%%d')) AS version_date_int,
        dates AS day_date,
        max(toUInt32(ops_counter_mi8)) AS target_mi8,
        max(toUInt32(ops_counter_mi17)) AS target_mi17
    FROM flight_program_ac
    WHERE version_date = toDate(parseDateTimeBestEffort(toString(%(version_date)s)))
    GROUP BY version_date, dates
),
targets AS (
    SELECT
        g.version_date AS version_date,
        g.version_id AS version_id,
        g.group_by AS group_by,
        g.day_date AS day_date,
        if(g.group_by = 1, p.target_mi8, p.target_mi17) AS target
    FROM daily_grid g
    -- Exact-date join is equivalent to forward-fill only for dense daily flight_program_ac;
    -- _check_deficit_target_coverage fails fast on sparse program data before this INSERT runs.
    INNER JOIN program_targets p
      ON p.version_date_int = g.version_date
     AND p.day_date = g.day_date
)
SELECT
    g.version_date AS version_date,
    g.version_id AS version_id,
    g.day_date AS day_date,
    g.group_by AS group_by,
    toUInt32(t.target) AS target,
    toUInt32(ifNull(o.ops_count, 0)) AS ops_count,
    toInt32(t.target) - toInt32(ifNull(o.ops_count, 0)) AS deficit
FROM daily_grid g
INNER JOIN targets t
  ON t.version_date = g.version_date
 AND t.version_id = g.version_id
 AND t.group_by = g.group_by
 AND t.day_date = g.day_date
LEFT JOIN ops_counts o
  ON o.version_date = g.version_date
 AND o.version_id = g.version_id
 AND o.group_by = g.group_by
 AND o.day_date = g.day_date
"""


DEFICIT_TARGET_COVERAGE_CHECK = f"""
WITH events_daily AS (
    SELECT
        version_date AS version_date,
        version_id AS version_id,
        group_by AS group_by,
        aircraft_number AS aircraft_number,
        day_date AS day_date,
        argMax(status_id, idx) AS status_id
    FROM {SOURCE_TABLE}
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND group_by IN (1, 2)
    GROUP BY version_date, version_id, group_by, aircraft_number, day_date
),
version_bounds AS (
    SELECT
        version_date AS version_date,
        version_id AS version_id,
        max(day_date) AS version_max_day
    FROM events_daily
    GROUP BY version_date, version_id
),
aircraft_span AS (
    SELECT
        e.version_date AS version_date,
        e.version_id AS version_id,
        e.group_by AS group_by,
        e.aircraft_number AS aircraft_number,
        min(e.day_date) AS aircraft_min_day,
        vb.version_max_day AS aircraft_max_day
    FROM events_daily e
    INNER JOIN version_bounds vb
      ON vb.version_date = e.version_date
     AND vb.version_id = e.version_id
    GROUP BY e.version_date, e.version_id, e.group_by, e.aircraft_number, vb.version_max_day
),
expanded_days AS (
    SELECT DISTINCT
        a.version_date AS version_date,
        a.version_id AS version_id,
        a.group_by AS group_by,
        addDays(a.aircraft_min_day, n) AS day_date
    FROM aircraft_span a
    ARRAY JOIN range(dateDiff('day', a.aircraft_min_day, a.aircraft_max_day) + 1) AS n
),
program_target_dates AS (
    SELECT
        dates AS day_date
    FROM flight_program_ac
    WHERE version_date = toDate(parseDateTimeBestEffort(toString(%(version_date)s)))
    GROUP BY dates
)
SELECT
    count() AS missing_grid_days,
    uniqExact(day_date) AS missing_dates,
    arraySlice(arraySort(groupUniqArray((group_by, day_date))), 1, 10) AS sample_missing_grid_days
FROM expanded_days
WHERE day_date NOT IN (SELECT day_date FROM program_target_dates)
"""


def _as_uint32(value: int, name: str) -> int:
    value_int = int(value)
    if value_int < 0 or value_int > 0xFFFFFFFF:
        raise ValueError(f"{name} must fit UInt32: {value}")
    return value_int


def _version_ids_csv(version_ids: list[int]) -> str:
    if not version_ids:
        raise ValueError("version_ids must not be empty")
    checked = [_as_uint32(version_id, "version_id") for version_id in version_ids]
    if len(set(checked)) != len(checked):
        raise ValueError(f"Duplicate version_id values are not allowed: {version_ids}")
    return ", ".join(str(version_id) for version_id in checked)


def _multi_version_query(query: str, version_ids: list[int]) -> str:
    return query.replace(
        "version_id = %(version_id)s",
        f"version_id IN ({_version_ids_csv(version_ids)})",
    )


def _count_partitions(client, table_name: str, version_date_int: int, version_ids: list[int]) -> int:
    return int(
        client.execute(
            f"""
            SELECT count()
            FROM {table_name}
            WHERE version_date = %(version_date)s
              AND version_id IN ({_version_ids_csv(version_ids)})
            """,
            {"version_date": version_date_int},
        )[0][0]
    )


def _count_partition(client, table_name: str, params: dict[str, int]) -> int:
    return _count_partitions(client, table_name, params["version_date"], [params["version_id"]])


def _format_missing_grid_days(sample_missing_grid_days) -> str:
    return ", ".join(
        f"group_by={int(group_by)} day_date={day_date}"
        for group_by, day_date in sample_missing_grid_days
    )


def _check_deficit_target_coverage(client, params: dict[str, int], version_ids: list[int]) -> None:
    missing_grid_days, missing_dates, sample_missing = client.execute(
        _multi_version_query(DEFICIT_TARGET_COVERAGE_CHECK, version_ids),
        params,
    )[0]
    if int(missing_grid_days) == 0:
        return

    sample_text = _format_missing_grid_days(sample_missing)
    raise RuntimeError(
        "flight_program_ac must be dense daily for sim_deficit_v9_daily exact-date targets: "
        f"version_date={params['version_date']}, version_ids={version_ids}, "
        f"missing_grid_days={int(missing_grid_days)}, missing_dates={int(missing_dates)}, "
        f"sample_missing=[{sample_text}]"
    )


def _clear_daily_partitions(client, version_date_int: int, version_ids: list[int]) -> None:
    if len(version_ids) == 1:
        version_id = version_ids[0]
        client.execute(f"ALTER TABLE {TABLE_NAME} DROP PARTITION tuple({version_date_int}, {version_id})")
        client.execute(f"ALTER TABLE {DEFICIT_TABLE_NAME} DROP PARTITION tuple({version_date_int}, {version_id})")
        return

    version_ids_csv = _version_ids_csv(version_ids)
    params = {"version_date": version_date_int}
    client.execute(
        f"""
        ALTER TABLE {TABLE_NAME} DELETE
        WHERE version_date = %(version_date)s
          AND version_id IN ({version_ids_csv})
        """,
        params,
        settings={"mutations_sync": 2},
    )
    client.execute(
        f"""
        ALTER TABLE {DEFICIT_TABLE_NAME} DELETE
        WHERE version_date = %(version_date)s
          AND version_id IN ({version_ids_csv})
        """,
        params,
        settings={"mutations_sync": 2},
    )


def materialize_daily_versions(client, version_date_int: int, version_ids: list[int]) -> tuple[int, int]:
    """Rebuild one or more version partitions of daily BI views."""
    version_date_int = _as_uint32(version_date_int, "version_date_int")
    version_ids = [_as_uint32(version_id, "version_id") for version_id in version_ids]
    params = {"version_date": version_date_int}

    client.execute(DDL_DAILY)
    client.execute(DDL_DEFICIT)
    _check_deficit_target_coverage(client, params, version_ids)
    _clear_daily_partitions(client, version_date_int, version_ids)
    client.execute(_multi_version_query(INSERT_DAILY, version_ids), params)
    client.execute(_multi_version_query(INSERT_DEFICIT, version_ids), params)
    return (
        _count_partitions(client, TABLE_NAME, version_date_int, version_ids),
        _count_partitions(client, DEFICIT_TABLE_NAME, version_date_int, version_ids),
    )


def materialize_daily(client, version_date_int: int, version_id: int) -> int:
    """Rebuild one version partition of daily BI views."""
    row_count, _deficit_row_count = materialize_daily_versions(client, version_date_int, [version_id])
    return row_count


def count_deficit_rows(client, version_date_int: int, version_id: int) -> int:
    params = {
        "version_date": _as_uint32(version_date_int, "version_date_int"),
        "version_id": _as_uint32(version_id, "version_id"),
    }
    return _count_partition(client, DEFICIT_TABLE_NAME, params)


def recreate_daily_table(client) -> None:
    """Recreate daily tables when their partition expression changes."""
    client.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    client.execute(f"DROP TABLE IF EXISTS {DEFICIT_TABLE_NAME}")
    client.execute(DDL_DAILY)
    client.execute(DDL_DEFICIT)


def _list_versions(client) -> list[tuple[int, int]]:
    return [
        (int(version_date), int(version_id))
        for version_date, version_id in client.execute(
            f"""
            SELECT version_date, version_id
            FROM {SOURCE_TABLE}
            WHERE group_by IN (1, 2)
            GROUP BY version_date, version_id
            ORDER BY version_date, version_id
            """
        )
    ]


def _target_version_ids(args: argparse.Namespace, parser: argparse.ArgumentParser) -> list[int]:
    if args.version_id is not None and args.version_ids is not None:
        parser.error("Pass only one of --version-id or --version-ids")
    if args.version_ids is not None:
        return [_as_uint32(version_id, "version_id") for version_id in args.version_ids]
    if args.version_id is not None:
        return [_as_uint32(args.version_id, "version_id")]
    parser.error("--version-id or --version-ids is required without --all")


def _chunks(values: list[int], chunk_size: int) -> list[list[int]]:
    if chunk_size <= 0:
        raise ValueError("--chunk-size must be > 0")
    return [values[i:i + chunk_size] for i in range(0, len(values), chunk_size)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Materialize sim_masterv2_v9_daily")
    parser.add_argument("--all", action="store_true", help="Materialize all present source versions")
    parser.add_argument("--version-date", type=int, help="Version date as YYYYMMDD")
    parser.add_argument("--version-id", type=int, help="Version id")
    parser.add_argument("--version-ids", type=int, nargs="+", help="Version ids")
    parser.add_argument("--chunk-size", type=int, default=15, help="Number of version ids per SQL pass")
    args = parser.parse_args()

    if args.all:
        client = get_client()
        versions_by_date: dict[int, list[int]] = {}
        for version_date_int, version_id in _list_versions(client):
            versions_by_date.setdefault(version_date_int, []).append(version_id)
        recreate_daily_table(client)
    else:
        if args.version_date is None:
            parser.error("--version-date is required without --all")
        client = get_client()
        versions_by_date = {args.version_date: _target_version_ids(args, parser)}

    total_rows = 0
    total_deficit_rows = 0
    started = time.perf_counter()
    for version_date_int, version_ids in versions_by_date.items():
        for version_chunk in _chunks(version_ids, args.chunk_size):
            version_started = time.perf_counter()
            row_count, deficit_row_count = materialize_daily_versions(
                client,
                version_date_int,
                version_chunk,
            )
            elapsed = time.perf_counter() - version_started
            total_rows += row_count
            total_deficit_rows += deficit_row_count
            print(
                "📊 sim_masterv2_v9_daily: "
                f"{row_count} строк (version_date={version_date_int}, "
                f"version_ids={version_chunk[0]}..{version_chunk[-1]}, count={len(version_chunk)}, {elapsed:.2f}с)"
            )
            print(
                "📊 sim_deficit_v9_daily: "
                f"{deficit_row_count} строк (version_date={version_date_int}, "
                f"version_ids={version_chunk[0]}..{version_chunk[-1]}, count={len(version_chunk)}, {elapsed:.2f}с)"
            )

    elapsed_total = time.perf_counter() - started
    print(f"✅ sim_masterv2_v9_daily: всего {total_rows} строк ({elapsed_total:.2f}с)")
    print(f"✅ sim_deficit_v9_daily: всего {total_deficit_rows} строк ({elapsed_total:.2f}с)")


if __name__ == "__main__":
    main()
