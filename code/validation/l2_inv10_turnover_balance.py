#!/usr/bin/env python3
"""
L2 INV-10: turnover balance по состояниям S={2,3,4,5,6}.
"""
import argparse
import re
import sys

from ch_client import get_client


TABLE_RE = re.compile(r"^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$")


def validate_table_name(table: str) -> str:
    if not TABLE_RE.match(table):
        raise SystemExit(f"Некорректное имя таблицы: {table}")
    return table


def ensure_columns(client, table: str, required) -> None:
    rows = client.execute(f"DESCRIBE TABLE {table}")
    existing = {row[0] for row in rows}
    missing = [col for col in required if col not in existing]
    if missing:
        missing_str = ", ".join(missing)
        raise SystemExit(f"Не найдены колонки в {table}: {missing_str}")


def print_result(name: str, passed: bool, details) -> None:
    status = "PASS" if passed else "FAIL"
    print("=" * 80)
    print(f"{name}: {status}")
    for line in details:
        print(line)
    print("=" * 80)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="L2 INV-10: turnover balance"
    )
    parser.add_argument("--planner-version-date", type=int, default=None)
    parser.add_argument("--units-version-date-int", required=True, type=int)
    parser.add_argument("--version-id", required=True, type=int)
    parser.add_argument("--table-main", default="sim_masterv2_v9")
    parser.add_argument("--table-units", default="sim_units_v2")
    args = parser.parse_args()

    table_units = validate_table_name(args.table_units)
    client = get_client()
    ensure_columns(
        client,
        table_units,
        [
            "version_date",
            "version_id",
            "day_u16",
            "psn",
            "group_by",
            "state",
            "pre_state_id",
        ],
    )

    params = {"uvd": args.units_version_date_int, "vid": args.version_id}
    minmax_query = f"""
    SELECT min(day_u16), max(day_u16)
    FROM {table_units}
    WHERE version_date = %(uvd)s
      AND version_id = %(vid)s
      AND group_by IN (3, 4)
    """
    min_day, max_day = client.execute(minmax_query, params)[0]

    if min_day is None or max_day is None:
        details = ["no_data=1 (min_day/max_day is NULL)"]
        print_result("L2 INV-10 turnover balance", True, details)
        return 0

    params = {**params, "min_day": int(min_day), "max_day": int(max_day)}
    query = f"""
    WITH base AS (
        SELECT
            day_u16,
            state,
            pre_state_id
        FROM {table_units}
        WHERE version_date = %(uvd)s
          AND version_id = %(vid)s
          AND group_by IN (3, 4)
    ),
    initial AS (
        SELECT pre_state_id AS state, count() AS initial_count
        FROM base
        WHERE day_u16 = %(min_day)s
          AND pre_state_id IN (2, 3, 4, 5, 6)
        GROUP BY pre_state_id
    ),
    final AS (
        SELECT state, count() AS final_count
        FROM base
        WHERE day_u16 = %(max_day)s
          AND state IN (2, 3, 4, 5, 6)
        GROUP BY state
    ),
    entries AS (
        SELECT state, count() AS entries
        FROM base
        WHERE state IN (2, 3, 4, 5, 6)
          AND pre_state_id > 0
          AND pre_state_id != state
        GROUP BY state
    ),
    spawn_entries AS (
        SELECT state, count() AS spawn
        FROM base
        WHERE state IN (2, 3, 4, 5, 6)
          AND pre_state_id = 0
        GROUP BY state
    ),
    exits AS (
        SELECT pre_state_id AS state, count() AS exits
        FROM base
        WHERE pre_state_id IN (2, 3, 4, 5, 6)
          AND state != pre_state_id
        GROUP BY pre_state_id
    )
    SELECT
        s.state,
        ifNull(i.initial_count, 0) AS initial,
        ifNull(en.entries, 0) AS entries,
        ifNull(sp.spawn, 0) AS spawn,
        ifNull(ex.exits, 0) AS exits,
        ifNull(f.final_count, 0) AS final,
        (ifNull(i.initial_count, 0) + ifNull(en.entries, 0) + ifNull(sp.spawn, 0)) AS left_total,
        (ifNull(ex.exits, 0) + ifNull(f.final_count, 0)) AS right_total,
        (ifNull(i.initial_count, 0) + ifNull(en.entries, 0) + ifNull(sp.spawn, 0)
            - ifNull(ex.exits, 0) - ifNull(f.final_count, 0)) AS diff
    FROM (SELECT arrayJoin([2, 3, 4, 5, 6]) AS state) s
    LEFT JOIN initial i USING state
    LEFT JOIN final f USING state
    LEFT JOIN entries en USING state
    LEFT JOIN exits ex USING state
    LEFT JOIN spawn_entries sp USING state
    ORDER BY state
    """
    rows = client.execute(query, params)

    details = [
        f"min_day={min_day}, max_day={max_day}",
        "state | initial | entries | spawn | exits | final | left | right | diff",
    ]
    passed = True
    for state, initial, entries, spawn, exits, final, left_total, right_total, diff in rows:
        details.append(
            f"{state} | {initial} | {entries} | {spawn} | {exits} | "
            f"{final} | {left_total} | {right_total} | {diff}"
        )
        if diff != 0:
            passed = False

    print_result("L2 INV-10 turnover balance", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
