#!/usr/bin/env python3
"""
L2 INV-0a: engine ops требуют planner ops (status_id=2).
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
        description="L2 INV-0a: engine ops требуют planner ops"
    )
    parser.add_argument("--planner-version-date", required=True, type=int)
    parser.add_argument("--units-version-date-int", required=True, type=int)
    parser.add_argument("--version-id", required=True, type=int)
    parser.add_argument("--table-main", default="sim_masterv2_v9")
    parser.add_argument("--table-units", default="sim_units_v2")
    args = parser.parse_args()

    table_main = validate_table_name(args.table_main)
    table_units = validate_table_name(args.table_units)
    client = get_client()
    ensure_columns(
        client,
        table_units,
        ["version_date", "version_id", "day_u16", "group_by", "state", "aircraft_number"],
    )
    ensure_columns(
        client,
        table_main,
        ["version_date", "version_id", "day_u16", "aircraft_number", "status_id"],
    )

    params = {
        "pvd": args.planner_version_date,
        "uvd": args.units_version_date_int,
        "vid": args.version_id,
    }
    query = f"""
    WITH engine_ops AS (
        SELECT day_u16, aircraft_number
        FROM {table_units}
        WHERE version_date = %(uvd)s
          AND version_id = %(vid)s
          AND group_by IN (3, 4)
          AND state = 2
          AND day_u16 > 0
          AND aircraft_number > 0
    )
    SELECT
        count() AS engine_ops_rows,
        countIf(p.status_id = 2) AS planner_ops_ok,
        countIf(isNull(p.status_id) OR p.status_id != 2) AS violations
    FROM engine_ops e
    ANY LEFT JOIN {table_main} p
        ON p.version_date = %(pvd)s
       AND p.version_id = %(vid)s
       AND p.day_u16 = e.day_u16
       AND p.aircraft_number = e.aircraft_number
    """
    engine_ops_rows, planner_ops_ok, violations = client.execute(query, params)[0]
    details = [
        f"engine_ops_rows={engine_ops_rows}",
        f"planner_ops_ok={planner_ops_ok}",
        f"violations={violations}",
    ]

    if violations:
        sample_query = f"""
        WITH engine_ops AS (
            SELECT day_u16, aircraft_number
            FROM {table_units}
            WHERE version_date = %(uvd)s
              AND version_id = %(vid)s
              AND group_by IN (3, 4)
              AND state = 2
              AND day_u16 > 0
              AND aircraft_number > 0
        )
        SELECT
            e.aircraft_number,
            e.day_u16,
            p.status_id
        FROM engine_ops e
        ANY LEFT JOIN {table_main} p
            ON p.version_date = %(pvd)s
           AND p.version_id = %(vid)s
           AND p.day_u16 = e.day_u16
           AND p.aircraft_number = e.aircraft_number
        WHERE isNull(p.status_id) OR p.status_id != 2
        ORDER BY e.day_u16
        LIMIT 5
        """
        rows = client.execute(sample_query, params)
        details.append("top5 sample (acn, day, planner_status):")
        for acn, day, st in rows:
            details.append(f"  acn={acn}, day={day}, planner_status={st}")

    passed = violations == 0
    print_result("L2 INV-0a engine ops require planner ops", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
