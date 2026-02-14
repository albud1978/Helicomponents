#!/usr/bin/env python3
"""
TEMP-1: длительность ремонта (упрощённая).
"""
import argparse
import re
import sys

from ch_client import get_client


def validate_table_name(table: str) -> str:
    if not re.match(r"^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$", table):
        raise SystemExit(f"Некорректное имя таблицы: {table}")
    return table


def print_result(name: str, passed: bool, details) -> None:
    status = "PASS" if passed else "FAIL"
    print("=" * 80)
    print(f"{name}: {status}")
    for line in details:
        print(line)
    print("=" * 80)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="TEMP-1: длительность ремонта (упрощённая)"
    )
    parser.add_argument("--version-id", required=True, type=int, help="version_id")
    parser.add_argument(
        "--table",
        default="sim_masterv2_v9",
        help="Таблица ClickHouse (по умолчанию: sim_masterv2_v9)",
    )
    parser.add_argument(
        "--tolerance-days",
        type=int,
        default=30,
        help="Допуск по длительности ремонта (по умолчанию: 30)",
    )
    args = parser.parse_args()
    table = validate_table_name(args.table)
    client = get_client()

    count_query = f"""
    WITH repair_spans AS (
        SELECT
            aircraft_number,
            min(day_u16) AS first_day,
            max(day_u16) AS last_day,
            any(repair_time) AS rt
        FROM {table}
        WHERE version_id = %(vid)s AND status_id = 4 AND group_by IN (1, 2)
        GROUP BY aircraft_number
    )
    SELECT count() AS violations
    FROM repair_spans
    WHERE (last_day - first_day) > rt + %(tol)s
    """
    violations = client.execute(
        count_query, {"vid": args.version_id, "tol": args.tolerance_days}
    )[0][0]

    sample_query = f"""
    WITH repair_spans AS (
        SELECT
            aircraft_number,
            min(day_u16) AS first_day,
            max(day_u16) AS last_day,
            any(repair_time) AS rt
        FROM {table}
        WHERE version_id = %(vid)s AND status_id = 4 AND group_by IN (1, 2)
        GROUP BY aircraft_number
    )
    SELECT aircraft_number, first_day, last_day, rt, (last_day - first_day) AS span
    FROM repair_spans
    WHERE span > rt + %(tol)s
    ORDER BY span DESC
    LIMIT 10
    """
    sample = client.execute(
        sample_query, {"vid": args.version_id, "tol": args.tolerance_days}
    )

    passed = violations == 0
    details = [
        f"tolerance_days={args.tolerance_days}",
        f"violations={violations}",
    ]
    if sample:
        details.append(f"sample={sample}")
    print_result("TEMP-1 repair duration", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
