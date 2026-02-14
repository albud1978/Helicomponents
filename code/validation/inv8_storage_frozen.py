#!/usr/bin/env python3
"""
INV-8: sne/ppr frozen в storage.
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
    parser = argparse.ArgumentParser(description="INV-8: storage frozen")
    parser.add_argument("--version-id", required=True, type=int, help="version_id")
    parser.add_argument(
        "--table",
        default="sim_masterv2_v9",
        help="Таблица ClickHouse (по умолчанию: sim_masterv2_v9)",
    )
    args = parser.parse_args()
    table = validate_table_name(args.table)
    client = get_client()

    query = f"""
    WITH numbered AS (
        SELECT
            aircraft_number,
            day_u16,
            status_id,
            sne,
            ppr,
            lagInFrame(sne) OVER (PARTITION BY aircraft_number ORDER BY day_u16) AS prev_sne,
            lagInFrame(ppr) OVER (PARTITION BY aircraft_number ORDER BY day_u16) AS prev_ppr,
            lagInFrame(status_id) OVER (PARTITION BY aircraft_number ORDER BY day_u16) AS prev_status
        FROM {table}
        WHERE version_id = %(vid)s AND group_by IN (1, 2)
    )
    SELECT count() AS violations
    FROM numbered
    WHERE prev_status = 6 AND status_id = 6 AND (sne != prev_sne OR ppr != prev_ppr)
    """
    violations = client.execute(query, {"vid": args.version_id})[0][0]
    passed = violations == 0
    details = [f"violations={violations}"]
    print_result("INV-8 storage frozen", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
