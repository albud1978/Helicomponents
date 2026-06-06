#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (P1 cleanup): устарел/дубликат, SSoT-замена: temp4_no_infinite_repair.py
"""
TEMP-4: нет бесконечного ремонта.
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
    parser = argparse.ArgumentParser(description="TEMP-4: нет бесконечного ремонта")
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
    SELECT count() AS violations
    FROM {table}
    WHERE version_id = %(vid)s
      AND day_u16 = (SELECT max(day_u16) FROM {table} WHERE version_id = %(vid)s)
      AND status_id = 4
      AND repair_days > repair_time
      AND group_by IN (1, 2)
    """
    violations = client.execute(query, {"vid": args.version_id})[0][0]
    passed = violations == 0
    details = [f"violations={violations}"]
    print_result("TEMP-4 repair liveness", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
