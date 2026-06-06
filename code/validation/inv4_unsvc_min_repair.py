#!/usr/bin/env python3
"""
INV-4: возврат из unsvc в ops не раньше repair_time.

DEPRECATED: вакуумный (scope=0), покрывается INV-4 inv4_unsvc_repair_time.py.
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
        description="INV-4: возврат из unsvc в ops не раньше repair_time"
    )
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
      AND status_id = 2
      AND pre_status_id = 7
      AND repair_days > 0
      AND group_by IN (1, 2)
    """
    violations = client.execute(query, {"vid": args.version_id})[0][0]
    passed = violations == 0
    details = [f"violations={violations}"]
    print_result("INV-4 unsvc->ops repair_days guard", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
