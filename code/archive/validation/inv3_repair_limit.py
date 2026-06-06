#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (P1 cleanup): устарел/дубликат, SSoT-замена: inv3_repair_capacity.py
"""
INV-3: одновременно в ремонте <= repair_number.
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
        description="INV-3: одновременно в ремонте <= repair_number"
    )
    parser.add_argument("--version-id", required=True, type=int, help="version_id")
    parser.add_argument(
        "--table",
        default="sim_masterv2_v9",
        help="Таблица ClickHouse (по умолчанию: sim_masterv2_v9)",
    )
    parser.add_argument(
        "--repair-number",
        type=int,
        default=20,
        help="Лимит ремонтов (по умолчанию: 20)",
    )
    args = parser.parse_args()
    table = validate_table_name(args.table)
    client = get_client()

    query = f"""
    SELECT day_u16, countIf(status_id = 4) AS n_repair
    FROM {table}
    WHERE version_id = %(vid)s AND group_by IN (1, 2)
    GROUP BY day_u16
    HAVING n_repair > %(limit)s
    ORDER BY day_u16
    """
    rows = client.execute(query, {"vid": args.version_id, "limit": args.repair_number})
    violations = len(rows)
    sample = rows[:10]
    passed = violations == 0
    details = [
        f"repair_number={args.repair_number}",
        f"violations={violations}",
    ]
    if sample:
        details.append(f"sample_days={sample}")
    print_result("INV-3 repair capacity", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
