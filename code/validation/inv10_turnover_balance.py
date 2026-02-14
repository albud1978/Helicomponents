#!/usr/bin/env python3
"""
INV-10: баланс оборота по статусам.
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


def status_name(status_id: int) -> str:
    return {
        1: "inactive",
        2: "ops",
        3: "svc",
        4: "repair",
        5: "reserve",
        6: "storage",
        7: "unsvc",
    }.get(status_id, f"status_{status_id}")


def main() -> int:
    parser = argparse.ArgumentParser(description="INV-10: баланс оборота по статусам")
    parser.add_argument("--version-id", required=True, type=int, help="version_id")
    parser.add_argument(
        "--table",
        default="sim_masterv2_v9",
        help="Таблица ClickHouse (по умолчанию: sim_masterv2_v9)",
    )
    parser.add_argument(
        "--tolerance",
        type=int,
        default=0,
        help="Допуск на расхождение (по умолчанию: 0)",
    )
    args = parser.parse_args()
    table = validate_table_name(args.table)
    client = get_client()

    query = f"""
    WITH bounds AS (
        SELECT min(day_u16) AS min_day, max(day_u16) AS max_day
        FROM {table}
        WHERE version_id = %(vid)s AND group_by IN (1, 2)
    ),
    lagged AS (
        SELECT
            day_u16,
            status_id,
            pre_status_id,
            leadInFrame(status_id, 1, status_id)
                OVER (PARTITION BY aircraft_number ORDER BY day_u16) AS next_status,
            bounds.min_day AS min_day,
            bounds.max_day AS max_day
        FROM {table}
        CROSS JOIN bounds
        WHERE version_id = %(vid)s AND group_by IN (1, 2)
    )
    SELECT
        status_id,
        countIf(pre_status_id != status_id) AS entries,
        countIf(next_status != status_id) AS exits,
        countIf(day_u16 = min_day) AS count_start,
        countIf(day_u16 = max_day) AS count_end
    FROM lagged
    GROUP BY status_id
    ORDER BY status_id
    """
    rows = client.execute(query, {"vid": args.version_id})

    violations = []
    for status_id, entries, exits, count_start, count_end in rows:
        balance = int(entries) - int(exits)
        delta = int(count_end) - int(count_start)
        diff = balance - delta
        if abs(diff) > args.tolerance:
            violations.append(
                (int(status_id), int(entries), int(exits), int(count_start), int(count_end), diff)
            )

    details = [f"tolerance={args.tolerance}", f"violations={len(violations)}"]
    if violations:
        sample = [
            {
                "status": status_name(v[0]),
                "entries": v[1],
                "exits": v[2],
                "count_start": v[3],
                "count_end": v[4],
                "diff": v[5],
            }
            for v in violations[:10]
        ]
        details.append("sample_violations=" + str(sample))

    passed = len(violations) == 0
    print_result("INV-10 turnover balance", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
