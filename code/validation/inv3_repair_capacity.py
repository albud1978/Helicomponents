#!/usr/bin/env python3
"""
INV-3: одновременно в ремонте <= repair_quota.
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
        description="INV-3: одновременно в ремонте <= repair_quota"
    )
    parser.add_argument("--version-id", required=True, type=int, help="version_id")
    parser.add_argument(
        "--version-date",
        type=int,
        default=None,
        help="version_date (YYYYMMDD) для фильтрации",
    )
    parser.add_argument(
        "--table",
        default="sim_repairline_v9",
        help="Таблица ClickHouse (по умолчанию: sim_repairline_v9)",
    )
    parser.add_argument(
        "--repair-quota",
        type=int,
        default=18,
        help="Лимит ремонтов (по умолчанию: 18)",
    )
    args = parser.parse_args()
    table = validate_table_name(args.table)
    client = get_client()

    vd_filter = ""
    params = {"vid": args.version_id, "quota": args.repair_quota}
    if args.version_date is not None:
        vd_filter = " AND version_date = %(vdate)s"
        params["vdate"] = args.version_date

    max_query = f"""
    SELECT max(n)
    FROM (
        SELECT day_u16, countIf(aircraft_number != 0) AS n
        FROM {table}
        WHERE version_id = %(vid)s{vd_filter}
        GROUP BY day_u16
    )
    """
    max_concurrent = client.execute(max_query, params)[0][0]
    if max_concurrent is None:
        max_concurrent = 0

    violations_query = f"""
    SELECT count()
    FROM (
        SELECT day_u16, countIf(aircraft_number != 0) AS n
        FROM {table}
        WHERE version_id = %(vid)s{vd_filter}
        GROUP BY day_u16
        HAVING n > %(quota)s
    )
    """
    violations = client.execute(violations_query, params)[0][0]
    details = [
        f"repair_quota={args.repair_quota}",
        f"max_concurrent_repair={max_concurrent}",
        f"violations={violations}",
    ]

    if violations:
        sample_query = f"""
        SELECT day_u16, n
        FROM (
            SELECT day_u16, countIf(aircraft_number != 0) AS n
            FROM {table}
            WHERE version_id = %(vid)s{vd_filter}
            GROUP BY day_u16
            HAVING n > %(quota)s
        )
        ORDER BY n DESC, day_u16
        LIMIT 5
        """
        rows = client.execute(sample_query, params)
        details.append("top5 days (day, n):")
        for day_u16, n in rows:
            details.append(f"  day={day_u16}, n={n}")

    passed = violations == 0
    print_result("INV-3 repair capacity", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
