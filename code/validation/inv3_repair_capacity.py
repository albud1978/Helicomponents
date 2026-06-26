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


def run(
    client,
    version_id: int,
    version_date=None,
    table: str = "sim_repairline_v9",
    repair_quota: int = 18,
) -> bool:
    table = validate_table_name(table)

    vd_filter = ""
    params = {"vid": version_id, "quota": repair_quota}
    if version_date is not None:
        vd_filter = " AND version_date = %(vdate)s"
        params["vdate"] = version_date

    # Занятость линии = есть назначенный борт; free_days теперь окно доступности, не прогресс ремонта.
    busy_expr = "aircraft_number != 0"

    max_query = f"""
    SELECT max(n)
    FROM (
        SELECT version_date, day_u16, countIf({busy_expr}) AS n
        FROM {table}
        WHERE version_id = %(vid)s{vd_filter}
        GROUP BY version_date, day_u16
    )
    """
    max_concurrent = client.execute(max_query, params)[0][0]
    if max_concurrent is None:
        max_concurrent = 0

    violations_query = f"""
    SELECT count()
    FROM (
        SELECT version_date, day_u16, countIf({busy_expr}) AS n
        FROM {table}
        WHERE version_id = %(vid)s{vd_filter}
        GROUP BY version_date, day_u16
        HAVING n > %(quota)s
    )
    """
    violations = client.execute(violations_query, params)[0][0]
    details = [
        f"repair_quota={repair_quota}",
        f"max_concurrent_repair={max_concurrent}",
        f"violations={violations}",
    ]

    if violations:
        sample_query = f"""
        SELECT version_date, day_u16, n
        FROM (
            SELECT version_date, day_u16, countIf({busy_expr}) AS n
            FROM {table}
            WHERE version_id = %(vid)s{vd_filter}
            GROUP BY version_date, day_u16
            HAVING n > %(quota)s
        )
        ORDER BY n DESC, version_date, day_u16
        LIMIT 5
        """
        rows = client.execute(sample_query, params)
        details.append("top5 days (version_date, day, n):")
        for version_date, day_u16, n in rows:
            details.append(f"  version_date={version_date}, day={day_u16}, n={n}")

    passed = violations == 0
    print_result("INV-3 repair capacity", passed, details)
    return passed


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
    client = get_client()
    passed = run(
        client,
        args.version_id,
        args.version_date,
        args.table,
        args.repair_quota,
    )
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
