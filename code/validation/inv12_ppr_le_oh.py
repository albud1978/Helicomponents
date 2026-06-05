#!/usr/bin/env python3
"""
INV-12: ppr <= oh по всему жизненному циклу планеров.

Проверка не фильтрует status_id: перелёт по ремонтному ресурсу может
материализоваться на exit-day строке с терминальным статусом.
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
        description="INV-12: ppr <= oh по всему жизненному циклу планеров"
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
        default="sim_masterv2_v9",
        help="Таблица ClickHouse (по умолчанию: sim_masterv2_v9)",
    )
    args = parser.parse_args()
    table = validate_table_name(args.table)
    client = get_client()

    vd_filter = ""
    params = {"vid": args.version_id}
    if args.version_date is not None:
        vd_filter = " AND version_date = %(vdate)s"
        params["vdate"] = args.version_date

    count_query = f"""
    SELECT count()
    FROM {table}
    WHERE version_id = %(vid)s{vd_filter}
      AND ppr > oh
      AND group_by IN (1, 2)
    """
    violations = client.execute(count_query, params)[0][0]
    details = [f"violations={violations}"]

    if violations:
        sample_query = f"""
        SELECT
            aircraft_number,
            group_by,
            status_id,
            day_u16,
            ppr,
            oh,
            (ppr - oh) AS exceed,
            limiter,
            daily_today_u32
        FROM {table}
        WHERE version_id = %(vid)s{vd_filter}
          AND ppr > oh
          AND group_by IN (1, 2)
        ORDER BY exceed DESC
        LIMIT 10
        """
        rows = client.execute(sample_query, params)
        details.append(
            "top10 sample (acn, group_by, status, day, "
            "ppr, oh, exceed, limiter, dt):"
        )
        for row in rows:
            acn, group_by, status_id, day, ppr, oh, exceed, limiter, dt = row
            details.append(
                f"  acn={acn}, group_by={group_by}, status={status_id}, day={day}, "
                f"ppr={ppr}, oh={oh}, exceed={exceed}, limiter={limiter}, dt={dt}"
            )

    passed = violations == 0
    print_result("INV-12 ppr<=oh", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
