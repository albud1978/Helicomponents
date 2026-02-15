#!/usr/bin/env python3
"""
INV-9: limiter=0 в ops => агент выходит из ops на этом же шаге.

Проверяет: если limiter=0 и status_id=2, то на СЛЕДУЮЩЕМ шаге
агент НЕ должен быть в status_id=2.
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
    parser = argparse.ArgumentParser(description="INV-9: limiter=0 => exit ops")
    parser.add_argument("--version-id", required=True, type=int, help="version_id")
    parser.add_argument(
        "--version-date", type=int, default=None,
        help="version_date (YYYYMMDD) для фильтрации",
    )
    parser.add_argument(
        "--table", default="sim_masterv2_v9",
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
    FROM (
        SELECT
            aircraft_number, day_u16, status_id, limiter, sne, ll, ppr, oh,
            leadInFrame(status_id) OVER w AS next_st,
            leadInFrame(day_u16, 1, 0) OVER w AS next_day
        FROM {table}
        WHERE version_id = %(vid)s{vd_filter} AND group_by IN (1, 2)
        WINDOW w AS (PARTITION BY aircraft_number, group_by ORDER BY day_u16)
    )
    WHERE status_id = 2
      AND limiter = 0
      AND next_st = 2
      AND next_day > 0
    """
    violations = client.execute(count_query, params)[0][0]
    details = [f"violations={violations}"]

    if violations:
        sample_query = f"""
        SELECT aircraft_number, day_u16, sne, ll, ppr, oh, next_st
        FROM (
            SELECT
                aircraft_number, day_u16, status_id, limiter, sne, ll, ppr, oh,
                leadInFrame(status_id) OVER w AS next_st,
                leadInFrame(day_u16, 1, 0) OVER w AS next_day
            FROM {table}
            WHERE version_id = %(vid)s{vd_filter} AND group_by IN (1, 2)
            WINDOW w AS (PARTITION BY aircraft_number, group_by ORDER BY day_u16)
        )
        WHERE status_id = 2
          AND limiter = 0
          AND next_st = 2
          AND next_day > 0
        ORDER BY day_u16
        LIMIT 5
        """
        rows = client.execute(sample_query, params)
        details.append("top5 sample (acn, day, sne, ll, ppr, oh, next_status):")
        for acn, day, sne, ll, ppr, oh, next_st in rows:
            details.append(
                f"  acn={acn}, day={day}, sne={sne}, ll={ll}, "
                f"ppr={ppr}, oh={oh}, next_status={next_st}"
            )

    passed = violations == 0
    print_result("INV-9 limiter exit ops", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
