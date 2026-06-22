#!/usr/bin/env python3
"""
INV-6: dt > 0 только в operations.
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


def run(client, version_id: int, version_date=None, table: str = "sim_masterv2_v9") -> bool:
    table = validate_table_name(table)

    vd_filter = ""
    params = {"vid": version_id}
    if version_date is not None:
        vd_filter = " AND version_date = %(vdate)s"
        params["vdate"] = version_date

    count_query = f"""
    SELECT count()
    FROM {table}
    WHERE version_id = %(vid)s{vd_filter}
      AND status_id != 2
      AND daily_today_u32 > 0
      AND group_by IN (1, 2)
    """
    violations = client.execute(count_query, params)[0][0]
    details = [f"violations={violations}"]

    if violations:
        sample_query = f"""
        SELECT
            aircraft_number,
            day_u16,
            status_id,
            pre_status_id,
            daily_today_u32
        FROM {table}
        WHERE version_id = %(vid)s{vd_filter}
          AND status_id != 2
          AND daily_today_u32 > 0
          AND group_by IN (1, 2)
        ORDER BY daily_today_u32 DESC, day_u16
        LIMIT 10
        """
        rows = client.execute(sample_query, params)
        details.append("top10 sample (acn, day, status, pre_status, dt):")
        for row in rows:
            acn, day, status_id, pre_status_id, dt = row
            details.append(
                f"  acn={acn}, day={day}, status_id={status_id}, "
                f"pre_status_id={pre_status_id}, dt={dt}"
            )

    passed = violations == 0
    print_result("INV-6 dt only ops", passed, details)
    return passed


def main() -> int:
    parser = argparse.ArgumentParser(description="INV-6: dt > 0 только в ops")
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
    client = get_client()
    passed = run(client, args.version_id, args.version_date, args.table)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
