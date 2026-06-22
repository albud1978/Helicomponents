#!/usr/bin/env python3
"""
INV-8: sne/ppr frozen в storage (status_id=6).

Для агентов, находящихся в storage на двух последовательных шагах,
sne и ppr не должны меняться.
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
    FROM (
        SELECT
            aircraft_number, version_date, day_u16, status_id, sne, ppr,
            lagInFrame(sne) OVER w AS prev_sne,
            lagInFrame(ppr) OVER w AS prev_ppr,
            lagInFrame(status_id) OVER w AS prev_st,
            lagInFrame(day_u16, 1, 0) OVER w AS prev_day
        FROM {table}
        WHERE version_id = %(vid)s{vd_filter} AND group_by IN (1, 2)
        WINDOW w AS (PARTITION BY aircraft_number, group_by, version_date ORDER BY day_u16)
    )
    WHERE status_id = 6
      AND prev_st = 6
      AND prev_day > 0
      AND (sne != prev_sne OR ppr != prev_ppr)
    """
    violations = client.execute(count_query, params)[0][0]
    details = [f"violations={violations}"]

    if violations:
        sample_query = f"""
        SELECT aircraft_number, day_u16, sne, prev_sne, ppr, prev_ppr
        FROM (
            SELECT
                aircraft_number, version_date, day_u16, status_id, sne, ppr,
                lagInFrame(sne) OVER w AS prev_sne,
                lagInFrame(ppr) OVER w AS prev_ppr,
                lagInFrame(status_id) OVER w AS prev_st,
                lagInFrame(day_u16, 1, 0) OVER w AS prev_day
            FROM {table}
            WHERE version_id = %(vid)s{vd_filter} AND group_by IN (1, 2)
            WINDOW w AS (PARTITION BY aircraft_number, group_by, version_date ORDER BY day_u16)
        )
        WHERE status_id = 6
          AND prev_st = 6
          AND prev_day > 0
          AND (sne != prev_sne OR ppr != prev_ppr)
        ORDER BY day_u16
        LIMIT 5
        """
        rows = client.execute(sample_query, params)
        details.append("top5 sample (acn, day, sne/prev_sne, ppr/prev_ppr):")
        for acn, day, sne, prev_sne, ppr, prev_ppr in rows:
            details.append(
                f"  acn={acn}, day={day}, sne={sne}/{prev_sne}, ppr={ppr}/{prev_ppr}"
            )

    passed = violations == 0
    print_result("INV-8 storage frozen", passed, details)
    return passed


def main() -> int:
    parser = argparse.ArgumentParser(description="INV-8: storage frozen")
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
    client = get_client()
    passed = run(client, args.version_id, args.version_date, args.table)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
