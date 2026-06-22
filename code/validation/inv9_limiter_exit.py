#!/usr/bin/env python3
"""
INV-9: look-ahead safety для ops.

Проверяет: агент не должен оставаться в operations на следующем шаге,
если следующий день строго превысил бы СНЭ или ППР ресурс.
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
            idx, aircraft_number, group_by, version_date, day_u16, status_id,
            sne, ll, ppr, oh, daily_next_u32,
            leadInFrame(status_id) OVER w AS next_st
        FROM {table}
        WHERE version_id = %(vid)s{vd_filter} AND group_by IN (1, 2)
        WINDOW w AS (PARTITION BY idx, group_by, version_date ORDER BY day_u16)
    )
    WHERE status_id = 2
      AND next_st = 2
      AND (sne + daily_next_u32 > ll OR ppr + daily_next_u32 > oh)
    """
    violations = client.execute(count_query, params)[0][0]
    details = [f"violations={violations}"]

    if violations:
        sample_query = f"""
        SELECT
            aircraft_number, group_by, version_date, day_u16,
            sne, daily_next_u32, ll,
            ppr, daily_next_u32, oh,
            next_st
        FROM (
            SELECT
                idx, aircraft_number, group_by, version_date, day_u16, status_id,
                sne, ll, ppr, oh, daily_next_u32,
                leadInFrame(status_id) OVER w AS next_st
            FROM {table}
            WHERE version_id = %(vid)s{vd_filter} AND group_by IN (1, 2)
            WINDOW w AS (PARTITION BY idx, group_by, version_date ORDER BY day_u16)
        )
        WHERE status_id = 2
          AND next_st = 2
          AND (sne + daily_next_u32 > ll OR ppr + daily_next_u32 > oh)
        ORDER BY day_u16
        LIMIT 5
        """
        rows = client.execute(sample_query, params)
        details.append(
            "top5 sample (acn, gb, vd, day, sne, dt_next, ll, ppr, dt_next, oh, next_status):"
        )
        for acn, gb, vd, day, sne, dt_sne, ll, ppr, dt_ppr, oh, next_st in rows:
            details.append(
                f"  acn={acn}, group_by={gb}, version_date={vd}, day={day}, "
                f"sne={sne}+{dt_sne}>{ll}, ppr={ppr}+{dt_ppr}>{oh}, "
                f"next_status={next_st}"
            )

    passed = violations == 0
    print_result("INV-9 ops look-ahead safety", passed, details)
    return passed


def main() -> int:
    parser = argparse.ArgumentParser(description="INV-9: ops look-ahead safety")
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
