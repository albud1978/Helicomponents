#!/usr/bin/env python3
"""
INV-5: баланс наработок (Σdt = Δsne).
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
    parser = argparse.ArgumentParser(description="INV-5: баланс Σdt = Δsne")
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
    WITH bounds AS (
        SELECT min(day_u16) AS min_day, max(day_u16) AS max_day
        FROM {table}
        WHERE version_id = %(vid)s AND group_by IN (1, 2)
    ),
    agent_dt AS (
        SELECT aircraft_number, sum(daily_today_u32) AS total_dt
        FROM {table}
        WHERE version_id = %(vid)s AND status_id = 2 AND group_by IN (1, 2)
        GROUP BY aircraft_number
    ),
    agent_sne AS (
        SELECT
            aircraft_number,
            maxIf(sne, day_u16 = (SELECT max_day FROM bounds)) AS sne_end,
            minIf(sne, day_u16 = (SELECT min_day FROM bounds)) AS sne_start
        FROM {table}
        WHERE version_id = %(vid)s AND group_by IN (1, 2)
        GROUP BY aircraft_number
    )
    SELECT sum(total_dt) AS sum_dt, sum(sne_end - sne_start) AS sum_delta_sne
    FROM agent_dt d
    INNER JOIN agent_sne s ON d.aircraft_number = s.aircraft_number
    """
    row = client.execute(query, {"vid": args.version_id})[0]
    sum_dt, sum_delta_sne = row[0], row[1]

    if sum_dt is None or sum_delta_sne is None:
        details = [f"sum_dt={sum_dt}", f"sum_delta_sne={sum_delta_sne}"]
        print_result("INV-5 sne balance", False, details)
        return 1

    diff = int(sum_delta_sne) - int(sum_dt)
    passed = diff == 0
    details = [
        f"sum_dt={int(sum_dt)}",
        f"sum_delta_sne={int(sum_delta_sne)}",
        f"diff={diff}",
    ]
    print_result("INV-5 sne balance", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
