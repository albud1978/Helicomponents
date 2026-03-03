#!/usr/bin/env python3
"""
L2 INV scope: только engines group_by 3/4 и допустимые состояния.
"""
import argparse
import re
import sys

from ch_client import get_client


TABLE_RE = re.compile(r"^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$")


def validate_table_name(table: str) -> str:
    if not TABLE_RE.match(table):
        raise SystemExit(f"Некорректное имя таблицы: {table}")
    return table


def ensure_columns(client, table: str, required) -> None:
    rows = client.execute(f"DESCRIBE TABLE {table}")
    existing = {row[0] for row in rows}
    missing = [col for col in required if col not in existing]
    if missing:
        missing_str = ", ".join(missing)
        raise SystemExit(f"Не найдены колонки в {table}: {missing_str}")


def print_result(name: str, passed: bool, details) -> None:
    status = "PASS" if passed else "FAIL"
    print("=" * 80)
    print(f"{name}: {status}")
    for line in details:
        print(line)
    print("=" * 80)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="L2 INV scope: только group_by=3/4 и допустимые состояния"
    )
    parser.add_argument("--planner-version-date", type=int, default=None)
    parser.add_argument("--units-version-date-int", required=True, type=int)
    parser.add_argument("--version-id", required=True, type=int)
    parser.add_argument("--table-main", default="sim_masterv2_v9")
    parser.add_argument("--table-units", default="sim_units_v2")
    args = parser.parse_args()

    table_units = validate_table_name(args.table_units)
    client = get_client()
    ensure_columns(client, table_units, ["version_date", "version_id", "group_by", "state"])

    params = {"uvd": args.units_version_date_int, "vid": args.version_id}
    query = f"""
    SELECT
        countIf(group_by NOT IN (3, 4)) AS bad_group_by,
        countIf(state NOT IN (2, 3, 4, 5, 6)) AS bad_state
    FROM {table_units}
    WHERE version_date = %(uvd)s AND version_id = %(vid)s
    """
    bad_group_by, bad_state = client.execute(query, params)[0]

    details = [
        f"bad_group_by={bad_group_by}",
        f"bad_state={bad_state}",
    ]
    passed = bad_group_by == 0 and bad_state == 0
    print_result("L2 INV scope engines 3/4 only", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
