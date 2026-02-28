#!/usr/bin/env python3
"""
L2 INV-17: допустимые переходы состояния для engines.
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
        description="L2 INV-17: allowed transitions"
    )
    parser.add_argument("--planner-version-date", type=int, default=None)
    parser.add_argument("--units-version-date-int", required=True, type=int)
    parser.add_argument("--version-id", required=True, type=int)
    parser.add_argument("--table-main", default="sim_masterv2_v9")
    parser.add_argument("--table-units", default="sim_units_v2")
    args = parser.parse_args()

    table_units = validate_table_name(args.table_units)
    client = get_client()
    ensure_columns(
        client,
        table_units,
        [
            "version_date",
            "version_id",
            "day_u16",
            "psn",
            "group_by",
            "state",
            "pre_state_id",
        ],
    )

    params = {"uvd": args.units_version_date_int, "vid": args.version_id}
    query = f"""
    WITH base AS (
        SELECT
            psn,
            day_u16,
            pre_state_id AS prev_state,
            state AS cur_state
        FROM {table_units}
        WHERE version_date = %(uvd)s
          AND version_id = %(vid)s
          AND group_by IN (3, 4)
          AND pre_state_id > 0
    )
    SELECT count()
    FROM base
    WHERE (prev_state, cur_state) NOT IN (
          (2, 2), (2, 3), (2, 4), (2, 6),
          (3, 2), (3, 3),
          (4, 4), (4, 5),
          (5, 5), (5, 2),
          (6, 6)
      )
    """
    violations = client.execute(query, params)[0][0]
    details = [f"violations={violations}"]

    if violations:
        sample_query = f"""
        WITH base AS (
            SELECT
                psn,
                day_u16,
                pre_state_id AS prev_state,
                state AS cur_state
            FROM {table_units}
            WHERE version_date = %(uvd)s
              AND version_id = %(vid)s
              AND group_by IN (3, 4)
              AND pre_state_id > 0
        )
        SELECT psn, day_u16, prev_state, cur_state
        FROM base
        WHERE (prev_state, cur_state) NOT IN (
              (2, 2), (2, 3), (2, 4), (2, 6),
              (3, 2), (3, 3),
              (4, 4), (4, 5),
              (5, 5), (5, 2),
              (6, 6)
          )
        ORDER BY day_u16
        LIMIT 5
        """
        rows = client.execute(sample_query, params)
        details.append("top5 sample (psn, day, prev_state, cur_state):")
        for psn, day, prev_state, cur_state in rows:
            details.append(
                f"  psn={psn}, day={day}, prev_state={prev_state}, cur_state={cur_state}"
            )

        breakdown_query = f"""
        WITH base AS (
            SELECT
                pre_state_id AS prev_state,
                state AS cur_state
            FROM {table_units}
            WHERE version_date = %(uvd)s
              AND version_id = %(vid)s
              AND group_by IN (3, 4)
              AND pre_state_id > 0
        )
        SELECT prev_state, cur_state, count() AS cnt
        FROM base
        WHERE (prev_state, cur_state) NOT IN (
              (2, 2), (2, 3), (2, 4), (2, 6),
              (3, 2), (3, 3),
              (4, 4), (4, 5),
              (5, 5), (5, 2),
              (6, 6)
        )
        GROUP BY prev_state, cur_state
        ORDER BY cnt DESC
        LIMIT 10
        """
        rows = client.execute(breakdown_query, params)
        details.append("top transitions (prev_state -> cur_state, cnt):")
        for prev_state, cur_state, cnt in rows:
            details.append(
                f"  {prev_state} -> {cur_state}: {cnt}"
            )

    passed = violations == 0
    print_result("L2 INV-17 allowed transitions", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
