#!/usr/bin/env python3
"""
L2 INV-0b: planner ops требуют полный комплект engines (>=2).
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
        description="L2 INV-0b: planner ops требуют >=2 engines"
    )
    parser.add_argument("--planner-version-date", required=True, type=int)
    parser.add_argument("--units-version-date-int", required=True, type=int)
    parser.add_argument("--version-id", required=True, type=int)
    parser.add_argument("--table-main", default="sim_masterv2_v9")
    parser.add_argument("--table-units", default="sim_units_v2")
    args = parser.parse_args()

    table_main = validate_table_name(args.table_main)
    table_units = validate_table_name(args.table_units)
    client = get_client()
    ensure_columns(
        client,
        table_units,
        ["version_date", "version_id", "day_u16", "group_by", "state", "aircraft_number"],
    )
    ensure_columns(
        client,
        table_main,
        ["version_date", "version_id", "day_u16", "aircraft_number", "status_id", "group_by"],
    )

    params = {
        "pvd": args.planner_version_date,
        "uvd": args.units_version_date_int,
        "vid": args.version_id,
    }
    query = f"""
    WITH planner_ops AS (
        SELECT
            day_u16,
            aircraft_number,
            group_by,
            if(group_by = 1, 3, 4) AS engine_group_by
        FROM {table_main}
        WHERE version_date = %(pvd)s
          AND version_id = %(vid)s
          AND status_id = 2
          AND group_by IN (1, 2)
    ),
    engines AS (
        SELECT day_u16, aircraft_number, group_by, count() AS engine_ops
        FROM {table_units}
        WHERE version_date = %(uvd)s
          AND version_id = %(vid)s
          AND state = 2
          AND group_by IN (3, 4)
          AND aircraft_number > 0
        GROUP BY day_u16, aircraft_number, group_by
    )
    SELECT
        count() AS planner_ops_rows,
        countIf(p.group_by = 1) AS planner_ops_gb1,
        countIf(p.group_by = 2) AS planner_ops_gb2,
        countIf(p.group_by = 1 AND ifNull(e.engine_ops, 0) >= 2) AS fullkit_gb1,
        countIf(p.group_by = 2 AND ifNull(e.engine_ops, 0) >= 2) AS fullkit_gb2,
        countIf(p.group_by = 1 AND ifNull(e.engine_ops, 0) < 2) AS violations_gb1,
        countIf(p.group_by = 2 AND ifNull(e.engine_ops, 0) < 2) AS violations_gb2
    FROM planner_ops p
    LEFT JOIN engines e
        ON e.day_u16 = p.day_u16
       AND e.aircraft_number = p.aircraft_number
       AND e.group_by = p.engine_group_by
    """
    (
        planner_ops_rows,
        planner_ops_gb1,
        planner_ops_gb2,
        fullkit_gb1,
        fullkit_gb2,
        violations_gb1,
        violations_gb2,
    ) = client.execute(query, params)[0]
    violations = int(violations_gb1) + int(violations_gb2)

    details = [
        f"planner_ops_rows={planner_ops_rows}",
        f"planner_ops_gb1={planner_ops_gb1}, fullkit_gb1={fullkit_gb1}, violations_gb1={violations_gb1}",
        f"planner_ops_gb2={planner_ops_gb2}, fullkit_gb2={fullkit_gb2}, violations_gb2={violations_gb2}",
        f"violations_total={violations}",
    ]

    if violations:
        sample_query = f"""
        WITH planner_ops AS (
            SELECT
                day_u16,
                aircraft_number,
                group_by,
                if(group_by = 1, 3, 4) AS engine_group_by
            FROM {table_main}
            WHERE version_date = %(pvd)s
              AND version_id = %(vid)s
              AND status_id = 2
              AND group_by IN (1, 2)
        ),
        engines AS (
            SELECT day_u16, aircraft_number, group_by, count() AS engine_ops
            FROM {table_units}
            WHERE version_date = %(uvd)s
              AND version_id = %(vid)s
              AND state = 2
              AND group_by IN (3, 4)
              AND aircraft_number > 0
            GROUP BY day_u16, aircraft_number, group_by
        )
        SELECT
            p.aircraft_number,
            p.day_u16,
            p.group_by,
            ifNull(e.engine_ops, 0) AS engine_ops
        FROM planner_ops p
        LEFT JOIN engines e
            ON e.day_u16 = p.day_u16
           AND e.aircraft_number = p.aircraft_number
           AND e.group_by = p.engine_group_by
        WHERE ifNull(e.engine_ops, 0) < 2
        ORDER BY engine_ops ASC, p.day_u16
        LIMIT 5
        """
        rows = client.execute(sample_query, params)
        details.append("top5 sample (acn, day, planner_gb, engine_ops):")
        for acn, day, gb, cnt in rows:
            details.append(f"  acn={acn}, day={day}, planner_gb={gb}, engine_ops={cnt}")

    passed = violations == 0
    print_result("L2 INV-0b planner ops full engine set", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
