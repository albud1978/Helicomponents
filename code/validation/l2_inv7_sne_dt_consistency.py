#!/usr/bin/env python3
"""
L2 INV-7: консистентность dt — delta_sne/delta_ppr == planner_dt для ops.
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
        description="L2 INV-7: delta_sne/delta_ppr == planner_dt"
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
        [
            "version_date",
            "version_id",
            "day_u16",
            "psn",
            "group_by",
            "state",
            "pre_state_id",
            "sne",
            "ppr",
            "aircraft_number",
        ],
    )
    ensure_columns(
        client,
        table_main,
        [
            "version_date",
            "version_id",
            "day_u16",
            "aircraft_number",
            "daily_today_u32",
            "status_id",
        ],
    )

    params = {
        "pvd": args.planner_version_date,
        "uvd": args.units_version_date_int,
        "vid": args.version_id,
    }
    query = f"""
    WITH base AS (
        SELECT
            psn,
            version_date,
            group_by,
            day_u16,
            aircraft_number,
            state,
            pre_state_id,
            sne,
            ppr,
            lagInFrame(sne, 1, 0) OVER w AS prev_sne,
            lagInFrame(ppr, 1, 0) OVER w AS prev_ppr,
            lagInFrame(day_u16, 1, 0) OVER w AS prev_day
        FROM {table_units}
        WHERE version_date = %(uvd)s
          AND version_id = %(vid)s
          AND group_by IN (3, 4)
        WINDOW w AS (PARTITION BY psn, version_date ORDER BY day_u16)
    ),
    ops_steps AS (
        SELECT
            psn,
            group_by,
            day_u16,
            aircraft_number,
            sne,
            ppr,
            prev_sne,
            prev_ppr,
            prev_day,
            toInt64(sne) - toInt64(prev_sne) AS sne_delta,
            toInt64(ppr) - toInt64(prev_ppr) AS ppr_delta,
            (prev_day = day_u16 - 1) AS is_consecutive
        FROM base
        WHERE state = 2
          AND pre_state_id = 2
          AND aircraft_number > 0
    )
    SELECT
        count() AS checked_rows,
        countIf(NOT is_consecutive) AS skipped_non_consecutive,
        countIf(is_consecutive AND isNull(p.daily_today_u32)) AS skipped_no_planner_dt,
        countIf(
            is_consecutive
            AND NOT isNull(p.daily_today_u32)
            AND (sne_delta != p.daily_today_u32 OR ppr_delta != p.daily_today_u32)
        ) AS violations
    FROM ops_steps s
    ANY LEFT JOIN {table_main} p
        ON p.version_date = %(pvd)s
       AND p.version_id = %(vid)s
       AND p.day_u16 = s.day_u16
       AND p.aircraft_number = s.aircraft_number
    """
    (
        checked_rows,
        skipped_non_consecutive,
        skipped_no_planner_dt,
        violations,
    ) = client.execute(query, params)[0]

    details = [
        f"checked_rows={checked_rows}",
        f"skipped_non_consecutive={skipped_non_consecutive}",
        f"skipped_no_planner_dt={skipped_no_planner_dt}",
        f"violations={violations}",
    ]

    if violations:
        sample_query = f"""
        WITH base AS (
            SELECT
                psn,
                version_date,
                group_by,
                day_u16,
                aircraft_number,
                state,
                pre_state_id,
                sne,
                ppr,
                lagInFrame(sne, 1, 0) OVER w AS prev_sne,
                lagInFrame(ppr, 1, 0) OVER w AS prev_ppr,
                lagInFrame(day_u16, 1, 0) OVER w AS prev_day
            FROM {table_units}
            WHERE version_date = %(uvd)s
              AND version_id = %(vid)s
              AND group_by IN (3, 4)
            WINDOW w AS (PARTITION BY psn, version_date ORDER BY day_u16)
        ),
        ops_steps AS (
            SELECT
                psn,
                group_by,
                day_u16,
                aircraft_number,
                sne,
                ppr,
                prev_sne,
                prev_ppr,
                prev_day,
                toInt64(sne) - toInt64(prev_sne) AS sne_delta,
                toInt64(ppr) - toInt64(prev_ppr) AS ppr_delta,
                (prev_day = day_u16 - 1) AS is_consecutive
            FROM base
            WHERE state = 2
              AND pre_state_id = 2
              AND aircraft_number > 0
        )
        SELECT
            psn,
            day_u16,
            aircraft_number,
            sne_delta,
            ppr_delta,
            p.daily_today_u32 AS planner_dt,
            sne_delta - toInt64(p.daily_today_u32) AS diff_sne,
            ppr_delta - toInt64(p.daily_today_u32) AS diff_ppr
        FROM ops_steps s
        ANY LEFT JOIN {table_main} p
            ON p.version_date = %(pvd)s
           AND p.version_id = %(vid)s
           AND p.day_u16 = s.day_u16
           AND p.aircraft_number = s.aircraft_number
        WHERE is_consecutive
          AND NOT isNull(p.daily_today_u32)
          AND (sne_delta != p.daily_today_u32 OR ppr_delta != p.daily_today_u32)
        ORDER BY abs(diff_sne) DESC
        LIMIT 5
        """
        rows = client.execute(sample_query, params)
        details.append("top5 sample (psn, day, acn, delta_sne/ppr, planner_dt, diff_sne/ppr):")
        for psn, day, acn, ds, dp, dt, diff_sne, diff_ppr in rows:
            details.append(
                f"  psn={psn}, day={day}, acn={acn}, "
                f"delta_sne={ds}, delta_ppr={dp}, planner_dt={dt}, "
                f"diff_sne={diff_sne}, diff_ppr={diff_ppr}"
            )

    passed = violations == 0
    print_result("L2 INV-7 dt consistency", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
