#!/usr/bin/env python3
"""
L2 TEMP-4: нет бесконечного ремонта (span <= max_repair_days).
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
        description="L2 TEMP-4: no infinite repair"
    )
    parser.add_argument("--planner-version-date", type=int, default=None)
    parser.add_argument("--units-version-date-int", required=True, type=int)
    parser.add_argument("--version-id", required=True, type=int)
    parser.add_argument("--table-main", default="sim_masterv2_v9")
    parser.add_argument("--table-units", default="sim_units_v2")
    parser.add_argument(
        "--max-repair-days",
        type=int,
        default=210,
        help="Макс допустимая длительность ремонта (default=210)",
    )
    args = parser.parse_args()

    table_units = validate_table_name(args.table_units)
    client = get_client()
    ensure_columns(
        client,
        table_units,
        ["version_date", "version_id", "day_u16", "psn", "group_by", "state"],
    )

    params = {
        "uvd": args.units_version_date_int,
        "vid": args.version_id,
        "max_days": args.max_repair_days,
    }
    query = f"""
    WITH base1 AS (
        SELECT
            psn,
            group_by,
            day_u16,
            state,
            lagInFrame(state, 1, 0) OVER w AS prev_state
        FROM {table_units}
        WHERE version_date = %(uvd)s
          AND version_id = %(vid)s
          AND group_by IN (3, 4)
        WINDOW w AS (PARTITION BY psn ORDER BY day_u16)
    ),
    base2 AS (
        SELECT
            psn,
            group_by,
            day_u16,
            state,
            prev_state,
            sum(if(state = 4 AND prev_state != 4, 1, 0)) OVER w AS span_id
        FROM base1
        WINDOW w AS (PARTITION BY psn ORDER BY day_u16)
    ),
    spans AS (
        SELECT
            psn,
            group_by,
            span_id,
            min(day_u16) AS enter_day,
            max(day_u16) AS last_day,
            max(day_u16) - min(day_u16) AS span
        FROM base2
        WHERE state = 4
        GROUP BY psn, group_by, span_id
    )
    SELECT count()
    FROM spans
    WHERE span > %(max_days)s
    """
    violations = client.execute(query, params)[0][0]
    details = [
        f"max_repair_days={args.max_repair_days}",
        f"violations={violations}",
    ]

    if violations:
        sample_query = f"""
        WITH base1 AS (
            SELECT
                psn,
                group_by,
                day_u16,
                state,
                lagInFrame(state, 1, 0) OVER w AS prev_state
            FROM {table_units}
            WHERE version_date = %(uvd)s
              AND version_id = %(vid)s
              AND group_by IN (3, 4)
            WINDOW w AS (PARTITION BY psn ORDER BY day_u16)
        ),
        base2 AS (
            SELECT
                psn,
                group_by,
                day_u16,
                state,
                prev_state,
                sum(if(state = 4 AND prev_state != 4, 1, 0)) OVER w AS span_id
            FROM base1
            WINDOW w AS (PARTITION BY psn ORDER BY day_u16)
        ),
        spans AS (
            SELECT
                psn,
                group_by,
                span_id,
                min(day_u16) AS enter_day,
                max(day_u16) AS last_day,
                max(day_u16) - min(day_u16) AS span
            FROM base2
            WHERE state = 4
            GROUP BY psn, group_by, span_id
        )
        SELECT psn, enter_day, last_day, span
        FROM spans
        WHERE span > %(max_days)s
        ORDER BY span DESC
        LIMIT 5
        """
        rows = client.execute(sample_query, params)
        details.append("top5 sample (psn, enter, last, span):")
        for psn, enter_day, last_day, span in rows:
            details.append(
                f"  psn={psn}, enter={enter_day}, last={last_day}, span={span}"
            )

    passed = violations == 0
    print_result("L2 TEMP-4 no infinite repair", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
