#!/usr/bin/env python3
"""
L2 TEMP-1: длительность ремонта не меньше repair_time_expected.
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
        description="L2 TEMP-1: repair duration >= repair_time_expected"
    )
    parser.add_argument("--planner-version-date", type=int, default=None)
    parser.add_argument("--units-version-date-int", required=True, type=int)
    parser.add_argument("--version-id", required=True, type=int)
    parser.add_argument("--table-main", default="sim_masterv2_v9")
    parser.add_argument("--table-units", default="sim_units_v2")
    parser.add_argument("--table-md", default="md_components")
    args = parser.parse_args()

    table_units = validate_table_name(args.table_units)
    table_md = validate_table_name(args.table_md)
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
            "partseqno_i",
        ],
    )
    ensure_columns(
        client,
        table_md,
        ["partno_comp", "repair_time"],
    )

    params = {"uvd": args.units_version_date_int, "vid": args.version_id}
    query = f"""
    WITH base1 AS (
        SELECT
            psn,
            group_by,
            day_u16,
            state,
            partseqno_i,
            lagInFrame(state, 1, 0) OVER w AS prev_state,
            leadInFrame(state, 1, 0) OVER w AS next_state,
            leadInFrame(day_u16, 1, 0) OVER w AS next_day
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
            partseqno_i,
            prev_state,
            next_state,
            next_day,
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
            max(day_u16) - min(day_u16) AS span,
            any(partseqno_i) AS partseqno_i,
            max(if(state = 4 AND next_state != 4 AND next_day > 0, 1, 0)) AS has_exit
        FROM base2
        WHERE state = 4
        GROUP BY psn, group_by, span_id
    )
    SELECT
        countIf(has_exit = 1) AS spans_ended,
        countIf(has_exit = 1 AND (m.repair_time IS NULL OR m.repair_time <= 0)) AS skipped_no_norm,
        countIf(has_exit = 1 AND m.repair_time > 0 AND span < m.repair_time) AS violations
    FROM spans s
    LEFT JOIN {table_md} m ON s.partseqno_i = m.partno_comp
    """
    spans_ended, skipped_no_norm, violations = client.execute(query, params)[0]

    details = [
        f"spans_ended={spans_ended}",
        f"count_skipped_no_norm={skipped_no_norm}",
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
                partseqno_i,
                lagInFrame(state, 1, 0) OVER w AS prev_state,
                leadInFrame(state, 1, 0) OVER w AS next_state,
                leadInFrame(day_u16, 1, 0) OVER w AS next_day
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
                partseqno_i,
                prev_state,
                next_state,
                next_day,
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
                max(day_u16) - min(day_u16) AS span,
                any(partseqno_i) AS partseqno_i,
                max(if(state = 4 AND next_state != 4 AND next_day > 0, 1, 0)) AS has_exit
            FROM base2
            WHERE state = 4
            GROUP BY psn, group_by, span_id
        )
        SELECT
            psn,
            group_by,
            enter_day,
            last_day,
            span,
            m.repair_time
        FROM spans s
        LEFT JOIN {table_md} m ON s.partseqno_i = m.partno_comp
        WHERE has_exit = 1 AND m.repair_time > 0 AND span < m.repair_time
        ORDER BY (m.repair_time - span) DESC
        LIMIT 5
        """
        rows = client.execute(sample_query, params)
        details.append("top5 sample (psn, gb, enter, last, span, repair_time):")
        for psn, gb, enter_day, last_day, span, rt in rows:
            details.append(
                f"  psn={psn}, gb={gb}, enter={enter_day}, last={last_day}, "
                f"span={span}, repair_time={rt}"
            )

    passed = violations == 0
    print_result("L2 TEMP-1 repair duration", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
