#!/usr/bin/env python3
"""
L2 INV-1: sne <= ll_expected для engines в operations.
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
    parser = argparse.ArgumentParser(description="L2 INV-1: sne <= ll_expected")
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
        ["version_date", "version_id", "state", "group_by", "sne", "partseqno_i"],
    )
    ensure_columns(
        client,
        table_md,
        ["partno_comp", "ll_mi8", "ll_mi17"],
    )

    params = {"uvd": args.units_version_date_int, "vid": args.version_id}
    query = f"""
    WITH joined AS (
        SELECT
            u.psn,
            u.group_by,
            u.sne,
            u.partseqno_i,
            if(u.group_by = 3, m.ll_mi8, m.ll_mi17) AS ll_expected
        FROM {table_units} u
        LEFT JOIN {table_md} m
            ON u.partseqno_i = m.partno_comp
        WHERE u.version_date = %(uvd)s
          AND u.version_id = %(vid)s
          AND u.group_by IN (3, 4)
          AND u.state = 2
    )
    SELECT
        countIf(ll_expected IS NULL OR ll_expected <= 0) AS skipped_no_norm,
        countIf(ll_expected > 0 AND sne > ll_expected) AS violations
    FROM joined
    """
    skipped_no_norm, violations = client.execute(query, params)[0]

    details = [
        f"count_skipped_no_norm={skipped_no_norm}",
        f"violations={violations}",
    ]

    if violations:
        sample_query = f"""
        WITH joined AS (
            SELECT
                u.psn,
                u.group_by,
                u.sne,
                u.partseqno_i,
                if(u.group_by = 3, m.ll_mi8, m.ll_mi17) AS ll_expected
            FROM {table_units} u
            LEFT JOIN {table_md} m
                ON u.partseqno_i = m.partno_comp
            WHERE u.version_date = %(uvd)s
              AND u.version_id = %(vid)s
              AND u.group_by IN (3, 4)
              AND u.state = 2
        )
        SELECT
            psn, group_by, sne, ll_expected,
            toInt64(sne) - toInt64(ll_expected) AS exceed
        FROM joined
        WHERE ll_expected > 0 AND sne > ll_expected
        ORDER BY exceed DESC
        LIMIT 5
        """
        rows = client.execute(sample_query, params)
        details.append("top5 sample (psn, gb, sne, ll_expected, exceed):")
        for psn, gb, sne, ll_expected, exceed in rows:
            details.append(
                f"  psn={psn}, gb={gb}, sne={sne}, ll_expected={ll_expected}, exceed={exceed}"
            )

    passed = violations == 0
    print_result("L2 INV-1 sne<=ll_expected", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
