#!/usr/bin/env python3
"""
L2 INV-8: sne/ppr frozen в storage (state=6).
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
    parser = argparse.ArgumentParser(description="L2 INV-8: storage frozen")
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
        ["version_date", "version_id", "day_u16", "psn", "group_by", "state", "sne", "ppr"],
    )

    params = {"uvd": args.units_version_date_int, "vid": args.version_id}
    count_query = f"""
    SELECT count()
    FROM (
        SELECT
            psn,
            group_by,
            day_u16,
            state,
            sne,
            ppr,
            lagInFrame(sne) OVER w AS prev_sne,
            lagInFrame(ppr) OVER w AS prev_ppr,
            lagInFrame(state) OVER w AS prev_state,
            lagInFrame(day_u16, 1, 0) OVER w AS prev_day
        FROM {table_units}
        WHERE version_date = %(uvd)s
          AND version_id = %(vid)s
          AND group_by IN (3, 4)
        WINDOW w AS (PARTITION BY psn ORDER BY day_u16)
    )
    WHERE state = 6
      AND prev_state = 6
      AND prev_day > 0
      AND (sne != prev_sne OR ppr != prev_ppr)
    """
    violations = client.execute(count_query, params)[0][0]
    details = [f"violations={violations}"]

    if violations:
        sample_query = f"""
        SELECT psn, day_u16, sne, prev_sne, ppr, prev_ppr
        FROM (
            SELECT
                psn,
                group_by,
                day_u16,
                state,
                sne,
                ppr,
                lagInFrame(sne) OVER w AS prev_sne,
                lagInFrame(ppr) OVER w AS prev_ppr,
                lagInFrame(state) OVER w AS prev_state,
                lagInFrame(day_u16, 1, 0) OVER w AS prev_day
            FROM {table_units}
            WHERE version_date = %(uvd)s
              AND version_id = %(vid)s
              AND group_by IN (3, 4)
            WINDOW w AS (PARTITION BY psn ORDER BY day_u16)
        )
        WHERE state = 6
          AND prev_state = 6
          AND prev_day > 0
          AND (sne != prev_sne OR ppr != prev_ppr)
        ORDER BY day_u16
        LIMIT 5
        """
        rows = client.execute(sample_query, params)
        details.append("top5 sample (psn, day, sne/prev_sne, ppr/prev_ppr):")
        for psn, day, sne, prev_sne, ppr, prev_ppr in rows:
            details.append(
                f"  psn={psn}, day={day}, sne={sne}/{prev_sne}, ppr={ppr}/{prev_ppr}"
            )

    passed = violations == 0
    print_result("L2 INV-8 storage frozen", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
