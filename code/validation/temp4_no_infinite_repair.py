#!/usr/bin/env python3
"""
TEMP-4: Liveness — нет бесконечного ремонта.

Проверяет: ни один агент не застревает в status_id=4 (repair)
дольше repair_time + tolerance дней.
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
    parser = argparse.ArgumentParser(description="TEMP-4: no infinite repair")
    parser.add_argument("--version-id", required=True, type=int)
    parser.add_argument("--version-date", type=int, default=None)
    parser.add_argument("--table", default="sim_masterv2_v9")
    parser.add_argument("--max-repair-days", type=int, default=210,
                        help="Макс допустимая длительность ремонта (repair_time + tolerance, default=210)")
    args = parser.parse_args()
    table = validate_table_name(args.table)
    client = get_client()

    vd_filter = ""
    params = {"vid": args.version_id}
    if args.version_date is not None:
        vd_filter = " AND version_date = %(vdate)s"
        params["vdate"] = args.version_date

    # Нельзя фильтровать status_id=4 до lag: так склеиваются разные repair-отрезки через промежуточные статусы.
    # Находим непрерывные интервалы в repair для каждого агента и проверяем что длительность <= max_repair_days.
    query = f"""
    WITH repair_spans AS (
        SELECT
            aircraft_number, group_by, version_date,
            min(day_u16) AS enter_day,
            max(day_u16) AS last_day_in_repair,
            max(day_u16) - min(day_u16) AS span,
            groupArray(day_u16) AS days
        FROM (
            SELECT
                   aircraft_number,
                   group_by,
                   version_date,
                   day_u16,
                   is_repair,
                   sum(new_span) OVER (PARTITION BY aircraft_number, group_by, version_date ORDER BY day_u16) AS span_id
            FROM (
                SELECT
                    aircraft_number,
                    group_by,
                    version_date,
                    day_u16,
                    is_repair,
                    prev_is_repair,
                    if(is_repair = 1 AND prev_is_repair = 0, 1, 0) AS new_span
                FROM (
                    SELECT
                        aircraft_number,
                        group_by,
                        version_date,
                        day_u16,
                        if(status_id = 4, 1, 0) AS is_repair,
                        lagInFrame(if(status_id = 4, 1, 0), 1, 0) OVER w AS prev_is_repair
                    FROM {table}
                    WHERE version_id = %(vid)s{vd_filter} AND group_by IN (1, 2)
                    WINDOW w AS (PARTITION BY aircraft_number, group_by, version_date ORDER BY day_u16)
                )
            )
            WHERE is_repair = 1
        )
        GROUP BY aircraft_number, group_by, version_date, span_id
    )
    SELECT count() FROM repair_spans
    WHERE span > {args.max_repair_days}
    """
    violations = client.execute(query, params)[0][0]
    details = [
        f"max_repair_days={args.max_repair_days}",
        f"violations={violations}",
    ]

    if violations:
        sample = client.execute(f"""
        WITH repair_spans AS (
            SELECT
                aircraft_number, group_by, version_date,
                min(day_u16) AS enter_day,
                max(day_u16) AS last_day,
                max(day_u16) - min(day_u16) AS span
            FROM (
                SELECT
                       aircraft_number,
                       group_by,
                       version_date,
                       day_u16,
                       is_repair,
                       sum(new_span) OVER (PARTITION BY aircraft_number, group_by, version_date ORDER BY day_u16) AS span_id
                FROM (
                    SELECT
                        aircraft_number,
                        group_by,
                        version_date,
                        day_u16,
                        is_repair,
                        prev_is_repair,
                        if(is_repair = 1 AND prev_is_repair = 0, 1, 0) AS new_span
                    FROM (
                        SELECT
                            aircraft_number,
                            group_by,
                            version_date,
                            day_u16,
                            if(status_id = 4, 1, 0) AS is_repair,
                            lagInFrame(if(status_id = 4, 1, 0), 1, 0) OVER w AS prev_is_repair
                        FROM {table}
                        WHERE version_id = %(vid)s{vd_filter} AND group_by IN (1, 2)
                        WINDOW w AS (PARTITION BY aircraft_number, group_by, version_date ORDER BY day_u16)
                    )
                )
                WHERE is_repair = 1
            )
            GROUP BY aircraft_number, group_by, version_date, span_id
        )
        SELECT aircraft_number, enter_day, last_day, span
        FROM repair_spans
        WHERE span > {args.max_repair_days}
        ORDER BY span DESC
        LIMIT 5
        """, params)
        details.append("top5 (acn, enter_day, last_day, span):")
        for acn, ed, ld, sp in sample:
            details.append(f"  acn={acn}, enter={ed}, last={ld}, span={sp}")

    passed = violations == 0
    print_result("TEMP-4 no infinite repair", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
