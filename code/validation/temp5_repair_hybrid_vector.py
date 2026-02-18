#!/usr/bin/env python3
"""
TEMP-5: Hybrid precondition (master transitions + repairline vectors).

Проверяет:
1) busy_today >= count_4to2_day для каждого дня с 4->2 переходами.
2) strict future-window [day, day+window) с tail adjustment:
   future_busy_sum >= count_4to2_day * effective_window.

RepairLine экспорт — lookback-only, без ACN: занятость определяем по (repair_time > 0 AND free_days < repair_time).
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
    parser = argparse.ArgumentParser(
        description=(
            "TEMP-5: repair hybrid vector "
            "(master + repairline, strict future-window + tail adjustment)"
        )
    )
    parser.add_argument("--version-id", required=True, type=int, help="version_id")
    parser.add_argument(
        "--version-date",
        type=int,
        default=None,
        help="version_date (YYYYMMDD) для фильтрации",
    )
    parser.add_argument("--table-main", default="sim_masterv2_v9")
    parser.add_argument("--table-repair", default="sim_repairline_v9")
    parser.add_argument("--repair-window", type=int, default=180)
    args = parser.parse_args()

    table_main = validate_table_name(args.table_main)
    table_repair = validate_table_name(args.table_repair)
    repair_window = int(args.repair_window)
    if repair_window <= 0:
        raise SystemExit("--repair-window должен быть > 0")

    client = get_client()

    vd_filter = ""
    params = {
        "vid": args.version_id,
        "window": repair_window,
        "window_follow": repair_window - 1,
    }
    if args.version_date is not None:
        vd_filter = " AND version_date = %(vdate)s"
        params["vdate"] = args.version_date

    base_cte = f"""
    WITH event_days AS (
        SELECT
            group_by,
            day_u16,
            countIf(pre_status_id = 4 AND status_id = 2) AS cnt_4to2_day
        FROM {table_main}
        WHERE version_id = %(vid)s{vd_filter}
          AND group_by IN (1, 2)
        GROUP BY group_by, day_u16
        HAVING cnt_4to2_day > 0
    ),
    busy_by_day_raw AS (
        SELECT
            group_by,
            day_u16,
            countIf(repair_time > 0 AND free_days < repair_time) AS busy_day_count
        FROM {table_repair}
        WHERE version_id = %(vid)s{vd_filter}
          AND group_by IN (1, 2)
        GROUP BY group_by, day_u16
    ),
    max_day_by_group AS (
        SELECT
            group_by,
            max(day_u16) AS max_day_group
        FROM {table_repair}
        WHERE version_id = %(vid)s{vd_filter}
          AND group_by IN (1, 2)
        GROUP BY group_by
    ),
    all_days AS (
        SELECT group_by, day_u16 FROM event_days
        UNION DISTINCT
        SELECT group_by, day_u16 FROM busy_by_day_raw
    ),
    busy_by_day AS (
        SELECT
            d.group_by,
            d.day_u16,
            ifNull(b.busy_day_count, 0) AS busy_day_count
        FROM all_days AS d
        LEFT JOIN busy_by_day_raw AS b
            ON b.group_by = d.group_by AND b.day_u16 = d.day_u16
    ),
    busy_with_window AS (
        SELECT
            group_by,
            day_u16,
            busy_day_count,
            sum(busy_day_count) OVER (
                PARTITION BY group_by
                ORDER BY day_u16
                RANGE BETWEEN CURRENT ROW AND %(window_follow)s FOLLOWING
            ) AS future_busy_sum
        FROM busy_by_day
    ),
    event_with_window AS (
        SELECT
            e.group_by AS group_by,
            e.day_u16 AS day_u16,
            e.cnt_4to2_day AS cnt_4to2_day,
            ifNull(w.future_busy_sum, 0) AS future_busy_sum,
            ifNull(
                least(
                    %(window)s,
                    greatest(toInt32(m.max_day_group) - toInt32(e.day_u16) + 1, 0)
                ),
                0
            ) AS effective_window
        FROM event_days AS e
        LEFT JOIN busy_with_window AS w
            ON w.group_by = e.group_by AND w.day_u16 = e.day_u16
        LEFT JOIN max_day_by_group AS m
            ON m.group_by = e.group_by
    )
    """

    total_query = base_cte + """
    SELECT count()
    FROM event_days
    """
    total_event_days = client.execute(total_query, params)[0][0] or 0

    day_sanity_query = base_cte + """
    SELECT count()
    FROM event_days AS e
    LEFT JOIN busy_by_day AS b
        ON b.group_by = e.group_by AND b.day_u16 = e.day_u16
    WHERE b.busy_day_count < e.cnt_4to2_day
    """
    day_sanity_violations = client.execute(day_sanity_query, params)[0][0] or 0

    window_query = base_cte + """
    SELECT count()
    FROM event_with_window
    WHERE future_busy_sum < (
        cnt_4to2_day * if(effective_window <= 0, 0, effective_window)
    )
    """
    window_capacity_violations = client.execute(window_query, params)[0][0] or 0

    details = [
        f"repair_window={repair_window}",
        "tail_adjusted=true (strict future window [day, day+repair_window))",
        f"total_event_days={total_event_days}",
        f"day_sanity_violations={day_sanity_violations}",
        f"window_capacity_violations={window_capacity_violations}",
    ]

    if day_sanity_violations:
        sample_query = base_cte + """
        SELECT
            e.group_by,
            e.day_u16,
            e.cnt_4to2_day,
            b.busy_day_count AS busy_today
        FROM event_days AS e
        LEFT JOIN busy_by_day AS b
            ON b.group_by = e.group_by AND b.day_u16 = e.day_u16
        WHERE b.busy_day_count < e.cnt_4to2_day
        ORDER BY (e.cnt_4to2_day - b.busy_day_count) DESC, e.day_u16
        LIMIT 5
        """
        rows = client.execute(sample_query, params)
        details.append("top5 day_sanity (gb, day, cnt, busy_today):")
        for gb, day, cnt, busy_today in rows:
            details.append(
                f"  gb={gb}, day={day}, cnt={cnt}, busy_today={busy_today}"
            )

    if window_capacity_violations:
        window_sample_query = base_cte + """
        SELECT
            group_by,
            day_u16,
            cnt_4to2_day,
            future_busy_sum,
            effective_window,
            (cnt_4to2_day * if(effective_window <= 0, 0, effective_window)) AS required,
            (
                (cnt_4to2_day * if(effective_window <= 0, 0, effective_window))
                - future_busy_sum
            ) AS deficit
        FROM event_with_window
        WHERE future_busy_sum < (
            cnt_4to2_day * if(effective_window <= 0, 0, effective_window)
        )
        ORDER BY deficit DESC, day_u16
        LIMIT 5
        """
        rows = client.execute(window_sample_query, params)
        details.append(
            "top5 window_capacity (gb, day, cnt, future_busy_sum, required, deficit):"
        )
        for gb, day, cnt, future_sum, required, deficit in rows:
            details.append(
                "  gb={gb}, day={day}, cnt={cnt}, future_busy_sum={fbs}, "
                "required={req}, deficit={defc}".format(
                    gb=gb,
                    day=day,
                    cnt=cnt,
                    fbs=future_sum,
                    req=required,
                    defc=deficit,
                )
            )

    passed = day_sanity_violations == 0 and window_capacity_violations == 0
    print_result("TEMP-5 repair hybrid vector", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
