#!/usr/bin/env python3
"""
INV-13: cumulative Mi-17 dynamic spawn <= cumulative spawn_limit.

Dynamic spawn is the Mi-17 0->2 transition:
group_by=2, pre_status_id=0, status_id=2.
"""
import argparse
import re
import sys
from datetime import date as _date

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


def require_columns(client, table: str, columns: set[str]) -> None:
    existing = {row[0] for row in client.execute(f"DESCRIBE TABLE {table}")}
    missing = sorted(columns - existing)
    if missing:
        raise SystemExit(
            f"{table} missing required columns: {', '.join(missing)}. "
            "Run the Layer 1 Program_AC loader/migration before spawn_limit validation."
        )


def to_date(version_date_int: int) -> _date:
    y = version_date_int // 10000
    m = (version_date_int % 10000) // 100
    d = version_date_int % 100
    try:
        return _date(y, m, d)
    except ValueError as exc:
        raise SystemExit(f"Некорректный version_date: {version_date_int}") from exc


def load_spawn_limits(client, version_date_int: int, version_id: int):
    vdate = to_date(version_date_int)
    require_columns(
        client,
        "flight_program_ac",
        {"dates", "spawn_limit", "spawn_limit_active"},
    )
    rows = client.execute(
        """
        SELECT dates, spawn_limit, spawn_limit_active
        FROM flight_program_ac
        WHERE version_date = %(vd)s AND version_id = %(vid)s
        ORDER BY dates
        """,
        {"vd": vdate, "vid": version_id},
    )
    if not rows:
        raise SystemExit(
            f"flight_program_ac пуст для version_date={vdate}, version_id={version_id}. "
            "Невозможно получить spawn_limit."
        )

    start_date = min(dt for dt, _limit, _active in rows)
    limits_by_day = {}
    spawn_limit_active = 0
    for dt, limit, active in rows:
        day_idx = (dt - start_date).days
        limits_by_day[day_idx] = int(limit or 0)
        spawn_limit_active = max(spawn_limit_active, int(active or 0))
    return limits_by_day, spawn_limit_active


def build_cumulative_by_day(values_by_day):
    cumulative = {}
    running_total = 0
    for day in sorted(values_by_day.keys()):
        running_total += int(values_by_day[day])
        cumulative[day] = running_total
    return cumulative, running_total


def get_value_for_day(values_by_day, day_u16: int) -> int:
    if day_u16 in values_by_day:
        return int(values_by_day[day_u16])
    prev_day = None
    for day in sorted(values_by_day.keys()):
        if day <= day_u16:
            prev_day = day
        else:
            break
    if prev_day is not None:
        return int(values_by_day[prev_day])
    first_day = min(values_by_day.keys())
    return int(values_by_day[first_day])


def run(client, version_id: int, version_date=None, table: str = "sim_masterv2_v9") -> bool:
    table = validate_table_name(table)

    params = {"vid": version_id}
    if version_date is not None:
        version_dates = [int(version_date)]
    else:
        version_dates_query = f"""
        SELECT version_date
        FROM {table}
        WHERE version_id = %(vid)s
          AND group_by = 2
        GROUP BY version_date
        ORDER BY version_date
        """
        version_dates = [
            int(row[0]) for row in client.execute(version_dates_query, params)
        ]

    if not version_dates:
        raise SystemExit("Не удалось определить version_date из данных симуляции")

    details = [f"version_id={version_id}, table={table}"]
    total_violations = 0

    for version_date_int in version_dates:
        scoped_params = {"vid": version_id, "vdate": version_date_int}
        limits_by_day, spawn_limit_active = load_spawn_limits(
            client,
            version_date_int,
            version_id,
        )
        cumulative_limit, total_limit = build_cumulative_by_day(limits_by_day)

        day_query = f"""
        SELECT DISTINCT day_u16
        FROM {table}
        WHERE version_id = %(vid)s
          AND version_date = %(vdate)s
          AND group_by = 2
        ORDER BY day_u16
        """
        step_days = [int(row[0]) for row in client.execute(day_query, scoped_params)]

        spawn_query = f"""
        SELECT day_u16, countDistinct(idx) AS dynamic_spawn_count
        FROM {table}
        WHERE version_id = %(vid)s
          AND version_date = %(vdate)s
          AND group_by = 2
          AND pre_status_id = 0
          AND status_id = 2
        GROUP BY day_u16
        """
        dynamic_spawn_by_day = {
            int(day_u16): int(spawn_count)
            for day_u16, spawn_count in client.execute(spawn_query, scoped_params)
        }

        cumulative_dynamic = 0
        violations = []
        for day in step_days:
            cumulative_dynamic += dynamic_spawn_by_day.get(day, 0)
            cumulative_allowed = get_value_for_day(cumulative_limit, day)
            if spawn_limit_active and cumulative_dynamic > cumulative_allowed:
                violations.append(
                    (
                        day,
                        cumulative_dynamic,
                        cumulative_allowed,
                        cumulative_dynamic - cumulative_allowed,
                    )
                )

        total_violations += len(violations)
        max_excess = max((row[3] for row in violations), default=0)
        details.extend(
            [
                f"version_date={version_date_int}",
                f"  spawn_limit_active={'true' if spawn_limit_active else 'false'}",
                f"  spawn_limit_total={total_limit}",
                f"  dynamic_spawn_mi17_total={cumulative_dynamic}",
                f"  checked_days={len(step_days)}",
                f"  violations={len(violations)}",
                f"  max_excess={max_excess}",
            ]
        )
        if not spawn_limit_active:
            details.append("  inactive spawn_limit: PASS by definition")
        elif violations:
            details.append(
                "  violation sample (day, cumulative_dynamic, cumulative_limit, excess):"
            )
            for day, dynamic, limit, excess in violations[:5]:
                details.append(
                    f"    day={day}: dynamic={dynamic}, limit={limit}, excess={excess}"
                )

    details.append(f"total_violations={total_violations}")
    passed = total_violations == 0
    print_result("INV-13 cumulative Mi-17 dynamic spawn <= spawn_limit", passed, details)
    return passed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="INV-13: cumulative Mi-17 dynamic spawn <= cumulative spawn_limit"
    )
    parser.add_argument("--version-id", required=True, type=int, help="version_id")
    parser.add_argument(
        "--version-date",
        type=int,
        default=None,
        help="version_date (YYYYMMDD) для фильтрации",
    )
    parser.add_argument(
        "--table",
        default="sim_masterv2_v9",
        help="Таблица ClickHouse (по умолчанию: sim_masterv2_v9)",
    )
    args = parser.parse_args()
    client = get_client()
    passed = run(client, args.version_id, args.version_date, args.table)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
