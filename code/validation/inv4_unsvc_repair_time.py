#!/usr/bin/env python3
"""
INV-4: Возврат из unserviceable в operations не раньше repair_time.

Проверяет: для каждого агента, вошедшего в ops через P2 (pre_status_id IN (7,4)),
количество дней между уходом из ops (2->7) и возвратом >= repair_time.

Реализация: через window-функции находим пары exit->entry для каждого агента.
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


def run(client, version_id: int, version_date=None, table: str = "sim_masterv2_v9") -> bool:
    table = validate_table_name(table)

    vd_filter = ""
    params = {"vid": version_id}
    if version_date is not None:
        vd_filter = " AND version_date = %(vdate)s"
        params["vdate"] = version_date

    # Стратегия:
    # 1. Собираем все переходы 2->7 (exit from ops) и 7/4->2 (return to ops через P2)
    # 2. Для каждого return-перехода находим ближайший предшествующий exit
    # 3. Проверяем gap >= repair_time агента

    # Шаг 1: все переходы (только момент перехода)
    # Шаг 2: для return-переходов ищем последний exit через window

    query = f"""
    WITH transitions AS (
        SELECT
            aircraft_number, group_by, version_date, day_u16,
            pre_status_id, status_id, repair_time
        FROM {table}
        WHERE version_id = %(vid)s{vd_filter}
          AND group_by IN (1, 2)
          AND pre_status_id != status_id
          AND pre_status_id > 0
    ),
    -- Переходы 2->7 (exit ops to unsvc)
    exits AS (
        SELECT aircraft_number, group_by, version_date, day_u16 AS exit_day,
               row_number() OVER (PARTITION BY aircraft_number, group_by, version_date ORDER BY day_u16) AS rn
        FROM transitions
        WHERE pre_status_id = 2 AND status_id = 7
    ),
    -- Переходы в ops через ремонт (7->2, 4->2)
    returns AS (
        SELECT aircraft_number, group_by, version_date, day_u16 AS return_day, repair_time AS rt,
               row_number() OVER (PARTITION BY aircraft_number, group_by, version_date ORDER BY day_u16) AS rn
        FROM transitions
        WHERE status_id = 2 AND pre_status_id IN (4, 7)
    )
    SELECT count()
    FROM exits e
    INNER JOIN returns r
        ON e.aircraft_number = r.aircraft_number
        AND e.group_by = r.group_by
        AND e.version_date = r.version_date
        AND e.rn = r.rn
    WHERE r.return_day - e.exit_day < r.rt
      AND r.return_day > e.exit_day
    """
    violations = client.execute(query, params)[0][0]
    details = [f"violations={violations}"]

    if violations:
        sample_query = f"""
        WITH transitions AS (
            SELECT
                aircraft_number, group_by, day_u16,
                version_date,
                pre_status_id, status_id, repair_time
            FROM {table}
            WHERE version_id = %(vid)s{vd_filter}
              AND group_by IN (1, 2)
              AND pre_status_id != status_id
              AND pre_status_id > 0
        ),
        exits AS (
            SELECT aircraft_number, group_by, version_date, day_u16 AS exit_day,
                   row_number() OVER (PARTITION BY aircraft_number, group_by, version_date ORDER BY day_u16) AS rn
            FROM transitions
            WHERE pre_status_id = 2 AND status_id = 7
        ),
        returns AS (
            SELECT aircraft_number, group_by, version_date, day_u16 AS return_day, repair_time AS rt,
                   row_number() OVER (PARTITION BY aircraft_number, group_by, version_date ORDER BY day_u16) AS rn
            FROM transitions
            WHERE status_id = 2 AND pre_status_id IN (4, 7)
        )
        SELECT e.aircraft_number, e.exit_day, r.return_day,
               r.return_day - e.exit_day AS gap, r.rt AS repair_time
        FROM exits e
        INNER JOIN returns r
            ON e.aircraft_number = r.aircraft_number
            AND e.group_by = r.group_by
            AND e.version_date = r.version_date
            AND e.rn = r.rn
        WHERE r.return_day - e.exit_day < r.rt
          AND r.return_day > e.exit_day
        ORDER BY gap ASC
        LIMIT 5
        """
        rows = client.execute(sample_query, params)
        details.append("top5 (acn, exit_day, return_day, gap, repair_time):")
        for acn, ed, rd, gap, rt in rows:
            details.append(
                f"  acn={acn}, exit={ed}, return={rd}, gap={gap}, repair_time={rt}"
            )

    passed = violations == 0
    print_result("INV-4 unsvc repair time", passed, details)
    return passed


def main() -> int:
    parser = argparse.ArgumentParser(description="INV-4: unsvc min repair time")
    parser.add_argument("--version-id", required=True, type=int)
    parser.add_argument("--version-date", type=int, default=None)
    parser.add_argument("--table", default="sim_masterv2_v9")
    args = parser.parse_args()
    client = get_client()
    passed = run(client, args.version_id, args.version_date, args.table)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
