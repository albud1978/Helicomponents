#!/usr/bin/env python3
"""
INV-7: dt = mp5_lin (фактический налёт = программа полётов).

Проверяет: для агентов в operations (status_id=2, pre_status_id=2),
daily_today_u32 должен совпадать с кумулятивной разницей mp5 за шаг.

Примечание: эта проверка требует доступа к mp5_cumsum, который хранится
только в GPU env. На уровне ClickHouse можно проверить только что:
- dt > 0 для агентов в ops с adaptive_days > 0
- dt == 0 для агентов с dt=0 (зимование/простой по программе)
Полная проверка mp5_lin требует отдельного экспорта mp5 матрицы.

Упрощённая проверка: для pre_status_id=2 AND status_id=2 агентов,
dt должен быть >= 0 (не отрицательный) и sne должен расти на dt.
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
        description="INV-7: dt = mp5_lin (simplified: sne consistency)"
    )
    parser.add_argument("--version-id", required=True, type=int)
    parser.add_argument("--version-date", type=int, default=None)
    parser.add_argument("--table", default="sim_masterv2_v9")
    args = parser.parse_args()
    table = validate_table_name(args.table)
    client = get_client()

    vd_filter = ""
    params = {"vid": args.version_id}
    if args.version_date is not None:
        vd_filter = " AND version_date = %(vdate)s"
        params["vdate"] = args.version_date

    # Проверка: для последовательных шагов в ops (pre_status=2, status=2),
    # sne должен увеличиться ровно на dt
    query = f"""
    SELECT count()
    FROM (
        SELECT
            aircraft_number, version_date, day_u16, status_id, pre_status_id,
            sne, daily_today_u32 AS dt,
            lagInFrame(sne, 1, 0) OVER w AS prev_sne,
            lagInFrame(status_id, 1, 0) OVER w AS lag_st,
            lagInFrame(day_u16, 1, 0) OVER w AS prev_day
        FROM {table}
        WHERE version_id = %(vid)s{vd_filter} AND group_by IN (1, 2)
        WINDOW w AS (PARTITION BY aircraft_number, group_by, version_date ORDER BY day_u16)
    )
    WHERE status_id = 2 AND pre_status_id = 2
      AND prev_day > 0 AND lag_st = 2
      AND sne != prev_sne + dt
    """
    violations = client.execute(query, params)[0][0]
    details = [f"violations={violations}"]

    if violations:
        sample_query = f"""
        SELECT aircraft_number, day_u16, sne, prev_sne, dt,
               toInt64(sne) - toInt64(prev_sne) AS actual_delta,
               toInt64(sne) - toInt64(prev_sne) - toInt64(dt) AS diff
        FROM (
            SELECT
                aircraft_number, version_date, day_u16, status_id, pre_status_id,
                sne, daily_today_u32 AS dt,
                lagInFrame(sne, 1, 0) OVER w AS prev_sne,
                lagInFrame(status_id, 1, 0) OVER w AS lag_st,
                lagInFrame(day_u16, 1, 0) OVER w AS prev_day
            FROM {table}
            WHERE version_id = %(vid)s{vd_filter} AND group_by IN (1, 2)
            WINDOW w AS (PARTITION BY aircraft_number, group_by, version_date ORDER BY day_u16)
        )
        WHERE status_id = 2 AND pre_status_id = 2
          AND prev_day > 0 AND lag_st = 2
          AND sne != prev_sne + dt
        ORDER BY abs(toInt64(sne) - toInt64(prev_sne) - toInt64(dt)) DESC
        LIMIT 5
        """
        rows = client.execute(sample_query, params)
        details.append("top5 (acn, day, sne, prev_sne, dt, actual_delta, diff):")
        for acn, d, sne, psne, dt, ad, diff in rows:
            details.append(
                f"  acn={acn}, day={d}, sne={sne}, prev_sne={psne}, "
                f"dt={dt}, actual_delta={ad}, diff={diff}"
            )

    passed = violations == 0
    print_result("INV-7 dt=mp5 (sne consistency)", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
