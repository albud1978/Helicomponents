#!/usr/bin/env python3
"""
TEMP-1: Длительность ремонта — агент проводит в repair ровно repair_time дней.

Логика:
- Day0 repair agents (pre_status_id=4 на min_day):
  фактическая длительность ≈ remaining_repair (repair_time - repair_days) (± tolerance).
  repair_days в heli_pandas = уже прошедшее время, remaining = оставшийся ремонт.
- Runtime repair exits (прочие выходы из repair):
  repair_days на выходе должен быть близок к 0 (<= tolerance).
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
    parser = argparse.ArgumentParser(description="TEMP-1: repair duration")
    parser.add_argument("--version-id", required=True, type=int)
    parser.add_argument("--version-date", type=int, default=None)
    parser.add_argument("--table", default="sim_masterv2_v9")
    parser.add_argument("--tolerance", type=int, default=15,
                        help="Допуск в днях для adaptive steps (default=15)")
    args = parser.parse_args()
    table = validate_table_name(args.table)
    client = get_client()

    vd_filter = ""
    params = {"vid": args.version_id}
    if args.version_date is not None:
        vd_filter = " AND version_date = %(vdate)s"
        params["vdate"] = args.version_date

    # 1) min_day
    min_day_query = f"""
    SELECT min(day_u16)
    FROM {table}
    WHERE version_id = %(vid)s{vd_filter}
      AND group_by IN (1, 2)
    """
    min_day = client.execute(min_day_query, params)[0][0]
    if min_day is None:
        details = [
            f"tolerance={args.tolerance} days",
            "no_data=1 (min_day is NULL)",
        ]
        print_result("TEMP-1 repair duration", True, details)
        return 0

    # 2) Day0 repair agents: ещё в ремонте на min_day (pre_status=4 AND status=4)
    #    Агенты с pre_status=4, status!=4 уже вышли на day 0 — пропускаем
    day0_query = f"""
    SELECT version_date, aircraft_number, group_by, repair_days, repair_time
    FROM {table}
    WHERE version_id = %(vid)s{vd_filter}
      AND group_by IN (1, 2)
      AND day_u16 = %(min_day)s
      AND pre_status_id = 4
      AND status_id = 4
    """
    day0_rows = client.execute(day0_query, {**params, "min_day": min_day})
    day0_set = {(int(r[0]), int(r[1]), int(r[2])) for r in day0_rows}
    # remaining_repair = repair_time - repair_days (оставшийся ремонт)
    day0_remaining = {
        (int(r[0]), int(r[1]), int(r[2])): max(0, int(r[4]) - int(r[3]))
        for r in day0_rows
    }

    # 3) Все выходы из repair
    all_exits_query = f"""
    SELECT version_date, aircraft_number, group_by, day_u16, repair_days, repair_time, status_id
    FROM {table}
    WHERE version_id = %(vid)s{vd_filter}
      AND group_by IN (1, 2)
      AND pre_status_id = 4
      AND status_id != 4
    ORDER BY version_date, aircraft_number, group_by, day_u16
    """
    all_exits = client.execute(all_exits_query, params)

    # 4) Классификация выходов
    tolerance = int(args.tolerance)
    day0_agents = len(day0_remaining)
    day0_exits = 0
    day0_violations = 0
    runtime_exits = 0
    runtime_violations = 0
    day0_samples = []
    runtime_samples = []

    for vdate, acn, gb, day, rd, rt, st in all_exits:
        key = (int(vdate), int(acn), int(gb))
        if key in day0_set:
            day0_exits += 1
            remaining = int(day0_remaining[key])
            actual_duration = int(day) - int(min_day)
            if abs(actual_duration - remaining) > tolerance:
                day0_violations += 1
                if len(day0_samples) < 5:
                    day0_samples.append(
                        f"  version_date={vdate}, acn={acn}, gb={gb}, exit_day={day}, "
                        f"remaining_repair={remaining}, actual_duration={actual_duration}, "
                        f"new_status={st}"
                    )
            # Следующие выходы считаем runtime
            day0_set.discard(key)
        else:
            runtime_exits += 1
            if int(rd) > tolerance:
                runtime_violations += 1
                if len(runtime_samples) < 5:
                    runtime_samples.append(
                        f"  version_date={vdate}, acn={acn}, gb={gb}, day={day}, repair_days={rd}, "
                        f"repair_time={rt}, new_status={st}"
                    )

    violations = day0_violations + runtime_violations
    details = [
        f"tolerance={tolerance} days",
        f"day0_agents={day0_agents}, day0_violations={day0_violations}",
        f"runtime_exits={runtime_exits}, runtime_violations={runtime_violations}",
        f"total_violations={violations}",
    ]

    if day0_violations > 0:
        details.append("day0 violations samples (acn, gb, exit_day, remaining, duration, new_status):")
        details.extend(day0_samples)

    if runtime_violations > 0:
        details.append("runtime violations samples (acn, gb, day, repair_days, repair_time, new_status):")
        details.extend(runtime_samples)

    passed = violations == 0
    print_result("TEMP-1 repair duration", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
