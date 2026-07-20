#!/usr/bin/env python3
"""
L2 INV-0c: co-spawn агрегатов при рождении планера.

Проверяет:
- каждый spawn-планер (acn >= 100000, pre_status_id=0) имеет ровно N записей
  в sim_l2_birth_v1 на день первого появления (N единообразен внутри planer_group_by);
- day-0 дефицитные борта (список acn) не получили synthetic births;
- PSN уникальны в рамках version_date/version_id.
"""
from __future__ import annotations

import argparse
import re
import sys

from ch_client import get_client

TABLE_RE = re.compile(r"^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$")

# Day-0 gb2 deficit (fullkit < 2 engines) — не покрываем co-spawn на старте.
DAY0_DEFICIT_ACNS = (
    22417,
    22419,
    22425,
    22428,
    22497,
    25751,
    27090,
)


def validate_table_name(table: str) -> str:
    if not TABLE_RE.match(table):
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
        description="L2 INV-0c: co-spawn births для spawn-планеров"
    )
    parser.add_argument("--version-date", required=True, type=int)
    parser.add_argument("--version-id", required=True, type=int)
    parser.add_argument("--table-main", default="sim_masterv2_v9")
    parser.add_argument("--table-birth", default="sim_l2_birth_v1")
    args = parser.parse_args()

    table_main = validate_table_name(args.table_main)
    table_birth = validate_table_name(args.table_birth)
    client = get_client()
    params = {"vd": args.version_date, "vid": args.version_id}

    spawn_rows = client.execute(
        f"""
        WITH first_day AS (
            SELECT
                aircraft_number,
                group_by AS planer_group_by,
                min(day_u16) AS birth_day
            FROM {table_main}
            WHERE version_date = %(vd)s
              AND version_id = %(vid)s
              AND aircraft_number >= 100000
              AND group_by IN (1, 2)
              AND pre_status_id = 0
            GROUP BY aircraft_number, planer_group_by
        )
        SELECT
            f.birth_day,
            f.aircraft_number,
            f.planer_group_by,
            count() AS birth_units
        FROM first_day AS f
        LEFT JOIN {table_birth} AS b
            ON b.version_date = %(vd)s
           AND b.version_id = %(vid)s
           AND b.birth_day = f.birth_day
           AND b.aircraft_number = f.aircraft_number
        GROUP BY f.birth_day, f.aircraft_number, f.planer_group_by
        ORDER BY f.birth_day, f.aircraft_number
        """,
        params,
    )

    if not spawn_rows:
        print_result(
            "L2 INV-0c",
            True,
            ["Нет spawn-планеров в sim_masterv2_v9 — проверка co-spawn пропущена (vacuous PASS)."],
        )
        return 0

    mismatches = []
    units_by_gb: dict[int, set[int]] = {1: set(), 2: set()}
    for birth_day, acn, pgb, birth_units in spawn_rows:
        birth_units = int(birth_units)
        if birth_units <= 0:
            mismatches.append(f"acn={acn} day={birth_day} gb={pgb}: нет births")
            continue
        units_by_gb[int(pgb)].add(birth_units)

    for pgb, counts in units_by_gb.items():
        if len(counts) > 1:
            mismatches.append(f"gb{pgb}: неоднородный BOM count {sorted(counts)}")

    dup_psn = client.execute(
        f"""
        SELECT psn, count() AS c
        FROM {table_birth}
        WHERE version_date = %(vd)s AND version_id = %(vid)s
        GROUP BY psn
        HAVING c > 1
        LIMIT 5
        """,
        params,
    )
    if dup_psn:
        mismatches.append(f"дубликаты PSN: {dup_psn}")

    deficit_hits = client.execute(
        f"""
        SELECT DISTINCT aircraft_number
        FROM {table_birth}
        WHERE version_date = %(vd)s
          AND version_id = %(vid)s
          AND aircraft_number IN %(acns)s
        """,
        {**params, "acns": DAY0_DEFICIT_ACNS},
    )
    if deficit_hits:
        mismatches.append(
            f"day-0 deficit борта получили births: {[int(r[0]) for r in deficit_hits]}"
        )

    spawn_count = len(spawn_rows)
    total_birth_rows = client.execute(
        f"""
        SELECT count()
        FROM {table_birth}
        WHERE version_date = %(vd)s AND version_id = %(vid)s
        """,
        params,
    )[0][0]

    details = [
        f"spawn-планеров: {spawn_count}",
        f"строк sim_l2_birth_v1: {int(total_birth_rows)}",
        f"units/planer gb1: {sorted(units_by_gb[1]) or 'n/a'}",
        f"units/planer gb2: {sorted(units_by_gb[2]) or 'n/a'}",
    ]
    if mismatches:
        details.extend(["--- mismatches ---", *mismatches])

    passed = not mismatches
    print_result("L2 INV-0c", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
