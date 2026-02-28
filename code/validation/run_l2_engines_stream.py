#!/usr/bin/env python3
"""
Потоковый раннер L2-валидаций engines (group_by 3/4).
"""
import argparse
import os
import re
import subprocess
import sys
from typing import List, Tuple


TABLE_RE = re.compile(r"^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$")


def validate_table_name(table: str) -> str:
    if not TABLE_RE.match(table):
        raise SystemExit(f"Некорректное имя таблицы: {table}")
    return table


def stream_process(cmd: List[str]) -> int:
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    )
    if process.stdout is not None:
        for line in process.stdout:
            print(line, end="")
    return process.wait()


def run_validator(
    script_path: str,
    planner_version_date: int,
    units_version_date_int: int,
    version_id: int,
    table_main: str,
    table_units: str,
) -> int:
    script_name = os.path.basename(script_path)
    print("\n" + "=" * 80)
    print(f"RUN {script_name}")
    print("=" * 80)
    cmd = [
        sys.executable,
        script_path,
        "--planner-version-date",
        str(planner_version_date),
        "--units-version-date-int",
        str(units_version_date_int),
        "--version-id",
        str(version_id),
        "--table-main",
        table_main,
        "--table-units",
        table_units,
    ]
    return stream_process(cmd)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Потоковый запуск L2 engines валидаторов"
    )
    parser.add_argument("--planner-version-date", required=True, type=int)
    parser.add_argument("--units-version-date-int", required=True, type=int)
    parser.add_argument("--version-id", required=True, type=int)
    parser.add_argument("--table-main", default="sim_masterv2_v9")
    parser.add_argument("--table-units", default="sim_units_v2")
    args = parser.parse_args()

    table_main = validate_table_name(args.table_main)
    table_units = validate_table_name(args.table_units)

    base_dir = os.path.dirname(os.path.abspath(__file__))
    scripts = [
        "l2_inv_scope_engines_3_4_only.py",
        "l2_inv0a_engine_ops_requires_planner_ops.py",
        "l2_inv0b_planner_ops_full_engine_set.py",
        "l2_inv1_sne_le_ll.py",
        "l2_inv7_sne_dt_consistency.py",
        "l2_inv8_storage_frozen.py",
        "l2_temp1_repair_duration.py",
        "l2_temp4_no_infinite_repair.py",
        "l2_inv17_allowed_transitions.py",
        "l2_inv10_turnover_balance.py",
    ]

    script_paths: List[str] = []
    missing = []
    for name in scripts:
        path = os.path.join(base_dir, name)
        if not os.path.exists(path):
            missing.append(name)
        script_paths.append(path)

    if missing:
        print("Не найдены файлы валидаторов:")
        for name in missing:
            print(f"  - {name}")
        return 2

    results: List[Tuple[str, int]] = []
    failed = 0

    for script_path in script_paths:
        rc = run_validator(
            script_path=script_path,
            planner_version_date=args.planner_version_date,
            units_version_date_int=args.units_version_date_int,
            version_id=args.version_id,
            table_main=table_main,
            table_units=table_units,
        )
        results.append((os.path.basename(script_path), rc))
        if rc != 0:
            failed += 1

    print("\n" + "=" * 80)
    print("SUMMARY")
    for script_name, rc in results:
        status = "PASS" if rc == 0 else "FAIL"
        print(f"  {script_name}: {status} (rc={rc})")
    print(f"\nTOTAL={len(results)} FAILED={failed}")
    print("=" * 80)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
