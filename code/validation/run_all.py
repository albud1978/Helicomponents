#!/usr/bin/env python3
"""
Мастер-скрипт: запуск всех проверок инвариантов.
"""
import argparse
import os
import subprocess
import sys


SCRIPTS = [
    ("INV-1", "inv1_sne_le_ll.py"),
    ("INV-2", "inv2_ops_vs_target.py"),
    ("INV-3", "inv3_repair_capacity.py"),
    ("INV-4", "inv4_unsvc_repair_time.py"),
    ("INV-5", "inv5_balance_increments.py"),
    ("INV-6", "inv6_dt_only_ops.py"),
    ("INV-7", "inv7_dt_eq_mp5.py"),
    ("INV-8", "inv8_storage_frozen.py"),
    ("INV-9", "inv9_limiter_exit.py"),
    ("INV-10", "inv10_turnover_balance.py"),
    ("TEMP-1", "temp1_repair_duration.py"),
    ("TEMP-4", "temp4_no_infinite_repair.py"),
    ("TEMP-5", "temp5_repair_hybrid_vector.py"),
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Запуск всех инвариантов (SQL-first)")
    parser.add_argument("--version-id", required=True, type=int, help="version_id")
    parser.add_argument(
        "--version-date",
        type=int,
        default=None,
        help="version_date (YYYYMMDD) для фильтрации",
    )
    parser.add_argument(
        "--table-main",
        default="sim_masterv2_v9",
        help="Основная таблица ClickHouse (по умолчанию: sim_masterv2_v9)",
    )
    parser.add_argument(
        "--table-repair",
        default="sim_repairline_v9",
        help="Таблица RepairLine (по умолчанию: sim_repairline_v9)",
    )
    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    total = 0
    failed = 0
    results = []

    for name, script in SCRIPTS:
        total += 1
        script_path = os.path.join(base_dir, script)
        print("\n" + "=" * 80)
        print(f"RUN {name} -> {script}")
        print("=" * 80)
        cmd = [
            sys.executable,
            script_path,
            "--version-id",
            str(args.version_id),
        ]
        if args.version_date is not None:
            cmd.extend(["--version-date", str(args.version_date)])
        if script == "inv3_repair_capacity.py":
            cmd.extend(["--table", args.table_repair])
        elif script == "temp5_repair_hybrid_vector.py":
            cmd.extend(["--table-main", args.table_main, "--table-repair", args.table_repair])
        else:
            cmd.extend(["--table", args.table_main])
        result = subprocess.run(cmd, check=False)
        ok = result.returncode == 0
        results.append((name, ok))
        if not ok:
            failed += 1

    print("\n" + "=" * 80)
    print("SUMMARY")
    for name, ok in results:
        status = "PASS" if ok else "FAIL"
        print(f"{name}: {status}")
    print(f"TOTAL={total} FAIL={failed}")
    print("=" * 80)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
