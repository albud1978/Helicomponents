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
    ("INV-3", "inv3_repair_limit.py"),
    ("INV-4", "inv4_unsvc_min_repair.py"),
    ("INV-5", "inv5_sne_balance.py"),
    ("INV-6", "inv6_dt_outside_ops.py"),
    ("INV-8", "inv8_storage_frozen.py"),
    ("INV-9", "inv9_limiter_zero_exit.py"),
    ("INV-10", "inv10_turnover_balance.py"),
    ("TEMP-1", "temp1_repair_duration.py"),
    ("TEMP-4", "temp4_liveness.py"),
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Запуск всех инвариантов (SQL-first)")
    parser.add_argument("--version-id", required=True, type=int, help="version_id")
    parser.add_argument(
        "--table",
        default="sim_masterv2_v9",
        help="Таблица ClickHouse (по умолчанию: sim_masterv2_v9)",
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
        result = subprocess.run(
            [
                sys.executable,
                script_path,
                "--version-id",
                str(args.version_id),
                "--table",
                args.table,
            ],
            check=False,
        )
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
