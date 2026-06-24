#!/usr/bin/env python3
"""
Мастер-скрипт: запуск всех проверок инвариантов.
"""
import argparse
import sys

from ch_client import get_client
from inv1_sne_le_ll import run as run_inv1
from inv2_ops_vs_target import run as run_inv2
from inv3_repair_capacity import run as run_inv3
from inv4_unsvc_repair_time import run as run_inv4
from inv5_balance_increments import run as run_inv5
from inv6_dt_only_ops import run as run_inv6
from inv7_dt_eq_mp5 import run as run_inv7
from inv8_storage_frozen import run as run_inv8
from inv9_limiter_exit import run as run_inv9
from inv10_turnover_balance import run as run_inv10
from inv11_spawn_limit_saturation import run as run_inv11
from inv12_ppr_le_oh import run as run_inv12
from inv13_spawn_limit_cumulative import run as run_inv13
from temp1_repair_duration import run as run_temp1
from temp4_no_infinite_repair import run as run_temp4
from temp5_repair_hybrid_vector import run as run_temp5


SCRIPTS = [
    ("INV-1", "inv1_sne_le_ll.py", run_inv1),
    ("INV-2", "inv2_ops_vs_target.py", run_inv2),
    ("INV-3", "inv3_repair_capacity.py", run_inv3),
    ("INV-4", "inv4_unsvc_repair_time.py", run_inv4),
    ("INV-5", "inv5_balance_increments.py", run_inv5),
    ("INV-6", "inv6_dt_only_ops.py", run_inv6),
    ("INV-7", "inv7_dt_eq_mp5.py", run_inv7),
    ("INV-8", "inv8_storage_frozen.py", run_inv8),
    ("INV-9", "inv9_limiter_exit.py", run_inv9),
    ("INV-10", "inv10_turnover_balance.py", run_inv10),
    ("INV-11", "inv11_spawn_limit_saturation.py", run_inv11),
    ("INV-12", "inv12_ppr_le_oh.py", run_inv12),
    ("INV-13", "inv13_spawn_limit_cumulative.py", run_inv13),
    ("TEMP-1", "temp1_repair_duration.py", run_temp1),
    ("TEMP-4", "temp4_no_infinite_repair.py", run_temp4),
    ("TEMP-5", "temp5_repair_hybrid_vector.py", run_temp5),
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

    client = get_client()
    total = 0
    failed = 0
    results = []

    for name, script, runner in SCRIPTS:
        total += 1
        print("\n" + "=" * 80)
        print(f"RUN {name} -> {script}")
        print("=" * 80)
        if script == "inv3_repair_capacity.py":
            ok = runner(
                client,
                args.version_id,
                args.version_date,
                args.table_repair,
            )
        elif script == "temp5_repair_hybrid_vector.py":
            ok = runner(
                client,
                args.version_id,
                args.version_date,
                args.table_main,
                args.table_repair,
            )
        else:
            ok = runner(
                client,
                args.version_id,
                args.version_date,
                args.table_main,
            )
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
