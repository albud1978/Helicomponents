#!/usr/bin/env python3
"""Batch: DWH load + flight_program clone + sim + INV validation for multiple dates."""
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CODE_ROOT / "utils"))

from config_loader import get_clickhouse_client

DATES_DEFAULT = [
    "2026-04-15",
    "2026-05-01",
    "2026-05-20",
    "2026-06-05",
    "2026-06-11",
]
FP_SRC = "2026-04-08"
CUDA_PYTHON = "/home/albud/miniconda3/envs/cuda13/bin/python3"


def _count(client, table: str, vd: str, vi: int = 1) -> int:
    if table.startswith("sim_"):
        vd_int = int(vd.replace("-", ""))
        return int(
            client.execute(
                f"SELECT count() FROM {table} WHERE version_date=%(vd)s AND version_id=%(vi)s",
                {"vd": vd_int, "vi": vi},
            )[0][0]
        )
    return int(
        client.execute(
            f"SELECT count() FROM {table} WHERE version_date=toDate(%(vd)s) AND version_id=%(vi)s",
            {"vd": vd, "vi": vi},
        )[0][0]
    )


def _ops_enriched(client, vd: str, vi: int = 1) -> int:
    return int(
        client.execute(
            """
            SELECT count()
            FROM heli_pandas
            WHERE version_date = toDate(%(vd)s) AND version_id = %(vi)s
              AND toUInt8(ifNull(status_id, 0)) > 0
            """,
            {"vd": vd, "vi": vi},
        )[0][0]
    )


REPO = Path(__file__).resolve().parents[2]


def _run(cmd: list[str], *, log: Path) -> int:
    with log.open("a", encoding="utf-8") as f:
        f.write(f"\n{'='*60}\n$ {' '.join(cmd)}\n")
        f.flush()
        p = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, cwd=REPO)
    return p.returncode


def ensure_load(vd: str, vi: int, log: Path) -> None:
    client = get_clickhouse_client()
    if _ops_enriched(client, vd, vi) > 1000:
        print(f"  load skip: heli_pandas enriched for {vd}")
        return

    hp = _count(client, "heli_pandas", vd, vi)
    pac = _count(client, "program_ac", vd, vi)

    loader = [sys.executable, "code/utils/dwh_loader.py", "--report-date", vd, "--version-id", str(vi)]

    if pac == 0:
        steps = ["--step", "all"]
    elif hp == 0:
        steps = ["--step", "status_components", "--step", "enrich"]
    else:
        steps = ["--step", "enrich"]

    rc = _run(loader + steps + ["--skip-existing"], log=log)
    if rc != 0:
        raise SystemExit(f"load failed {vd} rc={rc}")


def clone_flight_program(vd: str, vi: int = 1) -> None:
    client = get_clickhouse_client()
    for t, cols in (
        (
            "flight_program_fl",
            "aircraft_number, dates, daily_hours, ac_type_mask, version_date, version_id",
        ),
        (
            "flight_program_ac",
            "dates, ops_counter_mi8, ops_counter_mi17, trigger_program_mi8, trigger_program_mi17, new_counter_mi17, version_date, version_id",
        ),
    ):
        n = _count(client, t, vd, vi)
        if n:
            print(f"  {t} {vd}: already {n} rows")
            continue
        sel_cols = cols.replace(", version_date, version_id", "")
        client.execute(
            f"""
            INSERT INTO {t} ({cols})
            SELECT {sel_cols}, toDate(%(dst)s), version_id
            FROM {t}
            WHERE version_date=toDate(%(src)s) AND version_id=%(vi)s
            """,
            {"dst": vd, "src": FP_SRC, "vi": vi},
        )
        print(f"  {t} cloned {FP_SRC} -> {vd}: {_count(client, t, vd, vi)} rows")


def run_sim(vd: str, log: Path) -> int:
    import os

    env = os.environ.copy()
    env["CUDA_PATH"] = os.path.expanduser("~/miniconda3/targets/x86_64-linux")
    env["LD_LIBRARY_PATH"] = os.path.expanduser("~/miniconda3/lib") + ":" + env.get("LD_LIBRARY_PATH", "")
    cmd = [
        CUDA_PYTHON,
        "code/sim_v2/messaging/orchestrator_limiter_v8.py",
        "--version-date",
        vd,
        "--end-day",
        "3650",
        "--max-steps",
        "10000",
    ]
    with log.open("a", encoding="utf-8") as f:
        f.write(f"\n{'='*60}\n$ {' '.join(cmd)}\n")
        f.flush()
        p = subprocess.run(
            cmd,
            stdout=f,
            stderr=subprocess.STDOUT,
            cwd=REPO,
            env=env,
        )
    return p.returncode


def run_validation(vd: str, log: Path) -> tuple[int, str]:
    vd_int = vd.replace("-", "")
    cmd = [
        sys.executable,
        "code/validation/run_all.py",
        "--version-id",
        "1",
        "--version-date",
        vd_int,
        "--table-main",
        "sim_masterv2_v9",
        "--table-repair",
        "sim_repairline_v9",
    ]
    with log.open("a", encoding="utf-8") as f:
        f.write(f"\n{'='*60}\n$ {' '.join(cmd)}\n")
        f.flush()
        p = subprocess.run(
            cmd,
            stdout=f,
            stderr=subprocess.STDOUT,
            cwd=REPO,
        )
    text = log.read_text(encoding="utf-8")
    inv_lines = [ln for ln in text.splitlines() if ln.startswith("INV-") and ": PASS" in ln]
    inv_fail = [ln for ln in text.splitlines() if ln.startswith("INV-") and ": FAIL" in ln]
    if len(inv_lines) >= 12 and not inv_fail:
        return p.returncode, "PASS"
    return p.returncode, f"FAIL inv_fail={inv_fail[:3]}"


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dates", nargs="+", default=DATES_DEFAULT)
    p.add_argument("--skip-sim", action="store_true")
    p.add_argument("--out-dir", default="output/dwh_sim_batch")
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    results = []

    for vd in args.dates:
        print(f"\n=== {vd} ===")
        log = out_dir / f"sim_gate_{vd}.log"
        log.write_text(f"batch sim-gate {vd}\n", encoding="utf-8")

        print("  [1] flight_program")
        clone_flight_program(vd)
        print("  [2] load")
        ensure_load(vd, 1, log)
        if args.skip_sim:
            results.append((vd, "load-only", "OK"))
            continue

        print("  [3] sim")
        sim_rc = run_sim(vd, log)
        if sim_rc != 0:
            results.append((vd, "sim", f"FAIL rc={sim_rc}"))
            continue

        print("  [4] validation")
        val_rc, verdict = run_validation(vd, log)
        results.append((vd, "inv", verdict))

    print("\n=== BATCH SUMMARY ===")
    for row in results:
        print(f"  {row[0]}: {row[1]} -> {row[2]}")

    summary_path = out_dir / "summary.txt"
    summary_path.write_text(
        "\n".join(f"{a}\t{b}\t{c}" for a, b, c in results) + "\n",
        encoding="utf-8",
    )
    print(f"Written: {summary_path}")
    return 0 if all(r[2] == "PASS" or r[1] == "load-only" for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
