#!/usr/bin/env python3
"""Batch: extract_master DWH day0 + sim + INV validation for multiple dates."""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import datetime, date
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CODE_ROOT / "utils"))

DATES_DEFAULT = [
    "2026-04-15",
    "2026-05-01",
    "2026-05-20",
    "2026-06-05",
    "2026-06-11",
]
CUDA_PYTHON = (
    "/home/albud/miniconda3/envs/cuda13_nosb/bin/python3"
    if Path("/home/albud/miniconda3/envs/cuda13_nosb/bin/python3").exists()
    else sys.executable
)
SOURCE_DATA_DIR = "data_input/source_data"
_DATASET_RE = re.compile(r"^v_(\d{4}-\d{2}-\d{2})$")


REPO = Path(__file__).resolve().parents[2]


def _run(cmd: list[str], *, log: Path) -> int:
    with log.open("a", encoding="utf-8") as f:
        f.write(f"\n{'='*60}\n$ {' '.join(cmd)}\n")
        f.flush()
        p = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, cwd=REPO)
    return p.returncode


def find_nearest_dataset(vd: str) -> Path:
    """Возвращает папку датасета v_YYYY-MM-DD, ближайшую к дате vd.

    Берём строго именованные папки (v_YYYY-MM-DD без суффиксов asof/policy),
    выбираем минимальную |date - vd|; при равенстве — более позднюю дату.
    Требуем наличие Program_heli.xlsx и Program.xlsx в выбранной папке.
    """
    target = datetime.strptime(vd, "%Y-%m-%d").date()
    root = REPO / SOURCE_DATA_DIR
    candidates: list[tuple[int, date, Path]] = []
    for p in root.glob("v_*"):
        if not p.is_dir():
            continue
        m = _DATASET_RE.match(p.name)
        if not m:
            continue
        d = datetime.strptime(m.group(1), "%Y-%m-%d").date()
        if not (p / "Program_heli.xlsx").exists() or not (p / "Program.xlsx").exists():
            continue
        candidates.append((abs((d - target).days), d, p))
    if not candidates:
        raise SystemExit(f"find_nearest_dataset: нет подходящих датасетов в {root} для {vd}")
    # min по расстоянию, при равенстве — большая дата (свежее)
    candidates.sort(key=lambda x: (x[0], -x[1].toordinal()))
    chosen = candidates[0]
    print(f"  nearest dataset for {vd}: {chosen[2].name} (Δ={chosen[0]} дн.)")
    return chosen[2]


def run_extract_master(vd: str, vi: int, log: Path) -> None:
    """Готовит day0-срез через единый extract_master."""
    dataset = find_nearest_dataset(vd)
    cmd = [
        sys.executable,
        "code/extract/extract_master.py",
        "--source",
        "dwh",
        "--mode",
        "prod",
        "--version-date",
        vd,
        "--version-id",
        str(vi),
        "--dataset-path",
        str(dataset),
    ]
    rc = _run(cmd, log=log)
    if rc != 0:
        raise SystemExit(f"extract_master failed {vd} rc={rc}")


def run_sim(vd: str, log: Path) -> int:
    import os

    # Наследуем окружение текущего процесса (CUDA/LD_LIBRARY_PATH из config/load_env.sh).
    env = os.environ.copy()
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

        print("  [1] extract_master (DWH + flight_program + demote)")
        run_extract_master(vd, 1, log)
        if args.skip_sim:
            results.append((vd, "load-only", "OK"))
            continue

        print("  [2] sim")
        sim_rc = run_sim(vd, log)
        if sim_rc != 0:
            results.append((vd, "sim", f"FAIL rc={sim_rc}"))
            continue

        print("  [3] validation")
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
