#!/usr/bin/env python3
"""Batch: DWH load + flight_program (Excel из датасета) + sim + INV validation for multiple dates."""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import datetime, date
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
# Интерпретатор для симуляции: на этой машине FLAME GPU работает на системном python3
# (CUDA/окружение задаются через config/load_env.sh, наследуются текущим процессом).
CUDA_PYTHON = sys.executable
SOURCE_DATA_DIR = "data_input/source_data"
_DATASET_RE = re.compile(r"^v_(\d{4}-\d{2}-\d{2})$")


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


def load_flight_program(vd: str, vi: int, log: Path) -> None:
    """Генерирует flight_program_ac (Program_heli.xlsx) и flight_program_fl (Program.xlsx)
    из ближайшего датасета напрямую, с день_0 = vd. Загрузчики идемпотентны (rewrite по version_date).
    Порядок важен: сначала AC (fl читает new_counter_mi17 из последней версии AC)."""
    dataset = find_nearest_dataset(vd)
    for script in ("code/extract/program_ac_direct_loader.py", "code/extract/program_fl_direct_loader.py"):
        cmd = [
            sys.executable,
            script,
            "--version-date",
            vd,
            "--version-id",
            str(vi),
            "--dataset-path",
            str(dataset),
        ]
        rc = _run(cmd, log=log)
        if rc != 0:
            raise SystemExit(f"flight_program load failed ({script}) {vd} rc={rc}")
    client = get_clickhouse_client()
    print(
        f"  flight_program {vd}: ac={_count(client, 'flight_program_ac', vd, vi)} "
        f"fl={_count(client, 'flight_program_fl', vd, vi)} rows (из {dataset.name})"
    )


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

        print("  [1] load (DWH heli_pandas/program_ac/status)")
        ensure_load(vd, 1, log)
        print("  [2] flight_program (Excel из ближайшего датасета)")
        load_flight_program(vd, 1, log)
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
