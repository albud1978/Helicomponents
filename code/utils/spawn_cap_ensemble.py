#!/usr/bin/env python3
"""B1 CUDAEnsemble spike for LIMITER V8 per-run init/export isolation."""

import argparse
import hashlib
import os
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MESSAGING_DIR = PROJECT_ROOT / "code" / "sim_v2" / "messaging"
sys.path.insert(0, str(MESSAGING_DIR))

import pyflamegpu as fg
from orchestrator_limiter_v8 import LimiterV8Orchestrator
from rtc_mp2_export import SPIKEA_BUF_SIZE, SPIKEA_EXPORT_DIR, SPIKEA_FIELDS


def _build_orchestrator(args: argparse.Namespace) -> LimiterV8Orchestrator:
    orchestrator = LimiterV8Orchestrator(
        version_date=args.version_date,
        end_day=args.end_day,
        enable_mp2=True,
        clickhouse_client=None,
        version_id=args.version_ids[0],
        input_version_id=args.input_version_id,
    )
    orchestrator.prepare_data()
    orchestrator.build_model()
    return orchestrator


def _make_runs(model: fg.ModelDescription, version_ids: list[int], max_steps: int) -> fg.RunPlanVector:
    runs = fg.RunPlanVector(model, len(version_ids))
    for i, version_id in enumerate(version_ids):
        run = runs[i]
        run.setSteps(max_steps)
        run.setPropertyUInt("version_id", int(version_id))
        run.setPropertyUInt("ensemble_mode", 1)
        run.setPropertyUInt("ensemble_pop_inited", 0)
        run.setPropertyUInt("mp5_inited", 0)
        run.setPropertyUInt("spawnlim_inited", 0)
        run.setPropertyUInt("econ_inited", 0)
        run.setPropertyUInt("lines_inited", 0)
        run.setPropertyUInt("v8_inited", 0)
        run.setOutputSubdirectory(f"run_{version_id}")
    return runs


def _int_set(values: list[int]) -> fg.IntSet:
    result = fg.IntSet()
    for value in values:
        result.insert(int(value))
    return result


def _run_ensemble(model: fg.ModelDescription, args: argparse.Namespace, concurrent_runs: int) -> dict:
    runs = _make_runs(model, args.version_ids, args.max_steps)
    ensemble = fg.CUDAEnsemble(model)
    exit_log = fg.LoggingConfig(model)
    exit_log.logEnvironment("version_id")
    exit_log.logEnvironment("current_day")
    ensemble.setExitLog(exit_log)
    config = ensemble.Config()
    config.devices = _int_set(args.devices)
    config.concurrent_runs = int(concurrent_runs)
    config.out_directory = str(PROJECT_ROOT / "output" / "ensemble_b1" / f"logs_cr{concurrent_runs}")
    config.out_format = "json"
    config.truncate_log_files = True
    config.telemetry = False

    t0 = time.perf_counter()
    failures = ensemble.simulate(runs)
    wall = time.perf_counter() - t0
    if failures:
        raise RuntimeError(f"CUDAEnsemble failed runs: failures={failures}, concurrent_runs={concurrent_runs}")
    final_steps = {}
    final_days = {}
    for run_idx, run_log in ensemble.getLogs().items():
        frame = run_log.getExitLog()
        final_steps[int(run_idx)] = int(frame.getStepCount())
        final_days[int(run_idx)] = int(frame.getEnvironmentPropertyUInt("current_day"))
    return {
        "concurrent_runs": concurrent_runs,
        "wall_s": wall,
        "ensemble_elapsed_s": float(ensemble.getEnsembleElapsedTime()),
        "final_steps": final_steps,
        "final_days": final_days,
    }


def _file_info(path: Path) -> dict:
    data = path.read_bytes()
    return {
        "path": str(path),
        "size": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }


def _validate_spike_files(version_ids: list[int]) -> list[dict]:
    expected_size = SPIKEA_BUF_SIZE * 4
    rows = []
    by_field = {field: [] for field in SPIKEA_FIELDS}
    for version_id in version_ids:
        for field in SPIKEA_FIELDS:
            path = Path(SPIKEA_EXPORT_DIR) / f"run_{version_id}_{field}.bin"
            if not path.is_file():
                raise RuntimeError(f"Missing SpikeA export: {path}")
            info = _file_info(path)
            if info["size"] != expected_size:
                raise RuntimeError(
                    f"Unexpected SpikeA size for {path}: {info['size']} != {expected_size}"
                )
            rows.append(info)
            by_field[field].append(info)

    for field, infos in by_field.items():
        hashes = {info["sha256"] for info in infos}
        if len(hashes) != 1:
            raise RuntimeError(f"SpikeA field differs across identical runs: {field}, hashes={hashes}")
    return rows


def _nvidia_smi_snapshot() -> str:
    cmd = [
        "nvidia-smi",
        "--query-gpu=utilization.gpu,memory.used",
        "--format=csv,noheader",
    ]
    return subprocess.check_output(cmd, text=True).strip()


def _run_ensemble_with_smi(model: fg.ModelDescription, args: argparse.Namespace,
                           concurrent_runs: int) -> tuple[dict, str]:
    smi_cmd = [
        "nvidia-smi",
        "--query-gpu=utilization.gpu,memory.used",
        "--format=csv,noheader",
        "-lms",
        "1000",
    ]
    monitor = subprocess.Popen(
        smi_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        result = _run_ensemble(model, args, concurrent_runs)
    finally:
        monitor.terminate()
    smi_output, _ = monitor.communicate(timeout=5)
    return result, smi_output.strip()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="B1 CUDAEnsemble spike for LIMITER V8")
    parser.add_argument("--version-date", required=True, help="Input dataset date YYYY-MM-DD")
    parser.add_argument("--input-version-id", type=int, default=3, help="Source input version_id")
    parser.add_argument("--version-ids", type=int, nargs=2, default=[850, 851], help="Disposable output version_ids")
    parser.add_argument("--end-day", type=int, default=3650)
    parser.add_argument("--max-steps", type=int, default=10000)
    parser.add_argument("--devices", type=int, nargs="+", default=[0])
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    fg.Telemetry.disable()

    orchestrator = _build_orchestrator(args)
    model = orchestrator.model

    print("\n=== CUDAEnsemble B1 sequential via concurrent_runs=1 ===")
    seq = _run_ensemble(model, args, concurrent_runs=1)
    seq_files = _validate_spike_files(args.version_ids)

    print("\n=== CUDAEnsemble B1 concurrent via concurrent_runs=2 ===")
    before_smi = _nvidia_smi_snapshot()
    conc, smi_live = _run_ensemble_with_smi(model, args, concurrent_runs=2)
    after_smi = _nvidia_smi_snapshot()
    conc_files = _validate_spike_files(args.version_ids)

    ratio = conc["wall_s"] / seq["wall_s"] if seq["wall_s"] > 0 else 0.0
    print("\n=== B1 RESULT ===")
    print(f"concurrent_runs=1 wall_s={seq['wall_s']:.3f} ensemble_elapsed_s={seq['ensemble_elapsed_s']:.3f}")
    print(f"concurrent_runs=1 final_steps={seq['final_steps']} final_days={seq['final_days']}")
    print(f"concurrent_runs=2 wall_s={conc['wall_s']:.3f} ensemble_elapsed_s={conc['ensemble_elapsed_s']:.3f}")
    print(f"concurrent_runs=2 final_steps={conc['final_steps']} final_days={conc['final_days']}")
    print(f"ratio concurrent/sequential={ratio:.3f}")
    print(f"nvidia_smi_before={before_smi}")
    print("nvidia_smi_live_begin")
    print(smi_live)
    print("nvidia_smi_live_end")
    print(f"nvidia_smi_after={after_smi}")
    for info in conc_files:
        print(f"spike_file path={info['path']} size={info['size']} sha256={info['sha256']}")
    print(f"validated_initial_files={len(seq_files)} validated_concurrent_files={len(conc_files)}")


if __name__ == "__main__":
    main()
