#!/usr/bin/env python3
"""B2 CUDAEnsemble full MP2 export harness for LIMITER V8."""

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
from model_build import MAX_EXPORT_STEPS, MAX_FRAMES, MP2_BUF_SIZE, RL_BUF_SIZE
from rtc_mp2_export import (
    MP2_DYNAMIC_FIELDS,
    MP2_EXPORT_DIR,
    MP2_FIELDS,
    MP2_STATIC_FIELDS,
)
from rtc_repairline_export import RL_EXPORT_FIELDS


def _build_orchestrator(args: argparse.Namespace) -> LimiterV8Orchestrator:
    orchestrator = LimiterV8Orchestrator(
        version_date=args.version_date,
        end_day=args.end_day,
        enable_mp2=True,
        clickhouse_client=None,
        version_id=args.version_ids[0],
        input_version_id=args.input_version_id,
        ensemble_mode=True,
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
    config.out_directory = str(PROJECT_ROOT / "output" / "ensemble_b2" / f"logs_cr{concurrent_runs}")
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


def _expected_mp2_size(field: str) -> int:
    if field in MP2_DYNAMIC_FIELDS:
        return MP2_BUF_SIZE * 4
    if field in MP2_STATIC_FIELDS:
        return MAX_FRAMES * 4
    if field == "mp2_day_for_step":
        return MAX_EXPORT_STEPS * 4
    if field == "mp2_num_steps":
        return 2 * 4
    raise RuntimeError(f"Unknown MP2 export field: {field}")


def _expected_export_size(field: str) -> int:
    if field in RL_EXPORT_FIELDS:
        return RL_BUF_SIZE * 4
    return _expected_mp2_size(field)


def _validate_export_files(version_ids: list[int]) -> list[dict]:
    rows = []
    by_field = {
        field: []
        for field in [*MP2_FIELDS, "mp2_day_for_step", "mp2_num_steps", *RL_EXPORT_FIELDS]
    }
    for version_id in version_ids:
        for field in by_field:
            path = Path(MP2_EXPORT_DIR) / f"run_{version_id}_{field}.bin"
            if not path.is_file():
                raise RuntimeError(f"Missing ensemble export: {path}")
            info = _file_info(path)
            expected_size = _expected_export_size(field)
            if info["size"] != expected_size:
                raise RuntimeError(
                    f"Unexpected ensemble export size for {path}: {info['size']} != {expected_size}"
                )
            rows.append(info)
            by_field[field].append(info)

    for field, infos in by_field.items():
        hashes = {info["sha256"] for info in infos}
        if len(hashes) != 1:
            raise RuntimeError(f"Ensemble field differs across identical runs: {field}, hashes={hashes}")
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
    parser = argparse.ArgumentParser(description="B2 CUDAEnsemble full MP2 export for LIMITER V8")
    parser.add_argument("--version-date", required=True, help="Input dataset date YYYY-MM-DD")
    parser.add_argument("--input-version-id", type=int, default=3, help="Source input version_id")
    parser.add_argument("--version-ids", type=int, nargs="+", default=[860], help="Disposable output version_ids")
    parser.add_argument("--end-day", type=int, default=3650)
    parser.add_argument("--max-steps", type=int, default=10000)
    parser.add_argument("--devices", type=int, nargs="+", default=[0])
    parser.add_argument("--concurrent-runs", type=int, nargs="+", default=[1])
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    fg.Telemetry.disable()

    orchestrator = _build_orchestrator(args)
    model = orchestrator.model
    repair_quota = int(orchestrator.repair_quota)

    results = []
    before_smi = _nvidia_smi_snapshot()
    for concurrent_runs in args.concurrent_runs:
        print(f"\n=== CUDAEnsemble B2 concurrent_runs={concurrent_runs} ===")
        result, smi_live = _run_ensemble_with_smi(model, args, concurrent_runs)
        files = _validate_export_files(args.version_ids)
        results.append((result, smi_live, files))

    after_smi = _nvidia_smi_snapshot()
    print("\n=== B2 RESULT ===")
    for result, smi_live, files in results:
        cr = result["concurrent_runs"]
        print(f"concurrent_runs={cr} wall_s={result['wall_s']:.3f} ensemble_elapsed_s={result['ensemble_elapsed_s']:.3f}")
        print(f"concurrent_runs={cr} final_steps={result['final_steps']} final_days={result['final_days']}")
        print(f"nvidia_smi_live_cr{cr}_begin")
        print(smi_live)
        print(f"nvidia_smi_live_cr{cr}_end")
    print(f"nvidia_smi_before={before_smi}")
    print(f"nvidia_smi_after={after_smi}")
    print(f"repair_quota={repair_quota}")
    print(
        "loader_repair_quota_arg="
        f"--repair-quota {repair_quota}"
    )
    for info in results[-1][2]:
        print(f"ensemble_file path={info['path']} size={info['size']} sha256={info['sha256']}")
    print(f"validated_ensemble_files={len(results[-1][2])}")


if __name__ == "__main__":
    main()
