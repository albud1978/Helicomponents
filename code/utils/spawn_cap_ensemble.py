#!/usr/bin/env python3
"""B3b CUDAEnsemble spawn cap sweep harness for LIMITER V8."""

from __future__ import annotations

import argparse
import hashlib
import subprocess
import sys
import time
from datetime import date
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CODE_ROOT = PROJECT_ROOT / "code"
MESSAGING_DIR = CODE_ROOT / "sim_v2" / "messaging"
for path in (CODE_ROOT, CODE_ROOT / "utils", MESSAGING_DIR):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

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
from sim_env_setup import get_client
from spawn_cap_curve import get_reference_shape, synth_cumulative, synth_seed


@dataclass(frozen=True)
class RunSpec:
    version_id: int
    cap_n: int
    spawn_limit_active: int
    label: str


def _run_specs(args: argparse.Namespace) -> list[RunSpec]:
    caps = [int(cap) for cap in args.caps]
    if not caps:
        raise ValueError("caps list must not be empty")
    if len(set(caps)) != len(caps):
        raise ValueError(f"Duplicate caps are not allowed: {caps}")
    invalid_caps = [cap for cap in caps if cap < 0 or cap > 60]
    if invalid_caps:
        raise ValueError(f"B3b caps must be strictly within 0..60, got {invalid_caps}")
    if 61 in caps:
        raise ValueError("cap=61 is reserved for uncapped vid1061 and must not be in caps")

    specs = [
        RunSpec(
            version_id=int(args.out_base) + int(cap),
            cap_n=int(cap),
            spawn_limit_active=1,
            label=f"cap{int(cap)}",
        )
        for cap in caps
    ]
    if not args.skip_uncapped:
        specs.append(
            RunSpec(
                version_id=int(args.uncapped_version_id),
                cap_n=0,
                spawn_limit_active=0,
                label="uncapped",
            )
        )
    seen = set()
    for spec in specs:
        if spec.version_id in seen:
            raise ValueError(f"Duplicate output version_id in B3b run specs: {spec.version_id}")
        seen.add(spec.version_id)
    return specs


def _build_orchestrator(args: argparse.Namespace) -> tuple[LimiterV8Orchestrator, dict[str, int]]:
    orchestrator = LimiterV8Orchestrator(
        version_date=args.version_date,
        end_day=args.end_day,
        enable_mp2=True,
        clickhouse_client=None,
        version_id=int(args.out_base),
        input_version_id=args.input_version_id,
        ensemble_mode=True,
    )
    orchestrator.prepare_data()

    client = get_client()
    threshold, days_total = get_reference_shape(
        client,
        args.version_date,
        ref_vid=args.reference_version_id,
    )
    env_days = int(orchestrator.env_data["days_total_u16"])
    if days_total != env_days:
        raise RuntimeError(
            "reference spawn_limit shape days_total mismatch: "
            f"ref={days_total}, input={env_days}"
        )

    # B3a disables planned Mi-17 deliveries globally; dynamic spawn is tested alone.
    orchestrator.env_data["mp4_new_counter_mi17_seed"] = [0] * days_total
    orchestrator._deterministic_spawn_count = 0

    # Ensemble per-run cap injection needs only the reference step threshold.
    orchestrator.env_data["spawn_limit_cumulative"] = synth_cumulative(threshold, days_total, 1)
    orchestrator.env_data["mp4_spawn_limit_seed"] = synth_seed(threshold, days_total, 1)
    orchestrator.env_data["spawn_limit_active"] = 1
    orchestrator._collect_deterministic_dates()

    orchestrator.build_model()
    return orchestrator, {"threshold": threshold, "days_total": days_total}


def _make_runs(model: fg.ModelDescription, specs: list[RunSpec], max_steps: int) -> fg.RunPlanVector:
    runs = fg.RunPlanVector(model, len(specs))
    for i, spec in enumerate(specs):
        run = runs[i]
        run.setSteps(max_steps)
        run.setPropertyUInt("version_id", spec.version_id)
        run.setPropertyUInt("ensemble_mode", 1)
        run.setPropertyUInt("ensemble_pop_inited", 0)
        run.setPropertyUInt("mp5_inited", 0)
        run.setPropertyUInt("spawnlim_inited", 0)
        run.setPropertyUInt("econ_inited", 0)
        run.setPropertyUInt("lines_inited", 0)
        run.setPropertyUInt("v8_inited", 0)
        run.setPropertyUInt("spawn_cap_n", spec.cap_n)
        run.setPropertyUInt8("spawn_limit_active", spec.spawn_limit_active)
        run.setOutputSubdirectory(f"run_{spec.version_id}")
    return runs


def _int_set(values: list[int]) -> fg.IntSet:
    result = fg.IntSet()
    for value in values:
        result.insert(int(value))
    return result


def _run_ensemble(
    model: fg.ModelDescription,
    args: argparse.Namespace,
    specs: list[RunSpec],
    concurrent_runs: int,
) -> dict:
    runs = _make_runs(model, specs, args.max_steps)
    ensemble = fg.CUDAEnsemble(model)
    exit_log = fg.LoggingConfig(model)
    exit_log.logEnvironment("version_id")
    exit_log.logEnvironment("current_day")
    ensemble.setExitLog(exit_log)
    config = ensemble.Config()
    config.devices = _int_set(args.devices)
    config.concurrent_runs = int(concurrent_runs)
    config.out_directory = str(PROJECT_ROOT / "output" / "ensemble_b2" / f"logs_b3b_cr{concurrent_runs}")
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


def _validate_export_files(specs: list[RunSpec]) -> list[dict]:
    rows = []
    fields = [*MP2_FIELDS, "mp2_day_for_step", "mp2_num_steps", *RL_EXPORT_FIELDS]
    for spec in specs:
        for field in fields:
            path = Path(MP2_EXPORT_DIR) / f"run_{spec.version_id}_{field}.bin"
            if not path.is_file():
                raise RuntimeError(f"Missing ensemble export: {path}")
            info = _file_info(path)
            info["version_id"] = spec.version_id
            info["label"] = spec.label
            info["field"] = field
            expected_size = _expected_export_size(field)
            if info["size"] != expected_size:
                raise RuntimeError(
                    f"Unexpected ensemble export size for {path}: {info['size']} != {expected_size}"
                )
            rows.append(info)
    return rows


def _nvidia_smi_snapshot() -> str:
    cmd = [
        "nvidia-smi",
        "--query-gpu=utilization.gpu,memory.used",
        "--format=csv,noheader",
    ]
    return subprocess.check_output(cmd, text=True).strip()


def _run_ensemble_with_smi(model: fg.ModelDescription, args: argparse.Namespace, specs: list[RunSpec],
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
        result = _run_ensemble(model, args, specs, concurrent_runs)
    finally:
        monitor.terminate()
    smi_output, _ = monitor.communicate(timeout=5)
    return result, smi_output.strip()


def _version_date_int_arg(version_date: str) -> str:
    return date.fromisoformat(version_date).strftime("%Y%m%d")


def _load_exports(args: argparse.Namespace, specs: list[RunSpec], repair_quota: int) -> dict[str, str | float]:
    loader = PROJECT_ROOT / "code" / "utils" / "ensemble_mp2_loader.py"
    materializer = PROJECT_ROOT / "code" / "sim_v2" / "messaging" / "sim_daily_materializer.py"
    version_date_int = _version_date_int_arg(args.version_date)
    version_ids = [str(spec.version_id) for spec in specs]
    cmd = [
        sys.executable,
        str(loader),
        "--version-date",
        args.version_date,
        "--version-ids",
        *version_ids,
        "--baseline-version-id",
        str(args.input_version_id),
        "--repair-quota",
        str(repair_quota),
        "--parallel-workers",
        str(args.loader_workers),
    ]
    loader_started = time.perf_counter()
    completed = subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    loader_wall = time.perf_counter() - loader_started
    loader_output = completed.stdout.strip()
    if completed.stderr.strip():
        loader_output = loader_output + "\n" + completed.stderr.strip()
    print(loader_output)

    materializer_output = ""
    materializer_wall = 0.0
    if not args.skip_materialize:
        materialize_cmd = [
            sys.executable,
            str(materializer),
            "--version-date",
            version_date_int,
            "--version-ids",
            *version_ids,
            "--chunk-size",
            str(args.materialize_chunk_size),
        ]
        materializer_started = time.perf_counter()
        materialized = subprocess.run(
            materialize_cmd,
            cwd=PROJECT_ROOT,
            text=True,
            capture_output=True,
            check=True,
        )
        materializer_wall = time.perf_counter() - materializer_started
        materializer_output = materialized.stdout.strip()
        if materialized.stderr.strip():
            materializer_output = materializer_output + "\n" + materialized.stderr.strip()
        print(materializer_output)

    return {
        "loader_output": loader_output,
        "loader_wall_s": loader_wall,
        "materializer_output": materializer_output,
        "materializer_wall_s": materializer_wall,
        "total_wall_s": loader_wall + materializer_wall,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="B3b CUDAEnsemble spawn cap sweep for LIMITER V8")
    parser.add_argument("--version-date", required=True, help="Input dataset date YYYY-MM-DD")
    parser.add_argument("--input-version-id", type=int, default=3, help="Source input version_id")
    parser.add_argument("--caps", type=int, nargs="+", default=list(range(61)), help="Capped spawn_limit values")
    parser.add_argument("--out-base", type=int, default=1000, help="Output vid base: cap N -> out_base+N")
    parser.add_argument("--uncapped-version-id", type=int, default=1061)
    parser.add_argument("--skip-uncapped", action="store_true", help="Do not append uncapped baseline run")
    parser.add_argument("--reference-version-id", type=int, default=9, help="Reference vid for cap threshold")
    parser.add_argument("--repair-quota", type=int, required=True, help="Explicit repair quota for loader")
    parser.add_argument("--end-day", type=int, default=3650)
    parser.add_argument("--max-steps", type=int, default=10000)
    parser.add_argument("--devices", type=int, nargs="+", default=[0])
    parser.add_argument("--concurrent-runs", type=int, nargs="+", default=[4])
    parser.add_argument("--skip-load", action="store_true", help="Run CUDAEnsemble only; do not load .bin exports")
    parser.add_argument("--skip-materialize", action="store_true", help="Load .bin exports without daily materialization")
    parser.add_argument("--materialize-chunk-size", type=int, default=15, help="Version ids per materializer SQL pass")
    parser.add_argument("--loader-workers", type=int, default=4, help="Parallel loader workers for distinct version ids")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    fg.Telemetry.disable()

    specs = _run_specs(args)
    orchestrator, shape = _build_orchestrator(args)
    model = orchestrator.model
    repair_quota = int(args.repair_quota)
    if repair_quota != int(orchestrator.repair_quota):
        raise RuntimeError(
            f"Explicit repair_quota={repair_quota} does not match orchestrator repair_quota={orchestrator.repair_quota}"
        )

    results = []
    before_smi = _nvidia_smi_snapshot()
    for concurrent_runs in args.concurrent_runs:
        print(f"\n=== CUDAEnsemble B3b concurrent_runs={concurrent_runs} ===")
        result, smi_live = _run_ensemble_with_smi(model, args, specs, concurrent_runs)
        files = _validate_export_files(specs)
        results.append((result, smi_live, files))

    load_summary = None
    if not args.skip_load:
        load_summary = _load_exports(args, specs, repair_quota)

    after_smi = _nvidia_smi_snapshot()
    print("\n=== B3b RESULT ===")
    print(
        f"reference_shape threshold={shape['threshold']} days_total={shape['days_total']} "
        f"reference_vid={args.reference_version_id}"
    )
    print("planned_spawn_disabled=1 deterministic_spawn_count=0")
    for spec in specs:
        print(
            "run_spec "
            f"label={spec.label} version_id={spec.version_id} cap_n={spec.cap_n} "
            f"spawn_limit_active={spec.spawn_limit_active}"
        )
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
    if load_summary is None:
        print("loader_skipped=1")
    else:
        print("loader_runs=1")
        print(
            "phase_wall_s "
            f"loader={float(load_summary['loader_wall_s']):.3f} "
            f"materializer={float(load_summary['materializer_wall_s']):.3f} "
            f"load_total={float(load_summary['total_wall_s']):.3f}"
        )
    for info in results[-1][2]:
        print(
            "ensemble_file "
            f"label={info['label']} version_id={info['version_id']} field={info['field']} "
            f"path={info['path']} size={info['size']} sha256={info['sha256']}"
        )
    print(f"validated_ensemble_files={len(results[-1][2])}")


if __name__ == "__main__":
    main()
