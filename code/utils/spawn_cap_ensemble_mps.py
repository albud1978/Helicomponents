#!/usr/bin/env python3
"""MPS launcher for sharded spawn_limit CUDAEnsemble sweeps."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
HARNESS = PROJECT_ROOT / "code" / "utils" / "spawn_cap_ensemble.py"
LOADER = PROJECT_ROOT / "code" / "utils" / "ensemble_mp2_loader.py"
MATERIALIZER = PROJECT_ROOT / "code" / "sim_v2" / "messaging" / "sim_daily_materializer.py"
LOG_DIR = PROJECT_ROOT / "output" / "mps_shard"
WALL_RE = re.compile(r"\bwall_s=([0-9]+(?:\.[0-9]+)?)")


@dataclass(frozen=True)
class ShardSpec:
    index: int
    caps: list[int]
    include_uncapped: bool
    log_path: Path


@dataclass
class ShardProcess:
    spec: ShardSpec
    process: subprocess.Popen
    log_handle: object
    started_at: float


@dataclass(frozen=True)
class ShardSummary:
    spec: ShardSpec
    return_code: int
    measured_wall_s: float
    harness_wall_s: float | None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CUDA MPS sharded launcher for spawn cap ensemble sweep")
    parser.add_argument("--version-date", required=True)
    parser.add_argument("--input-version-id", type=int, required=True)
    parser.add_argument("--repair-quota", type=int, required=True)
    parser.add_argument("--caps", type=int, nargs="+", default=list(range(61)))
    parser.add_argument("--shards", type=int, default=4)
    parser.add_argument("--concurrent-runs", type=int, default=8)
    parser.add_argument("--out-base", type=int, default=1000)
    parser.add_argument("--uncapped-version-id", type=int, default=None)
    parser.add_argument("--skip-uncapped", action="store_true")
    parser.add_argument("--threshold", type=int, default=None)
    parser.add_argument("--end-day", type=int, default=3650)
    parser.add_argument("--max-steps", type=int, default=10000)
    parser.add_argument("--skip-load", action="store_true")
    return parser.parse_args()


def _normalize_args(args: argparse.Namespace) -> argparse.Namespace:
    if args.uncapped_version_id is None:
        args.uncapped_version_id = int(args.out_base) + 61
    if args.shards <= 0:
        raise RuntimeError(f"--shards must be > 0, got {args.shards}")
    if args.concurrent_runs <= 0:
        raise RuntimeError(f"--concurrent-runs must be > 0, got {args.concurrent_runs}")
    if args.end_day <= 0:
        raise RuntimeError(f"--end-day must be > 0, got {args.end_day}")
    if args.max_steps <= 0:
        raise RuntimeError(f"--max-steps must be > 0, got {args.max_steps}")

    caps = [int(cap) for cap in args.caps]
    if not caps:
        raise RuntimeError("--caps must not be empty")
    if len(set(caps)) != len(caps):
        raise RuntimeError(f"Duplicate caps are not allowed: {caps}")
    invalid_caps = [cap for cap in caps if cap < 0 or cap > 60]
    if invalid_caps:
        raise RuntimeError(f"--caps values must be within 0..60, got {invalid_caps}")
    args.caps = sorted(caps)

    version_ids = [int(args.out_base) + cap for cap in args.caps]
    if not args.skip_uncapped:
        version_ids.append(int(args.uncapped_version_id))
    if len(set(version_ids)) != len(version_ids):
        raise RuntimeError(f"Output version_id collision detected: {version_ids}")
    return args


def _mps_pids() -> list[str]:
    completed = subprocess.run(
        ["pgrep", "-f", "nvidia-cuda-mps-control"],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode == 1:
        return []
    if completed.returncode != 0:
        raise RuntimeError(
            "Cannot check CUDA MPS daemon with pgrep: "
            f"rc={completed.returncode} stderr={completed.stderr.strip()}"
        )
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def _ensure_mps() -> None:
    before = _mps_pids()
    if before:
        print(f"mps_already_running pids={','.join(before)}")
        return

    started = subprocess.run(
        ["nvidia-cuda-mps-control", "-d"],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if started.returncode != 0:
        raise RuntimeError(
            "CUDA MPS daemon failed to start: "
            f"rc={started.returncode} stdout={started.stdout.strip()} stderr={started.stderr.strip()}"
        )
    time.sleep(1.0)
    after = _mps_pids()
    if not after:
        raise RuntimeError("CUDA MPS daemon did not appear after nvidia-cuda-mps-control -d")
    print(f"mps_started pids={','.join(after)}")


def _stop_mps() -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["nvidia-cuda-mps-control"],
        input="quit\n",
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def _chunk_caps(caps: list[int], shard_count: int) -> list[list[int]]:
    effective = min(int(shard_count), len(caps))
    base_size, remainder = divmod(len(caps), effective)
    chunks: list[list[int]] = []
    start = 0
    for idx in range(effective):
        size = base_size + (1 if idx < remainder else 0)
        chunk = caps[start : start + size]
        if chunk:
            chunks.append(chunk)
        start += size
    return chunks


def _label(args: argparse.Namespace) -> str:
    version_date = date.fromisoformat(args.version_date).strftime("%Y%m%d")
    return f"vd{version_date}_out{int(args.out_base)}"


def _shard_specs(args: argparse.Namespace) -> list[ShardSpec]:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    chunks = _chunk_caps(args.caps, int(args.shards))
    label = _label(args)
    last_index = len(chunks) - 1
    specs = []
    for idx, caps in enumerate(chunks):
        specs.append(
            ShardSpec(
                index=idx + 1,
                caps=caps,
                include_uncapped=(idx == last_index and not args.skip_uncapped),
                log_path=LOG_DIR / f"{label}_shard{idx + 1}.log",
            )
        )
    return specs


def _format_cap_span(caps: list[int]) -> str:
    if len(caps) == 1:
        return str(caps[0])
    return f"{caps[0]}..{caps[-1]}"


def _shard_command(args: argparse.Namespace, spec: ShardSpec) -> list[str]:
    cmd = [
        sys.executable,
        str(HARNESS),
        "--version-date",
        args.version_date,
        "--input-version-id",
        str(args.input_version_id),
        "--repair-quota",
        str(args.repair_quota),
        "--caps",
        *[str(cap) for cap in spec.caps],
        "--out-base",
        str(args.out_base),
        "--concurrent-runs",
        str(args.concurrent_runs),
        "--end-day",
        str(args.end_day),
        "--max-steps",
        str(args.max_steps),
        "--skip-load",
    ]
    if args.threshold is not None:
        cmd.extend(["--threshold", str(args.threshold)])
    if spec.include_uncapped:
        cmd.extend(["--uncapped-version-id", str(args.uncapped_version_id)])
    else:
        cmd.append("--skip-uncapped")
    return cmd


def _read_last_harness_wall(log_path: Path) -> float | None:
    matches = WALL_RE.findall(log_path.read_text(encoding="utf-8", errors="replace"))
    if not matches:
        return None
    return float(matches[-1])


def _tail_text(log_path: Path, line_count: int = 80) -> str:
    lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-line_count:])


def _terminate_running(processes: list[ShardProcess]) -> None:
    for running in processes:
        if running.process.poll() is None:
            running.process.terminate()
    for running in processes:
        if running.process.poll() is None:
            running.process.wait(timeout=10)
        running.log_handle.close()


def _launch_shards(args: argparse.Namespace, specs: list[ShardSpec]) -> tuple[list[ShardSummary], float]:
    env = os.environ.copy()
    processes: list[ShardProcess] = []
    gpu_started = time.perf_counter()
    for spec in specs:
        cmd = _shard_command(args, spec)
        log_handle = spec.log_path.open("w", encoding="utf-8")
        log_handle.write("$ " + " ".join(cmd) + "\n")
        log_handle.flush()
        process = subprocess.Popen(
            cmd,
            cwd=PROJECT_ROOT,
            env=env,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        processes.append(ShardProcess(spec, process, log_handle, time.perf_counter()))
        print(
            "shard_launched "
            f"shard={spec.index} caps={_format_cap_span(spec.caps)} "
            f"uncapped={int(spec.include_uncapped)} pid={process.pid} log={spec.log_path}"
        )

    summaries: list[ShardSummary] = []
    remaining = set(range(len(processes)))
    while remaining:
        for process_idx in list(remaining):
            shard_process = processes[process_idx]
            rc = shard_process.process.poll()
            if rc is None:
                continue
            remaining.remove(process_idx)
            measured_wall = time.perf_counter() - shard_process.started_at
            shard_process.log_handle.close()
            harness_wall = _read_last_harness_wall(shard_process.spec.log_path)
            summary = ShardSummary(shard_process.spec, rc, measured_wall, harness_wall)
            summaries.append(summary)
            print(
                "shard_done "
                f"shard={summary.spec.index} rc={summary.return_code} "
                f"caps={_format_cap_span(summary.spec.caps)} "
                f"wall_s={summary.measured_wall_s:.3f} "
                f"harness_wall_s={summary.harness_wall_s if summary.harness_wall_s is not None else 'NA'} "
                f"log={summary.spec.log_path}"
            )
            if rc != 0:
                _terminate_running([processes[idx] for idx in remaining])
                print(f"\n=== shard{summary.spec.index} log tail ===", file=sys.stderr)
                print(_tail_text(summary.spec.log_path), file=sys.stderr)
                print(f"=== shard{summary.spec.index} log tail end ===\n", file=sys.stderr)
                return summaries, -1.0
        if remaining:
            time.sleep(0.2)

    return sorted(summaries, key=lambda item: item.spec.index), time.perf_counter() - gpu_started


def _version_ids(args: argparse.Namespace) -> list[int]:
    version_ids = [int(args.out_base) + cap for cap in args.caps]
    if not args.skip_uncapped:
        version_ids.append(int(args.uncapped_version_id))
    return version_ids


def _run_captured(cmd: list[str]) -> tuple[int, float, str]:
    started = time.perf_counter()
    completed = subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        env=os.environ.copy(),
        text=True,
        capture_output=True,
        check=False,
    )
    wall = time.perf_counter() - started
    output = completed.stdout.strip()
    if completed.stderr.strip():
        output = output + "\n" + completed.stderr.strip()
    return completed.returncode, wall, output


def _version_date_int_arg(version_date: str) -> str:
    return date.fromisoformat(version_date).strftime("%Y%m%d")


def _load_exports(args: argparse.Namespace) -> tuple[int, float, float]:
    version_ids = [str(version_id) for version_id in _version_ids(args)]
    loader_cmd = [
        sys.executable,
        str(LOADER),
        "--version-date",
        args.version_date,
        "--version-ids",
        *version_ids,
        "--baseline-version-id",
        str(args.input_version_id),
        "--repair-quota",
        str(args.repair_quota),
        "--parallel-workers",
        "4",
    ]
    loader_rc, loader_wall, loader_output = _run_captured(loader_cmd)
    print(loader_output)
    if loader_rc != 0:
        print(f"loader_failed rc={loader_rc}", file=sys.stderr)
        return loader_rc, loader_wall, 0.0

    materialize_cmd = [
        sys.executable,
        str(MATERIALIZER),
        "--version-date",
        _version_date_int_arg(args.version_date),
        "--version-ids",
        *version_ids,
        "--chunk-size",
        "15",
    ]
    materializer_rc, materializer_wall, materializer_output = _run_captured(materialize_cmd)
    print(materializer_output)
    if materializer_rc != 0:
        print(f"materializer_failed rc={materializer_rc}", file=sys.stderr)
        return materializer_rc, loader_wall, materializer_wall
    return 0, loader_wall, materializer_wall


def _run(args: argparse.Namespace, total_started: float) -> int:
    args = _normalize_args(args)
    _ensure_mps()
    specs = _shard_specs(args)
    print(
        "mps_shard_plan "
        f"version_date={args.version_date} input_version_id={args.input_version_id} "
        f"caps={args.caps[0]}..{args.caps[-1]} requested_shards={args.shards} "
        f"effective_shards={len(specs)} concurrent_runs={args.concurrent_runs} "
        f"out_base={args.out_base} skip_uncapped={int(args.skip_uncapped)} "
        f"skip_load={int(args.skip_load)}"
    )
    for spec in specs:
        print(
            "shard_plan "
            f"shard={spec.index} caps={_format_cap_span(spec.caps)} "
            f"uncapped={int(spec.include_uncapped)} log={spec.log_path}"
        )

    summaries, gpu_wall = _launch_shards(args, specs)
    if gpu_wall < 0:
        return 1

    loader_wall = 0.0
    materializer_wall = 0.0
    load_rc = 0
    if args.skip_load:
        print("loader_skipped=1")
    else:
        load_rc, loader_wall, materializer_wall = _load_exports(args)
        if load_rc != 0:
            return load_rc

    total_wall = time.perf_counter() - total_started
    print("\n=== MPS SHARD RESULT ===")
    for summary in summaries:
        print(
            "shard_wall_s "
            f"shard={summary.spec.index} rc={summary.return_code} "
            f"caps={_format_cap_span(summary.spec.caps)} "
            f"measured={summary.measured_wall_s:.3f} "
            f"harness={summary.harness_wall_s if summary.harness_wall_s is not None else 'NA'} "
            f"log={summary.spec.log_path}"
        )
    print(
        "phase_wall_s "
        f"gpu={gpu_wall:.3f} loader={loader_wall:.3f} "
        f"materializer={materializer_wall:.3f} load_total={loader_wall + materializer_wall:.3f}"
    )
    print(f"total_wall_s={total_wall:.3f}")
    return 0


def main() -> int:
    args = _parse_args()
    total_started = time.perf_counter()
    exit_code = 1
    try:
        exit_code = _run(args, total_started)
    finally:
        stopped = _stop_mps()
        print(f"mps_stop_rc={stopped.returncode}")
        if stopped.stdout.strip():
            print(f"mps_stop_stdout={stopped.stdout.strip()}")
        if stopped.stderr.strip():
            print(f"mps_stop_stderr={stopped.stderr.strip()}")
        after = _mps_pids()
        print(f"mps_pids_after_stop={'none' if not after else ','.join(after)}")
        if exit_code == 0 and (stopped.returncode != 0 or after):
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
