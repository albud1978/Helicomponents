#!/usr/bin/env python3
"""
Spawn cap curve layer 2 launcher.

This launcher slices cap ranges into concurrent subprocesses that call layer 1.
WARNING: actual launch starts GPU simulation and writes simulation outputs through
Limiter V8. The default invocation with no flags is a dry-run plan only.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "code" / "utils" / "spawn_cap_curve.py"
CUDA_PYTHON = "/home/albud/miniconda3/envs/cuda13/bin/python3"
DEFAULT_VERSION_DATE = "2026-06-22"


def _cap_range(cap_min: int, cap_max: int) -> list[int]:
    if cap_min < 0 or cap_max < 0:
        raise ValueError("cap-min/cap-max must be non-negative")
    if cap_min > cap_max:
        raise ValueError("cap-min must be <= cap-max")
    return list(range(cap_min, cap_max + 1))


def _chunks(caps: list[int], max_concurrency: int) -> list[list[int]]:
    if max_concurrency <= 0:
        raise ValueError("max-concurrency must be positive")
    chunk_count = min(max_concurrency, len(caps))
    if chunk_count == 0:
        return []
    base_size, remainder = divmod(len(caps), chunk_count)
    chunks: list[list[int]] = []
    start = 0
    for idx in range(chunk_count):
        size = base_size + (1 if idx < remainder else 0)
        chunk = caps[start : start + size]
        if chunk:
            chunks.append(chunk)
        start += size
    return chunks


def _run_env() -> dict[str, str]:
    env = os.environ.copy()
    home = Path.home()
    cuda_path = home / "miniconda3" / "targets" / "x86_64-linux"
    conda_lib = home / "miniconda3" / "lib"
    env_lib = home / "miniconda3" / "envs" / "cuda13" / "lib"
    old_ld = env.get("LD_LIBRARY_PATH", "")
    env["CUDA_PATH"] = str(cuda_path)
    env["LD_LIBRARY_PATH"] = f"{conda_lib}:{env_lib}:{old_ld}" if old_ld else f"{conda_lib}:{env_lib}"
    return env


def _run_command(args: argparse.Namespace, chunk: list[int]) -> list[str]:
    return [
        CUDA_PYTHON,
        str(SCRIPT_PATH),
        "--version-date",
        args.version_date,
        "--cap-min",
        str(chunk[0]),
        "--cap-max",
        str(chunk[-1]),
        "--out-base",
        str(args.out_base),
        "--src-vid",
        str(args.src_vid),
        "--end-day",
        str(args.end_day),
        "--run",
    ]


def _collect_command(args: argparse.Namespace) -> list[str]:
    return [
        sys.executable,
        str(SCRIPT_PATH),
        "--version-date",
        args.version_date,
        "--cap-min",
        str(args.cap_min),
        "--cap-max",
        str(args.cap_max),
        "--out-base",
        str(args.out_base),
        "--src-vid",
        str(args.src_vid),
        "--collect",
    ]


def _print_plan(args: argparse.Namespace, chunks: list[list[int]]) -> None:
    print("DRY RUN: launcher will not start subprocesses")
    print(
        f"version_date={args.version_date} cap_min={args.cap_min} cap_max={args.cap_max} "
        f"src_vid={args.src_vid} out_base={args.out_base} max_concurrency={args.max_concurrency}"
    )
    for idx, chunk in enumerate(chunks, 1):
        cmd = " ".join(_run_command(args, chunk))
        print(f"  process {idx}: caps {chunk[0]}..{chunk[-1]} -> {cmd}")
    if args.collect_after:
        print("  collect-after: " + " ".join(_collect_command(args)))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Launch spawn cap curve subprocesses.")
    parser.add_argument("--max-concurrency", type=int, default=8)
    parser.add_argument("--cap-min", type=int, default=0)
    parser.add_argument("--cap-max", type=int, default=60)
    parser.add_argument("--version-date", default=DEFAULT_VERSION_DATE)
    parser.add_argument("--out-base", type=int, default=100)
    parser.add_argument("--src-vid", type=int, default=3)
    parser.add_argument("--end-day", type=int, default=3650)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--collect-after", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    caps = _cap_range(args.cap_min, args.cap_max)
    chunks = _chunks(caps, args.max_concurrency)

    if args.dry_run or (argv is None and len(sys.argv) == 1):
        _print_plan(args, chunks)
        return 0

    env = _run_env()
    processes: list[tuple[list[int], subprocess.Popen]] = []
    for chunk in chunks:
        cmd = _run_command(args, chunk)
        print(f"launch: caps {chunk[0]}..{chunk[-1]}")
        processes.append((chunk, subprocess.Popen(cmd, cwd=REPO_ROOT, env=env)))

    failures: list[tuple[list[int], int]] = []
    for chunk, process in processes:
        return_code = process.wait()
        print(f"done: caps {chunk[0]}..{chunk[-1]} rc={return_code}")
        if return_code != 0:
            failures.append((chunk, return_code))

    if failures:
        for chunk, return_code in failures:
            print(f"failed: caps {chunk[0]}..{chunk[-1]} rc={return_code}", file=sys.stderr)
        return 1

    if args.collect_after:
        cmd = _collect_command(args)
        print("collect-after: " + " ".join(cmd))
        return subprocess.run(cmd, cwd=REPO_ROOT, env=os.environ.copy()).returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
