#!/usr/bin/env python3
"""Create a timestamped Superset dashboard backup bundle and Git tag."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import requests

from superset_git_sync import (
    _build_base_url,
    _login,
    _require_non_empty,
    export_dashboard_bundle,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
BI_ROOT = Path(__file__).resolve().parents[1]
BUNDLES_DIR = BI_ROOT / "superset" / "bundles"


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def _ids_label(dashboard_ids: list[int]) -> str:
    return "-".join(str(x) for x in dashboard_ids)


def _health_check(base_url: str, timeout_sec: int) -> None:
    response = requests.get(f"{base_url}/health", timeout=timeout_sec)
    if response.status_code != 200:
        raise RuntimeError(f"Superset health check failed: HTTP {response.status_code}")
    print(f"health OK: {base_url}/health")


def _run_git(args: list[str]) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"git {' '.join(args)} failed with exit {result.returncode}:\n"
            f"{result.stderr.strip() or result.stdout.strip()}"
        )
    return result


def _ensure_tag_absent(tag_name: str) -> None:
    result = subprocess.run(
        ["git", "rev-parse", "-q", "--verify", f"refs/tags/{tag_name}"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode == 0:
        raise RuntimeError(f"Git tag already exists: {tag_name}")
    if result.returncode != 1:
        raise RuntimeError(
            f"git rev-parse failed with exit {result.returncode}: "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )


def _add_note_to_meta(backup_dir: Path, note: str | None) -> None:
    if not note:
        return
    meta_path = backup_dir / "_export_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta["note"] = note
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    default_timeout = int(os.getenv("SUPERSET_API_TIMEOUT_SEC", "120"))
    parser = argparse.ArgumentParser(
        description="Create timestamped Superset backup bundle without mutating Superset."
    )
    parser.add_argument("--dashboard-ids", nargs="+", type=int, default=[1])
    parser.add_argument("--base-url", default=os.getenv("SUPERSET_API_BASE_URL"))
    parser.add_argument("--username", default=os.getenv("SUPERSET_API_USERNAME"))
    parser.add_argument("--password", default=os.getenv("SUPERSET_API_PASSWORD"))
    parser.add_argument("--provider", default=os.getenv("SUPERSET_API_PROVIDER", "db"))
    parser.add_argument("--timeout-sec", type=int, default=default_timeout)
    parser.add_argument("--note")
    parser.add_argument("--no-tag", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    base_url = _build_base_url(
        _require_non_empty("--base-url / SUPERSET_API_BASE_URL", args.base_url)
    )
    username = _require_non_empty("--username / SUPERSET_API_USERNAME", args.username)
    password = _require_non_empty("--password / SUPERSET_API_PASSWORD", args.password)
    stamp = _utc_stamp()
    ids_label = _ids_label(args.dashboard_ids)
    tag_name = f"bi-backup-{ids_label}-{stamp}"
    backup_dir = BUNDLES_DIR / f"dashboard_{ids_label}_backup_{stamp}"

    if not args.no_tag:
        _ensure_tag_absent(tag_name)

    _health_check(base_url, args.timeout_sec)
    ss = _login(base_url, username, password, provider=args.provider, timeout_sec=args.timeout_sec)
    export_dashboard_bundle(ss, args.dashboard_ids, backup_dir, timeout_sec=args.timeout_sec)
    _add_note_to_meta(backup_dir, args.note)
    print(f"export OK: {backup_dir.relative_to(REPO_ROOT)}")

    # Каталоги *_backup_* игнорируются от случайного коммита; здесь add является явным действием оператора.
    backup_rel = backup_dir.relative_to(REPO_ROOT).as_posix()
    _run_git(["add", "-A", "-f", "--", backup_rel])
    print(f"git add OK: {backup_rel}")

    if args.no_tag:
        print("git tag skipped: --no-tag")
    else:
        _run_git(["tag", tag_name])
        print(f"git tag OK: {tag_name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
