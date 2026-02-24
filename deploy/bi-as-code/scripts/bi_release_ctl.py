#!/usr/bin/env python3
"""Minimal BI release controller for corporate sandbox.

This tool is intentionally conservative:
- no implicit deploy actions;
- apply requires explicit confirmation phrase;
- rollback works from the last recorded apply state.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CONFIRM_PHRASE = "DEPLOY_TO_CORP_SANDBOX"


@dataclass
class ManifestEntry:
    step: int
    kind: str
    logical_id: str
    action: str
    source: str
    notes: str


def _load_yaml_or_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Manifest not found: {path}")
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        return json.loads(text)
    try:
        import yaml  # type: ignore
    except Exception as exc:  # pragma: no cover - runtime guidance
        raise RuntimeError(
            "PyYAML is required for .yaml manifests. Install with: python -m pip install pyyaml"
        ) from exc
    return yaml.safe_load(text)


def _parse_entries(manifest: dict[str, Any]) -> list[ManifestEntry]:
    raw_steps = manifest.get("steps", [])
    entries: list[ManifestEntry] = []
    for idx, item in enumerate(raw_steps, start=1):
        if not isinstance(item, dict):
            continue
        entries.append(
            ManifestEntry(
                step=int(item.get("step", idx)),
                kind=str(item.get("kind", "unknown")),
                logical_id=str(item.get("logical_id", f"step_{idx}")),
                action=str(item.get("action", "upsert")),
                source=str(item.get("source", "")),
                notes=str(item.get("notes", "")),
            )
        )
    entries.sort(key=lambda x: x.step)
    return entries


def _print_plan(title: str, entries: list[ManifestEntry]) -> None:
    print(title)
    for e in entries:
        line = f"  [{e.step:02d}] {e.action.upper():8} {e.kind:10} id={e.logical_id}"
        if e.source:
            line += f" src={e.source}"
        if e.notes:
            line += f" | {e.notes}"
        print(line)


def cmd_dry_run(manifest_path: Path) -> int:
    manifest = _load_yaml_or_json(manifest_path)
    entries = _parse_entries(manifest)
    print(f"manifest: {manifest_path}")
    print(f"target_environment: {manifest.get('target_environment', 'unknown')}")
    print(f"release_id: {manifest.get('release_id', 'n/a')}")
    print(f"steps: {len(entries)}")
    _print_plan("dry-run plan:", entries)
    return 0


def cmd_apply(manifest_path: Path, state_file: Path, confirm: str) -> int:
    if confirm != CONFIRM_PHRASE:
        print(
            f"Refusing apply: confirmation phrase mismatch. Use --confirm {CONFIRM_PHRASE}",
            file=sys.stderr,
        )
        return 2

    manifest = _load_yaml_or_json(manifest_path)
    entries = _parse_entries(manifest)
    if not entries:
        print("No steps in manifest, nothing to apply.")
        return 0

    print("apply simulation (safe mode):")
    _print_plan("planned actions:", entries)

    # Record state for deterministic rollback planning.
    state = {
        "applied_at": datetime.now(timezone.utc).isoformat(),
        "manifest_path": str(manifest_path),
        "release_id": manifest.get("release_id", "n/a"),
        "target_environment": manifest.get("target_environment", "corp-sandbox"),
        "steps": [e.__dict__ for e in entries],
    }
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"state recorded: {state_file}")
    print("NOTE: This controller records and validates release intent; API execution is wired in next stage.")
    return 0


def cmd_rollback(state_file: Path) -> int:
    if not state_file.exists():
        print(f"Rollback state file does not exist: {state_file}", file=sys.stderr)
        return 2
    state = json.loads(state_file.read_text(encoding="utf-8"))
    raw_steps = state.get("steps", [])
    entries = [ManifestEntry(**s) for s in raw_steps if isinstance(s, dict)]
    entries.sort(key=lambda x: x.step, reverse=True)
    print("rollback simulation (reverse order):")
    _print_plan("rollback actions:", entries)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="BI release control for corporate sandbox (dry-run/apply/rollback)."
    )
    parser.add_argument(
        "--manifest",
        default="deploy/bi-as-code/release/templates/deployment_manifest.template.yaml",
        help="Path to release manifest (yaml/json).",
    )
    parser.add_argument(
        "--state-file",
        default="deploy/bi-as-code/release/.last_apply_state.json",
        help="Path to last apply state file for rollback planning.",
    )

    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("dry-run", help="Validate manifest and print planned operations.")
    p_apply = sub.add_parser("apply", help="Record apply intent (requires explicit confirmation).")
    p_apply.add_argument("--confirm", required=True, help=f"Must equal: {CONFIRM_PHRASE}")
    sub.add_parser("rollback", help="Print rollback operations from last apply state.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    manifest_path = Path(args.manifest)
    state_file = Path(args.state_file)

    if args.command == "dry-run":
        return cmd_dry_run(manifest_path)
    if args.command == "apply":
        return cmd_apply(manifest_path, state_file, args.confirm)
    if args.command == "rollback":
        return cmd_rollback(state_file)

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
