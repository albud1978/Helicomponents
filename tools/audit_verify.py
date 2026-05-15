#!/usr/bin/env python3
"""Verify tamper-evident hash chains in local audit logs."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


def compute_entry_hash(entry: dict[str, Any], prev_hash: str | None) -> str:
    """Compute the audit hash for one entry, excluding chain metadata fields."""
    content = json.dumps(
        {
            key: value
            for key, value in entry.items()
            if key not in ("prev_hash", "current_hash")
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(((prev_hash or "") + content).encode("utf-8")).hexdigest()


def verify_log(path: Path) -> tuple[bool, str]:
    """Verify hashed JSONL entries in an audit log."""
    prev_hash: str | None = None
    verified = 0
    skipped_legacy = 0

    with path.open("r", encoding="utf-8") as f:
        for line_no, raw_line in enumerate(f, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                skipped_legacy += 1
                continue
            if not isinstance(entry, dict) or "current_hash" not in entry:
                skipped_legacy += 1
                continue

            if entry.get("prev_hash") != prev_hash:
                return (
                    False,
                    f"BROKEN at line {line_no}: expected {prev_hash}, got {entry.get('prev_hash')}",
                )

            expected = compute_entry_hash(entry, prev_hash)
            current_hash = entry.get("current_hash")
            if current_hash != expected:
                return (
                    False,
                    f"BROKEN at line {line_no}: expected {expected}, got {current_hash}",
                )

            prev_hash = str(current_hash)
            verified += 1

    message = f"OK: {verified} entries verified"
    if skipped_legacy:
        message += f" ({skipped_legacy} legacy lines skipped)"
    return True, message


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify audit log hash-chain integrity.")
    parser.add_argument("--log-file", type=Path, required=True)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    ok, message = verify_log(args.log_file)
    print(message)
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
