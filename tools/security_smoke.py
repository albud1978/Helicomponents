#!/usr/bin/env python3
"""Lite security smoke runner for prompt/policy guard payloads."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TESTS_DIR = REPO_ROOT / "tests" / "security"
HOOKS_DIR = REPO_ROOT / ".cursor" / "hooks"
SUPPORTED_TYPES = {"static_payload_check", "hook_simulation", "regex_pattern"}


def _load_case(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"expected JSON object in {path}")
    return data


def _payload_text(case: dict[str, Any]) -> str:
    return json.dumps(case.get("payload", {}), ensure_ascii=False, sort_keys=True)


def _hook_path(name: str) -> Path:
    hook_name = name.strip()
    if not hook_name.endswith(".py"):
        hook_name = f"{hook_name}.py"
    return HOOKS_DIR / hook_name


def _matches(pattern: str, text: str) -> bool:
    return bool(re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL))


def _run_hook(case: dict[str, Any]) -> tuple[bool, str]:
    hook_name = str(case.get("expected_blocking_hook") or "").strip()
    pattern = str(case.get("expected_block_pattern") or "").strip()
    if not hook_name:
        return False, "missing expected_blocking_hook"
    if not pattern:
        return False, "missing expected_block_pattern"

    hook_path = _hook_path(hook_name)
    if not hook_path.exists():
        return False, f"hook not found: {hook_path.relative_to(REPO_ROOT)}"

    result = subprocess.run(
        [sys.executable, str(hook_path)],
        cwd=str(REPO_ROOT),
        input=_payload_text(case),
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )
    output = f"{result.stdout}\n{result.stderr}".strip()
    if _matches(pattern, output) or _matches(r'"decision"\s*:\s*"deny"', output):
        return True, output or "matched deny"
    return False, output or "empty hook output"


def _run_regex(case: dict[str, Any]) -> tuple[bool, str]:
    pattern = str(case.get("expected_block_pattern") or "").strip()
    if not pattern:
        return False, "missing expected_block_pattern"
    text = _payload_text(case)
    if _matches(pattern, text):
        return True, f"payload matches /{pattern}/"
    return False, f"payload does not match /{pattern}/"


def _run_case(path: Path) -> tuple[bool, str, str]:
    case = _load_case(path)
    case_id = str(case.get("id") or path.stem)
    test_type = str(case.get("test_type") or "")
    if test_type not in SUPPORTED_TYPES:
        return False, case_id, f"unsupported test_type: {test_type}"

    if test_type == "regex_pattern":
        ok, reason = _run_regex(case)
    elif test_type == "hook_simulation":
        ok, reason = _run_hook(case)
    else:
        ok, reason = _run_hook(case) if case.get("expected_blocking_hook") else _run_regex(case)
    return ok, case_id, reason


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tests", type=Path, default=DEFAULT_TESTS_DIR)
    parser.add_argument("--summary-only", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    paths = sorted(args.tests.glob("*.json"))
    if not paths:
        print(f"Total: 0, PASS: 0, FAIL: 0")
        raise SystemExit(1)

    passed = 0
    failed = 0
    for path in paths:
        try:
            ok, case_id, reason = _run_case(path)
        except (OSError, json.JSONDecodeError, subprocess.SubprocessError, ValueError) as exc:
            ok, case_id, reason = False, path.stem, str(exc)
        if ok:
            passed += 1
        else:
            failed += 1
        if not args.summary_only:
            status = "PASS" if ok else "FAIL"
            print(f"{status} {case_id}: {reason}")

    print(f"Total: {len(paths)}, PASS: {passed}, FAIL: {failed}")
    raise SystemExit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
