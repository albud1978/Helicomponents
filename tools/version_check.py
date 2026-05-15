#!/usr/bin/env python3
"""Check versioned framework files for content-hash drift."""

from __future__ import annotations

import argparse
import fnmatch
import glob
import hashlib
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = REPO_ROOT / "config" / "versions_manifest.json"
DRIFT_STATUSES = {"DRIFT", "MISSING", "UNTRACKED"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Detect SHA-256 drift for versioned framework files."
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST,
        help="Path to versions manifest JSON.",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Refresh sha256 values for all existing tracked files.",
    )
    parser.add_argument(
        "--exit-on-drift",
        action="store_true",
        help="Exit with code 1 if DRIFT, MISSING, or UNTRACKED files are found.",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Print a one-line report.",
    )
    return parser


def resolve_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    return Path.cwd() / path


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"manifest not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"manifest must be a JSON object: {path}")
    files = data.get("files")
    if not isinstance(files, list):
        raise ValueError(f"manifest files must be a list: {path}")
    return data


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def relative_posix(path: Path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()


def is_excluded(path: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(path, pattern) for pattern in patterns)


def scan_scope(manifest: dict[str, Any]) -> set[str]:
    scope = manifest.get("scope") or {}
    include_paths = scope.get("include_paths") or []
    exclude_paths = scope.get("exclude_paths") or []
    if not isinstance(include_paths, list) or not isinstance(exclude_paths, list):
        raise ValueError("manifest scope include_paths/exclude_paths must be lists")

    found: set[str] = set()
    for pattern in include_paths:
        if not isinstance(pattern, str):
            raise ValueError("manifest scope patterns must be strings")
        full_pattern = str(REPO_ROOT / pattern)
        for raw_path in glob.glob(full_pattern, recursive=True):
            path = Path(raw_path)
            if not path.is_file():
                continue
            rel_path = relative_posix(path)
            if not is_excluded(rel_path, exclude_paths):
                found.add(rel_path)
    return found


def tracked_entries(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for item in manifest["files"]:
        if not isinstance(item, dict) or not isinstance(item.get("path"), str):
            raise ValueError("each manifest file entry must be an object with path")
        entries.append(item)
    return entries


def refresh_hashes(entries: list[dict[str, Any]]) -> None:
    for entry in entries:
        path = REPO_ROOT / str(entry["path"])
        if path.exists():
            entry["sha256"] = sha256_file(path)


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def build_report(manifest: dict[str, Any]) -> dict[str, list[dict[str, str]]]:
    report: dict[str, list[dict[str, str]]] = {
        "OK": [],
        "DRIFT": [],
        "MISSING": [],
        "UNTRACKED": [],
    }
    entries = tracked_entries(manifest)
    tracked_paths = {str(entry["path"]) for entry in entries}

    for entry in entries:
        rel_path = str(entry["path"])
        path = REPO_ROOT / rel_path
        semver = str(entry.get("semver", ""))
        recorded = str(entry.get("sha256", ""))

        if not path.exists():
            report["MISSING"].append({"path": rel_path, "semver": semver})
            continue

        actual = sha256_file(path)
        if actual == recorded:
            report["OK"].append({"path": rel_path, "semver": semver})
        else:
            report["DRIFT"].append(
                {
                    "path": rel_path,
                    "semver": semver,
                    "recorded": recorded,
                    "actual": actual,
                }
            )

    for rel_path in sorted(scan_scope(manifest) - tracked_paths):
        report["UNTRACKED"].append({"path": rel_path})

    return report


def print_summary(report: dict[str, list[dict[str, str]]]) -> None:
    total = len(report["OK"]) + len(report["DRIFT"]) + len(report["MISSING"])
    print(
        "Version drift report: "
        f"total={total} ok={len(report['OK'])} drift={len(report['DRIFT'])} "
        f"missing={len(report['MISSING'])} untracked={len(report['UNTRACKED'])}"
    )


def print_markdown(report: dict[str, list[dict[str, str]]]) -> None:
    total = len(report["OK"]) + len(report["DRIFT"]) + len(report["MISSING"])
    print("# Version drift report")
    print()
    print(f"**Total tracked**: {total}")
    print(f"**OK**: {len(report['OK'])}")
    print(f"**DRIFT**: {len(report['DRIFT'])}")
    print(f"**MISSING**: {len(report['MISSING'])}")
    print(f"**UNTRACKED**: {len(report['UNTRACKED'])}")

    for status in ("DRIFT", "MISSING", "UNTRACKED"):
        print()
        print(f"## {status} ({len(report[status])} files)")
        if not report[status]:
            print("- none")
            continue
        for item in report[status]:
            if status == "DRIFT":
                print(
                    f"- {item['path']} — semver: {item['semver']}, hash mismatch "
                    f"({item['recorded']} vs {item['actual']})"
                )
            elif status == "MISSING":
                print(f"- {item['path']} — semver: {item['semver']}")
            else:
                print(f"- {item['path']}")


def main() -> int:
    args = build_parser().parse_args()
    manifest_path = resolve_path(args.manifest)
    manifest = load_manifest(manifest_path)
    entries = tracked_entries(manifest)

    if args.update:
        refresh_hashes(entries)
        write_manifest(manifest_path, manifest)

    report = build_report(manifest)
    if args.summary_only:
        print_summary(report)
    else:
        print_markdown(report)

    has_drift = any(report[status] for status in DRIFT_STATUSES)
    return 1 if args.exit_on_drift and has_drift else 0


if __name__ == "__main__":
    raise SystemExit(main())
