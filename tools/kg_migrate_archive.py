#!/usr/bin/env python3
"""Archive old closed Agent KG workflows into JSONL files."""

from __future__ import annotations

import argparse
import json
import os
import re
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_KG_PATH = REPO_ROOT / "config" / "agent_kg.json"
DEFAULT_ARCHIVE_ROOT = REPO_ROOT / "config" / "agent_kg_archive"
RETENTION_RECENT_CLOSED = 20


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _event_time(item: dict[str, Any]) -> str:
    return str(item.get("closed_at") or item.get("updated_at") or item.get("created_at") or "")


def _event_date(item: dict[str, Any]) -> str:
    timestamp = _event_time(item)
    if re.match(r"^\d{4}-\d{2}-\d{2}", timestamp):
        return timestamp[:10]
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _safe_filename(workflow_id: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", workflow_id)
    return f"{safe}.jsonl"


def _load_kg(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    data.setdefault("metadata", {})["updated_at"] = _now()
    tmp_path = path.with_suffix(".json.tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp_path, path)


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(content, encoding="utf-8")
    os.replace(tmp_path, path)


def _archive_events(
    workflow: dict[str, Any],
    handoffs: list[dict[str, Any]],
    contexts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    events.append({"type": "workflow", **workflow})
    events.extend({"type": "handoff", **handoff} for handoff in handoffs)
    events.extend({"type": "context", **context} for context in contexts)
    return events


def build_migration(
    data: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]], dict[str, int]]:
    """Build reduced active KG and per-workflow archive events."""
    workflows = data.get("workflows", [])
    handoffs = data.get("handoffs", [])
    contexts = data.get("contexts", [])

    closed = [
        workflow
        for workflow in workflows
        if isinstance(workflow, dict)
        and str(workflow.get("status") or "").lower() == "closed"
    ]
    closed_sorted = sorted(closed, key=_event_time, reverse=True)
    retained_closed_ids = {
        str(workflow.get("workflow_id")) for workflow in closed_sorted[:RETENTION_RECENT_CLOSED]
    }
    archived_closed = closed_sorted[RETENTION_RECENT_CLOSED:]
    archived_ids = {str(workflow.get("workflow_id")) for workflow in archived_closed}

    reduced = deepcopy(data)
    reduced["workflows"] = [
        workflow
        for workflow in workflows
        if not (
            isinstance(workflow, dict)
            and str(workflow.get("workflow_id")) in archived_ids
        )
    ]
    retained_ids = {
        str(workflow.get("workflow_id"))
        for workflow in reduced["workflows"]
        if isinstance(workflow, dict)
    }
    reduced["handoffs"] = [
        handoff
        for handoff in handoffs
        if not (
            isinstance(handoff, dict)
            and str(handoff.get("workflow_id")) in archived_ids
        )
    ]
    reduced["contexts"] = [
        context
        for context in contexts
        if not (
            isinstance(context, dict)
            and str(context.get("workflow_id")) in archived_ids
        )
    ]
    reduced.setdefault("meta", {})
    reduced["meta"]["schema_version"] = reduced["meta"].get("schema_version", 1)
    reduced["meta"]["last_archive_at"] = _now()
    reduced["meta"]["retention_recent_closed"] = RETENTION_RECENT_CLOSED

    archive_map: dict[str, list[dict[str, Any]]] = {}
    for workflow in archived_closed:
        workflow_id = str(workflow.get("workflow_id"))
        wf_handoffs = [
            handoff
            for handoff in handoffs
            if isinstance(handoff, dict) and handoff.get("workflow_id") == workflow_id
        ]
        wf_contexts = [
            context
            for context in contexts
            if isinstance(context, dict) and context.get("workflow_id") == workflow_id
        ]
        archive_map[workflow_id] = _archive_events(workflow, wf_handoffs, wf_contexts)

    stats = {
        "workflows_before": len(workflows),
        "workflows_after": len(reduced["workflows"]),
        "closed_before": len(closed),
        "retained_closed": len(retained_closed_ids),
        "workflows_archived": len(archived_ids),
        "handoffs_archived": len(handoffs) - len(reduced["handoffs"]),
        "contexts_archived": len(contexts) - len(reduced["contexts"]),
        "retained_workflow_ids": len(retained_ids),
    }
    return reduced, archive_map, stats


def estimate_json_size(data: dict[str, Any]) -> int:
    """Return byte size of the pretty-printed active KG representation."""
    rendered = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    return len(rendered.encode("utf-8"))


def execute_migration(
    kg_path: Path,
    archive_root: Path,
    reduced: dict[str, Any],
    archive_map: dict[str, list[dict[str, Any]]],
) -> None:
    """Write JSONL archive files and atomically replace the active KG."""
    workflow_by_id = {
        str(events[0].get("workflow_id")): events[0]
        for events in archive_map.values()
        if events
    }
    for workflow_id, events in archive_map.items():
        workflow = workflow_by_id[workflow_id]
        archive_path = archive_root / _event_date(workflow) / _safe_filename(workflow_id)
        content = "\n".join(
            json.dumps(event, ensure_ascii=False, sort_keys=True) for event in events
        )
        _atomic_write_text(archive_path, content + "\n")
    _atomic_write_json(kg_path, reduced)


def print_report(stats: dict[str, int], size_before: int, size_after: int, mode: str) -> None:
    """Print migration statistics in a stable text format."""
    print(f"mode={mode}")
    print(f"size_before_bytes={size_before}")
    print(f"size_after_bytes={size_after}")
    for key in (
        "workflows_before",
        "workflows_after",
        "closed_before",
        "retained_closed",
        "workflows_archived",
        "handoffs_archived",
        "contexts_archived",
        "retained_workflow_ids",
    ):
        print(f"{key}={stats[key]}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Archive old closed Agent KG workflows into JSONL files."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Print planned migration only")
    mode.add_argument("--execute", action="store_true", help="Apply migration")
    parser.add_argument("--kg-path", type=Path, default=DEFAULT_KG_PATH)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE_ROOT)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    kg_path = args.kg_path.resolve()
    archive_root = args.archive_root.resolve()
    data = _load_kg(kg_path)
    size_before = kg_path.stat().st_size
    reduced, archive_map, stats = build_migration(data)
    size_after = estimate_json_size(reduced)

    if args.execute:
        execute_migration(kg_path, archive_root, reduced, archive_map)
        size_after = kg_path.stat().st_size
        print_report(stats, size_before, size_after, "execute")
    else:
        print_report(stats, size_before, size_after, "dry-run")


if __name__ == "__main__":
    main()
