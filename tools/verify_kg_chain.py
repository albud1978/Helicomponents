#!/usr/bin/env python3
"""Verify forward-only Agent KG handoff hash chains."""

import argparse
import importlib.util
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_KG_PATH = REPO_ROOT / "config" / "agent_kg.json"
AGENT_KG_PATH = REPO_ROOT / "code" / "utils" / "agent_kg.py"


def _load_agent_kg_helper():
    spec = importlib.util.spec_from_file_location("agent_kg_helper", AGENT_KG_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load helper module: {AGENT_KG_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_AGENT_KG = _load_agent_kg_helper()
_compute_handoff_hash = _AGENT_KG._compute_handoff_hash
HASH_CHAIN_GENESIS = _AGENT_KG.HASH_CHAIN_GENESIS


def _load_active_kg(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _archive_root(kg_path: Path) -> Path:
    return kg_path.resolve().parent / "agent_kg_archive"


def _load_archived_handoffs(kg_path: Path) -> List[Dict[str, Any]]:
    root = _archive_root(kg_path)
    if not root.exists():
        return []

    handoffs: List[Dict[str, Any]] = []
    for jsonl_path in sorted(root.glob("*/*.jsonl")):
        with jsonl_path.open("r", encoding="utf-8") as f:
            for raw_line in f:
                if not raw_line.strip():
                    continue
                event = json.loads(raw_line)
                if event.get("type") != "handoff":
                    continue
                handoff = dict(event)
                handoff.pop("type", None)
                handoffs.append(handoff)
    return handoffs


def _group_handoffs(handoffs: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for handoff in handoffs:
        workflow_id = handoff.get("workflow_id")
        if workflow_id:
            grouped[str(workflow_id)].append(handoff)
    for workflow_handoffs in grouped.values():
        workflow_handoffs.sort(key=lambda item: str(item.get("created_at") or ""))
    return dict(grouped)


def _pct(part: int, total: int) -> float:
    if total == 0:
        return 0.0
    return round(part * 100 / total, 1)


def _baseline_label(metadata: Dict[str, Any]) -> str:
    baseline = metadata.get("hash_chain_baseline")
    if not isinstance(baseline, dict):
        return "baseline not yet established"
    started_at = baseline.get("started_at")
    return f"baseline {started_at}" if started_at else "baseline established"


def _verify_workflow(
    workflow_id: str, handoffs: List[Dict[str, Any]]
) -> Dict[str, Any]:
    findings = []
    chained = 0

    for index, handoff in enumerate(handoffs):
        claimed_hash = handoff.get("prev_handoff_hash")
        if not claimed_hash:
            continue
        chained += 1
        if claimed_hash == HASH_CHAIN_GENESIS:
            continue
        if index == 0:
            findings.append(
                {
                    "workflow_id": workflow_id,
                    "handoff_id": handoff.get("handoff_id", ""),
                    "expected_hash": HASH_CHAIN_GENESIS,
                    "claimed_hash": claimed_hash,
                    "prev_handoff_id": "",
                }
            )
            continue

        previous = handoffs[index - 1]
        expected_hash = _compute_handoff_hash(previous)
        if claimed_hash != expected_hash:
            findings.append(
                {
                    "workflow_id": workflow_id,
                    "handoff_id": handoff.get("handoff_id", ""),
                    "expected_hash": expected_hash,
                    "claimed_hash": claimed_hash,
                    "prev_handoff_id": previous.get("handoff_id", ""),
                }
            )

    return {
        "workflow_id": workflow_id,
        "handoffs_total": len(handoffs),
        "handoffs_with_chain": chained,
        "tampered": len(findings),
        "findings": findings,
    }


def _build_report(args: argparse.Namespace) -> Dict[str, Any]:
    kg_path = Path(args.kg_path).resolve()
    active = _load_active_kg(kg_path)
    handoffs = [
        item for item in active.get("handoffs", []) if isinstance(item, dict)
    ]
    if args.include_archive:
        handoffs.extend(_load_archived_handoffs(kg_path))

    if args.workflow_id:
        handoffs = [
            handoff
            for handoff in handoffs
            if handoff.get("workflow_id") == args.workflow_id
        ]

    grouped = _group_handoffs(handoffs)
    workflow_reports = [
        _verify_workflow(workflow_id, workflow_handoffs)
        for workflow_id, workflow_handoffs in sorted(grouped.items())
    ]
    findings = [
        finding
        for workflow_report in workflow_reports
        for finding in workflow_report["findings"]
    ]
    total_handoffs = sum(report["handoffs_total"] for report in workflow_reports)
    chained_handoffs = sum(
        report["handoffs_with_chain"] for report in workflow_reports
    )
    return {
        "status": "FAIL" if findings else "PASS",
        "workflows_checked": len(workflow_reports),
        "handoffs_total": total_handoffs,
        "handoffs_with_chain": chained_handoffs,
        "coverage_pct": _pct(chained_handoffs, total_handoffs),
        "baseline": active.get("metadata", {}).get("hash_chain_baseline"),
        "baseline_label": _baseline_label(active.get("metadata", {})),
        "tampered": len(findings),
        "findings": findings,
        "workflows": workflow_reports,
    }


def _print_stdout_report(report: Dict[str, Any]) -> None:
    print(f"KG chain integrity: {report['status']}")
    print(f"- Workflows checked: {report['workflows_checked']}")
    print(f"- Handoffs total: {report['handoffs_total']}")
    print(
        "- Handoffs with chain: "
        f"{report['handoffs_with_chain']} ({report['coverage_pct']}%) - "
        f"{report['baseline_label']}"
    )
    print(f"- Tampered: {report['tampered']}")

    for finding in report["findings"]:
        print(f"  Workflow: {finding['workflow_id']}")
        print(f"  Handoff:  {finding['handoff_id']}")
        print(f"  Expected: {finding['expected_hash']}")
        print(f"  Claimed:  {finding['claimed_hash']}")
        print(f"  Prev:     {finding['prev_handoff_id']}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify Agent KG handoff prev_handoff_hash chain"
    )
    parser.add_argument(
        "--kg-path",
        default=str(DEFAULT_KG_PATH),
        help="Path to active agent_kg.json",
    )
    parser.add_argument(
        "--include-archive",
        action="store_true",
        help="Also load config/agent_kg_archive/*/*.jsonl",
    )
    parser.add_argument("--workflow-id", help="Optional workflow_id filter")
    parser.add_argument(
        "--report",
        choices=("stdout", "json"),
        default="stdout",
        help="Output format",
    )
    parser.add_argument(
        "--exit-on-failure",
        action="store_true",
        help="Exit with code 1 when a mismatch is found",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    report = _build_report(args)
    if args.report == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        _print_stdout_report(report)
    if args.exit_on_failure and report["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
