#!/usr/bin/env python3
"""Read-only hygiene checks for Agent KG and context capsules."""

import argparse
import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
AGENT_KG_PATH = REPO_ROOT / "config" / "agent_kg.json"
CAPSULES_PATH = REPO_ROOT / "config" / "capsules_manifest.json"
INVARIANTS_PATH = REPO_ROOT / "config" / "transitions" / "invariants.json"
APPROVAL_CONTEXT_TYPES = {"approval_request", "approval_gate", "pending_approval"}


def _load_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"required file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"expected JSON object in {path}")
    return data


def _parse_ts(value: object) -> datetime | None:
    if not value:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_date(value: object) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.strptime(str(value), "%Y-%m-%d")
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc)


def _age_days(now: datetime, then: datetime) -> float:
    return max(0.0, (now - then).total_seconds() / 86400.0)


def _workflow_ts(item: dict) -> datetime | None:
    return _parse_ts(item.get("updated_at") or item.get("created_at"))


def _closed_ts(item: dict) -> datetime | None:
    return _parse_ts(item.get("closed_at") or item.get("updated_at") or item.get("created_at"))


def _all_invariant_ids(data: dict) -> set[str]:
    ids: set[str] = set()
    section_names = (
        "global",
        "global_invariants",
        "temporal",
        "temporal_invariants",
        "gpu",
        "gpu_constraints",
    )
    for section_name in section_names:
        section = data.get(section_name, [])
        if not isinstance(section, list):
            continue
        for item in section:
            if isinstance(item, dict) and item.get("id"):
                ids.add(str(item["id"]))
    return ids


def _stale_workflows(agent_kg: dict, now: datetime, threshold: timedelta) -> list[dict]:
    stale = []
    workflows = agent_kg.get("workflows", [])
    if not isinstance(workflows, list):
        return stale
    cutoff = now - threshold
    for item in workflows:
        if not isinstance(item, dict) or str(item.get("status") or "").lower() != "active":
            continue
        ts = _workflow_ts(item)
        if ts is not None and ts < cutoff:
            stale.append(
                {
                    "workflow_id": str(item.get("workflow_id") or ""),
                    "phase": str(item.get("phase") or "N/A"),
                    "age_days": _age_days(now, ts),
                }
            )
    return stale


def _stale_capsules(manifest: dict, now: datetime, threshold: timedelta) -> list[dict]:
    stale = []
    capsules = manifest.get("capsules", [])
    if not isinstance(capsules, list):
        return stale
    today = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    cutoff = today - threshold
    for item in capsules:
        if not isinstance(item, dict):
            continue
        verified = str(item.get("last_verified_against_ssot") or "")
        ts = _parse_date(verified)
        if ts is not None and ts < cutoff:
            stale.append(
                {
                    "id": str(item.get("id") or ""),
                    "last_verified_against_ssot": verified,
                    "age_days": _age_days(now, ts),
                }
            )
    return stale


def _phantom_invariants(manifest: dict, invariants: dict) -> list[dict]:
    known_ids = _all_invariant_ids(invariants)
    findings = []
    capsules = manifest.get("capsules", [])
    if not isinstance(capsules, list):
        return findings
    for item in capsules:
        if not isinstance(item, dict):
            continue
        capsule_id = str(item.get("id") or "")
        capsule_invariants = item.get("invariants", [])
        if not isinstance(capsule_invariants, list):
            continue
        for invariant_id in capsule_invariants:
            invariant = str(invariant_id)
            if invariant and invariant not in known_ids:
                findings.append({"capsule_id": capsule_id, "phantom_id": invariant})
    return findings


def _incomplete_handoffs(agent_kg: dict, now: datetime) -> list[dict]:
    findings = []
    workflows = agent_kg.get("workflows", [])
    handoffs = agent_kg.get("handoffs", [])
    if not isinstance(workflows, list) or not isinstance(handoffs, list):
        return findings
    recent_cutoff = now - timedelta(days=30)
    for item in workflows:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "").lower()
        if status not in {"closed", "cancelled"}:
            continue
        closed_at = _closed_ts(item)
        if closed_at is None or closed_at < recent_cutoff:
            continue
        workflow_id = str(item.get("workflow_id") or "")
        has_orchestrator_handoff = any(
            isinstance(handoff, dict)
            and str(handoff.get("workflow_id") or "") == workflow_id
            and str(handoff.get("agent") or "") == "orchestrator"
            and bool(str(handoff.get("trace_id") or "").strip())
            and bool(str(handoff.get("plan_step_id") or "").strip())
            for handoff in handoffs
        )
        if workflow_id and not has_orchestrator_handoff:
            findings.append({"workflow_id": workflow_id})
    return findings


def _dangling_approval_requests(agent_kg: dict, now: datetime, threshold: timedelta) -> list[dict]:
    findings = []
    workflows = agent_kg.get("workflows", [])
    contexts = agent_kg.get("contexts", [])
    if not isinstance(workflows, list) or not isinstance(contexts, list):
        return findings
    cutoff = now - threshold
    active_ages: dict[str, float] = {}
    for item in workflows:
        if not isinstance(item, dict) or str(item.get("status") or "").lower() != "active":
            continue
        ts = _workflow_ts(item)
        workflow_id = str(item.get("workflow_id") or "")
        if workflow_id and ts is not None and ts < cutoff:
            active_ages[workflow_id] = _age_days(now, ts)
    for item in contexts:
        if not isinstance(item, dict):
            continue
        workflow_id = str(item.get("workflow_id") or "")
        context_type = str(item.get("context_type") or "")
        if workflow_id in active_ages and context_type in APPROVAL_CONTEXT_TYPES:
            findings.append(
                {
                    "workflow_id": workflow_id,
                    "context_type": context_type,
                    "age_days": active_ages[workflow_id],
                }
            )
    return findings


def _build_findings(stale_days: int) -> tuple[dict, datetime]:
    now = datetime.now(timezone.utc)
    threshold = timedelta(days=stale_days)
    agent_kg = _load_json(AGENT_KG_PATH)
    manifest = _load_json(CAPSULES_PATH)
    invariants = _load_json(INVARIANTS_PATH)
    findings = {
        "stale_workflows": _stale_workflows(agent_kg, now, threshold),
        "stale_capsules": _stale_capsules(manifest, now, threshold),
        "phantom_invariants": _phantom_invariants(manifest, invariants),
        "incomplete_handoffs": _incomplete_handoffs(agent_kg, now),
        "dangling_approval_requests": _dangling_approval_requests(agent_kg, now, threshold),
    }
    return findings, now


def _summary_line(findings: dict) -> str:
    return (
        f"Hygiene: {len(findings['stale_workflows'])} stale workflows, "
        f"{len(findings['stale_capsules'])} stale capsules, "
        f"{len(findings['phantom_invariants'])} phantom invariants, "
        f"{len(findings['incomplete_handoffs'])} incomplete handoffs, "
        f"{len(findings['dangling_approval_requests'])} dangling approval_requests."
    )


def _print_section(title: str, items: list[dict], lines: list[str]) -> None:
    lines.append(f"## {title} ({len(items)})")
    if not items:
        lines.append("None.")
        lines.append("")
        return
    for item in items:
        if title == "Stale workflows":
            lines.append(f"- {item['workflow_id']} (phase={item['phase']}, age={item['age_days']:.1f}d)")
        elif title == "Stale capsules":
            lines.append(
                f"- {item['id']} (last_verified_against_ssot={item['last_verified_against_ssot']}, "
                f"age={item['age_days']:.1f}d)"
            )
        elif title == "Phantom invariants":
            lines.append(f"- {item['capsule_id']} references missing {item['phantom_id']}")
        elif title == "Incomplete handoffs":
            lines.append(f"- {item['workflow_id']} (closed but no orchestrator handoff with trace_id+plan_step_id)")
        elif title == "Dangling approval_requests":
            lines.append(f"- {item['workflow_id']} ({item['context_type']}, age={item['age_days']:.1f}d)")
    lines.append("")


def _detailed_report(findings: dict, now: datetime, stale_days: int) -> str:
    lines = [
        "# Hygiene Check Report",
        "",
        f"Generated: {now.strftime('%Y-%m-%dT%H:%M:%SZ')}",
        f"Stale threshold: {stale_days} day(s)",
        "",
        "## Summary",
        f"- Stale workflows: {len(findings['stale_workflows'])}",
        f"- Stale capsules: {len(findings['stale_capsules'])}",
        f"- Phantom invariants: {len(findings['phantom_invariants'])}",
        f"- Incomplete handoffs: {len(findings['incomplete_handoffs'])}",
        f"- Dangling approval_requests: {len(findings['dangling_approval_requests'])}",
        "",
    ]
    _print_section("Stale workflows", findings["stale_workflows"], lines)
    _print_section("Stale capsules", findings["stale_capsules"], lines)
    _print_section("Phantom invariants", findings["phantom_invariants"], lines)
    _print_section("Incomplete handoffs", findings["incomplete_handoffs"], lines)
    _print_section("Dangling approval_requests", findings["dangling_approval_requests"], lines)
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only Agent KG and capsule hygiene check")
    parser.add_argument("--stale-days", type=int, default=1, help="Staleness threshold in days")
    parser.add_argument("--summary-only", action="store_true", help="Print counts only")
    parser.add_argument("--no-color", action="store_true", help="Disable ANSI color output")
    args = parser.parse_args()
    if args.stale_days < 0:
        print("error: --stale-days must be non-negative", file=sys.stderr)
        return 2

    try:
        findings, now = _build_findings(args.stale_days)
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.summary_only:
        print(_summary_line(findings))
    else:
        sys.stdout.write(_detailed_report(findings, now, args.stale_days))

    has_findings = any(findings[key] for key in findings)
    return 1 if has_findings else 0


if __name__ == "__main__":
    sys.exit(main())
