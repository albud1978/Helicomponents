#!/usr/bin/env python3
"""Read-only token usage analytics for Agent KG.

Запускается по команде Алексея, не интегрирован в hygiene или другие хуки.
Помогает понять token-расход по модели/агенту/risk-tier для оптимизации.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
KG_PATH = REPO_ROOT / "config" / "agent_kg.json"
ISSUE_TYPES = (
    "governance_verbose",
    "docs_curator_over_trigger",
    "handoff_bloat",
    "goal_duplicate_legacy",
    "verbose_checklist_pre_s3",
    "low_risk_with_na_boilerplate_pre_s5",
)


def _load_kg(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _aggregate(handoffs: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_model = defaultdict(lambda: {"count": 0, "tokens": 0})
    by_agent = defaultdict(lambda: {"count": 0, "tokens": 0})
    by_risk = defaultdict(lambda: {"count": 0, "tokens": 0})
    by_source = defaultdict(int)
    total_count = 0
    total_with_usage = 0
    total_tokens = 0

    for h in handoffs:
        total_count += 1
        usage = h.get("usage") or {}
        if not isinstance(usage, dict):
            continue
        if usage:
            total_with_usage += 1
        model = usage.get("model") or "unknown"
        tokens = usage.get("est_tokens") or 0
        if not isinstance(tokens, int):
            tokens = 0
        source = usage.get("source") or "unknown"
        agent = h.get("agent") or "unknown"
        risk = h.get("risk_tier") or "unknown"

        by_model[model]["count"] += 1
        by_model[model]["tokens"] += tokens
        by_agent[agent]["count"] += 1
        by_agent[agent]["tokens"] += tokens
        by_risk[risk]["count"] += 1
        by_risk[risk]["tokens"] += tokens
        by_source[source] += 1
        total_tokens += tokens

    return {
        "total_handoffs": total_count,
        "with_usage": total_with_usage,
        "total_tokens": total_tokens,
        "by_model": dict(by_model),
        "by_agent": dict(by_agent),
        "by_risk": dict(by_risk),
        "by_source": dict(by_source),
    }


def _usage_tokens(handoff: Dict[str, Any]) -> int:
    usage = handoff.get("usage") or {}
    if not isinstance(usage, dict):
        return 0
    tokens = usage.get("est_tokens") or 0
    return tokens if isinstance(tokens, int) else 0


def _workflow_by_id(kg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    workflows = kg.get("workflows", [])
    if not isinstance(workflows, list):
        return {}
    return {
        workflow.get("workflow_id"): workflow
        for workflow in workflows
        if isinstance(workflow, dict) and workflow.get("workflow_id")
    }


def _workflow_risk_tier(
    workflow: Dict[str, Any], handoff: Dict[str, Any]
) -> Optional[str]:
    risk_tier = workflow.get("risk_tier")
    if isinstance(risk_tier, str) and risk_tier:
        return risk_tier
    risk_tier = handoff.get("risk_tier")
    return risk_tier if isinstance(risk_tier, str) and risk_tier else None


def _has_standalone_na(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    for line in value.splitlines() or [value]:
        stripped = line.strip()
        if stripped == "N/A" or stripped.startswith("N/A ") or stripped.startswith("N/A("):
            return True
    return False


def _issue(
    issue_type: str,
    workflow_id: str,
    handoff_id: str,
    message: str,
) -> Dict[str, str]:
    return {
        "type": issue_type,
        "workflow_id": workflow_id,
        "handoff_id": handoff_id,
        "line": message,
    }


def _detect_issues(
    kg: Dict[str, Any],
    handoffs: List[Dict[str, Any]],
    issue_types: set[str],
) -> List[Dict[str, str]]:
    workflows = _workflow_by_id(kg)
    findings: List[Dict[str, str]] = []

    for handoff in handoffs:
        if not isinstance(handoff, dict):
            continue
        workflow_id = str(handoff.get("workflow_id") or "unknown")
        handoff_id = str(handoff.get("handoff_id") or "unknown")
        agent = str(handoff.get("agent") or "unknown")
        workflow = workflows.get(workflow_id, {})
        risk_tier = _workflow_risk_tier(workflow, handoff)
        est_tokens = _usage_tokens(handoff)

        if (
            "governance_verbose" in issue_types
            and agent == "governance-compliance"
            and est_tokens > 1500
        ):
            findings.append(
                _issue(
                    "governance_verbose",
                    workflow_id,
                    handoff_id,
                    f"[governance_verbose] workflow={workflow_id} "
                    f"handoff={handoff_id} est_tokens={est_tokens} (target <1500)",
                )
            )

        if (
            "docs_curator_over_trigger" in issue_types
            and agent == "docs-curator"
            and risk_tier == "medium"
        ):
            findings.append(
                _issue(
                    "docs_curator_over_trigger",
                    workflow_id,
                    handoff_id,
                    f"[docs_curator_over_trigger] workflow={workflow_id} "
                    f"handoff={handoff_id} risk=medium",
                )
            )

        if "handoff_bloat" in issue_types and est_tokens > 2500:
            findings.append(
                _issue(
                    "handoff_bloat",
                    workflow_id,
                    handoff_id,
                    f"[handoff_bloat] workflow={workflow_id} handoff={handoff_id} "
                    f"agent={agent} est_tokens={est_tokens} (target <2500)",
                )
            )

        if (
            "goal_duplicate_legacy" in issue_types
            and "goal" in handoff
            and handoff.get("goal") == handoff.get("user_goal")
        ):
            findings.append(
                _issue(
                    "goal_duplicate_legacy",
                    workflow_id,
                    handoff_id,
                    f"[goal_duplicate_legacy] workflow={workflow_id} "
                    f"handoff={handoff_id} (pre-S1)",
                )
            )

        checklist = handoff.get("compliance_checklist")
        if (
            "verbose_checklist_pre_s3" in issue_types
            and isinstance(checklist, str)
            and (
                "policy_status=" in checklist
                or "scope_match=" in checklist
                or "traceability_status=" in checklist
            )
        ):
            findings.append(
                _issue(
                    "verbose_checklist_pre_s3",
                    workflow_id,
                    handoff_id,
                    f"[verbose_checklist_pre_s3] workflow={workflow_id} "
                    f"handoff={handoff_id} agent={agent}",
                )
            )

        if "low_risk_with_na_boilerplate_pre_s5" in issue_types and risk_tier == "low":
            fields_with_na = [
                field
                for field in (
                    "plan_card",
                    "evidence_pack",
                    "compliance_checklist",
                    "approval_gate",
                )
                if _has_standalone_na(handoff.get(field))
            ]
            if fields_with_na:
                findings.append(
                    _issue(
                        "low_risk_with_na_boilerplate_pre_s5",
                        workflow_id,
                        handoff_id,
                        f"[low_risk_with_na_boilerplate_pre_s5] workflow={workflow_id} "
                        f"handoff={handoff_id} fields_with_na={','.join(fields_with_na)}",
                    )
                )

    return findings


def _parse_issue_types(raw: Optional[str]) -> set[str]:
    if not raw:
        return set(ISSUE_TYPES)
    requested = {item.strip() for item in raw.split(",") if item.strip()}
    unknown = requested - set(ISSUE_TYPES)
    if unknown:
        raise ValueError(f"unknown issue type(s): {', '.join(sorted(unknown))}")
    return requested


def _print_issue_summary(findings: List[Dict[str, str]], issue_types: set[str]) -> None:
    counts = Counter(finding["type"] for finding in findings)
    print(f"Total findings: {len(findings)}")
    print("By issue type:")
    for issue_type in ISSUE_TYPES:
        if issue_type in issue_types:
            print(f"  {issue_type}: {counts.get(issue_type, 0)}")


def _pct(used: float, cap: float) -> Optional[float]:
    if cap == 0:
        return 0.0 if used == 0 else 100.0
    return round((used / cap) * 100, 2)


def _utilization(caps: Any, usage: Any, token_fallback: int = 0) -> Dict[str, Optional[float]]:
    if not isinstance(caps, dict):
        return {"steps": None, "tokens": None, "cost": None}

    usage_dict = usage if isinstance(usage, dict) else {}
    values = {
        "steps": ("max_steps", usage_dict.get("cumulative_steps", 0)),
        "tokens": ("max_tokens", usage_dict.get("cumulative_tokens", token_fallback)),
        "cost": ("max_cost", usage_dict.get("cumulative_cost", 0.0)),
    }
    result: Dict[str, Optional[float]] = {}
    for name, (cap_key, used_value) in values.items():
        cap_value = caps.get(cap_key)
        if cap_value is None:
            result[name] = None
        else:
            result[name] = _pct(float(used_value), float(cap_value))
    return result


def _workflow_rows(
    kg: Dict[str, Any], top: int, highlight_threshold: int
) -> List[Dict[str, Any]]:
    workflows_raw = kg.get("workflows", [])
    handoffs_raw = kg.get("handoffs", [])
    workflows = workflows_raw if isinstance(workflows_raw, list) else []
    handoffs = handoffs_raw if isinstance(handoffs_raw, list) else []
    workflow_by_id = {
        w.get("workflow_id"): w
        for w in workflows
        if isinstance(w, dict) and w.get("workflow_id")
    }
    workflow_ids = set(workflow_by_id)
    workflow_ids.update(
        h.get("workflow_id")
        for h in handoffs
        if isinstance(h, dict) and h.get("workflow_id")
    )

    rows: List[Dict[str, Any]] = []
    for workflow_id in workflow_ids:
        workflow = workflow_by_id.get(workflow_id, {})
        wf_handoffs = [
            h for h in handoffs if isinstance(h, dict) and h.get("workflow_id") == workflow_id
        ]
        total_tokens = sum(_usage_tokens(h) for h in wf_handoffs)
        caps = workflow.get("caps") if isinstance(workflow.get("caps"), dict) else None
        usage = workflow.get("usage") if isinstance(workflow.get("usage"), dict) else None
        utilization = _utilization(caps, usage, token_fallback=total_tokens)
        goal = str(workflow.get("goal") or "")
        rows.append(
            {
                "workflow_id": workflow_id,
                "goal": goal[:80],
                "handoff_count": len(wf_handoffs),
                "total_tokens": total_tokens,
                "over_threshold": total_tokens >= highlight_threshold,
                "caps_max_tokens": caps.get("max_tokens") if caps else None,
                "utilization_pct": utilization["tokens"],
            }
        )

    rows.sort(key=lambda row: (-row["total_tokens"], row["workflow_id"]))
    return rows[:top]


def _workflow_summary(kg: Dict[str, Any], workflow_id: str) -> Optional[Dict[str, Any]]:
    workflows = kg.get("workflows", [])
    handoffs = kg.get("handoffs", [])
    if not isinstance(workflows, list) or not isinstance(handoffs, list):
        return None

    workflow = next(
        (
            w
            for w in workflows
            if isinstance(w, dict) and w.get("workflow_id") == workflow_id
        ),
        None,
    )
    if workflow is None:
        return None

    wf_handoffs = [
        h for h in handoffs if isinstance(h, dict) and h.get("workflow_id") == workflow_id
    ]
    handoffs_with_usage = [
        {
            "handoff_id": h.get("handoff_id"),
            "agent": h.get("agent"),
            "risk_tier": h.get("risk_tier"),
            "created_at": h.get("created_at"),
            "usage": h.get("usage"),
        }
        for h in wf_handoffs
        if isinstance(h.get("usage"), dict) and h.get("usage")
    ]
    total_tokens = sum(_usage_tokens(h) for h in wf_handoffs)
    caps = workflow.get("caps") if isinstance(workflow.get("caps"), dict) else None
    usage = workflow.get("usage") if isinstance(workflow.get("usage"), dict) else None
    return {
        "workflow_id": workflow_id,
        "goal": workflow.get("goal"),
        "phase": workflow.get("phase"),
        "status": workflow.get("status"),
        "caps": caps,
        "usage": usage,
        "handoff_count": len(wf_handoffs),
        "handoffs_with_usage": handoffs_with_usage,
        "total_tokens": total_tokens,
        "utilization_pct": _utilization(caps, usage, token_fallback=total_tokens),
    }


def _format_section(title: str, data: Dict[str, Any], with_tokens: bool = True) -> str:
    if not data:
        return f"### {title}\n(empty)\n"
    lines = [f"### {title}\n"]
    sorted_items = sorted(
        data.items(),
        key=lambda kv: -(
            kv[1].get("tokens", 0) if isinstance(kv[1], dict) else kv[1]
        ),
    )
    for key, val in sorted_items:
        if isinstance(val, dict):
            count = val["count"]
            tokens = val["tokens"]
            avg = tokens // count if count else 0
            tok_str = f"~{tokens:,} tokens" if with_tokens else ""
            avg_str = f", avg ~{avg:,}/handoff" if with_tokens and count else ""
            lines.append(
                f"- **{key}**: {count} handoffs"
                f"{', ' + tok_str if tok_str else ''}{avg_str}"
            )
        else:
            lines.append(f"- **{key}**: {val} handoffs")
    return "\n".join(lines) + "\n"


def _format_by_workflow(rows: List[Dict[str, Any]], highlight_threshold: int) -> str:
    lines = ["# Token usage by workflow\n"]
    if not rows:
        return (
            "# Token usage by workflow\n\n(empty)\n\n"
            f"> 0 workflow(s) >= порог {highlight_threshold} est_tokens.\n"
        )
    over_count = 0
    for row in rows:
        if row["over_threshold"]:
            over_count += 1
        cap = row["caps_max_tokens"]
        cap_str = f"cap max_tokens={cap:,}" if isinstance(cap, int) else "cap max_tokens=N/A"
        util = row["utilization_pct"]
        util_str = f"{util}%" if util is not None else "N/A"
        marker = "⚠ " if row["over_threshold"] else ""
        lines.append(
            f"- {marker}**{row['workflow_id']}**: ~{row['total_tokens']:,} tokens, "
            f"{row['handoff_count']} handoffs, {cap_str}, utilization={util_str}; "
            f"goal: {row['goal']}"
        )
    lines.append("")
    lines.append(f"> {over_count} workflow(s) >= порог {highlight_threshold} est_tokens.")
    return "\n".join(lines) + "\n"


def _format_workflow_summary(summary: Dict[str, Any]) -> str:
    lines = [
        f"# Workflow token summary: {summary['workflow_id']}\n",
        f"**Goal**: {summary.get('goal') or ''}",
        f"**Phase**: {summary.get('phase') or ''}",
        f"**Status**: {summary.get('status') or ''}",
        f"**Caps**: `{json.dumps(summary.get('caps'), ensure_ascii=False)}`",
        f"**Usage**: `{json.dumps(summary.get('usage'), ensure_ascii=False)}`",
        f"**Total estimated tokens from handoffs**: ~{summary['total_tokens']:,}",
        f"**Utilization pct**: `{json.dumps(summary['utilization_pct'], ensure_ascii=False)}`",
        "\n### Handoffs with usage\n",
    ]
    if not summary["handoffs_with_usage"]:
        lines.append("(empty)")
    for handoff in summary["handoffs_with_usage"]:
        usage = handoff["usage"] or {}
        lines.append(
            f"- `{handoff.get('handoff_id')}`: agent={handoff.get('agent')}, "
            f"risk={handoff.get('risk_tier')}, created={handoff.get('created_at')}, "
            f"model={usage.get('model', 'unknown')}, "
            f"est_tokens={usage.get('est_tokens', 0)}, source={usage.get('source', 'unknown')}"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Token usage analytics (read-only)")
    parser.add_argument("--workflow-id", help="Filter to single workflow")
    parser.add_argument("--summary-only", action="store_true", help="One-line summary")
    parser.add_argument("--by-workflow", action="store_true", help="Top workflows by token usage")
    parser.add_argument("--workflow-summary", help="Detailed report for one workflow")
    parser.add_argument("--top", type=int, default=20, help="Top-N limit for --by-workflow")
    parser.add_argument(
        "--highlight-threshold",
        type=int,
        default=150000,
        help="Tier-L: порог est_tokens/workflow для подсветки (soft)",
    )
    parser.add_argument("--export-json", action="store_true", help="Print machine-readable JSON")
    parser.add_argument("--show-issues", action="store_true", help="Show Tier-S/Tier-M token hygiene findings")
    parser.add_argument(
        "--exit-on-issues",
        action="store_true",
        help="Return exit 1 when --show-issues finds any issue",
    )
    parser.add_argument(
        "--issue-types",
        help="Comma-separated issue types to include for --show-issues",
    )
    args = parser.parse_args()
    if args.by_workflow and args.workflow_summary:
        parser.error("--by-workflow и --workflow-summary нельзя использовать вместе")
    if args.top < 1:
        parser.error("--top должен быть >= 1")
    if args.exit_on_issues and not args.show_issues:
        parser.error("--exit-on-issues требует --show-issues")

    try:
        kg = _load_kg(KG_PATH)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Error reading {KG_PATH}: {exc}", file=sys.stderr)
        return 2

    handoffs = kg.get("handoffs", [])
    if not isinstance(handoffs, list):
        print("KG.handoffs malformed", file=sys.stderr)
        return 2

    if args.workflow_id:
        handoffs = [h for h in handoffs if h.get("workflow_id") == args.workflow_id]

    if args.show_issues:
        try:
            issue_types = _parse_issue_types(args.issue_types)
        except ValueError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 2
        findings = _detect_issues(kg, handoffs, issue_types)
        if args.export_json:
            print(json.dumps({"findings": findings}, ensure_ascii=False, indent=2))
            return 1 if args.exit_on_issues and findings else 0
        if not args.summary_only:
            for finding in findings:
                print(finding["line"])
        _print_issue_summary(findings, issue_types)
        return 1 if args.exit_on_issues and findings else 0

    if args.by_workflow:
        rows = _workflow_rows(kg, args.top, args.highlight_threshold)
        if args.export_json:
            print(json.dumps({"by_workflow": rows}, ensure_ascii=False, indent=2))
        else:
            print(_format_by_workflow(rows, args.highlight_threshold))
        return 0

    if args.workflow_summary:
        summary = _workflow_summary(kg, args.workflow_summary)
        if summary is None:
            print(f"Workflow not found: {args.workflow_summary}", file=sys.stderr)
            return 1
        if args.export_json:
            print(json.dumps(summary, ensure_ascii=False, indent=2))
        else:
            print(_format_workflow_summary(summary))
        return 0

    agg = _aggregate(handoffs)
    total = agg["total_handoffs"]
    with_usage = agg["with_usage"]
    coverage = (with_usage * 100 // total) if total else 0

    if args.summary_only:
        print(
            f"Token analytics: {total} handoffs, {with_usage} с usage ({coverage}%), "
            f"total ~{agg['total_tokens']:,} est_tokens"
        )
        return 0

    if args.export_json:
        agg["coverage_pct"] = coverage
        print(json.dumps(agg, ensure_ascii=False, indent=2))
        return 0

    print("# Token usage analytics\n")
    print(f"**Scope**: {total} handoffs total, {with_usage} с usage block ({coverage}% coverage)")
    print(f"**Total estimated tokens**: ~{agg['total_tokens']:,}\n")
    print(_format_section("By model", agg["by_model"]))
    print(_format_section("By agent", agg["by_agent"]))
    print(_format_section("By risk_tier", agg["by_risk"]))
    print(_format_section("Source breakdown", agg["by_source"], with_tokens=False))

    if coverage < 50 and total > 5:
        print(
            f"\n> NOTE: usage coverage = {coverage}%. Большинство handoffs без `usage` block — "
            "усилить отчётность субагентов или дать orchestrator char-heuristic fallback."
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
