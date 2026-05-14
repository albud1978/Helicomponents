#!/usr/bin/env python3
"""Read-only token usage analytics for Agent KG.

Запускается по команде Алексея, не интегрирован в hygiene или другие хуки.
Помогает понять token-расход по модели/агенту/risk-tier для оптимизации.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
KG_PATH = REPO_ROOT / "config" / "agent_kg.json"


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


def main() -> int:
    parser = argparse.ArgumentParser(description="Token usage analytics (read-only)")
    parser.add_argument("--workflow-id", help="Filter to single workflow")
    parser.add_argument("--summary-only", action="store_true", help="One-line summary")
    args = parser.parse_args()

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
