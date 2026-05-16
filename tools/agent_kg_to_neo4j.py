#!/usr/bin/env python3
"""
On-demand projection of Agent KG JSON/JSONL records into Neo4j.

Agent KG remains the source of truth in config/agent_kg.json and JSONL archives;
Neo4j is only a read-only visualization/query projection.

Examples:
    python3 tools/agent_kg_to_neo4j.py --dry-run
    python3 tools/agent_kg_to_neo4j.py --include-archive --reset
    DOMAIN_NEO4J_PASSWORD=secret python3 tools/agent_kg_to_neo4j.py
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Set

from neo4j import GraphDatabase


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_KG_PATH = ROOT_DIR / "config" / "agent_kg.json"
DEFAULT_ARCHIVE_DIR = ROOT_DIR / "config" / "agent_kg_archive"

CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (w:Workflow) REQUIRE w.id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (h:Handoff) REQUIRE h.id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Context) REQUIRE c.id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Agent) REQUIRE a.name IS UNIQUE",
]

RESET_QUERY = "MATCH (n) WHERE n:Workflow OR n:Handoff OR n:Context OR n:Agent DETACH DELETE n"

WORKFLOW_QUERY = """
MERGE (w:Workflow {id: $id})
SET w.goal = $goal, w.status = $status, w.owner = $owner, w.phase = $phase,
    w.created_at = $created_at, w.updated_at = $updated_at, w.source = $source,
    w.risk_tier = $risk_tier, w.cumulative_tokens = $cumulative_tokens,
    w.cumulative_steps = $cumulative_steps, w.max_steps = $max_steps,
    w.max_tokens = $max_tokens, w.profile = $profile,
    w.utilization_tokens_pct = $util_tokens, w.utilization_steps_pct = $util_steps,
    w.last_usage_updated = $last_usage_updated
WITH w MATCH (a:Agent {name: $owner}) MERGE (w)-[:OWNED_BY]->(a)
"""

HANDOFF_QUERY = """
MERGE (h:Handoff {id: $id})
SET h.agent = $agent, h.plan_step_id = $plan_step_id, h.trace_id = $trace_id,
    h.risk_tier = $risk_tier, h.risk_owner = $risk_owner,
    h.risk_validated_by = $risk_validated_by, h.approval_status = $approval_status,
    h.human_gate_required = $human_gate_required, h.graph_update = $graph_update,
    h.drift_check = $drift_check, h.success_criteria = $success_criteria,
    h.created_at = $created_at, h.est_tokens = $est_tokens,
    h.model = $model, h.usage_source = $usage_source,
    h.next_owner_name = $next_owner, h.evidence_pack_len = $evidence_pack_len,
    h.changes_len = $changes_len, h.synthesized_legacy = $synthesized_legacy,
    h.cc_decision = $cc_decision, h.cc_required_gates = $cc_required_gates,
    h.cc_exceptions = $cc_exceptions, h.cc_approval_ref = $cc_approval_ref,
    h.prev_handoff_hash = $prev_handoff_hash
WITH h MATCH (w:Workflow {id: $workflow_id}) MERGE (w)-[:HAS_HANDOFF]->(h)
WITH h MATCH (a:Agent {name: $agent}) MERGE (h)-[:BY_AGENT]->(a)
"""

CONTEXT_QUERY = """
MERGE (c:Context {id: $id})
SET c.context_type = $context_type, c.agent = $agent,
    c.created_at = $created_at, c.updated_at = $updated_at,
    c.content_len = $content_len, c.content_type = $content_type
WITH c MATCH (w:Workflow {id: $workflow_id}) MERGE (w)-[:HAS_CONTEXT]->(c)
"""

NEXT_OWNER_QUERY = """
MATCH (h:Handoff {id: $handoff_id})
MATCH (a:Agent {name: $next_owner})
MERGE (h)-[:NEXT_OWNER]->(a)
"""

Projection = Dict[str, List[Dict[str, Any]]]


def _empty_projection() -> Projection:
    return {"workflows": [], "handoffs": [], "contexts": []}


def _add_agent(names: Set[str], raw_name: Any) -> None:
    if raw_name is not None and str(raw_name).strip():
        names.add(str(raw_name).strip())


def _agents(data: Projection) -> Set[str]:
    names: Set[str] = set()
    for workflow in data["workflows"]:
        _add_agent(names, workflow.get("owner"))
    for handoff in data["handoffs"]:
        _add_agent(names, handoff.get("agent"))
        next_owner = handoff.get("next_owner")
        if next_owner and str(next_owner).strip().lower() != "human":
            _add_agent(names, next_owner)
    for context in data["contexts"]:
        _add_agent(names, context.get("agent"))
    return names


def _summary(data: Projection) -> str:
    return (
        f"workflows={len(data['workflows'])} "
        f"handoffs={len(data['handoffs'])} "
        f"contexts={len(data['contexts'])} "
        f"agents={len(_agents(data))} "
        f"synthesized_legacy_ids={_synthesized_legacy_ids(data)}"
    )


def _require(record: Dict[str, Any], key: str, record_type: str) -> str:
    value = record.get(key)
    if value is None or str(value).strip() == "":
        raise ValueError(f"{record_type} record without required key: {key}")
    return str(value)


def _synthesized_legacy_ids(data: Projection) -> int:
    return sum(1 for handoff in data["handoffs"] if handoff.get("synthetic_legacy_id"))


def _prepare_archive_handoff(record: Dict[str, Any]) -> Dict[str, Any]:
    item = dict(record)
    if item.get("handoff_id"):
        return item

    # Legacy archive records (pre-handoff_id era) get deterministic synthetic ids;
    # this is an explicit transformation, not a silent fallback.
    workflow_id = _require(item, "workflow_id", "archive handoff")
    created_at = _require(item, "created_at", "archive handoff")
    agent = str(item.get("agent") or "")
    digest = hashlib.sha1(f"{workflow_id}{agent}{created_at}".encode("utf-8")).hexdigest()
    item["handoff_id"] = f"legacy_{digest[:12]}"
    item["synthetic_legacy_id"] = True
    return item


def _int_or_none(value: Any) -> Any:
    return value if isinstance(value, int) else None


def _usage_est_tokens(usage: Any) -> Any:
    if not isinstance(usage, dict):
        return None
    # Tier-2b writes est_tokens; total_tokens is kept only for legacy records.
    value = usage.get("est_tokens") if usage.get("est_tokens") is not None else usage.get("total_tokens")
    return _int_or_none(value)


def _utilization_pct(current: Any, maximum: Any) -> Any:
    current_int = _int_or_none(current)
    maximum_int = _int_or_none(maximum)
    if current_int is None or maximum_int is None or maximum_int <= 0:
        return None
    return round((current_int / maximum_int) * 100, 2)


def _parse_compliance_checklist(text: Any) -> Dict[str, Any]:
    """Parse Tier-S S3 compact compliance_checklist into discrete fields."""
    if not isinstance(text, str) or not text.strip():
        return {}
    out = {}
    for part in text.split(";"):
        part = part.strip()
        if "=" not in part:
            continue
        key, _, value = part.partition("=")
        key = key.strip()
        value = value.strip()
        if key in ("decision", "required_gates", "exceptions", "evidence_refs", "approval_ref"):
            out[key] = value if value else None
    return out


def _content_len(content: Any) -> int:
    if isinstance(content, str):
        return len(content)
    return 0 if content is None else len(json.dumps(content, ensure_ascii=False))


def _load_active(path: Path) -> Projection:
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    data = _empty_projection()
    for workflow in raw.get("workflows", []):
        item = dict(workflow)
        item["source"] = "active"
        data["workflows"].append(item)
    for handoff in raw.get("handoffs", []):
        _require(handoff, "handoff_id", "active handoff")
        data["handoffs"].append(handoff)
    data["contexts"].extend(raw.get("contexts", []))
    return data


def _load_archive(archive_dir: Path) -> Projection:
    data = _empty_projection()
    if not archive_dir.exists():
        return data
    for jsonl_path in sorted(archive_dir.glob("**/*.jsonl")):
        with jsonl_path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                if not raw_line.strip():
                    continue
                record = json.loads(raw_line)
                record_type = record.get("type")
                item = {k: v for k, v in record.items() if k != "type"}
                if record_type == "workflow":
                    item["source"] = "archive"
                    data["workflows"].append(item)
                elif record_type == "handoff":
                    data["handoffs"].append(_prepare_archive_handoff(item))
                elif record_type == "context":
                    data["contexts"].append(item)
    return data


def _merge(active: Projection, archive: Projection) -> Projection:
    active_ids = {workflow.get("workflow_id") for workflow in active["workflows"]}
    data = _empty_projection()
    data["workflows"].extend(active["workflows"])
    data["workflows"].extend(
        w for w in archive["workflows"] if w.get("workflow_id") not in active_ids
    )
    data["handoffs"].extend([*active["handoffs"], *archive["handoffs"]])
    data["contexts"].extend([*active["contexts"], *archive["contexts"]])
    return data


def _project(settings: argparse.Namespace, data: Projection) -> None:
    with GraphDatabase.driver(
        settings.uri,
        auth=(settings.user, settings.password),
        connection_timeout=5,
    ) as driver:
        driver.verify_connectivity()
        with driver.session(database=settings.db) as session:
            for query in CONSTRAINTS:
                session.run(query)
            print("constraints=ok")
            if settings.reset:
                session.run(RESET_QUERY)
                print("reset=ok")
            for name in sorted(_agents(data)):
                kind = "orchestrator" if name == "orchestrator" else "subagent"
                session.run("MERGE (a:Agent {name: $name}) SET a.kind = $kind", name=name, kind=kind)
            _write_workflows(session, data["workflows"])
            _write_handoffs(session, data["handoffs"])
            _write_contexts(session, data["contexts"])
    print(f"projected: {_summary(data)}")


def _write_workflows(session: Any, workflows: List[Dict[str, Any]]) -> None:
    for workflow in workflows:
        usage = workflow.get("usage") if isinstance(workflow.get("usage"), dict) else {}
        caps = workflow.get("caps") if isinstance(workflow.get("caps"), dict) else {}
        cumulative_tokens = _int_or_none(usage.get("cumulative_tokens"))
        cumulative_steps = _int_or_none(usage.get("cumulative_steps"))
        max_tokens = _int_or_none(caps.get("max_tokens"))
        max_steps = _int_or_none(caps.get("max_steps"))
        session.run(
            WORKFLOW_QUERY,
            id=_require(workflow, "workflow_id", "workflow"),
            goal=workflow.get("goal"),
            status=workflow.get("status"),
            owner=workflow.get("owner"),
            phase=workflow.get("phase"),
            created_at=workflow.get("created_at"),
            updated_at=workflow.get("updated_at"),
            source=workflow.get("source", "active"),
            risk_tier=workflow.get("risk_tier"),
            cumulative_tokens=cumulative_tokens,
            cumulative_steps=cumulative_steps,
            max_steps=max_steps,
            max_tokens=max_tokens,
            profile=caps.get("profile"),
            util_tokens=_utilization_pct(cumulative_tokens, max_tokens),
            util_steps=_utilization_pct(cumulative_steps, max_steps),
            last_usage_updated=usage.get("last_updated"),
        )


def _write_handoffs(session: Any, handoffs: List[Dict[str, Any]]) -> None:
    for handoff in handoffs:
        handoff_id = _require(handoff, "handoff_id", "handoff")
        usage = handoff.get("usage") if isinstance(handoff.get("usage"), dict) else {}
        compliance = _parse_compliance_checklist(handoff.get("compliance_checklist"))
        next_owner = handoff.get("next_owner")
        session.run(
            HANDOFF_QUERY,
            id=handoff_id,
            agent=handoff.get("agent"),
            plan_step_id=handoff.get("plan_step_id"),
            trace_id=handoff.get("trace_id"),
            risk_tier=handoff.get("risk_tier"),
            risk_owner=handoff.get("risk_owner"),
            risk_validated_by=handoff.get("risk_validated_by"),
            approval_status=handoff.get("approval_status"),
            human_gate_required=handoff.get("human_gate_required"),
            graph_update=handoff.get("graph_update"),
            drift_check=handoff.get("drift_check"),
            success_criteria=handoff.get("success_criteria"),
            created_at=handoff.get("created_at"),
            est_tokens=_usage_est_tokens(usage),
            model=usage.get("model"),
            usage_source=usage.get("source"),
            next_owner=next_owner,
            evidence_pack_len=_content_len(handoff.get("evidence_pack")),
            changes_len=_content_len(handoff.get("changes")),
            synthesized_legacy=bool(handoff.get("synthetic_legacy_id")),
            cc_decision=compliance.get("decision"),
            cc_required_gates=compliance.get("required_gates"),
            cc_exceptions=compliance.get("exceptions"),
            cc_approval_ref=compliance.get("approval_ref"),
            prev_handoff_hash=handoff.get("prev_handoff_hash"),
            workflow_id=handoff.get("workflow_id"),
        )
        if next_owner and str(next_owner).strip().lower() != "human":
            session.run(NEXT_OWNER_QUERY, handoff_id=handoff_id, next_owner=str(next_owner).strip())


def _write_contexts(session: Any, contexts: List[Dict[str, Any]]) -> None:
    for context in contexts:
        content = context.get("content")
        session.run(
            CONTEXT_QUERY,
            id=_require(context, "context_id", "context"),
            context_type=context.get("context_type"),
            agent=context.get("agent"),
            created_at=context.get("created_at"),
            updated_at=context.get("updated_at"),
            content_len=_content_len(content),
            content_type="none" if content is None else type(content).__name__,
            workflow_id=context.get("workflow_id"),
        )


def _settings(args: argparse.Namespace) -> argparse.Namespace:
    args.uri = args.uri or os.getenv("DOMAIN_NEO4J_URI") or "bolt://localhost:7687"
    args.user = args.user or os.getenv("DOMAIN_NEO4J_USER") or "neo4j"
    args.password = args.password or os.getenv("DOMAIN_NEO4J_PASSWORD") or "changeme-on-first-login"
    args.db = args.db or os.getenv("DOMAIN_NEO4J_DB") or "neo4j"
    args.kg_path = Path(args.kg_path)
    args.archive_dir = Path(args.archive_dir)
    return args


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Project Agent KG JSON/JSONL into Neo4j.")
    parser.add_argument("--uri", default=None, help="Neo4j URI")
    parser.add_argument("--user", default=None, help="Neo4j user")
    parser.add_argument("--password", default=None, help="Neo4j password")
    parser.add_argument("--db", default=None, help="Neo4j database")
    parser.add_argument("--kg-path", default=str(DEFAULT_KG_PATH), help="Agent KG JSON")
    parser.add_argument("--archive-dir", default=str(DEFAULT_ARCHIVE_DIR), help="Agent KG JSONL archive")
    parser.add_argument("--include-archive", action="store_true", help="Include archive JSONL records")
    parser.add_argument("--dry-run", action="store_true", help="Count records only")
    parser.add_argument("--reset", action="store_true", help="Delete projection nodes before writing")
    return parser


def main() -> None:
    settings = _settings(build_parser().parse_args())
    active = _load_active(settings.kg_path)
    print(f"active: {_summary(active)}")
    archive = _load_archive(settings.archive_dir) if settings.include_archive else _empty_projection()
    if settings.include_archive:
        print(f"archive: {_summary(archive)}")
    data = _merge(active, archive)
    print(f"total: {_summary(data)}")
    if settings.dry_run:
        print("dry_run=true")
        return
    _project(settings, data)


if __name__ == "__main__":
    main()
