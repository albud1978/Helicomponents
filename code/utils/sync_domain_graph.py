#!/usr/bin/env python3
"""
Синхронизация доменного графа (JSON SSoT -> Neo4j Aura).

Читает канонические JSON:
  - config/transitions/transitions_rules.json  (переходы, RTC order)
  - config/transitions/quota_rules.json        (квоты, RepairLine, spawn)

Формирует Cypher и пишет в облачный Neo4j Aura.

Переменные окружения:
  DOMAIN_NEO4J_URI, DOMAIN_NEO4J_USER, DOMAIN_NEO4J_PASSWORD, DOMAIN_NEO4J_DB

Использование:
    python code/utils/sync_domain_graph.py                 # инкрементальный MERGE
    python code/utils/sync_domain_graph.py --clear          # очистить и перезаписать
    python code/utils/sync_domain_graph.py --dry-run        # показать Cypher без записи
"""

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Tuple

from neo4j import GraphDatabase
from neo4j.exceptions import AuthError, ServiceUnavailable


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
TRANSITIONS_JSON = os.path.join(ROOT_DIR, "config", "transitions", "transitions_rules.json")
QUOTA_JSON = os.path.join(ROOT_DIR, "config", "transitions", "quota_rules.json")


def _require_env() -> Tuple[str, str, str, str]:
    """Читает переменные подключения к Aura."""
    uri = os.getenv("DOMAIN_NEO4J_URI")
    user = os.getenv("DOMAIN_NEO4J_USER")
    password = os.getenv("DOMAIN_NEO4J_PASSWORD")
    db = os.getenv("DOMAIN_NEO4J_DB") or "neo4j"
    missing = [
        name
        for name, value in [
            ("DOMAIN_NEO4J_URI", uri),
            ("DOMAIN_NEO4J_USER", user),
            ("DOMAIN_NEO4J_PASSWORD", password),
        ]
        if not value
    ]
    if missing:
        raise RuntimeError(f"Env missing: {', '.join(missing)}")
    return uri, user, password, db


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ─── Cypher generation ──────────────────────────────────────────────


def _build_clear_queries() -> List[str]:
    """Удаляет все доменные узлы."""
    return [
        "MATCH (n) WHERE n:TransitionSpec OR n:State OR n:Rule OR n:QuotaFlow OR n:SelectionRule OR n:RepairLineRule OR n:SpawnRule OR n:MessageBucket DETACH DELETE n",
    ]


def _build_transitions_queries(data: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    """Генерирует Cypher для transitions_rules.json."""
    queries: List[Tuple[str, Dict[str, Any]]] = []

    # TransitionSpec
    queries.append((
        """
MERGE (spec:TransitionSpec {id: "transitions_rules"})
SET spec.version = $version,
    spec.architecture = $architecture,
    spec.matrix_from = $matrix_from,
    spec.matrix_to = $matrix_to
""",
        {
            "version": data.get("version", 0),
            "architecture": data.get("architecture", ""),
            "matrix_from": data.get("matrix", {}).get("from", "state"),
            "matrix_to": data.get("matrix", {}).get("to", "state"),
        },
    ))

    # States
    states = data.get("states", {})
    for state_id_str, state_name in states.items():
        queries.append((
            """
MERGE (s:State {id: $state_id})
SET s.name = $state_name
WITH s
MATCH (spec:TransitionSpec {id: "transitions_rules"})
MERGE (spec)-[:HAS_STATE]->(s)
""",
            {"state_id": int(state_id_str), "state_name": state_name},
        ))

    # Rules
    for rule in data.get("rules", []):
        pre_expr = ""
        pre = rule.get("pre")
        if pre and "expr" in pre:
            pre_expr = pre["expr"]

        post_exprs: List[str] = []
        post = rule.get("post")
        if post and "all" in post:
            post_exprs = [p.get("expr", "") for p in post["all"]]

        queries.append((
            """
MERGE (r:Rule {id: $rule_id})
SET r.from_state = $from_state,
    r.to_state = $to_state,
    r.pre_expr = $pre_expr,
    r.post_exprs = $post_exprs,
    r.owner_module = $owner_module,
    r.notes = $notes
WITH r
MATCH (spec:TransitionSpec {id: "transitions_rules"})
MERGE (spec)-[:HAS_RULE]->(r)
WITH r
MATCH (from_s:State {id: $from_state})
MERGE (r)-[:FROM_STATE]->(from_s)
WITH r
MATCH (to_s:State {id: $to_state})
MERGE (r)-[:TO_STATE]->(to_s)
WITH r
MATCH (from_s:State {id: $from_state}), (to_s:State {id: $to_state})
MERGE (from_s)-[t:TRANSITION {rule: $rule_id}]->(to_s)
""",
            {
                "rule_id": rule["id"],
                "from_state": rule["from"],
                "to_state": rule["to"],
                "pre_expr": pre_expr,
                "post_exprs": post_exprs,
                "owner_module": rule.get("owner_module", ""),
                "notes": rule.get("notes", ""),
            },
        ))

    return queries


def _build_quota_queries(data: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    """Генерирует Cypher для quota_rules.json."""
    queries: List[Tuple[str, Dict[str, Any]]] = []

    # QuotaFlow steps
    for step in data.get("quota_flow", []):
        queries.append((
            """
MERGE (q:QuotaFlow {id: $id})
SET q.owner_module = $owner_module,
    q.expr = $expr,
    q.notes = $notes
""",
            {
                "id": step["id"],
                "owner_module": step.get("owner_module", ""),
                "expr": step.get("expr", ""),
                "notes": step.get("notes", ""),
            },
        ))

    # Selection rules
    for sr in data.get("selection_rules", []):
        queries.append((
            """
MERGE (s:SelectionRule {id: $id})
SET s.expr = $expr,
    s.notes = $notes
""",
            {
                "id": sr["id"],
                "expr": sr.get("expr", ""),
                "notes": sr.get("notes", ""),
            },
        ))

    # RepairLine rules
    for rl in data.get("repair_line_rules", []):
        queries.append((
            """
MERGE (r:RepairLineRule {id: $id})
SET r.expr = $expr,
    r.notes = $notes
""",
            {
                "id": rl["id"],
                "expr": rl.get("expr", ""),
                "notes": rl.get("notes", ""),
            },
        ))

    # Spawn rules
    for sp in data.get("spawn_rules", []):
        queries.append((
            """
MERGE (s:SpawnRule {id: $id})
SET s.expr = $expr,
    s.notes = $notes
""",
            {
                "id": sp["id"],
                "expr": sp.get("expr", ""),
                "notes": sp.get("notes", ""),
            },
        ))

    # MessageBucket
    mb = data.get("message_bucket", {})
    if mb:
        for bucket_name, bucket_data in mb.items():
            if not isinstance(bucket_data, dict):
                continue
            queries.append((
                """
MERGE (b:MessageBucket {id: $id})
SET b.keys = $keys,
    b.payload = $payload,
    b.notes = $notes
""",
                {
                    "id": bucket_name,
                    "keys": [str(k) for k in bucket_data.get("keys", [])],
                    "payload": bucket_data.get("payload", []),
                    "notes": bucket_data.get("notes", ""),
                },
            ))

    return queries


# ─── Main ───────────────────────────────────────────────────────────


def sync(clear: bool = False, dry_run: bool = False) -> None:
    transitions = _load_json(TRANSITIONS_JSON)
    quota = _load_json(QUOTA_JSON)

    all_queries: List[Tuple[str, Dict[str, Any]]] = []

    if clear:
        for q in _build_clear_queries():
            all_queries.append((q, {}))

    all_queries.extend(_build_transitions_queries(transitions))
    all_queries.extend(_build_quota_queries(quota))

    if dry_run:
        print(f"=== Dry run: {len(all_queries)} queries ===\n")
        for query, params in all_queries:
            print(query.strip())
            if params:
                print(f"  params: {params}")
            print()
        return

    uri, user, password, db = _require_env()
    driver = GraphDatabase.driver(uri, auth=(user, password))

    try:
        with driver.session(database=db) as session:
            for i, (query, params) in enumerate(all_queries):
                session.run(query, **params)
            print(f"Synced {len(all_queries)} queries to {uri}")
    except AuthError as e:
        print(f"Auth error: {e}", file=sys.stderr)
        raise SystemExit(1)
    except ServiceUnavailable as e:
        print(f"Service unavailable: {e}", file=sys.stderr)
        raise SystemExit(1)
    finally:
        driver.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Синхронизация доменного графа (JSON -> Neo4j Aura)"
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Очистить все доменные узлы перед записью",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Показать Cypher без записи в Neo4j",
    )
    args = parser.parse_args()
    sync(clear=args.clear, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
