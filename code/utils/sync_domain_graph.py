#!/usr/bin/env python3
"""
Синхронизация доменного графа (JSON SSoT -> Neo4j).

Default: Neo4j Community local (Docker) — см. deploy/neo4j-local/.
Также работает с Aura/любым Neo4j Server (URI в DOMAIN_NEO4J_URI).

Читает канонические JSON:
  - config/transitions/transitions_rules.json  (переходы, RTC order)
  - config/transitions/quota_rules.json        (квоты, RepairLine, spawn)

Формирует Cypher и пишет в Neo4j.

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
from typing import Any, Dict, List, Optional, Tuple

from neo4j import GraphDatabase
from neo4j.exceptions import AuthError, ServiceUnavailable


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
TRANSITIONS_JSON = os.path.join(ROOT_DIR, "config", "transitions", "transitions_rules.json")
QUOTA_JSON = os.path.join(ROOT_DIR, "config", "transitions", "quota_rules.json")
MULTIBOM_JSON = os.path.join(
    ROOT_DIR, "config", "transitions", "multibom_template.json"
)


def _require_env() -> Tuple[str, str, str, str]:
    """Читает переменные подключения к Neo4j."""
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
    """Удаляет только узлы доменной проекции."""
    return [
        (
            "MATCH (n) WHERE any(l IN labels(n) WHERE l STARTS WITH 'Domain_') "
            "DETACH DELETE n"
        ),
    ]


def _build_transitions_queries(data: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    """Генерирует Cypher для transitions_rules.json."""
    queries: List[Tuple[str, Dict[str, Any]]] = []

    # TransitionSpec
    queries.append((
        """
MERGE (spec:Domain_TransitionSpec {id: "transitions_rules"})
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
MERGE (s:Domain_State {id: $state_id})
SET s.name = $state_name
WITH s
MATCH (spec:Domain_TransitionSpec {id: "transitions_rules"})
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
MERGE (r:Domain_Rule {id: $rule_id})
SET r.from_state = $from_state,
    r.to_state = $to_state,
    r.pre_expr = $pre_expr,
    r.post_exprs = $post_exprs,
    r.owner_module = $owner_module,
    r.notes = $notes
WITH r
MATCH (spec:Domain_TransitionSpec {id: "transitions_rules"})
MERGE (spec)-[:HAS_RULE]->(r)
WITH r
MATCH (from_s:Domain_State {id: $from_state})
MERGE (r)-[:FROM_STATE]->(from_s)
WITH r
MATCH (to_s:Domain_State {id: $to_state})
MERGE (r)-[:TO_STATE]->(to_s)
WITH r
MATCH (from_s:Domain_State {id: $from_state}), (to_s:Domain_State {id: $to_state})
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
MERGE (q:Domain_QuotaFlow {id: $id})
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
MERGE (s:Domain_SelectionRule {id: $id})
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
MERGE (r:Domain_RepairLineRule {id: $id})
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
MERGE (s:Domain_SpawnRule {id: $id})
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
MERGE (b:Domain_MessageBucket {id: $id})
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


def _build_rtc_execution_queries(data: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    """Генерирует Cypher для rtc_execution_order."""
    queries: List[Tuple[str, Dict[str, Any]]] = []
    rtc_order = data.get("rtc_execution_order", [])
    prev_order: Optional[int] = None

    for entry in rtc_order:
        raw_order = entry.get("order", 0)
        order_label = ""
        if isinstance(raw_order, int):
            order = raw_order
        elif isinstance(raw_order, str):
            try:
                order = int(raw_order)
            except ValueError:
                order = (prev_order or 0) + 1
                order_label = raw_order
        else:
            order = (prev_order or 0) + 1
            order_label = str(raw_order)
        queries.append((
            """
MERGE (l:Domain_RTCLayer {order: $order})
SET l.phase = $phase,
    l.layer = $layer,
    l.function = $function,
    l.state = $state,
    l.notes = $notes,
    l.order_label = $order_label
""",
            {
                "order": order,
                "phase": entry.get("phase", ""),
                "layer": entry.get("layer", ""),
                "function": entry.get("function", ""),
                "state": entry.get("state", ""),
                "notes": entry.get("notes", ""),
                "order_label": order_label,
            },
        ))

        if prev_order is not None:
            queries.append((
                """
MATCH (prev:Domain_RTCLayer {order: $prev_order})
MATCH (curr:Domain_RTCLayer {order: $order})
MERGE (prev)-[:NEXT_LAYER]->(curr)
""",
                {"prev_order": prev_order, "order": order},
            ))

        state_value = entry.get("state", "")
        if isinstance(state_value, str) and "→" in state_value:
            from_state, to_state = [part.strip() for part in state_value.split("→", 1)]
            if from_state.isdigit():
                queries.append((
                    """
MATCH (s:Domain_State {id: $state_id})
MATCH (l:Domain_RTCLayer {order: $order})
MERGE (l)-[:FROM_STATE]->(s)
""",
                    {"state_id": int(from_state), "order": order},
                ))
            if to_state.isdigit():
                queries.append((
                    """
MATCH (s:Domain_State {id: $state_id})
MATCH (l:Domain_RTCLayer {order: $order})
MERGE (l)-[:TO_STATE]->(s)
""",
                    {"state_id": int(to_state), "order": order},
                ))

        prev_order = order

    return queries


def _build_bom_queries(data: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    """Генерирует Cypher для multibom_template.json."""
    queries: List[Tuple[str, Dict[str, Any]]] = []
    template_id = data.get("template_id", "multibom_template")

    def _dedupe(values: List[int]) -> List[int]:
        seen = set()
        out: List[int] = []
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            out.append(value)
        return out

    def _expand_group_ranges(raw_values: Any) -> List[int]:
        if raw_values is None:
            return []
        if isinstance(raw_values, (int, float)):
            return [int(raw_values)]
        if isinstance(raw_values, str):
            raw_values = [raw_values]
        if not isinstance(raw_values, (list, tuple)):
            return []
        groups: List[int] = []
        for item in raw_values:
            if isinstance(item, (int, float)):
                groups.append(int(item))
                continue
            if not isinstance(item, str):
                continue
            token = item.strip()
            if not token:
                continue
            if "-" in token:
                left, right = token.split("-", 1)
                if left.strip().isdigit() and right.strip().isdigit():
                    start = int(left)
                    end = int(right)
                    if start <= end:
                        groups.extend(list(range(start, end + 1)))
                continue
            if token.isdigit():
                groups.append(int(token))
        return groups

    def _collect_groups(spec: Dict[str, Any]) -> List[int]:
        groups: List[int] = []
        groups.extend(_expand_group_ranges(spec.get("groups")))
        groups.extend(_expand_group_ranges(spec.get("group_range")))
        groups.extend(_expand_group_ranges(spec.get("group_ranges")))
        groups.extend(_expand_group_ranges(spec.get("range")))
        groups.extend(_expand_group_ranges(spec.get("ranges")))
        return _dedupe(groups)

    def _resolve_group_scope(scope: Any, l2_matrix: Dict[str, Any]) -> List[int]:
        if not scope:
            return []
        if isinstance(scope, str):
            if scope.upper() == "ALL":
                return []
            if scope in l2_matrix and isinstance(l2_matrix[scope], dict):
                return _collect_groups(l2_matrix[scope])
            return _expand_group_ranges([scope])
        if isinstance(scope, (list, tuple)):
            groups: List[int] = []
            for item in scope:
                if isinstance(item, str) and item in l2_matrix and isinstance(
                    l2_matrix[item], dict
                ):
                    groups.extend(_collect_groups(l2_matrix[item]))
                else:
                    groups.extend(_expand_group_ranges([item]))
            return _dedupe(groups)
        if isinstance(scope, (int, float)):
            return [int(scope)]
        return []

    def _normalize_group_catalog(raw_catalog: Any) -> List[Dict[str, Any]]:
        if not raw_catalog:
            return []
        if isinstance(raw_catalog, dict):
            raw_catalog = [raw_catalog]
        if not isinstance(raw_catalog, list):
            return []
        catalog: List[Dict[str, Any]] = []
        for entry in raw_catalog:
            if not isinstance(entry, dict):
                continue
            group_by = entry.get("group_by")
            if group_by is None:
                continue
            try:
                group_by_int = int(group_by)
            except (TypeError, ValueError):
                continue
            level = str(entry.get("level", "")).strip().upper()
            if not level:
                level = "L1" if group_by_int in (1, 2) else "L2"
            try:
                number = int(entry.get("group_number", group_by_int))
            except (TypeError, ValueError):
                number = group_by_int
            name = entry.get("group_name", "")
            group_type = entry.get("group_type", "")
            group_title = entry.get("group_title", "")
            try:
                mask = int(entry.get("ac_type_mask_effective", 0))
            except (TypeError, ValueError):
                mask = 0
            samples = entry.get("partno_samples", [])
            if isinstance(samples, str):
                samples = [item.strip() for item in samples.split(",") if item.strip()]
            elif isinstance(samples, (list, tuple)):
                samples = [str(item) for item in samples if item]
            else:
                samples = []
            members = entry.get("partno_members")
            if members is None or (isinstance(members, list) and not members):
                members = samples
            elif isinstance(members, str):
                members = [item.strip() for item in members.split(",") if item.strip()]
            elif isinstance(members, (list, tuple)):
                members = [str(item) for item in members if item]
            else:
                members = samples
            catalog.append(
                {
                    "group_by": group_by_int,
                    "level": level,
                    "number": number,
                    "name": name,
                    "group_type": group_type,
                    "group_title": group_title,
                    "ac_type_mask_effective": mask,
                    "partno_samples": samples,
                    "partno_members": list(dict.fromkeys(members)),
                }
            )
        return catalog

    def _normalize_l1_l2_link_rules(raw_rules: Any) -> Tuple[str, Dict[int, List[int]]]:
        via = "ac_type_mask"
        mapping: Dict[int, List[int]] = {}
        if not isinstance(raw_rules, dict):
            return via, mapping
        via = raw_rules.get("via", via)
        raw_mapping = raw_rules.get("mapping")
        mapping_items: List[Tuple[Any, Any]] = []
        if isinstance(raw_mapping, dict):
            mapping_items = list(raw_mapping.items())
        elif isinstance(raw_mapping, list):
            for entry in raw_mapping:
                if not isinstance(entry, dict):
                    continue
                mapping_items.append((entry.get("ac_type_mask"), entry.get("l1_groups")))
        if not mapping_items:
            mask_to_l1 = raw_rules.get("mask_to_l1")
            if isinstance(mask_to_l1, dict):
                mapping_items = list(mask_to_l1.items())
        for mask, l1_groups in mapping_items:
            try:
                mask_int = int(mask)
            except (TypeError, ValueError):
                continue
            if isinstance(l1_groups, (int, float, str)):
                l1_groups = [l1_groups]
            if not isinstance(l1_groups, (list, tuple)):
                continue
            l1_list: List[int] = []
            for group_id in l1_groups:
                try:
                    l1_list.append(int(group_id))
                except (TypeError, ValueError):
                    continue
            if l1_list:
                mapping[mask_int] = _dedupe(l1_list)
        return via, mapping

    queries.append((
        """
MERGE (t:Domain_BomTemplate {id: $id})
SET t.version = $version,
    t.description = $description
""",
        {
            "id": template_id,
            "version": data.get("version", 0),
            "description": data.get("description", ""),
        },
    ))

    group_catalog = _normalize_group_catalog(data.get("group_catalog"))
    for entry in group_catalog:
        partno_list = entry.get("partno_members", entry.get("partno_samples", []))
        partno_count = len(partno_list) if isinstance(partno_list, (list, tuple)) else 0
        queries.append((
            """
MERGE (g:Domain_BomGroup {group_by: $group_by})
SET g.group_title = $group_title,
    g.ac_type_mask_effective = $ac_type_mask_effective,
    g.partno_members = $partno_members,
    g.partno_count = $partno_count
""",
            {
                "group_by": entry["group_by"],
                "group_title": entry.get("group_title", ""),
                "ac_type_mask_effective": entry["ac_type_mask_effective"],
                "partno_members": list(partno_list) if isinstance(partno_list, (list, tuple)) else [],
                "partno_count": partno_count,
            },
        ))

    for entry in group_catalog:
        partno_list = entry.get("partno_members", entry.get("partno_samples", []))
        if not isinstance(partno_list, (list, tuple)):
            partno_list = []
        for partno in partno_list:
            if not partno or not str(partno).strip():
                continue
            partno_str = str(partno).strip()
            partno_id = f"{entry['group_by']}::{partno_str}"
            queries.append((
                """
MERGE (p:Domain_BomPartNo {id: $partno_id})
SET p.partno = $partno,
    p.group_by = $group_by
WITH p
MATCH (g:Domain_BomGroup {group_by: $group_by})
MERGE (g)-[:HAS_PARTNO]->(p)
""",
                {
                    "partno_id": partno_id,
                    "partno": partno_str,
                    "group_by": entry["group_by"],
                },
            ))

    group_hierarchy = data.get("group_hierarchy", {})
    if isinstance(group_hierarchy, dict):
        for level_id, spec in group_hierarchy.items():
            if not isinstance(spec, dict):
                continue
            tier = spec.get("tier", "")
            groups = _collect_groups(spec)
            range_value = (
                spec.get("range")
                or spec.get("group_range")
                or spec.get("group_ranges")
                or spec.get("ranges")
            )
            if isinstance(range_value, list):
                range_value = [str(item) for item in range_value]
            elif range_value is not None:
                range_value = str(range_value)
            queries.append((
                """
MERGE (l:Domain_BomGroupLevel {id: $id})
SET l.tier = $tier,
    l.label = $label,
    l.groups = $groups,
    l.group_range = $group_range,
    l.notes = $notes
WITH l
MATCH (t:Domain_BomTemplate {id: $template_id})
MERGE (t)-[:HAS_GROUP_LEVEL]->(l)
""",
                {
                    "id": str(level_id),
                    "tier": tier,
                    "label": spec.get("label", ""),
                    "groups": groups,
                    "group_range": range_value or "",
                    "notes": spec.get("notes", ""),
                    "template_id": template_id,
                },
            ))

            for group_id in groups:
                queries.append((
                    """
MERGE (g:Domain_BomGroup {group_by: $group_by})
WITH g
MATCH (l:Domain_BomGroupLevel {id: $level_id})
MERGE (l)-[:HAS_GROUP]->(g)
""",
                    {
                        "group_by": int(group_id),
                        "level_id": str(level_id),
                    },
                ))

    # L1 -> L2 hierarchy: (L1_PLANERS)-[:HAS_LEVEL]->(L2_COMPONENTS)
    level_hierarchy = data.get("level_hierarchy", {})
    parent_id, child_id = "", ""
    if isinstance(level_hierarchy, dict):
        for pid, spec in level_hierarchy.items():
            if isinstance(spec, dict) and spec.get("child_level"):
                parent_id, child_id = str(pid), str(spec["child_level"])
                break
    if not parent_id and isinstance(group_hierarchy, dict) and "L1_PLANERS" in group_hierarchy and "L2_COMPONENTS" in group_hierarchy:
        parent_id, child_id = "L1_PLANERS", "L2_COMPONENTS"
    if parent_id and child_id:
        queries.append((
            """
MATCH (l1:Domain_BomGroupLevel {id: $parent_id})
MATCH (l2:Domain_BomGroupLevel {id: $child_id})
MERGE (l1)-[:HAS_LEVEL]->(l2)
""",
            {"parent_id": parent_id, "child_id": child_id},
        ))

    l1_l2_rules = data.get("l1_l2_link_rules", {})
    via, mask_mapping = _normalize_l1_l2_link_rules(l1_l2_rules)
    if group_catalog and mask_mapping:
        l1_groups = _dedupe(
            [g["group_by"] for g in group_catalog if g.get("level") == "L1"]
        )
        l2_groups = [g for g in group_catalog if g.get("level") == "L2"]
        if not l1_groups:
            l1_groups = [1, 2]
        for l2 in l2_groups:
            mask = int(l2.get("ac_type_mask_effective", 0))
            target_l1 = mask_mapping.get(mask, [])
            if not target_l1:
                continue
            for l1_group in target_l1:
                if l1_groups and l1_group not in l1_groups:
                    continue
                queries.append((
                    """
MATCH (l1:Domain_BomGroup {group_by: $l1_group_by})
MATCH (l2:Domain_BomGroup {group_by: $l2_group_by})
MERGE (l1)-[rel:HAS_L2_GROUP]->(l2)
SET rel.via = $via,
    rel.ac_type_mask = $ac_type_mask
""",
                    {
                        "l1_group_by": int(l1_group),
                        "l2_group_by": int(l2.get("group_by", 0)),
                        "via": via,
                        "ac_type_mask": mask,
                    },
                ))

    numbering = data.get("numbering_logic", {})
    if isinstance(numbering, dict) and numbering:
        group_number = numbering.get("group_number", "")
        level_fields: List[str] = []
        level_notes: List[str] = []
        levels = numbering.get("levels", {})
        if isinstance(levels, dict):
            for level_id, spec in levels.items():
                if not isinstance(spec, dict):
                    continue
                field = spec.get("entity_number_field", "")
                if field:
                    level_fields.append(f"{level_id}:{field}")
                note = spec.get("notes", "")
                if note:
                    level_notes.append(f"{level_id}:{note}")
        queries.append((
            """
MERGE (n:Domain_BomNumberingRule {id: $id})
SET n.group_number = $group_number,
    n.level_fields = $level_fields,
    n.level_notes = $level_notes,
    n.notes = $notes
WITH n
MATCH (t:Domain_BomTemplate {id: $template_id})
MERGE (t)-[:HAS_NUMBERING_RULE]->(n)
""",
            {
                "id": f"{template_id}::numbering",
                "group_number": group_number,
                "level_fields": level_fields,
                "level_notes": level_notes,
                "notes": numbering.get("notes", ""),
                "template_id": template_id,
            },
        ))

    l2_matrix = data.get("l2_matrix", {})
    l2_matrix_dict = l2_matrix if isinstance(l2_matrix, dict) else {}
    if isinstance(l2_matrix, dict):
        for category, spec in l2_matrix.items():
            if not isinstance(spec, dict):
                continue
            groups = spec.get("groups", [])
            for group_id in groups:
                queries.append((
                    """
MERGE (g:Domain_BomGroup {group_by: $group_by})
WITH g
MATCH (t:Domain_BomTemplate {id: $template_id})
MERGE (t)-[:HAS_GROUP]->(g)
""",
                    {
                        "group_by": int(group_id),
                        "template_id": template_id,
                    },
                ))

    for rule in data.get("compatibility_rules", []):
        if not isinstance(rule, dict):
            continue
        rule_id = rule.get("id", "")
        if not rule_id:
            continue
        queries.append((
            """
MERGE (r:Domain_BomCompatibilityRule {id: $id})
SET r.title = $title,
    r.type = $rule_type,
    r.scope = $scope,
    r.expr = $expr,
    r.groups = $groups,
    r.alias_from = $alias_from,
    r.alias_to = $alias_to,
    r.notes = $notes
WITH r
MATCH (t:Domain_BomTemplate {id: $template_id})
MERGE (t)-[:HAS_COMPATIBILITY_RULE]->(r)
""",
            {
                "id": rule_id,
                "title": rule.get("title", ""),
                "rule_type": rule.get("type", ""),
                "scope": rule.get("scope", ""),
                "expr": rule.get("expr", ""),
                "groups": [int(g) for g in rule.get("groups", [])],
                "alias_from": rule.get("alias_from", ""),
                "alias_to": rule.get("alias_to", ""),
                "notes": rule.get("notes", ""),
                "template_id": template_id,
            },
        ))

        group_scope = _resolve_group_scope(rule.get("group_scope"), l2_matrix_dict)
        for group_id in group_scope:
            queries.append((
                """
MATCH (r:Domain_BomCompatibilityRule {id: $id})
MATCH (g:Domain_BomGroup {group_by: $group_by})
MERGE (r)-[:RULE_FOR_GROUP]->(g)
""",
                {"id": rule_id, "group_by": int(group_id)},
            ))

    replaceability_rules = data.get("replaceability_rules", [])
    if isinstance(replaceability_rules, list):
        for idx, rule in enumerate(replaceability_rules, start=1):
            if not isinstance(rule, dict):
                continue
            rule_id = rule.get("id") or f"RB{idx}"
            queries.append((
                """
MERGE (r:Domain_BomReplaceabilityRule {id: $id})
SET r.result_kind = $result_kind,
    r.expr = $expr,
    r.notes = $notes,
    r.title = $title
WITH r
MATCH (t:Domain_BomTemplate {id: $template_id})
MERGE (t)-[:HAS_REPLACEABILITY_RULE]->(r)
""",
                {
                    "id": rule_id,
                    "result_kind": rule.get("result_kind", ""),
                    "expr": rule.get("expr", ""),
                    "notes": rule.get("notes", ""),
                    "title": rule.get("title", ""),
                    "template_id": template_id,
                },
            ))

    return queries


# ─── Main ───────────────────────────────────────────────────────────


def sync(clear: bool = False, dry_run: bool = False) -> None:
    transitions = _load_json(TRANSITIONS_JSON)
    quota = _load_json(QUOTA_JSON)
    multibom = _load_json(MULTIBOM_JSON)

    all_queries: List[Tuple[str, Dict[str, Any]]] = []

    if clear:
        for q in _build_clear_queries():
            all_queries.append((q, {}))

    all_queries.extend(_build_transitions_queries(transitions))
    all_queries.extend(_build_rtc_execution_queries(transitions))
    all_queries.extend(_build_quota_queries(quota))
    all_queries.extend(_build_bom_queries(multibom))

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
        description="Синхронизация доменного графа (JSON -> Neo4j)"
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
