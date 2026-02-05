import argparse
import os
import subprocess
from typing import Any, Dict, Iterable, List, Optional, Tuple, TypedDict

from langgraph.graph import END, StateGraph
from neo4j import GraphDatabase

REL_TYPES = [
    "MODIFIES",
    "DEPENDS_ON",
    "CONSTRAINS",
    "VALIDATES",
    "ABOUT",
    "INTRODUCES",
]

REQUIRED_SECTIONS = [
    "Scope",
    "Invariants (≤12)",
    "Decisions (≤7)",
    "Impact Paths",
    "Validation Proof",
    "Risks (≤7) + Mitigations",
    "Open Questions (≤7)",
    "Pointers (≤15)",
]


class CapsuleState(TypedDict, total=False):
    topic: Optional[str]
    sha: Optional[str]
    branch: Optional[str]
    artifact: Optional[str]
    k_hops: int
    out: str
    scope_items: List[str]
    kg_nodes: List[Dict[str, Any]]
    kg_rels: List[Dict[str, Any]]
    sections: Dict[str, List[str]]
    markdown: str


def run_cmd(command: List[str]) -> str:
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return result.stdout


def _parse_git_status_lines(output: str) -> List[str]:
    items: List[str] = []
    for raw in output.splitlines():
        line = raw.rstrip()
        if not line:
            continue
        if len(line) < 3:
            continue
        path_part = line[3:].strip()
        if "->" in path_part:
            path_part = path_part.split("->", 1)[1].strip()
        if path_part:
            items.append(path_part)
    return items


def fetch_git_files(sha: Optional[str]) -> List[str]:
    if sha:
        output = run_cmd(["git", "show", "--name-only", "--pretty=", sha])
        items = [line.strip() for line in output.splitlines() if line.strip()]
        return sorted(set(items))
    output = run_cmd(["git", "status", "--porcelain"])
    items = _parse_git_status_lines(output)
    return sorted(set(items))


def _require_kg_env() -> Tuple[str, str, str, str]:
    uri = os.getenv("KG_NEO4J_URI")
    user = os.getenv("KG_NEO4J_USER")
    password = os.getenv("KG_NEO4J_PASSWORD")
    db = os.getenv("KG_NEO4J_DB") or "neo4j"
    missing = [name for name, value in [
        ("KG_NEO4J_URI", uri),
        ("KG_NEO4J_USER", user),
        ("KG_NEO4J_PASSWORD", password),
    ] if not value]
    if missing:
        raise RuntimeError(f"KG env missing: {', '.join(missing)}")
    return uri, user, password, db


def _node_to_info(node: Any) -> Dict[str, Any]:
    labels = sorted(list(node.labels))
    props = dict(node)
    element_id = getattr(node, "element_id", None)
    if element_id is None:
        element_id = str(getattr(node, "id"))
    return {
        "labels": labels,
        "props": props,
        "element_id": str(element_id),
    }


def _node_display(node_info: Dict[str, Any]) -> str:
    props = node_info["props"]
    for key in ["text", "title", "name", "id", "path", "sha"]:
        value = props.get(key)
        if value:
            return str(value)
    labels = node_info["labels"]
    if labels:
        return f"{labels[0]}:{node_info['element_id']}"
    return node_info["element_id"]


def _node_source(node_info: Dict[str, Any]) -> str:
    props = node_info["props"]
    for key in ["source", "path", "sha", "id"]:
        value = props.get(key)
        if value:
            return str(value)
    label = node_info["labels"][0] if node_info["labels"] else "Node"
    return f"kg:{label}:{node_info['element_id']}"


def _node_sort_key(node_info: Dict[str, Any]) -> Tuple[str, str, str]:
    props = node_info["props"]
    labels = node_info["labels"]
    type_key = labels[0] if labels else ""
    name_key = (
        props.get("name")
        or props.get("id")
        or props.get("title")
        or props.get("text")
        or props.get("sha")
        or props.get("path")
        or node_info["element_id"]
    )
    path_key = props.get("path") or ""
    return (str(type_key).lower(), str(name_key).lower(), str(path_key).lower())


def _rel_sort_key(rel: Dict[str, Any], node_map: Dict[str, Dict[str, Any]]) -> Tuple[str, str, str]:
    rel_type = rel["type"]
    start = node_map.get(rel["start"])
    end = node_map.get(rel["end"])
    start_name = _node_display(start) if start else rel["start"]
    end_name = _node_display(end) if end else rel["end"]
    return (rel_type.lower(), str(start_name).lower(), str(end_name).lower())


def fetch_kg(sha: Optional[str], artifact: Optional[str], k_hops: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if not sha and not artifact:
        raise ValueError("KG fetch requires --sha or --artifact")
    if k_hops < 1:
        raise ValueError("--k-hops must be >= 1")

    uri, user, password, db = _require_kg_env()
    driver = GraphDatabase.driver(uri, auth=(user, password))
    rel_types = "|".join(REL_TYPES)
    if sha:
        start_label = "Change"
        start_prop = "sha"
        start_value = sha
    else:
        start_label = "Artifact"
        start_prop = "path"
        start_value = artifact

    query = f"""
MATCH (start:{start_label} {{{start_prop}: $start_value}})
OPTIONAL MATCH p=(start)-[r:{rel_types}*1..$k]-(n)
WITH start, [p IN collect(p) WHERE p IS NOT NULL] as paths
CALL {{
  WITH start, paths
  WITH start, paths WHERE size(paths)=0
  RETURN [start] as nodes
  UNION
  WITH start, paths WHERE size(paths) > 0
  UNWIND paths as p
  UNWIND nodes(p) as n
  RETURN collect(distinct n) + [start] as nodes
}}
CALL {{
  WITH paths
  WITH paths WHERE size(paths)=0
  RETURN [] as rels_out
  UNION
  WITH paths WHERE size(paths) > 0
  UNWIND paths as p
  UNWIND relationships(p) as rel
  RETURN collect(distinct rel) as rels_out
}}
RETURN nodes, rels_out as rels
"""

    nodes: List[Dict[str, Any]] = []
    rels: List[Dict[str, Any]] = []
    with driver.session(database=db) as session:
        records = list(session.run(query, start_value=start_value, k=k_hops))
        if records:
            record = records[0]
            raw_nodes = record.get("nodes") or []
            raw_rels = record.get("rels") or []
            node_map: Dict[str, Dict[str, Any]] = {}
            for node in raw_nodes:
                info = _node_to_info(node)
                node_map[info["element_id"]] = info
            rel_list: List[Dict[str, Any]] = []
            for rel in raw_rels:
                start_info = _node_to_info(rel["start"])
                end_info = _node_to_info(rel["end"])
                node_map[start_info["element_id"]] = start_info
                node_map[end_info["element_id"]] = end_info
                rel_list.append({
                    "type": rel["type"],
                    "start": start_info["element_id"],
                    "end": end_info["element_id"],
                })
            nodes = list(node_map.values())
            rels = rel_list
    driver.close()
    return nodes, rels


def _format_items(items: List[Tuple[str, str, Tuple[str, str, str]]], limit: Optional[int] = None) -> List[str]:
    items_sorted = sorted(items, key=lambda item: item[2])
    if limit is not None:
        items_sorted = items_sorted[:limit]
    return [f"- {text} — {source}" for text, source, _ in items_sorted]


def _no_data_line() -> List[str]:
    return ["- (нет данных)"]


def _section_from_nodes(nodes: List[Dict[str, Any]], labels: Iterable[str], limit: Optional[int]) -> List[str]:
    label_set = set(labels)
    items: List[Tuple[str, str, Tuple[str, str, str]]] = []
    for node in nodes:
        if label_set.intersection(node["labels"]):
            items.append((
                _node_display(node),
                _node_source(node),
                _node_sort_key(node),
            ))
    if not items:
        return _no_data_line()
    return _format_items(items, limit)


def _impact_paths(rels: List[Dict[str, Any]], nodes: List[Dict[str, Any]]) -> List[str]:
    node_map = {node["element_id"]: node for node in nodes}
    unique: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for rel in rels:
        key = (rel["start"], rel["type"], rel["end"])
        if key not in unique:
            unique[key] = rel
    rel_list = sorted(unique.values(), key=lambda r: _rel_sort_key(r, node_map))
    lines: List[str] = []
    for rel in rel_list[:20]:
        start = node_map.get(rel["start"])
        end = node_map.get(rel["end"])
        from_name = _node_display(start) if start else rel["start"]
        to_name = _node_display(end) if end else rel["end"]
        lines.append(f"- {from_name} -> {rel['type']} -> {to_name} — kg:{rel['type']}")
    if len(lines) < 5:
        return _no_data_line()
    return lines


def _validation_proof(nodes: List[Dict[str, Any]], rels: List[Dict[str, Any]]) -> List[str]:
    items = _section_from_nodes(nodes, ["Check", "Validation", "ValidationProof", "Proof", "Test"], None)
    if items != _no_data_line():
        return items
    rel_items: List[Tuple[str, str, Tuple[str, str, str]]] = []
    node_map = {node["element_id"]: node for node in nodes}
    for rel in rels:
        if rel["type"] != "VALIDATES":
            continue
        start = node_map.get(rel["start"])
        end = node_map.get(rel["end"])
        from_name = _node_display(start) if start else rel["start"]
        to_name = _node_display(end) if end else rel["end"]
        text = f"{from_name} -> VALIDATES -> {to_name}"
        source = "kg:VALIDATES"
        rel_items.append((text, source, (rel["type"].lower(), from_name.lower(), to_name.lower())))
    if not rel_items:
        return _no_data_line()
    return _format_items(rel_items, None)


def _risks_with_mitigations(nodes: List[Dict[str, Any]]) -> List[str]:
    risks = _section_from_nodes(nodes, ["Risk"], 7)
    mitigations = _section_from_nodes(nodes, ["Mitigation"], None)
    if risks == _no_data_line() and mitigations == _no_data_line():
        return _no_data_line()
    lines: List[str] = []
    if risks != _no_data_line():
        for line in risks:
            lines.append(line.replace("- ", "- Risk: ", 1))
    if mitigations != _no_data_line():
        for line in mitigations:
            lines.append(line.replace("- ", "- Mitigation: ", 1))
    return lines


def _scope_lines(scope_items: List[str], sha: Optional[str]) -> List[str]:
    if not scope_items:
        return _no_data_line()
    source = f"git:{sha}" if sha else "git:status"
    items = []
    for item in sorted(scope_items):
        items.append((item, source, ("scope", item.lower(), "")))
    return _format_items(items, None)


def build_sections(state: CapsuleState) -> Dict[str, List[str]]:
    nodes = state.get("kg_nodes", [])
    rels = state.get("kg_rels", [])
    sections: Dict[str, List[str]] = {}
    sections["Scope"] = _scope_lines(state.get("scope_items", []), state.get("sha"))
    sections["Invariants (≤12)"] = _section_from_nodes(nodes, ["Invariant", "Constraint"], 12)
    sections["Decisions (≤7)"] = _section_from_nodes(nodes, ["Decision", "DecisionRecord", "ADR"], 7)
    sections["Impact Paths"] = _impact_paths(rels, nodes)
    sections["Validation Proof"] = _validation_proof(nodes, rels)
    sections["Risks (≤7) + Mitigations"] = _risks_with_mitigations(nodes)
    sections["Open Questions (≤7)"] = _section_from_nodes(nodes, ["OpenQuestion", "Question"], 7)
    sections["Pointers (≤15)"] = _section_from_nodes(nodes, ["Pointer", "Artifact", "Document"], 15)
    return sections


def render_markdown(sections: Dict[str, List[str]], topic: Optional[str]) -> str:
    title = f"# Context Capsule — {topic}" if topic else "# Context Capsule"
    lines = [title, ""]
    for section in REQUIRED_SECTIONS:
        lines.append(f"## {section}")
        lines.extend(sections.get(section, _no_data_line()))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def lint_capsule(markdown: str) -> List[str]:
    errors: List[str] = []
    sections: Dict[str, List[str]] = {}
    current: Optional[str] = None
    for raw in markdown.splitlines():
        line = raw.strip()
        if line.startswith("## "):
            current = line[3:].strip()
            sections[current] = []
            continue
        if current and line.startswith("- "):
            sections[current].append(line)

    for required in REQUIRED_SECTIONS:
        if required not in sections:
            errors.append(f"Missing section: {required}")
            continue
        if not sections[required]:
            errors.append(f"Section '{required}' has no items")

    def is_no_data(item: str) -> bool:
        return item.strip() == "- (нет данных)"

    def count_data(items: List[str]) -> int:
        return sum(1 for item in items if not is_no_data(item))

    need_source = {
        "Invariants (≤12)",
        "Decisions (≤7)",
        "Impact Paths",
        "Validation Proof",
        "Risks (≤7) + Mitigations",
    }
    for section_name, items in sections.items():
        if section_name in need_source:
            for item in items:
                if is_no_data(item):
                    continue
                if "—" not in item:
                    errors.append(f"Section '{section_name}' item missing source: {item}")

    invariants = sections.get("Invariants (≤12)", [])
    decisions = sections.get("Decisions (≤7)", [])
    impacts = sections.get("Impact Paths", [])
    risks = sections.get("Risks (≤7) + Mitigations", [])
    questions = sections.get("Open Questions (≤7)", [])
    pointers = sections.get("Pointers (≤15)", [])

    if count_data(invariants) > 12:
        errors.append("Invariants exceeds limit (12)")
    if count_data(decisions) > 7:
        errors.append("Decisions exceeds limit (7)")

    impact_count = count_data(impacts)
    if impact_count > 0 and impact_count < 5:
        errors.append("Impact Paths below minimum (5)")
    if impact_count > 20:
        errors.append("Impact Paths exceeds limit (20)")

    risk_count = sum(1 for item in risks if item.lower().startswith("- risk:"))
    if risk_count > 7:
        errors.append("Risks exceeds limit (7)")

    if count_data(questions) > 7:
        errors.append("Open Questions exceeds limit (7)")
    if count_data(pointers) > 15:
        errors.append("Pointers exceeds limit (15)")

    return errors


def fetch_git_step(state: CapsuleState) -> CapsuleState:
    return {"scope_items": fetch_git_files(state.get("sha"))}


def fetch_kg_step(state: CapsuleState) -> CapsuleState:
    nodes, rels = fetch_kg(state.get("sha"), state.get("artifact"), state["k_hops"])
    return {"kg_nodes": nodes, "kg_rels": rels}


def render_step(state: CapsuleState) -> CapsuleState:
    sections = build_sections(state)
    markdown = render_markdown(sections, state.get("topic"))
    return {"sections": sections, "markdown": markdown}


def write_output_step(state: CapsuleState) -> CapsuleState:
    out_path = state.get("out")
    if not out_path:
        raise ValueError("--out is required for build")
    with open(out_path, "w", encoding="utf-8") as file:
        file.write(state["markdown"])
    return {}


def build_capsule(args: argparse.Namespace) -> None:
    state: CapsuleState = {
        "topic": args.topic,
        "sha": args.sha,
        "branch": args.branch,
        "artifact": args.artifact,
        "k_hops": args.k_hops,
        "out": args.out,
    }
    graph = StateGraph(CapsuleState)
    graph.add_node("fetch_git", fetch_git_step)
    graph.add_node("fetch_kg", fetch_kg_step)
    graph.add_node("render", render_step)
    graph.add_node("write_output", write_output_step)
    graph.set_entry_point("fetch_git")
    graph.add_edge("fetch_git", "fetch_kg")
    graph.add_edge("fetch_kg", "render")
    graph.add_edge("render", "write_output")
    graph.add_edge("write_output", END)
    app = graph.compile()
    app.invoke(state)


def lint_file(out_path: str) -> None:
    if not out_path:
        raise ValueError("--out is required for lint")
    with open(out_path, "r", encoding="utf-8") as file:
        markdown = file.read()
    errors = lint_capsule(markdown)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        raise SystemExit(1)
    print("OK")


def push_local(args: argparse.Namespace) -> None:
    if not args.topic:
        raise ValueError("--topic is required for push-local")
    if not args.sha:
        raise ValueError("--sha is required for push-local")
    if not args.out:
        raise ValueError("--out is required for push-local")
    uri, user, password, db = _require_kg_env()
    with open(args.out, "r", encoding="utf-8") as file:
        content = file.read()
    branch = args.branch or ""
    path = args.out

    driver = GraphDatabase.driver(uri, auth=(user, password))
    query = """
MERGE (c:ContextCapsule {topic: $topic, sha: $sha})
SET c.content = $content,
    c.path = $path,
    c.branch = $branch,
    c.updated_at = datetime()
WITH c
OPTIONAL MATCH (ch:Change {sha: $sha})
FOREACH (_ IN CASE WHEN ch IS NULL THEN [] ELSE [1] END |
  MERGE (c)-[:ABOUT]->(ch)
)
"""
    with driver.session(database=db) as session:
        session.run(
            query,
            topic=args.topic,
            sha=args.sha,
            content=content,
            path=path,
            branch=branch,
        )
    driver.close()


# =============================================================================
# Workflow State Management (Hybrid Orchestrator Dispatch + Graph Context)
# =============================================================================


def init_workflow(args: argparse.Namespace) -> None:
    """Initialize a new workflow state in the knowledge graph."""
    if not args.workflow_id:
        raise ValueError("--workflow-id is required for init-workflow")
    if not args.goal:
        raise ValueError("--goal is required for init-workflow")

    uri, user, password, db = _require_kg_env()
    driver = GraphDatabase.driver(uri, auth=(user, password))

    query = """
MERGE (w:WorkflowState {workflow_id: $workflow_id})
SET w.goal = $goal,
    w.phase = $phase,
    w.owner = $owner,
    w.status = 'active',
    w.created_at = datetime(),
    w.updated_at = datetime()
RETURN w.workflow_id as id
"""
    with driver.session(database=db) as session:
        result = session.run(
            query,
            workflow_id=args.workflow_id,
            goal=args.goal,
            phase=args.phase or "analysis",
            owner=args.owner or "orchestrator",
        )
        record = result.single()
        if record:
            print(f"Workflow initialized: {record['id']}")
    driver.close()


def write_handoff(args: argparse.Namespace) -> None:
    """Write a handoff node to the knowledge graph."""
    if not args.workflow_id:
        raise ValueError("--workflow-id is required for write-handoff")
    if not args.agent:
        raise ValueError("--agent is required for write-handoff")
    if not args.goal:
        raise ValueError("--goal is required for write-handoff")

    uri, user, password, db = _require_kg_env()
    driver = GraphDatabase.driver(uri, auth=(user, password))

    # Generate unique handoff ID
    import uuid
    handoff_id = f"handoff_{args.workflow_id}_{args.agent}_{uuid.uuid4().hex[:8]}"

    query = """
MATCH (w:WorkflowState {workflow_id: $workflow_id})
CREATE (h:HandoffNode {
    handoff_id: $handoff_id,
    workflow_id: $workflow_id,
    agent: $agent,
    goal: $goal,
    changes: $changes,
    evidence: $evidence,
    risks: $risks,
    next_owner: $next_owner,
    open_questions: $open_questions,
    graph_update: $graph_update,
    created_at: datetime()
})
CREATE (h)-[:BELONGS_TO]->(w)
WITH w, h
SET w.phase = $phase,
    w.owner = $next_owner,
    w.updated_at = datetime()
RETURN h.handoff_id as id
"""
    with driver.session(database=db) as session:
        result = session.run(
            query,
            handoff_id=handoff_id,
            workflow_id=args.workflow_id,
            agent=args.agent,
            goal=args.goal,
            changes=args.changes or "",
            evidence=args.evidence or "не запускалось",
            risks=args.risks or "нет",
            next_owner=args.next_owner or "orchestrator",
            open_questions=args.open_questions or "",
            graph_update=args.graph_update or "нет",
            phase=args.phase or "implementation",
        )
        record = result.single()
        if record:
            print(f"Handoff written: {record['id']}")
    driver.close()


def read_state(args: argparse.Namespace) -> None:
    """Read current workflow state and recent handoffs from the knowledge graph."""
    if not args.workflow_id:
        raise ValueError("--workflow-id is required for read-state")

    uri, user, password, db = _require_kg_env()
    driver = GraphDatabase.driver(uri, auth=(user, password))

    query = """
MATCH (w:WorkflowState {workflow_id: $workflow_id})
OPTIONAL MATCH (h:HandoffNode)-[:BELONGS_TO]->(w)
WITH w, h ORDER BY h.created_at DESC
WITH w, collect(h)[0..5] as recent_handoffs
RETURN w.workflow_id as workflow_id,
       w.goal as goal,
       w.phase as phase,
       w.owner as owner,
       w.status as status,
       w.created_at as created_at,
       w.updated_at as updated_at,
       [h IN recent_handoffs | {
           handoff_id: h.handoff_id,
           agent: h.agent,
           goal: h.goal,
           changes: h.changes,
           evidence: h.evidence,
           risks: h.risks,
           next_owner: h.next_owner,
           created_at: toString(h.created_at)
       }] as handoffs
"""
    with driver.session(database=db) as session:
        result = session.run(query, workflow_id=args.workflow_id)
        record = result.single()
        if not record:
            print(f"Workflow not found: {args.workflow_id}")
            return

        print(f"=== Workflow State: {record['workflow_id']} ===")
        print(f"Goal: {record['goal']}")
        print(f"Phase: {record['phase']}")
        print(f"Owner: {record['owner']}")
        print(f"Status: {record['status']}")
        print(f"Created: {record['created_at']}")
        print(f"Updated: {record['updated_at']}")
        print()

        handoffs = record["handoffs"]
        if handoffs:
            print(f"=== Recent Handoffs ({len(handoffs)}) ===")
            for h in handoffs:
                print(f"\n--- {h['handoff_id']} ---")
                print(f"Agent: {h['agent']}")
                print(f"Goal: {h['goal']}")
                print(f"Changes: {h['changes']}")
                print(f"Evidence: {h['evidence']}")
                print(f"Risks: {h['risks']}")
                print(f"Next Owner: {h['next_owner']}")
                print(f"Created: {h['created_at']}")
        else:
            print("No handoffs yet.")
    driver.close()


def write_context(args: argparse.Namespace) -> None:
    """Write a context node for sharing between chats/agents."""
    if not args.workflow_id:
        raise ValueError("--workflow-id is required for write-context")
    if not args.context_type:
        raise ValueError("--context-type is required for write-context")
    if not args.content:
        raise ValueError("--content is required for write-context")

    uri, user, password, db = _require_kg_env()
    driver = GraphDatabase.driver(uri, auth=(user, password))

    import uuid
    context_id = f"ctx_{args.workflow_id}_{args.context_type}_{uuid.uuid4().hex[:8]}"

    query = """
MATCH (w:WorkflowState {workflow_id: $workflow_id})
MERGE (c:ContextNode {workflow_id: $workflow_id, context_type: $context_type})
SET c.context_id = $context_id,
    c.content = $content,
    c.agent = $agent,
    c.updated_at = datetime()
MERGE (c)-[:BELONGS_TO]->(w)
RETURN c.context_id as id
"""
    with driver.session(database=db) as session:
        result = session.run(
            query,
            context_id=context_id,
            workflow_id=args.workflow_id,
            context_type=args.context_type,
            content=args.content,
            agent=args.agent or "unknown",
        )
        record = result.single()
        if record:
            print(f"Context written: {record['id']}")
    driver.close()


def read_context(args: argparse.Namespace) -> None:
    """Read context nodes for a workflow."""
    if not args.workflow_id:
        raise ValueError("--workflow-id is required for read-context")

    uri, user, password, db = _require_kg_env()
    driver = GraphDatabase.driver(uri, auth=(user, password))

    query = """
MATCH (c:ContextNode {workflow_id: $workflow_id})
OPTIONAL MATCH (c)-[:BELONGS_TO]->(w:WorkflowState)
RETURN c.context_id as context_id,
       c.context_type as context_type,
       c.content as content,
       c.agent as agent,
       c.updated_at as updated_at
ORDER BY c.updated_at DESC
"""
    with driver.session(database=db) as session:
        results = list(session.run(query, workflow_id=args.workflow_id))
        if not results:
            print(f"No context found for workflow: {args.workflow_id}")
            return

        print(f"=== Context Nodes for {args.workflow_id} ({len(results)}) ===")
        for record in results:
            print(f"\n--- {record['context_type']} ---")
            print(f"ID: {record['context_id']}")
            print(f"Agent: {record['agent']}")
            print(f"Updated: {record['updated_at']}")
            print(f"Content:\n{record['content'][:500]}...")
    driver.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Context capsule builder and workflow state manager")
    mode = parser.add_mutually_exclusive_group(required=True)
    # Capsule modes
    mode.add_argument("--build", action="store_true", help="Build context capsule")
    mode.add_argument("--lint", action="store_true", help="Lint context capsule")
    mode.add_argument("--push-local", action="store_true", help="Push capsule to local KG")
    # Workflow state modes (Hybrid Orchestrator Dispatch + Graph Context)
    mode.add_argument("--init-workflow", action="store_true", help="Initialize new workflow state")
    mode.add_argument("--write-handoff", action="store_true", help="Write agent handoff to graph")
    mode.add_argument("--read-state", action="store_true", help="Read current workflow state")
    mode.add_argument("--write-context", action="store_true", help="Write context node for sharing")
    mode.add_argument("--read-context", action="store_true", help="Read context nodes for workflow")

    # Capsule arguments
    parser.add_argument("--topic", type=str, help="Capsule topic")
    parser.add_argument("--sha", type=str, help="Git commit sha")
    parser.add_argument("--branch", type=str, help="Git branch")
    parser.add_argument("--artifact", type=str, help="Artifact path")
    parser.add_argument("--k-hops", type=int, default=2, help="KG hops")
    parser.add_argument("--out", type=str, help="Output markdown path")

    # Workflow state arguments
    parser.add_argument("--workflow-id", type=str, help="Workflow identifier")
    parser.add_argument("--goal", type=str, help="Workflow/handoff goal")
    parser.add_argument("--phase", type=str, help="Current phase (analysis/research/implementation/review/validation/capsule)")
    parser.add_argument("--owner", type=str, help="Current owner (agent name)")
    parser.add_argument("--agent", type=str, help="Agent name for handoff")
    parser.add_argument("--changes", type=str, help="Changes description for handoff")
    parser.add_argument("--evidence", type=str, help="Evidence for handoff")
    parser.add_argument("--risks", type=str, help="Risks for handoff")
    parser.add_argument("--next-owner", type=str, help="Next owner for handoff")
    parser.add_argument("--open-questions", type=str, help="Open questions for handoff")
    parser.add_argument("--graph-update", type=str, help="Graph update required (да/нет)")
    parser.add_argument("--context-type", type=str, help="Context type (research/specification/decision)")
    parser.add_argument("--content", type=str, help="Content for context node")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    # Capsule modes
    if args.build:
        if not args.out:
            raise ValueError("--out is required for build")
        build_capsule(args)
        return
    if args.lint:
        lint_file(args.out)
        return
    if args.push_local:
        push_local(args)
        return

    # Workflow state modes
    if args.init_workflow:
        init_workflow(args)
        return
    if args.write_handoff:
        write_handoff(args)
        return
    if args.read_state:
        read_state(args)
        return
    if args.write_context:
        write_context(args)
        return
    if args.read_context:
        read_context(args)
        return

    raise ValueError("Mode is required")


if __name__ == "__main__":
    main()
