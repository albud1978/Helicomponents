# Agent KG Projection To Neo4j

## Назначение

Variant C hybrid сохраняет Agent KG как JSON SSoT:

- active state: `config/agent_kg.json`
- archives: `config/agent_kg_archive/YYYY-MM-DD/W_*.jsonl`

Neo4j используется только как on-demand projection для визуального анализа связей в
Neo4j Browser. Граф не является source-of-truth и не заменяет JSON/JSONL audit.

## Когда запускать

Запускайте projection ad-hoc, когда нужно посмотреть связи workflow, handoff,
context и agent в графовом виде. Для обычного workflow и аудита источником фактов
остаются `config/agent_kg.json` и JSONL-архивы.

## Запуск

```bash
make kg-project-neo4j
```

Расширенные режимы:

```bash
python3 tools/agent_kg_to_neo4j.py --dry-run
python3 tools/agent_kg_to_neo4j.py --include-archive
python3 tools/agent_kg_to_neo4j.py --include-archive --reset
make kg-project-neo4j-full
```

Подключение берётся из `DOMAIN_NEO4J_URI`, `DOMAIN_NEO4J_USER`,
`DOMAIN_NEO4J_PASSWORD`, `DOMAIN_NEO4J_DB` или из CLI-аргументов `--uri`,
`--user`, `--password`, `--db`.

## Schema

Все Agent KG labels используют префикс `AgentKG_` (с 2026-05-20). Префикс
изолирует Agent KG от Domain Graph (`Domain_*`) в одной Community-базе.

Nodes:

- `(:AgentKG_Workflow {id})`: goal, status, owner, phase, timestamps, source,
  parent_workflow (опционально для derived workflows).
- `(:AgentKG_Handoff {id})`: agent, plan step, trace, risk, gate/status,
  criteria, created_at, usage_total_tokens, prev_handoff_hash.
- `(:AgentKG_Context {id})`: context_type, agent, timestamps, content_len,
  content_type. Full content is not copied.
- `(:AgentKG_Agent {name})`: kind = `orchestrator` или `subagent`.
- `(:AgentKG_Module {id})`: domain, not_includes (из `config/kg_modules.json`).

Relations:

- `(AgentKG_Workflow)-[:HAS_HANDOFF]->(AgentKG_Handoff)`
- `(AgentKG_Workflow)-[:HAS_CONTEXT]->(AgentKG_Context)`
- `(AgentKG_Workflow)-[:OWNED_BY]->(AgentKG_Agent)`
- `(AgentKG_Workflow)-[:DERIVED_FROM]->(AgentKG_Workflow)` (parent_workflow lineage)
- `(AgentKG_Handoff)-[:BY_AGENT]->(AgentKG_Agent)`
- `(AgentKG_Handoff)-[:NEXT_OWNER]->(AgentKG_Agent)` when next owner is not `human`
- `(AgentKG_Handoff)-[:TOUCHES]->(AgentKG_Module)` (long-term memory, modules field)
- `(AgentKG_Handoff)-[:SUPERSEDES]->(AgentKG_Handoff)` (handoff replacement chain)

## Browser Favorites

Готовый набор Cypher-закладок для Neo4j Browser (Domain Graph + Agent KG в одном инстансе): [`docs/neo4j_browser_favorites.md`](neo4j_browser_favorites.md).

## Cypher Examples

Top-5 workflow by handoff count:

```cypher
MATCH (w:AgentKG_Workflow)-[:HAS_HANDOFF]->(h:AgentKG_Handoff)
RETURN w.id AS workflow, count(h) AS handoffs
ORDER BY handoffs DESC
LIMIT 5;
```

Agent with max handoffs:

```cypher
MATCH (h:AgentKG_Handoff)-[:BY_AGENT]->(a:AgentKG_Agent)
RETURN a.name AS agent, count(h) AS handoffs
ORDER BY handoffs DESC
LIMIT 1;
```

Workflow with graph updates:

```cypher
MATCH (w:AgentKG_Workflow)-[:HAS_HANDOFF]->(h:AgentKG_Handoff)
WHERE h.graph_update IN ["yes", "да"]
RETURN DISTINCT w.id AS workflow, h.id AS handoff, h.graph_update AS graph_update;
```

Recent medium/high-risk handoffs:

```cypher
MATCH (h:AgentKG_Handoff)-[:BY_AGENT]->(a:AgentKG_Agent)
WHERE h.risk_tier IN ["medium", "high"]
RETURN h.created_at AS created_at, h.risk_tier AS risk, a.name AS agent, h.id AS handoff
ORDER BY created_at DESC
LIMIT 10;
```

## Audit Note

Context content is intentionally limited to metadata (`content_len`,
`content_type`). For full audit details use `config/agent_kg.json` and
`config/agent_kg_archive/**/*.jsonl`.

Legacy archive handoff records created before `handoff_id` existed receive
deterministic synthetic ids during projection:
`legacy_<sha1(workflow_id + agent + created_at)[:12]>`. This is an explicit
archive transformation for stable Neo4j `MERGE`; active KG handoffs without
`handoff_id` still fail fast.
