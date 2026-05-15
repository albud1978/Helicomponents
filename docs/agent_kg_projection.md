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

Nodes:

- `(:Workflow {id})`: goal, status, owner, phase, timestamps, source.
- `(:Handoff {id})`: agent, plan step, trace, risk, gate/status, criteria,
  created_at, usage_total_tokens.
- `(:Context {id})`: context_type, agent, timestamps, content_len,
  content_type. Full content is not copied.
- `(:Agent {name})`: kind = `orchestrator` или `subagent`.

Relations:

- `(Workflow)-[:HAS_HANDOFF]->(Handoff)`
- `(Workflow)-[:HAS_CONTEXT]->(Context)`
- `(Workflow)-[:OWNED_BY]->(Agent)`
- `(Handoff)-[:BY_AGENT]->(Agent)`
- `(Handoff)-[:NEXT_OWNER]->(Agent)` when next owner is not `human`

## Cypher Examples

Top-5 workflow by handoff count:

```cypher
MATCH (w:Workflow)-[:HAS_HANDOFF]->(h:Handoff)
RETURN w.id AS workflow, count(h) AS handoffs
ORDER BY handoffs DESC
LIMIT 5;
```

Agent with max handoffs:

```cypher
MATCH (h:Handoff)-[:BY_AGENT]->(a:Agent)
RETURN a.name AS agent, count(h) AS handoffs
ORDER BY handoffs DESC
LIMIT 1;
```

Workflow with graph updates:

```cypher
MATCH (w:Workflow)-[:HAS_HANDOFF]->(h:Handoff)
WHERE h.graph_update IN ["yes", "да"]
RETURN DISTINCT w.id AS workflow, h.id AS handoff, h.graph_update AS graph_update;
```

Recent medium/high-risk handoffs:

```cypher
MATCH (h:Handoff)-[:BY_AGENT]->(a:Agent)
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
