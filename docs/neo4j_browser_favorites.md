# Neo4j Browser Favorites — Helicomponents

Набор из **10 targeted Cypher-запросов** для навигации по двум графам в одном Neo4j Community инстансе. Используется label namespace prefix:

- **`Domain_*`** (16 labels): доменная модель симуляции (transitions, quotas, RTC, BOM).
- **`AgentKG_*`** (5 labels): operational state мультиагентного workflow (Workflow, Handoff, Context, Agent, Module).

## Способ A — Автоматический инжектор (рекомендуется)

1. Открыть Neo4j Browser (`http://localhost:7474/browser/`).
2. F12 → вкладка «Console».
3. Открыть [`neo4j_browser_favorites_injector.js`](neo4j_browser_favorites_injector.js), Ctrl+A, Ctrl+C.
4. Paste в Console → Enter.
5. F5 (refresh).
6. Слева в «Favorites» — папка `Helicomponents` с 10 закладками.

При первой пасте Chrome покажет защиту от self-XSS — набери «разрешить вставку» в **диалоге Chrome** (не в Console prompt).

## Способ B — Ручное копирование

В Browser → Favorites → New Folder `Helicomponents` → Add empty favorite → имя + Cypher из блоков ниже → Save.

## Набор закладок

### Domain Graph (5)

#### 01 — Domain — Full Schema (visual)

```cypher
MATCH (n)
WHERE any(l IN labels(n) WHERE l STARTS WITH 'Domain_')
WITH labels(n)[0] AS lbl, collect(n)[0] AS rep
WITH collect(rep) AS reps
UNWIND reps AS rep
OPTIONAL MATCH (rep)-[r]->(other)
WHERE any(l IN labels(other) WHERE l STARTS WITH 'Domain_')
RETURN rep, r, other;
```

#### 02 — Domain — State Transitions Matrix

```cypher
MATCH (from:Domain_State)-[t:TRANSITION]->(to:Domain_State)
RETURN from, t, to;
```

#### 03 — Domain — Quotas Ops Graph

```cypher
MATCH (q:Domain_QuotaFlow)
OPTIONAL MATCH (q)-[r]-(other)
RETURN q, r, other;
```

#### 04 — Domain — Quotas Repair Graph

```cypher
MATCH (n)
WHERE n:Domain_RepairLineRule OR n:Domain_SelectionRule OR n:Domain_SpawnRule
OPTIONAL MATCH (n)-[r]-(other)
RETURN n, r, other;
```

#### 05 — Domain — RTC Pipeline (48 functions)

```cypher
MATCH (l:Domain_RTCLayer)
OPTIONAL MATCH (l)-[r:NEXT_LAYER]->(next:Domain_RTCLayer)
RETURN l, r, next
ORDER BY l.order;
```

### Agent KG (4)

#### 06 — Agent KG — Full Schema (visual)

```cypher
MATCH (n)
WHERE any(l IN labels(n) WHERE l STARTS WITH 'AgentKG_')
WITH labels(n)[0] AS lbl, collect(n)[0] AS rep
WITH collect(rep) AS reps
UNWIND reps AS rep
OPTIONAL MATCH (rep)-[r]->(other)
WHERE any(l IN labels(other) WHERE l STARTS WITH 'AgentKG_')
RETURN rep, r, other;
```

#### 07 — Agent KG — Last 50 Handoffs

```cypher
MATCH (w:AgentKG_Workflow)-[:HAS_HANDOFF]->(h:AgentKG_Handoff)-[:BY_AGENT]->(a:AgentKG_Agent)
RETURN h.created_at AS created_at,
       a.name AS agent,
       h.next_owner_name AS next_owner,
       h.risk_tier AS risk,
       w.id AS workflow,
       h.id AS handoff_id
ORDER BY h.created_at DESC
LIMIT 50;
```

#### 08 — Agent KG — Last 10 Orchestrator → Human

```cypher
MATCH (w:AgentKG_Workflow)-[:HAS_HANDOFF]->(h:AgentKG_Handoff)-[:BY_AGENT]->(a:AgentKG_Agent {name: 'orchestrator'})
WHERE h.next_owner_name = 'human'
RETURN h.created_at AS created_at,
       h.risk_tier AS risk,
       w.id AS workflow,
       w.goal AS goal,
       h.id AS handoff_id
ORDER BY h.created_at DESC
LIMIT 10;
```

#### 09 — Agent KG — Modules Touched (visual)

```cypher
MATCH (h:AgentKG_Handoff)-[t:TOUCHES]->(m:AgentKG_Module)
RETURN h, t, m;
```

### Mixed (1)

#### 10 — Both Graphs — Sanity Check

```cypher
MATCH (n)
WITH labels(n)[0] AS label, count(*) AS cnt
RETURN
  CASE
    WHEN label STARTS WITH 'AgentKG_' THEN 'Agent KG'
    WHEN label STARTS WITH 'Domain_' THEN 'Domain Graph'
    ELSE 'Other'
  END AS graph_group,
  collect({label: label, count: cnt}) AS labels_breakdown,
  sum(cnt) AS total_nodes
ORDER BY graph_group;
```

## Label namespacing convention (2026-05-20)

После перехода на local-first контур введён namespace prefix для изоляции двух графов в одной Community-базе:

| Префикс | Источник | Labels |
|---|---|---|
| `Domain_*` | `config/transitions/*.json` через `code/utils/sync_domain_graph.py` | TransitionSpec, State, Rule, QuotaFlow, SelectionRule, RepairLineRule, SpawnRule, MessageBucket, RTCLayer, BomTemplate, BomGroup, BomPartNo, BomGroupLevel, BomNumberingRule, BomCompatibilityRule, BomReplaceabilityRule |
| `AgentKG_*` | `config/agent_kg.json` + archives через `tools/agent_kg_to_neo4j.py` | Workflow, Handoff, Context, Agent, Module |

**Правило**: новые labels — всегда с одним из двух префиксов. Без префикса = ошибка namespace, ловится через `10 — Sanity Check` (попадает в `Other`).

## Текущее состояние local Docker

| Графа | Узлы |
|---|---|
| Agent KG (`AgentKG_*`) | 1356 |
| Domain Graph (`Domain_*`) | 219 |
| Без префикса | 0 |
| **Итого** | **1575** |

## Регулярная репликация в local

После изменений в JSON SSoT — пере-проектить оба графа:

```bash
make sync-domain-graph         # Domain Graph (с префиксами Domain_*)
make kg-project-neo4j-full     # Agent KG (с префиксами AgentKG_*)
```

Aura контур запаркован (см. `.env`).
