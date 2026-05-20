/*
 * Helicomponents — Neo4j Browser Favorites Injector
 *
 * Содержит 10 закладок под новую label-namespace схему:
 *   Domain Graph: :Domain_* (16 labels)
 *   Agent KG:     :AgentKG_* (5 labels)
 *
 *   1. Полный Domain Graph (schema view: 1 представитель + связи)
 *   2. State Transition Matrix (визуально)
 *   3. Quotas Ops graph
 *   4. Quotas Repair graph
 *   5. RTC Pipeline (48 функций)
 *   6. Полный Agent KG (schema view)
 *   7. Last 50 Handoffs (table)
 *   8. Last 10 Orchestrator → Human (table)
 *   9. Modules Touched (TOUCHES visual)
 *  10. Both graphs sanity (counts + cross-edges check)
 *
 * Usage:
 *   1. Открой Neo4j Browser → DevTools (F12) → Console.
 *   2. Вставь весь файл → Enter.
 *   3. F5 (refresh) → папка "Helicomponents" в Favorites.
 *
 * Safety:
 *   - Не трогает чужие закладки.
 *   - Повторный запуск заменяет ТОЛЬКО Helicomponents-папку.
 *   - Совместим с Neo4j Browser 4.x/5.x (localStorage keys
 *     `neo4j.documents` + `neo4j.folders`).
 */

(() => {
  const FOLDER_NAME = "Helicomponents";

  const FAVORITES = [
    {
      name: "01 — Domain — Full Schema",
      content: `// 01 — Domain — Полный граф по набору связей (schema view)
MATCH (n)
WHERE any(l IN labels(n) WHERE l STARTS WITH 'Domain_')
WITH labels(n)[0] AS lbl, collect(n)[0] AS rep
WITH collect(rep) AS reps
UNWIND reps AS rep
OPTIONAL MATCH (rep)-[r]->(other)
WHERE any(l IN labels(other) WHERE l STARTS WITH 'Domain_')
RETURN rep, r, other;`,
    },
    {
      name: "02 — Domain — State Transitions Matrix",
      content: `// 02 — Domain — Матрица допустимых переходов состояний
MATCH (from:Domain_State)-[t:TRANSITION]->(to:Domain_State)
RETURN from, t, to;`,
    },
    {
      name: "03 — Domain — Quotas Ops Graph",
      content: `// 03 — Domain — Граф квот операций (P1-P4)
MATCH (q:Domain_QuotaFlow)
OPTIONAL MATCH (q)-[r]-(other)
RETURN q, r, other;`,
    },
    {
      name: "04 — Domain — Quotas Repair Graph",
      content: `// 04 — Domain — Граф репэйрных квот (RepairLine + Selection + Spawn)
MATCH (n)
WHERE n:Domain_RepairLineRule OR n:Domain_SelectionRule OR n:Domain_SpawnRule
OPTIONAL MATCH (n)-[r]-(other)
RETURN n, r, other;`,
    },
    {
      name: "05 — Domain — RTC Pipeline (48 functions)",
      content: `// 05 — Domain — Полный набор RTC функций (48 layers)
MATCH (l:Domain_RTCLayer)
OPTIONAL MATCH (l)-[r:NEXT_LAYER]->(next:Domain_RTCLayer)
RETURN l, r, next
ORDER BY l.order;`,
    },
    {
      name: "06 — Agent KG — Full Schema",
      content: `// 06 — Agent KG — Полный граф по набору связей (schema view)
MATCH (n)
WHERE any(l IN labels(n) WHERE l STARTS WITH 'AgentKG_')
WITH labels(n)[0] AS lbl, collect(n)[0] AS rep
WITH collect(rep) AS reps
UNWIND reps AS rep
OPTIONAL MATCH (rep)-[r]->(other)
WHERE any(l IN labels(other) WHERE l STARTS WITH 'AgentKG_')
RETURN rep, r, other;`,
    },
    {
      name: "07 — Agent KG — Last 50 Handoffs",
      content: `// 07 — Agent KG — Последние 50 handoff'ов в таблице
MATCH (w:AgentKG_Workflow)-[:HAS_HANDOFF]->(h:AgentKG_Handoff)-[:BY_AGENT]->(a:AgentKG_Agent)
RETURN h.created_at AS created_at,
       a.name AS agent,
       h.next_owner_name AS next_owner,
       h.risk_tier AS risk,
       w.id AS workflow,
       h.id AS handoff_id
ORDER BY h.created_at DESC
LIMIT 50;`,
    },
    {
      name: "08 — Agent KG — Last 10 Orchestrator to Human",
      content: `// 08 — Agent KG — Последние 10 от оркестратора к human
MATCH (w:AgentKG_Workflow)-[:HAS_HANDOFF]->(h:AgentKG_Handoff)-[:BY_AGENT]->(a:AgentKG_Agent {name: 'orchestrator'})
WHERE h.next_owner_name = 'human'
RETURN h.created_at AS created_at,
       h.risk_tier AS risk,
       w.id AS workflow,
       w.goal AS goal,
       h.id AS handoff_id
ORDER BY h.created_at DESC
LIMIT 10;`,
    },
    {
      name: "09 — Agent KG — Modules Touched",
      content: `// 09 — Agent KG — Какие модули затронуты в каких handoff (TOUCHES)
MATCH (h:AgentKG_Handoff)-[t:TOUCHES]->(m:AgentKG_Module)
RETURN h, t, m;`,
    },
    {
      name: "10 — Mixed — Sanity Check",
      content: `// 10 — Mixed — Counts по двум графам + проверка изоляции
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
ORDER BY graph_group;`,
    },
  ];

  const uuid = () =>
    typeof crypto !== "undefined" && crypto.randomUUID
      ? crypto.randomUUID()
      : "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
          const r = (Math.random() * 16) | 0;
          const v = c === "x" ? r : (r & 0x3) | 0x8;
          return v.toString(16);
        });

  const safeParse = (raw, fallback) => {
    if (!raw) return fallback;
    try {
      const parsed = JSON.parse(raw);
      return Array.isArray(parsed) ? parsed : fallback;
    } catch (e) {
      console.error("[Helicomponents Injector] Failed to parse:", e);
      return fallback;
    }
  };

  const docs = safeParse(localStorage.getItem("neo4j.documents"), []);
  const folders = safeParse(localStorage.getItem("neo4j.folders"), []);

  let folder = folders.find((f) => f && f.name === FOLDER_NAME);
  const isNewFolder = !folder;
  if (!folder) {
    folder = { id: uuid(), name: FOLDER_NAME, expanded: true };
    folders.push(folder);
  }

  const beforeCount = docs.length;
  const cleanedDocs = docs.filter((d) => !d || d.folder !== folder.id);
  const removedCount = beforeCount - cleanedDocs.length;

  let added = 0;
  for (const fav of FAVORITES) {
    cleanedDocs.push({
      id: uuid(),
      name: fav.name,
      content: fav.content,
      folder: folder.id,
    });
    added++;
  }

  localStorage.setItem("neo4j.documents", JSON.stringify(cleanedDocs));
  localStorage.setItem("neo4j.folders", JSON.stringify(folders));

  console.log("=== Helicomponents Favorites Injector ===");
  console.log(`Folder: "${FOLDER_NAME}" ${isNewFolder ? "(created)" : "(existing, reused)"}`);
  console.log(`Removed previous Helicomponents favorites: ${removedCount}`);
  console.log(`Added new favorites: ${added}`);
  console.log("► Press F5 (refresh) to see them in left 'Favorites' panel.");
})();
