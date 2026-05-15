# RACI matrix — multi-agent framework

**Owner**: orchestrator
**Status**: living document (обновляется при изменении ролей агентов или зон ответственности)
**Created**: 15-05-2026 (C9 из A∪B∪C unified roadmap, workflow `W_tier3_template_2026_05_15`)
**Related**: [agent_risk_classification.md](agent_risk_classification.md) (risk-tier × impact-zone), [suppliers.md](suppliers.md) (vendor governance), [.cursor/rules/90_multiagent_workflow.mdc](../../.cursor/rules/90_multiagent_workflow.mdc) (canonical workflow rules)

## Назначение

RACI matrix формализует распределение ролей по 14 типам действий между **9 акторами**:
- **Human** (Алексей) — final authority, architectural/algorithmic approval, business decisions
- **Orchestrator** — planning, routing, governance gates, KG management
- **coder-flame** — FLAME GPU/CUDA/RTC код
- **coder-general** — ETL/extract/analysis/utils/tools/BI-as-code
- **reviewer-flame** — RTC/CUDA code review
- **validator-judge** — simulation result validation (SQL-first)
- **research-graph-analyst / bi-semantic-analyst** — read-only investigation
- **governance-compliance** — policy/risk/traceability checker
- **docs-curator / capsule-builder** — docs synchronization
- **System** — hooks (`pre_gate_guard`, `pre_close_guard`, `orchestrator_guard`, `ssot_approval_guard`, `user_comm_audit`), CI workflow, KG validators

## Легенда

- **R** — Responsible: выполняет работу.
- **A** — Accountable: owns outcome, подписывает (≤1 на action).
- **C** — Consulted: запрашивается мнение перед решением.
- **I** — Informed: уведомляется после решения (handoff/audit/changelog).

## Матрица

| # | Action type | Human | Orchestrator | coder-flame | coder-general | reviewer-flame | validator-judge | research/bi analysts | governance | docs-curator / capsule | System hooks |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | **Architectural changes** (новые модули, новые публичные API, удаление объектов, изменение state machine) | **A**, R | C, I | — | — | I | I | C | C | I | I (hooks audit) |
| 2 | **Algorithmic changes** (расчётные функции, инварианты в логике) | **A**, R | C, I | C | C | I | I | C | C | I | I |
| 3 | **SSoT schema changes** (`config/transitions/**`, `invariants.json`, Domain Graph sync) | **A** | R (orchestrate), I | — | — | I | C (validate after) | C | **C** (gate block) | I | **R** (`ssot_approval_guard`) |
| 4 | **Code changes — RTC/GPU** (`code/sim_v2/**`) | **A** для high/medium-risk | R (dispatch), I | **R** | — | **R** (reviewer loop required) | C (results validate) | C | C (medium/high) | I | I (audit edit) |
| 5 | **Code changes — General** (`code/etl/**`, `code/utils/**`, `code/extract/**`, `tools/**`) | A для medium, I для low | R (dispatch), I | — | **R** | — | I | C | C (medium) | I | I (audit edit) |
| 6 | **BI as code — personal sandbox** (`deploy/bi-as-code/**` SSoT) | A | R (orchestrate), I | — | **R** | — | — | C (bi-semantic-analyst) | I (low/medium) | I | — |
| 7 | **BI as code — corporate sandbox** (deploy/clone в corporate Superset) | **A** (явная команда) | R, I | — | R | — | — | C | **C** (verdict required) | I | — |
| 8 | **BI as code — production** | **A** (только handoff пакет, не apply) | R (prepare handoff), I | — | — | — | — | C | **C** | I (handoff doc) | — |
| 9 | **Documentation updates** (`docs/**`, `README.md`, `changelog.md`) | I | R (small) или dispatch | — | — | — | — | — | I (если governance handoff) | **R** (`docs-curator`) | — |
| 10 | **Rule/agent profile changes** (`.cursor/agents/**`, `.cursor/rules/**`, `.cursor/hooks/**`) | A для policy semantics | **R** (orchestrator allowlist) | — | — | — | — | — | C (medium) | I (changelog) | I (audit edit) |
| 11 | **Workflow management** (`Agent KG`: init/dispatch/handoff/close) | I | **R**, **A** | I | I | I | I | I | I | I | **R** (`pre_gate_guard`, `pre_close_guard`) |
| 12 | **Governance verdicts** (medium/high approval, policy/scope/traceability) | A для override | C (target) | — | — | — | — | — | **R**, **A** | I | I (`user_comm_audit`) |
| 13 | **Capsule maintenance** (`docs/*_capsule.md`, `config/capsules_manifest.json`) | I | R (dispatch) | I (если flame_gpu_capsule) | — | — | — | C | I | **R** (`capsule-builder`) | — |
| 14 | **Domain Graph sync** (`make sync-domain-graph`, Neo4j) | **A** (явное согласование) | R (orchestrate), I | — | R (execute SQL/JSON sync) | — | C (verify counts) | C (read-only Cypher) | **C** (gate) | I | **R** (`ssot_approval_guard`) |

## Ключевые принципы

### 1. Human всегда `A` для архитектуры/алгоритмов/SSoT
Никакой агент не может самостоятельно принимать решения по типам **#1, #2, #3, #14**. Orchestrator может предложить и реализовать, но Accountable остаётся Алексей.

### 2. Орchestrator не пишет код в `code/**` / `tools/**`
Type #4, #5, #6 — orchestrator **R** для **orchestrate/dispatch**, но **R+A** за код принадлежит coder-агентам. Self-coding в этих зонах = policy violation (`orchestrator_write_guard.py` hard-block).

### 3. Reviewer-flame loop для RTC medium/high
Type #4 medium/high → **reviewer-flame R** обязателен в том же workflow. `orchestrator_guard.py` soft WARNING при пропуске.

### 4. Governance-compliance — control plane, не worker
Type #12 — governance единственный **R+A** для policy verdicts. Никто другой не может вынести verdict от его имени. Human может override через явную команду.

### 5. System hooks — независимый actor
Type #3, #11, #14 — **System R** (через hooks) дополняет human/orchestrator. Hooks могут заблокировать действие без участия LLM-агента (hard-block на основании policy enforcement).

### 6. Один `A` на action
Если в строке два **A** (например row #1 `Human A` и `Orchestrator C, I`), это означает что Алексей — primary accountable, orchestrator — executor accountable (sub-A для execution discipline, не для outcome ownership).

## Operational gates (mapping на правила)

| Action type | Gate | Enforced by |
|---|---|---|
| #1, #2 | Architectural approval phrase в чате | `00_global_always.mdc` — manual policy |
| #3 | `--register-approval-request` context + `ssot_approval_guard` workflow-scoped block | `90_multiagent_workflow.mdc` + hook |
| #4 (medium/high) | `coder-flame → reviewer-flame` loop + governance verdict | `90_multiagent_workflow.mdc` |
| #5 (medium) | governance verdict | `90_multiagent_workflow.mdc` |
| #7, #8 | Явная команда Алексея в чате + governance verdict | `00_global_always.mdc` BI section |
| #10 | orchestrator allowlist enforced by `orchestrator_write_guard` | hook |
| #11 | workflow_id + active workflow + caps not exceeded | `pre_gate_guard` |
| #12 | конкретный matrix чек по risk-tier | `governance-compliance` agent |
| #14 | `ssot_approval_guard` workflow-scoped + явное Алексея согласование | hook + manual |

## Обновление RACI

- **Trigger**: добавление нового агента, изменение allowlist у существующего, смена ownership BI/Domain Graph, обновление policy в `.cursor/rules/90_multiagent_workflow.mdc`.
- **Process**: предложение через orchestrator handoff → human approval (`A`) → docs-curator updates this file → workflow handoff с `drift_check`.
- **Версионирование**: после внедрения C12 (Tier-3 same batch) этот файл будет включён в `config/versions_manifest.json` с content-hash для drift detection.

## История

- 15-05-2026: создан в рамках C9 из A∪B∪C roadmap (workflow `W_tier3_template_2026_05_15`).
