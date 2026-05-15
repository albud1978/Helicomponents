# Agent Risk Classification

**Цель документа.** Формализованная матрица "агент → тип действий → зона импакта → допустимый risk-tier → требования к human-gate" для multi-agent framework. Этот документ дополняет `.cursor/rules/00_global_always.mdc` (правила) и `.cursor/rules/90_multiagent_workflow.mdc` (workflow), сводя их в одну таблицу для governance/audit.

**Source of truth.** Operational policy — в `.cursor/rules/*.mdc`. Этот документ — derived view для human-readable audit и onboarding.

**Last reviewed.** 15-05-2026 (orchestrator, Tier-1 batch C4).

---

## 1. Risk-tier framework (recap из `00_global_always.mdc`)

- **`low`** — локальные обратимые изменения в `.cursor/**`, `docs/**`, BI metadata, process housekeeping; нет влияния на доменную модель.
- **`medium`** — изменения логики с ограниченным blast radius; validation/BI semantics/runtime-конфигурации; без смены архитектуры.
- **`high`** — `code/sim_v2/**`, `config/transitions/**`, `invariants.json`, `make sync-domain-graph`, архитектурные/алгоритмические изменения, новые публичные API, удаление объектов, production/corporate BI apply.

Default escalation: при неопределённости — выбирать более высокий риск.

## 2. Action-type taxonomy

- **Type-A Mutating code** — прямой write/edit в `code/**` или `tools/**`.
- **Type-B Mutating docs/config/policy** — edit в `docs/**`, `.cursor/**`, `README.md`, `Makefile`, `requirements.txt`, `deploy/bi-as-code/**`, etc.
- **Type-C Read-only analysis** — SELECT/Grep/Read без write; SQL fact-check.
- **Type-D Review/Governance** — read-only assessment с verdict; advisory.
- **Type-E Orchestration** — план, маршрутизация, governance-гейты; без прямого кода.

## 3. Impact-zone taxonomy

- **Zone-1 High-risk core** — `code/sim_v2/**`, `config/transitions/**`, `invariants.json`, `make sync-domain-graph`.
- **Zone-2 Medium-risk codebase** — `code/**` (кроме `sim_v2`), `tools/**`, `deploy/**`, schemas validation.
- **Zone-3 Low-risk meta** — `docs/**`, `.cursor/**`, `README.md`, BI metadata, configuration files в `config/**` кроме `transitions/`.
- **Zone-BI BI-as-code** — `deploy/bi-as-code/**`, Superset personal/corporate sandbox, production BI.

## 4. Agent matrix

### Orchestrator-layer

- **`orchestrator`** (claude-opus-4-7-thinking-xhigh) — Type-E. Allowlist: `.cursor/agents/**`, `.cursor/hooks/**`, `.cursor/rules/**`, `docs/**`, `README.md`, plan-артефакты. Denylist: `code/**`, `tools/**`, `deploy/**` (кроме docs). Risk-tier reachable through delegation: any (включая high). Human-gate: required для high-risk dispatch. Delegation depth: 1.

### Governance-layer

- **`governance-compliance`** (claude-opus-4-7-thinking-high) — Type-D. Allowlist: read-only по всему репо; write только в Agent KG через `agent_kg.py --write-handoff` и related `--register-*`. Denylist: file edits (кроме self-handoff в KG). Risk-tier scope: all (verdict для всех tiers). Human-gate: required для `decision=reject` на high-risk.

### Coding-layer

- **`coder-flame`** (gpt-5.5-high) — Type-A. Allowlist: `code/sim_v2/messaging/**`, `code/sim_v2/**` (RTC/CUDA/FLAME GPU). Denylist: `config/transitions/**`, `invariants.json`, `code/archive/**`, `.cursor/**`. Risk-tier scope: medium-high (high требует reviewer-flame). Human-gate: required для high. Reviewer: `reviewer-flame` после каждой mutation.
- **`coder-general`** (gpt-5.5-high) — Type-A. Allowlist: ETL/extract/analysis/utils/config/docs/tools/deploy. Denylist: `code/sim_v2/**`, `config/transitions/**`, `invariants.json`. Risk-tier scope: low-medium (high требует явного approval). Human-gate: required для medium+ в `tools/**`. Reviewer: для high-risk — отдельный handoff к reviewer-flame даже на не-RTC код.

### Review-layer

- **`reviewer-flame`** (claude-opus-4-7-thinking-high) — Type-D. Allowlist: read-only по `code/sim_v2/**`, `tests/**`; write только в Agent KG handoff. Denylist: file edits. Risk-tier scope: high (RTC/CUDA). Triggered: проактивно после `coder-flame` mutation.

### Research / Analysis layer

- **`research-graph-analyst`** (claude-opus-4-7-thinking-high) — Type-C. Allowlist: read по репо; SELECT в ClickHouse (read-only); Cypher SELECT в Neo4j. Denylist: write/mutate в DB; file edits. Risk-tier scope: low (read-only). Human-gate: not required.
- **`bi-semantic-analyst`** (claude-opus-4-7-thinking-high) — Type-C. Allowlist: read Superset metadata; SELECT в ClickHouse; analysis BI charts/datasets. Denylist: BI artifact mutations (только handoff с предложениями). Risk-tier scope: low-medium. Human-gate: required для медиа-уровневых рекомендаций по production BI.
- **`validator-judge`** (gpt-5.5-high) — Type-C. Allowlist: SELECT по ClickHouse (read-only); SQL invariant verification; запуск read-only валидаторов. Denylist: write в DB; file edits. Risk-tier scope: low-medium. Human-gate: required для verdict=fail на high-risk simulation runs (escalation).

### Docs / Capsule layer

- **`docs-curator`** (gpt-5.5-high) — Type-B. Allowlist: `docs/**`, `README.md`, `docs/changelog.md`. Denylist: `code/**`, `tools/**`, `config/**` (кроме docs-related). Risk-tier scope: low-medium. Human-gate: required для doc-changes, влияющих на high-risk solo/team policy.
- **`capsule-builder`** (gpt-5.5-high) — Type-B. Allowlist: `docs/*_capsule.md`, `config/capsules_manifest.json`. Denylist: `code/**`, `tools/**`. Risk-tier scope: low-medium. Human-gate: required для изменений принятых архитектурных decisions в capsules.

### Deprecated / legacy

- **`analyst-sql-graph`** (auto) — **DEPRECATED**. Сохранён только для совместимости с историческими handoff в Agent KG. Не маршрутизируется новыми orchestrator dispatch. Risk-tier scope: N/A. Удалить файл-профиль — отдельный low-risk patch.

## 5. Mapping: типичные task'и → risk-tier

- Drift fixes документации (INV/TEMP/links) → low → `docs-curator` или orchestrator.
- BI chart/dashboard add в personal sandbox → low-medium → `bi-semantic-analyst` + `coder-general` (BI-as-code).
- ETL refactor (без semantic changes) → medium → `coder-general` + review (по необходимости).
- RTC kernel edit / FLAME GPU bug fix → high → `coder-flame` + `reviewer-flame` + governance.
- Transitions/quota rule change → high → `coder-general` + governance + human gate + `make sync-domain-graph` после approval.
- Domain Graph sync (`make sync-domain-graph`) → high → orchestrator + human gate.
- Production BI deploy (corporate/production) → high → orchestrator + governance + human gate.
- Удаление любого "объекта проекта" (file/dir/table/dashboard) → high → human gate (правило `00_global_always.mdc`).

## 6. Update policy

- При добавлении нового агента — обновить разделы 4 и (при необходимости) 5.
- При изменении risk-tier rules в `.cursor/rules/00_global_always.mdc` — синхронизировать разделы 1 и 5.
- Любое изменение этого документа на high-risk policy — через `docs-curator` handoff (medium-risk doc-impact).
- Cross-reference с future `.cursor/agents/*.md` extended YAML frontmatter (Tier-2 эпик S3+C1+C3+S4) — этот документ остаётся human-readable summary, frontmatter — machine-readable single source of truth.
