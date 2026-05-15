# Suppliers matrix

**Цель документа.** Supplier-centric view сторонних компонентов (vendor / поставщик → используемые компоненты → criticality → continuity plan). Дополняет [`THIRD_PARTY.md`](../../THIRD_PARTY.md), которая организована component-centric. Используется для governance/audit (Compass C7) и для оценки бизнес-рисков supply chain.

**Source of truth.** [`THIRD_PARTY.md`](../../THIRD_PARTY.md) (license inventory) + [`deploy/sbom/sbom.cdx.json`](../../deploy/sbom/sbom.cdx.json) (machine-readable SBOM). Эта матрица — derived view.

**Last reviewed.** 15-05-2026 (orchestrator, Tier-1 batch C7).

---

## 1. Criticality framework

- **Critical** — без этого supplier'а проект **не работает** (нет alternative с приемлемым cost). Замена = re-architecture.
- **High** — supplier важен, но есть теоретическая замена за 1-3 месяца с заметным cost.
- **Medium** — заменим за 1-4 недели; есть open-source alternative.
- **Low** — utility/permissive код, заменим за дни.

## 2. Supplier matrix

### S1 — NVIDIA Corporation

- **Components.** CUDA Toolkit 13.0 (proprietary EULA, см. THIRD_PARTY §4); RAPIDS cuDF 25.12.0 (Apache 2.0, NVIDIA-led project).
- **License model.** Proprietary EULA для Toolkit; Apache 2.0 для RAPIDS components.
- **Criticality.** **Critical** — без CUDA Toolkit GPU-симуляция не работает; CPU fallback неприемлемо медленный для production-scale моделей.
- **Update cadence.** CUDA Toolkit — major release раз в 1-2 года; RAPIDS — quarterly.
- **Continuity plan.** Toolkit поставляется с GPU. Risk: NVIDIA меняет EULA в сторону жёстче — обновляем доcument compliance. Risk: NVIDIA прекращает support конкретной CUDA версии — миграция на новый Toolkit + регрессионные тесты RTC kernels.
- **Vendor contact.** NVIDIA Developer Program (free), для commercial support — NVIDIA Enterprise.

### S2 — University of Sheffield / FLAME GPU team

- **Components.** pyflamegpu 2.0.0rc4+cuda130 (AGPL v3 + Commercial dual).
- **License model.** Strong copyleft AGPL v3 с § 13 (Remote Network Interaction) — критично для future-state SaaS. Commercial dual licence доступна для proprietary deployments.
- **Criticality.** **Critical** — основа всей агент-based симуляции; замены equivalent-quality GPU framework нет (только academic FLAME-CPU, MASS, GAMA — несравнимо по производительности).
- **Update cadence.** Releases раз в 6-18 месяцев; current тег — release candidate (2.0.0rc4), production-ready не отмечен.
- **Continuity plan.** Если planning SaaS-режим / SaaS-API — требуется AGPL § 13 review или commercial license. См. [`THIRD_PARTY.md` §5](../../THIRD_PARTY.md). Risk: project freezes — fork в private mirror; risk: пересмотр лицензии upstream — оценить commercial dual.
- **Vendor contact.** [flamegpu.com](https://flamegpu.com), GitHub issues. Commercial inquiry — University of Sheffield licensing office.

### S3 — Neo4j Inc.

- **Components.** Neo4j Community Server 5.20 (GPL v3, Docker self-hosted); neo4j Python driver (Apache 2.0).
- **License model.** Server — strong copyleft GPL v3, но **используется как separate service** через bolt:// API — нет linking, не триггерит copyleft на наш код. Driver — Apache 2.0 (permissive).
- **Criticality.** **Medium** — Neo4j используется как **визуализация** Domain Graph + Agent KG projection (Variant C); основной SSoT — JSON в репозитории. Замена: KuzuDB embedded (MIT), Memgraph (BSL), Apache AGE (PostgreSQL extension, Apache 2.0).
- **Update cadence.** Major раз в год (5.x → 6.x); patch monthly.
- **Continuity plan.** Если Neo4j Inc. меняет CE на restrictive license — миграция на KuzuDB embedded (упомянуто в audit) или Apache AGE. Текущие Cypher запросы переносимы.
- **Vendor contact.** [neo4j.com](https://neo4j.com); GitHub issues для CE.

### S4 — ClickHouse Inc. / Yandex Managed Service

- **Components.** ClickHouse Server (external, YC managed, Apache 2.0); clickhouse-driver 0.2.10 (MIT); clickhouse-connect 0.10.0 (Apache 2.0).
- **License model.** Все permissive (Apache 2.0 + MIT).
- **Criticality.** **High** — основной DWH для всех симуляционных результатов и BI; замена потребует значительной миграции (но Apache 2.0 разрешает self-host).
- **Update cadence.** ClickHouse Server — quarterly; драйверы — monthly.
- **Continuity plan.** Yandex Cloud Managed Service — risk: тарифные изменения / regional restrictions. Self-hosted ClickHouse Server — fallback (тот же Apache 2.0).
- **Vendor contact.** Yandex Cloud Support (managed instance); ClickHouse Inc. community (driver issues).

### S5 — Apache Software Foundation

- **Components.** Apache Superset (external sandbox Docker, Apache 2.0).
- **License model.** Apache 2.0 — permissive.
- **Criticality.** **High** — основная BI-платформа; замена (Metabase, Grafana, Redash) требует переноса 50+ dashboards.
- **Update cadence.** Superset — quarterly major; patch monthly.
- **Continuity plan.** Apache Software Foundation — vendor-neutral; supply risk минимальный. Risk: значительный security CVE — patch cadence.
- **Vendor contact.** [superset.apache.org](https://superset.apache.org); GitHub issues.

### S6 — PyData / NumFOCUS ecosystem

- **Components.** pandas 2.3.3 (BSD 3-Clause); numpy 2.2.6 (BSD 3-Clause); openpyxl 3.1.5 (MIT); PyYAML 6.0.3 (MIT); pathlib2 2.3.7 (MIT).
- **License model.** Все permissive.
- **Criticality.** **Critical** (numpy/pandas) — основа всего ETL и анализа; замена невозможна без полной переписки.
- **Update cadence.** Major numpy/pandas — yearly; patch monthly.
- **Continuity plan.** Community-driven (NumFOCUS sponsored). Supply risk минимальный — широкая community, корпоративные sponsors.
- **Vendor contact.** [numfocus.org](https://numfocus.org); GitHub issues.

### S7 — LangChain Inc.

- **Components.** langgraph (unpinned, MIT).
- **License model.** MIT — permissive.
- **Criticality.** **Low** — используется в `code/agents/`; зависимости умеренные, есть alternative (LlamaIndex, AutoGen, raw OpenAI SDK).
- **Update cadence.** Active development, monthly releases; API breaking changes возможны.
- **Continuity plan.** Pin version для stability; если LangChain Inc. меняет license — fork последней MIT-версии.
- **Vendor contact.** [langchain.com](https://langchain.com); GitHub issues.

### S8 — Anysphere Inc. (Cursor AI)

- **Components.** Cursor IDE + Cursor CLI + Cursor SDK (SaaS, proprietary).
- **License model.** Proprietary SaaS subscription (Cursor Terms of Service).
- **Criticality.** **High** — текущая работа multi-agent framework полагается на Cursor agent infrastructure; теоретически переносимо на Claude Code/Cody/Continue, но требует подгонки hooks и rule format.
- **Update cadence.** Cursor — frequent updates (weekly+); breaking changes в agent profile format возможны (см. недавние правки `90_multiagent_workflow.mdc`).
- **Continuity plan.** Hooks написаны на Python независимо от Cursor; rules в `.mdc` (с YAML frontmatter) переносимы. При смене вендора — adapter layer для нового agent runtime.
- **Vendor contact.** Cursor support; [cursor.com](https://cursor.com).

## 3. Cross-vendor risks

- **Lock-in концентрации.** S1 (NVIDIA) + S2 (FLAME GPU) — оба critical, оба не-альтернативны → высокая концентрация в GPU-симуляционном стеке. Mitigation: capture архитектурные decisions в Domain Graph SSoT (JSON), чтобы при миграции не потерять semantics.
- **License combinatorics.** S2 AGPL v3 + S3 GPL v3 + S8 proprietary SaaS — гетерогенно. Compliance проверяется поартефактно: AGPL v3 § 13 только если planning SaaS API (см. THIRD_PARTY §5).
- **Supply chain integrity.** SBOM (`deploy/sbom/sbom.cdx.json`) фиксирует hashes; обновление SBOM при каждом dependency change (P0.6 практика).

## 4. Update policy

- При добавлении нового supplier (новый vendor, не просто новый component уже tracked supplier'а) — добавить раздел в §2.
- При смене license model upstream-supplier — Q-уровень workflow с governance check (см. THIRD_PARTY update policy).
- При деprecation supplier'а (предупреждение об EOL) — поднять risk на 1 уровень и план миграции в OpenQuestions.
- Cross-reference с THIRD_PARTY.md — при изменении одной матрицы синхронизировать смежные записи.
