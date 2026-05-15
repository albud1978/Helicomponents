# Multi-Agent Workflow Appendices

> **Назначение**: extracted detail из `.cursor/rules/90_multiagent_workflow.mdc` core. Не дублирует core, а описывает enhancements / history / operational detail. Core содержит **что**, appendices — **как** и **почему**.

> **Cross-link**: `.cursor/rules/90_multiagent_workflow.mdc` (canonical core).

> **Created**: 15-05-2026 (Tier-M M1 split, workflow `W_optim_tier_m_2026_05_15`).

## A1. Запрет архитектурных исследований и разработки для оркестратора

- Оркестратор не ведёт самостоятельно архитектурные исследования и разработку. Сюда входят: анализ дизайна системы и компонентов, dependency/topology mapping, ревизия и проектирование правил/процессов/хуков/профилей агентов, оценка trade-off архитектурных решений, любое написание/правка production-кода.
- Это не относится к operational data-проверкам: read-only SQL, `grep`/`rg`, count/lookup в `config/agent_kg.json` и других JSON, точечная сверка фактов из репо. Их оркестратор выполняет сам для оперативной фактологии и handoff/audit.
- Базовый режим архитектурного исследования: один subagent на `claude-opus-4-7-thinking-high` — **не требует** approval пользователя (Cursor Task tool whitelist для subagent не содержит `xhigh`, использовать `high`).
- Two-model cross-check (параллельный dispatch двух subagent — `claude-opus-4-7-thinking-high` и `gpt-5.5-high` — с debate-loop до 5 раундов) применяется только при **явном approval пользователя** в текущем чате. Без approval оркестратор может предложить cross-check, но не запускает второго subagent.
- Архитектурная разработка ведётся по объёму:
  - **Малый атомарный патч** (≤3 файла, ≤150 строк, одно смысловое изменение) — оркестратор может выполнить сам в allowlist (`.cursor/agents/**`, `.cursor/hooks/**`, `.cursor/rules/**`, `docs/**`) после approval пользователя на предложение.
  - **Крупная архитектурная разработка** (>3 файлов, >150 строк или новые публичные контракты) — делегируется `coder-general` через Task tool после approval пользователя.
  - Оба режима сохраняют human-gate; two-model cross-check на исследование/предложение применяется по запросу пользователя.

## A2. Workflow lifecycle enhancements (Tier-1 to Tier-4 lite history)

### A2.1. C2 approval_request context (Tier-1)

- **`approval_request` context (C2):** для `medium/high` обязательно через `python3 code/utils/agent_kg.py --register-approval-request --workflow-id W_<id> --content "..."` до запроса approval. `orchestrator_guard.py` выдаёт soft WARNING при пропуске.
- Перед запросом `medium/high-risk` approval orchestrator обязан зарегистрировать `approval_request` context в `Agent KG` через `python3 code/utils/agent_kg.py --register-approval-request --workflow-id W_<id> --content "..."`. `orchestrator_guard.py` выдаёт soft WARNING (на `beforeSubmitPrompt`), если active medium/high workflow без зарегистрированного context.

### A2.2. F2 init PRE-approval (Tier-1)

- **Init workflow PRE-approval (Tier-1 F2):** для continuation high-risk approval orchestrator должен `--init-workflow` ДО ответа на approval phrase. `orchestrator_guard.py` выдаёт soft WARNING `init_pre_approval` при approval phrase + 0 active medium/high workflow.
- **Init workflow PRE-approval (continuation high-risk).** Для continuation high-risk approval'а в существующем чате (где новый scope требует нового `W_<id>`) orchestrator обязан выполнить `--init-workflow` + `--register-approval-request` **до** ответа на approval phrase. Иначе `user_comm_audit.log` зафиксирует `workflow_id_source=none`, что снижает аудит-trace полноту. `orchestrator_guard.py` выдаёт soft WARNING `init_pre_approval`, если approval phrase в prompt + нет active workflow с `risk_tier ∈ {medium, high}`.

### A2.3. D1 SSoT-gate (Variant B)

- **SSoT-gate (D1):** для `high-risk` SSoT-операций (`config/transitions/*.json`, `make sync-domain-graph`) — workflow-scoped hard-block через `ssot_approval_guard.py`.
- `ssot_approval_guard.py` (preToolUse) требует active workflow в Agent KG, зарегистрированный `approval_request` context, и audit-confirmation `approval_hint=yes` для того же `workflow_id`. KG unavailable → fail-safe deny. Auto-invalidate при close-workflow.

### A2.4. D2 Reviewer-flame loop

- **Reviewer-flame loop (D2):** `coder-flame` `medium/high` → `reviewer-flame` обязателен в том же workflow. `orchestrator_guard.py` выдаёт soft WARNING.
- **Mandatory reviewer-flame loop**: для любого `coder-flame` handoff с `risk_tier ∈ {medium, high}` обязателен последующий `reviewer-flame` handoff **в том же workflow** до его закрытия. `orchestrator_guard.py` (`beforeSubmitPrompt`) выдаёт soft WARNING; жёсткий блок не применяется — оркестратор реагирует превентивно.

### A2.5. C11 Workflow caps (Tier-2b)

- **Workflow caps (Tier-2b C11):**
  - `--init-workflow` **всегда** применяет default caps `max_steps=50` / `max_tokens=500000` (`code/utils/agent_kg.py:DEFAULT_MAX_STEPS/DEFAULT_MAX_TOKENS`).
  - Явные `--max-steps`/`--max-tokens` переопределяют defaults.
  - `--set-caps` корректирует caps на existing workflow.
  - `pre_gate_guard.py` блокирует `Task` dispatch когда `usage.cumulative_steps >= caps.max_steps` или `cumulative_tokens >= max_tokens` (hard-block).
  - `--write-handoff` накапливает `usage.cumulative_steps/tokens` и выдаёт soft WARNING при превышении.
  - Legacy workflow (созданные до Tier-2b commit) без caps пропускаются (backward-compat).

### A2.6. S5 Token coverage analytics (Tier-2b)

- **Token coverage analytics (Tier-2b S5):**
  - `python3 tools/token_analytics.py --by-workflow --top N`
  - `python3 tools/token_analytics.py --workflow-summary W_<id>`
  - `python3 tools/token_analytics.py --export-json`
  - Назначение: анализ token-расхода по workflow с utilization% vs caps.
  - Источник: `handoffs[].usage.{model,est_tokens,source}` + `workflows[].usage/caps`.

### A2.7. S1+S2 Framework template (Tier-3 Variant B)

- **Framework template (Tier-3 S1+S2 Variant B):**
  - `framework/manifest.yaml` — inventory L1 (generic, reusable) vs L3 (project-specific) для 10 agent profiles + hooks + rules + tools + docs.
  - Extraction: `python3 tools/extract_framework.py [--force|--dry-run]`.
  - Output: `framework/template_out/` с L1-only template (L3 fields заменяются на TODO placeholders).
  - `framework/template_out/` — build artifact, в `.gitignore`.
  - См. также `framework/manifest.yaml.not_in_template` для зон которые никогда не входят в template (SSoT, data, capsules-as-L2).

### A2.8. C12 Version drift detection (Tier-3)

- **Version drift detection (Tier-3 C12):**
  - `config/versions_manifest.json` — semver + SHA-256 baseline для 48 versioned files (agents/hooks/rules/capsules/schemas/framework manifest).
  - Drift check: `python3 tools/version_check.py [--summary-only|--exit-on-drift|--update]`.
  - Reports: `OK/DRIFT/MISSING/UNTRACKED`.
  - `--update` refresh только hash (semver bump — manual decision orchestrator/human).
  - Не включает `config/agent_kg.json` (state-store), `config/transitions/**` (SSoT, отдельный versioning), `docs/changelog.md` (history log).

### A2.9. C9 RACI matrix (Tier-3)

- **RACI matrix (Tier-3 C9):** `docs/governance/raci.md` — 14 типов action × 9 акторов с Responsible/Accountable/Consulted/Informed. Operational gates mapping: SSoT → `ssot_approval_guard`; RTC medium/high → reviewer-flame loop; orchestrator allowlist → `orchestrator_write_guard`; workflow caps → `pre_gate_guard`. Living doc, обновляется при изменении ролей.
- Cross-link: `docs/governance/raci.md`.

### A2.10. Tier-4 lite Maturity tools

- **Maturity tools (Tier-4 lite, opt-in, не блокирующие):**
  - `tools/kg_to_otel.py` — OTel JSON trace export для analysis в Jaeger/Grafana.
  - `tools/security_smoke.py` — 8 prompt injection / plan deviation / SSoT bypass cases; informational.
  - `tools/pii_scan.py` — PII regex scan KG/handoffs; default strict, `--include-loose-patterns` для broad E.164.
  - Policy doc: `docs/governance/privacy.md` (что хранится, что не должно, cleanup procedures).
- Cross-link: `docs/governance/privacy.md`.

## A3. Pre_gate / Pre_close detail

### A3.1. pre_gate hook detail

- Не выполняет LLM-governance сам по себе.
- Проверяет:
  - указан ли `workflow_id`
  - существует ли workflow и находится ли он в статусе `active`
  - требует ли prompt явный `Handoff` назад оркестратору

### A3.2. pre_close hook detail

- Не запускает `governance-compliance`, а валидирует **уже записанные** артефакты.
- Проверяет:
  - traceability (`trace_id`, `plan_step_id`)
  - orchestrator handoff с `drift_check` и `graph_update=yes|no`
  - наличие обязательных governance/docs handoff по риску и policy

## A4. Phase recap (medium/high / coder-flame / long task)

- В начале фазы делай короткий context recap из файлов, если есть риск context rot: `medium/high-risk`, `coder-flame`, длинная задача, resume/compaction или явный запрос человека.
- Recap берётся из `config/agent_kg.json` (`workflow_id`, `phase`, `risk_tier`/последний handoff), `config/capsules_manifest.json` и 1-2 релевантных `docs/*_capsule.md`.
- Капсулы остаются read-only проекциями, не SSoT; при расхождении верить исходным JSON/коду/SQL.
- Для low-risk housekeeping без context rot phase recap не обязателен, чтобы не раздувать Handoff-lite.

## A5. Daily hygiene check

- `tools/hygiene_check.py` сканирует `config/agent_kg.json`, `config/capsules_manifest.json` и `config/transitions/invariants.json` на 5 категорий rot:
  - **Stale workflows** — active дольше `--stale-days` (default 1 день).
  - **Stale capsules** — `last_verified_against_ssot` старше threshold.
  - **Phantom invariants** — invariants в манифесте без определения в SSoT.
  - **Incomplete handoffs** — closed workflows без orchestrator handoff с `trace_id+plan_step_id`.
  - **Dangling approval_requests** — `approval_request` contexts в workflows active дольше threshold.
- **Trigger**: `orchestrator_guard.py` запускает `--summary-only` при первом prompt в новом UTC-дне (через state file `.cursor/hooks/.hygiene_last_check.txt`). При findings — добавляет reminder в `agentMessage` с counts.
- **Human-in-the-loop**: Алексей решает — закрыть workflow, re-verify капсулу, или skip. Никакого hard-block и automated cleanup.
- **Manual run**: `python3 tools/hygiene_check.py` — детальный отчёт с конкретными ID.
- Exit `1` при findings (CI-friendly).

## A6. Observability и аудит — Decision log

- В handoff и Agent KG фиксировать:
  - `decision_summary`
  - `alternatives_considered`
  - `selected_route`
  - `why_not_chosen`
- Не сохранять полный chain of thought в `Agent KG`.

## A7. Audit field rules

- `user_comm_audit.log` — метаданные взаимодействия с человеком.
- `code_edit_audit.log` — технический след правок.
- `docs/changelog.md` — короткий аудит изменений и итогов ревью/валидации, когда есть doc-impact.
- В пользовательском approval-запросе для `medium/high-risk` должен явно фигурировать `W_<workflow_id>`.
- `user_comm_audit.py` должен логировать `workflow_id_source`; для `high-risk` approval trace считается полным, если `workflow_id` пришёл явно или был выведен из:
  - единственного активного approval-context (`inferred_approval_context`),
  - единственного pending high-risk handoff (`inferred_pending_high_risk`),
  - единственного active workflow при confirmation-like prompt (`inferred_unique_active_approval`) — fallback для continuation после late init.
