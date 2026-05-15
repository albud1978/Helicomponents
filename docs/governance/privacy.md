# Privacy and data lifecycle

**Owner**: orchestrator
**Status**: living document
**Created**: 15-05-2026 (C14-lite из A∪B∪C unified roadmap, workflow `W_tier4_lite_maturity_2026_05_15`)
**Related**: [raci.md](raci.md), [suppliers.md](suppliers.md), [SECURITY.md](../../SECURITY.md)

## Назначение

Документ описывает что хранится в `Agent KG`, hook'ах, audit logs и handoff'ах **multi-agent framework**, какие данные **не** должны попадать в эти артефакты, и как сделать ad-hoc cleanup при необходимости. Это **maturity practice** — не формальный DPIA/GDPR framework. Цель: явность data lifecycle и предотвращение случайного leakage чувствительных данных через workflow артефакты.

## Что хранится — каталог артефактов

### 1. `config/agent_kg.json` (operational state-store)

| Поле | Тип | Содержимое | Чувствительность |
|---|---|---|---|
| `workflows[].workflow_id` | string | identifier `W_<purpose>_<date>` | low |
| `workflows[].goal` | string | описание задачи от orchestrator | **medium** — может содержать имена систем, scope changes |
| `workflows[].owner` | string | `orchestrator` или другой agent slug | low |
| `workflows[].caps`, `usage` | object | budget tracking | low |
| `handoffs[].user_goal` | string | дословная цитата запроса пользователя | **high** — может содержать любой пользовательский ввод |
| `handoffs[].changes`, `facts`, `evidence_pack` | string | summary изменений + ссылки на файлы/SQL | **medium** — может содержать пути, SQL queries |
| `handoffs[].usage` | object | model, est_tokens, source | low |
| `contexts[].content` | string | approval request texts, phase recap notes | **medium** — может содержать approval phrases пользователя |

### 2. JSONL archives (`config/agent_kg.*.jsonl`)
- Закрытые workflows + старые handoffs (после split). Те же поля что выше, plus retention по дате.

### 3. Audit logs (`.cursor/hooks/*.log`, append-only)

| Лог | Содержимое | Чувствительность |
|---|---|---|
| `user_comm_audit.log` | timestamp, `prompt_len`, `approval_hint`, `workflow_id`, hash chain — НЕ полный prompt | **low** (только metadata) |
| `code_edit_audit.log` | path, agent, model, conv id, timestamp — НЕ полный diff | **low** (только metadata) |
| `.cursor/hooks/.hygiene_last_check.txt` | дата последнего hygiene run | low |

### 4. Capsules (`docs/*_capsule.md`)
- Read-only domain context для агентов. Не должны содержать operational data; статичные референсы на SSoT.

### 5. Output / data directories
- `data_input/**`, `output/**`, `logs/**`, `data/**` — **scope бизнес-данных**, не управляется этим документом (см. project-level data governance).

## Что **не** должно попадать в KG/handoffs/audit

### Строго запрещено
- **Secrets**: пароли, токены, API keys, `.env` values (CLICKHOUSE_PASSWORD, NEO4J_PASSWORD, JIRA_TOKEN и т.д.)
- **PII клиентов/третьих лиц**:
  - Email адреса вне команды разработки
  - Номера телефонов
  - ИНН/IIN/SSN/паспортные данные
  - Полные имена сторонних лиц
- **PHI / медицинские данные** (не применимо для Helicomponents, но как принцип)
- **Финансовые данные** (номера карт, банковские реквизиты)

### Допустимо
- Имена систем и vendors (NVIDIA, ClickHouse, Cursor, Anysphere)
- Названия проектных таблиц БД (`heli_pandas`, `mp3_arrays`)
- Конфигурационные пути (`code/sim_v2/messaging/`, `config/transitions/`)
- Email команды разработки (1-3 человека, если упомянуты в `pyproject.toml`/`SECURITY.md`)
- Internal IPs если они не secret (`10.96.96.47` — LAN, OK; production endpoints — НЕ OK)

## Retention policy

- `config/agent_kg.json` (active): держится размером ≤300KB через периодический split в JSONL архивы (тригер: ручной `--split` или hygiene check warning).
- JSONL archives: append-only, retention неограничен пока не превысит 100MB total. После — manual prune старейших workflows по дате.
- Audit logs: append-only, retention неограничен. При достижении 50MB на лог — manual rotation.
- Capsules: версионируются через `tools/version_check.py` (Tier-3 C12), drift detection активен.

## Tooling

- **`tools/pii_scan.py`** (C14-lite, Tier-4 lite): regex scan handoffs/contexts/KG goal+facts на PII patterns (email вне whitelist, phone numbers, ИНН/IIN). Informational warning, не блокирует.
- **`tools/audit_verify.py`** (Tier-1): verifies hash chain в `user_comm_audit.log` / `code_edit_audit.log`. Tamper-evidence.
- **`tools/hygiene_check.py`**: stale workflows + stale capsules + dangling approvals — daily check.

## Cleanup procedures

### Удалить конкретный handoff с leaked PII
1. `python3 code/utils/agent_kg.py --delete-handoff --handoff-id <id>` (если такой CLI существует; иначе manual JSON edit + audit note в changelog)
2. Сделать backup `cp config/agent_kg.json config/agent_kg.backup.$(date +%Y%m%d).json` перед manual edit.
3. После cleanup — `--write-handoff` с `agent=orchestrator`, `risk_tier=low`, `changes="cleanup: removed handoff_id <X> per privacy policy"`, и `trace_id` с указанием причины.

### Удалить весь workflow (rare)
- Только при критическом leak. Backup KG + JSONL archives → удалить из `workflows[]` + связанные `handoffs[]` + `contexts[]` → audit note + push.

### Audit logs append-only — НЕ редактировать
- Если PII попало в `user_comm_audit.log` — оставить (hash chain нарушится при правке). Альтернатива: **truncate** лог + создать новый chain (с записью причины в changelog).

## Boundary с третьими лицами (vendor data flow)

См. [suppliers.md](suppliers.md) — vendor-centric matrix. Ключевые data flows:
- **Cursor / Anysphere**: prompts + outputs идут на их API (включая содержимое файлов, открытых в IDE). Это **inherent**, не контролируется framework. Не помещай secrets в открытые файлы.
- **NVIDIA / OpenAI / Anthropic** (через Cursor): аналогично, prompts идут на эти LLM endpoints.
- **Neo4j / ClickHouse**: local (Docker), data residency на машине разработчика.
- **Superset (corporate/utair)**: BI dashboard contents — данные клиентских систем, доступ control через Superset RBAC, не через framework.

## Что НЕ покрывает этот документ

- Full GDPR DPIA framework
- Data classification scheme (Tier-0/1/2/3 sensitivity labels)
- Cross-jurisdiction residency requirements
- Right-to-erasure SLA enforcement
- DSAR (Data Subject Access Request) handling

Эти аспекты — для enterprise/regulated environments. Текущий scope: **maturity practice** для personal/team-scale framework.

## Обновление документа

- **Trigger**: новый MCP server / vendor добавлен, новый тип данных в KG/handoffs, изменение retention policy, обнаруженный leak.
- **Process**: orchestrator предлагает update → human approval → docs-curator updates → version_check refresh hash.

## История

- 15-05-2026: создан в рамках C14-lite из A∪B∪C roadmap (workflow `W_tier4_lite_maturity_2026_05_15`).
