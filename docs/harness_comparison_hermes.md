# Сравнительный анализ harness'ов: Helicomponents vs Hermes

Дата: 2026-07-13. Объекты сравнения:

- **Helicomponents** (`albud1978/helicomponents`) — прогнозирование жизненного цикла компонентов вертолётов (FLAME GPU / CUDA, ClickHouse, Neo4j, Superset). Harness: governance-first обвязка под Cursor с Knowledge Graph bookkeeping и явным слоем loop engineering.
- **Hermes** (`albud1978/hermes`, HEAD `2c43a7a`) — open-source агентный фреймворк от Nous Research (Python-ядро, SQLite/FTS5, Ink/React TUI, 20+ LLM-провайдеров). Harness: распределённая мета-обвязка для парка автономных агентов на нескольких машинах.

Важная асимметрия: в Hermes два слоя — **продуктовый рантайм** (Hermes сам является агентным harness'ом: loop, делегирование, skills, память, kanban, cron) и **мета-обвязка разработки** («канон»-governance, fleet SSoT, kanban-шина, heartbeat'ы, git-гарды). Прямой аналог harness'а Helicomponents — именно мета-слой; продуктовый слой привлекается там, где он выполняет ту же функцию.

---

## 1. Профили в одну строку

| | Helicomponents | Hermes |
|---|---|---|
| Философия | Governance-first: детерминированные гейты вокруг необратимых side-эффектов (GPU-симуляции, DROP в ClickHouse, корпоративный BI) | Fleet-first: координация парка автономных агентов на нескольких машинах через durable-шину и git-«канон» |
| Единица работы | Workflow `W_*` в JSON Knowledge Graph | Карточка kanban (SQLite REST-доска) |
| Замыкание цикла | Машинное стоп-условие: verifiable SuccessCriteria + `pre_close_guard` | Artifact-verification: независимая проверка координатором (WALL-E) по 5 артефактам |
| Носитель правил | `.cursor/rules/*.mdc` (SSoT) + 11 карточек агентов | `AGENTS.md` (53 КБ) + `docs/deploy/canon-rules.md` + 2 тонких `.mdc` |
| Enforcement | ~13 хуков-guardrails (preToolUse, fail-safe deny) | git-хуки pre-commit/pre-push + risk-классификатор diff |
| Топология | 1 машина, 1 оркестратор, одноуровневая делегация | N машин, координатор + девопс-агенты + профили-исполнители |

---

## 2. Loop engineering

### Helicomponents: явный внутрисессионный цикл

Единственный из двух harness'ов с формализованным слоем, названным «loop engineering» (Tier-L):

- Канонический 10-шаговый цикл: Анализ → Pre-gate → Проверка → Реализация → Review/Validation → Governance → Docs → Capsule → Pre-close → Handoff/Close. Оркестратор ведёт явную state-машину («логика как в LangGraph, без внедрения LangGraph»), single active worker, переход только по артефакту (`--write-handoff`).
- **Auto-drive для low-risk**: оркестратор проходит цикл без ручных подтверждений между обратимыми шагами; стоп-условия — SuccessCriteria выполнен, token-подсветка, high-risk scope, явный input человека.
- **Машинное стоп-условие**: для medium/high `pre_close_guard.py` блокирует закрытие workflow без ссылки на `validation_sql` / `INV-N` / acceptance.
- **Token budgeting**: soft-подсветка 150k est_tokens (≈p90), hard-cap 500k в `pre_gate_guard`.
- **Анти-зацикливание**: max 3 итерации `implement → review`, затем эскалация. Triage-очередь (`W_triage_queue`) для non-blocking follow-ups.
- Матрица 10-step × risk-tier (low / medium / high) + 4 профиля интенсивности governance.

### Hermes: межсессионный цикл через доску

Цикл работы вынесен из сессии агента на durable-инфраструктуру:

- Стейт-машина карточки: `ready → in_progress → blocked/done → archived`. Working-поллер будит агента под ready-карточку с его assignee; агент клеймит атомарно и исполняет автономно.
- **HARD RULE верификации**: перед `done` каждый пункт приёмки сверяется с фактом (cron существует и включён, файл на диске, конфиг применён по grep живого файла, сервис active, SHA в origin/main). Неподтверждённый пункт → `blocked`, не `done`. Мотивировано задокументированным инцидентом: компрессия контекста стёрла промежуточный шаг, карточка закрылась «на слово».
- **Гейт координатора**: «Без WALL-E никакая карточка не уходит в done» — независимая верификация вторым агентом по 5 артефактам.
- Продуктовый рантайм-цикл: синхронный tool-calling loop с бюджетом итераций и авто-компрессией контекста; `todo`-toolset, standing `/goal`, `/queue`, background-задачи.
- Анти-луп на шине: максимум 3 карточки в ready на агента, bot↔bot переписка закрыта, `failure_limit=2` → авто-блок диспетчера.

### Сопоставление

| Аспект | Helicomponents | Hermes |
|---|---|---|
| Где живёт цикл | Внутри сессии (state-машина оркестратора) | Между сессиями (доска + поллеры + cron) |
| Стоп-условие | Машинное: verifiable SuccessCriteria, enforced хуком | Социотехническое: независимый ревью артефактов вторым агентом |
| Границы ресурсов | Токены (soft 150k / hard 500k), шаги (`max_steps=50`) | Итерации LLM-цикла; лимиты карточек на агента |
| Анти-зацикливание | Лимит implement→review = 3 | failure_limit=2, лимит ready=3, backoff поллеров |
| Отложенная работа | Triage-очередь в KG | Карточки в ready + cron-задачи |
| Durability цикла | KG переживает сессию, но драйвер — живая сессия оркестратора | Полностью durable: поллеры/cron перезапускают работу без человека |

Ключевое различие: у Helicomponents цикл **глубже** (10 формализованных шагов с гейтами на каждом), у Hermes — **живучее** (переживает смерть сессии, машины и даже компрессию контекста, потому что состояние и верификация вынесены наружу).

---

## 3. Состояние, bookkeeping, знания

| Механизм | Helicomponents | Hermes |
|---|---|---|
| SSoT координации | `config/agent_kg.json` (3.4 МБ; 211 workflows, 620 handoffs, 392 contexts) + JSONL-архивы по датам | SQLite-kanban на выделенном хосте (REST :8080), карточки/комментарии/runs |
| Версионирование состояния | В git: отдельные коммиты `chore(kg): bookkeeping init W_*` / `close W_*` | Вне git: доска durable, git хранит только код и доки |
| Целостность аудита | **Hash-chain**: каждый handoff несёт `prev_handoff_hash` (SHA-256, forward-only, `verify_kg_chain.py`) | Идемпотентность (idempotency-keys heartbeat'ов), но криптографической цепочки нет |
| Граф знаний | Neo4j — **только on-demand проекция**, не SSoT | Графовый LightRAG сознательно отложен («НЕ строить впрок») |
| Память агента | Контекстные капсулы (RLM, 8 обязательных секций, read-only проекции JSON) | 3 уровня: pgvector recall (hot/warm/cold), `MEMORY.md`/`USER.md` (только указатели), `session_search` по SQLite |
| Избыточность знаний | Одна копия (KG в git) + капсулы-проекции | Правило §15: «Ни одно знание — в единственной копии» (git + Obsidian + MEMORY.md + еженедельные бэкапы) |
| Телеметрия | `token_analytics.py`, usage в WorkflowState, экспорт в OTel (`kg_to_otel.py`) | Heartbeat раз в час (health шлюза, recall-ошибки, curator-статус), digest владельцу, board-watch аномалий |

Helicomponents трактует bookkeeping как **аудит** (tamper-evident журнал: кто, что, почему, с каким риском), Hermes — как **операционный мониторинг** (жив ли агент, не застряла ли карточка). Hash-chain — уникальная фича Helicomponents; мультикопийность знаний и heartbeat-мониторинг — уникальные фичи Hermes.

---

## 4. Правила и SSoT

**Helicomponents** — антидрейф-максимализм: правила ровно в одном месте (`.cursor/rules/*.mdc`), `.cursorrules` — только указатель; строгая иерархия SSoT (правила → доменные JSON → инварианты → шаблон handoff → runbook → BI-as-code), производные (Neo4j, капсулы) явно объявлены не-истиной («При расхождении с JSON — верить JSON», «Запрещено верифицировать логику по Neo4j»). Изменение SSoT — только с явного human approval, enforced хуком (`ssot_approval_guard`, fail-safe deny при недоступности KG). Плюс `versions_manifest.json`: SHA-256 baseline 48 файлов harness'а с проверкой DRIFT.

**Hermes** — SSoT через git-топологию: модель «трёх зон» (канон в `main` только через PR; оверлей машинных веток — свободно; вне git `$HERMES_HOME` — свободно). Явные SSoT: `fleet-ssot.md` (топология парка, менять только по команде владельца), `main` как канон кода, реестры-как-SSoT (`commands.py`, `toolsets.py` — всё производное генерится из них), «canonical source is the filesystem». Правила разложены: тонкие `.mdc` (2 файла, alwaysApply) → полный свод `canon-rules.md` → история в `repo-governance.md`.

Различие в механизме защиты SSoT: Helicomponents защищает **рантайм-хуками в момент правки** (deny до записи), Hermes — **git-хуками в момент фиксации** (pre-commit/pre-push + risk-классификатор diff: 🔴 CRITICAL / 🟡 HIGH / 🟢 SAFE / ❓→HIGH). Подход Hermes дешевле и переносимее, подход Helicomponents ловит нарушение раньше и не обходится через `git commit --no-verify`.

---

## 5. Роли и иерархия агентов

| | Helicomponents | Hermes |
|---|---|---|
| Модель | 11 деклараций-карточек (`.cursor/agents/*.md`, YAML `agent_card` со схемой и валидатором): orchestrator, coder-flame/general, reviewer-flame, validator-judge, governance-compliance, docs-curator, capsule-builder, 3 аналитика | Роли прозой в `fleet-ssot.md` + код делегирования: координатор WALL-E, девопс-агенты по машинам (BB-8, Nafanya, GTD, Kuzya), профили-исполнители (flamedev, invest, paralegal, dvk) |
| Права | Per-card scope (allowed/denied/read_only paths), tools allow/deny, `risk_tier_max`, бюджеты; RACI 14×9 | Матрица прав §16 (кто читает/создаёт/правит навыки; часть профилей полностью read-only) |
| Иерархия | Жёстко одноуровневая: запрет субагентов ≥3 уровня, single active worker, последовательный pipeline | Рантайм: leaf/orchestrator-делегирование, глубина 2, до 3 конкурентных детей; мета-уровень: координатор + распределённые исполнители, параллельные по машинам |
| Разделение полномочий | Оркестратору запрещён кодинг (allowlist `.cursor/**`, `docs/**`); validator-judge read-only | Только WALL-E имеет прямой push в main и право закрывать карточки |

Helicomponents формализует роли **декларативно** (карточки со схемой, валидируются в CI) — это сильнее, чем проза Hermes. Hermes зато реально исполняет **параллельность и распределённость**, которых в Helicomponents нет вовсе (сознательно: строго последовательный pipeline без параллельных мутирующих воркеров).

---

## 6. Верификация и гейты качества

**Helicomponents:**
- ~13 детерминированных хуков-guardrails (`preToolUse`/`beforeSubmitPrompt`/`afterFileEdit`): pre_gate, pre_close, ssot_approval (fail-safe deny), orchestrator_write, clickhouse_drop_guard (тотальный запрет DROP/TRUNCATE после инцидента 2026-07-03, обход только `DROP_APPROVED_BY_ALEXEY=1`), superset_docker_guard и др.
- SSoT инвариантов: INV-1..13 + TEMP + GPU, каждый с `expr`, severity, validator-скриптом и `validation_sql`; validator-judge выносит verdict только по этим полям (SQL-first).
- CI `quality.yml`: JSON validity → py-compile → invariant drift check → валидация agent cards → hygiene → audit verify. Тестов симуляции в CI нет (нужен CUDA-хост).
- Гигиена: `hygiene_check.py` (5 категорий rot, авто-триггер раз в день), `version_check.py` (SHA-256 baseline), `security_smoke.py` (8 injection-кейсов, informational), `pii_scan.py`.

**Hermes:**
- Git-гейты: pre-commit («канон-гард» — блок правок вне оверлея в машинных ветках), pre-push (блок force-push и прямого push в main) + `canon-risk-assess.py` в трёх точках (pre-push, auto-sync workflow, auto-pull).
- Тестовый гейт: обязательный `run_tests.sh` (hermetic: сброс всех `*_API_KEY`, TZ=UTC, temp HERMES_HOME, subprocess-per-test изоляция); дисциплина «no change-detector tests» (инварианты вместо снапшотов).
- CI почти выключен: 14 из 15 workflows в `disabled/`; активен только auto-sync main→ветки (с risk-гейтом; при блоке — issue). Docker smoke-test action с регресс-гардами на конкретные инциденты.
- Skill-гейт HARDLINE: 8 формальных правил для SKILL.md, нарушение — reject PR.
- Runtime-верификация: artifact-check перед done + независимый ревью WALL-E + heartbeat/board-watch/watchdog-мониторинг.

Симметричное различие: Helicomponents силён в **предотвращении** (deny до действия, инварианты домена, hash-chain), Hermes — в **обнаружении и восстановлении** (мониторинг, независимая верификация фактов, авто-синк, избыточность знаний). У Helicomponents многие гейты — soft warning (полагаются на реактивность оркестратора); у Hermes аналогично многое держится на дисциплине координатора, но критический путь (done-гейт) продублирован независимым агентом.

---

## 7. Совместимость с инструментами

| | Helicomponents | Hermes |
|---|---|---|
| Cursor | Полная: `.cursor/rules` (12 файлов), `.cursor/agents` (11), `.cursor/hooks` (13), `.cursor/skills` (3, один пустой) | Минимальная: 2 тонких `.mdc` |
| Claude Code | Нет `CLAUDE.md`, нет `.claude/` — hooks в формате Cursor не переносятся | Нет `CLAUDE.md`/`.claude/`, но `AGENTS.md` читается Claude Code нативно |
| Универсальность | Низкая: harness жёстко привязан к Cursor hook-API | Выше: AGENTS.md + git-хуки + внешняя шина работают с любым агентом |
| Переносимость harness'а | Спроектирована: `framework/manifest.yaml` (L1 generic / L3 project-specific), `extract_framework.py` | Не выделена как шаблон; мета-обвязка вросла в конкретный парк машин |

---

## 8. Чего нет у каждого (пробелы)

**Helicomponents:**
- Нет durable-исполнения: цикл умирает вместе с сессией оркестратора; нет поллеров/cron, которые перезапустят работу.
- Нет независимой верификации вторым агентом перед close (pre_close проверяет форму handoff'а, не факты в живой системе).
- Нет избыточности знаний (одна копия KG) и нет heartbeat-мониторинга.
- Нет параллелизма; нет CI-прогона тестов симуляции; секрет-менеджмент реактивный (инцидент с паролем в git-истории, filter-repo purge).
- Нет нативного формата Claude Code / AGENTS.md.

**Hermes:**
- Нет формализованного внутрисессионного цикла plan→execute→verify→record вне kanban-контракта (планирование — ad-hoc `.plans/` + карточки).
- Нет декларативных карточек ролей со схемой/валидатором; нет RACI.
- Нет hash-chain аудита; нет token budgeting per work unit; нет контролируемого словаря модулей.
- Нет рантайм-хуков уровня инструмента (deny до записи) — только git-гейты, обходимые `--no-verify`.
- CI выключен (14/15 workflows); нет `Makefile`; нет извлекаемого framework-шаблона; governance-доки на двух языках без единого стандарта.

---

## 9. Что можно позаимствовать

### Helicomponents ← из Hermes

1. **Artifact-verification перед close** — самое ценное. Сейчас `pre_close_guard` проверяет наличие ссылки на validation в handoff'е, но не факты. Добавить в close-протокол обязательную сверку каждого пункта SuccessCriteria с живой системой (запись в ClickHouse существует, скрипт запускается, SHA в origin) — прямой аналог HARD RULE Hermes, рождённого из инцидента с компрессией контекста.
2. **Избыточность знаний** («ни одно знание в единственной копии»): периодический бэкап `agent_kg.json` + архивов за пределы git-репозитория.
3. **Durable-возобновление работы**: лёгкий аналог working-поллера — cron/скрипт, который по открытым `W_*` в KG формирует «карточку пробуждения» для новой сессии оркестратора.
4. **AGENTS.md** как универсальный вход для не-Cursor агентов (Claude Code его читает нативно) — тонкий указатель на `.cursor/rules/` SSoT, без дублирования правил.
5. **Hermetic-тесты** и дисциплина «no change-detector tests» для валидаторов инвариантов.
6. **Risk-классификатор diff** как второй эшелон к хукам (git-уровень, ловит правки в обход Cursor).

### Hermes ← из Helicomponents

1. **Hash-chained audit** для kanban-комментариев/handoff'ов — tamper-evident история для парка, где агенты автономны и уже терялись изменения.
2. **Декларативные карточки ролей** (YAML + schema + валидатор в CI) вместо прозы в `fleet-ssot.md` — матрица прав §16 напрашивается на машинную проверку.
3. **Token budgeting per карточка** (soft/hard caps + аналитика) — у Hermes бюджета работы на единицу нет.
4. **Инварианты как SSoT с validation_sql** — формализовать проверки приёмки карточек, чтобы artifact-verification был проверкой по декларированному списку, а не по памяти координатора.
5. **Framework L1/L3 extraction** — выделить переносимый шаблон мета-обвязки из конкретного парка.

---

## 10. Вывод

Два harness'а решают разные главные задачи и потому почти не пересекаются в сильных сторонах:

- **Helicomponents** оптимизирован против **необратимых ошибок в одной глубокой сессии**: детерминированные deny-хуки, доменные инварианты, hash-chain аудит, формализованный 10-шаговый цикл с машинным стоп-условием. Это самый глубокий из двух «loop engineering»: цикл управляется, бюджетируется и закрывается по проверяемому критерию.
- **Hermes** оптимизирован против **потери управления парком автономных агентов во времени**: durable-шина работы, независимая верификация фактов, мониторинг живости, избыточность знаний, git-канон с risk-гейтами. Цикл проще, но не умирает вместе с сессией.

Идеальный harness — комбинация: глубина цикла и prevention-гейты Helicomponents + живучесть, независимая верификация и избыточность Hermes. Конкретные первые шаги для Helicomponents: artifact-verification в pre_close (п. 9.1), бэкап KG (9.2) и AGENTS.md-мост для Claude Code (9.4).

---

*Источники: полное обследование `/home/user/Helicomponents` (`.cursor/`, `config/agent_kg.json`, `tools/`, `framework/`, `docs/governance/`) и `/workspace/hermes` @ `2c43a7a` (`AGENTS.md`, `docs/deploy/canon-rules.md`, `fleet-ssot.md`, `fleet-bus-runbook.md`, `scripts/githooks/`, `scripts/fleet-*.py`).*
