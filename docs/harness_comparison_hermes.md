# Сравнительный анализ harness'ов: Helicomponents vs Hermes

Дата: 2026-07-13. Ревизия 2 от 2026-07-14 — по итогам внешней рецензии скорректированы уровни доказанности утверждений (см. Приложение А).

**Audit header (воспроизводимость):**

```text
Helicomponents: branch claude/harness-comparison-analysis-fasg7q,
                commit babbceb61df8f248eb180bf877e78b85cc135ab2, dirty=false
                (файлы harness'а идентичны базовой ветке; ветка добавляет только этот документ)
Hermes:         commit 2c43a7a, dirty=false
Live systems inspected: НЕТ — только статический анализ репозиториев.
```

**Легенда уровней доказанности** (используется в спорных местах):

| Маркер | Значение |
|---|---|
| `D` | описано в документации/правилах |
| `C` | присутствует код |
| `W` | подключено в конфигурации (hooks.json, workflows, cron-реестр) |
| `T` | покрыто автоматическим тестом |
| `O` | подтверждено наблюдением живой системы |

Из GitHub-снапшота достижим максимум уровень `W` (для CI — частично `T`). Ни одно утверждение этого документа не имеет уровня `O`: включённость systemd/cron, git `core.hooksPath`, branch protection и бэкапов на конкретных машинах по репозиторию проверить нельзя.

Объекты сравнения:

- **Helicomponents** (`albud1978/helicomponents`) — прогнозирование жизненного цикла компонентов вертолётов (FLAME GPU / CUDA, ClickHouse, Neo4j, Superset). Harness: governance-first обвязка под Cursor с Knowledge Graph bookkeeping и явным слоем loop engineering.
- **Hermes** (`albud1978/hermes`, HEAD `2c43a7a`) — open-source агентный фреймворк от Nous Research (Python-ядро, SQLite/FTS5, Ink/React TUI, 20+ LLM-провайдеров). Harness: распределённая мета-обвязка для парка автономных агентов на нескольких машинах.

Важная асимметрия: в Hermes два слоя — **продуктовый рантайм** (Hermes сам является агентным harness'ом: loop, делегирование, skills, память, kanban, cron) и **мета-обвязка разработки** («канон»-governance, fleet SSoT, kanban-шина, heartbeat'ы, git-гарды). Прямой аналог harness'а Helicomponents — именно мета-слой; продуктовый слой привлекается там, где он выполняет ту же функцию.

---

## 1. Профили в одну строку

| | Helicomponents | Hermes |
|---|---|---|
| Философия | Governance-first: детерминированные гейты вокруг необратимых side-эффектов (GPU-симуляции, DROP в ClickHouse, корпоративный BI) | Fleet-first: координация парка автономных агентов на нескольких машинах через durable-шину и git-«канон» |
| Единица работы | Workflow `W_*` в JSON Knowledge Graph | Карточка kanban (SQLite REST-доска) |
| Замыкание цикла | Машинно-обязательная **декларация** evidence: `pre_close_guard` требует структуры handoff'ов и текстового маркера SuccessCriteria, но не исполняет проверки (`C`+`W`) | Процедурная верификация координатором (WALL-E) по 5 артефактам (`D`) + header-гейт `done` в dashboard API (`C`+`W`) |
| Носитель правил | `.cursor/rules/*.mdc` (SSoT) + 11 карточек агентов | `AGENTS.md` (53 КБ) + `docs/deploy/canon-rules.md` + 2 тонких `.mdc` |
| Enforcement | 13 зарегистрированных хуков-guardrails (`W`; preToolUse, fail-safe deny) | git-хуки pre-commit/pre-push (opt-in через `core.hooksPath`) + risk-классификатор diff |
| Топология | 1 машина, 1 оркестратор, одноуровневая делегация | N машин, координатор + девопс-агенты + профили-исполнители |

---

## 2. Loop engineering

### Helicomponents: явный внутрисессионный цикл

Единственный из двух harness'ов с формализованным слоем, названным «loop engineering» (Tier-L):

- Канонический 10-шаговый цикл: Анализ → Pre-gate → Проверка → Реализация → Review/Validation → Governance → Docs → Capsule → Pre-close → Handoff/Close. Точная квалификация: это **декларативный протокол с частичным hook-enforcement** (`D`+частично `W`), а не исполняемая state-машина. Правило формулирует «логику как в LangGraph, без внедрения LangGraph», но transition engine'а, который хранит текущий шаг, машинно считает review-итерации и запрещает перепрыгивание этапов, в коде нет — hooks гейтят только отдельные переходы (dispatch и close), остальное держится на дисциплине оркестратора.
- **Auto-drive для low-risk**: оркестратор проходит цикл без ручных подтверждений между обратимыми шагами; стоп-условия — SuccessCriteria выполнен, token-подсветка, high-risk scope, явный input человека. Оговорка: token-подсветка 150k в самом правиле перечислена среди стоп-условий auto-drive и тут же названа «soft, не stop» — терминологическая несогласованность источника, воспроизведённая ревизией 1 без критики.
- **Стоп-условие закрытия** — машинно-обязательная **декларация** evidence, не проверка результата: для medium/high `pre_close_guard.py` блокирует close, если в Facts/SuccessCriteria orchestrator-handoff'а нет текстового маркера по регэкспу (`validation_sql`, `INV-N`, `acceptance`, `manual-check`, даже просто `success_criteria`). Хук проверяет «оркестратор написал, что evidence есть», но не «evidence существует, актуален и показывает PASS» — SQL/валидаторы не запускаются. Плюс структурные проверки уровня `C`+`W`: наличие обязательных handoff'ов по risk-tier, trace-поля, governance verdict (reject/needs_human_gate блокируют), graph_update, drift_check.
- **Policy drift между гейтами** (найдено внешней рецензией, подтверждено кодом): `pre_gate_guard` запрещает `manual-check` для medium/high на входе (dispatch), а `pre_close_guard` принимает `manual-check` как достаточный маркер для тех же medium/high на выходе (close). Критерий, запрещённый на входе, разрешён на выходе.
- **Token budgeting**: soft-подсветка 150k est_tokens (≈p90); hard-cap 500k проверяется **только в `pre_gate_guard` при dispatch subagent/task** — это блок дальнейшей делегации после исчерпания cap, а не глобальный hard stop всей работы. Режимы деградации: KG не читается → fail-open; legacy workflow без caps/usage → пропуск; caps повышаются через CLI (`--set-caps`).
- **Анти-зацикливание**: max 3 итерации `implement → review`, затем эскалация. Triage-очередь (`W_triage_queue`) для non-blocking follow-ups.
- Матрица 10-step × risk-tier (low / medium / high) + 4 профиля интенсивности governance.

### Hermes: межсессионный цикл через доску

Цикл работы вынесен из сессии агента на durable-инфраструктуру:

- Стейт-машина карточки: `ready → in_progress → blocked/done → archived`. Working-поллер будит агента под ready-карточку с его assignee; агент клеймит атомарно и исполняет автономно.
- **HARD RULE верификации** (уровень `D` — правило, не механизм): перед `done` каждый пункт приёмки сверяется с фактом (cron существует и включён, файл на диске, конфиг применён по grep живого файла, сервис active, SHA в origin/main). Неподтверждённый пункт → `blocked`, не `done`. Мотивировано задокументированным инцидентом: компрессия контекста стёрла промежуточный шаг, карточка закрылась «на слово». Исполнение этого правила машиной не enforced: машиночитаемого verification report нет, привязки к commit SHA нет, freshness-проверки нет.
- **Гейт координатора** — точная квалификация: dashboard API отклоняет перевод в `done`, если вызывающая сторона не передала HTTP-заголовок `X-Fleet-Caller` со значением из env `FLEET_DONE_GATEKEEPER` (default `wall-e`) — `plugin_api.py:644-650, 1005-1008` (`C`+`W`). Заголовок ставит сам клиент, он не подписан и не привязан к сервисной учётной записи; авторизация dashboard — один общий session token (модель прямо задокументирована как не multi-user). Любой клиент с токеном может представиться `wall-e`. Это защита от **случайного** неверного вызова честным агентом, а не строгая граница полномочий. Проверка живёт в HTTP-handler'е dashboard-плагина; покрытие остальных write-path'ов (CLI, прямой SQLite) по коду не подтверждено. Формула «без WALL-E никакая карточка не уходит в done» из ревизии 1 — преувеличение: enforcement — только header-гейт, остальное — организационная дисциплина (`D`).
- Продуктовый рантайм-цикл: синхронный tool-calling loop с бюджетом итераций и авто-компрессией контекста; `todo`-toolset, standing `/goal`, `/queue`, background-задачи.
- Анти-луп на шине: максимум 3 карточки в ready на агента, bot↔bot переписка закрыта, `failure_limit=2` → авто-блок диспетчера.

### Сопоставление

| Аспект | Helicomponents | Hermes |
|---|---|---|
| Где живёт цикл | Внутри сессии (декларативный протокол оркестратора + hook-гейты на dispatch/close) | Между сессиями (доска + поллеры + cron) |
| Стоп-условие | Машинно-обязательная декларация evidence (attestation), enforced хуком; сами проверки не исполняются | Процедурный ревью артефактов вторым агентом (`D`) + header-гейт `done` в API (`C`+`W`) |
| Границы ресурсов | Токены (soft 150k подсветка / hard 500k — блок делегации при dispatch), шаги (`max_steps=50`, та же точка) | Итерации LLM-цикла; лимиты карточек на агента |
| Анти-зацикливание | Лимит implement→review = 3 (`D`, машинно не считается) | failure_limit=2, лимит ready=3, backoff поллеров |
| Отложенная работа | Triage-очередь в KG | Карточки в ready + cron-задачи |
| Durability цикла | KG переживает сессию, но драйвер — живая сессия оркестратора | Durable относительно сессий и worker-машин; control plane (SQLite-доска на PowerHome) централизован, HA нет — см. §3 |

Ключевое различие: у Helicomponents цикл **глубже формализован** (10 декларированных шагов, из которых два — dispatch и close — гейтятся хуками), у Hermes — **живучее** (переживает смерть сессии, worker-машины и компрессию контекста, потому что состояние вынесено наружу). Оговорка к живучести: наружу — значит на PowerHome; отказ самого PowerHome (хост, диск, центральный SQLite, сеть до него) переживается только через восстановление из еженедельного бэкапа (recoverability), автоматического failover нет. «Переживает смерть машины» верно для worker-машин, но не для control plane.

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

Две оговорки к Hermes-стороне таблицы:

- **PowerHome — центральная точка отказа control plane.** Доска, REST `:8080` и dashboard живут на одном хосте. Еженедельный бэкап (`fleet-backup-weekly`, `W`) даёт восстанавливаемость, но не высокую доступность: репликации и failover нет, RPO/RTO не декларированы, тест восстановления SQLite не зафиксирован.
- **Операционный контур парка — в процессе внедрения, не однородно подтверждён.** Fleet SSoT на `2c43a7a` фиксирует 38 Hermes cron-задач по четырём машинам + «ожидание Kuzya»; часть задач помечена требующими исправления. Heartbeat/polling/backup/watchdog корректно описывать как спроектированные и зарегистрированные в SSoT (`D`+`C`+частично `W`), но не как наблюдаемо работающие на всём парке (`O` недостижим из репозитория).

---

## 4. Правила и SSoT

**Helicomponents** — антидрейф-максимализм: правила ровно в одном месте (`.cursor/rules/*.mdc`), `.cursorrules` — только указатель; строгая иерархия SSoT (правила → доменные JSON → инварианты → шаблон handoff → runbook → BI-as-code), производные (Neo4j, капсулы) явно объявлены не-истиной («При расхождении с JSON — верить JSON», «Запрещено верифицировать логику по Neo4j»). Изменение SSoT — только с явного human approval, enforced хуком (`ssot_approval_guard`, fail-safe deny при недоступности KG). Плюс `versions_manifest.json`: SHA-256 baseline 48 файлов harness'а с проверкой DRIFT.

**Hermes** — SSoT через git-топологию: модель «трёх зон» (канон в `main` только через PR; оверлей машинных веток — свободно; вне git `$HERMES_HOME` — свободно). Явные SSoT: `fleet-ssot.md` (топология парка, менять только по команде владельца), `main` как канон кода, реестры-как-SSoT (`commands.py`, `toolsets.py` — всё производное генерится из них), «canonical source is the filesystem». Правила разложены: тонкие `.mdc` (2 файла, alwaysApply) → полный свод `canon-rules.md` → история в `repo-governance.md`.

Различие в механизме защиты SSoT: Helicomponents защищает **рантайм-хуками в момент правки** (deny до записи), Hermes — **git-хуками в момент фиксации** (pre-commit/pre-push + risk-классификатор diff: 🔴 CRITICAL / 🟡 HIGH / 🟢 SAFE / ❓→HIGH). Подход Hermes дешевле и переносимее, подход Helicomponents ловит нарушение раньше и не обходится через `git commit --no-verify`.

Уточнение по силе git-governance Hermes — это operational-модель, а не строгая security boundary. Собственные canon-rules содержат существенные оговорки: локальные хуки работают только после ручного `git config core.hooksPath scripts/githooks` (opt-in на каждой машине); предусмотрены переменные санкционированного обхода (push в main, `HERMES_FORCE_PUSH_OK=1` для force-push); WALL-E разрешён прямой push в `main`; branch protection приватного личного репозитория может не enforce'иться GitHub без соответствующего тарифа. Корректная декомпозиция: policy (в main через PR) ≠ local guard (хук, если включён) ≠ central enforcement (ruleset, не подтверждён) ≠ authorised bypass (WALL-E/admin).

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
- 13 зарегистрированных вызовов хуков в `.cursor/hooks.json` на `babbceb6` (`W`): 1 postToolUseFailure + 8 preToolUse + 3 beforeSubmitPrompt + 1 afterFileEdit; 13 hook-скриптов + библиотека `kg_io.py`. В составе: pre_gate, pre_close, ssot_approval (fail-safe deny), orchestrator_write, clickhouse_drop_guard (зарегистрирован с matcher `Shell`; тотальный запрет DROP/TRUNCATE после инцидента 2026-07-03, обход только `DROP_APPROVED_BY_ALEXEY=1`), superset_docker_guard и др. Уровень «cannot be bypassed» не установлен: хуки исполняются рантаймом Cursor, действия вне Cursor (прямой shell, другой агент) ими не покрываются — второй эшелон на git-уровне отсутствует (см. §9.6).
- SSoT инвариантов: INV-1..13 + TEMP + GPU, каждый с `expr`, severity, validator-скриптом и `validation_sql`; validator-judge выносит verdict только по этим полям (SQL-first).
- CI `quality.yml`: JSON validity → py-compile → invariant drift check → валидация agent cards → hygiene → audit verify. Тестов симуляции в CI нет (нужен CUDA-хост).
- Гигиена: `hygiene_check.py` (5 категорий rot, авто-триггер раз в день), `version_check.py` (SHA-256 baseline), `security_smoke.py` (8 injection-кейсов, informational), `pii_scan.py`.

**Hermes:**
- Git-гейты: pre-commit («канон-гард» — блок правок вне оверлея в машинных ветках), pre-push (блок force-push и прямого push в main) + `canon-risk-assess.py` в трёх точках (pre-push, auto-sync workflow, auto-pull).
- Тестовый гейт: обязательный `run_tests.sh` (hermetic: сброс всех `*_API_KEY`, TZ=UTC, temp HERMES_HOME, subprocess-per-test изоляция); дисциплина «no change-detector tests» (инварианты вместо снапшотов).
- CI почти выключен: 14 из 15 workflows в `disabled/`; активен только auto-sync main→ветки (с risk-гейтом; при блоке — issue). Docker smoke-test action с регресс-гардами на конкретные инциденты.
- Skill-гейт HARDLINE: 8 формальных правил для SKILL.md, нарушение — reject PR.
- Runtime-верификация: artifact-check перед done + независимый ревью WALL-E + heartbeat/board-watch/watchdog-мониторинг.

Симметричное различие: Helicomponents силён в **предотвращении** (deny до действия, инварианты домена, hash-chain), Hermes — в **обнаружении и восстановлении** (мониторинг, процедурная верификация фактов, авто-синк, избыточность знаний). У Helicomponents многие гейты — soft warning (полагаются на реактивность оркестратора); у Hermes аналогично многое держится на дисциплине координатора, а done-гейт технически сводится к доверенному HTTP-заголовку (§2). Ни один из двух не замыкает цепочку exists → configured → invoked → tested → cannot be bypassed дальше третьего звена.

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
- Нет исполняемой acceptance-верификации: `pre_close_guard` проверяет декларацию evidence по регэкспу, а не факты в живой системе; SQL/валидаторы при close не запускаются; evidence не привязан к commit SHA/среде/времени и не аннулируется при последующих правках (нет freshness-check).
- Policy drift между enforcement-точками: `manual-check` запрещён pre_gate'ом для medium/high, но принимается pre_close'ом для тех же тиров.
- Нет durable-исполнения: цикл умирает вместе с сессией оркестратора; нет поллеров/cron, которые перезапустят работу.
- Нет независимой верификации вторым агентом перед close.
- Нет избыточности знаний (одна копия KG, восстановление не тестируется) и нет heartbeat-мониторинга.
- Нет параллелизма; нет CI-прогона тестов симуляции; секрет-менеджмент реактивный (инцидент с паролем в git-истории, filter-repo purge).
- Нет нативного формата Claude Code / AGENTS.md.

**Hermes:**
- Нет identity/authorization-модели агентов: done-гейт доверяет самодекларируемому заголовку, авторизация — один общий session token; подделка caller тривиальна для любого держателя токена.
- Done-гейт живёт только в HTTP-handler'е dashboard; доменный/DB-слой и альтернативные write-path'ы (CLI, прямой SQLite) не покрыты.
- Верификация WALL-E — процедура без машиночитаемого артефакта: нет verification report (checks, exit codes, commit SHA, verdict, подпись), независимость контекста проверяющего не гарантирована.
- Control plane централизован на PowerHome без HA; RPO/RTO не декларированы, восстановление из бэкапа не тестируется.
- Нет формализованного внутрисессионного цикла plan→execute→verify→record вне kanban-контракта (планирование — ad-hoc `.plans/` + карточки).
- Нет декларативных карточек ролей со схемой/валидатором; нет RACI. Нет hash-chain аудита; нет token budgeting per work unit.
- Нет рантайм-хуков уровня инструмента (deny до записи) — только git-гейты: opt-in (`core.hooksPath`), обходимые `--no-verify` и санкционированными env-переменными.
- CI почти выключен (14/15 workflows в disabled), включая control-plane компоненты; нет извлекаемого framework-шаблона.

---

## 9. Что можно позаимствовать

Принцип приоритизации (уточнён после рецензии): исходить из threat model каждой системы, а не из полноты feature list. Механический перенос всех «хороших механизмов» обоих проектов рискует породить deadlock между гейтами, split-brain, governance overhead и конфликт последовательной модели Helicomponents с параллельной моделью Hermes. Порядок ниже — приоритетный.

### Helicomponents ← из Hermes (и из рецензии)

1. **Исполняемый acceptance runner в close-протоколе** — самое ценное. Типизированные проверки (SQL, command, numeric, invariant, git, service) реально запускаются при close, результат сохраняется; регэксп-attestation остаётся лишь как быстрый прекондишн. Прямой аналог HARD RULE Hermes, рождённого из инцидента с компрессией контекста, — но доведённый до машинного исполнения, которого нет и у Hermes.
2. **Привязка evidence** к workflow ID, commit SHA, среде и времени + **freshness-check**: evidence аннулируется, если проверяемые файлы изменились после его получения.
3. **Устранение policy drift**: единый словарь verifiable-маркеров для pre_gate и pre_close (`manual-check` либо запрещён для medium/high в обеих точках, либо ни в одной).
4. **Избыточность знаний с тестом восстановления**: периодический бэкап `agent_kg.json` за пределы git + регулярная проверка, что из бэкапа реально восстанавливается консистентный KG.
5. **Durable-возобновление работы**: лёгкий аналог working-поллера — cron/скрипт, который по открытым `W_*` в KG формирует «карточку пробуждения» для новой сессии оркестратора.
6. **Risk-классификатор diff** как второй эшелон к хукам (git-уровень, ловит правки в обход Cursor) — закрывает «cannot be bypassed»-звено.
7. **AGENTS.md** как универсальный вход для не-Cursor агентов и **hermetic-тесты** — полезны, но менее критичны, чем 1–3.

### Hermes ← из Helicomponents (и из рецензии)

1. **Identity/authorization-модель агентов**: отдельные agent credentials вместо общего session token, scope вида `kanban:close` вместо самодекларируемого `X-Fleet-Caller`.
2. **Done-гейт в доменном слое**, чтобы он покрывал API, CLI и все прочие write-path'ы, а не только HTTP-handler dashboard.
3. **Машиночитаемый verification report** как единственный ключ к `done`: task_id, verified_by, source_commit, среда, список checks с exit codes, verdict, timestamp, подпись.
4. **Явные RPO/RTO для PowerHome + тест восстановления SQLite** (или HA-репликация) — устранение центральной точки отказа control plane.
5. **Обязательный CI для control-plane компонентов** (kanban, dispatcher, auth, fleet sync) — сейчас CI выключен именно там, где ошибка дороже всего.
6. **Hash-chained audit** и **token budgeting per карточка** — полезны, но вторичны относительно 1–4: подделываемый caller и SPOF контрол-плейна опаснее отсутствия криптографической цепочки.
7. **Декларативные карточки ролей** (YAML + schema + валидатор в CI) вместо прозы в `fleet-ssot.md`.

---

## 10. Вывод

Два harness'а решают разные главные задачи и потому почти не пересекаются в сильных сторонах:

- **Helicomponents** имеет более формализованный и частично машинно enforced внутрисессионный governance: ограничения делегации, обязательные handoff'ы, risk-tier, traceability, close-гейты. Однако его close-контур проверяет **наличие деклараций** evidence, а не фактическое выполнение acceptance-критериев; между входным и выходным гейтом есть policy drift.
- **Hermes** имеет более сильную межсессионную и межмашинную координацию: внешняя очередь задач, поллеры, heartbeat, автоматическое возобновление исполнителей. Однако control plane централизован на PowerHome без HA, а done-гейт основан на доверенном HTTP-заголовке и процедурной проверке WALL-E, а не на аутентифицированной независимой верификации.

Итого: Helicomponents сегодня сильнее в **policy enforcement и traceability**, Hermes — в **work persistence и fleet orchestration**. Ни один из двух пока не обеспечивает одновременно end-to-end machine-verifiable acceptance, строгую идентификацию агентов и подтверждённую отказоустойчивость control plane. Первые шаги: для Helicomponents — исполняемый acceptance runner + привязка/freshness evidence + устранение policy drift (9.1–9.3); для Hermes — identity-модель + done-гейт в доменном слое + verification report (9.1–9.3 своего списка).

---

## Приложение А. Проверка встречной рецензии (2026-07-14)

Документ получил внешнюю рецензию (архитектор агентных платформ; общая оценка ~6/10, главная претензия — смешение уровней доказанности «описано → закодировано → подключено → протестировано → наблюдаемо работает»). Каждое фактическое утверждение рецензии проверено по коду на зафиксированных в audit header ревизиях. Итоги:

| № | Утверждение рецензии | Вердикт по коду | Основание |
|---|---|---|---|
| 1 | `pre_close_guard` не запускает проверки, а ищет текстовые маркеры регэкспом; это attestation, не verification | **Подтверждено** | `pre_close_guard.py:24-27` (`SUCCESS_CRITERIA_EVIDENCE_RE`, матчит в т.ч. само слово `success_criteria`), `:384-395`; исполняемых проверок в хуке нет |
| 1а | Policy drift: `manual-check` запрещён на входе, разрешён на выходе | **Подтверждено** | `pre_gate_guard.py:212-213` (запрет для medium/high) vs `pre_close_guard.py:25` (принимается) |
| 2 | Caps токенов/шагов гейтят только dispatch subagent/task; fail-open при нечитаемом KG; legacy пропускаются; caps повышаемы через CLI | **Подтверждено** | `pre_gate_guard.py:237-239, 130-172` |
| 2а | Противоречие «150k в списке stop conditions, но soft, не stop» | **Подтверждено** (несогласованность в самом правиле) | `.cursor/rules/90_multiagent_workflow.mdc:57` |
| 3 | 10-шаговый цикл — протокол, а не исполняемая state machine | **Подтверждено** | Transition engine (хранение текущего шага, машинный счёт review-итераций, запрет перепрыгивания) в `.cursor/hooks/` отсутствует |
| 4 | В hooks.json 12 регистраций, `clickhouse_drop_guard` не подключён | **Опровергнуто на зафиксированном ref** | `.cursor/hooks.json` @ `babbceb6`: 13 регистраций (1+8+3+1), `clickhouse_drop_guard.py` подключён с matcher `Shell` (строки 30–32). Вероятная причина расхождения — другой commit у рецензента; мета-претензия об отсутствии SHA в ревизии 1 справедлива и закрыта audit header'ом |
| 5 | Done-гейт Hermes — самодекларируемый заголовок `X-Fleet-Caller`, один session token, без подписи и сервисной учётки | **Подтверждено** | `plugins/kanban/dashboard/plugin_api.py:644-650, 1005-1008`; `fleet-kanban-design.md:86-102` (модель не multi-user) |
| 6 | Независимость верификации WALL-E не доказана (нет отдельного контекста, verification report, привязки к SHA) | **Подтверждено как inference** | Machine-readable report и его схема в репозитории отсутствуют; HARD RULE — уровень `D` |
| 7 | «Полностью durable» — преувеличение; PowerHome — SPOF без HA | **Подтверждено** | Доска/REST/dashboard на одном хосте; из защит — только `fleet-backup-weekly`; репликация/failover не обнаружены |
| 8 | Операционный контур парка в процессе внедрения | **Подтверждено** | `fleet-ssot.md:306`: «Всего Hermes cron: 38 (7+7+17+7) + ожидание Kuzya»; статусные пометки в реестре |
| 9 | Воспроизводимость слабая: нет SHA Helicomponents; на более позднем commit Hermes risk-гейт удалён из auto-sync | **Частично подтверждено** | SHA добавлен в audit header (ревизия 2). На `2c43a7a` risk-гейт в `auto-sync-branches.yml:68-80` присутствует; состояние более поздних commit'ов из данного снапшота непроверяемо — принято как заявление рецензента |
| 10 | Git-governance Hermes — operational-модель с opt-in хуками и санкционированными обходами, не security boundary | **Подтверждено** | `canon-rules.md:93, 117-122, 222, 265` (`core.hooksPath` вручную; bypass-переменные; ruleset-bypass для main) |

Принятые следствия: переформулированы «машинное стоп-условие» → «машинно-обязательная декларация evidence», «явная state-машина» → «декларативный протокол с частичным hook-enforcement», «полностью durable» → «durable к сессиям/воркерам, control plane без HA», «без WALL-E карточка не уходит в done» → «header-гейт + процедурная дисциплина»; добавлены audit header, легенда уровней доказанности и threat-model-приоритизация рекомендаций. Отклонено: занижение числа хуков и невключённость `clickhouse_drop_guard` (п. 4).

---

*Источники: полное обследование `/home/user/Helicomponents` @ `babbceb6` (`.cursor/`, `config/agent_kg.json`, `tools/`, `framework/`, `docs/governance/`) и `/workspace/hermes` @ `2c43a7a` (`AGENTS.md`, `docs/deploy/canon-rules.md`, `fleet-ssot.md`, `fleet-bus-runbook.md`, `scripts/githooks/`, `scripts/fleet-*.py`, `plugins/kanban/`). Живые системы не инспектировались.*
