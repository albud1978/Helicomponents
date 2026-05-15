# Third-party dependencies and licenses

**Цель документа.** Полный реестр сторонних компонентов проекта Helicomponents с указанием лицензий, SPDX-идентификаторов, обязательств и observed risk. Документ — companion для `LICENSE` (MIT — наш собственный код) и future-state `sbom.cdx.json` (CycloneDX 1.6 SBOM, см. `deploy/sbom/`).

**Source of truth.** Этот файл — manual-tracked human-readable inventory. Автоматический SBOM (`sbom.cdx.json`) считается primary machine-readable источником после его генерации (P0.6).

**Supplier-centric view.** Для оценки vendor-risk и continuity plan см. [`docs/governance/suppliers.md`](docs/governance/suppliers.md) (supplier matrix, criticality, kill-switch).

**Last reviewed.** 15-05-2026 (orchestrator, W_compliance_remediation_2026_05_15).

---

## Сводная таблица

| Компонент | Версия | License | SPDX | Тип | Risk |
|---|---|---|---|---|---|
| Helicomponents (наш код) | — | MIT | `MIT` | Permissive | — |
| pandas | 2.3.3 | BSD 3-Clause | `BSD-3-Clause` | Permissive | None |
| numpy | 2.2.6 | BSD 3-Clause | `BSD-3-Clause` | Permissive | None |
| openpyxl | 3.1.5 | MIT | `MIT` | Permissive | None |
| clickhouse-driver | 0.2.10 | MIT | `MIT` | Permissive | None |
| clickhouse-connect | 0.10.0 | Apache 2.0 | `Apache-2.0` | Permissive | None |
| PyYAML | 6.0.3 | MIT | `MIT` | Permissive | None |
| pathlib2 | 2.3.7 | MIT | `MIT` | Permissive | None |
| langgraph | unpinned | MIT | `MIT` | Permissive | None |
| neo4j (Python driver) | unpinned | Apache 2.0 | `Apache-2.0` | Permissive | None |
| cudf-cu12 (RAPIDS) | 25.12.0 | Apache 2.0 | `Apache-2.0` | Permissive | None |
| **Neo4j Community Server** | 5.x (Docker) | **GPL v3** | `GPL-3.0-only` | **Strong copyleft** | Low (network-use only) |
| **pyflamegpu** (FLAME GPU 2) | 2.0.0rc4+cuda130 | **AGPL v3 + Commercial dual** | `AGPL-3.0-only OR LicenseRef-FlameGPU-Commercial` | **Strong copyleft + § 13** | **Critical for future-state** |
| ClickHouse Server | external (YC managed) | Apache 2.0 | `Apache-2.0` | Permissive | None |
| Apache Superset | external (sandbox Docker) | Apache 2.0 | `Apache-2.0` | Permissive | None |
| CUDA Toolkit | 13.0 | NVIDIA Software License Agreement | `LicenseRef-NVIDIA-CUDA-EULA` | **Proprietary commercial** | Low |
| Cursor AI | SaaS subscription | Cursor Terms of Service | `LicenseRef-Cursor-Commercial` | **Proprietary SaaS** | Low |

---

## Не-MIT лицензии: суть и compliance

### 1. BSD 3-Clause (pandas, numpy)

**Суть.** Permissive, аналогична MIT по разрешениям. Отличие — третий пункт: запрет использовать имена авторов / контрибьюторов для endorsement продукта без письменного разрешения.

**Compliance.**
- Сохранять copyright notice + license text при дистрибуции
- Не использовать имена авторов в маркетинге без разрешения

**Impact.** Zero risk для проекта.

### 2. Apache 2.0 (clickhouse-connect, neo4j driver, cudf, ClickHouse Server, Superset)

**Суть.** Permissive, два важных отличия от MIT:
- **Patent grant** — автоматическая лицензия на патенты автора, использованные в коде. При судебном иске за патент против автора грант теряется.
- **NOTICE file** — обязательство сохранять `NOTICE` файл если он есть в апстриме.

**Compliance.**
- Сохранять copyright notice + license + NOTICE (если есть)
- При значимой модификации — пометить изменения
- При distribution — сохранять license headers в modified files

**Impact.** Zero risk для проекта. Использование в proprietary коде разрешено.

### 3. GPL v3 (Neo4j Community Server)

**Суть.** **Strong copyleft**:
- Distribution software, который **linked** (статически/динамически) с GPL'd кодом → ваш software **тоже становится GPL** (исходный код обязан быть открыт)
- **Network use ≠ distribution** — использование GPL software через сеть (REST/gRPC/bolt) **НЕ triggers** copyleft

**Наше использование.**
- Neo4j Community Edition разворачивается в **Docker container** как **отдельный процесс** (`deploy/neo4j-local/docker-compose.yml`)
- Общение через `bolt://localhost:7687` — network protocol
- Python driver `neo4j` (Apache 2.0) — отдельная библиотека, не linked с Neo4j Server source code
- **Internal-only**, не дистрибутируется

**Compliance.**
- Документировать: "used as separate service via bolt://, no source linking" ✅ (этот файл)
- Не embedding Neo4j Java libraries в собственный код
- Не distributing Neo4j binaries в собственных artifacts

**Impact.** Low risk при текущей архитектуре. Critical если когда-то будет принято решение об embedded Neo4j (мы не планируем).

### 4. AGPL v3 + Commercial dual (pyflamegpu / FLAME GPU 2)

**Суть.** Усиленный GPL с **сетевой клаузой § 13**:
- Все обязательства GPL v3 (strong copyleft при distribution)
- **§ 13 Remote Network Interaction**: если software offered to users over a network (web service, API, agentic service), эти remote users должны иметь возможность **получить соответствующий source code** AGPL'd версии

**Двойная лицензия от University of Sheffield.**
- **AGPL-3.0-only**: бесплатно, но triggered copyleft + § 13 при network exposure
- **Commercial license** ($$$ через Sheffield): без copyleft и § 13 обязательств

**Наше текущее состояние (15-05-2026).**

✅ **Internal dev tool без сетевого endpoint** → § 13 НЕ triggered:
- Симуляция запускается локально на dev-машине (`python3 code/sim_v2/messaging/orchestrator_limiter_v8.py`)
- Результаты пишутся в ClickHouse (database, не network service exposing FLAME GPU)
- Никакого REST/gRPC/MCP/A2A wrapper'а вокруг FLAME GPU 2 нет

✅ **AGPL разрешает** internal-only use без обязательств открыть source.

⚠️ **Future-state риск.** Если архитектура изменится:
- Если симуляция exposed через API (REST/gRPC/MCP) → § 13 triggered → выбор:
  - (a) Open source весь стек по AGPL включая proprietary integration code
  - (b) Купить commercial license у Sheffield
- Если sell to customer (включая ЮТэйр коммерческие unit'ы) → commercial license рекомендована

**Compliance действия.**
- ✅ Документировать (этот файл)
- ✅ В SBOM пометить как `AGPL-3.0-only`
- ⏳ При планировании production-сервиса — решение о commercial license

**Impact.** Critical для будущей архитектурной траектории. Для текущего use case — none.

### 5. NVIDIA Software License Agreement (CUDA 13.0 Toolkit)

**Суть.** Proprietary commercial EULA, НЕ open source.

**Разрешено.**
- Use for personal use, research, academic, development — **бесплатно**
- Commercial use **на NVIDIA hardware** — разрешено по standard EULA terms
- Распространение **runtime libraries** в собственных artifacts — по правилам redistribution clauses EULA

**Запрещено.**
- Reverse engineering CUDA libraries
- Distribute CUDA **toolkit** binaries в собственных artifacts (можно требовать установки пользователем)
- Использование на не-NVIDIA hardware (irrelevant)

**Наш use case.** ✅ Полностью compliant — internal dev, NVIDIA RTX hardware, не distributing toolkit.

### 6. Cursor AI Commercial Proprietary (SaaS)

**Суть.** Proprietary SaaS, использование по Terms of Service Cursor.

**Compliance.**
- Subscription terms (Pro / Business plan)
- Data handling согласно privacy policy Cursor — **код отправляется в LLM провайдеров** (Anthropic, OpenAI, Google) при invocations
- НЕ distribute Cursor product

**Impact.**
- Data residency: код проекта может временно передаваться в LLM API провайдеров
- Для проекта с PII/sensitive data — отдельный privacy audit рекомендован (P1+)
- Для текущего use case (helicopter MRO simulation, не PII) — low risk

---

## AGPL v3 § 13 расширенный анализ (FLAME GPU 2)

**§ 13 текст (упрощённо).** Если modified version of AGPL-licensed software offered to remote users for the purpose of running it via computer network, the provider must give those remote users an opportunity to receive the corresponding source code.

**Что "triggers" § 13:**
- Web service, exposing functionality of FLAME GPU 2 (e.g., REST API "submit simulation")
- Multi-tenant agentic service где remote users инициируют симуляцию
- MCP server / A2A endpoint обёрнутый вокруг FLAME GPU 2

**Что НЕ "triggers" § 13:**
- Internal CI/CD running simulation как build step
- Desktop tool используемый локально
- Backend job processor (e.g., cron, Airflow) если результаты не exposed через сеть direct from FLAME GPU output
- Database storing simulation results, доступная пользователям (database != network service exposing FLAME GPU code)

**Boundary cases (требуют юридической консультации).**
- BI dashboards (Superset) с готовыми данными симуляции — **скорее всего OK** потому что Superset экспонирует data, не FLAME GPU runtime
- Multi-agent orchestration где результаты передаются другим агентам — **скорее всего OK** internally; **возможно not OK** если сервис exposed remote users
- Внутренний tool для ЮТэйр инженеров через web UI — **boundary case**; зависит от того, считается ли intranet "network" в смысле AGPL

**Действия при изменении архитектуры.**
1. **Triggered scenario detected** → STOP, эскалация Алексею
2. Решение между AGPL compliance (open all source) vs commercial license
3. Если commercial — связаться с Sheffield University Software Licensing
4. Update этого документа и SBOM

---

## Update policy

**Когда обновлять этот файл.**
- Новая зависимость с не-MIT лицензией добавлена в `requirements.txt` / Dockerfile / Makefile
- Обновление существующей dep с изменением license
- Изменение архитектуры в сторону network-exposed service (AGPL § 13 trigger check)
- Регулярный review (suggest: quarterly)

**Как обновлять.**
1. Найти license через `pip show <package>` или PyPI/upstream
2. Добавить строку в "Сводная таблица"
3. Если не-MIT и risk > None — добавить дополнительный анализ в соответствующую секцию
4. Update `Last reviewed` в header
5. Регенерировать `sbom.cdx.json` (см. `deploy/sbom/README.md`)

**Связанные файлы.**
- `LICENSE` — MIT для нашего кода
- `sbom.cdx.json` — machine-readable SBOM (P0.6, в работе)
- `SECURITY.md` — vulnerability reporting policy (P0.4, в работе)
- `requirements.txt` / `constraints.txt` — Python dependencies
- `deploy/neo4j-local/docker-compose.yml` — Neo4j CE Docker setup (GPL v3 service-use, P0.7)
