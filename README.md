# Helicopter Component Lifecycle

🚁 **Система прогнозирования жизненного цикла компонентов вертолётов с использованием Agent-Based моделирования на FLAME GPU**

> ⚠️ **Все команды выполняются из корня проекта (Helicomponents/)**

---

## 🎯 Почему имитационная модель?

**Причина:** Невозможность аналитически предсказать все переходы состояний компонентов без тяжёлых расчётов из-за влияния множества взаимозависимых факторов:

| Фактор | Влияние |
|--------|---------|
| **Исходное состояние ресурсов** | SNE, PPR, repair_days у каждого агента |
| **Темпы утилизации** | Индивидуальный вектор налёта (MP5) |
| **Программа полётов** | Изменение target по Mi-8/Mi-17 (MP4) |
| **Сроки ремонтов** | repair_time, очередь на ремонт |
| **Ресурсные лимиты** | LL, OH, BR — пороги списания и ремонта |

Эти факторы создают **нелинейные зависимости**, где состояние одного агента влияет на решения по другим (квотирование, очередь ремонтов). Аналитическое решение системы уравнений невозможно — требуется **пошаговая симуляция**.

---

## 📂 Структура проекта

| Папка | Назначение |
|-------|------------|
| `code/` | Активная разработка (ETL, симуляция, утилиты) |
| `code/sim_v2/` | Оркестратор симуляции FLAME GPU |
| `docs/` | Документация проекта |
| `data_input/source_data/` | Входные датасеты (`v_YYYY-MM-DD/`); перед первым extract на новой выгрузке см. `code/utils/prep_source_dataset.py` |
| `output/` | Выходные отчёты и результаты |
| `config/` | Конфигурационные файлы (transitions, quotas, invariants) |
| `logs/` | Логи работы системы |

---

## 🚀 Быстрый старт

```bash
# 1. Активировать окружение CUDA13 (обязательно)
source ~/miniconda3/etc/profile.d/conda.sh
conda activate cuda13
if [ "$CONDA_DEFAULT_ENV" != "cuda13" ]; then
  echo "❌ Требуется conda env cuda13"
  return 1 2>/dev/null || exit 1
fi

# 2. Загрузить переменные окружения
source config/load_env.sh
export CUBE_CONFIG_PATH="$PWD/config"

# 3. Проверить подключение к БД
python3 code/utils/test_db_connection.py

# 3b. (опционально) Новая папка v_YYYY-MM-DD: нормализовать Program_AC / lease_restricted
#     перед extract — см. docs/architecture/extract.md (секция «Утилита нормализации нового датасета»)
# python3 code/utils/prep_source_dataset.py --dataset data_input/source_data/v_YYYY-MM-DD

# 4. Запустить ETL (интерактивный выбор датасета и режима)
python3 code/extract/extract_master.py

# 5. Запустить симуляцию V8 (LIMITER)
python3 code/sim_v2/messaging/orchestrator_limiter_v8.py \
  --version-date YYYY-MM-DD --steps 3650 --enable-mp2 --drop-table
```

> **Эталонная команда V8:** V8 НЕ использует `--modules` (порядок слоёв зашит в `build_model()`).  
> **SSoT по порядку слоёв:** `config/transitions/transitions_rules.json` (секция `rtc_execution_order`).  
> **Полные команды с параметрами:** см. `.cursor/rules/` (секции "Загрузка данных" и "Команда запуска симуляции")

---

## 📈 BI (Superset) — быстрый доступ и порядок работы

### Доступы и адреса (sandbox)

| Что | Значение |
|-----|----------|
| Local URL | `http://127.0.0.1:8088` |
| LAN URL (корп. сеть) | `http://10.95.19.132:8088` |
| Логин/пароль sandbox | `admin / admin` |
| Docker контейнер Superset | `superset-local` |
| Docker контейнер Postgres | `superset-db-local` |
| Docker контейнер Redis | `superset-redis-local` |

> Использовать только для песочницы. Прод-контур в рамках этого репозитория не обслуживается.

### Канонические BI-объекты (текущая рабочая конфигурация)

| Тип | ID | Название |
|-----|----|----------|
| Dashboard | `1` | `Планеры 10Y Гистограмма v9` |
| Chart | `1` | `Датасет 1` |
| Chart | `2` | `Датасет 2` |
| Chart | `5` | `График Ремонта` |

Быстрые ссылки:
- Dashboard: `http://127.0.0.1:8088/superset/dashboard/1/`
- Explore chart 1: `http://127.0.0.1:8088/explore/?slice_id=1`
- Explore chart 2: `http://127.0.0.1:8088/explore/?slice_id=2`
- Explore chart 5: `http://127.0.0.1:8088/explore/?slice_id=5`

### Порядок проверки (чтобы не искать каждый раз)

1. Проверить доступность:
   - `curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8088/health`
2. Войти в UI (`admin/admin`) и открыть dashboard `1`.
3. Убедиться, что видны 3 чарта: `Датасет 1`, `Датасет 2`, `График Ремонта`.
4. Проверить фильтры:
   - отдельный фильтр версии для `График Ремонта` (изолированный scope);
   - timegrain-фильтры для гистограмм.

### Brandbook baseline для BI (зафиксировано)

- Типографика UI: `Suisse Int'l Regular`, fallback `Arial` / `Helvetica Neue`.
- Дата в UI/tooltip: `DD-MM-YYYY`; оси времени на гистограммах: `YYYY`.
- Цвета статусов:
  - `Списание` `#003594`
  - `Операции` `#7CA3DC`
  - `Исправные` `#008C5F`
  - `Ремонт` `#FFC332`
  - `Хранение` `#737576`
  - `Неисправные` `#DC2328`
- Цвета типов ВС на Gantt:
  - `Ми-8` `#003594`
  - `Ми-17` `#7CA3DC`

### Текущее состояние Gantt (24-02-2026)

- Легенда с селекторами включена: `Ми-8`, `Ми-17`, `Слоты`.
- `Слоты` отображаются как пустой прямоугольник с рамкой (без заливки).
- Цвета Gantt (борта) приведены к брендбуку:
  - `Ми-8` `#003594`
  - `Ми-17` `#7CA3DC`
- UI-стиль Gantt выровнен под более "чистый" ECharts-подход: мягкий фон, аккуратные оси/зум-слайдеры, читаемая легенда.
- Применение сделано runtime-патчем ассетов в контейнере `superset-local` (песочница).
  - Важно: это не Git-артефакт Superset frontend внутри репозитория; после пересборки/замены контейнера патч нужно воспроизвести.

### Быстрые recovery-действия

- Перезапуск Superset:
  - `docker restart superset-local`
- Если UI не видит свежие изменения:
  - обычное обновление: `F5`
  - жёсткое обновление: `Ctrl+Shift+R`
- Если есть кэш-конфликт ассетов после runtime-патча:
  - перезапустить `superset-local`, затем повторить `Ctrl+Shift+R`.

### Полная миграция через Git (регулярно между машинами)

- Подробный runbook: `deploy/bi-as-code/README.md` (раздел `Git migration mode`).
- Ключевой инструмент синхронизации dashboard bundle:
  - `python deploy/bi-as-code/scripts/superset_git_sync.py export ...`
  - `python deploy/bi-as-code/scripts/superset_git_sync.py import --overwrite ...`
- Зафиксированный режим переноса: только `Mode B (repo-only)`.
- Для `Mode B` использовать запуск с плагином:
  - `bash deploy/superset-local/start_local_plugin.sh`
  - этот скрипт при необходимости соберёт локальный image из `superset-frontend/plugins/plugin-chart-echarts6-gantt/`.
- Для нового агента обязателен onboarding перед действиями:
  - прочитать `deploy/bi-as-code/README.md`;
  - прочитать `.cursor/rules/00_global_always.mdc` и `.cursor/rules/90_multiagent_workflow.mdc`;
  - проверить `.cursor/hooks/code_edit_audit.log` (в Git) и локальный `.cursor/hooks/user_comm_audit.log` (на текущей машине).
- Для WSL2 поддержан тот же контур (см. WSL notes в `deploy/bi-as-code/README.md`).

## 📚 Документация

### Правила и методология
| Файл | Описание |
|------|----------|
| **`.cursor/rules/`** | Модульные правила разработки для Cursor AI |
| **`.cursor/agents/`** | Субагенты проекта (coder-flame, coder-general, reviewer-flame, validator-judge, capsule-builder, analyst-sql-graph, governance-compliance, docs-curator) |
| `docs/validation.md` | Операционный runbook: порядок прогона V8 + потоковые валидации без автозапуска |
| `docs/backlog.md` | Короткие идеи на будущее (формат и правила внутри файла) |
| `docs/migration.md` | Промт для новых разработчиков |
| `docs/limiter_v8_capsule.md` | Контекстная капсула LIMITER V8 (handoff) |

Методология отладки логики: см. `.cursor/rules/00_global_always.mdc`.

### Архитектура
| Файл | Описание |
|------|----------|
| `docs/architecture/rtc_pipeline_architecture.md` | **Baseline** — архитектура RTC пайплайна (intent-based, orchestrator_v2) |
| `docs/architecture/limiter_architecture.md` | **LIMITER V8** — RepairLine + repair_days в unsvc/inactive (readiness/спавн учитывают RepairLine) |
| `docs/architecture/validation_rules.md` | Методология SQL-first валидации и процедуры тестирования |
| `docs/spawn_dynamic_architecture.md` | Архитектура динамического спавна |

### Контракты и инварианты (SSoT)
| Файл | Описание |
|------|----------|
| **`config/transitions/invariants.json`** | Формализованные инварианты (INV-1..9), temporal-контракты (TEMP-1..4), GPU-ограничения (GPU-1..6) |
| `config/transitions/transitions_rules.json` | Матрица переходов state→state, condition precedent/subsequent, порядок RTC (51 слой) |
| `config/transitions/quota_rules.json` | Логика квотирования, RepairLine, spawn |

### Контекстные капсулы (RLM)

Капсулы — сжатые проекции доменного знания (~100-200 строк), read-only производные от JSON SSoT. Агенты читают манифест → выбирают нужную капсулу → работают в узком фокусе вместо загрузки всего проекта.

| Файл | Домен |
|------|-------|
| **`config/capsules_manifest.json`** | **Индекс всех капсул** (точка входа для агентов) |
| `docs/limiter_v8_capsule.md` | Оркестратор V8, адаптивные шаги, RTC слои |
| `docs/transitions_capsule.md` | Матрица переходов, condition precedent/subsequent |
| `docs/quota_capsule.md` | Квотирование P1/P2/P3/P4, RepairLine, spawn |
| `docs/validation_capsule.md` | Валидация, скрипты, маппинг инвариант→валидатор |
| `docs/flame_gpu_capsule.md` | Ограничения FLAME GPU, RTC-паттерны, типы данных |
| `docs/etl_extract_capsule.md` | ETL пайплайн, 18 стадий, ClickHouse таблицы |

**Иерархия источников:** JSON (SSoT) → Neo4j (визуализация) → Капсулы (контекст для агентов). Капсулы не являются SSoT; при расхождении — верить JSON.

### 🚀 LIMITER V8 — основная архитектура (ветка feature/flame-messaging)

> **⚠️ В этой ветке основной код — LIMITER V8 (`orchestrator_limiter_v8.py`)**
> V8 = RepairLine + adaptive steps с deterministic_dates; `min_dynamic` сбрасывается в `rtc_compute_global_min_v8` (без отдельного reset‑слоя), источник шага (limiter/repair_days) пишется в `adaptive_result_mp[1]`, а шаги по детерминированным датам помечаются как `deterministic_date:<day>`; P2/P3 ранжируются по idx и своему типу с раздельными квотами, выбор линии — минимальный `free_days >= repair_time` по MacroProperty (`repair_line_free_days_mp`/`repair_line_acn_mp`); spawn считает дефицит как `target − curr_ops − used(P1/P2/P3 commit)`, storage не участвует. Квоты распределяются через MessageBucket (`QuotaBucket`) по rank. Для диагностики RepairLine/квот используются `sim_repair_lines_v8` и `sim_quota_mgr_v8` (QM ops/target/quota_left).
> Дополнительно: тикеты spawn читают параметры по текущему дню (один день/один шаг), Mi‑8 использует `mi8_ll/oh/br` из `md_components` (через env).

| Файл | Статус | Описание |
|------|--------|----------|
| **`code/sim_v2/messaging/orchestrator_limiter_v8.py`** | ✅ **ОСНОВНОЙ** | RepairLine + adaptive steps (deterministic_dates) |
| `code/sim_v2/messaging/orchestrator_limiter_v5.py` | ⚡ Резервный | Двухфазная (intent-based), 100% GPU-only |
| `code/sim_v2/messaging/orchestrator_limiter_v3.py` | 📦 Архивный | `while step()` + HF |
| `code/sim_v2/messaging/rtc_quota_v8_base.py` | ✅ Актуальный | Локальные квоты V8 (без зависимости от V7) |
| `code/sim_v2/messaging/rtc_state_transitions_v7.py` | ✅ Актуальный | Однофазные переходы состояний |
| `code/sim_v2/messaging/rtc_limiter_optimized.py` | ✅ Актуальный | Бинарный поиск limiter через mp5_cumsum |
| `code/sim_v2/messaging/rtc_limiter_v5.py` | ✅ Актуальный | GPU-only модули (current_day, adaptive) |
| **`docs/architecture/limiter_architecture.md`** | 📄 Документ | **Архитектура LIMITER V8 с таблицей слоёв** |

**Архивные** (не использовать):
| Файл | Описание |
|------|----------|
| `code/sim_v2/messaging/orchestrator_limiter_v7.py` | Legacy V7 (однофазная архитектура) |
| `orchestrator_limiter.py` | Старая версия без оптимизации limiter |
| `orchestrator_limiter_v2.py` | Промежуточная версия (ежедневные шаги) |
| `orchestrator_limiter_v4.py` | Промежуточная GPU-only версия |
| `orchestrator_adaptive*.py` | Эксперименты с adaptive step |
| `orchestrator_gpu_only.py` | GPU-only эксперимент |

**Результаты LIMITER V7:**
- **219 адаптивных шагов** вместо 3650 ежедневных
- **1.59с** на 10 лет симуляции (**2296 дней/сек**)
- **100% GPU** — единый вызов `simulate()` без Python-цикла
- **Однофазная архитектура** — 26 RTC функций (было 45 в V5)
- **3 счётчика в 1 проход** — SNE, PPR, limiter в одной RTC
- **3 счётчика в 1 проход** — sne++, ppr++, limiter-- в одной RTC
- **Точный limiter** — бинарный поиск по mp5_cumsum (без аппроксимации)
- dt = программа ✅, Δsne = Σdt ✅

**Архитектура V7 vs V5:**
| Метрика | V7 | V5 |
|---------|-----|-----|
| Шаги | 219 | 332 |
| Время | **1.59с** | 3.71с |
| Ускорение | **2.3x** | — |
| GPU % | 100% | 100% |
| RTC функций | 26 | 45 |
| Архитектура | Single-phase | Intent-based |

### ETL процессы
| Файл | Описание |
|------|----------|
| `code/utils/prep_source_dataset.py` | Нормализация новой выгрузки (`v_YYYY-MM-DD`): заголовок `directorate`, колонка `lease_restricted` по `owner` — см. `docs/architecture/extract.md` |
| `docs/extract.md` | Извлечение данных (включает проверку комплектности) |
| `docs/architecture/validation_rules.md` | Правила валидации данных |
| `docs/architecture/transform.md` | Трансформация данных |
| `docs/architecture/load.md` | Загрузка результатов |

### История
| Файл | Описание |
|------|----------|
| `docs/changelog.md` | История изменений проекта |
| `docs/Tasktracker.md` | Текущие и активные задачи |

---

## 🛠 Технологии

- **Python 3.12** — основной язык
- **ClickHouse** — хранилище данных
- **FLAME GPU 2.0.0rc4** — Agent-Based симуляция на GPU
- **CUDA 13.0** — GPU вычисления

### Версии библиотек (актуально на 03-01-2026)

| Библиотека | Версия |
|------------|--------|
| pandas | 2.3.3 |
| numpy | 2.2.6 |
| cudf-cu12 | 25.12.0 |
| pyflamegpu | 2.0.0rc4+cuda130 |
| clickhouse-driver | 0.2.10 |
| clickhouse-connect | 0.10.0 |

### Поддерживаемые платформы

| Платформа | GPU | CUDA | Статус |
|-----------|-----|------|--------|
| Linux native | RTX 30xx/40xx/50xx | 13.0 | ✅ Основная |
| Windows + WSL2 | RTX 30xx/40xx/50xx | 13.0 | ✅ Поддерживается |
| Windows native | — | — | ❌ Не поддерживается |

---

## ⚙️ Настройка окружения

### Первый запуск
```bash
# 1. Клонировать проект
git clone https://github.com/albud1978/Helicomponents.git
cd Helicomponents

# 2. Активировать conda окружение CUDA13
source ~/miniconda3/etc/profile.d/conda.sh
conda activate cuda13

# 3. Установить зависимости
pip install -r requirements.txt -c constraints.txt

# 4. Настроить .env
cp .env.example .env
# Заполнить в .env пароль(и) и при необходимости локальные overrides

# 5. Первый запуск ETL
python3 code/extract/extract_master.py  # → выбрать 1 (ТЕСТ)
```

> Для воспроизводимости используйте `constraints.txt`: он фиксирует проверенные версии ключевых библиотек,
> при этом `requirements.txt` остаётся “верхнеуровневым” списком зависимостей.

### Переменные окружения (.env)

> `.env` содержит секреты и не должен коммититься в Git.

#### Профиль доступа к внешнему ClickHouse (YC, слой analytics/AMOS)
```bash
CLICKHOUSE_HOST=rc1a-fhb99q2hquq89uhp.mdb.yandexcloud.net
CLICKHOUSE_PORT=8443
CLICKHOUSE_DATABASE=default
CLICKHOUSE_USER=budnik_an
CLICKHOUSE_PASSWORD=<ваш_пароль>

# SSL для HTTPS подключения (clickhouse-connect)
CLICKHOUSE_SECURE=true
CLICKHOUSE_VERIFY=true
CLICKHOUSE_CA_CERT=/absolute/path/to/RootCA.pem

# Скачивание сертификата YC
# curl -fsSL "https://storage.yandexcloud.net/cloud-certs/RootCA.pem" -o "/absolute/path/to/RootCA.pem"
```

#### Альтернативный namespace для внешнего DWH
```bash
# Используйте, если в .env уже заняты CLICKHOUSE_* под локальный/рабочий контур.
DWH_CLICKHOUSE_HOST=rc1a-fhb99q2hquq89uhp.mdb.yandexcloud.net
DWH_CLICKHOUSE_PORT=8443
DWH_CLICKHOUSE_DATABASE=default
DWH_CLICKHOUSE_USER=budnik_an
DWH_CLICKHOUSE_PASSWORD=<ваш_пароль>

DWH_CLICKHOUSE_SECURE=true
DWH_CLICKHOUSE_VERIFY=true
DWH_CLICKHOUSE_CA_CERT=/absolute/path/to/RootCA.pem
```

### Форматы доступа к данным

Ниже зафиксированы практические форматы доступа, которые используются в проекте для ClickHouse и AMOS-данных.

#### Форматы подключения

| Контур | Как подключаться | Где задаётся |
|---|---|---|
| Локальный / рабочий ClickHouse проекта | `config/database_config.yaml` + ENV `CLICKHOUSE_*` | `config/database_config.yaml`, `.env` |
| Внешний YC DWH (AMOS) | HTTPS / SSL через `clickhouse-connect` | `.env` (`CLICKHOUSE_*` или `DWH_CLICKHOUSE_*`) |
| Внутренние ETL/validation скрипты | Обычно читают `CLICKHOUSE_*` | `config/load_env.sh`, `config/database_config.yaml` |
| Ad-hoc анализ внешнего DWH | Предпочтительно через `DWH_CLICKHOUSE_*` | `.env`, локальные Python/SQL скрипты |

#### Форматы доступа по слоям DWH

| Слой | Как читать | Что означает |
|---|---|---|
| `source` | `source.amos_heli_*` | Сырые исторические AMOS-таблицы за всё время |
| `staging` | `staging.amos_heli_*` | Промежуточный слой загрузки/подготовки |
| `analytics` | `analytics.amos_heli_*_view` | Представления поверх `source` |
| `integrated` | `integrated.amos_heli_*` | Материализованные промежуточные таблицы DAG |
| `reports` | `reports.amos_heli_*` | Финальные витрины / отчётные датасеты |

#### Форматы идентификации объектов AMOS

| Объект | Канонические ключи | Комментарий |
|---|---|---|
| Планер / ВС в AMOS | `ac_registr` | Базовый ключ борта в `amos_heli_*`, например `06012` |
| Полный регистрационный код | `ac_registr_prefix + ac_registr` | Например `RA-06012` |
| Проектный числовой ключ ВС | `aircraft_number` | Используется в extract/simulation, например `6012`; не равен автоматически AMOS internal aircraft key |
| Агрегат / компонент | `psn` | Базовый идентификатор rotable в AMOS |
| Позиция номенклатуры | `partno` | Партномер агрегата |
| Серийный номер | `serialno` | Серийник агрегата |
| Внутренний ID партномера | `partseqno_i` | Внутренний идентификатор номенклатуры в AMOS |

#### Форматы доступа по датам

| Контур | Поле даты | Как использовать |
|---|---|---|
| Сырые таблицы `source` | `processing_date_at` | Для исторических snapshot/as-of срезов |
| Финальные витрины `reports` | `report_date` | Для датирования готового датасета |
| История счетчиков | `readout_date` | Для даты фактического считывания, а не только загрузки в DWH |

#### Форматы сравнения и привязки

| Сценарий | Рекомендуемый ключ |
|---|---|
| Сравнение `Status_Components` между датасетами | `psn + partno` |
| Идентификация одного агрегата | `psn`, при проверке полезно держать рядом `partno` и `serialno` |
| Поиск борта в AMOS | `ac_registr` или полный `RA-XXXXX` |
| Привязка к extract/симуляции | `aircraft_number` / `registration_code` |

#### Важные оговорки по полям

Для финального датасета оборотных агрегатов (`reports.amos_heli_rotables_components_status`) часть полей является `derived/enriched`, а не прямой копией сырого AMOS. В частности это относится к `LL`, `OH`, `OH_threshold`, а также к части полей `ppr`, `sne`, `location`, `condition`, `removal_date`, `target_date`.

Поэтому:

- сравнение этих полей между raw/source и reports нужно трактовать как сравнение логики витрины, а не побайтового равенства;
- при расследовании историй агрегатов идентичность объекта лучше опирать на `psn + partno` и только затем смотреть enrichment-поля.

#### Справочник схемы AMOS

Полное описание полей AMOS, которое используется как вспомогательный справочник по таблицам/полям:

- `data_input/master_data/Database-Description39331920032025_0.csv`

#### Базовые переменные проекта
```bash
WORK_MODE=dev
LOG_LEVEL=INFO

# Domain Graph (Neo4j Aura) — см. секцию "Графы"
DOMAIN_NEO4J_URI=neo4j+s://...
DOMAIN_NEO4J_USER=neo4j
DOMAIN_NEO4J_PASSWORD=<ваш_пароль>
```

### Графы

Проект использует два графа с разным хранилищем:

| Граф | Хранилище | Назначение |
|------|-----------|------------|
| **Agent KG** | `config/agent_kg.json` | JSON-шина координации агентов (workflow, handoff, context) |
| **Domain Graph** | Cloud Neo4j Aura | Визуализация доменной модели (производная от JSON SSoT) |

**Agent KG — JSON-шина агентов:**
```bash
# Инициализация workflow
python3 code/utils/agent_kg.py --init-workflow --workflow-id "task-123" --goal "..."

# Запись hand-off
python3 code/utils/agent_kg.py --write-handoff --workflow-id "task-123" --agent "coder-flame" --user-goal "..." --trace-id "wf:task-123:2026-02-14T10:00:00Z" --plan-step-id "P1" --changes "..." --facts "..." --assumptions "..."

# Чтение состояния
python3 code/utils/agent_kg.py --read-state --workflow-id "task-123"

# Закрытие workflow
python3 code/utils/agent_kg.py --close-workflow --workflow-id "task-123" --close-reason "done" --agent "orchestrator"

# Визуализация
make agent-kg-viewer   # -> tools/agent_kg_viewer/index.html
```

**Domain Graph — синхронизация в Aura:**
```bash
make sync-domain-graph        # MERGE из JSON
make sync-domain-graph-clear  # Очистить и перезаписать
make sync-domain-graph-dry    # Показать Cypher без записи

# Проверка подключения
python3 code/utils/test_neo4j_connections.py
```

**SSoT для домена** — JSON в репозитории (`config/transitions/*.json`, включая `invariants.json`) и код (`code/sim_v2/**`). Domain Graph в Aura — производная для визуализации.

### RTC кэширование (ускоряет повторные запуски симуляции)
```bash
mkdir -p .rtc_cache
export FLAMEGPU_RTC_EXPORT_CACHE_PATH="$(pwd)/.rtc_cache"
```

---

## 📊 Штатные процедуры

| Процедура | Команда | Результат |
|-----------|---------|-----------|
| Проверка комплектности | `python3 code/heli_pandas_ops_other_groups.py --version-date YYYY-MM-DD --version-id 1` | `output/` |
| Анализ лизинга | `python3 code/heli_pandas_lease_restricted.py` | `output/` |
| Диагностика БД | `python3 code/utils/test_db_connection.py` | — |
| Очистка словарей | `python3 code/utils/cleanup_dictionaries.py` | — |
| Полная очистка | `python3 code/utils/database_cleanup.py` | — |

> **Подробнее:** `.cursor/rules/` (секция "Штатные процедуры проверки данных")

---

## 🔧 Документированные хардкоды

> ⚠️ **Правило проекта**: Любой хардкод должен быть документирован. Недокументированный хардкод запрещён.

### Архитектурные константы (фиксированные)

| Константа | Значение | Файл | Назначение |
|-----------|----------|------|------------|
| `RTC_MAX_FRAMES` | 400 | `code/model_build.py` | Размер буферов MacroProperty для планеров (покрывает рост флота) |
| `MAX_DAYS` | 4000 | `code/model_build.py` | Максимум дней симуляции (~10.9 лет) |
| `MAX_SIZE` | 1,600,400 | `code/model_build.py` | MAX_FRAMES × (MAX_DAYS + 1) |

### Справочные константы (из md_components)

| Константа | Значение | Файл | Назначение |
|-----------|----------|------|------------|
| `SPAWN_PARTSEQNO_MI8` | 70387 | `code/sim_env_setup.py` | partseqno для планера Mi-8Т (group_by=1) |
| `SPAWN_PARTSEQNO_MI17` | 70386 | `code/sim_env_setup.py` | partseqno для планера Mi-8АМТ (group_by=2) |
| `mi8_*_const` | из md_components | `code/sim_env_setup.py` | ll, oh, br, repair_time, partout_time, assembly_time |
| `mi17_*_const` | из md_components | `code/sim_env_setup.py` | ll, oh, br, br2_mi17, repair_time, partout_time, assembly_time |

### Диапазоны нумерации

| Диапазон | Назначение | Файл |
|----------|------------|------|
| `1–99999` | Реальные борта из MP3 | — |
| `100000–999999` | Зарезервировано для spawn (новорожденные) | `code/sim_env_setup.py` |
| `1000000+` | Зарезервировано (будущее расширение) | — |

### Битовые маски типов ВС

| Тип ВС | Маска | Двоичное | Файл |
|--------|-------|----------|------|
| МИ-8Т, МИ-8МТВ, МИ-8П | 32 | `0b00100000` | `code/enrich_heli_pandas.py` |
| МИ-8АМТ, МИ-17 | 64 | `0b01000000` | `code/enrich_heli_pandas.py` |
| МИ-26 | 128 | `0b10000000` | `code/enrich_heli_pandas.py` |

### Статусы агентов

**Рабочие статусы (status_id 1-6)** — назначаются только при `error_flags = 0`:

| status_id | Название | FLAME GPU State | Описание |
|-----------|----------|-----------------|----------|
| 1 | Inactive | `inactive` | Неактивный (каннибализация, ожидание комплектации) |
| 2 | Operations | `operations` | В эксплуатации (летает) |
| 3 | Serviceable | `serviceable` | Исправен, на складе |
| 4 | Repair | `repair` | В ремонте |
| 5 | Reserve | `reserve` | Резерв (готов к вводу) |
| 6 | Storage | `storage` | Хранение (списан из активной эксплуатации) |

**Статусы ошибок (status_id 10-15)** — битовая маска `error_flags`:

| status_id | Флаг | Бит | Условие | Описание |
|-----------|------|-----|---------|----------|
| 10 | 1 | 0 | `ll = 0 OR NULL` | Недостаточные данные |
| 11 | 2 | 1 | `target_date < version_date` | Дата ремонта в прошлом |
| 12 | 4 | 2 | `condition ≠ ИСПРАВНЫЙ AND sne = 0` | Неисправен при нулевой наработке |
| 13 | 8 | 3 | `sne > ll OR ppr > oh` | Превышение ресурса |
| 14 | 16 | 4 | `condition NOT IN (...)` | Некорректное condition |
| 15 | 32 | 5 | `ДОНОР AND sne < br` | Донор при ремонтопригодном (warning) |

> **Подробнее:** `docs/architecture/validation_rules.md`

### Бизнес-логика (параметризуемые)

| Параметр | Значение | Источник | Назначение |
|----------|----------|----------|------------|
| `br2_mi17` | 3500 ч (210000 мин) | `MD_Сomponents.xlsx` | Порог ppr для подъёма Mi-17 из inactive: < порога → без ремонта, ≥ порога → ремонт |
| `br_mi8/br_mi17` | расчёт | `calculate_beyond_repair.py` | Beyond Repair — экономический порог списания |
| `oh_mi8/oh_mi17` | из Excel | `MD_Сomponents.xlsx` | Межремонтный ресурс (часы → минуты при загрузке) |
| `ll_mi8/ll_mi17` | из Excel | `MD_Сomponents.xlsx` | Назначенный ресурс (часы → минуты при загрузке) |

### История аудита хардкодов

| Дата | Действие | Статус |
|------|----------|--------|
| 02-10-2025 | Централизация partseqno (7 мест → 1) | ✅ Исправлено |
| 02-10-2025 | Константы времени из md_components | ✅ Исправлено |
| 02-10-2025 | Архивирование старых spawn модулей | ✅ Выполнено |
| 04-01-2026 | Добавлен br2_mi17 для логики inactive | ✅ Реализовано |

> **Подробнее:** `docs/archive/analysis/hardcode_audit_02-10-2025.md`

---

## 📄 Лицензия

MIT License — см. файл `LICENSE`.
