# Пакет задач для DE: оптимизация аналитических таблиц DWH

Дата: 2026-06-13
Инициатор: проект Helicomponents (симуляция на FLAME GPU)

## Контекст

Сейчас данные идут по цепочке: AMOS → DWH (staging → integrated → reports) → Excel → Project ClickHouse → 9-этапный каскад обогащения → GPU. Хотим убрать Excel и загружаться напрямую из DWH analytics на любую дату.

Подготовлен и проверен прототип прямой загрузки (ветка `feature/dwh-bb8`, коммиты `8ffff135`…`e4974b84`):

| Компонент | Назначение |
|---|---|
| `code/utils/dwh_loader.py` | Загрузка 3 AMOS-источников из DWH в Project CH |
| `code/utils/dwh_post_enrichment.py` | Post-load enrichment: planner cascade + `status_id` |
| `code/utils/dwh_golden_replay_export.py` | Shared SQL / Excel replay / golden compare |
| `code/utils/dwh_direct_load.py` | CLI-алиас для replay |

**Порядок загрузки:** `program_ac` → `status_overhaul` → `status_components` → `enrich` (шаг `--no-enrich` для отключения).

**Приёмка MVP (2026-06-12 v1):** extract load + post-enrichment; `heli_pandas` без `status_id=0`. **Полная приёмка — только после sim + INV-1…INV-12 PASS** (см. `docs/changelog.md` 2026-06-13).

**Interim-источники:** `Status_Components` — `reports.amos_heli_rotables_components_status`; `Program_AC` / `Status_Overhaul` — analytics/source views (as-of). Целевой слой — `analytics.sim_input_*` (задачи ниже).

**Важно (контракт модели, 2026-06-13):** golden **`Status_Components.xlsx`** / **`Program_AC.xlsx`** **не имеют** колонки `status`. **`status_id`** в `heli_pandas` — наше поле (каскад обогащения). AMOS **`aircraft.status`** (`source.amos_heli_aircraft`, record lifecycle) **не читать** для воспроизведения golden: фильтр `AND a.status = 0` в `_program_ac_sql` — ошибочный артефакт прототипа, даёт расхождение roster `program_ac` (169 vs 170) без связи с SC/симуляцией. **Follow-up:** default `program_ac_dataframe(..., strict_status=False)` или убрать фильтр; витрина `sim_input_program_ac` — только колонки golden.

## Задачи

### Задача 1. Расширить `query2.sql`: добавить числовые ID

**Приоритет:** высокий
**Файл:** `data_input/analytics/SQL/query2.txt`

**Что изменить:**
1. Добавить `a.aircraftno_i` (числовой ID борта из `aircraft`) — поле уже есть в AMOS, но не извлекается
2. Добавить `r.locationno_i` — числовой ID локации
3. `oh_threshold` уже в запросе, но проверить что он попадает в reports

**Почему:** сейчас `aircraft_number` вычисляется регэкспом из текстовой `location` (склейка `RA-XXXXX`), но в AMOS есть готовый числовой ID. Цикл «текст → склейка → регэксп → число» не имеет бизнес-смысла.

**Приёмка:** `aircraftno_i` доступен в `reports.amos_heli_rotables_components_status`

---

### Задача 2. Построить `analytics.sim_input_planery`

**Приоритет:** высокий (целевая архитектура)
**Зависит от:** Задачи 1

**Что нужно:**
Новая витрина `analytics.sim_input_planery` на базе `reports` + `integrated`, которая:
- Содержит все поля текущего `reports.amos_heli_rotables_components_status`
- Добавляет числовые ID вместо текстовых: `aircraft_number` (из `aircraftno_i`), `ac_type_i` (уже есть)
- `mfg_date` хранить как `Date32`, не как `String 'DD.MM.YYYY'`
- `condition` хранить как `UInt8` код + словарь, не как `String`
- `location` хранить как `aircraftno_i` + fallback `address_i` для складских позиций, не как `String 'RA-XXXXX'`
- Добавляет поля `aircraft_number`, `ac_type_mask`, `group_by` (из `md_components` — либо хардкод на стороне DWH, либо JOIN)

**Целевой формат (ключевые колонки):**
```
partno String, partseqno_i Int32, serialno String, psn Int64,
aircraft_number Int32 (из aircraftno_i), ac_type_mask UInt8 (0=Mi-8, 1=Mi-17),
group_by UInt8, ll UInt32, oh UInt32, oh_threshold UInt32,
sne UInt32, ppr UInt32, mfg_date Date32, condition UInt8,
owner UInt8, address_i Int32, removal_date Date32, target_date Date32,
processing_dt DateTime
```

**Приёмка:** `SELECT * FROM analytics.sim_input_planery WHERE processing_dt = '2026-06-12'` возвращает данные в формате, готовом для GPU

---

### Задача 3. Построить `analytics.sim_input_program_ac`

**Приоритет:** средний
**Что нужно:**
Витрина на базе `staging.amos_heli_aircraft` + SCD-2 срез на дату:
- Колонки **как в golden** `Program_AC.xlsx`: `ac_registr, ac_typ, object_type, description, owner, operator, homebase, homebase_name, directorate`
- Партиция по `processing_dt` (аналог reports)
- **Не включать** AMOS `aircraft.status` (record lifecycle) в контракт витрины и не фильтровать `status = 0` без явного бизнес-правила — golden Excel этого не делает

Сейчас эти данные грузятся из Excel `Program_AC.xlsx` (ручная выгрузка из AMOS).

**Приёмка:** row/key parity с golden `Program_AC` на ту же дату (не произвольный AMOS status-filter)

---

### Задача 4. Построить `analytics.sim_input_status_overhaul`

**Приоритет:** средний
**Что нужно:**
Витрина открытых WP на базе `staging.amos_heli_wp_header` + SCD-2:
- `wpno, ac_registr, ac_typ, description, sched_start_date, sched_end_date, act_start_date, act_end_date, status, owner, operator`
- Даты в `Date32`, не в AMOS-формате (дни от 1971-12-31)

Сейчас данные грузятся из Excel `Status_Overhaul.xlsx`.

**Приёмка:** запрос на любую дату возвращает открытые ремонтные работы

---

### Задача 5 (опционально). Перенести каскад `status_id` на сторону DWH

**Приоритет:** низкий (можно оставить в Python)
**Что нужно:**
Реализовать логику 9-этапного каскада (см. `dwh_as_is.md` Часть 2) как SQL-представление в analytics:
- `analytics.sim_input_planery_status` — с вычисленным `status_id` и `repair_days`

Это разгрузит Python-пайплайн и ускорит симуляцию.

---

## Технические замечания

1. **Типы данных для GPU:** все числовые поля должны быть `UInt32` (или меньше). Никаких `Int64`, `Nullable`, `Float64`.
2. **Формат дат:** только `Date32`. `mfg_date` как `String 'DD.MM.YYYY'` — пережиток Excel-эпохи.
3. **location:** поле `location` в reports сейчас содержит склейку `RA-XXXXX` или складскую позицию. Для GPU нужно числовое представление: `aircraftno_i` для бортов, `address_i` для складов.
4. **Партиция:** все витрины — `ORDER BY processing_dt`, `PARTITION BY toYYYYMM(processing_dt)`.
5. **Версионность:** витрины материализованные (INSERT на каждый новый `processing_dt`), не перезаписываемые.

## Приёмка DWH-среза (обязательный sim-gate)

Extract/load **недостаточен**. После `dwh_loader` + post-enrichment на целевой `version_date`:

1. **Sim:** `code/sim_v2/messaging/orchestrator_limiter_v8.py` — прогон на том же `version_date` / `version_id`.
2. **Validators:** `code/validation/run_all.py` (или `run_all_stream.py`) — **INV-1…INV-12 PASS** по SSoT `config/transitions/invariants.json`; verdict — `validator-judge`.
3. Extract smoke (counts, churn, golden keys) — **pre-sim**, не финальный verdict.

Без sim **выводы о пригодности среза для GPU не делаются**.

**Результат `2026-06-12 v1` (2026-06-13):** sim-gate **PASS** — workflow `W_dwh_sim_gate_20260612`, runbook `docs/dwh_sim_gate.md`.

**Batch (5 срезов после 08.04):** `code/utils/dwh_batch_sim_gate.py` — 4/5 PASS; FAIL `2026-05-20` → triage `docs/dwh_inv12_acn24223_triage.md`, workflow `W_inv12_acn24223_20260520`.

## Контакты

- Проект: Алексей Будник
- Код: `code/utils/dwh_loader.py`, `code/utils/dwh_post_enrichment.py` (ветка `feature/dwh-bb8`)
- Workflow KG: `W_dwh_analytics_load` (extract), `W_dwh_sim_gate_20260612` (sim-gate PASS)
- Анализ AS-IS: `data_input/analytics/DWH/dwh_as_is.md` (требует актуализации по фактам P1–P2)
- Replay/golden: `python3 code/utils/dwh_direct_load.py --report-date YYYY-MM-DD --match-golden`
- **Regression churn (агрегаты):** `code/utils/dwh_aggregate_churn_export.py` — см. `docs/dwh_aggregate_churn_analytics.md`
- **Sim-gate (обязательная приёмка):** `docs/dwh_sim_gate.md` — orchestrator + INV-1…INV-12
