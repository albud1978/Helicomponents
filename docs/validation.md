# Validation

## SSoT инвариантов
- **`config/transitions/invariants.json`** — единый формализованный реестр всех инвариантов, temporal-контрактов и GPU-ограничений модели V8.
  - INV-1..INV-9: глобальные инварианты (sne ≤ ll, ops = target, repair ≤ repair_number, баланс наработок и др.)
  - TEMP-1..TEMP-4: temporal-контракты (длительность ремонта, минимальное время в unsvc, liveness)
  - GPU-1..GPU-6: ограничения платформы FLAME GPU (read/write, mp5_lin read-only, Float64 запрет и др.)
- Правило Cursor: `.cursor/rules/25_invariants_contract.mdc` — автоматически подтягивается при работе с `code/sim_v2/**`, `code/validation/**`, `code/analysis/**`, `config/transitions/**`.

## Версия ClickHouse (актуальный runtime)
- Проверено на сервере: **`24.10.1.2812`** (проверка `SELECT version();`, дата фиксации: 20-02-2026).
- SQL-проверки и runbook в этом документе считаются валидированными для этой версии.
- При смене версии ClickHouse перед прогонами валидации сначала перепроверять совместимость SQL (особенно window-функции и deprecated-конструкции).

## Партицирование runtime-таблиц (v9)
- `sim_masterv2_v9`: `PARTITION BY (version_date, toYYYYMM(day_date))`
- `sim_repairline_v9`: `PARTITION BY (version_date, toYYYYMM(day_date))`
- Текущие `ORDER BY` сохранены для совместимости с существующими SQL и валидаторами.

## Семантика occupancy в `sim_repairline_v9`
- Единый источник правды по занятости ремонта (`aircraft_number`, `group_by`) — `sim_masterv2_v9`.
- В `sim_repairline_v9` occupancy формируется как overlay:
  - claim-based сегменты (`repair_claim_start_day/end_day/line_id/source`);
  - claimless repair-эпизоды из траектории `status/pre_status` в master.
- Runtime `acn/group_by` линии больше не трактуется как доменная occupancy (это только телеметрия слоя RepairLine).
- Для строк с `aircraft_number != 0` допускается только детерминированное сопоставление с master; конфликт/неоднозначность трактуется как ошибка данных.

## Статус триггеров (`active_trigger`, `assembly_trigger`) в V8
- `active_trigger` сохранён в схеме как артефактный маркер (backward compatibility/аналитика), но не является источником восстановления repair-окна.
- Восстановление окна ремонта в V8 выполняется по claim metadata (`repair_claim_*`) + commit-событиям.
- Текущая установка `active_trigger`: однодневный marker на шаге валидного P2/P3 commit после проверок claim-окна и дневного cap.
- `assembly_trigger` остаётся рабочим маркером фазы сборки:
  - для day0-repair инициализируется при загрузке популяции;
  - в postprocess проставляется на хвосте claim-окна (`days_to_end <= assembly_time`).
- Решение по удалению `active_trigger` отложено; до следующего уровня сборки поле хранится без изменения контракта таблиц.

### Важно по применению DDL
- Изменение partition key в ClickHouse не применяется через `ALTER`.
- Для действующих таблиц обязателен цикл: `DROP TABLE` -> `CREATE TABLE` -> повторная загрузка данных.
- Рекомендуемая последовательность после смены DDL:
  1) прогон D1 с `--drop-table`;
  2) прогон D2 без `--drop-table`;
  3) проверка `run_all_stream` по обоим dataset-ключам.

### Мини-runbook для Superset
- Всегда фильтровать `version_date` и `version_id` в датасете/чарте.
- Временная колонка: `day_date`; grain: `day/month/year`.
- Не строить графики по всем версиям сразу (это размывает метрики и увеличивает скан партиций).
- Для тяжёлых чартов использовать предагрегации по дням (status/repairline daily counts) и затем строить month/year поверх них.

## Методология
- `docs/architecture/validation_rules.md` — методология SQL‑first валидации, heli_pandas проверки, NVRTC правила.

## Связанные SSoT
- `config/transitions/transitions_rules.json` — матрица переходов, condition precedent/subsequent, порядок RTC.
- `config/transitions/quota_rules.json` — логика квотирования, RepairLine, spawn.

## Контекстные капсулы
- Формат капсулы проверяется вручную по шаблону секций (8 обязательных секций с лимитами).
- Формат капсулы: `docs/limiter_v8_capsule.md` (строгий шаблон и лимиты секций).

## Графы знаний
См. `README.md` → секция "Графы знаний (Neo4j)".

## Операционный runbook (для новых агентов)

Этот документ не дублирует формулы инвариантов. Формулы, severity и mapping `инвариант -> validator` хранятся только в `config/transitions/invariants.json`.

### 1) Подготовка окружения

```bash
source ~/miniconda3/etc/profile.d/conda.sh
conda activate cuda13
source config/load_env.sh
export CUBE_CONFIG_PATH="$PWD/config"
```

### 2) Правильная последовательность симуляции для двух датасетов

```bash
# D1: первый датасет с очисткой таблиц
python3 code/sim_v2/messaging/orchestrator_limiter_v8.py --version-date 2025-07-04 --drop-table

# D2: второй датасет БЕЗ drop
python3 code/sim_v2/messaging/orchestrator_limiter_v8.py --version-date 2025-12-30
```

### 3) Потоковый массовый прогон валидаций

Актуальный раннер: `code/validation/run_all_stream.py`.

```bash
# Показать доступные dataset-ключи (YYYYMMDD:version_id)
python3 code/validation/run_all_stream.py --list-datasets

# Запуск только выбранных датасетов (рекомендуется)
python3 code/validation/run_all_stream.py --dataset 20250704:1 --dataset 20251230:2

# Альтернатива: запустить все обнаруженные
python3 code/validation/run_all_stream.py --all-datasets
```

### 4) Принцип опционального запуска

- Без `--dataset` и без `--all-datasets` раннер валидации не запускает проверки.
- В этом режиме он только показывает подсказку и список обнаруженных датасетов.
- Это защищает от случайного автозапуска на новых данных.

### 5) Что делать при новом датасете

1. Сначала прогнать симуляцию с нужным `--version-date`.
2. Проверить ключ через `--list-datasets` (формат `YYYYMMDD:ID`).
3. Явно передать этот ключ в `--dataset`.

### 6) Примечание по legacy-раннеру

- `code/validation/run_all.py` оставлен для совместимости.
- Для новых прогонов использовать `code/validation/run_all_stream.py`, так как он читает актуальный список валидаторов из SSoT (`invariants.json`) и поддерживает потоковый вывод.
