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
