---
name: clickhouse-v9-guard
description: Executes read-only ClickHouse analysis for V8/V9 simulation tables with strict dataset scoping. Use when tasks mention ClickHouse, SQL, sim_masterv2_v9, sim_repairline_v9, version_id, version_date, group_by, validations, or transition counts.
---

# ClickHouse V9 Guard

## Когда применять

Применяй этот skill для задач с ClickHouse/SQL, особенно если упоминаются:
- `sim_masterv2_v9`, `sim_repairline_v9`
- `version_id`, `version_date`, `day_u16`, `day_date`
- `group_by` (Mi-8=1, Mi-17=2)
- проверки инвариантов, массовая/потоковая валидация, подсчёт переходов `pre_status_id -> status_id`.

## Обязательный старт

Если запускаются команды/скрипты, сначала подготовь окружение:

```bash
source ~/miniconda3/etc/profile.d/conda.sh
conda activate cuda13
source config/load_env.sh
export CUBE_CONFIG_PATH="$PWD/config"
```

Подключение к CH:
- использовать `clickhouse_driver` (native, порт `9000`);
- host/port/user брать из `config/database_config.yaml` (с ENV overrides);
- пароль брать из `CLICKHOUSE_PASSWORD`.

## Безопасность и ограничения

1. Только `SELECT`, если пользователь явно не разрешил мутации.
2. Всегда явно фиксируй скоуп датасета:
   - минимум: `version_date` и `version_id`;
   - для планеров/типов: обязательно `group_by`.
3. Не смешивай `group_by` без явной причины (критичное измерение проекта).
4. Не предполагай схему таблиц — сначала проверяй `DESCRIBE TABLE`.
5. Если пользователь пишет "последний прогон", явно проговаривай критерий:
   - по умолчанию: `max(version_date)` для каждого `version_id`;
   - если нужен другой критерий (`run_id`, timestamp, commit) — спроси.

## Версия сервера и совместимость

- Базовая целевая версия ClickHouse в проекте: **`24.10.1.2812`** (зафиксировано 20-02-2026).
- Перед сложными SQL (window/CTE/функции) сверяйся с реальной версией:

```sql
SELECT version();
```

- Если версия отличается, в отчёте явно помечай риск несовместимости и проверяй SQL на этой версии до финальных выводов.

## Актуальная V9 схема (проверять через DESCRIBE перед отчётом)

### `sim_masterv2_v9`
Ключевые поля для SQL-аналитики:
- `version_date`, `version_id`, `day_u16`, `day_date`
- `aircraft_number`, `group_by`
- `status_id`, `pre_status_id`
- `repair_claim_start_day`, `repair_claim_end_day`, `repair_claim_source`, `repair_claim_line_id`

Переходы считать по правилу:
- `pre_status_id = X AND status_id = Y`.

### `sim_repairline_v9`
Ключевые поля:
- `version_date`, `version_id`, `day_u16`, `day_date`
- `line_id`, `free_days`, `repair_time`
- `aircraft_number` (lookback telemetry), `group_by`
- `bank_count`, `bank_head_start`, `bank_head_end`

Важно:
- `day_date` materialized присутствует;
- `group_by` присутствует и должен использоваться в фильтрах/агрегациях по типу борта.

## Шаблоны запросов

### 1) Проверка схемы перед анализом

```sql
DESCRIBE TABLE sim_masterv2_v9;
DESCRIBE TABLE sim_repairline_v9;
```

### 2) Доступные датасеты

```sql
SELECT version_date, version_id
FROM sim_masterv2_v9
WHERE group_by IN (1, 2)
GROUP BY version_date, version_id
ORDER BY version_date, version_id;
```

### 3) Рождения Mi-8 по переходам 0->2 и 0->3 (последний прогон на version_id)

```sql
WITH latest AS (
  SELECT version_id, max(version_date) AS version_date
  FROM sim_masterv2_v9
  WHERE version_id IN (1, 2)
  GROUP BY version_id
)
SELECT
  m.version_id,
  m.version_date,
  countIf(m.pre_status_id = 0 AND m.status_id = 2) AS count_0_2,
  countIf(m.pre_status_id = 0 AND m.status_id = 3) AS count_0_3
FROM sim_masterv2_v9 AS m
INNER JOIN latest AS l
  ON m.version_id = l.version_id
 AND m.version_date = l.version_date
WHERE m.group_by = 1
GROUP BY m.version_id, m.version_date
ORDER BY m.version_id;
```

### 4) Дневной контроль repairline по Mi-8/Mi-17

```sql
SELECT
  version_date,
  version_id,
  day_u16,
  group_by,
  countIf(aircraft_number != 0) AS busy_lines
FROM sim_repairline_v9
WHERE version_date = 20250704
  AND version_id = 1
  AND group_by IN (1, 2)
GROUP BY version_date, version_id, day_u16, group_by
ORDER BY day_u16, group_by;
```

## Стандарт отчёта

В ответе обязательно фиксируй:
1. Какие таблицы/поля проверены (`DESCRIBE`/SQL).
2. Какой критерий "последнего прогона" использован.
3. Полные фильтры (`version_date`, `version_id`, `group_by`).
4. Табличный результат.
5. Допущения в явном виде (`Risks if false: ...`).

