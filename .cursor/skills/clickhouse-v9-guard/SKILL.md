---
name: clickhouse-sim-guard
description: Executes read-only ClickHouse analysis for simulation tables (v9/v10+) with strict scoping and schema-first checks. Use for ClickHouse/SQL, Superset BI diagnostics, version_id/version_date/group_by, validations, transitions, and repairline metrics.
---

# ClickHouse SIM Guard (v9/v10+)

## Когда применять

Применяй skill для задач с ClickHouse/SQL и BI, особенно если упоминаются:
- симуляционные таблицы вида `sim_masterv2_v*`, `sim_repairline_v*`;
- `version_id`, `version_date`, `day_u16`, `day_date`, `group_by`;
- проверки инвариантов, переходов `pre_status_id -> status_id`, анализ дашбордов Superset.

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
4. Не предполагай схему таблиц — сначала `DESCRIBE TABLE`.
5. Если пользователь говорит "последний прогон", явно фиксируй критерий:
   - по умолчанию: `max(version_date)` для каждого `version_id`;
   - если нужен иной критерий (`run_id`, timestamp, commit) — уточнить.

## Версия сервера и совместимость

- Базовая целевая версия в проекте: `24.10.1.2812` (может измениться).
- Перед сложными SQL/оконными функциями:

```sql
SELECT version();
```

- Если версия отличается, явно помечай риск совместимости в выводе.

## Универсальный выбор таблиц (без жёсткой привязки к v9)

### 1) Найти кандидатов

```sql
SHOW TABLES FROM default LIKE 'sim_masterv2_v%';
SHOW TABLES FROM default LIKE 'sim_repairline_v%';
```

### 2) Выбрать рабочие таблицы

- Предпочитать таблицы с максимальным суффиксом (`v10` > `v9`) **только после проверки схемы**.
- Использовать переменные в запросах/отчёте:
  - `{master_table}`
  - `{repair_table}`

### 3) Проверить схему перед каждым анализом

```sql
DESCRIBE TABLE {master_table};
DESCRIBE TABLE {repair_table};
```

## Feature-gates по колонкам (обязательно)

Перед SQL-логикой проверяй наличие нужных полей:
- Базовые: `version_date`, `version_id`, `day_date`, `group_by`.
- Переходы: `pre_status_id`, `status_id`.
- Repairline telemetry: `line_id`, `free_days`, `repair_time`, `aircraft_number`.
- Опциональные bank-поля: `bank_count`, `bank_head_start`, `bank_head_end` (могут отсутствовать в некоторых версиях).

Если колонки нет — не падать в догадки, а переключать запрос на совместимую ветку и явно писать это в `Facts/Assumptions`.

## BI/Superset guardrails (накопленный опыт)

1. **Row-limit в Pivot**  
   Для матрицы `line_id × day_date` часто нужно >10k строк.  
   Проверка:
   - ожидаемая кардинальность по SQL;
   - `row_limit` в чарте должен покрывать ожидаемое число строк.

2. **Разрывы на оси времени**  
   В адаптивных данных возможны месяцы без snapshot-дней.  
   Это не всегда баг. Для “безразрывной” оси:
   - использовать календарную ось (virtual dataset);
   - fill-стратегию согласовывать явно: `zero` или `carry-forward`.

3. **Cross-filter vs native filter конфликт**  
   Проверять `chartsInScope` и `scope.excluded`.  
   Дубли фильтров по `version_date`/`time_grain_sqla` могут дать пустые результаты.

4. **Сортировка по времени**  
   Для агрегатов по времени в CH использовать валидную сортировку, совместимую с `GROUP BY` (например, через агрегат по дате в `orderby`), а не сырой `day_date`.

5. **Sentinel-значения**  
   Для BI выводов явно нормализовать sentinel (`4294967295`) в `NULL` там, где это согласовано бизнес-логикой.

## Универсальные шаблоны запросов

### 1) Доступные датасеты (version scope)

```sql
SELECT version_date, version_id
FROM {master_table}
WHERE group_by IN (1, 2)
GROUP BY version_date, version_id
ORDER BY version_date, version_id;
```

### 2) Рождения Mi-8 по переходам (последний прогон)

```sql
WITH latest AS (
  SELECT version_id, max(version_date) AS version_date
  FROM {master_table}
  WHERE version_id IN (1, 2)
  GROUP BY version_id
)
SELECT
  m.version_id,
  m.version_date,
  countIf(m.pre_status_id = 0 AND m.status_id = 2) AS count_0_2,
  countIf(m.pre_status_id = 0 AND m.status_id = 3) AS count_0_3
FROM {master_table} AS m
INNER JOIN latest AS l
  ON m.version_id = l.version_id
 AND m.version_date = l.version_date
WHERE m.group_by = 1
GROUP BY m.version_id, m.version_date
ORDER BY m.version_id;
```

### 3) Repairline дневной контроль

```sql
SELECT
  version_date,
  version_id,
  day_u16,
  group_by,
  countIf(aircraft_number != 0) AS busy_lines
FROM {repair_table}
WHERE version_date = 20250704
  AND version_id = 1
  AND group_by IN (1, 2)
GROUP BY version_date, version_id, day_u16, group_by
ORDER BY day_u16, group_by;
```

## Стандарт отчёта

В ответе обязательно фиксируй:
1. Какие таблицы и их версии выбраны (`{master_table}`, `{repair_table}`) и почему.
2. Какие поля проверены (`DESCRIBE`) и какие feature-gates сработали.
3. Полные фильтры (`version_date`, `version_id`, `group_by`).
4. Табличный результат.
5. Допущения в явном виде (`Risks if false: ...`).

