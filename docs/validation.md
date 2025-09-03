# Валидация симуляции и экспорта (sim_results)

Дата: 2025-09-02

## Active Trigger (1→2)

- Определение: `active_trigger` — дата активации (входа в эксплуатацию), дни от эпохи.
- Правило: в день перехода 1→2 должно выполняться:
  - `active_trigger = day_abs - repair_time`.
  - `assembly_trigger = (D+1) − assembly_time` (для справки).

### SQL‑проверки

```sql
-- Переход 1→2 (self-join по предыдущему дню)
SELECT count()
FROM sim_results s
INNER JOIN sim_results p
  ON s.version_date = p.version_date
 AND s.version_id   = p.version_id
 AND s.aircraft_number = p.aircraft_number
 AND s.day_abs = p.day_abs + 1
WHERE p.status_id = 1 AND s.status_id = 2;
```

```sql
-- Соответствие формуле active_trigger
SELECT count()
FROM sim_results s
INNER JOIN sim_results p
  ON s.version_date = p.version_date
 AND s.version_id   = p.version_id
 AND s.aircraft_number = p.aircraft_number
 AND s.day_abs = p.day_abs + 1
WHERE p.status_id = 1 AND s.status_id = 2
  AND s.active_trigger != (s.day_abs - s.repair_time);
```

```sql
-- Нули в active_trigger на переходах 1→2
SELECT count()
FROM sim_results s
INNER JOIN sim_results p
  ON s.version_date = p.version_date
 AND s.version_id   = p.version_id
 AND s.aircraft_number = p.aircraft_number
 AND s.day_abs = p.day_abs + 1
WHERE p.status_id = 1 AND s.status_id = 2
  AND s.active_trigger = 0;
```

### Результаты (10‑летний прогон, 3650 суток)
- Переходов 1→2: 29
- Нарушений формулы `active_trigger = day_abs - repair_time`: 0
- `active_trigger = 0` на переходах 1→2: 0

### Уточнение формулы (подтверждено данными)
- Эквивалентно: `day_abs − active_trigger = repair_time` в день установки `active_trigger`.
- Проверка:
  - Совпадений `(day_abs − active_trigger) = repair_time`: 29/29
  - Совпадений `(day_abs − active_trigger) = repair_time − 1`: 0/29

Примечание: в режиме `--export-triggers-only` не выгружается `repair_time`, поэтому формальная проверка равенства выполняется по данным полного экспорта. В триггер‑only режиме валидируем инвариант «по одному дню на борт».

## GPU‑постпроцессинг MP2 (окна ремонта от active_trigger)

Семантика (подтверждено):
- Пусть `d_set` — день, когда в статусе 1 установлен `active_trigger>0` (момент 1→2 в исходной RTC), а `s = value(active_trigger[d_set])` — абсолютный день начала ремонта для постпроцессинга.
- Окно ремонта: `[s..e]`, где `e = d_set − 1`. День `d_set` не изменяется постпроцессингом.
- Внутри окна `[s..e]`:
  - `status_id := 4`,
  - `repair_days(d) := d − s + 1` (на `s` → 1; на `e` → `repair_time`),
  - `assembly_trigger(d) := 1` только в день `d = e − assembly_time`, если он попадает внутрь окна; иначе 0.
- `partout_trigger` не корректируется в этой фазе.

Инварианты:
- Для каждого окна `[s..e]` выполняется `status_id==4` и `repair_days` монотонно 1..`repair_time`.
- В окне есть не более одного `assembly_trigger=1` (ровно один при `e−A∈[s..e]`).
- День `d_set` сохраняет оригинальные значения RTC.

Валидатор:
- Проверка окон: сверка диапазона `[s..e]` по MP2 после postprocess, соответствия `repair_days`, и точечного `assembly_trigger`.

## Assembly/Partout Trigger (статусы 4 и 6)

- Определения:
  - `assembly_trigger` — дата сборки (дни от эпохи, UInt16), выставляется в день события на статусе 4; в другие дни = 0.
  - `partout_trigger` — флаг снятия (0/1) на день события статуса 4 и 6; в другие дни = 0.
- Инварианты:
  - Однократно на борт: сумма по дням `assembly_trigger>0` ∈ {0,1}; сумма `partout_trigger=1` ∈ {0,1}.
  - Метки `assembly_trigger_mark/partout_trigger_mark` = 1 строго в день события.
- Статус 365 суток: событий `assembly/partout` на тестовом горизонте не зафиксировано (суммы = 0). На 3650 — ожидаются единичные события.

### Методика проверки vs переходы 2→4/2→6 (учтены стартовые S4 и off‑by‑one)
- Базовая логика перехода: переход 2→4 происходит на дне `day_2to4`. Ожидаемые даты триггеров:
  - Ожидаемый день снятия (2→4): `expected_partout_day = day_2to4 + (partout_time − 1)`.
  - Ожидаемый день сборки (2→4): `expected_assembly_day = day_2to4 + (repair_time − assembly_time − 1)`.
  - Ожидаемый день снятия (2→6): `expected_partout_day = day_2to6 + partout_time` (счётчик `s6_days` стартует на следующий день).
  - Если борт уже в `status_id=4` на первую дату горизонта (D0):
    - Пусть `d0 = repair_days(D0)`, тогда
      - `expected_partout_day = day_first + max(0, partout_time − d0)` (если `d0 ≤ partout_time`).
      - `expected_assembly_day = day_first + max(0, (repair_time − assembly_time) − d0)` (если `d0 ≤ repair_time − assembly_time`).
- Сопоставление делаем только если ожидаемая дата попадает ВНУТРИ горизонта симуляции `[min(day_abs) .. max(day_abs)]`.
- Если ожидаемая дата лежит ПОСЛЕ горизонта, расхождение считается объяснимым (нет данных).

### SQL‑эскиз
```
WITH transitions AS (
  -- day_2to4 и нормативы на этот период
), actual AS (
  -- фактические однодневные срабатывания
)
SELECT
  count() AS transitions_2to4,
  sumIf(1, (day_2to4 + partout_time) <= horizon_last_day)          AS expected_partout_within,
  sumIf(has(jours_partout, day_2to4 + partout_time), (day_2to4 + partout_time) <= horizon_last_day) AS matched_partout,
  sumIf(1, (day_2to4 + (repair_time - assembly_time)) <= horizon_last_day) AS expected_assembly_within,
  sumIf(has(jours_assembly, day_2to4 + (repair_time - assembly_time)), (day_2to4 + (repair_time - assembly_time)) <= horizon_last_day) AS matched_assembly
FROM transitions t
LEFT JOIN actual a USING (aircraft_number);
```

### Скрипт
- `code/utils/validate_triggers_vs_2to4.py`: выполняет сопоставление (2→4 и 2→6 для partout), выводит JSON‑резюме с totals и per_aircraft (усечённо). Сверка выполняется по `day_u16` (UInt16).

### Итоги последнего 10‑летнего прогона (с экспортом D0)
- Переходы 2→4: 199
- Переходы 2→6: 38
- Partout (2→4 + 2→6 + стартовые S4 в горизонте): expected_within=237, matched=237 (100%)
- Assembly (только 2→4 + стартовые S4): expected_within=200, matched=200 (100%)
- D0 в `sim_results`: присутствует (`day_u16=0`, `day_abs=version_date`), помогает валидации стартовых S4.

## Экспортные режимы (для валидаторов)
- `legacy` (по умолчанию): полный экспорт, включая производные поля и метки (CPU‑постпроцессинг).
- `triggers-only`: минимальный экспорт без производных полей (для отладки триггеров и подготовки GPU‑постпроцессинга).

## План следующей валидации
- Сформулировать fallback‑окно ремонта для derived/mark при `active_trigger=0` на основе `(orig_status_id=4, orig_repair_days)` и MP1.
- Подтвердить расчёт `partout_trigger_mark` и `assembly_trigger_mark` на выборке.
- Собрать единый валидационный скрипт с агрегатами и sample‑выборками.

