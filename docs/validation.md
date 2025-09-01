# Валидация симуляции и экспорта (sim_results)

Дата: 2025-09-01

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

## Assembly/Partout Trigger (статус 4)

- Определения:
  - `assembly_trigger` — дата сборки (дни от эпохи), выставляется в день события на статусе 4; в другие дни = 0.
  - `partout_trigger` — флаг снятия (0/1) на день события статуса 4; в другие дни = 0.
- Инварианты:
  - Однократно на борт: сумма по дням `assembly_trigger>0` ∈ {0,1}; сумма `partout_trigger=1` ∈ {0,1}.
  - Метки `assembly_trigger_mark/partout_trigger_mark` = 1 строго в день события.
- Статус 365 суток: событий `assembly/partout` на тестовом горизонте не зафиксировано (суммы = 0). На 3650 — ожидаются единичные события.

## Экспортные режимы (для валидаторов)
- `legacy` (по умолчанию): полный экспорт, включая производные поля и метки (CPU‑постпроцессинг).
- `triggers-only`: минимальный экспорт без производных полей (для отладки триггеров и подготовки GPU‑постпроцессинга).

## План следующей валидации
- Сформулировать fallback‑окно ремонта для derived/mark при `active_trigger=0` на основе `(orig_status_id=4, orig_repair_days)` и MP1.
- Подтвердить расчёт `partout_trigger_mark` и `assembly_trigger_mark` на выборке.
- Собрать единый валидационный скрипт с агрегатами и sample‑выборками.

