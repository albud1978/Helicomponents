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

## Инварианты статусов и квот (добавлено)

Дата: 2025-09-04

- По умолчанию проверки выполняются для последней версии: `vd = max(version_date)`, `vid = max(version_id)`.

### 1) Статус 6 — неизменность sne и ppr
- Правило: у каждого агрегата во все дни, когда `status_id=6`, значения `sne` и `ppr` постоянны (не меняются по дням).
- SQL (агрегатная проверка по бортам):
```sql
WITH params AS (
  SELECT toUInt32(max(version_date)) vd, toUInt32(max(version_id)) vid FROM sim_results
)
SELECT
  countIf(m_sne != M_sne OR m_ppr != M_ppr) AS s6_violations,
  count() AS s6_bords
FROM (
  SELECT aircraft_number,
         min(sne) AS m_sne, max(sne) AS M_sne,
         min(ppr) AS m_ppr, max(ppr) AS M_ppr
  FROM sim_results, params
  WHERE version_date=vd AND version_id=vid AND status_id=6
  GROUP BY aircraft_number
) t;
```

### 2) Разница sne между двумя входами в статус 4
- Правило: для агрегата с двумя последовательными переходами `2→4` разница `sne` между датами этих входов превышает дневной лимит: `Δsne > (oh − daily_today_u32 на второй дате)`.
- Примечания: проверяется только у тех, у кого найдено ≥2 события `2→4` в горизонте; `oh` берём из `sim_results` для этой второй даты.
- SQL (эскиз на массивах для первых двух событий):
```sql
WITH params AS (
  SELECT toUInt32(max(version_date)) vd, toUInt32(max(version_id)) vid FROM sim_results
), t24 AS (
  SELECT s.aircraft_number AS id, s.day_u16 AS d, s.sne, s.oh, s.daily_today_u32 AS dt
  FROM sim_results s
  INNER JOIN sim_results p
    ON s.version_date=p.version_date AND s.version_id=p.version_id
   AND s.aircraft_number=p.aircraft_number AND s.day_u16 = p.day_u16 + 1
  , params
  WHERE s.version_date=vd AND s.version_id=vid AND p.status_id=2 AND s.status_id=4
), pairs AS (
  SELECT id,
         arraySort(groupArray((d, sne, oh, dt))) AS ev
  FROM t24 GROUP BY id HAVING length(ev) >= 2
)
SELECT
  countIf( (ev[2].2 - ev[1].2) > (ev[2].3 - ev[2].4) ) AS ok,
  count() AS total
FROM pairs;
```

### 3) Квоты: `seed − approved − left = 0` (по дням и группам)
- Правило: на каждый день выполняется баланс семян, утверждений и остатка квоты по каждой группе (MI‑8/MI‑17).
- Источники: `seed*` — `mp4_ops_counter_mi8/mi17` (берём D+1), `approved*` — `sum(ops_ticket=1 AND intent_flag=1)` среди агентов соответствующей группы, `left* = seed* − approved*`.
- SQL (расчёт и проверка неотрицательного остатка и баланса):
```sql
WITH params AS (
  SELECT toUInt32(max(version_date)) vd, toUInt32(max(version_id)) vid FROM sim_results
), days AS (
  SELECT distinct day_u16 FROM sim_results, params WHERE version_date=vd AND version_id=vid
), seeds AS (
  SELECT d AS day_u16,
         arrayElement(mp4_ops_counter_mi8, least(d+1, length(mp4_ops_counter_mi8))) AS seed8,
         arrayElement(mp4_ops_counter_mi17, least(d+1, length(mp4_ops_counter_mi17))) AS seed17
  FROM days CROSS JOIN (
    SELECT mp4_ops_counter_mi8, mp4_ops_counter_mi17 FROM sim_results, params
    WHERE version_date=vd AND version_id=vid LIMIT 1
  )
), appr AS (
  SELECT day_u16,
         sumIf(1, group_by=1 AND ops_ticket=1 AND intent_flag=1) AS approved8,
         sumIf(1, group_by=2 AND ops_ticket=1 AND intent_flag=1) AS approved17
  FROM sim_results, params
  WHERE version_date=vd AND version_id=vid
  GROUP BY day_u16
)
SELECT
  countIf(seed8 < approved8) AS neg8,
  countIf(seed17 < approved17) AS neg17,
  countIf((seed8 - approved8) < 0) AS left_neg8,
  countIf((seed17 - approved17) < 0) AS left_neg17,
  countIf((seed8 - approved8) + (seed17 - approved17) != (seed8 + seed17 - (approved8 + approved17))) AS balance_mismatch
FROM seeds s
LEFT JOIN appr a USING(day_u16);
```

### 4) Выборочный контроль инкрементов (status 2) и `repair_days` (status 4)
- Правила:
  - Status 2: на `d→d+1` приросты `sne` и `ppr` равны `daily_today_u32(d)` для тех же агентов, которые оставались в статусе 2.
  - Status 4: `repair_days` возрастает на 1 при сохранении статуса 4 на `d→d+1`.
- SQL (проверки выборки):
```sql
-- Status 2: инкременты по dt (выборочно 10k пар)
WITH params AS (
  SELECT toUInt32(max(version_date)) vd, toUInt32(max(version_id)) vid FROM sim_results
)
SELECT countIf((s2n.sne - s2p.sne) = s2p.daily_today_u32) AS ok_sne,
       countIf((s2n.ppr - s2p.ppr) = s2p.daily_today_u32) AS ok_ppr,
       count() AS checked
FROM (
  SELECT * FROM sim_results, params
  WHERE version_date=vd AND version_id=vid AND status_id=2
  ORDER BY rand() LIMIT 10000
) s2p
INNER JOIN sim_results s2n
  ON s2n.version_date=s2p.version_date AND s2n.version_id=s2p.version_id
 AND s2n.aircraft_number=s2p.aircraft_number AND s2n.day_u16=s2p.day_u16+1
WHERE s2n.status_id=2;

-- Status 4: repair_days монотонен +1 (выборочно 10k пар)
WITH params AS (
  SELECT toUInt32(max(version_date)) vd, toUInt32(max(version_id)) vid FROM sim_results
)
SELECT countIf(s4n.repair_days = s4p.repair_days + 1) AS ok_repair_inc,
       count() AS checked
FROM (
  SELECT * FROM sim_results, params
  WHERE version_date=vd AND version_id=vid AND status_id=4
  ORDER BY rand() LIMIT 10000
) s4p
INNER JOIN sim_results s4n
  ON s4n.version_date=s4p.version_date AND s4n.version_id=s4p.version_id
 AND s4n.aircraft_number=s4p.aircraft_number AND s4n.day_u16=s4p.day_u16+1
WHERE s4n.status_id=4;
```

## Журнал переходов и проверки (transitions_log_export)

Дата: 2025-09-04

Скрипт `code/validators/transitions_log_export.py` формирует постфактум‑журнал переходов на базе `sim_results` (self‑join по дню `d−1→d`) и экспортирует в Excel:
- Листы: `transitions` (все строки), `summary` (свод по предопределённым меткам), `summary_all` (все комбинации `prev→curr`), `matrix` (матрица `prev_status × curr_status`), `violations` (счётчик нарушений по флагам).

Структура строки журнала (минимально важные поля):
- `prev_status, curr_status, transition_code` — классификация перехода.
- `sne_prev/curr, ppr_prev/curr, repair_prev/curr, dt_prev/dt_curr` — динамика счётчиков.
- `ops_ticket_prev, intent_flag_prev, active_trigger_prev, assembly_trigger_curr, group_by, oh, br` — контекст.

Флаги условий (cond_*) и сайд‑эффектов (side_*):
- `cond_ops_approved`: (prev=1 AND ops_ticket_prev=1 AND intent_flag_prev=1) — условие для 1→2.
- `cond_trigger_set`: (active_trigger_prev>0) — факт триггера (для 1→4/4→2 при постпроцессинге, диагностика).
- `cond_ppr_ge_oh`: (ppr_prev≥oh) — условие входа 2→4.
- `cond_reach_br`: (sne_prev≥br) — условие входа 2→6.
- `cond_leave_simple`: (prev=3) — исходная предпосылка 3→2.
- `cond_assembly_day`: (assembly_trigger_curr=1) — маркер дня сборки (ожидаемый 4→5).
- `side_ppr_reset`: (prev=1 AND curr=2 AND ppr_curr=0) — сброс PPR при 1→2.
- `side_sne_inc_by_dt`: (prev=2 AND curr=2 AND sne_curr−sne_prev=dt_prev) — инкремент SNE в 2.
- `side_ppr_inc_by_dt`: (prev=2 AND curr=2 AND ppr_curr−ppr_prev=dt_prev) — инкремент PPR в 2.
- `side_repair_inc`: (curr=4 AND repair_curr=repair_prev+1) — рост `repair_days` при нахождении в 4.
- `side_repair_stop`: (prev=4 AND curr=2 AND repair_curr≥repair_prev) — остановка счётчика при выходе из 4.
- `side_asm_trigger`: (assembly_trigger_curr=1) — однодневный флаг сборки в день события.
- `side_fix_in_s6`: (curr=6 AND sne_curr=sne_prev AND ppr_curr=ppr_prev) — фиксация счётчиков в 6.

Что означает `violations`:
- В листе `violations` для каждого флага показано число строк журнала, где флаг равен `false`.
- Для корректной интерпретации фильтровать строки по релевантному типу перехода. Примеры:
  - 1→2: обязаны быть `cond_ops_approved=true` и `side_ppr_reset=true`.
  - 2→4: `cond_ppr_ge_oh=true` (при наличии `oh`), а на интервалах 2→2 — `side_sne_inc_by_dt/side_ppr_inc_by_dt=true`.
  - 2→6: `cond_reach_br=true`.
  - 4→5: в день 4→5 ожидается `cond_assembly_day=true`; на интервалах 4→4 — `side_repair_inc=true`.
  - 4→2 в один день с 4→5 — индикатор каскада (см. диагностику ниже). Обязательно проверять ops_tecket=1.

Рекомендованный порядок чтения отчёта:
1) `summary_all` — посмотреть все фактические комбинации `prev→curr` и их количество.
2) `matrix` — быстро увидеть нетипичные пары (например, 4→2 без 5 как промежуточного).
3) `transitions` — отфильтровать нужный `transition_code` и анализировать cond_*/side_*.
4) `violations` — использовать как индикатор, но интерпретировать только в срезе релевантных строк.

Диагностика каскадных переходов в один шаг (пример 4→5→2):
- Симптом: в журнале видны строки 4→2 на день d, при этом `ops_ticket_prev=1` и нет `assembly_trigger_curr`.
- Причина: ранний слой переводит 4→5, поздний слой 5 учитывает квоту и делает 5→2 в тот же день ⇒ внешне 4→2.
- Решение (предложение): ввести флаг `status_changed_today`, обнулять на `begin_day`, и блокировать повторные смены статуса в последующих слоях того же шага.

