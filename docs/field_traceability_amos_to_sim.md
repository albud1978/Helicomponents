# Трассировка полей: AMOS → DWH → Excel → Extract → heli_pandas → Симуляция

> Дата создания: 2026-03-24
> Цель: полная карта трансформаций каждого поля от сырых данных AMOS до загрузки в FLAME GPU симуляцию.
> Задача: последовательное спрямление загрузки (DWH reports → heli_pandas_dwh) с валидацией по golden rule (heli_pandas из Excel).

## Цепочка данных (текущая)

```
AMOS Oracle DB
  ↓  query2.txt (SQL: JOIN, CASE, Oracle-функции info_amos.counter_psn_*)
Excel (Status_Components.xlsx)
  ↓  dual_loader.py (фильтрация md_components + ALLOWED_OWNERS, типы, clip/fillna)
heli_pandas (raw, status_id=0)
  ↓  18 скриптов enrichment (ac_type_mask, group_by, status_id, repair_days, ...)
heli_pandas (enriched)
  ↓  agent_population_units.py (SELECT 11 полей)
FLAME GPU симуляция
```

## Цепочка данных (целевая, спрямлённая)

```
DWH ClickHouse (reports.amos_heli_rotables_components_status)
  ↓  SQL + фильтры + JOIN md_components
heli_pandas_dwh (enriched)
  ↓  agent_population_units.py (SELECT 11 полей)
FLAME GPU симуляция
```

## Поля, загружаемые в симуляцию

Из `code/sim_v2/units/agent_population_units.py` и `agent_population_units_v1.py`:

```sql
SELECT psn, aircraft_number, partseqno_i, group_by, status_id,
       sne, ppr, ll, repair_days, mfg_date, ac_type_mask
FROM heli_pandas
WHERE version_date = ... AND version_id = ... AND group_by >= 3
```

---

## Детальная трассировка каждого поля

### 1. `psn` — идентификатор серийного экземпляра

| Этап | Что происходит |
|------|----------------|
| **AMOS raw** | `rotables.psn` — внутренний ID серийного экземпляра (Part Serial Number) |
| **query2.txt** | Прямое чтение: `r.psn` (строка 56) |
| **DWH reports** | `psn` Int64 — прямая копия |
| **Excel** | Колонка `psn` |
| **dual_loader.py** | `pd.to_numeric().clip(≥0).fillna(0).astype(int64)` |
| **Enrichment** | Не модифицируется |
| **Симуляция** | `int(row[0] or 0)` → UInt32 |
| **Спрямление** | ✅ Прямое чтение из DWH, cast to UInt32. **100% совпадение с golden rule** |

### 2. `partseqno_i` — ID номера детали (партномера)

| Этап | Что происходит |
|------|----------------|
| **AMOS raw** | `part.partseqno_i` — внутренний ID партномера |
| **query2.txt** | JOIN: `part p ON r.partno = p.partno` → `p.partseqno_i` (строка 4) |
| **DWH reports** | `partseqno_i` Int32 |
| **Excel** | Колонка `partseqno_i` |
| **dual_loader.py** | `pd.to_numeric().clip(≥0).fillna(0).astype(int64)` |
| **Enrichment** | Используется как ключ JOIN с `md_components.partno_comp` |
| **Симуляция** | `int(row[2] or 0)` → UInt32 |
| **Спрямление** | ✅ Прямое чтение из DWH, cast to UInt32. **100% совпадение с golden rule** |

### 3. `sne` — наработка с начала эксплуатации (минуты)

| Этап | Что происходит |
|------|----------------|
| **AMOS raw** | Таблица счётчиков. Рассчитывается Oracle-функцией `info_amos.counter_psn_on_date(r.psn, 'Ч', current_date - '1971-12-31', 3)` |
| **query2.txt** | Строка 38: вызов Oracle-функции. Результат в минутах |
| **DWH reports** | `sne` Nullable(Int32) — предрассчитано ETL DWH |
| **Excel** | Колонка `sne` |
| **dual_loader.py** | `pd.to_numeric().clip(≥0).fillna(0).astype(int64)` |
| **Enrichment** | Используется в storage_status: сравнение sne >= br_effective |
| **Симуляция** | `int(row[5] or 0)` → UInt32 |
| **Спрямление** | ✅ `clip(≥0).fillna(0)` из DWH. Расхождение с golden: **90.7%** match (1070 различий — малые отклонения ±100-300 мин, разный момент снятия показаний) |
| **DWH может давать отрицательные** | Да (-490443 мин зафиксировано). Нужен `clip(≥0)` |

### 4. `ppr` — наработка после последнего ремонта (минуты)

| Этап | Что происходит |
|------|----------------|
| **AMOS raw** | Oracle-функция `info_amos.counter_psn_overhaul(psn, 'Ч', ...)`. Условная логика: если overhaul=NULL И shop_visit_counter=0 И sne/60<1500, то берётся sne вместо overhaul |
| **query2.txt** | Строки 40-44: CASE expression с двумя Oracle-функциями |
| **DWH reports** | `ppr` Nullable(Int32) — предрассчитано ETL DWH |
| **Excel** | Колонка `ppr` |
| **dual_loader.py** | `pd.to_numeric().clip(≥0).fillna(0).astype(int64)` |
| **Enrichment** | Не модифицируется |
| **Симуляция** | `int(row[6] or 0)` → UInt32 |
| **Спрямление** | ✅ `clip(≥0).fillna(0)` из DWH. Расхождение с golden: **77.4%** match (2029 — golden>0, DWH=0; 569 — оба>0 но разные). Основная разница: DWH ETL иначе рассчитывает ppr |

### 5. `ll` — назначенный ресурс Life Limit (минуты)

| Этап | Что происходит |
|------|----------------|
| **AMOS raw** | Цепочка из 8 таблиц: `wo_transfer_dimension.due_at` → `treq_interval.amount_interval * 60` через work orders |
| **query2.txt** | Строки 20, 34, 60-88: `max(LL)` из work order chain. Строки 117-167: fallback `req_ll` (применимость по PSN из АПН 2773). Строки 195-222: fallback `req_ll2` (типовой по partno+ac_type). Итог: `coalesce(LL, req_ll.interval, req_ll2.interval)` |
| **DWH reports** | `LL` Nullable(Int64) — предрассчитано ETL DWH |
| **Excel** | Колонка `ll` |
| **dual_loader.py** | `pd.to_numeric().clip(≥0).fillna(0).astype(int64)` |
| **Enrichment** | Не модифицируется |
| **Симуляция** | `int(row[7] or 0)` → UInt32 |
| **Спрямление** | ✅ `clip(≥0).fillna(0)` из DWH. Расхождение с golden: **96.2%** match (435 различий). Причина: разные fallback-цепочки в DWH ETL vs query2.txt |

### 6. `mfg_date` — дата изготовления

| Этап | Что происходит |
|------|----------------|
| **AMOS raw** | `rotables.mfg_date` — int, дни от 1971-12-31 |
| **query2.txt** | `CASE WHEN mfg_unknown='Y' THEN 'unknown' ELSE to_char(date '1971-12-31' + r.mfg_date, 'DD.MM.YYYY') END` |
| **DWH reports** | `mfg_date` String (формат 'DD.MM.YYYY' или 'unknown') |
| **Excel** | Колонка `mfg_date` (Date) |
| **dual_loader.py** | `pd.to_datetime(dayfirst=True).dt.date`. None для невалидных |
| **Enrichment** | Не модифицируется |
| **Симуляция** | `row[9] if row[9] else None` → Python date |
| **Спрямление** | ✅ Парсить строку DWH ('DD.MM.YYYY' → Date). 'unknown' → NULL. **100% совпадение с golden rule** |

### 7. `ac_type_mask` — битовая маска типа ВС

| Этап | Что происходит |
|------|----------------|
| **AMOS raw** | `ac_typ.ac_typ` — код типа ВС (МИ8, МИ8АМТ, МИ8МТВ, ...) |
| **query2.txt** | `CASE МИ8→'Ми-8Т', МИ8АМТ/МИ8МТВ→'Ми-17', ELSE→ac_typ END` (строки 6-8). Подмена для презентации |
| **DWH reports** | `ac_typ` LowCardinality(String) — **уже с подменой**: 'Ми-8Т', 'Ми-17' |
| **Excel** | Колонка `ac_typ` (String) — с подменой |
| **dual_loader.py** | Просто строка |
| **Enrichment** | `enrich_heli_pandas.py`: маппинг строки → UInt8 битовая маска. Ми-8Т→32, Ми-17→64, МИ8→32, МИ8АМТ→32, МИ8МТВ→32 |
| **Симуляция** | `int(row[10] or 0)` → UInt8 |
| **Спрямление** | ✅ Можно маппить DWH `ac_typ` напрямую → mask. Подмена МИ8→'Ми-8Т' не нужна модели — модель использует только mask. Маппинг: МИ8/МИ8АМТ/МИ8МТВ/Ми-8Т → 32, Ми-17/МИ171/171А2 → 64. **99.99% совпадение** (1 различие из 11480 — вероятно edge case в ac_typ) |

### 8. `aircraft_number` — бортовой номер ВС

| Этап | Что происходит |
|------|----------------|
| **AMOS raw** | `aircraft.ac_registr` (строка, номер без префикса) + `aircraft.ac_registr_prefix` ('RA-', 'OB-', ...) |
| **query2.txt** | `CASE WHEN ac_registr IS NOT NULL THEN concat(ac_registr_prefix, ac_registr) ELSE concat(store, station, location) END` (строки 17-18). Для ВС → 'RA-XXXXX', для склада → 'РЕМФОНД ТЮМ U/S' |
| **DWH reports** | `location` — содержит 'RA-XXXXX' или склад |
| **Excel** | Колонка `location` |
| **dual_loader.py** | Просто строка |
| **Enrichment** | Вычисляется: `regexp_replace(location, 'RA-', '')` → UInt32 (если location starts with 'RA-') |
| **Симуляция** | `int(row[1] or 0)` → UInt32 |
| **Спрямление** | ✅ Из DWH `location`, если starts with 'RA-' → extract number. **99.97% совпадение** (3 различия — edge cases) |

### 9. `group_by` — группа взаимозаменяемости

| Этап | Что происходит |
|------|----------------|
| **AMOS raw** | Нет в AMOS — это проектный справочник |
| **query2.txt** | Нет |
| **DWH reports** | Нет |
| **Excel** | Нет |
| **dual_loader.py** | `group_by = 0` |
| **Enrichment** | `heli_pandas_group_by_enricher.py`: JOIN `md_components ON partno_comp = partseqno_i` → `md_components.group_by` |
| **Симуляция** | `int(row[3] or 0)` → UInt8. Используется для фильтрации (group_by >= 3) и группировки агентов |
| **Спрямление** | ⚠️ Требует JOIN с локальным `md_components`. Нельзя взять из DWH. **100% совпадение** после enrichment |

### 10. `status_id` — статус компонента

| Этап | Что происходит |
|------|----------------|
| **AMOS raw** | Нет единого поля. Вычисляется из: condition, location, target_date, sne, br |
| **query2.txt** | Нет |
| **DWH reports** | Нет |
| **Excel** | Нет |
| **dual_loader.py** | `status_id = 0` |
| **Enrichment** | Цепочка из 6 скриптов: |
| | 1. `heli_pandas_component_status.py`: group_by>2 + aircraft_number>0 + condition='ИСПРАВНЫЙ' + плнр status=2 → **status_id=2** (эксплуатация) |
| | 2. `heli_pandas_serviceable_status.py`: group_by>2 + condition='ИСПРАВНЫЙ' + status_id=0 → **status_id=3** (исправен на складе) |
| | 3. `heli_pandas_repair_status.py`: target_date < version_date → **status_id=2** (ремонт завершён); target_date >= version_date → **status_id=4** (в ремонте) |
| | 4. `heli_pandas_storage_status.py`: sne >= br_effective → **status_id=6** (beyond repair); ДОНОР/ВОЗМ.ПРОДЛЕНИЕ НР → **6**; НЕИСПРАВНЫЙ → **status_id=7** (unserviceable) |
| | 5. `heli_pandas_terminal_br_gate.py`: status_id IN (1,7) + sne >= br → **status_id=6** (terminal) |
| **Симуляция** | `int(row[4] or 0)` → UInt8 |
| **Входные данные для расчёта** | `condition` (DWH), `target_date` (DWH), `aircraft_number` (DWH→вычисл.), `sne` (DWH), `br_mi8/br_mi17` (md_components) |
| **Спрямление** | ⚠️ Можно воспроизвести SQL-логику на DWH-данных. Зависит от md_components (br). **92.5% совпадение** с golden (основная разница — 85 планеров с status_id=1) |

### 11. `repair_days` — остаток дней до окончания ремонта

| Этап | Что происходит |
|------|----------------|
| **AMOS raw** | `od_detail.target_date` (через history vm='SX') — плановая дата окончания ремонта |
| **query2.txt** | `CASE WHEN l.location = 'EXTERNAL' THEN to_char(date '1971-12-31' + od.target_date, 'DD.MM.YYYY') END` (строка 28) |
| **DWH reports** | `target_date` Nullable(Date32) |
| **Excel** | Колонка `target_date` (Date) |
| **dual_loader.py** | `pd.to_datetime().dt.date` |
| **Enrichment** | `repair_status.py`: `repair_days = max(0, repair_time - (target_date - version_date))`. repair_time из `md_components` через JOIN partseqno_i=partno_comp |
| **Симуляция** | `int(row[8] or 0)` → UInt16 |
| **Спрямление** | ⚠️ target_date из DWH + repair_time из md_components. Формула простая, можно вычислить в SQL |

---

## Приоритеты спрямления

### Группа A: Прямое чтение из DWH (без потерь)
1. **psn** — 100% match
2. **partseqno_i** — 100% match
3. **mfg_date** — 100% match (парсинг строки)

### Группа B: Минимальная трансформация из DWH
4. **aircraft_number** — из location, 99.97% match
5. **ac_type_mask** — маппинг ac_typ→mask, 99.99% match
6. **sne** — clip(≥0), 90.7% match (разные моменты снятия)
7. **ll** — clip(≥0), 96.2% match (разные fallback)
8. **ppr** — clip(≥0), 77.4% match (разный расчёт overhaul)

### Группа C: Требуют локального справочника
9. **group_by** — JOIN md_components, 100% match

### Группа D: Вычисляемые поля (enrichment логика)
10. **status_id** — SQL-логика на DWH-данных + md_components, 92.5% match
11. **repair_days** — формула + md_components, зависит от status_id

---

## Таблица сравнения golden rule vs DWH (2026-04-08)

| Поле | Golden rows | DWH rows | Match | Match% | Природа расхождений |
|------|------------|----------|-------|--------|---------------------|
| psn | 11480 | 11480 | 11480 | 100.0% | — |
| partseqno_i | 11480 | 11480 | 11480 | 100.0% | — |
| mfg_date | 11480 | 11480 | 11480 | 100.0% | — |
| aircraft_number | 11480 | 11480 | 11477 | 99.97% | 3 edge cases |
| ac_type_mask | 11480 | 11480 | 11479 | 99.99% | 1 edge case |
| group_by | 11480 | 11480 | 11480 | 100.0% | — |
| ll | 11480 | 11480 | 11045 | 96.2% | Разные fallback-цепочки |
| sne | 11480 | 11480 | 10410 | 90.7% | Разный момент снятия |
| ppr | 11480 | 11480 | 8882 | 77.4% | Разный расчёт overhaul |
| status_id | 11480 | 11480 | 10621 | 92.5% | 85 планеров initial + каскад |
| repair_days | 11480 | 11480 | — | — | Зависит от status_id |
