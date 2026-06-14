# DWH / heli_pandas: сводная аналитика входа–выхода агрегатов

> **Статус:** наработка для регрессионной проверки загрузок (ветка `feature/dwh-bb8`).  
> **Инструмент:** `code/utils/dwh_aggregate_churn_export.py`  
> **Scope:** агрегаты `group_by > 2`, фильтр partno из `md_components` (71 номенклатура на `2026-06-11 v1`).  
> **Ключ сравнения:** `(psn, partno)`.

## Быстрая проверка (количественно)

| Период | Источник | Baseline → New | Scope (keys) | Exited | Entered | Net | location_changed* |
|---|---|---|---:|---:|---:|---:|---:|
| 2 мес. | `heli_pandas` | 2026-04-08 → 2026-06-12 | ~11.2k | **64** | **108** | **+44** | — |
| 1 нед. | `dwh` reports | 2026-06-05 → 2026-06-12 | 13 467 → 13 480 | **3** | **16** | **+13** | 317 |
| 1 сут. | `dwh` reports | 2026-06-11 → 2026-06-12 | 13 480 → 13 480 | **0** | **0** | **0** | 13 |

\* `location_changed` — ключ сохранён, изменился `location` (суточный/недельный операционный сигнал; не churn).

**Вывод для короткой приёмки:**
- Длинный горизонт (2 мес.): заметный приток молодых агрегатов (entered median mfg **2025**), отток смещён в **gb=38** и старые mfg (exited median **2017**).
- Неделя 05→12: умеренный churn ключей (+13 net), основная активность — перестановки (317), не вход/выход.
- Сутки 11→12: churn ключей **нулевой**; только location.

## Структура по group_by

### 2026-04-08 → 2026-06-12 (`heli_pandas`)

| group_by | partno (образец) | exited | entered |
|---:|---|---:|---:|
| 6 | 8АТ-2710-00 | 5 | **40** |
| 38 | АК-50Т1 | **39** | 3 |
| 41 | СВ-78БА | 6 | 8 |
| 39 | АРМ-406П | 1 | 8 |
| 21 | 246-3925-00 | — | 13 |
| 36 | АГБ-96Д | 1 | 6 |
| 42 | НР-3ВМ | 3 | 5 |
| прочие | … | 9 | 25 |

### 2026-06-05 → 2026-06-12 (`dwh`)

| group_by | partno (образец) | exited | entered |
|---:|---|---:|---:|
| 36 | АГБ-96Д | — | **6** |
| 38 | АК-50Т1 | — | 5 |
| 7 | 246-1517-000 | 1 | — |
| 19 | 246-3904 | 1 | — |
| 22 | КАУ-30Б | 1 | — |
| прочие | … | — | 5 |

## Диапазоны дат производства (mfg)

| Период | Сторона | min–max mfg | median | <1990 | 1990–99 | 2000–09 | 2010–19 | 2020+ |
|---|---|---|---:|---:|---:|---:|---:|---:|
| 08.04→12.06 | exited | 1979–2025 | 2017 | 8 | 3 | 5 | 22 | **26** |
| 08.04→12.06 | entered | 1971–2026 | **2025** | 17 | 7 | 4 | 3 | **77** |
| 05.06→12.06 | exited | 1994–2004 | 2002 | 0 | 1 | 2 | 0 | 0 |
| 05.06→12.06 | entered | 1987–2026 | 2022 | 2 | 0 | 1 | 2 | **11** |

**Паттерн:** entered на длинном горизонте — преимущественно **2020+** (71% entered); exited — bimodal (старые gb=38 + молодые 2020+). На неделе exited — только **2000-е**, entered — в основном **2020+** и свежие АГБ mfg 2026-05.

## Артефакты

| Файл | Описание |
|---|---|
| `output/aggregate_churn_2026-04-08_vs_2026-06-12.xlsx` | Baseline Excel-era vs DWH load (heli_pandas) |
| `output/aggregate_churn_2026-06-05_vs_2026-06-12.xlsx` | Неделя DWH |
| `output/aggregate_churn_2026-06-11_vs_2026-06-12.xlsx` | Сутки DWH |

Листы Excel: `meta`, `entered`, `exited`, `location_changed`, `planner_agg_*`.

## Запуск

```bash
# heli_pandas (два version_date в Project CH)
python3 code/utils/dwh_aggregate_churn_export.py \
  --source heli_pandas --baseline-date 2026-04-08 --new-date 2026-06-12 \
  --out output/aggregate_churn_2026-04-08_vs_2026-06-12.xlsx

# DWH snapshots (reports.amos_heli_rotables_components_status)
python3 code/utils/dwh_aggregate_churn_export.py \
  --source dwh --baseline-date 2026-06-05 --new-date 2026-06-12 \
  --out output/aggregate_churn_2026-06-05_vs_2026-06-12.xlsx
```

## Отложенная аналитика (ценные наработки)

Следующие срезы **не входят** в короткую количественную проверку, но подготовлены для возврата:

1. **location_changed (317 за неделю):** ~65% чисто склад (TRANSFER→ИСПРАВНЫЕ, U/S→КОНСЕРВА); ~35% с RA-.
2. **Кластеры бортов (`planner_agg_moved`):** RA-22517 (−10 разбор), RA-22428 (+5 сборка 8АТ-2710), RA-22435/22571 (ротация с TREE).
3. **TREE → EXTERNAL (7 шт.):** НР/СВ gb 41–42 — вывод из учётного дерева AMOS, отдельный от warehouse-churn сигнал.
4. **DWH history по ключам:** mfg, dwh_entry/exit, ever_on_planner, board_periods — колонки в детальных листах.
5. **Ограничение:** `heli_pandas` для 2026-06-11 не загружен (loader schema 33 vs 26); суточное сравнение — только DWH reports.

**Связанные документы:** `docs/de_tasks.md`, `docs/field_traceability_amos_to_sim.md`, `docs/changelog.md` (2026-06-13).

**Sim-gate (обязателен для приёмки DWH-среза):** после load — `orchestrator_limiter_v8.py` + `code/validation/run_all.py` → INV-1…INV-12 PASS. Extract-only churn **не заменяет** sim validation.
