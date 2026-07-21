# Project Backlog (SSoT идей без немедленной реализации)

**Файл-якорь:** [docs/backlog.md](backlog.md)  
**Упоминание в README:** [README.md § Документация](../README.md)

> Это **канонический backlog проекта Helicomponents**. Файл создан раньше (см. changelog / README) как заготовка; до 2026-07-20 почти не наполнялся. С этой даты backlog ведётся **системно**: индекс + детальные карточки с алгоритмами/кодом, кликабельными путями и handoff для следующего агента.

---

## Зачем нужен backlog

| Не сюда | Сюда |
| --- | --- |
| Активный workflow Agent KG (`config/agent_kg.json`) | Идеи/долги **после** закрытия исследования, без немедленного dispatch |
| Hotfix текущего спринта | Отложенная визуализация, DQ-мониторинг, UX «подумаю позже» |
| SSoT домена (`config/transitions/**`) | Спеки «как детектить» для будущего BI/ETL без изменения симулятора |

**Связанные контуры (не дублировать смысл, а ссылаться):**

| Контур | Путь | Роль |
| --- | --- | --- |
| Agent KG triage | workflow `W_triage_queue` через `code/utils/agent_kg.py --write-context --context-type triage` | машинная очередь follow-up |
| Singularity (личное) | проект «Ресурсы ВС» | напоминание человеку |
| Changelog | [docs/changelog.md](changelog.md) | что уже **сделано** |
| DE-задачи DWH | [docs/de_tasks.md](de_tasks.md) | внешний analytics/DWH backlog |

---

## Правила ведения (обязательные)

1. **Один индекс + карточка.** Строка в таблице «Записи» + секция `## YYYY-MM-DD — …` с деталями.
2. **Кликабельные пути** в markdown-ссылках на файлы репо (относительные от `docs/`).
3. **Алгоритм + код.** Для задач, которые будет реализовывать другой агент: входы, SQL/предикаты, функции, команды прогона, артефакты, DoD, явные «не трогать».
4. **Не маскировать DQ в ETL** без отдельного human-gate: backlog фиксирует *видимость* дефекта, не «костыль в extract».
5. **Владелец + фаза.** Пока фаза «отложено» — не открывать high-risk workflow без новой команды человека.
6. **При закрытии пункта:** строка индекса → статус Done + ссылка на changelog; карточку не удалять (история).

### Формат строки индекса

| Поле | Смысл |
| --- | --- |
| Дата | YYYY-MM-DD заведения |
| Название | короткое, уникальное |
| Фаза | precondition («после UX дашбордов») |
| Контекст | 1–2 строки + ссылка на карточку |
| Следующий шаг | конкретный |
| Владелец | человек |

---

## Записи (индекс)

| Дата | Название | Фаза | Контекст | Следующий шаг | Владелец |
| --- | --- | --- | --- | --- | --- |
| 2026-07-21 | [Воронка классификации планеров day0](#2026-07-21--воронка-классификации-планеров-day0-dwh--heli_pandas) | Done (extract) | Полная воронка + calendar/program гейты; demote-only fallback 10y; прогоны 19/20.07 | age40 — отдельно | Алексей |
| 2026-07-20 | [AMOS DQ: WP + задержка установки](#2026-07-20--amos-dq-wp-не-закрыт--задержка-установки-агрегатов) | После выбора UX ежедневных дашбордов | Два дефекта дисциплины AMOS; day0 2026-07-12 v1; Singularity `T-f5440b35…` | Спека виджетов A/B/C по карточке ниже | Алексей |
| 2026-02-01 | Дообогащение СУБД между adaptive днями | После стабилизации adaptive days | Заполнение интервалов между adaptive днями | Оценить объём / MP2 | — |

---

## 2026-07-21 — Воронка классификации планеров day0 (DWH → heli_pandas)

**Статус:** implemented + documented (согласовано 2026-07-22).  
**Канон воронки:** этот раздел. Код: [`planer_calendar_remain.py`](../code/extract/planer_calendar_remain.py), cascade в [`dwh_post_enrichment.py`](../code/utils/dwh_post_enrichment.py) / [`dual_loader.py`](../code/extract/dual_loader.py), demote [`day0_ops_deficit_demote_runner.py`](../code/extract/day0_ops_deficit_demote_runner.py).  
**UI-якорь AMOS (календарь OH):** Actual Status → `Last Overhaul Date`; Requirements → `OH (OVERHAUL)` dim=`D`.

### Полная воронка планеров (`group_by ∈ {1,2}`)

Порядок **жёсткий**. Статусы: `0` сырой → `4` ремонт → `2` OPS → `1` inactive → `3` serviceable / `6` storage / `7` unserviceable.

```
DWH load (heli_pandas status_id=0)
  │
  ├─[1] overhaul_status_processor        status_overhaul активен → 4 (+ repair_days/time)
  ├─[2] program_ac_status_processor      в program_ac as-of day0 и ещё 0 → 2 (OPS)
  │                                       (4 не перезаписывается)
  ├─[3] inactive_planery_processor       остаток планеров (partno planer) ещё 0 → 1 (OOR)
  ├─[3b] inactive_serviceable_classifier ТОЛЬКО status=1 (OOR): destination gates
  │                                       календарь = только treq OH(D); fallback 10y ВЫКЛ
  │
  ├─ post: program_ac_precheck_d1        ТОЛЬКО status=2: хватает ли oh/ll на 1-й день MP5
  │                                       rem_oh|rem_ll < dt → 6 или 7 (часы, не календарь)
  ├─ post: component / serviceable / repair / storage / terminal_br …
  │
  └─[после flight_program_*] day0 demote ТОЛЬКО excess OPS (status=2) vs MP4:
        rank by aggregate deficit → top excess
        destination gates (+ fallback 10y если нет treq и hist с 2025-07-04)
```

| Шаг | Вход | Выход планера | Часы OH? | Календарь OH(D)? |
| --- | --- | --- | --- | --- |
| 1 overhaul | сырой 0 | 4 | нет (статус из ремонта) | нет |
| 2 program_ac | 0, в реестре day0 | 2 | нет | нет |
| 3 inactive | хвост планеров 0 | 1 | нет | нет |
| 3b OOR classify | **только 1** | 1 или 3 (+agg 3\|7) | нет (раньше по воронке) | **treq only** |
| precheck D1 | **только 2** | 2 / 6 / 7 | **да** (`oh−ppr`, `ll−sne` vs dt) | нет |
| demote | **только 2** excess | 3 или 1 (+agg) | нет (уже в OPS) | treq **или** fallback 10y |

**Часы не дублируются** в 3b/demote: OPS уже прошёл precheck; OOR/demote решают destination по **программе + календарю**.

### Два входа destination (demote vs 3b)

`program_ac` — SCD **as-of** «числится в эксплуатации» на дату (не накопитель).  
`flight_program_ac.ops_counter_*` — **план** OPS на день (MP4).

| Контур | Вход | Смысл |
| --- | --- | --- |
| **demote** | `status=2`, top excess vs MP4 | roster AMOS шире plan → excess уводим из OPS |
| **3b** | `status=1` после шагов 1–3 | OOR-хвост: кто ещё serviceable-запас |

Пересечение входов в одном цикле: `∅`. Общая функция: `destination_for_remain` (порядок **программа → календарь**).

### Destination gates (симметрия Mi-8 / Mi-17)

`destination_for_remain(remain_d, in_program_history)`:

1. **нет** в `program_ac` с **2025-07-04** → planer **1**, agg **7** (`not_in_program`; календарь не смотрим)
2. в программе + `remain_d > 0` → planer **3**, agg **3** (`in_program_calendar_positive`)
3. в программе + нет положительного календаря → planer **1**, agg **3** (`in_program_no_calendar`)

### Календарный OH(D)

```
vitriina oh_at/mfg → forecast OH% → treq (threshold=N, dim=I, counter_defno_i=3)
int_days = raw*365 if raw<100 else raw
base = mfg if oh_at≤1972-01-01 else oh_at
due = base + int_days;  remain_d = due − day0
```

**Fallback (только demote):** нет treq OH(D) **и** serial ∈ history с 2025-07-04  
→ `due = base + 10 calendar years − 1 day` (inclusive 10y; как директор для 27130).  
**3b:** `fallback_10y_psns=None` — без treq → `remain_d=None` → не 3 по календарю.

Открытый gap: age40 (`mfg+40y`) не в гейтах.

### Приёмка (одинаковые правила)

| срез | demote excess | serviceable | 3b→3 | evidence |
| --- | --- | --- | --- | --- |
| 2026-07-19 v1 | 12 (10+2) | **12** (все demote; 5 treq + 7 fb) | 0 | `output/day0_ops_deficit_demote_2026-07-19_v1_demote_fallback/` |
| 2026-07-20 v1 | 11 (10+1) | **11** (все demote; 4 treq + 7 fb) | 0 | `output/day0_ops_deficit_demote_2026-07-20_v1_demote_fallback/` |

Diff 19→20 — **только исходные данные**, не дрейф алгоритма: **22491** вышел из `program_ac` (−1 Mi-17 OPS → не demotят 22417); у **24500** сняли 2×АГБ → вытеснил **24246** из Mi-8 demote top-10.

### Ссылки

| Что | Путь |
| --- | --- |
| Shared gates + calendar | [code/extract/planer_calendar_remain.py](../code/extract/planer_calendar_remain.py) |
| 3b OOR | [code/extract/inactive_serviceable_classifier.py](../code/extract/inactive_serviceable_classifier.py) |
| Demote apply | [code/extract/deficit_demoter.py](../code/extract/deficit_demoter.py) |
| Demote CLI | [code/extract/day0_ops_deficit_demote_runner.py](../code/extract/day0_ops_deficit_demote_runner.py) |
| Cascade entry | [code/utils/dwh_post_enrichment.py](../code/utils/dwh_post_enrichment.py) |
| Precheck часы OPS | [code/extract/program_ac_precheck_next_day.py](../code/extract/program_ac_precheck_next_day.py) |
| Runbook | [docs/runbook_sim_launch.md](runbook_sim_launch.md) §1 + § day0 demote gates |
| Capsule ETL | [docs/etl_extract_capsule.md](etl_extract_capsule.md) |

---

## 2026-07-20 — AMOS DQ: WP не закрыт + задержка установки агрегатов

**Статус:** deferred (дашборды начаты, UX отложен — не реализовывать без новой команды).  
**Срез-доказательство:** `heli_pandas` + `sim_masterv2_v9`, `version_date=2026-07-12`, `version_id=1`.  
**Handoff для BI-агента:** эта карточка = спецификация детекции; визуализация — `deploy/bi-as-code/**` + skill [bi-superset-api](../.cursor/skills/bi-superset-api/SKILL.md).

### Ссылки (открывать отсюда)

| Что | Ссылка |
| --- | --- |
| Этот backlog | [docs/backlog.md](backlog.md) |
| Excel дефицита OPS day0 | [output/engine_completeness_2026-07-12/OPS_Aggregate_Deficit_day0_2026-07-12_v1.xlsx](../output/engine_completeness_2026-07-12/OPS_Aggregate_Deficit_day0_2026-07-12_v1.xlsx) |
| Алгоритм комплектности (док) | [docs/architecture/completeness_check.md](architecture/completeness_check.md) |
| Ядро дефицита по группам | [code/heli_pandas_ops_other_groups.py](../code/heli_pandas_ops_other_groups.py) |
| Day0 обёртка + Excel | [code/analysis/ops_aggregate_completeness_day0.py](../code/analysis/ops_aggregate_completeness_day0.py) |
| DWH-native / BI materialize | [code/analysis/ops_aggregate_completeness_dwh.py](../code/analysis/ops_aggregate_completeness_dwh.py) |
| Force-exit past target → OPS | [code/extract/heli_pandas_repair_status.py](../code/extract/heli_pandas_repair_status.py) (`update_past_to_operations`) |
| Overhaul → repair cascade | [code/extract/overhaul_status_processor.py](../code/extract/overhaul_status_processor.py) |
| Post-enrich orchestration | [code/utils/dwh_post_enrichment.py](../code/utils/dwh_post_enrichment.py) |
| Runbook extract/sim | [docs/runbook_sim_launch.md](runbook_sim_launch.md) |
| Уже начатый BI completeness | [deploy/bi-as-code/scripts/build_ops_completeness_dashboard.py](../deploy/bi-as-code/scripts/build_ops_completeness_dashboard.py) |
| Agent KG triage | `W_triage_queue` / context `AMOS-DQ-20260720` |
| Singularity | проект «Ресурсы ВС», task id `T-f5440b35-2ff6-4f48-a637-0b13d2a320a5` |

### Команды воспроизведения (read-only / analysis)

```bash
cd /media/DATA_BIG/Projects/Heli/Helicomponents
set -a && source .env && set +a

# Дефект 2 — комплектность OPS day0 (канон)
.venv/bin/python code/analysis/ops_aggregate_completeness_day0.py \
  --version-date 2026-07-12 --version-id 1

# Дефект 1 — SQL-сигнал (пример предикат для витрины)
# см. блок «Алгоритм детекции дефекта 1» ниже
```

---

### Дефект 1 — Ремонт «закрыт по дате», WP не закрыт

#### Корневая причина (процесс AMOS)

Плановая/целевая дата окончания ремонта уходит в прошлое, work pack в overhaul остаётся **«В процессе»** (`act_end_date IS NULL`). Дисциплина требует закрытия WP вместе с завершением ремонта.

#### Пайплайн extract (где сигнал рождается)

```
DWH status_overhaul + Status_Components
        ↓ dwh_loader / dual_loader
heli_pandas (сырой status_id)
        ↓ overhaul_status_processor  → открытый WP ⇒ кандидат status_id=4
        ↓ heli_pandas_repair_status.update_past_to_operations
          если target_date < version_date ⇒ ПРИНУДИТЕЛЬНО status_id=2 (OPS)
          даже при открытом WP
```

Ключевой код force-exit планеров:

```157:167:code/extract/heli_pandas_repair_status.py
    # Затем ПРИНУДИТЕЛЬНО обрабатываем планеры (status_id любой, т.к. ремонт завершён)
    query_planers = """
    ALTER TABLE heli_pandas
    UPDATE status_id = 2
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND toUInt32(ifNull(group_by, 0)) IN (1, 2)
      AND target_date IS NOT NULL
      AND target_date != toDate('1970-01-01')
      AND target_date < %(version_date)s
    """
```

Это **намеренная** доменная реакция extract на «дата в прошлом», не баг симулятора. Дефект данных AMOS становится видимым именно как противоречие WP vs статус планера.

#### Алгоритм детекции (для ежедневной витрины / дашборда)

**Входы:** `heli_pandas`, `status_overhaul`, опционально `program_ac` (тот же `version_date` / `version_id`).

**Предикат «ложный OPS из-за незакрытого WP»** (логика, не обязательно один SQL):

1. Планер: `heli_pandas.group_by IN (1,2)` AND `status_id = 2`.
2. `target_date IS NOT NULL` AND `target_date > '1971-01-01'` AND `target_date < version_date`.
3. Существует строка `status_overhaul` для того же борта (`ac_registr` ↔ `aircraft_number`) и среза: `status` ≈ «В процессе» AND `act_end_date IS NULL` (и обычно актуальный КР по `sched_end` / description).
4. Флаги обогащения: `in_program_ac` (есть/нет в `program_ac`); `deficit_flag` из джойна с дефектом 2.

**Срез-доказательство 2026-07-12 v1:** борта **22419**, **22497**, **22428**.  
22497 — вне `program_ac`; 22419/22497 также в дефиците комплектности.

#### Как видно в симуляции

- Старт агента: `status_id=2` (OPS), не `4` → **не** занимает repairline.
- Участвует в квоте OPS vs `flight_program_ac.ops_counter_*` ([INV-2](../code/validation/inv2_ops_vs_target.py)), налёте, демоуте.
- Эффект: завышенный day0 OPS до демоута; «мёртвый» с точки зрения AMOS-ремонта борт выглядит лётным.

**Не делать в ETL:** не отключать force-exit молча; сначала мониторинг/дашборд.

---

### Дефект 2 — Задержка отражения установки агрегатов (> ~2 недель)

#### Корневая причина (процесс AMOS)

Физическая установка агрегата на борт не отражена вовремя в `Status_Components` (location / привязка) → в day0-снимке нет актуальной связки планер↔агрегаты.

#### Пайплайн детекции (код)

```
heli_pandas OPS planers (status_id=2, group_by∈{1,2})
        ↓ fetch_plane_meta / fetch_group_counts
md_components norms (comp_number, ac_type_mask)
        ↓ heli_pandas_ops_other_groups.fetch_aggregations
shortage_groups: "gb:installed/required" (+ альтернативы Mi-17)
        ↓ ops_aggregate_completeness_day0.parse_shortage
deficit_units = sum(max(0, required - installed))
Excel: all_ops_boards / deficit_by_board / deficit_detail / deficit_by_nomenclature
```

Ядро: [fetch_aggregations](../code/heli_pandas_ops_other_groups.py) — константы альтернатив/опционала в шапке файла:

- `OPTIONAL_GROUPS = {32,33,34}`
- `MI17_OR_GROUPS = {14,15,16}`, `MI17_ALT_GROUPS = {22,23,24}`, `MI17_AGB_GROUPS = {35,36,37}`, `MI17_ENGINE_GROUPS = {28,29,30}`
- Shortage только при `installed < required` (лишние по `ALLOW_EXTRA_GROUPS` не в дефицит единиц в day0-обёртке)

Обёртка day0: [ops_aggregate_completeness_day0.py](../code/analysis/ops_aggregate_completeness_day0.py) — парсит shortage → номенклатура из `md_components`.

Для BI уже есть путь материализации: [ops_aggregate_completeness_dwh.py](../code/analysis/ops_aggregate_completeness_dwh.py) → таблицы `bi_ops_completeness_board` / `_detail` + [build_ops_completeness_dashboard.py](../deploy/bi-as-code/scripts/build_ops_completeness_dashboard.py).

#### Алгоритм детекции (для ежедневной витрины)

1. Скоуп планеров: OPS day0 (`status_id=2`, `group_by IN (1,2)`).
2. Для каждого борта — тот же расчёт, что `fetch_aggregations` (не упрощать до «COUNT group_by» без альтернатив).
3. `has_deficit ⇔ deficit_units > 0`.
4. SLA (продуктовое): флаг «подозрение на задержку отражения», если дефицит стабилен N дней подряд (N≥14 — согласовать) **или** ручная метка; сам факт дефицита на одном срезе уже сигнал.
5. Тяжёлые кейсы: нет двигателей (ТВ2/ТВ3 норма 2) при OPS; `installed_total` ≪ `required`.

**Срез-доказательство 2026-07-12 v1:** 13 / 168 OPS, 131 шт.; тяжёлые **22419** (0/35), **24107**, **22425**. Топ номенклатура: `8АТ.2710.000`.  
Пересечение с дефектом 1: **22419**, **22497**.

#### Как видно в симуляции

- RTC **не** проверяет day0 fullkit для уже существующих OPS.
- Co-spawn BOM ([planer_spawn_bom.py](../code/sim_v2/components/planer_spawn_bom.py) / L2 INV-0c) — только на **рождениях**, не чинит day0-deficit boards.
- Демоут квоты (младший `idx`, excess OPS) **не** связан с комплектностью: на day1 из 13 дефицитных **0** выдавлены из OPS.

**Не делать в ETL:** не «дорисовывать» агрегаты; не снимать OPS по комплектности без отдельного архитектурного решения.

---

### Отложенные виджеты дашборда (handoff BI-агенту)

| ID | Название | Источник / алгоритм | Примечание |
| --- | --- | --- | --- |
| **A** | WP open ∧ past end ∧ планер OPS | Предикат дефекта 1; join `heli_pandas`↔`status_overhaul` | Ежедневный срез по `report_date` / `version_date` |
| **B** | OPS incompleteness | Переиспользовать `fetch_aggregations` или `bi_ops_completeness_*` | + breakdown по номенклатуре; опционально SLA>14d |
| **C** | Пересечение A∩B | Inner join ключей acn | Приоритетный список «ложный OPS + пустой/дырявый борт» |

**Уже начато:** [build_ops_completeness_dashboard.py](../deploy/bi-as-code/scripts/build_ops_completeness_dashboard.py) — развивать, не плодить третий параллельный движок дефицита.

**DoD при возврате к реализации:**

1. Ежедневная витрина (CH) + personal Superset sandbox, API-first ([bi-superset-api skill](../.cursor/skills/bi-superset-api/SKILL.md)).
2. Эквивалентность логики дефицита с `heli_pandas_ops_other_groups` (есть `--validate-against` в DWH-скрипте).
3. Дефекты **fail-visible**; без маскировки в extract/sim.
4. Запись в [changelog](changelog.md) + закрытие строки индекса backlog.

**Вне scope без новой команды:** правка RTC, отключение force-exit, corporate/prod BI apply.
