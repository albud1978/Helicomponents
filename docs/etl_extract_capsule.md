# Context Capsule — etl_extract

## Scope
Трогаем:
- `code/extract/extract_master.py` (оркестратор 18 стадий ETL)
- `code/extract/*.py` (загрузчики, обогатители, воронка планеров day0)
- `code/utils/dwh_loader.py`, `code/utils/dwh_post_enrichment.py` (DWH path + cascade)
- `config/database_config.yaml`, `code/utils/config_loader.py`, `code/utils/etl_version_manager.py`, `code/utils/version_utils.py`

Не трогаем:
- Симуляцию (`code/sim_v2/`) → `docs/limiter_v8_capsule.md`, `docs/transitions_capsule.md`, `docs/quota_capsule.md`
- Финальную post-sim валидацию → `docs/validation_capsule.md`

## Invariants (≤12)

SSoT доменных инвариантов: `config/transitions/invariants.json`

- **INV-2** (`invariants.json`): ETL day0 demote выравнивает `status_id=2` (OPS) планеров `group_by∈{1,2}` под `flight_program_ac.ops_counter_*` на day0; вход sim должен удовлетворять `abs(ops_count − mp4_target[day0]) ≤ tolerance` (validator: `code/validation/inv2_ops_vs_target.py`). Источник воронки: `docs/backlog.md` §2026-07-21.
- **INV-12** (`invariants.json`): для планеров `group_by∈{1,2}` всегда `ppr ≤ oh`; precheck D1 на OPS использует `oh−ppr` vs `dt` (validator: `code/validation/inv12_ppr_le_oh.py`).
- **ETL-версионирование**: каждый прогон создаёт уникальный `version_date`/`version_id`; срез иммутабелен после записи (`code/utils/etl_version_manager.py`).
- **ETL-последовательность**: 18 стадий Excel-path / DWH enrich выполняются строго по порядку; пропуск стадии = fail-fast.
- **ETL-типы**: ресурсные поля (`sne`, `ppr`, `ll`, `oh`) → `UInt32`; `repair_days` → `UInt16`; `Float64` запрещён без согласования (`.cursor/rules/00_global_always.mdc`).
- **ETL-источники**: только реальные Excel + ClickHouse/DWH; синтетика запрещена без явного разрешения.
- **Раздельные входы demote vs 3b**: demote — только `status=2` excess OPS; 3b — только `status=1` OOR; пересечение входов в одном цикле `∅` (`docs/backlog.md` §2026-07-21).
- **Fallback +10y−1d**: только demote (+ hist с 2025-07-04); 3b — treq OH(D) only, `fallback_10y_psns=None` (`planer_calendar_remain.py`).

## Decisions (≤7)

1. **18-стадийный sequential pipeline (Excel path)** — MD → Status → Program → Dual → Enrich → Dictionaries → Tensors → Final; каждая стадия зависит от предыдущей (`extract_master.py`).
2. **DWH cascade планеров** — после load `heli_pandas status_id=0`: overhaul→program_ac→inactive→**3b** → post (precheck часов OPS, component/serviceable/repair/storage/BR). Entry: `dwh_post_enrichment.py` / `dual_loader.py`. Канон: `docs/architecture/extract.md` §Day0; приёмка: `docs/backlog.md` §2026-07-21.
3. **Destination gates (program→calendar)** — общий `destination_for_remain`: сначала hist `program_ac` ≥2025-07-04, затем календарный OH(D). Demote (`status=2` excess) и 3b (`status=1` OOR) — **разные входы**, одна функция гейтов. Код: `planer_calendar_remain.py`.
4. **Demote-only fallback +10y−1d** — если нет treq OH(D) **и** serial ∈ history с 2025-07-04 → `due = base + 10 calendar years − 1 day` (inclusive). **3b без fallback** — без treq → не 3 по календарю. Решение согласовано 2026-07-22 (`docs/backlog.md` §2026-07-21).
5. **Day0 OPS demote после MP4** — excess OPS vs `flight_program_ac` ранжируется по deficit комплектации; destination через те же гейты (+ demote-only fallback). Runner: `day0_ops_deficit_demote_runner.py` после `flight_program_*`; re-enrich сбрасывает статусы → demote заново (`docs/runbook_sim_launch.md` §1).
6. **Блок storage** — неисправные агрегаты без `target_date` → `status_id=7` (unserviceable), не 4; `heli_pandas_storage_status.py`.
7. **Native ClickHouse driver** (порт 9000) + **мультиверсионность** — `version_date`/`version_id`; day0 sim = `version_date` среза; DWH-path MP4/MP5 из Excel `v_2026-04-08` (runbook).

## Impact Paths

```
DWH path (канон runbook):
  program_ac + status_overhaul + Status_Components
    → heli_pandas (status_id=0)
    → cascade: overhaul→program_ac→inactive→3b(OOR gates)
    → post: precheck(D1 hours OPS) → component/serviceable/repair/storage/BR
    → flight_program_ac/fl (Excel v_2026-04-08)
    → day0 OPS deficit demote (destination gates + demote-only 10y fallback)
    → sim (читает ClickHouse по version_date/version_id)

Excel legacy path: data_input/v_*/*.xlsx → extract_master.py [1–18] → те же таблицы
```

- `data_input/source_data/v_*/*.xlsx` → loaders → ClickHouse (`heli_pandas`, `heli_raw`, `flight_program_*`, `program_ac`, `status_overhaul`, `md_components`, `dict_*`)
- `config/database_config.yaml` + `.env` → все ETL-скрипты
- `heli_pandas` + `flight_program_fl`/`flight_program_ac` → начальное состояние агентов sim; `version_date` → `preload_mp5_maps` / `preload_mp4_by_day`
- Ключевые таблицы (per-dataset): `heli_pandas` ~10.9–11.4k; `flight_program_fl` ~1.16M; `flight_program_ac` ~4k (`docs/backlog.md`, `.cursor/rules/10_extract_and_env.mdc`)

## Validation Proof

**Day0 воронка + demote (extract-phase):**
- **Runbook**: `docs/runbook_sim_launch.md` §1 — `dwh_loader.py --step all` → `program_*_direct_loader` → `day0_ops_deficit_demote_runner.py` (fail-fast без MP4).
- **Manual-check status_id**: после enrich+demote — counts планеров `group_by∈{1,2}` по `status_id` (0/1/2/3/4/6/7) на срезе; шаги 1–3b до demote, demote только на excess `status=2`.
- **Manual-check OPS==MP4**: после demote — `count(status_id=2)` по Mi-8/Mi-17 == `ops_counter_*` day0 в `flight_program_ac` (acceptance в `docs/backlog.md` §2026-07-21).
- **Evidence dirs** (прогоны 19–20.07.2026):
  - `output/day0_ops_deficit_demote_2026-07-19_v1_demote_fallback/` — 12 demote (10+2), serviceable 12, 3b→3: 0
  - `output/day0_ops_deficit_demote_2026-07-20_v1_demote_fallback/` — 11 demote (10+1), serviceable 11, 3b→3: 0
- **Комплектность (rank input demote)**: `code/analysis/ops_aggregate_completeness_day0.py` / `code/heli_pandas_ops_other_groups.py` — deficit ranking для top excess OPS.
- **Post-sim (optional)**: INV suite после sim — `docs/validation_capsule.md`; **INV-2** `code/validation/inv2_ops_vs_target.py`, **INV-12** `code/validation/inv12_ppr_le_oh.py` (SSoT: `config/transitions/invariants.json`).

## Risks (≤7) + Mitigations
- Некорректные Excel/DWH → ошибка начальных данных → precheck D1 + `validate_heli_pandas.py`
- Пропуск стадии / demote до MP4 → неконсистентные OPS → sequential pipeline + fail-fast в `day0_ops_deficit_demote_runner.py`
- Re-enrich без повторного demote → сброс статусов → runbook: demote заново после `--step enrich`
- Дубли `version_date` → смешение прогонов → `etl_version_manager`: уникальный срез per run
- Demote vs 3b пересечение входов → двойная классификация → disjoint inputs enforced в cascade (`docs/backlog.md`)
- Fallback 10y только demote → 3b без treq остаётся OOR → явный `fallback_10y_psns=None` в 3b path
- Storage reclass (status 4→7 для неисправных без target_date) → ретроспективные сверки учитывать `heli_pandas_storage_status.py`

## Open Questions (≤7)
- **age40** — отдельный контур (не входит в day0 воронку 2026-07-21); scope TBD (`docs/backlog.md` §2026-07-21).
- Миграция с Excel на API-источник данных?
- Автоматизация запуска ETL по расписанию (cron)?

*Settled (не open): demote-only fallback +10y−1d — согласовано 2026-07-22, см. Decisions §4.*

## Pointers (≤15)
- `docs/architecture/extract.md` §Day0 воронка (канон)
- `docs/backlog.md` §2026-07-21 (приёмка + evidence)
- `docs/runbook_sim_launch.md` §1 + § day0 demote gates
- `code/extract/planer_calendar_remain.py`
- `code/extract/inactive_serviceable_classifier.py`
- `code/extract/deficit_demoter.py` / `day0_ops_deficit_demote_runner.py`
- `code/extract/program_ac_precheck_next_day.py`
- `code/utils/dwh_loader.py` / `dwh_post_enrichment.py`
- `code/extract/dual_loader.py`
- `code/extract/overhaul_status_processor.py` / `program_ac_status_processor.py` / `inactive_planery_processor.py`
- `code/extract/heli_pandas_storage_status.py`
- `code/extract/extract_master.py`
- `config/transitions/invariants.json` (INV-2, INV-12)
- `config/database_config.yaml`
- `docs/validation_capsule.md`
