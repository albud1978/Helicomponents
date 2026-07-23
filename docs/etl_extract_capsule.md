# Context Capsule — etl_extract

## Operator entrypoint (читать первым)

SSoT команд: [`docs/runbook_sim_launch.md`](runbook_sim_launch.md) **§0**. Agent-правило: `.cursor/rules/10_extract_and_env.mdc`.

```bash
.venv/bin/python code/extract/extract_master.py \
  --source dwh --mode prod \
  --version-date YYYY-MM-DD --version-id 1 \
  --dataset-path data_input/source_data/v_YYYY-MM-DD
```

Одна команда = готово к sim (включая demote + OPS==MP4). **Не** собирать из leaf-скриптов. Excel/interactive — legacy, только по явной команде Алексея.

## Scope
Трогаем:
- `code/extract/extract_master.py` (единственный оркестратор Excel + DWH day0)
- `code/extract/*.py` (загрузчики, обогатители, воронка планеров day0)
- `code/utils/dwh_loader.py`, `code/utils/dwh_post_enrichment.py`, `code/utils/dwh_batch_sim_gate.py`
- `config/database_config.yaml`, `code/utils/config_loader.py`, `code/utils/etl_version_manager.py`, `code/utils/version_utils.py`

Не трогаем:
- Симуляцию (`code/sim_v2/`) → `docs/limiter_v8_capsule.md`, `docs/transitions_capsule.md`, `docs/quota_capsule.md`
- Финальную post-sim валидацию → `docs/validation_capsule.md`

## Invariants (≤12)

SSoT доменных инвариантов: `config/transitions/invariants.json`

- **INV-2** (`invariants.json`): ETL day0 demote выравнивает `status_id=2` (OPS) планеров `group_by∈{1,2}` под `flight_program_ac.ops_counter_*` на day0; вход sim должен удовлетворять `abs(ops_count − mp4_target[day0]) ≤ tolerance` (validator: `code/validation/inv2_ops_vs_target.py`). Источник воронки: `docs/backlog.md` §2026-07-21.
- **INV-12** (`invariants.json`): для планеров `group_by∈{1,2}` всегда `ppr ≤ oh`; precheck D1 на OPS использует `oh−ppr` vs `dt` (validator: `code/validation/inv12_ppr_le_oh.py`).
- **ETL-версионирование**: каждый прогон создаёт уникальный `version_date`/`version_id`; срез иммутабелен после записи (`code/utils/etl_version_manager.py`).
- **ETL-последовательность**: `extract_master.py` — единый orchestrator для Excel-path и DWH-path; стадии выполняются строго по порядку; пропуск стадии = fail-fast.
- **ETL-типы**: ресурсные поля (`sne`, `ppr`, `ll`, `oh`) → `UInt32`; `repair_days` → `UInt16`; `Float64` запрещён без согласования (`.cursor/rules/00_global_always.mdc`).
- **ETL-источники**: только реальные Excel + ClickHouse/DWH; синтетика запрещена без явного разрешения.
- **Раздельные входы demote vs 3b**: demote — только `status=2` excess OPS; 3b — только `status=1` OOR; пересечение входов в одном цикле `∅` (`docs/backlog.md` §2026-07-21).
- **Fallback +10y−1d**: только demote (+ hist с 2025-07-04); 3b — treq OH(D) only, `fallback_10y_psns=None` (`planer_calendar_remain.py`).

## Decisions (≤7)

1. **Sequential pipeline через `extract_master.py`** — единый entrypoint для Excel и DWH: MD → source-head → Enrich/Dictionaries → Tensors → status/repair/BR → demote; каждая стадия зависит от предыдущей.
2. **DWH cascade планеров** — в DWH-режиме `extract_master.py` заменяет Excel head одним `dwh_loader.py --step all`; после load `heli_pandas status_id=0`: overhaul→program_ac→inactive→**3b** → post (precheck часов OPS, component/serviceable/repair/storage/BR). Канон: `docs/architecture/extract.md` §Day0; приёмка: `docs/backlog.md` §2026-07-21.
3. **Destination gates (program→calendar)** — общий `destination_for_remain`: сначала hist `program_ac` ≥2025-07-04, затем календарный OH(D). Demote (`status=2` excess) и 3b (`status=1` OOR) — **разные входы**, одна функция гейтов. Код: `planer_calendar_remain.py`.
4. **Demote-only fallback +10y−1d** — если нет treq OH(D) **и** serial ∈ history с 2025-07-04 → `due = base + 10 calendar years − 1 day` (inclusive). **3b без fallback** — без treq → не 3 по календарю. Решение согласовано 2026-07-22 (`docs/backlog.md` §2026-07-21).
5. **Day0 OPS demote после terminal BR** — excess OPS vs `flight_program_ac` ранжируется по deficit комплектации; destination через те же гейты (+ demote-only fallback). Runner: `day0_ops_deficit_demote_runner.py` — последний критичный шаг `extract_master.py`; финальная acceptance проверяет OPS==MP4 через `compare_ops_to_target`.
6. **Блок storage** — неисправные агрегаты без `target_date` → `status_id=7` (unserviceable), не 4; `heli_pandas_storage_status.py`.
7. **Native ClickHouse driver** (порт 9000) + **мультиверсионность** — `version_date`/`version_id`; day0 sim = `version_date` среза; MP4/MP5 из Excel `--dataset-path` (предпочтительно `v_<та же дата>`).

## Impact Paths

```
DWH path (канон = extract_master --source dwh):
  md_components → dwh_loader --step all
    → heli_pandas (status_id=0) + cascade 1→3b + post enrich
    → flight_program_ac/fl (Excel --dataset-path)
    → status/repair/BR tail → day0 OPS demote → OPS==MP4 gate
    → sim (ClickHouse version_date/version_id=1)

Excel legacy: extract_master --source excel | interactive menu → тот же хвост с demote
```

- `data_input/source_data/v_*/*.xlsx` + DWH → ClickHouse day0 таблицы
- Smoke 2026-07-22: `heli_pandas` 11625; `flight_program_ac` 4000; `flight_program_fl` до ~2.85M (ACN×4000) — см. `output/extract_master_smoke_2026-07-22.log`

## Validation Proof

- **Runbook §0**: одна команда `extract_master --source dwh`; leaf запрещены.
- **Acceptance в master**: `status_id=0==0` + OPS==MP4 (`compare_ops_to_target`).
- **Evidence**: `output/extract_master_smoke_2026-07-22.log`; demote audit `output/day0_ops_deficit_demote_2026-07-22_v1/`; также 19–20.07 в `docs/backlog.md` §2026-07-21.
- **Post-sim (optional)**: `docs/validation_capsule.md`; INV-2 / INV-12.

## Risks (≤7) + Mitigations
- Некорректные Excel/DWH → precheck D1 + validate
- Сборка day0 из leaf / остановка на enrich → OPS≠MP4 → **запрет в runbook §0** + demote финал master
- Ручной re-enrich без master → сброс статусов → всегда полный `extract_master --source dwh` (при необходимости `--replace-slice`)
- Дубли `version_date` → `etl_version_manager`
- Demote vs 3b пересечение → disjoint inputs (`docs/backlog.md`)
- Fallback 10y только demote → `fallback_10y_psns=None` в 3b
- Storage 4→7 без target_date → учитывать `heli_pandas_storage_status.py`

## Open Questions (≤7)
- **age40** — отдельный контур (не входит в day0 воронку 2026-07-21); scope TBD (`docs/backlog.md` §2026-07-21).
- Миграция с Excel на API-источник данных?
- Автоматизация запуска ETL по расписанию (cron)?

*Settled (не open): demote-only fallback +10y−1d — согласовано 2026-07-22, см. Decisions §4.*

## Pointers (≤15)
- `docs/runbook_sim_launch.md` **§0** (copy-paste запуск) + §1 (фазы)
- `.cursor/rules/10_extract_and_env.mdc`
- `docs/architecture/extract.md` §Day0 воронка (логика)
- `docs/backlog.md` §2026-07-21 (приёмка + evidence)
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
