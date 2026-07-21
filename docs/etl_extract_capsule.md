# Context Capsule — etl_extract

## Scope
Трогаем:
- `code/extract/extract_master.py` (оркестратор 18 стадий ETL)
- `code/extract/*.py` (все загрузчики и обогатители)
- `config/database_config.yaml` (подключение к ClickHouse)
- `code/utils/config_loader.py`, `code/utils/etl_version_manager.py`, `code/utils/version_utils.py`

Не трогаем:
- Симуляцию (code/sim_v2/) → другие капсулы
- Валидацию результатов симуляции → `docs/validation_capsule.md`

## Invariants (≤12)

ETL-специфичные инварианты (не в invariants.json, обеспечиваются пайплайном):
- Версионирование: каждый прогон ETL создаёт уникальный version_date; данные иммутабельны после записи
- Полнота: 18 стадий выполняются строго последовательно; пропуск стадии = ошибка
- Типы данных: все ресурсные поля (sne, ppr, ll, oh) → UInt32; repair_days → UInt16
- Источники: только реальные Excel + ClickHouse; синтетика запрещена без разрешения

## Decisions (≤7)

1. **18-стадийный sequential pipeline** — жёсткий порядок: MD → Status → Program → Dual → Enrich → Dictionaries → Tensors → Final. Причина: каждая стадия зависит от предыдущей.
2. **Dual Loader / DWH enrich** — `heli_pandas` после load проходит planner cascade: overhaul→program_ac→inactive→**3b** → post (precheck часов OPS, component/storage…). Канон воронки планеров: [docs/backlog.md](backlog.md) §2026-07-21.
3. **Destination gates (program→calendar)** — общий `destination_for_remain`: сначала история `program_ac`≥2025-07-04, потом календарный OH(D). Demote и 3b — **разные входы** (`status=2` excess vs `status=1` OOR), одна функция гейтов. Fallback `+10y−1д` **только demote** (+ hist); 3b — только treq. Код: `planer_calendar_remain.py`.
4. **Day0 OPS demote после MP4** — excess OPS vs `flight_program_ac` ранжируется по deficit комплектации; destination через те же гейты. Runner: `day0_ops_deficit_demote_runner.py` (после `flight_program_*`; re-enrich сбрасывает статусы → demote заново).
5. **Блок storage** — неисправные агрегаты без target_date → status_id=7 (unserviceable), не 4; `heli_pandas_storage_status.py`.
6. **Native ClickHouse driver** (порт 9000) — clickhouse_driver; Float64 запрещён без согласования.
7. **Мультиверсионность** — `version_date`/`version_id`; day0 sim = `version_date` среза. DWH-path: `dwh_loader.py` + Excel `v_2026-04-08` для MP4/MP5 (см. runbook).

## Impact Paths
- `data_input/source_data/v_*/*.xlsx` → extract_master.py → ClickHouse tables → симуляция читает из ClickHouse
- `config/database_config.yaml` + `.env` → параметры подключения → все скрипты ETL и анализа
- `heli_pandas` (ClickHouse) → начальное состояние агентов → корректность всей симуляции
- `flight_program_fl` → mp5_lin (программа полётов) → определяет налёт агентов
- `version_date` фильтр → `preload_mp5_maps` + `preload_mp4_by_day` → корректное стартовое состояние симуляции

## Data Flow

```
DWH path (канон runbook):
  program_ac + status_overhaul + Status_Components
       → heli_pandas (status_id=0)
       → planner cascade: overhaul→program_ac→inactive→3b(OOR gates)
       → post: precheck(D1 hours OPS) → component/serviceable/repair/storage/BR
       → flight_program_ac/fl (Excel v_2026-04-08)
       → day0 OPS deficit demote (destination gates + demote-only 10y fallback)
       → sim

Excel legacy path (extract_master): см. стадии 1–18 ниже.
```

```
Excel files (data_input/)
  │
  ├── MD_Components.xlsx ──────── [1] md_components_loader ──→ md_components
  ├── Status_Overhaul.xlsx ────── [2] status_overhaul_loader → status_overhaul
  ├── Program_AC.xlsx ─────────── [3] program_ac_loader ────→ program_ac
  ├── Status_Components.xlsx ──── [4] dual_loader ──────────→ heli_raw + heli_pandas
  │
  │   [5-8] Enrichment: ac_type_mask, BR, MD enrichment, dictionaries
  │
  ├── Program.xlsx ────────────── [9]  flight_program_fl ───→ flight_program_fl
  │                                [10] flight_program_ac ──→ flight_program_ac
  │
  │   [11-18] Final: group_by, precheck, statuses, repair_days
  │
  └──→ ClickHouse (all tables versioned by version_date)
```

## Key ClickHouse Tables

| Таблица | Записей | Назначение |
|---------|---------|------------|
| heli_pandas (v_2025-07-04) | 10,913 | Центральная таблица компонентов (per-dataset) |
| heli_pandas (v_2025-12-30) | 11,389 | Центральная таблица компонентов (per-dataset) |
| heli_raw (v_2025-07-04) | 10,913 | Архив всех данных без фильтрации (per-dataset) |
| heli_raw (v_2025-12-30) | 11,389 | Архив всех данных без фильтрации (per-dataset) |
| md_components | 64 | Мастер-данные компонентов |
| flight_program_fl | 1,164,000 | Тензор программы полётов (FRAMES × DAYS) |
| flight_program_ac | 4,000 | Тензор операций (борт × день) |
| status_overhaul | 46 | Статус капремонтов |
| program_ac | 189 | Реестр бортов |
| dict_* | variable | Справочники (partno, serialno, owner, ac_type, status, aircraft_number) |

## Risks (≤7) + Mitigations
- Некорректные Excel → ошибка начальных данных → precheck стадия (D1) + validate_heli_pandas.py
- Пропуск стадии → неконсистентные данные → sequential pipeline (ошибка останавливает всё)
- Дубли version_date → смешение прогонов → etl_version_manager: уникальный version_date per run
- Ретроспективные сверки статусов: блок 4 изменён (status_id=4 → status_id=1 для неисправных без target_date) → учитывать смену классификации — `code/extract/heli_pandas_storage_status.py`

## Open Questions (≤7)
- Миграция с Excel на API-источник данных?
- Автоматизация запуска ETL по расписанию (cron)?

## Pointers (≤15)
- `docs/backlog.md` §2026-07-21 (канон воронки планеров)
- `docs/runbook_sim_launch.md`
- `code/extract/planer_calendar_remain.py`
- `code/extract/inactive_serviceable_classifier.py`
- `code/extract/deficit_demoter.py` / `day0_ops_deficit_demote_runner.py`
- `code/extract/program_ac_precheck_next_day.py`
- `code/utils/dwh_loader.py` / `dwh_post_enrichment.py`
- `code/extract/dual_loader.py`
- `code/extract/overhaul_status_processor.py` / `program_ac_status_processor.py` / `inactive_planery_processor.py`
- `code/extract/heli_pandas_storage_status.py`
- `code/extract/program_fl_direct_loader.py` / `program_ac_direct_loader.py`
- `config/database_config.yaml`
- `code/utils/config_loader.py`
