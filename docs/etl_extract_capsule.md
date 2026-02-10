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
2. **Dual Loader** (стадия 4) — одновременная загрузка heli_raw (все данные, ~10736) и heli_pandas (фильтрованные). Причина: разделение архивных и рабочих данных.
3. **Native ClickHouse driver** (порт 9000) — clickhouse_driver вместо clickhouse_connect (HTTP). Причина: производительность + совместимость с типами.
4. **Additive vs Rewrite** — словари (dict_*) additive (append-only); status_flat — rewrite. Причина: словари растут монотонно, статусы перезаписываются.
5. **Excel как входные данные** — Status_Components.xlsx, Status_Overhaul.xlsx, Program_AC.xlsx, Program.xlsx из `data_input/source_data/v_YYYY-MM-DD/`. Причина: бизнес-формат заказчика.

## Impact Paths
- `data_input/source_data/v_*/*.xlsx` → extract_master.py → ClickHouse tables → симуляция читает из ClickHouse
- `config/database_config.yaml` + `.env` → параметры подключения → все скрипты ETL и анализа
- `heli_pandas` (ClickHouse) → начальное состояние агентов → корректность всей симуляции
- `flight_program_fl` → mp5_lin (программа полётов) → определяет налёт агентов

## Data Flow

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
  ├── Program.xlsx ────────────── [9]  flight_program_fl ───→ flight_program_fl (1.16M rows)
  │                                [10] flight_program_ac ──→ flight_program_ac (4K rows)
  │
  │   [11-18] Final: group_by, precheck, statuses, repair_days
  │
  └──→ ClickHouse (all tables versioned by version_date)
```

## Key ClickHouse Tables

| Таблица | Записей | Назначение |
|---------|---------|------------|
| heli_pandas | ~10,736 | Центральная таблица компонентов |
| heli_raw | ~10,736 | Архив всех данных (без фильтрации) |
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

## Open Questions (≤7)
- Миграция с Excel на API-источник данных?
- Автоматизация запуска ETL по расписанию (cron)?

## Pointers (≤15)
- `code/extract/extract_master.py`
- `code/extract/dual_loader.py`
- `code/extract/md_components_loader.py`
- `code/extract/status_overhaul_loader.py`
- `code/extract/program_fl_direct_loader.py`
- `code/extract/program_ac_loader.py`
- `config/database_config.yaml`
- `code/utils/config_loader.py`
- `code/utils/etl_version_manager.py`
- `code/utils/dataset_manager.py`
- `code/analysis/validate_heli_pandas.py`
- `docs/architecture/extract.md`
