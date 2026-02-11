# Context Capsule — limiter_v8

## Scope
Трогаем:
- `code/sim_v2/messaging/orchestrator_limiter_v8.py` (основной оркестратор V8) — `README.md`
- `docs/architecture/limiter_architecture.md` (источник истины V8) — `docs/architecture/limiter_architecture.md`
- `docs/architecture/rtc_pipeline_architecture.md` (MP5/mp5_lin инварианты) — `docs/architecture/rtc_pipeline_architecture.md`
- `docs/architecture/validation_rules.md` (методология валидации/SQL-first) — `docs/architecture/validation_rules.md`
- `config/transitions/transitions_rules.json` (матрица переходов state→state) — `docs/architecture/limiter_architecture.md`
- `config/transitions/quota_rules.json` (логика квот/RepairLine) — `docs/architecture/limiter_architecture.md`
- `config/transitions/invariants.json` (формализованные инварианты INV/TEMP/GPU) — SSoT инвариантов
- `tools/transitions_viewer/index.html` (визуализация переходов/квот) — `docs/architecture/limiter_architecture.md`

Не трогаем:
- `archive_vnv_cpu_project/` (архив, запрещено правилами) — `.cursorrules`
- Архивные версии оркестраторов V1–V7 (не использовать) — `README.md`

## Invariants (≤12)

**SSoT**: `config/transitions/invariants.json` (INV-1..INV-9, TEMP-1..TEMP-4, GPU-1..GPU-6)

Ключевые (из invariants.json):
- INV-1: sne ≤ ll в operations — `invariants.json`
- INV-2: ops_count ≈ target (±1) — `invariants.json`
- INV-3: в ремонте ≤ repair_number — `invariants.json`
- INV-5: баланс наработок Σdt = Δsne — `invariants.json`
- INV-9: limiter=0 → выход из ops — `invariants.json`
- GPU-2: mp5_lin read-only после init — `invariants.json`
- GPU-6: один переход за шаг — `invariants.json`

Архитектурные (не в invariants.json):
- V8 — источник истины; противоречия в других документах считаются устаревшими — `docs/architecture/limiter_architecture.md`
- Для unsvc exit_date удалён; RepairLine — основной механизм ремонта — `docs/architecture/limiter_architecture.md`
- `repair_days`: для unsvc декрементируется до 0; для inactive всегда 0 — `docs/architecture/limiter_architecture.md`
- `MAX_DETERMINISTIC_DATES=500`; лишние даты отбрасываются — `docs/architecture/limiter_architecture.md`
- Порядок слоёв = порядок регистрации, критичен для логики — `docs/architecture/limiter_architecture.md`

## Decisions (≤7)
- V8 использует RepairLine вместо exit_date (пул линий из MP) — `docs/architecture/limiter_architecture.md`
- `repair_days` введён для unsvc, inactive не декрементируется — `docs/architecture/limiter_architecture.md`
- Правило ресурса — next-day dt (чтобы исключить переналёт) — `docs/architecture/limiter_architecture.md`
- **HF_StepController** (вместо HF_UpdateDayV8) выполняется в конце шага после layer 50 (после квотирования): adaptive_days = MIN(mp_min_limiter, days_to_next_deterministic_date), prev_day = current_day, current_day += adaptive_days; синхронизация MacroProperty → Environment; reset mp_min_limiter = 0xFFFFFFFF. Layers 14–16 удалены — `code/sim_v2/messaging/orchestrator_limiter_v8.py`.
- **HF_InitV8 убран**; deterministic_dates_mp инициализируется при сборке модели — `code/sim_v2/messaging/orchestrator_limiter_v8.py`.
- **Inline limiter**: layer 49 убран как отдельный слой; бинарный поиск встроен в X→ops; early-out в layers 11–12: if (limiter > 0) return false — `code/sim_v2/messaging/rtc_limiter_v8.py`.
- V8 не эквивалентен V7 по переходам; MAX_DETERMINISTIC_DATES=500 (лишние даты отбрасываются) — `docs/architecture/limiter_architecture.md`

## Impact Paths
- `code/sim_v2/messaging/orchestrator_limiter_v8.py` → порядок слоёв V8, HF_StepController (end-of-step adaptive), стабильный current_day в шаге → поведение симуляции — `code/sim_v2/messaging/orchestrator_limiter_v8.py`
- `docs/architecture/limiter_architecture.md` → канон V8 → правила/слои — `docs/architecture/limiter_architecture.md`
- `config/transitions/transitions_rules.json` → матрица state→state → переходы — `docs/architecture/limiter_architecture.md`
- `config/transitions/quota_rules.json` → логика квот/RepairLine → P1/P2/P3 решения — `docs/architecture/limiter_architecture.md`
- `tools/transitions_viewer/index.html` → визуализация переходов/квот → отладка — `docs/architecture/limiter_architecture.md`
- `docs/architecture/rtc_pipeline_architecture.md` → MP5/mp5_lin инварианты → корректный dt/dn — `docs/architecture/rtc_pipeline_architecture.md`
- `docs/architecture/validation_rules.md` → SQL-first/инварианты → критерии корректности — `docs/architecture/validation_rules.md`
- `README.md` → статус V8 как основной архитектуры → зона ответственности — `README.md`

## Validation Proof
- Формализованные инварианты: `config/transitions/invariants.json` (INV-1..INV-9, TEMP-1..TEMP-4, GPU-1..GPU-6) — SSoT
- Скрипты валидации: `code/analysis/sim_validation_runner_msg.py`, `code/analysis/sim_validation_quota.py`, `code/validation/validate_state2ops_*.py`
- `python code/analysis/validate_heli_pandas.py --analyze/--update/--all` — базовая валидация `heli_pandas` — `docs/architecture/validation_rules.md`
- Проверка отсутствия NVRTC warning'ов (log clean) — `docs/architecture/validation_rules.md`
- Формат капсулы проверяется по шаблону (8 обязательных секций с лимитами) — `docs/validation.md`

## Risks (≤7) + Mitigations
- Переполнение `MAX_DETERMINISTIC_DATES` → потеря событий → контролировать число дат и лимит — `docs/architecture/limiter_architecture.md`
- Нарушение порядка слоёв → логические сбои → использовать таблицу слоёв как источник истины — `docs/architecture/limiter_architecture.md`
- Ошибка размера/индексации MP5 → неверный dt → жёсткие проверки длины и формулы — `docs/architecture/validation_rules.md`
- Запись в `mp5_lin` после инициализации → порча данных → read-only режим — `docs/architecture/validation_rules.md`
- NVRTC warning'и → скрытые дефекты → фикс + очистка кэша — `docs/architecture/validation_rules.md`
- Неправильная обработка `limiter=0` → исключение → соблюдать правило выхода — `docs/architecture/limiter_architecture.md`

## Open Questions (≤7)
- Какие дополнительные SQL-инварианты для limiter_v8 обязательны кроме базовых `validation_rules`?
- Нужны ли дополнительные проверки по RepairLine (slots/commit consistency) в SQL?
- Какой k-hop радиус использовать в KG влияния для “Impact Paths” limiter_v8?

## Pointers (≤15)
- `code/sim_v2/messaging/orchestrator_limiter_v8.py`
- `code/sim_v2/messaging/rtc_limiter_v8.py`
- `docs/architecture/limiter_architecture.md`
- `docs/architecture/rtc_pipeline_architecture.md`
- `docs/architecture/validation_rules.md`
- `config/transitions/invariants.json`
- `config/transitions/transitions_rules.json`
- `config/transitions/quota_rules.json`
- `code/utils/agent_kg.py`
- `docs/validation.md`
- `tools/transitions_viewer/index.html`
- `README.md`
