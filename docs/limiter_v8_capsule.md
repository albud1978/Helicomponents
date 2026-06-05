# Context Capsule — limiter_v8

## Scope
Трогаем:
- `code/sim_v2/messaging/orchestrator_limiter_v8.py` (основной оркестратор V8) — `README.md`
- `code/sim_v2/messaging/rtc_compute_limiter_device.py` (общий device-include compute_limiter) — `docs/changelog.md`
- `code/sim_v2/messaging/rtc_state_transitions_v8.py` (ops→storage/unsvc look-ahead conditions) — `config/transitions/transitions_rules.json`
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

**SSoT**: `config/transitions/invariants.json` для реестра инвариантов; `config/transitions/transitions_rules.json` для активных pre/post условий V8-переходов — invariants.json v15

Ключевые (из invariants.json + active V8 `transitions_rules.json`):
- INV-1: sne ≤ ll в operations — `invariants.json`
- INV-2: ops_count == mp4_target (tolerance=0; warmup-исключение [0, repair_time)) — `invariants.json`
- INV-3: countIf(repair_time>0 AND free_days<repair_time) <= repair_quota; lookback-only семантика, source: sim_repairline_v9; validator: `code/validation/inv3_repair_capacity.py` — PASS — `invariants.json`
- INV-5: баланс наработок Σdt = Δsne — `invariants.json`
- V8 resource no-exceed / INV-9 boundary: `ppr<=oh` и `sne<=ll` в operations; переход из ops считается по look-ahead `dt_next` и срабатывает только если следующий день строго превысил бы ресурс. `limiter=0` — postcondition выхода/границы, не precondition немедленного ухода; на последнем дне горизонта `status_id=2 AND limiter=0` допустим, если ресурс не превышен — `invariants.json`, `transitions_rules.json`
- INV-10: баланс оборота initial+entries+spawn=exits+final + LEGAL/ILLEGAL transition_matrix — `invariants.json`
- INV-11: post-warmup deficit ops при saturation dynamic spawn Mi-17 — `invariants.json`
- TEMP-5: hybrid repair precondition (per_day_per_group) — `invariants.json`
- GPU-2: mp5_lin read-only после init — `invariants.json`
- GPU-6: один переход за шаг — `invariants.json`

Архитектурные (не в invariants.json):
- exit_date НЕ хранится как отдельный MP; вычисляется как `exit_date = repair_time - repair_days` в `rtc_repair_to_svc_v7` (см. TEMP-1 mechanism в invariants.json); RepairLine остаётся основным механизмом claim-based ремонта — `docs/architecture/limiter_architecture.md`
- `repair_days`: для unsvc декрементируется до 0; для inactive всегда 0 — `docs/architecture/limiter_architecture.md`

## Decisions (≤7)
- V8 использует RepairLine вместо exit_date (пул линий из MP); `repair_days` введён для unsvc (декрементируется до 0), для inactive не декрементируется (всегда 0) — `docs/architecture/limiter_architecture.md`
- Правило ресурса — next-day dt/look-ahead: adaptive limiter возвращает последний безопасный день `L`; ops→storage/unsvc уходят, если `ppr+dt_next>oh` или `sne+dt_next>ll`, а равенство (`==`) остаётся допустимым. Порядок "сначала летаем, потом меняем статус в конце дня" сохранён — `rtc_compute_limiter_device.py`, `rtc_state_transitions_v8.py`, `transitions_rules.json`
- **HF_StepController** (вместо HF_UpdateDayV8) выполняется в конце шага после квотирования: adaptive_days = MIN(mp_min_limiter, days_to_next_deterministic_date), prev_day = current_day, current_day += adaptive_days; синхронизация MacroProperty → Environment; reset mp_min_limiter = 0xFFFFFFFF — `code/sim_v2/messaging/orchestrator_limiter_v8.py`.
- **HF_InitV8 убран**; deterministic_dates_mp инициализируется при сборке модели — `code/sim_v2/messaging/orchestrator_limiter_v8.py`.
- **Layer numbering V8**: старые "phase 0.5" слои intent-state удалены; текущие orders 12-16 — RepairLine increment/write/publish + reset flags/buffers перед quota; inline limiter — бинарный поиск встроен в X→ops; отдельный layer-49 слой исключён (max order в текущей нумерации = 47) — `code/sim_v2/messaging/orchestrator_limiter_v8.py`, `code/sim_v2/messaging/rtc_limiter_v8.py`.
- V8 не эквивалентен V7 по переходам; MAX_DETERMINISTIC_DATES=500 (лишние даты отбрасываются) — `docs/architecture/limiter_architecture.md`
- **MP2/RL export + master-SSoT overlay**: Phase MP2 export (orders 44-45) + Phase RL export (order 46) пишут host dataframes; в `sim_repairline_v9` occupancy реконструируется из `sim_masterv2_v9` claim segments + claimless episodes (runtime acn/gb — телеметрия, не SSoT). Источник: `config/transitions/transitions_rules.json` (lines 118-120); `config/transitions/quota_rules.json` (lines 60-64, 124-128).

## Impact Paths
- `code/sim_v2/messaging/orchestrator_limiter_v8.py` → порядок слоёв V8, HF_StepController (end-of-step adaptive), стабильный current_day в шаге → поведение симуляции — `code/sim_v2/messaging/orchestrator_limiter_v8.py`
- `rtc_compute_limiter_device.py` + `rtc_state_transitions_v8.py` → граница safe-day `L`, look-ahead strict `>`, `limiter=0` postcondition → no-exceed resource invariant — `config/transitions/transitions_rules.json`
- `docs/architecture/limiter_architecture.md` → канон V8 → правила/слои — `docs/architecture/limiter_architecture.md`
- `config/transitions/transitions_rules.json` → матрица state→state → переходы — `docs/architecture/limiter_architecture.md`
- `config/transitions/quota_rules.json` → логика квот/RepairLine → P1/P2/P3 решения — `docs/architecture/limiter_architecture.md`
- `tools/transitions_viewer/index.html` → визуализация переходов/квот → отладка — `docs/architecture/limiter_architecture.md`
- `docs/architecture/rtc_pipeline_architecture.md` → MP5/mp5_lin инварианты → корректный dt/dn — `docs/architecture/rtc_pipeline_architecture.md`
- `docs/architecture/validation_rules.md` → SQL-first/инварианты → критерии корректности — `docs/architecture/validation_rules.md`
- `README.md` → статус V8 как основной архитектуры → зона ответственности — `README.md`
- MP2 export + RL export → host dataframes → `sim_masterv2_v9` + `sim_repairline_v9` (CH) → validation/BI — `config/transitions/transitions_rules.json`, `config/transitions/quota_rules.json`

## Validation Proof
- Формализованные инварианты: `config/transitions/invariants.json` (INV-1..INV-11, TEMP-1/TEMP-4/TEMP-5, GPU-1..GPU-6) — SSoT
- Скрипты валидации: `code/analysis/sim_validation_runner_msg.py`, `code/analysis/sim_validation_quota.py`, `code/validation/validate_state2ops_*.py`
- `python code/analysis/validate_heli_pandas.py --analyze/--update/--all` — базовая валидация `heli_pandas` — `docs/architecture/validation_rules.md`
- Resource no-exceed acceptance: workflow `W_sim_v8_resource_no_exceed_20260605T165249Z`, baseline dataset `version_date=2026-04-08`, test `version_id=8101` — `count(ppr > oh OR sne > ll)=0`, `entry-edge=0`, `check_2=0`, group counts match baseline; test version очищен
- Проверка отсутствия NVRTC warning'ов (log clean) — `docs/architecture/validation_rules.md`
- Формат капсулы проверяется по шаблону (8 обязательных секций с лимитами) — `docs/validation.md`

## Risks (≤7) + Mitigations
- Переполнение `MAX_DETERMINISTIC_DATES` → потеря событий → контролировать число дат и лимит — `docs/architecture/limiter_architecture.md`
- Нарушение порядка слоёв → логические сбои → использовать таблицу слоёв как источник истины — `docs/architecture/limiter_architecture.md`
- Ошибка размера/индексации MP5 → неверный dt → жёсткие проверки длины и формулы — `docs/architecture/validation_rules.md`
- Запись в `mp5_lin` после инициализации → порча данных → read-only режим — `docs/architecture/validation_rules.md`
- NVRTC warning'и → скрытые дефекты → фикс + очистка кэша — `docs/architecture/validation_rules.md`
- Неправильная обработка `limiter=0` как precondition вместо postcondition → off-by-one на границе ресурса/горизонта → соблюдать look-ahead strict `>` из `transitions_rules.json`

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
