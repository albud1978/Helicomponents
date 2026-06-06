# Context Capsule — validation

## Scope
Трогаем:
- `config/transitions/invariants.json` (SSoT инвариантов — 12 глобальных (INV-1..INV-12), 3 временных (TEMP-1, TEMP-4, TEMP-5), 6 GPU (GPU-1..GPU-6) — invariants.json v16)
- `code/analysis/sim_validation_runner_msg.py` (оркестратор валидации MESSAGING)
- `code/analysis/sim_validation_quota.py` (INV-2: ops vs target)
- `code/analysis/sim_validation_increments.py` (INV-6: dt>0 только в ops)
- `code/analysis/sim_validation_ops_exits.py` (ops exits: limiter==0, ресурс, маскировка)
- `code/analysis/sim_validation_transitions.py` (матрица переходов)
- `code/validation/validate_state2ops_increments.py` (INV-5: баланс наработок)
- `code/validation/validate_state2ops_transitions.py` (переходы state_2_operations)
- `code/sim_v2/messaging/validate_limiter_flight_hours.py` (INV-7: dt = программа)
- `code/sim_v2/messaging/validate_limiter_ops_target.py` (INV-2: ops = target на шагах)
- `code/sim_v2/messaging/validate_limiter_v3.py` (комплексная LIMITER валидация)
- `code/sim_v2/components/validation_rules.py` (INV-8: storage frozen)
- `docs/architecture/validation_rules.md` (методология)

Не трогаем:
- ETL-валидация (validate_heli_pandas.py) → `docs/etl_extract_capsule.md`
- Инструменты триггеров (validate_triggers_vs_2to4.py) — утилита, не часть core

## Invariants (≤12)

SSoT: `config/transitions/invariants.json`

Маппинг инвариант → валидатор:
| Инвариант | Валидатор | Статус |
|-----------|-----------|--------|
| INV-1 | `code/validation/inv1_sne_le_ll.py` | PASS |
| INV-2 | `code/validation/inv2_ops_vs_target.py` | PASS |
| INV-3 | `code/validation/inv3_repair_capacity.py` | PASS (D1 max 10, D2 max 14, quota=18) |
| INV-4 | `code/validation/inv4_unsvc_repair_time.py` | PASS |
| INV-5 | `code/validation/inv5_balance_increments.py` | PASS |
| INV-6 | `code/validation/inv6_dt_only_ops.py` | PASS |
| INV-7 | `code/validation/inv7_dt_eq_mp5.py` | PASS |
| INV-8 | `code/validation/inv8_storage_frozen.py` | PASS |
| INV-9 | `code/validation/inv9_limiter_exit.py` | PASS |
| INV-10 | `code/validation/inv10_turnover_balance.py` | PASS |
| INV-11 | `code/validation/inv11_spawn_limit_saturation.py` | PENDING |
| TEMP-1 | `code/validation/temp1_repair_duration.py` | PASS |
| TEMP-4 | `code/validation/temp4_no_infinite_repair.py` | PASS |
| TEMP-5 | `code/validation/temp5_repair_hybrid_vector.py` | PENDING |

Примечания по обновлениям (SSoT: `invariants.json`):
- INV-10 (turnover balance + LEGAL/ILLEGAL transitions) — sentinel-инвариант transitions-домена
- INV-11 (post-warmup deficit ops при saturation dynamic spawn Mi-17) — PENDING, для диагностики лимитов dynamic birth
- TEMP-5 (hybrid repair precondition per_day_per_group) — PENDING, валидатор `temp5_repair_hybrid_vector.py`

## Decisions (≤7)

1. **SQL-first** — итоговая проверка по данным СУБД (sim_masterv2 экспорт), не по in-memory состоянию GPU. Причина: воспроизводимость + аудит.
2. **Два оркестратора** — sim_validation_runner.py (legacy V7) и sim_validation_runner_msg.py (MESSAGING V8). Причина: разные архитектуры intent_state vs state-based.
3. **Baseline заморожен** — sim_masterv2 baseline не изменяется; сравнение с ним только по запросу.
4. **Реальные данные only** — синтетика/заглушки запрещены без явного разрешения.
5. **JIT warnings = дефект** — каждый NVRTC warning исправляется немедленно (GPU-4).
6. **Ops-exit проверки вынесены в отдельный скрипт** — `sim_validation_ops_exits.py` (4 проверки).

## Impact Paths
- `invariants.json` → определяет ЧТО проверять → все скрипты валидации
- `sim_masterv2` (ClickHouse) → данные для проверки → SQL-запросы
- `sim_validation_runner_msg.py` → вызывает все суб-валидаторы → итоговый отчёт
- Результат валидации → решение о коммите (оркестратор workflow)

## Validation Proof
Рекурсивное: капсула описывает саму систему валидации. Верификация — ручной аудит + SQL-запросы к реальным данным + `sim_validation_ops_exits.py` (4 проверки).

## Risks (≤7) + Mitigations
- PENDING валидаторы: INV-11 (last_validated 2026-03-03), TEMP-5 (last_validated 2026-02-17) — оба new validators, ожидают full sim run для PASS verdict
- Расхождение V7/V8 валидации → разные оркестраторы, но общие принципы; унификация в будущем
- Stale данные в sim_masterv2 → каждый прогон перезаписывает по version_date
- Ложноположительные (warmup window) → INV-2 учитывает warmup

## Open Questions (≤7)
- Объединять ли sim_validation_runner.py и sim_validation_runner_msg.py?
- Нужен ли автоматический Python-скрипт для INV-1 (сейчас только SQL)?

## Pointers (≤15)
- `config/transitions/invariants.json`
- `code/analysis/sim_validation_runner_msg.py`
- `code/validation/inv1_sne_le_ll.py`
- `code/validation/inv2_ops_vs_target.py`
- `code/validation/inv3_repair_capacity.py`
- `code/validation/inv4_unsvc_repair_time.py`
- `code/validation/inv5_balance_increments.py`
- `code/validation/inv6_dt_only_ops.py`
- `code/validation/inv7_dt_eq_mp5.py`
- `code/validation/inv8_storage_frozen.py`
- `code/validation/inv9_limiter_exit.py`
- `code/validation/inv10_turnover_balance.py`
- `code/validation/inv11_spawn_limit_saturation.py`
- `code/validation/temp1_repair_duration.py`
- `code/validation/temp5_repair_hybrid_vector.py`
