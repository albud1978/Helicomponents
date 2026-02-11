# Context Capsule — transitions

## Scope
Трогаем:
- `config/transitions/transitions_rules.json` (SSoT матрицы переходов) 
- `config/transitions/invariants.json` (SSoT инвариантов)
- `code/sim_v2/messaging/rtc_state_transitions_v8.py` (RTC переходы ops→storage/unsvc)
- `code/sim_v2/messaging/rtc_state_transitions_v7.py` (RTC однофазные переходы)
- `code/sim_v2/messaging/orchestrator_limiter_v8.py` (HF_StepController, порядок слоёв)
- `docs/architecture/limiter_architecture.md` (таблица слоёв и канон V8)

Не трогаем:
- Квотирование (P1/P2/P3/P4) → `docs/quota_capsule.md`
- Валидация → `docs/validation_capsule.md`

## Invariants (≤12)

SSoT: `config/transitions/invariants.json`

Связанные с переходами:
- INV-1: sne ≤ ll в operations — `invariants.json`
- INV-4: возврат из unsvc не раньше repair_time — `invariants.json`
- INV-8: sne/ppr frozen в storage — `invariants.json`
- INV-9: limiter=0 → обязательный выход из operations — `invariants.json`
- GPU-6: один агент — максимум один переход за шаг — `invariants.json`
- forbidden_transitions: всё, что не в rules, запрещено; spawn 5→3 (serviceable), НЕ 5→2 (ops) — `invariants.json`
- TEMP-2: promote P2 невозможен пока repair_days > 0; layer 12 обязан ставить repair_days=repair_time — `invariants.json`

Структурные:
- 8 состояний: spawn(0), inactive(1), operations(2), serviceable(3), repair(4), reserve(5), storage(6), unserviceable(7) — `transitions_rules.json`
- 10 правил переходов с condition precedent (pre) и subsequent (post) — `transitions_rules.json`
- 18 RTC + 1 HF_StepController в строгом порядке — `transitions_rules.json → rtc_execution_order`

## Decisions (Key Decisions) (≤7)
- V8: однофазная архитектура (прямой setInitialState/setEndState, без intent) — `docs/architecture/limiter_architecture.md`
- Порядок слоёв сокращён до 18 RTC + 1 HF_StepController; слои 5–8 (фаза 0.5) удалены; слои 14–16 перенесены в конец — `config/transitions/transitions_rules.json`, `docs/architecture/limiter_architecture.md`
- HF_UpdateDayV8 удалён, заменён на HF_StepController — `code/sim_v2/messaging/orchestrator_limiter_v8.py`
- Inline limiter: бинарный поиск встроен в X→ops, слой 49 удалён — `code/sim_v2/messaging/rtc_state_transitions_v8.py`, `code/sim_v2/messaging/rtc_limiter_optimized.py`
- Spawn: детерминированный 5→3 (serviceable), НЕ 5→2 (ops) — `config/transitions/invariants.json`
- Layer 12: ops→unsvc обязан ставить repair_days=repair_time; early-out в слоях 11–12 при limiter > 0 — `config/transitions/invariants.json`, `code/sim_v2/messaging/rtc_state_transitions_v8.py`

## Impact Paths
- `transitions_rules.json → rules` → какие переходы возможны → поведение всей симуляции
- `transitions_rules.json → rtc_execution_order` → 18 RTC + HF_StepController → детерминизм результата
- `code/sim_v2/messaging/orchestrator_limiter_v8.py` → HF_StepController → reset/обновление дня перед квотами
- `invariants.json → global_invariants/TEMP-2` → repair_days guard → корректность P2
- `rtc_state_transitions_v8.py` → ops→storage/unsvc (early-out, repair_days=repair_time) → INV-9/TEMP-2

## Validation Proof
- Матрица переходов: `code/analysis/sim_validation_transitions.py` (allowed transitions)
- Условия 2→6/2→7: `code/validation/validate_state2ops_transitions.py`
- Инкременты: `code/validation/validate_state2ops_increments.py`
- INV-9: `code/analysis/sim_validation_runner_msg.py` (validate_limiter_exit)
- Формализовано: `config/transitions/invariants.json`

## Risks (≤7) + Mitigations
- Ошибка порядка после сокращения до 18 RTC → логические сбои → использовать rtc_execution_order как SSoT
- Inline limiter в X→ops ломает INV-9 при limiter==0 → покрывать validate_limiter_exit
- Early-out в слоях 11–12 пропускает обязательные апдейты → проверять TEMP-2 и переходы 2→6/2→7

## Open Questions (≤7)
- Нужен ли переход inactive→serviceable (1→3) минуя operations?

## Pointers (≤15)
- `config/transitions/transitions_rules.json`
- `config/transitions/invariants.json`
- `code/sim_v2/messaging/rtc_state_transitions_v8.py`
- `code/sim_v2/messaging/rtc_state_transitions_v7.py`
- `code/sim_v2/messaging/orchestrator_limiter_v8.py`
- `docs/architecture/limiter_architecture.md`
- `code/analysis/sim_validation_transitions.py`
- `code/validation/validate_state2ops_transitions.py`
