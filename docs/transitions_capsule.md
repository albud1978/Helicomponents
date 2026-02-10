# Context Capsule — transitions

## Scope
Трогаем:
- `config/transitions/transitions_rules.json` (SSoT матрицы переходов) 
- `config/transitions/invariants.json` (SSoT инвариантов)
- `code/sim_v2/messaging/rtc_state_transitions_v8.py` (RTC переходы ops→storage/unsvc)
- `code/sim_v2/messaging/rtc_state_transitions_v7.py` (RTC однофазные переходы)
- `docs/architecture/limiter_architecture.md` (таблица 51 слой)

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
- forbidden_transitions: всё, что не в rules, запрещено — `invariants.json`

Структурные:
- 8 состояний: spawn(0), inactive(1), operations(2), serviceable(3), repair(4), reserve(5), storage(6), unserviceable(7) — `transitions_rules.json`
- 10 правил переходов с condition precedent (pre) и subsequent (post) — `transitions_rules.json`
- 51 слой RTC в строгом порядке — `transitions_rules.json → rtc_execution_order`

## Decisions (≤7)
- V8: однофазная архитектура (прямой setInitialState/setEndState, без intent) — `limiter_architecture.md`
- V8: next-day dt правило ресурса (sne + dt >= ll → storage) вместо post-increment — `limiter_architecture.md`
- V8: exit_date для unsvc удалён, заменён на RepairLine — `limiter_architecture.md`
- Storage (state 6) — терминальное, выхода нет — `transitions_rules.json`
- Порядок приоритетов ops→out: LL (приоритет 1) > BR (приоритет 2) > OH (приоритет 3) — `transitions_rules.json`

## Impact Paths
- `transitions_rules.json → rules` → какие переходы возможны → поведение всей симуляции
- `transitions_rules.json → rtc_execution_order` → в каком порядке → детерминизм результата
- `invariants.json → global_invariants` → что должно быть true всегда → критерии корректности
- `rtc_state_transitions_v8.py` → реализация ops→storage/unsvc → фактические переходы

## Validation Proof
- Матрица переходов: `code/analysis/sim_validation_transitions.py` (allowed transitions)
- Условия 2→6/2→7: `code/validation/validate_state2ops_transitions.py`
- Инкременты: `code/validation/validate_state2ops_increments.py`
- Формализовано: `config/transitions/invariants.json`

## Risks (≤7) + Mitigations
- Нарушение порядка слоёв → логические сбои → использовать rtc_execution_order как SSoT
- Каскадный переход за один шаг → непредсказуемое состояние → GPU-6 + FunctionCondition
- Stale данные после перехода → reset перед сбором (GPU-5)

## Open Questions (≤7)
- Нужен ли переход inactive→serviceable (1→3) минуя operations?

## Pointers (≤15)
- `config/transitions/transitions_rules.json`
- `config/transitions/invariants.json`
- `code/sim_v2/messaging/rtc_state_transitions_v8.py`
- `code/sim_v2/messaging/rtc_state_transitions_v7.py`
- `docs/architecture/limiter_architecture.md`
- `code/analysis/sim_validation_transitions.py`
- `code/validation/validate_state2ops_transitions.py`
