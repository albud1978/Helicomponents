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

SSoT: `config/transitions/invariants.json` для реестра инвариантов; `config/transitions/transitions_rules.json` для активных pre/post условий переходов.

Связанные с переходами:
- INV-1 / V8 no-exceed: ресурс не превышается в operations; активные V8-переходы 2→6/2→7 уходят по look-ahead, если следующий день (`dt_next`) СТРОГО превысил бы предел; равенство (`==`) допустимо — `invariants.json`, `transitions_rules.json`
- INV-4: возврат из unsvc не раньше repair_time — `invariants.json`
- INV-8: sne/ppr frozen в storage — `invariants.json`
- INV-9: `limiter=0` в активном V8 трактуется как postcondition выхода/границы, а не как precondition немедленного ухода; условия ухода задаются look-ahead в `ops_to_storage_v8`/`ops_to_unsvc_v8` — `invariants.json`, `transitions_rules.json`
- INV-10: баланс оборота initial+entries+spawn=exits+final; transition_matrix LEGAL: 0→2, 0→3, 1→2, 1→4, 2→3, 2→6, 2→7, 3→2, 4→2, 4→3, 7→2, 7→4 (все прочие — ILLEGAL); validator: `code/validation/inv10_turnover_balance.py` — PASS — `invariants.json`
- GPU-6: один агент — максимум один переход за шаг — `invariants.json`
- forbidden_transitions: spawn detect 0→3 (serviceable), spawn dynamic 0→2 (ops); state 5 (reserve) — legacy, не используется в V9 — `invariants.json`

Структурные:
- 8 состояний (state 5 reserve — legacy, не используется в V9): spawn(0)/inactive(1)/operations(2)/serviceable(3)/repair(4)/storage(6)/unserviceable(7) — `transitions_rules.json`
- 10 правил переходов с condition precedent (pre) и subsequent (post) — `transitions_rules.json`
- 48 layers (orders 0..47): Host init / Day advance / Pre snapshot / Det spawn / Phase 0/1/1.5/2 quota / 3 apply / Telemetry / Post-quota recount / Spawn recount / Phase 4 spawn / Diagnostics / Limiter / MP2 export / RL export / Sync — `transitions_rules.json → rtc_execution_order`

## Decisions (Key Decisions) (≤7)
- V8: однофазная архитектура (прямой setInitialState/setEndState, без intent) — `docs/architecture/limiter_architecture.md`
- Порядок исполнения: 48 layers (orders 0..47) + HF_StepController; SSoT — `transitions_rules.json → rtc_execution_order` — `config/transitions/transitions_rules.json`, `docs/architecture/limiter_architecture.md`
- HF_UpdateDayV8 удалён, заменён на HF_StepController — `code/sim_v2/messaging/orchestrator_limiter_v8.py`
- Inline limiter: бинарный поиск встроен в X→ops (отдельный layer не нужен) — `code/sim_v2/messaging/rtc_state_transitions_v8.py`, `code/sim_v2/messaging/rtc_limiter_optimized.py`
- Spawn: детерминированный (detect) 0→3 (serviceable), динамический 0→2 (ops); state 5 (reserve) не используется в V9 — `config/transitions/invariants.json`
- Layer ops→storage/unsvc работает в end-of-day semantics: сначала выполняется налёт текущего шага, затем look-ahead проверяет, превысил бы следующий день ресурс (`ppr+dt_next>oh` / `sne+dt_next>ll`). `limiter==0` не является гейтом; при уходе `ops→storage` и `ops→unsvc` выставляют `limiter=0`, `ops→unsvc` также ставит `repair_days=repair_time` — `config/transitions/transitions_rules.json`, `code/sim_v2/messaging/rtc_state_transitions_v8.py`
- Постпроцессинг P2/P3 (Вариант B, 2026-02-17): реконструирует окно ремонта 7→4→2 и 1→4→2 с обновлением pre_status_id; поддерживает баланс оборота (INV-10) и lookback-only repairline-семантику — `config/transitions/invariants.json` (INV-10.notes)

## Impact Paths
- `transitions_rules.json → rules` → какие переходы возможны → поведение всей симуляции
- `transitions_rules.json → rtc_execution_order` → 48 layers (orders 0..47) + HF_StepController → детерминизм результата
- `code/sim_v2/messaging/orchestrator_limiter_v8.py` → HF_StepController → reset/обновление дня перед квотами
- `invariants.json → INV-4` → repair_days guard (precondition `repair_days == 0` в правиле `p2_unsvc_to_ops_v8`) → корректность P2
- `rtc_state_transitions_v8.py` → ops→storage/unsvc (look-ahead strict `>`, equality allowed, `limiter=0` postcondition, `repair_days=repair_time`) → resource no-exceed / INV-4

## Validation Proof
- Матрица переходов: `code/analysis/sim_validation_transitions.py` (allowed transitions)
- Условия 2→6/2→7: `code/validation/validate_state2ops_transitions.py`
- Инкременты: `code/validation/validate_state2ops_increments.py`
- V8 resource no-exceed: workflow `W_sim_v8_resource_no_exceed_20260605T165249Z`, `version_date=2026-04-08`, test `version_id=8101`, `end_day=3650` — `count(ppr > oh OR sne > ll)=0`, `entry-edge=0`, `check_2=0`; test version очищен
- INV-10 (turnover balance + LEGAL/ILLEGAL transitions): `code/validation/inv10_turnover_balance.py` — PASS
- Формализовано: `config/transitions/invariants.json` (реестр) и `config/transitions/transitions_rules.json` (активные pre/post условия 2→6/2→7)

## Risks (≤7) + Mitigations
- Ошибка порядка слоёв (48 orders) → логические сбои → использовать rtc_execution_order как SSoT
- Inline limiter / ops→storage/unsvc могут ошибиться на границе `==` vs `>` → покрывать no-exceed (`ppr<=oh`, `sne<=ll`) и entry-edge checks
- Возврат к `limiter==0` как precondition ухода из ops ломает end-of-day/look-ahead semantics → проверять `transitions_rules.json` и условия 2→6/2→7

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
