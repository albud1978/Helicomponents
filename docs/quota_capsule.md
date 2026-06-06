# Context Capsule — quota

## Scope
Трогаем:
- `config/transitions/quota_rules.json` (SSoT квотирования)
- `config/transitions/invariants.json` (INV-2, INV-3, TEMP-5)
- `code/sim_v2/messaging/rtc_quota_v8.py` (реализация P1/P2/P3 + MessageBucket)
- `code/sim_v2/messaging/rtc_quota_v8_base.py` (базовые функции квотирования)
- `code/sim_v2/messaging/rtc_repair_lines_v8.py` (RepairLine управление)
- `code/sim_v2/messaging/rtc_spawn_dynamic_v7.py` (P4 — динамический spawn)
- `code/sim_v2/messaging/base_model_messaging.py` (определение MessageBucket)
- `docs/architecture/limiter_architecture.md` (канон V8: message-only квоты)

Не трогаем:
- Переходы состояний → `docs/transitions_capsule.md`
- Limiter/адаптивные шаги → `docs/limiter_v8_capsule.md`

## Invariants (≤12)

SSoT: `config/transitions/invariants.json`

- INV-2: ops_count == mp4_target (tolerance=0; warmup exception [0, repair_time))
- INV-3: countIf(repair_time>0 AND free_days<repair_time) <= repair_quota; lookback-only семантика, source: sim_repairline_v9; validator: code/validation/inv3_repair_capacity.py — PASS (D1 max 10, D2 max 14, quota=18)
- TEMP-5: hybrid repair precondition (per_day_per_group); validator: code/validation/temp5_repair_hybrid_vector.py — PENDING

## Decisions (Key Decisions) (≤7)

1. **Message-based отбор promote** — выбор кандидатов P1/P2/P3 через MessageBucket (key=0, promote/deficit broadcast → rank-based selection). Причина: O(N/K) вместо O(N²). NB: это про *отбор promote*; назначение repair-**линий** внутри commit использует CAS по MacroProperty (см. Decision 3). Источник: `docs/architecture/limiter_architecture.md`, `code/sim_v2/messaging/rtc_quota_v8.py`.
2. **Выбор линии — детерминированный позиционный захват по RO-snapshot** (анти‑фрагментация + детерминизм). В P2/P3 commit агент вычисляет `commit_pos` = число коммитящих той же фазы бортов с более высоким приоритетом (Mi-17 раньше Mi-8; внутри типа youngest-first = больший `idx`) и берёт линию на позиции `commit_pos` в отсортированном списке свободных линий (`acn==0 AND free_days>=repair_time`, сортировка по **минимальному free_days** = min запас сверх окна, tie‑break меньший `line_id`). Позиции уникальны → один CAS без гонки (`rtc_quota_v8.py`, P2 commit / P3 commit). Кандидат-маркеры: `mi8/mi17_commit_p2/p3_candidate` (ставятся в bucket только для non-stale). Слой `publish_status` / сообщение `RepairLineStatus` — **мёртвая телеметрия** (активного консьюмера нет). Источник: `code/sim_v2/messaging/rtc_quota_v8.py`, `config/transitions/quota_rules.json:100-102`. (verified W_sim_deterministic_line_capture)
3. **Назначение/освобождение линий — CAS-on-MacroProperty по детерминированной позиции, НЕ сообщения.** Захват: `repair_line_acn_mp[line].exchange(acn)` + `free_days_mp[line].exchange(0)` на линии, выбранной по `commit_pos` (см. Decision 2), bank source2 (`bank_pos = commit_pos - free_total`, newest-first). RO-snapshot пересоздаётся между P2-commit и P3-commit (слой `v8_repair_line_snapshot_p3`), чтобы P3 видел занятые P2 линии. Освобождение: `increment` обнуляет agent.acn при `free_days>=rt` → `write` пишет 0 в `repair_line_acn_mp` (`rtc_repair_lines_v8.py:62-87`). Сообщение `LineAssignment` **не имеет продюсера** в `code/sim_v2/**`; слой `repair_line_apply_assignment` инертен. Источник: `code/sim_v2/messaging/rtc_quota_v8.py`, `rtc_repair_lines_v8.py`. (verified W_sim_deterministic_line_capture)
4. **Четыре приоритета (P1>P2>P3>P4)** — P1 serviceable, P2 unserviceable, P3 inactive, P4 spawn. Источник: `docs/architecture/limiter_architecture.md`, `config/transitions/quota_rules.json`.
5. **Rank = count(idx > my_idx)** — детерминистический отбор (youngest first), без race conditions. Источник: `docs/architecture/limiter_architecture.md`.
6. **Bank-окна и source1/source2 commit**: P2/P3 commit идёт line-first source1(today best-fit, min free_days) → source2(bank head, newest-first, binary lock); в bank-only день ranking переключается на bank-friendly (older status_change_day first). Источник: `config/transitions/quota_rules.json:42-51, 110-128`.
7. **Master-SSoT repairline occupancy overlay**: sim_repairline_v9 occupancy строится из sim_masterv2_v9 claim segments ∪ claimless master episodes; runtime acn/gb на линии — только телеметрия, не SSoT. Источник: `config/transitions/quota_rules.json:60-64,124-128`; `config/transitions/transitions_rules.json:120`; `config/transitions/invariants.json:54`.
8. **Дефицит spawn**: deficit = target − curr_ops (без двойного учёта) — `code/sim_v2/messaging/rtc_spawn_dynamic_v7.py`.
9. **Только message‑only модуль**: register_quota_v8_full и legacy agent‑based P2/P3 удалены; остаётся `register_quota_v8_messages`. Источник: `docs/architecture/limiter_architecture.md`.
10. **line_id↔aircraft биекция ДЕТЕРМИНИРОВАНА** (исправлено в W_sim_deterministic_line_capture; ранее была недетерминирована из‑за гонки параллельных CAS). Гонка устранена позиционным захватом (Decision 2): порядок назначения линий = Mi-17 youngest-first → Mi-8 youngest-first, линия по min free_days (tie `line_id`). Доказательство (4 датасета × 3650 дней, master+repairline): два независимых прогона побайт-идентичны (8005 vs 8006: 0 diff по всем колонкам, включая `repair_claim_line_id`); совпадение с baseline (8005 vs 8001: 0 diff по state и по line_id); ops=target PASS. Следствие: построчная регрессия по `repair_claim_line_id`/per‑line acn теперь валидна (не false‑negative); структурные изменения слоёв (напр. удаление мёртвых веток) больше не меняют раскладку. Источник: verified W_sim_deterministic_line_capture.

## Impact Paths
- `quota_rules.json → target_*` → сколько агентов должно быть в ops → управляет всем поведением квотирования
- `quota_rules.json → message_bucket QuotaBucket payload (today_ready_slots/bank_ready_slots/today_committable_slots)` → bank-only ranking switch в P2/P3 → корректный commit в bank-only дни
- `rtc_quota_v8.py → QuotaManager` → MessageBucket (key=0) → P1/P2/P3 → фактические promote
- `rtc_quota_v8.py → P2/P3 commit (детерминированный позиционный захват: commit_pos → CAS на repair_line_acn_mp, min free_days)` → захват линий; `rtc_repair_lines_v8.py → increment/write` → освобождение. (Message‑путь RepairLineStatus/LineAssignment — мёртвый, на SSoT не влияет.)
- `rtc_spawn_dynamic_v7.py → deficit/need` → динамическое создание агентов → закрытие дефицита

## Validation Proof
- INV-2 (ops vs target): `code/archive/analysis/sim_validation_quota.py` (archived 2026-06-06; legacy), `code/sim_v2/messaging/validate_limiter_ops_target.py`, `code/validation/inv2_ops_vs_target.py`
- INV-3 (repair capacity): SQL готов в `invariants.json`, validator: `code/validation/inv3_repair_capacity.py` — PASS (D1 max 10, D2 max 14, quota=18)
- TEMP-5: hybrid precondition validator — temp5_repair_hybrid_vector.py — PENDING

## Risks (≤7) + Mitigations
- P2/P3 не находят свободную RepairLine → дефицит ops → мониторинг через INV-2, компенсация через P4 spawn
- Ошибка централизованного назначения линий в QM → перерасход repair_number → мониторинг INV-3
- Удаление post‑quota пересчёта снижает «додобор» → опора на P4 spawn и INV-2

## Open Questions (≤7)
- Нужна ли P5 (резерв → operations) для горячего резервирования?
- Оптимальное значение dynamic_reserve: текущие mi8=50, mi17=50 — достаточно?
- Мёртвая message‑ветка repair‑line (`RepairLineStatus`/`LineAssignment`, слои `publish_status`/`apply_assignment`): достроить рефактор ИЛИ удалить? Удаление доказано state‑эквивалентным (8001 vs 8004), но переставляет permutation‑чувствительную line‑id телеметрию → требует permutation‑aware оракула. Перф‑выгоды нет. Отложено (workflow W_sim_repairline_qm_refactor).

## Pointers (≤15)
- `config/transitions/quota_rules.json`
- `config/transitions/invariants.json`
- `code/sim_v2/messaging/rtc_quota_v8.py`
- `code/sim_v2/messaging/rtc_quota_v8_base.py`
- `code/sim_v2/messaging/rtc_repair_lines_v8.py`
- `code/sim_v2/messaging/rtc_spawn_dynamic_v7.py`
- `code/sim_v2/messaging/base_model_messaging.py`
- `code/archive/analysis/sim_validation_quota.py` (archived 2026-06-06)
- `code/sim_v2/messaging/validate_limiter_ops_target.py`
- `docs/architecture/limiter_architecture.md`