# Context Capsule — quota

## Scope
Трогаем:
- `config/transitions/quota_rules.json` (SSoT квотирования)
- `config/transitions/invariants.json` (INV-2, INV-3, TEMP-2, TEMP-3)
- `code/sim_v2/messaging/rtc_quota_v8.py` (реализация P1/P2/P3 + MessageBucket)
- `code/sim_v2/messaging/rtc_quota_v8_base.py` (базовые функции квотирования)
- `code/sim_v2/messaging/rtc_repair_lines_v8.py` (RepairLine управление)
- `code/sim_v2/messaging/rtc_spawn_dynamic_v7.py` (P4 — динамический spawn)
- `code/sim_v2/messaging/base_model_messaging.py` (определение MessageBucket)

Не трогаем:
- Переходы состояний → `docs/transitions_capsule.md`
- Limiter/адаптивные шаги → `docs/limiter_v8_capsule.md`

## Invariants (≤12)

SSoT: `config/transitions/invariants.json`

- INV-2: ops_count ≈ target (tolerance=1, critical_threshold=3); исключение — warmup период ≤ repair_time
- INV-3: одновременно в ремонте ≤ repair_number (validator: TODO)
- TEMP-2: promote P2 невозможен пока repair_days > 0 (декремент в rtc_unsvc_decrement_v8)
- TEMP-3: ops_count < target допустимо только если sim_day < max(repair_time) (warmup)

## Decisions (≤7)

1. **MessageBucket для квот** — QM пишет 1 broadcast (key=0) с полями promote_p1/p2/p3 и deficit по типам → агенты читают → rank-based selection. Причина: O(N/K) вместо O(N²).
2. **MacroProperty для spawn** — spawn_dynamic_need/base_idx/base_acn записываются в MP per-day. Причина: индексный доступ эффективнее broadcast.
3. **Четыре приоритета (P1>P2>P3>P4)** — P1 serviceable (быстрее всего), P2 unserviceable (после ремонта), P3 inactive (начальный ввод), P4 spawn (крайняя мера).
4. **Rank = count(idx > my_idx)** — детерминистический отбор (youngest first), без race conditions.
5. **Two-Phase Commit для P2/P3** — decide фаза (repair_candidate=1, выбор repair_line_id) → commit фаза (atomic exchange aircraft_number). Причина: предотвращение двойного выделения RepairLine.
6. **Anti-fragmentation RepairLine** — выбор линии с min(free_days) где free_days >= repair_time. Причина: предотвращение накопления «жирных» линий.
7. **Shared RepairLine pool** — P2 и P3 конкурируют за один пул RepairLine slots.

## Impact Paths
- `quota_rules.json → target_*` → сколько агентов должно быть в ops → управляет всем поведением квотирования
- `quota_rules.json → repair_number` → максимум ремонтных линий → ограничивает пропускную способность P2/P3
- `rtc_quota_v8.py → QuotaManager` → broadcast → P1/P2/P3 агенты → фактические promote
- `rtc_spawn_dynamic_v7.py → need/tickets` → динамическое создание агентов → баланс при нехватке

## Validation Proof
- INV-2 (ops vs target): `code/analysis/sim_validation_quota.py`, `code/sim_v2/messaging/validate_limiter_ops_target.py`
- INV-3 (repair capacity): SQL готов в `invariants.json`, validator: TODO
- TEMP-2 (repair_days guard): механизм в RTC (rtc_unsvc_decrement_v8 + P2 guard)
- TEMP-3 (warmup): `code/analysis/sim_validation_quota.py` (учитывает warmup window)

## Risks (≤7) + Mitigations
- P2/P3 не находят свободную RepairLine → дефицит ops → мониторинг через INV-2, компенсация через P4 spawn
- Двойное выделение RepairLine → два агента на одну линию → two-phase commit + atomic exchange
- target изменён без пересчёта → несогласованность ops_count → target берётся из flight_program_ac (SSoT в ClickHouse)

## Open Questions (≤7)
- Нужна ли P5 (резерв → operations) для горячего резервирования?
- Оптимальное значение dynamic_reserve: текущие mi8=50, mi17=50 — достаточно?

## Pointers (≤15)
- `config/transitions/quota_rules.json`
- `config/transitions/invariants.json`
- `code/sim_v2/messaging/rtc_quota_v8.py`
- `code/sim_v2/messaging/rtc_quota_v8_base.py`
- `code/sim_v2/messaging/rtc_repair_lines_v8.py`
- `code/sim_v2/messaging/rtc_spawn_dynamic_v7.py`
- `code/sim_v2/messaging/base_model_messaging.py`
- `code/analysis/sim_validation_quota.py`
- `code/sim_v2/messaging/validate_limiter_ops_target.py`
