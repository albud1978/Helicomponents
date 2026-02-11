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
- `docs/architecture/limiter_architecture.md` (канон V8: message-only квоты)

Не трогаем:
- Переходы состояний → `docs/transitions_capsule.md`
- Limiter/адаптивные шаги → `docs/limiter_v8_capsule.md`

## Invariants (≤12)

SSoT: `config/transitions/invariants.json`

- INV-2: ops_count ≈ target (tolerance=1, critical_threshold=3); исключение — warmup период ≤ repair_time
- INV-3: одновременно в ремонте ≤ repair_number (validator: TODO)
- TEMP-2: promote P2 невозможен пока repair_days > 0 (декремент в rtc_unsvc_decrement_v8)
- TEMP-3: ops_count < target допустимо только если sim_day < max(repair_time) (warmup)

## Decisions (Key Decisions) (≤7)

1. **Message-based квотирование** — только MessageBucket, без CAS/commit в MacroProperty для P1/P2/P3. QM шлёт 1 broadcast (key=0) с promote/deficit → агенты читают → rank-based selection. Причина: O(N/K) вместо O(N²). Источник: `docs/architecture/limiter_architecture.md`, `code/sim_v2/messaging/rtc_quota_v8.py`.
2. **RepairLine публикует доступность через сообщения** (только если free_days >= repair_time); QM потребляет и выбирает линию с min(free_days) — анти‑фрагментация. Источник: `docs/architecture/limiter_architecture.md`, `code/sim_v2/messaging/rtc_repair_lines_v8.py`.
3. **QM централизованно назначает и освобождает линии** (shared pool, Mi‑17 first) через LineAssignment; commit‑слои 33–34 удалены. Источник: `code/sim_v2/messaging/rtc_quota_v8.py`.
4. **Четыре приоритета (P1>P2>P3>P4)** — P1 serviceable, P2 unserviceable, P3 inactive, P4 spawn. Источник: `docs/architecture/limiter_architecture.md`, `config/transitions/quota_rules.json`.
5. **Rank = count(idx > my_idx)** — детерминистический отбор (youngest first), без race conditions. Источник: `docs/architecture/limiter_architecture.md`.
6. **Дефицит spawn**: deficit = target − curr_ops (без двойного учёта) — `code/sim_v2/messaging/rtc_spawn_dynamic_v7.py`.
7. **Только message‑only модуль**: register_quota_v8_full и legacy agent‑based P2/P3 удалены; остаётся `register_quota_v8_messages`. Источник: `docs/architecture/limiter_architecture.md`.

## Impact Paths
- `quota_rules.json → target_*` → сколько агентов должно быть в ops → управляет всем поведением квотирования
- `quota_rules.json → repair_number` → максимум ремонтных линий → ограничивает пропускную способность P2/P3
- `rtc_quota_v8.py → QuotaManager` → MessageBucket (key=0) → P1/P2/P3 → фактические promote
- `rtc_repair_lines_v8.py → publish status` → QM назначает LineAssignment → P2/P3 корректно используют линии
- `rtc_spawn_dynamic_v7.py → deficit/need` → динамическое создание агентов → закрытие дефицита

## Validation Proof
- INV-2 (ops vs target): `code/analysis/sim_validation_quota.py`, `code/sim_v2/messaging/validate_limiter_ops_target.py`
- INV-3 (repair capacity): SQL готов в `invariants.json`, validator: TODO
- TEMP-2 (repair_days guard): механизм в RTC (rtc_unsvc_decrement_v8 + P2 guard)
- TEMP-3 (warmup): `code/analysis/sim_validation_quota.py` (учитывает warmup window)

## Risks (≤7) + Mitigations
- P2/P3 не находят свободную RepairLine → дефицит ops → мониторинг через INV-2, компенсация через P4 spawn
- Ошибка централизованного назначения линий в QM → перерасход repair_number → мониторинг INV-3
- Удаление post‑quota пересчёта снижает «додобор» → опора на P4 spawn и INV-2

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
- `docs/architecture/limiter_architecture.md`