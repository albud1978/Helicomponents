# Context Capsule — validation

## Scope
Трогаем:
- `config/transitions/invariants.json` (SSoT инвариантов — 9 глобальных, 4 временных, 6 GPU)
- `code/analysis/sim_validation_runner_msg.py` (оркестратор валидации MESSAGING)
- `code/analysis/sim_validation_quota.py` (INV-2: ops vs target)
- `code/analysis/sim_validation_increments.py` (INV-6: dt>0 только в ops)
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
| INV-1 (sne ≤ ll) | SQL в invariants.json | SQL готов, скрипта нет |
| INV-2 (ops ≈ target) | sim_validation_quota.py + validate_limiter_ops_target.py | OK |
| INV-3 (ремонт ≤ capacity) | TODO | SQL готов, скрипта нет |
| INV-4 (repair_time guard) | механизм в RTC | RTC-only |
| INV-5 (баланс Σdt = Δsne) | validate_state2ops_increments.py | OK |
| INV-6 (dt>0 → ops only) | sim_validation_increments.py | OK |
| INV-7 (dt = mp5_lin) | validate_limiter_flight_hours.py | OK |
| INV-8 (storage frozen) | validation_rules.py (InvariantValidator) | OK |
| INV-9 (limiter=0 → exit) | sim_validation_runner_msg.py | OK |

## Decisions (≤7)

1. **SQL-first** — итоговая проверка по данным СУБД (sim_masterv2 экспорт), не по in-memory состоянию GPU. Причина: воспроизводимость + аудит.
2. **Два оркестратора** — sim_validation_runner.py (legacy V7) и sim_validation_runner_msg.py (MESSAGING V8). Причина: разные архитектуры intent_state vs state-based.
3. **Baseline заморожен** — sim_masterv2 baseline не изменяется; сравнение с ним только по запросу.
4. **Реальные данные only** — синтетика/заглушки запрещены без явного разрешения.
5. **JIT warnings = дефект** — каждый NVRTC warning исправляется немедленно (GPU-4).

## Impact Paths
- `invariants.json` → определяет ЧТО проверять → все скрипты валидации
- `sim_masterv2` (ClickHouse) → данные для проверки → SQL-запросы
- `sim_validation_runner_msg.py` → вызывает все суб-валидаторы → итоговый отчёт
- Результат валидации → решение о коммите (оркестратор workflow)

## Validation Proof
Рекурсивное: капсула описывает саму систему валидации. Верификация — ручной аудит + SQL-запросы к реальным данным.

## Risks (≤7) + Mitigations
- Пробелы покрытия (INV-1, INV-3 без скриптов) → TODO в invariants.json; SQL готов
- Расхождение V7/V8 валидации → разные оркестраторы, но общие принципы; унификация в будущем
- Stale данные в sim_masterv2 → каждый прогон перезаписывает по version_date
- Ложноположительные (warmup window) → INV-2 и TEMP-3 учитывают warmup

## Open Questions (≤7)
- Объединять ли sim_validation_runner.py и sim_validation_runner_msg.py?
- Нужен ли автоматический Python-скрипт для INV-1 (сейчас только SQL)?

## Pointers (≤15)
- `config/transitions/invariants.json`
- `code/analysis/sim_validation_runner_msg.py`
- `code/analysis/sim_validation_runner.py`
- `code/analysis/sim_validation_quota.py`
- `code/analysis/sim_validation_increments.py`
- `code/analysis/sim_validation_transitions.py`
- `code/validation/validate_state2ops_increments.py`
- `code/validation/validate_state2ops_transitions.py`
- `code/sim_v2/messaging/validate_limiter_flight_hours.py`
- `code/sim_v2/messaging/validate_limiter_ops_target.py`
- `code/sim_v2/messaging/validate_limiter_v3.py`
- `code/sim_v2/components/validation_rules.py`
- `docs/architecture/validation_rules.md`
