# Validation

## SSoT инвариантов
- **`config/transitions/invariants.json`** — единый формализованный реестр всех инвариантов, temporal-контрактов и GPU-ограничений модели V8.
  - INV-1..INV-9: глобальные инварианты (sne ≤ ll, ops = target, repair ≤ repair_number, баланс наработок и др.)
  - TEMP-1..TEMP-4: temporal-контракты (длительность ремонта, минимальное время в unsvc, liveness)
  - GPU-1..GPU-6: ограничения платформы FLAME GPU (read/write, mp5_lin read-only, Float64 запрет и др.)
- Правило Cursor: `.cursor/rules/25_invariants_contract.mdc` — автоматически подтягивается при работе с `code/sim_v2/**`, `code/validation/**`, `code/analysis/**`, `config/transitions/**`.

## Методология
- `docs/architecture/validation_rules.md` — методология SQL‑first валидации, heli_pandas проверки, NVRTC правила.

## Связанные SSoT
- `config/transitions/transitions_rules.json` — матрица переходов, condition precedent/subsequent, порядок RTC.
- `config/transitions/quota_rules.json` — логика квотирования, RepairLine, spawn.

## Контекстные капсулы
- Формат капсулы проверяется вручную по шаблону секций (8 обязательных секций с лимитами).
- Формат капсулы: `docs/limiter_v8_capsule.md` (строгий шаблон и лимиты секций).

## Графы знаний
См. `README.md` → секция "Графы знаний (Neo4j)".
