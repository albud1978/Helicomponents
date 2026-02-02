# Validation

## Основные правила
- `docs/architecture/validation_rules.md` — канон SQL‑first валидации и инвариантов.

## Контекстные капсулы
- Линт капсулы: `python code/analysis/context_capsule_builder.py --lint docs/limiter_v8_capsule.md`
- Формат капсулы: `docs/limiter_v8_capsule.md` (строгий шаблон и лимиты секций).

## Local KG (Neo4j)
- Запуск: `make kg-up`
- Останов: `make kg-down`
- Логи: `make kg-logs`
