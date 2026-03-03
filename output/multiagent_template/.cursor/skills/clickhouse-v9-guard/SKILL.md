---
name: sql-db-guard
description: Performs safe database analysis with strict dataset scoping and read-only SQL discipline. Use for SQL analytics, validations, transition counts, and DB-backed diagnostics. Adapt to your database engine and schema.
---

# SQL/DB Guard (Template)

## Когда применять

Используй skill для задач, где есть:
- SQL/ClickHouse анализ;
- валидации по данным;
- аналитика переходов `from_status -> to_status` (или аналогичные поля);
- любые запросы к таблицам operational/simulation домена.

## Базовые правила

1. По умолчанию только `SELECT` (мутации только по явному разрешению человека).
2. Всегда фиксируй dataset scope (например: `version_date`, `version_id`, `group_by`, `tenant_id` — по модели проекта).
3. Не смешивай доменные срезы (группы/тенанты/версии) без явной цели.
4. Перед выводами проверяй схему:

```sql
DESCRIBE TABLE <table_name>;
```

5. Если пользователь пишет «последний прогон», явно зафиксируй критерий (например, `max(version_date)` по `version_id`).

## Версия СУБД и совместимость

- Зафиксируй в отчёте текущую версию сервера:

```sql
SELECT version();
```

- Для конкретного проекта укажи «базовый референс» версии в документации.
- Если версия отличается от референса — явно отмечай риск несовместимости SQL.

## Шаблон краткого отчёта

В финальном ответе укажи:
1. Какие таблицы и поля проверены (включая `DESCRIBE`).
2. Какие фильтры применены (dataset scope поля проекта).
3. Какой критерий «последнего прогона» использован.
4. Табличный результат.
5. Допущения с `Risks if false: ...`.

