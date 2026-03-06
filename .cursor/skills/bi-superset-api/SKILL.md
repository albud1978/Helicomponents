---
name: bi-superset-api
description: Implements and maintains Superset BI artifacts in API-only mode for deploy/bi-as-code, including chart/dashboard fixes, datasource remap by table_name, bundle sync, and render smoke-checks. Use when user asks about Superset, dashboards, charts, datasets, BI-as-code, or API migration between instances.
---

# BI Superset API Skill

## Назначение

Этот skill применяется для BI-разработки в `deploy/bi-as-code/**` в режиме API-only.
По правилам проекта основной исполнитель BI-разработки в мультиагентном контуре: `coder-general`.

## Когда применять

Применяй skill, если пользователь просит:
- исправить/добавить dashboard, chart, dataset в Superset;
- синхронизировать bundle между инстансами;
- устранить `Data error` в чартах;
- проверить рендер и корректность SQL через API;
- описать/настроить API-only процесс для Superset.

## Обязательные ограничения

1. Не управлять Docker/Superset runtime из этого репозитория.
2. Работать только через Superset API (`SUPERSET_API_*` из `.env`).
3. Между инстансами не доверять "сырым" `dataset_id`; маппить datasource по `table_name`.
4. Для `api_user` не использовать admin-only endpoints (users/roles/permissions/db admin), если нет явной задачи на provisioning от администратора.

## Базовый workflow

1) Подключение:
- `GET /health`
- `POST /api/v1/security/login`
- `GET /api/v1/security/csrf_token/`

2) Диагностика объекта:
- прочитать `chart/dashboard/dataset` через API;
- проверить, что datasource чарта указывает на правильный `table_name`.

3) Правка:
- обновить chart/dashboard через API (`PUT/POST`);
- при переносе между инстансами выполнить rebinding по `table_name`.

4) Валидация рендера:
- `POST /api/v1/chart/data` для ключевых чартов;
- критерий: HTTP 200, без `UNKNOWN_IDENTIFIER`/`Columns missing in dataset`.

5) Синхронизация артефактов:
- экспортировать актуальный bundle в `deploy/bi-as-code/superset/bundles/dashboard_1`;
- обновить документацию (`docs/changelog.md`, при необходимости `deploy/bi-as-code/README.md` и playbook).

## Режимы выполнения

### Multi-agent
- Реализация BI: `coder-general`
- BI semantics: `bi-semantic-analyst`
- Routine SQL checks: `sql-checker`
- Repo / graph research: `research-graph-analyst`
- Документация: `docs-curator`

### Single-agent
- Один агент выполняет те же шаги последовательно: реализация -> semantic checks -> smoke-check -> doc sync.

## Мини-checklist перед завершением

- datasource mapping выполнен по `table_name`;
- `chart/data` smoke-check пройден для целевых чартов;
- bundle экспортирован после правок;
- документация синхронизирована;
- указано, какие действия были admin-only и кто их должен выполнять.
