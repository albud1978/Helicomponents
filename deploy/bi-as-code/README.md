# BI as Code (sandbox + handoff)

## Область и текущие ограничения
- Текущий контур: `personal sandbox -> corporate sandbox`.
- Поддерживаемый режим переноса: **только Mode B (repo-only)**.
- Exact clone переведён в архив и в штатном процессе не используется.
- Продакшен из этого репозитория не деплоится: только handoff-пакет для внешних администраторов.
- Любой deploy/clone в corporate sandbox выполняется только по явной команде человека.

## Активный контракт доступа (обязательно)
- Режим работы в этом репозитории: **API-only** (create/update dashboards/charts/datasets, export/import bundle).
- Адрес Superset берётся из переменной окружения: `SUPERSET_API_BASE_URL`.
  - Пример универсального значения: `https://superset.example.corp`.
- Учетные данные/параметры:
  - `SUPERSET_API_BASE_URL`
  - `SUPERSET_API_PROVIDER`
  - `SUPERSET_API_USERNAME`
  - `SUPERSET_API_PASSWORD`
  - `SUPERSET_API_TIMEOUT_SEC`
- **Запрещено** управлять Docker/Superset runtime из этого репозитория.
- Нужна роль API-пользователя с правами на dashboards/charts/datasets и import/export.

### Уточнение по `SUPERSET_API_PROVIDER`
- Текущее значение: `SUPERSET_API_PROVIDER=db`.
- Это backend аутентификации для `/api/v1/security/login`, а не "провайдер API-ключа".
- Модель авторизации в нашем контуре:
  1. Login по `username/password/provider=db` -> получение `access_token` (JWT)
  2. Получение `csrf_token` через `/api/v1/security/csrf_token/`
  3. Все `POST/PUT/DELETE` выполняются с `Bearer + X-CSRFToken`

## Роли (строгая BI-модель)
- `orchestrator`: планирование фаз, встроенные `pre_gate` / `pre_close`, сбор handoff.
- `research-graph-analyst`: разведка по репо, graph impact, структурный контекст.
- `bi-semantic-analyst`: проверка семантики KPI, агрегаций, фильтров и scope.
- `sql-checker`: регулярные SQL-проверки по заданной логике.
- `coder-general`: поддержка BI-артефактов и API-applier в `deploy/bi-as-code/**`.
- `governance-compliance`: policy / human-gate / traceability checker для `pre_gate` / `pre_close`.
- `docs-curator`: синхронизация runbook/changelog/документации.
- `corp BI admins` (внешний контур): выполнение production deploy по утверждённому runbook.

## Структура каталога
- `contracts/` - семантические и брендовые контракты.
- `domains/` - доменные BI-декларации.
- `superset/datasets/` - манифесты датасетов.
- `superset/charts/` - манифесты чартов.
- `superset/dashboards/` - манифесты дашбордов и layout metadata.
- `scripts/` - утилиты export/import/dry-run.
- `release/` - handoff-артефакты для review/prod.

## Операционные гейты
1. Согласованы scope и role matrix.
2. Определён `semantic_contract`.
3. Получен и сопоставлен `brandbook_contract`.
4. Получена явная команда на deploy/clone в corporate sandbox.
5. Подготовлен и валидирован production handoff package (без prod apply).

## Правила безопасности
- Секреты не коммитим; использовать переменные окружения/секрет-хранилища.
- Любые артефакты должны быть идемпотентны и ревью-пригодны в Git.
- Экстренные runtime-фиксы обязательно backport в source-артефакты.

## Safe-dev цикл для BI
- Подробный порядок: `deploy/bi-as-code/runbook_safe_dev.md`.
- `scripts/bi_backup.py` снимает timestamped backup через Superset API export и ставит локальный `bi-backup-*` tag без push.
- `scripts/bi_smoke.py` проверяет `/api/v1/chart/data`; для `График Ремонта` дополнительно проверяет overlap по `sim_repairline_v9` напрямую в ClickHouse.
- Published chart/dashboard id не менять до promote; все правки сначала идут в `[WIP]`-копию с `published:false`.

## Политика секретов (обязательно)
- Локальный рабочий файл: `.env.private` (или локальный `.env`, не в Git).
- Шаблон для передачи между проектами: `.env.template` / `.env.example` без реальных значений.
- После миграций/копирования репозитория в новый контур выполнять ротацию:
  - `SUPERSET_API_PASSWORD`
  - `CLICKHOUSE_PASSWORD`
  - `NEO4J_PASSWORD` / `DOMAIN_NEO4J_PASSWORD`
  - `AURA_API_KEY`
- Запрещено передавать реальные секреты через commit, bundle и текстовые отчеты.

## Модель изоляции инстансов
- Каждый инстанс Superset изолирован (своя metadata DB).
- Состояние между инстансами передаётся через Git bundle.
- Без явного import содержимое инстансов расходится.

### Что означает "одинаковое содержимое"
После bootstrap новый инстанс пуст. Чтобы синхронизировать:
1) `git pull`
2) `import --overwrite` из bundle (см. ниже)

## Git migration mode (A <-> B)

### Что хранится в Git для BI-переноса
- `deploy/bi-as-code/contracts/**`
- `deploy/bi-as-code/superset/**`
- `deploy/bi-as-code/scripts/superset_git_sync.py`
- `deploy/superset-local/**` как архивные reference-артефакты (исполнение из этого репозитория запрещено API-only политикой)
- `superset-frontend/plugins/plugin-chart-echarts6-gantt/**`
- `.cursor/hooks/code_edit_audit.log` версионируется
- `.cursor/hooks/user_comm_audit.log` локальный и в Git не синхронизируется

### Onboarding нового агента (обязательно)
1) Прочитать `README.md` (секция BI/Superset).
2) Прочитать этот файл `deploy/bi-as-code/README.md`.
3) Прочитать `.cursor/rules/00_global_always.mdc` и `.cursor/rules/90_multiagent_workflow.mdc`.
4) Проверить audit:
   - `.cursor/hooks/code_edit_audit.log` (в Git)
   - `.cursor/hooks/user_comm_audit.log` (локально)
5) Проверить актуальный bundle:
   - `deploy/bi-as-code/superset/bundles/dashboard_1/`

## Экспорт текущего BI-состояния в Git (машина A)
```bash
python "deploy/bi-as-code/scripts/superset_git_sync.py" \
  --base-url "${SUPERSET_API_BASE_URL}" \
  --username "${SUPERSET_API_USERNAME}" \
  --password "${SUPERSET_API_PASSWORD}" \
  --provider "${SUPERSET_API_PROVIDER:-db}" \
  export \
  --dashboard-ids "1" \
  --output-dir "deploy/bi-as-code/superset/bundles/dashboard_1"
```

```bash
git add deploy/bi-as-code/superset/bundles/dashboard_1
git commit -m "sync superset dashboard bundle"
git push
```

## Импорт BI-состояния из Git (машина B)
```bash
git pull
python "deploy/bi-as-code/scripts/superset_git_sync.py" \
  --base-url "${SUPERSET_API_BASE_URL}" \
  --username "${SUPERSET_API_USERNAME}" \
  --password "${SUPERSET_API_PASSWORD}" \
  --provider "${SUPERSET_API_PROVIDER:-db}" \
  import \
  --bundle-dir "deploy/bi-as-code/superset/bundles/dashboard_1" \
  --overwrite
```

## Критичное правило маппинга после import
- Нельзя доверять "сырым" числовым `dataset_id` между инстансами.
- `dataset_id` — локальный metadata ID и может указывать на другую физическую таблицу.
- При rebinding chart/filter сначала маппить datasource по `table_name`, затем подставлять локальный `dataset_id`.
- Типовые сигнатуры неправильного маппинга:
  - `UNKNOWN_IDENTIFIER` для валидных полей (`pre_status_id`, `status_count_ffill` и т.д.);
  - `Columns missing in dataset` для `line_id`, `day_u16`, `aircraft_number`.

## Post-import smoke-check (обязательно)
- До handoff проверить ключевые чарты через API `/api/v1/chart/data`:
  - `Датасет 1`
  - `Датасет 2`
  - `График Ремонта`
  - `График поставки ВС`
- Критерий успеха: все ответы `200`, без ClickHouse identifier/column ошибок.

## Регулярный цикл синхронизации
- A изменил BI в UI -> export -> commit/push.
- B сделал pull -> import --overwrite.
- B изменил BI в UI -> export -> commit/push.
- A сделал pull -> import --overwrite.

## Чеклист после `git pull` (Mode B)
1) Подтвердить, что контур API-only.
2) Проверить переменные `SUPERSET_API_*` в локальной `.env`.
3) Проверить health:
```bash
curl -s -o /dev/null -w "%{http_code}\n" "${SUPERSET_API_BASE_URL}/health"
```
4) Выполнить import bundle.
5) Выполнить smoke-check ключевых чартов.
6) Если требуется карта паролей БД, использовать ключ `databases/clickhouse.yaml`.
7) Если чарт не рендерится, сначала `Ctrl+Shift+R`, затем повторный import/export sync.

## Критические примечания
- `.env` локальный файл, не коммитится.
- Скрипты exact clone в `deploy/superset-local/scripts/*exact*` оставлены как архивные reference-артефакты.
- Если bundle содержит database YAML с секретами, при import передавать JSON-карты:
  - `--passwords-file`
  - `--ssh-tunnel-passwords-file`
  - `--ssh-tunnel-private-key-passwords-file`
  - `--ssh-tunnel-private-keys-file`
- Для `--passwords-file` ключ должен быть путём внутри ZIP bundle, а не display name базы.
  - Для текущего bundle: `databases/clickhouse.yaml`.

Пример:
```bash
cat > /tmp/superset_passwords.json <<'EOF'
{"databases/clickhouse.yaml":"REPLACE_WITH_REAL_PASSWORD"}
EOF

python "deploy/bi-as-code/scripts/superset_git_sync.py" \
  --base-url "${SUPERSET_API_BASE_URL}" \
  --username "${SUPERSET_API_USERNAME}" \
  --password "${SUPERSET_API_PASSWORD}" \
  --provider "${SUPERSET_API_PROVIDER:-db}" \
  import \
  --bundle-dir "deploy/bi-as-code/superset/bundles/dashboard_1" \
  --overwrite \
  --passwords-file "/tmp/superset_passwords.json"
```
