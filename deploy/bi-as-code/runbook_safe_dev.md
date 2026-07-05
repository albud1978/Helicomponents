# Safe BI Dev Runbook

Цель: менять BI безопасно через WIP-копии, не трогая published chart/dashboard до promote.

## 1. Backup

Проверить API health и снять backup текущего состояния:

```bash
source config/load_env.sh
python3 deploy/bi-as-code/scripts/bi_backup.py --dashboard-ids 1 --note "before WIP change"
```

Скрипт экспортирует timestamped bundle в `deploy/bi-as-code/superset/bundles/dashboard_1_backup_<UTC>/`, выполняет `git add -A -f` для этого каталога и ставит локальный tag `bi-backup-1-<UTC>`. Push не выполняется.

## 2. WIP-копия

Создать отдельный объект с именем вида:

- `[WIP] <original chart name> / <workflow-id>`
- `[WIP] <dashboard name> / <workflow-id>`

Для WIP-объекта держать `published:false`. Published id исходного объекта не менять до шага promote.

## 3. Update WIP

Все изменения применять только к WIP-id через MCP/API (`update_chart` или эквивалентный API-вызов). Перед patch проверить datasource mapping по `table_name`, а не по сырому `dataset_id`.

## 4. Smoke WIP

Проверить WIP через `/api/v1/chart/data` и источник Gantt:

```bash
python3 deploy/bi-as-code/scripts/bi_smoke.py \
  --chart-ids 3 \
  --version-date 20260704 \
  --version-id 1 \
  --strict
```

Для `График Ремонта` обязательны оба режима:

- без `--version-id` — проверяет, что smoke ловит смешение версий;
- с `--version-id` — проверяет чистый pinned-срез.

## 5. Promote

После PASS smoke на WIP применить promote: обновить published объект или переключить published-флаг по согласованному API-пути. Только на этом шаге разрешено менять published id.

После promote повторить smoke published-чартов:

```bash
python3 deploy/bi-as-code/scripts/bi_smoke.py --strict --version-date 20260704 --version-id 1
```

## 6. Rollback

Откат выполняется из Git tag, затем штатный import bundle. Секрет ClickHouse не хранится в bundle, поэтому пароль передаётся отдельной JSON-картой:

```bash
git checkout <tag> -- deploy/bi-as-code/superset/bundles/dashboard_1

python3 deploy/bi-as-code/scripts/superset_git_sync.py \
  import \
  --bundle-dir deploy/bi-as-code/superset/bundles/dashboard_1 \
  --overwrite \
  --passwords-file /path/to/superset_passwords.json
```

Формат `/path/to/superset_passwords.json`:

```json
{"databases/clickhouse.yaml":"REPLACE_WITH_REAL_PASSWORD"}
```

После rollback снова выполнить `bi_smoke.py --strict`.
