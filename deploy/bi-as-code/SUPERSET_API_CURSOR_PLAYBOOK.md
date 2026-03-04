# Superset API-only Playbook for Cursor AI

## Назначение
- Этот документ предназначен для передачи в другие проекты как стандарт подключения и работы с Superset через API.
- Контур: только API-режим, без управления Docker runtime из репозитория.

## Контекст Cursor AI

| Параметр | Значение |
|---|---|
| Среда | Cursor AI (агентный режим) |
| Текущая модель в нашем контуре | `gpt-5.3-codex-high` |
| Базовый Superset endpoint | `http://10.96.96.47:8088/` |
| Обязательный режим работы | API-only |
| Docker runtime из этого репозитория | Запрещен политикой |

## Кто выполняет BI-разработку (по нашим правилам)

| Контур | Ответственный |
|---|---|
| Мультиагентный workflow (по `.cursor/rules/90_multiagent_workflow.mdc`) | `coder-general` |
| Семантическая BI-проверка (метрики/агрегации/фильтры) | `analyst-sql-graph` |
| Документация BI артефактов | `docs-curator` |
| Оркестрация и гейты | `orchestrator` |

Рекомендуемый project-skill для Cursor AI:
- `.cursor/skills/bi-superset-api/SKILL.md` (применять для задач Superset BI-as-code в API-only режиме).

## Режимы использования playbook

| Режим | Как применять |
|---|---|
| Multi-agent | Оркестратор делегирует реализацию BI в `coder-general`, SQL/semantic checks в `analyst-sql-graph`, документацию в `docs-curator` |
| Single-agent | Один агент выполняет те же шаги последовательно: реализация -> semantic checks -> smoke-check -> doc sync |

Принцип совместимости: содержание этого playbook одинаково для обоих режимов; меняется только распределение ролей.

## Переменные окружения

| Переменная | Назначение | Пример |
|---|---|---|
| `SUPERSET_API_BASE_URL` | Базовый URL Superset API | `http://10.96.96.47:8088` |
| `SUPERSET_API_PROVIDER` | Провайдер авторизации | `db` |
| `SUPERSET_API_USERNAME` | Логин API-пользователя | `bi_api_user` |
| `SUPERSET_API_PASSWORD` | Пароль API-пользователя | `***` |
| `SUPERSET_API_TIMEOUT_SEC` | Таймаут HTTP-запросов | `120` |

## Обязательный Docker-guard hook (рекомендация для всех проектов)

| Пункт | Рекомендация |
|---|---|
| Что сделать | Включить preToolUse hook, который блокирует любые Docker/Superset runtime команды в этом репозитории |
| Зачем | Исключить случайное управление runtime из проекта, где разрешен только API-only режим |
| Где хранить | `.cursor/hooks/superset_docker_guard.py` + регистрация в `.cursor/hooks.json` |
| Что блокировать | `docker`, `docker compose`, `deploy/superset-local/*`, `start_local_plugin.sh`, `build_superset_with_plugin.sh` |

Пример минимального guard-скрипта:

```python
#!/usr/bin/env python3
import json
import re
import sys

DOCKER_PATTERN = re.compile(r"(^|\\s)docker(\\s|$)")
BLOCK_MARKERS = (
    "deploy/superset-local/",
    "start_local_plugin.sh",
    "build_superset_with_plugin.sh",
    "superset-local",
)

def allow():
    sys.stdout.write(json.dumps({"decision": "allow"}))

def deny(reason: str):
    sys.stdout.write(json.dumps({"decision": "deny", "reason": reason}))

def main():
    raw = sys.stdin.read()
    payload = json.loads(raw) if raw else {}
    if payload.get("tool_name") != "Shell":
        allow()
        return
    tool_input = payload.get("tool_input", {})
    command = (tool_input.get("command") or payload.get("command") or "").strip().lower()
    if command and (DOCKER_PATTERN.search(command) or any(m in command for m in BLOCK_MARKERS)):
        deny("Политика API-only: Docker/Superset runtime запрещен в этом репозитории.")
        return
    allow()

if __name__ == "__main__":
    main()
```

Пример подключения в `.cursor/hooks.json`:

```json
{
  "preToolUse": [
    { "command": "python3 .cursor/hooks/superset_docker_guard.py" }
  ]
}
```

## Порядок подключения (обязательный)

| Шаг | Действие | Endpoint | Метод | Обязательно |
|---|---|---|---|---|
| 1 | Проверить доступность сервиса | `/health` | `GET` | Да |
| 2 | Получить access token | `/api/v1/security/login` | `POST` | Да |
| 3 | Получить CSRF token для write-операций | `/api/v1/security/csrf_token/` | `GET` | Да |
| 4 | Выполнить целевые read/write операции | `/api/v1/*` | `GET/POST/PUT/DELETE` | Да |
| 5 | Выполнить smoke-check рендера | `/api/v1/chart/data` | `POST` | Да |

## Матрица API-методов и прав

### Рабочие методы (BI-as-code)

| Зона | Endpoint pattern | Методы | API user (non-admin) | Admin | Комментарий |
|---|---|---|---|---|---|
| Аутентификация | `/api/v1/security/login` | `POST` | ✅ | ✅ | Выдача JWT |
| CSRF | `/api/v1/security/csrf_token/` | `GET` | ✅ | ✅ | Нужен для write |
| Датасеты | `/api/v1/dataset/`, `/api/v1/dataset/{id}` | `GET/POST/PUT/DELETE` | ✅* | ✅ | Доступ зависит от RBAC |
| Чарты | `/api/v1/chart/`, `/api/v1/chart/{id}` | `GET/POST/PUT/DELETE` | ✅* | ✅ | Основной слой BI |
| Дашборды | `/api/v1/dashboard/`, `/api/v1/dashboard/{id}` | `GET/POST/PUT/DELETE` | ✅* | ✅ | Основной слой BI |
| Export bundle | `/api/v1/dashboard/export/` | `GET` | ✅* | ✅ | Экспорт ZIP |
| Import bundle | `/api/v1/dashboard/import/` | `POST` | ✅* | ✅ | Обычно с `overwrite` |
| Рендер/SQL чарта | `/api/v1/chart/data` | `POST` | ✅* | ✅ | Техническая проверка |
| SQL Lab (опц.) | `/api/v1/sqllab/*` | `GET/POST` | ⚠️ опционально | ✅ | Часто отключено для API user |

\* только при явном назначении соответствующих RBAC permissions.

### Методы/зоны, обычно доступные только админу

| Зона | Endpoint pattern | Методы | Типовой доступ |
|---|---|---|---|
| Пользователи | `/api/v1/user/`, `/api/v1/user/{id}` | `GET/POST/PUT/DELETE` | Admin / Security Manager |
| Роли | `/api/v1/role/`, `/api/v1/role/{id}` | `GET/POST/PUT/DELETE` | Admin / Security Manager |
| Permission management | `/api/v1/security/permissions/*` | `GET/POST/PUT/DELETE` | Admin / Security Manager |
| Управление DB-коннектами | `/api/v1/database/`, `/api/v1/database/{id}` | `POST/PUT/DELETE` | Admin (или отдельная повышенная роль) |
| Security policy объекты (RLS/guest/security) | Security endpoints | `POST/PUT/DELETE` | Admin / Security Manager |

## Роль `api_user`: расширенный реестр разрешений (dashboard-as-a-code)

Ниже приведен реестр, который используется в нашем контуре для роли `api_user` (по вашему каталогу "144 permissions").  
Формат: `permission [id]`.

### Глобальный доступ

| Разрешения |
|---|
| `all database access on all_database_access [212]` |
| `all datasource access on all_datasource_access [211]` |

### Dashboard

| Разрешения |
|---|
| `can read on Dashboard [15]` |
| `can write on Dashboard [16]` |
| `can export on Dashboard [104]` |
| `can get embedded on Dashboard [106]` |
| `can set embedded on Dashboard [108]` |
| `can delete embedded on Dashboard [107]` |
| `can cache dashboard screenshot on Dashboard [105]` |
| `can drill on Dashboard [220]` |
| `can view chart as table on Dashboard [219]` |
| `can view query on Dashboard [218]` |
| `can tag on Dashboard [222]` |
| `can read on DashboardFilterStateRestApi [101]` |
| `can write on DashboardFilterStateRestApi [102]` |
| `can read on DashboardPermalinkRestApi [103]` |
| `can write on DashboardPermalinkRestApi [104]` |
| `can read on EmbeddedDashboard [116]` |
| `menu access on Dashboards [197]` |

### Chart

| Разрешения |
|---|
| `can read on Chart [7]` |
| `can write on Chart [8]` |
| `can export on Chart [93]` |
| `can warm up cache on Chart [94]` |
| `can tag on Chart [221]` |
| `menu access on Charts [198]` |

### Dataset

| Разрешения |
|---|
| `can read on Dataset [11]` |
| `can write on Dataset [12]` |
| `can export on Dataset [110]` |
| `can duplicate on Dataset [113]` |
| `can get or create dataset on Dataset [114]` |
| `can get drill info on Dataset [111]` |
| `can warm up cache on Dataset [112]` |
| `menu access on Datasets [199]` |

### Database

| Разрешения |
|---|
| `can read on Database [17]` |
| `can write on Database [18]` |
| `can export on Database [109]` |
| `can upload on Database [25]` |
| `menu access on Databases [196]` |

### Datasource / API query

| Разрешения |
|---|
| `can get on Datasource [149]` |
| `can get column values on Datasource [115]` |
| `can external metadata on Datasource [151]` |
| `can external metadata by name on Datasource [152]` |
| `can samples on Datasource [153]` |
| `can query on Api [148]` |
| `can query form data on Api [147]` |
| `can time range on Api [146]` |

### Explore

| Разрешения |
|---|
| `can explore on Superset [155]` |
| `can explore json on Superset [162]` |
| `can read on Explore [117]` |
| `can read on ExploreFormDataRestApi [119]` |
| `can write on ExploreFormDataRestApi [118]` |
| `can read on ExplorePermalinkRestApi [121]` |
| `can write on ExplorePermalinkRestApi [120]` |
| `can fetch datasource metadata on Superset [161]` |

### SQL Lab

| Разрешения |
|---|
| `can read on SQLLab [133]` |
| `can execute sql query on SQLLab [135]` |
| `can get results on SQLLab [132]` |
| `can estimate query cost on SQLLab [134]` |
| `can export csv on SQLLab [131]` |
| `can format sql on SQLLab [130]` |
| `can sqllab on Superset [217]` |
| `can sqllab history on Superset [158]` |
| `can csv on Superset [214]` |
| `can read on SqlLabPermalinkRestApi [137]` |
| `can write on SqlLabPermalinkRestApi [136]` |
| `can read on Query [19]` |
| `menu access on SQL Lab [207]` |
| `menu access on SQL Editor [208]` |
| `menu access on Query Search [210]` |
| `menu access on Saved Queries [209]` |

### TabState / TableSchema

| Разрешения |
|---|
| `can activate on TabStateView [174]` |
| `can get on TabStateView [168]` |
| `can post on TabStateView [170]` |
| `can put on TabStateView [169]` |
| `can delete on TabStateView [173]` |
| `can delete query on TabStateView [171]` |
| `can migrate query on TabStateView [172]` |
| `can delete on TableSchemaView [166]` |
| `can expanded on TableSchemaView [167]` |
| `can post on TableSchemaView [165]` |

### SavedQuery

| Разрешения |
|---|
| `can read on SavedQuery [1]` |
| `can write on SavedQuery [2]` |
| `can export on SavedQuery [126]` |
| `can list on SavedQuery [125]` |

### Import / Export API

| Разрешения |
|---|
| `can import on ImportExportRestApi [123]` |
| `can export on ImportExportRestApi [122]` |

### Tags

| Разрешения |
|---|
| `can read on Tag [129]` |
| `can write on Tag [128]` |
| `can bulk create on Tag [127]` |
| `can tags on TagView [176]` |
| `can list on Tags [175]` |
| `menu access on Tags [204]` |

### Reports / Alerts

| Разрешения |
|---|
| `can read on ReportSchedule [5]` |
| `can write on ReportSchedule [6]` |
| `menu access on Alerts & Report [205]` |

### Annotation

| Разрешения |
|---|
| `can read on Annotation [9]` |
| `can write on Annotation [10]` |
| `menu access on Annotation Layers [206]` |

### Security / Auth related

| Разрешения |
|---|
| `can read on SecurityRestApi [185]` |
| `can read on security [139]` |
| `can read on RowLevelSecurity [186]` |
| `can read on CurrentUserRestApi [99]` |
| `can write on CurrentUserRestApi [100]` |
| `can userinfo on UserDBModelView [34]` |
| `resetmypassword on UserDBModelView [39]` |
| `can this form get on ResetMyPasswordView [30]` |
| `can this form post on ResetMyPasswordView [31]` |
| `can add on UserRegistrationsRestAPI [179]` |
| `can delete on UserRegistrationsRestAPI [181]` |
| `can edit on UserRegistrationsRestAPI [182]` |
| `can list on UserRegistrationsRestAPI [183]` |
| `can show on UserRegistrationsRestAPI [180]` |
| `can read on user [id не указан в реестре]` |

### Theme / CSS

| Разрешения |
|---|
| `can read on Theme [95]` |
| `can write on Theme [96]` |
| `can export on Theme [97]` |
| `can read on CssTemplate [3]` |
| `can write on CssTemplate [4]` |
| `menu access on Themes [203]` |
| `menu access on CSS Templates [202]` |

### Permalinks / Share

| Разрешения |
|---|
| `can dashboard permalink on Superset [156]` |
| `can dashboard on Superset [160]` |
| `can share dashboard on Superset [215]` |
| `can share chart on Superset [216]` |
| `can slice on Superset [163]` |

### Superset core

| Разрешения |
|---|
| `can warm up cache on Superset [157]` |
| `can language pack on Superset [164]` |
| `can invalidate on CacheRestApi [92]` |
| `can read on AvailableDomains [91]` |
| `can read on AdvancedDataType [90]` |
| `can get on OpenApi [86]` |
| `can show on SwaggerView [87]` |
| `can get on MenuApi [88]` |
| `can list on AsyncEventsRestApi [89]` |
| `can recent activity on Log [138]` |

### Plugins / Dynamic

| Разрешения |
|---|
| `can show on DynamicPlugin [142]` |
| `menu access on Plugins [201]` |

### Menu navigation

| Разрешения |
|---|
| `menu access on Home [194]` |
| `menu access on Data [195]` |
| `menu access on Manage [200]` |
| `menu access on Action Log [192]` |

## Provisioning `api_user` и рабочее использование через API (dashboard-as-a-code)

Важно: шаг создания/назначения роли сервисному пользователю выполняется **только администратором**.  
Это **не** право роли `api_user`.

| Этап | Кто выполняет | Endpoint | Метод | Пример |
|---|---|---|---|---|
| Создать service account и назначить роль `api_user` | Admin / Security Manager | `/api/v1/security/users/` | `POST` | `{\"username\":\"svc_bi\",\"password\":\"***\",\"roles\":[<api_user_role_id>],\"active\":true}` |
| Получить JWT | `api_user` (или сервис от его имени) | `/api/v1/security/login/` | `POST` | `{\"username\":\"svc_bi\",\"password\":\"***\",\"provider\":\"db\",\"refresh\":true}` |
| Выполнять BI API-операции | `api_user` | любой `/api/v1/*` в рамках выданных прав | `GET/POST/PUT/DELETE` | `Authorization: Bearer <access_token>` |

Примечание: в некоторых версиях Superset endpoint создания пользователя/ролей может отличаться по пути.  
Перед автоматизацией проверяйте OpenAPI (`/api/v1/_openapi` или Swagger UI) на конкретном инстансе.

## Ролевой профиль (рекомендуемый baseline)

| Профиль | Разрешено | Запрещено |
|---|---|---|
| API ReadOnly | Только `GET` + `/health` | Любые write-операции |
| API Editor | Read + create/update chart/dashboard/dataset + import/export + `/chart/data` | RBAC/user/role/security admin действия |
| Admin | Полный API-контур | Нет технических ограничений (только governance) |

## Критические правила миграции между инстансами

| Правило | Почему |
|---|---|
| Никогда не полагаться на "сырой" `dataset_id` между инстансами | `dataset_id` локальный для конкретного Superset metadata DB |
| Всегда маппить datasource по `table_name` | Исключает ложные привязки чарта к другой таблице |
| После import делать smoke-check `/api/v1/chart/data` | Быстро ловит `UNKNOWN_IDENTIFIER` и `Columns missing in dataset` |

## Наши методы проверки рендера

| Метод | Что проверяет | Как запускать | Критерий успеха |
|---|---|---|---|
| Server-side render smoke | Валидность SQL/метрик/фильтров чарта | `POST /api/v1/chart/data` с `query_context` чарта | HTTP `200`, без ошибок ClickHouse |
| Dashboard metadata integrity | Корректность связей chart<->dashboard<->filters | `GET /api/v1/dashboard/{id}` + проверка `json_metadata/position_json` | Нет ссылок на неверные dataset/chart IDs |
| UI визуальный рендер | Фактическое отображение чарта в интерфейсе | Открыть dashboard/chart в UI, затем hard refresh (`Ctrl+Shift+R`) | Чарт отображается, без Data error |
| SQL контроль в ClickHouse (опц.) | Корректность сырого запроса вне Superset | Выполнить SQL напрямую в CH (`SELECT ...`) | SQL выполняется и возвращает ожидаемые поля |

## Мини-чеклист перед передачей другим проектам
- Проверен `/health` целевого Superset.
- Проверен login + CSRF для API user.
- Подтверждены права API user на чтение/запись нужных BI объектов.
- Выполнен export/import bundle.
- Выполнен datasource remap по `table_name` (если перенос между инстансами).
- Выполнен smoke-check через `/api/v1/chart/data` для ключевых чартов.
- Выполнена UI-проверка рендера после hard refresh.
