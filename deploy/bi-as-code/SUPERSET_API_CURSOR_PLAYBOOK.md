# Superset API Playbook для нового BI-проекта (Cursor AI)

## Назначение

- Это универсальная инструкция для запуска BI-разработки в новом проекте через Superset API.
- Документ не зависит от типа автоматизации и применим в любом проектном контуре.
- В документе оставлены рабочие адреса нашей песочницы как дефолт.

## Дефолтные адреса песочницы (можно переиспользовать в новом проекте)


| Контур                                   | Адрес                      |
| ---------------------------------------- | -------------------------- |
| Superset API base URL                    | `http://10.96.96.47:8088`  |
| ClickHouse HTTP (для SQL-проверок, опц.) | `http://10.95.19.132:8123` |


## Контракт доступа

- Режим работы: **API-only**.
- Docker/Superset runtime из BI-репозитория не управляется.
- Права разделяются на:
  - `api_user` — рабочие BI API-операции;
  - `admin/security manager` — provisioning пользователей/ролей/глобальных политик.

## Переменные окружения


| Переменная                 | Значение по умолчанию     | Назначение              |
| -------------------------- | ------------------------- | ----------------------- |
| `SUPERSET_API_BASE_URL`    | `http://10.96.96.47:8088` | Базовый URL Superset    |
| `SUPERSET_API_PROVIDER`    | `db`                      | Провайдер авторизации   |
| `SUPERSET_API_USERNAME`    | `bi_api_user`             | Логин API-пользователя  |
| `SUPERSET_API_PASSWORD`    | `*`**                     | Пароль API-пользователя |
| `SUPERSET_API_TIMEOUT_SEC` | `120`                     | Таймаут запросов        |


## Порядок подключения (обязательно)


| Шаг | Endpoint                       | Метод                 | Ожидаемый результат         |
| --- | ------------------------------ | --------------------- | --------------------------- |
| 1   | `/health`                      | `GET`                 | `200`                       |
| 2   | `/api/v1/security/login`       | `POST`                | Получен `access_token`      |
| 3   | `/api/v1/security/csrf_token/` | `GET`                 | Получен CSRF token          |
| 4   | Целевые read/write endpoints   | `GET/POST/PUT/DELETE` | Выполнены BI-операции       |
| 5   | `/api/v1/chart/data`           | `POST`                | Успешный render smoke-check |


## Матрица API-операций (`api_user` vs `admin`)


| Зона                    | Endpoint                                                 | Методы                | `api_user` | `admin` |
| ----------------------- | -------------------------------------------------------- | --------------------- | ---------- | ------- |
| Auth                    | `/api/v1/security/login`                                 | `POST`                | ✅          | ✅       |
| CSRF                    | `/api/v1/security/csrf_token/`                           | `GET`                 | ✅          | ✅       |
| Dashboard               | `/api/v1/dashboard/*`                                    | `GET/POST/PUT/DELETE` | ✅*         | ✅       |
| Chart                   | `/api/v1/chart/`*                                        | `GET/POST/PUT/DELETE` | ✅*         | ✅       |
| Dataset                 | `/api/v1/dataset/`*                                      | `GET/POST/PUT/DELETE` | ✅*         | ✅       |
| Dashboard export/import | `/api/v1/dashboard/export`, `/api/v1/dashboard/import`   | `GET/POST`            | ✅*         | ✅       |
| Chart render            | `/api/v1/chart/data`                                     | `POST`                | ✅*         | ✅       |
| Users/Roles/Permissions | `/api/v1/security/`*, `/api/v1/user/*`, `/api/v1/role/*` | `GET/POST/PUT/DELETE` | ❌ (обычно) | ✅       |


 при наличии соответствующих RBAC разрешений.

## Правила миграции между инстансами (обязательные)

1. Не доверять "сырым" `dataset_id` при переносе между инстансами.
2. Маппить datasource chart/filter по `table_name`, затем подставлять локальный `dataset_id`.
3. После import всегда запускать smoke-check рендера.

## Методы проверки рендера


| Метод                    | Что проверяет                              | Команда/endpoint             | Критерий PASS                                          |
| ------------------------ | ------------------------------------------ | ---------------------------- | ------------------------------------------------------ |
| Server-side chart render | SQL/метрики/фильтры на стороне Superset+CH | `POST /api/v1/chart/data`    | HTTP `200`, без `UNKNOWN_IDENTIFIER`/`Columns missing` |
| Metadata integrity       | Связи chart <-> dashboard <-> filters      | `GET /api/v1/dashboard/{id}` | Нет битых ссылок и неверных datasource                 |
| UI render check          | Фактическое отображение графика            | UI + `Ctrl+Shift+R`          | Нет `Data error`                                       |
| SQL direct check (опц.)  | Сырой SQL вне Superset                     | Запрос напрямую в CH         | SQL выполняется и возвращает ожидаемые поля            |


## Быстрый API-чек (универсальный)

```bash
curl -s -o /dev/null -w "%{http_code}\n" "${SUPERSET_API_BASE_URL}/health"
```

## Рекомендуемый набор Cursor Skills

Оговорка:
- Этот набор ориентирован на мультиагентную разработку.
- Решение о необходимости подключения и фактическом использовании skills принимает пользователь проекта.


| Skill                            | Назначение                                                                                                                  |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `bi-superset-api-implementer`    | Внесение изменений в dashboard/chart/dataset через Superset API, rebinding datasource по `table_name`, export/import bundle |
| `bi-superset-semantic-validator` | Проверка семантики BI: метрики, фильтры, `time_grain`, `orderby`, совместимость SQL с реальной схемой датасета              |
| `bi-superset-render-smoke`       | Техническая проверка рендера через `/api/v1/chart/data` и диагностика ошибок (`UNKNOWN_IDENTIFIER`, `Columns missing`)      |
| `bi-superset-doc-sync`           | Синхронизация технической документации после BI-изменений (changelog, runbook, migration notes)                             |


## Как установить skills в новом проекте

1. Создать каталог skill в проекте по пути:
  - `.cursor/skills/<любой_идентификатор_skill>/`
2. Положить файл инструкции в:
  - `.cursor/skills/<любой_идентификатор_skill>/SKILL.md`
3. При необходимости использовать персональные skills (вне репозитория):
  - `~/.cursor/skills/<любой_идентификатор_skill>/SKILL.md`
4. Проверить, что Cursor подхватывает skill на релевантных BI-задачах.

## Мини-чеклист запуска нового BI-проекта

- Настроены `SUPERSET_API_*`.
- Проверены `health/login/csrf`.
- Подтверждён RBAC для `api_user`.
- Подключен как минимум один релевантный skill (или набор skills по необходимости проекта).
- Проверены render smoke-check и datasource remap правила.

---

## Приложение A. Полный реестр разрешений роли `api_user` (dashboard-as-a-code)

Коротко про API-контур:

- Superset API (v1) — REST-слой для работы с dashboard/chart/dataset/database, import/export, explore и SQL Lab.
- Практически все write-операции требуют JWT + CSRF.
- Права на endpoint зависят от набора permission, назначенных роли.
- Ниже приведён полный реестр для `api_user` (по каталогу из рабочей песочницы).

### Глобальный доступ


| #   | Permission                                       | ID  | Описание                    |
| --- | ------------------------------------------------ | --- | --------------------------- |
| 1   | `all database access on all_database_access`     | 212 | Доступ ко всем базам данных |
| 2   | `all datasource access on all_datasource_access` | 211 | Доступ ко всем датасетам    |


### Dashboard


| #   | Permission                                    | ID  |
| --- | --------------------------------------------- | --- |
| 3   | `can read on Dashboard`                       | 15  |
| 4   | `can write on Dashboard`                      | 16  |
| 5   | `can export on Dashboard`                     | 104 |
| 6   | `can get embedded on Dashboard`               | 106 |
| 7   | `can set embedded on Dashboard`               | 108 |
| 8   | `can delete embedded on Dashboard`            | 107 |
| 9   | `can cache dashboard screenshot on Dashboard` | 105 |
| 10  | `can drill on Dashboard`                      | 220 |
| 11  | `can view chart as table on Dashboard`        | 219 |
| 12  | `can view query on Dashboard`                 | 218 |
| 13  | `can tag on Dashboard`                        | 222 |
| 14  | `can read on DashboardFilterStateRestApi`     | 101 |
| 15  | `can write on DashboardFilterStateRestApi`    | 102 |
| 16  | `can read on DashboardPermalinkRestApi`       | 103 |
| 17  | `can write on DashboardPermalinkRestApi`      | 104 |
| 18  | `can read on EmbeddedDashboard`               | 116 |
| 19  | `menu access on Dashboards`                   | 197 |


### Chart


| #   | Permission                   | ID  |
| --- | ---------------------------- | --- |
| 20  | `can read on Chart`          | 7   |
| 21  | `can write on Chart`         | 8   |
| 22  | `can export on Chart`        | 93  |
| 23  | `can warm up cache on Chart` | 94  |
| 24  | `can tag on Chart`           | 221 |
| 25  | `menu access on Charts`      | 198 |


### Dataset


| #   | Permission                             | ID  |
| --- | -------------------------------------- | --- |
| 26  | `can read on Dataset`                  | 11  |
| 27  | `can write on Dataset`                 | 12  |
| 28  | `can export on Dataset`                | 110 |
| 29  | `can duplicate on Dataset`             | 113 |
| 30  | `can get or create dataset on Dataset` | 114 |
| 31  | `can get drill info on Dataset`        | 111 |
| 32  | `can warm up cache on Dataset`         | 112 |
| 33  | `menu access on Datasets`              | 199 |


### Database


| #   | Permission                 | ID  |
| --- | -------------------------- | --- |
| 34  | `can read on Database`     | 17  |
| 35  | `can write on Database`    | 18  |
| 36  | `can export on Database`   | 109 |
| 37  | `can upload on Database`   | 25  |
| 38  | `menu access on Databases` | 196 |


### Datasource


| #   | Permission                                    | ID  |
| --- | --------------------------------------------- | --- |
| 39  | `can get on Datasource`                       | 149 |
| 40  | `can get column values on Datasource`         | 115 |
| 41  | `can external metadata on Datasource`         | 151 |
| 42  | `can external metadata by name on Datasource` | 152 |
| 43  | `can samples on Datasource`                   | 153 |
| 44  | `can query on Api`                            | 148 |
| 45  | `can query form data on Api`                  | 147 |
| 46  | `can time range on Api`                       | 146 |


### Explore


| #   | Permission                                  | ID  |
| --- | ------------------------------------------- | --- |
| 47  | `can explore on Superset`                   | 155 |
| 48  | `can explore json on Superset`              | 162 |
| 49  | `can read on Explore`                       | 117 |
| 50  | `can read on ExploreFormDataRestApi`        | 119 |
| 51  | `can write on ExploreFormDataRestApi`       | 118 |
| 52  | `can read on ExplorePermalinkRestApi`       | 121 |
| 53  | `can write on ExplorePermalinkRestApi`      | 120 |
| 54  | `can fetch datasource metadata on Superset` | 161 |


### SQL Lab


| #   | Permission                            | ID  |
| --- | ------------------------------------- | --- |
| 55  | `can read on SQLLab`                  | 133 |
| 56  | `can execute sql query on SQLLab`     | 135 |
| 57  | `can get results on SQLLab`           | 132 |
| 58  | `can estimate query cost on SQLLab`   | 134 |
| 59  | `can export csv on SQLLab`            | 131 |
| 60  | `can format sql on SQLLab`            | 130 |
| 61  | `can sqllab on Superset`              | 217 |
| 62  | `can sqllab history on Superset`      | 158 |
| 63  | `can csv on Superset`                 | 214 |
| 64  | `can read on SqlLabPermalinkRestApi`  | 137 |
| 65  | `can write on SqlLabPermalinkRestApi` | 136 |
| 66  | `can read on Query`                   | 19  |
| 67  | `menu access on SQL Lab`              | 207 |
| 68  | `menu access on SQL Editor`           | 208 |
| 69  | `menu access on Query Search`         | 210 |
| 70  | `menu access on Saved Queries`        | 209 |


### TabStateView (SQL вкладки)


| #   | Permission                          | ID  |
| --- | ----------------------------------- | --- |
| 71  | `can activate on TabStateView`      | 174 |
| 72  | `can get on TabStateView`           | 168 |
| 73  | `can post on TabStateView`          | 170 |
| 74  | `can put on TabStateView`           | 169 |
| 75  | `can delete on TabStateView`        | 173 |
| 76  | `can delete query on TabStateView`  | 171 |
| 77  | `can migrate query on TabStateView` | 172 |
| 78  | `can delete on TableSchemaView`     | 166 |
| 79  | `can expanded on TableSchemaView`   | 167 |
| 80  | `can post on TableSchemaView`       | 165 |


### SavedQuery


| #   | Permission                 | ID  |
| --- | -------------------------- | --- |
| 81  | `can read on SavedQuery`   | 1   |
| 82  | `can write on SavedQuery`  | 2   |
| 83  | `can export on SavedQuery` | 126 |
| 84  | `can list on SavedQuery`   | 125 |


### Import / Export


| #   | Permission                          | ID  |
| --- | ----------------------------------- | --- |
| 85  | `can import on ImportExportRestApi` | 123 |
| 86  | `can export on ImportExportRestApi` | 122 |


### Tags


| #   | Permission               | ID  |
| --- | ------------------------ | --- |
| 87  | `can read on Tag`        | 129 |
| 88  | `can write on Tag`       | 128 |
| 89  | `can bulk create on Tag` | 127 |
| 90  | `can tags on TagView`    | 176 |
| 91  | `can list on Tags`       | 175 |
| 92  | `menu access on Tags`    | 204 |


### Reports & Alerts


| #   | Permission                       | ID  |
| --- | -------------------------------- | --- |
| 93  | `can read on ReportSchedule`     | 5   |
| 94  | `can write on ReportSchedule`    | 6   |
| 95  | `menu access on Alerts & Report` | 205 |


### Annotation


| #   | Permission                         | ID  |
| --- | ---------------------------------- | --- |
| 96  | `can read on Annotation`           | 9   |
| 97  | `can write on Annotation`          | 10  |
| 98  | `menu access on Annotation Layers` | 206 |


### Security & Auth


| #   | Permission                                  | ID  |
| --- | ------------------------------------------- | --- |
| 99  | `can read on SecurityRestApi`               | 185 |
| 100 | `can read on security`                      | 139 |
| 101 | `can read on RowLevelSecurity`              | 186 |
| 102 | `can read on CurrentUserRestApi`            | 99  |
| 103 | `can write on CurrentUserRestApi`           | 100 |
| 104 | `can userinfo on UserDBModelView`           | 34  |
| 105 | `resetmypassword on UserDBModelView`        | 39  |
| 106 | `can this form get on ResetMyPasswordView`  | 30  |
| 107 | `can this form post on ResetMyPasswordView` | 31  |
| 108 | `can add on UserRegistrationsRestAPI`       | 179 |
| 109 | `can delete on UserRegistrationsRestAPI`    | 181 |
| 110 | `can edit on UserRegistrationsRestAPI`      | 182 |
| 111 | `can list on UserRegistrationsRestAPI`      | 183 |
| 112 | `can show on UserRegistrationsRestAPI`      | 180 |
| 113 | `can read on user`                          | —   |


### Theme / CSS


| #   | Permission                     | ID  |
| --- | ------------------------------ | --- |
| 114 | `can read on Theme`            | 95  |
| 115 | `can write on Theme`           | 96  |
| 116 | `can export on Theme`          | 97  |
| 117 | `can read on CssTemplate`      | 3   |
| 118 | `can write on CssTemplate`     | 4   |
| 119 | `menu access on Themes`        | 203 |
| 120 | `menu access on CSS Templates` | 202 |


### Permalinks & Share


| #   | Permission                            | ID  |
| --- | ------------------------------------- | --- |
| 121 | `can dashboard permalink on Superset` | 156 |
| 122 | `can dashboard on Superset`           | 160 |
| 123 | `can share dashboard on Superset`     | 215 |
| 124 | `can share chart on Superset`         | 216 |
| 125 | `can slice on Superset`               | 163 |


### Superset Core


| #   | Permission                       | ID  |
| --- | -------------------------------- | --- |
| 126 | `can warm up cache on Superset`  | 157 |
| 127 | `can language pack on Superset`  | 164 |
| 128 | `can invalidate on CacheRestApi` | 92  |
| 129 | `can read on AvailableDomains`   | 91  |
| 130 | `can read on AdvancedDataType`   | 90  |
| 131 | `can get on OpenApi`             | 86  |
| 132 | `can show on SwaggerView`        | 87  |
| 133 | `can get on MenuApi`             | 88  |
| 134 | `can list on AsyncEventsRestApi` | 89  |
| 135 | `can recent activity on Log`     | 138 |


### Plugins & Dynamic


| #   | Permission                  | ID  |
| --- | --------------------------- | --- |
| 136 | `can show on DynamicPlugin` | 142 |
| 137 | `menu access on Plugins`    | 201 |


### Menu навигация


| #   | Permission                  | ID  |
| --- | --------------------------- | --- |
| 138 | `menu access on Home`       | 194 |
| 139 | `menu access on Data`       | 195 |
| 140 | `menu access on Manage`     | 200 |
| 141 | `menu access on Action Log` | 192 |


## Как использовать этот блок в новом проекте

```bash
# 1) Admin/Security Manager создает service account и назначает роль api_user
POST /api/v1/security/users/
{
  "username": "your_service_account",
  "password": "...",
  "roles": [<api_user_role_id>],
  "active": true
}

# 2) api_user получает access token
POST /api/v1/security/login/
{"username": "...", "password": "...", "provider": "db", "refresh": true}

# 3) api_user использует токен для рабочих BI-операций
Authorization: Bearer <access_token>
```

Ключевые возможности `api_user` для dashboard-as-a-code:

- CRUD для Dashboard/Chart/Dataset/Database (при наличии RBAC);
- Import/Export дашбордов и датасетов;
- Embedded-операции с дашбордами;
- SQL execution/получение результатов;
- Tags и Reports;
- Global datasource/database access (если включён соответствующий permission).

