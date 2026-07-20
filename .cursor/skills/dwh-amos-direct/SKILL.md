---
name: dwh-amos-direct
description: Read-only direct access to the external AMOS DWH (managed ClickHouse over TLS). Use whenever you need to query DWH tables directly — raw SCD tables under source.* (od_detail, od_header, part_special, aircraft, address) and enriched daily marts under reports.* (amos_heli_rotables_components_status) — including connecting via clickhouse_connect, building SCD "as-of" snapshots, resolving the latest available report_date, and exporting DWH data. Self-contained: assumes no prior project knowledge.
---

# DWH AMOS — прямой доступ (read-only)

Навык описывает, как **напрямую** обращаться к внешнему DWH и корректно читать из него данные.
Он самодостаточен: не требует знания остального проекта. Речь идёт **только** про DWH
(managed ClickHouse в Yandex Cloud, витрина данных AMOS — система ТОиР/MRO для вертолётов).

> Это НЕ локальный ClickHouse проекта. У проекта есть отдельный внутренний CH (native, порт 9000).
> DWH — другой инстанс: облачный, по TLS, со своими БД `source` и `reports`.

## 0. Когда применять

- Нужно прочитать сырой AMOS из DWH: заказы (ремонты/поставки), компоненты, ВС, адреса/вендоры.
- Нужен «срез на дату» (as-of snapshot) из SCD-таблиц `source.*`.
- Нужна готовая дневная витрина компонентов `reports.amos_heli_rotables_components_status`.
- Нужно выгрузить что-то из DWH в Excel/DataFrame.

## 1. Подключение

DWH работает по **HTTPS/TLS** (clickhouse-connect, secure=True), поэтому:
- используется библиотека **`clickhouse_connect`** (HTTP-протокол), а **не** `clickhouse_driver`;
- обязателен **CA-сертификат**: `config/certs/yandex_cloud_RootCA.pem`.

Все параметры берутся из переменных окружения (лежат в `.env` репозитория). Никогда не хардкодить хост/пароль.

Обязательные ENV:

```
DWH_CLICKHOUSE_HOST
DWH_CLICKHOUSE_PORT        # TLS-порт (как правило 8443)
DWH_CLICKHOUSE_DATABASE
DWH_CLICKHOUSE_USER
DWH_CLICKHOUSE_PASSWORD
```

Опциональные ENV:

```
DWH_CLICKHOUSE_SECURE=true     # по умолчанию true
DWH_CLICKHOUSE_VERIFY=true     # проверка TLS-сертификата
DWH_CLICKHOUSE_CA_CERT=config/certs/yandex_cloud_RootCA.pem
```

### Готовые хелперы (предпочтительно)

В репозитории уже есть проверенные функции подключения — используй их, не пиши своё:

- `code/utils/dwh_golden_replay_export.py` → `dwh_client()`
- `code/utils/export_engine_repair_dwh.py` → `_dwh_connect()` (таймауты 60/300s + авто-подстановка CA)

```python
import sys; sys.path.insert(0, "code/utils")
from dwh_golden_replay_export import dwh_client

client = dwh_client()
rows = client.query("SELECT 1").result_rows          # список кортежей
df = client.query_df("SELECT 1 AS x")                 # pandas.DataFrame
```

### Минимальный self-contained коннект (если хелперы недоступны)

```python
import os, clickhouse_connect
client = clickhouse_connect.get_client(
    host=os.environ["DWH_CLICKHOUSE_HOST"],
    port=int(os.environ["DWH_CLICKHOUSE_PORT"]),
    username=os.environ["DWH_CLICKHOUSE_USER"],
    password=os.environ["DWH_CLICKHOUSE_PASSWORD"],
    database=os.environ["DWH_CLICKHOUSE_DATABASE"],
    secure=True, verify=True,
    ca_cert="config/certs/yandex_cloud_RootCA.pem",
    connect_timeout=60, send_receive_timeout=300,
)
```

> Если ENV не загружены — подгрузи `.env` (например через `python-dotenv` или проектный `auto_load_env_file()`),
> но **не** печатай пароль в логи/вывод.

## 2. Безопасность (жёстко)

1. **Только `SELECT`.** Любые мутации (INSERT/ALTER/UPDATE/DELETE) — запрещены без явного разрешения.
2. **DROP TABLE / DROP DATABASE / TRUNCATE — тотальный запрет** (действует по всему проекту, DWH не исключение).
3. Не создавать/не изменять объекты в DWH.
4. Не выгружать секреты (пароль) в вывод/файлы.

## 3. Структура DWH: две БД, две модели

| БД | Модель | Ключ среза | Как читать |
|---|---|---|---|
| **`source.*`** | Сырой AMOS, **SCD** (историзация) | `valid_from` / `valid_to` | нужен **as-of dedup** (см. §5) |
| **`reports.*`** | Обогащённые дневные витрины | `report_date` (Date) | фильтр `WHERE report_date = ...`, дедуп не нужен |

Служебные поля в обеих БД: `valid_from`, `valid_to`, `processing_date_at`, `meta_source`, `meta_loading_at`.

## 4. Обнаружение схемы (schema-first, обязательно)

Не угадывай состав колонок — сперва проверь.

```sql
-- какие таблицы есть
SELECT database, name FROM system.tables
WHERE database IN ('source','reports') AND name LIKE 'amos_heli_%'
ORDER BY database, name;

-- схема конкретной таблицы
DESCRIBE TABLE source.amos_heli_od_detail;
```

Полные DDL всех таблиц с комментариями к колонкам собраны в репозитории:
**`docs/inventory_as_is.md`** (искать по имени таблицы) — используй как справочник, но перед SQL всё равно сверяйся с `DESCRIBE`.

**SSoT структуры БД AMOS (производитель):**
`data_input/analytics/Database-Description39331920032025_0.csv` — официальное описание структуры базы данных
от производителя AMOS (перечень таблиц, полей, типов и текстовых описаний). Формат: секции `Table <name>;;;;`,
затем заголовок `Keys;Name;Mime-Type;Type;Description` и строки полей. Это **источник истины по СТРУКТУРЕ и
семантике полей**, но:
- ⚠️ Это описание **generic-схемы производителя**. Оно **не гарантирует**, что конкретная конфигурация/настройка
  вашего инстанса AMOS и наполнение полей значениями (особенно кодов-справочников) полностью соответствуют
  описанию. Конкретные значения кодов проверяй по данным DWH и уточняй у владельцев AMOS.
- Имена таблиц в DWH имеют префикс (`source.amos_heli_<name>`), в SSoT — базовое имя (`<name>`, напр. `aircraft`).

## 5. SCD as-of snapshot (ключевой паттерн для `source.*`)

Строки в `source.*` историзированы: у одной бизнес-сущности несколько версий. Чтобы получить
состояние **на дату `report_date`**, нужно (а) отфильтровать по интервалу валидности,
(б) взять последнюю версию на каждый бизнес-ключ.

```sql
WITH snap AS (
    SELECT * FROM (
        SELECT *,
               row_number() OVER (PARTITION BY {business_key}
                                  ORDER BY valid_from DESC) AS rn
        FROM source.{table}
        WHERE valid_from <= toDateTime('{report_date} 23:59:59')
          AND (valid_to IS NULL OR valid_to > toDateTime('{report_date} 00:00:00'))
    )
    WHERE rn = 1
)
SELECT * FROM snap;
```

Бизнес-ключи для дедупа:
- `source.amos_heli_od_detail` → `detailno_i`
- `source.amos_heli_od_header` → `orderno_i`
- `source.amos_heli_part_special` → `partno`
- `source.amos_heli_aircraft` → `ac_registr`

> «admin»-режим (текущее состояние без as-of): берётся последняя версия по бизнес-ключу
> **без** фильтра по `valid_from/valid_to`. Использовать только когда явно нужен «прямо сейчас», а не срез на дату.

## 6. Ключевые таблицы и поля

### `reports.amos_heli_rotables_components_status` (витрина компонентов, ключ `report_date`)
Готовый дневной срез оборотных агрегатов/компонентов. Основные поля:
`partno, partseqno_i, serialno, psn, ac_typ, ac_type_i, location, LL, OH, OH_threshold, sne, ppr,
mfg_date, oh_at_date, shop_visit_counter, owner, address_i, condition, removal_date, target_date, aircraftno_i`.

- `LL` — предел ресурса (Life Limit); `OH` — наработка с капремонта; `OH_threshold` — межремонтный порог.
- `sne`, `ppr` — счётчики наработки/циклов; `condition` — состояние по AMOS (напр. `ИСПРАВНЫЙ`).
- `location` — местоположение/борт установки; `removal_date` — дата снятия; `target_date` — плановая дата.
- ⚠️ Часть полей **derived/enriched**, а не прямая копия сырого AMOS: `LL`, `OH`, `OH_threshold`, частично
  `ppr`, `sne`, `location`, `condition`, `removal_date`, `target_date`. Для «сырья» иди в `source.*`.

### `source.amos_heli_od_detail` / `source.amos_heli_od_header` (заказы)
Ремонты/поставки. Связь `od.orderno_i = oh.orderno_i`. Поля detail:
`orderno_i, detailno_i, partno, serialno, vendor, order_type, state, ext_state,
target_date, orig_target_date, confirmed_date, del_date, condition, ac_registr`.
- `order_type='R'` — ремонт (Repair); `state='O'` — открыт (Open); header `ext_state IN ('O','BO','PB','PR')`.
- ⚠️ `od_header` исторически **отсутствовал** в DWH (сейчас присутствует, но это может меняться) — всегда проверяй наличие через `system.tables` и умей деградировать на поля `od_detail` (там тоже есть `order_type`, `state`).

### `source.amos_heli_part_special` (спец-признаки partno)
`partno, special` — напр. `special='ДВИГ'` помечает двигатели. Дедуп по `partno`.

### `source.amos_heli_aircraft` (реестр ВС)
`ac_registr, ac_typ, owner, manual_owner, status, homebase, ...`.
- 🚫 **ЗАПРЕТ (решение Алексея, 2026-07-17): поле `status` НЕ ИСПОЛЬЗОВАТЬ в доменной логике вообще.**
  По SSoT производителя это generic **«The status of the record. This column has a different meaning for
  every table.»** — служебный статус ЗАПИСИ, фоновый шум, не имеющий отношения к эксплуатационному
  состоянию ВС или агрегатов. Запрещено фильтровать/классифицировать по нему, ссылаться на его коды
  и строить на нём выводы. Доменное состояние определяется ТОЛЬКО так:
  планеры — `status_overhaul` (капремонт) + членство в `program_ac` (ростер по `non_managed`,
  владельцу/эксплуатанту, типу); агрегаты — поле `condition`.
- Отдельное поле `init_status` (SSoT: `0 = normal, 1 = blocked, 2 = in progress`) относится к MP-активациям и
  к эксплуатационному статусу/ростеру отношения не имеет.

### `source.amos_heli_address` (вендоры/адреса)
`address_i, vendor, name` — джойнить по `address_i` для расшифровки вендора.

### Прочее
`source.amos_heli_wp_header` (work packages), `source.amos_heli_ac_typ` (типы ВС),
`source.amos_heli_condition` (справочник состояний), `source.amos_heli_adr_special`.
Полный список — через `system.tables` и `docs/inventory_as_is.md`.

## 7. Подводные камни (проверено на практике)

1. **Sentinel-дата AMOS.** Даты в AMOS = число дней от `1971-12-31`. Значение `0` → дата `1971-12-31` =
   **«не задана»**, а не реальная дата. При фильтрах/аналитике трактуй `1971-12-31` (и `NULL`) как «пусто».
2. **Лаг и разрывы витрин.** `reports.*` появляется только с даты первой заливки DAG; на некоторые
   календарные даты строк **0**. Всегда сперва определяй доступную дату:
   ```sql
   SELECT max(report_date) FROM reports.amos_heli_rotables_components_status;
   ```
   Для `source.*` — `max(toDate(valid_from))` соответствующей таблицы.
3. **Два разных контура по датам.** Витрина статусов и заказы (`od_detail`) обновляются независимо —
   у них могут быть **разные** максимальные даты. Не считай, что они синхронны.
4. **SCD-дубли.** Без dedup (§5) один объект вернётся несколькими версиями → двойной счёт. Всегда `row_number()`+`rn=1`.
5. **Snapshot vs день+1.** «Последняя версия без as-of» может отражать изменение, случившееся **после**
   интересующей даты. Для состояния на дату — строго as-of по `valid_from/valid_to`.
6. **enriched ≠ raw.** Не выводи бизнес-заключения о «сырых» значениях по полям витрины `reports.*`,
   которые помечены как derived (см. §6). Для сверки бери `source.*`.
7. **TLS.** При ошибке сертификата проверь `DWH_CLICKHOUSE_CA_CERT` и наличие `config/certs/yandex_cloud_RootCA.pem`;
   `verify=False` — только как временная диагностика и с явной пометкой риска.

## 8. Рабочий цикл

1. Подключиться готовым хелпером (`dwh_client()` / `_dwh_connect()`).
2. Определить доступную дату среза (`max(report_date)` / `max(valid_from)`), зафиксировать её.
3. `DESCRIBE TABLE` нужных таблиц (schema-first), сверка с `docs/inventory_as_is.md`.
4. Для `source.*` — as-of dedup (§5); для `reports.*` — фильтр по `report_date`.
5. Применить трактовку sentinel-дат и enriched-полей.
6. Только `SELECT`. Результат — таблицей/DataFrame; при выгрузке — в `output/`.

## 9. Стандарт отчёта

В ответе всегда фиксируй:
1. Инстанс = **DWH** (не локальный CH), режим доступа (as-of дата / admin).
2. Таблицы и их фактические даты среза (`report_date` / `max(valid_from)`).
3. Полный набор фильтров и бизнес-ключ dedup.
4. Трактовку sentinel-дат и какие поля брались из `reports.*` (enriched) vs `source.*` (raw).
5. Табличный результат и явные допущения (`Risks if false: ...`).
