# RTC Pipeline Architecture

## Статус baseline (sim_masterv2)
- Baseline остановлен: выявлены критические баги, разработка прекращена из‑за сближения в adaptive.
- Baseline не является источником истины и не используется для обязательного сравнения.
- Актуальные источники: LIMITER V8 (`sim_masterv2_v8`), `config/transitions/transitions_rules.json`.

## Примечания по LIMITER V8 (17-01-2026)
- `deterministic_dates_mp` ограничен `MAX_DETERMINISTIC_DATES=500`; при превышении лимита даты обрезаются.
- `unserviceable` не участвует в `min_dynamic` и не влияет на adaptive‑шаги.
- `min_dynamic` сбрасывается в `rtc_compute_global_min_v8` (GPU‑only), отдельного reset‑слоя нет.
- Для логгера шага `min_dynamic` кодируется с источником (limiter/repair_days) и сохраняется в `adaptive_result_mp[1]`.
- Шаги по `deterministic_dates` фиксируются в логгере как `deterministic_date:<day>` (например repair_time/spawn).
- RepairLine — общий пул линий, используется только в квотировании (P2/P3), не в scheduler.
- QM берёт free_days/acn напрямую из MacroProperty (`repair_line_free_days_mp`, `repair_line_acn_mp`) после pre‑quota sync.
- QM пишет debug по ops/target/quota_left (Mi‑8/Mi‑17) в `sim_quota_mgr_v8` для диагностики спавна.
- QuotaManager в V8 использует MessageBucket (`QuotaBucket`) и рассылает один broadcast‑пакет с квотами.
- P1/P2/P3 решаются агентами по rank (youngest first) на основе MacroProperty‑буферов готовности.
- Спавн использует `qm_ops_mp` и commit‑флаги P1/P2/P3 (по факту переходов).
- P2/P3: условия `day >= repair_time`, `repair_days == 0`, линия с `free_days >= repair_time` и `aircraft_number == 0` (+ защита от повтора acn в соседние дни).
- `repair_days` декрементируется только в `unserviceable`; для `inactive` всегда 0 и не участвует в шаге.
- V8 readiness для `unserviceable`: `repair_days == 0` и `repair_line_id == 0xFFFFFFFF` (day‑барьер не нужен, т.к. repair_days уже отсчитывает ожидание).
- P2 ранжирует **только готовые** `unserviceable` по `unsvc_ready_count` (не по общему `unsvc_count`).
- Динамический спавн Mi‑17 запускается по дефициту `target − curr_ops − used`, где used = commit P1/P2/P3; storage не участвует, post‑quota counts используются для актуального ops перед спавном.
- Приоритет P2/P3 по idx (молодые раньше) внутри своего типа с раздельными квотами; RepairLine выбирается по минимальному `free_days >= repair_time`.
- Debug спавна: `SpawnDynamicMgr.debug_curr_ops/target/need` + `debug_current_day` в MP2.
- Валидация MESSAGING поддерживает фильтр `version_id` для изоляции прогонов без DROP.
- V8 spawn: тикеты читают параметры по текущему `day` (один день/один шаг).
- V8 spawn Mi-8: константы `mi8_ll/oh/br` берутся из `md_components` через env.
- Методология дебага логики: см. `.cursor/rules/00_global_always.mdc`.
- Визуализация переходов и квот: `tools/transitions_viewer/index.html`.
- Канонические JSON: `config/transitions/transitions_rules.json`, `config/transitions/quota_rules.json`.
- Временное логирование: `debug_step/debug_prev_day/debug_adaptive_days`, `debug_rl_*` и `debug_*_mi17` для диагностики RepairLine/квотирования; состояние линий пишется в `sim_repair_lines_v8` (включая `last_acn/last_day`), слоты и P2‑метрики — в `sim_quota_mgr_v8` (первые 6 слотов Mi‑17). P2/P3 commit при занятом слоте выбирает следующий доступный в пределах слотов.
- V8 квоты используют локальные копии (rtc_quota_v8_base) и берут target по `current_day`.

## Контекстные капсулы (handoff)
- После приёмки оркестратора обновляется `docs/limiter_v8_capsule.md`.
- Формат капсулы фиксирован (8 обязательных секций с лимитами).

## Анализ всех RTC функций системы

### Полная таблица RTC функций

| № | RTC Функция | Флаг активации | Назначение | Что делает | Входные данные | Выходные данные | Когда выполняется |
|---|-------------|----------------|------------|------------|----------------|----------------|-------------------|
| 1 | `rtc_probe_mp5` | `HL_ENABLE_MP5_PROBE` | Чтение ежедневных часов работы | Читает из MP5 часы работы на сегодня/завтра и записывает в агентные переменные | MP5 массив `mp5_daily_hours`, `idx`, `day` | `daily_today_u32`, `daily_next_u32` | Каждый шаг, самым первым |
| 2 | `rtc_quota_begin_day` | `HL_ALWAYS_ON` | Сброс суточных флагов | Обнуляет операционные флаги в начале каждых суток | Нет | `ops_ticket=0`, `intent_flag=0` | Каждый шаг, в начале |
| 3 | `rtc_status_6` | `HL_ENABLE_STATUS_6` | Обработка статуса хранения | Ведет счетчик дней в статусе 6, проверяет триггер partout | `status_id`, `s6_started`, `s6_days`, `partout_time` | `s6_days++`, `partout_trigger` | Каждый шаг, для агентов в статусе 6 |
| 4 | `rtc_status_4` | `HL_ENABLE_STATUS_4` | Обработка статуса ремонта | Ведет счетчик дней ремонта, проверяет триггеры завершения | `status_id`, `repair_days`, `repair_time`, `assembly_time`, `partout_time` | `repair_days++`, `active_trigger`, `assembly_trigger`, `partout_trigger` | Каждый шаг, для агентов в статусе 4 |
| 5 | `rtc_status_2` | `HL_ENABLE_STATUS_2` | Обработка статуса эксплуатации | Начисляет наработку, проверяет LL/OH лимиты, Beyond Repair | `status_id`, `daily_today_u32`, `sne`, `ppr`, `ll`, `oh`, `br` | `sne+=dt`, `ppr+=dt`, переходы статусов | Каждый шаг, для агентов в статусе 2 |
| 6 | `rtc_quota_intent_s2` | `HL_ENABLE_QUOTA_S2` | Заявка квоты для статуса 2 | Агенты в статусе 2 подают заявку на операционную квоту | `status_id==2`, `group_by`, `idx` | MacroProperty `mi8_intent[idx]` или `mi17_intent[idx]` | Каждый шаг, после обработки статусов |
| 7 | `rtc_quota_approve_s2` | `HL_ENABLE_QUOTA_S2` | Одобрение квоты для статуса 2 | Менеджер (idx==0) распределяет квоты между заявителями | `mi8_intent`, `mi17_intent`, `mp4_ops_counter` | `mi8_approve`, `mi17_approve` | Каждый шаг, после intent |
| 8 | `rtc_quota_apply_s2` | `HL_ENABLE_QUOTA_S2` | Получение квоты для статуса 2 | Агенты получают операционный билет при одобрении | `mi8_approve[idx]`, `mi17_approve[idx]`, `group_by` | `ops_ticket=1` при одобрении | Каждый шаг, после approve |
| 9 | `rtc_quota_clear_s2` | `HL_ENABLE_QUOTA_S2` | Очистка intent для статуса 2 | Менеджер обнуляет intent массивы после распределения | Нет | `mi8_intent[]=0`, `mi17_intent[]=0` | Каждый шаг, после apply |
| 10 | `rtc_quota_intent_s3` | `HL_ENABLE_QUOTA_S3` | Заявка квоты для статуса 3 | Агенты в статусе 3 подают заявку на квоту | `status_id==3`, `group_by`, `idx` | MacroProperty intent массивы | То же что s2, но для статуса 3 |
| 11 | `rtc_quota_approve_s3` | `HL_ENABLE_QUOTA_S3` | Одобрение квоты для статуса 3 | Менеджер распределяет квоты для статуса 3 | Intent массивы, квоты MP4 | Approve массивы | То же что s2, но для статуса 3 |
| 12 | `rtc_quota_apply_s3` | `HL_ENABLE_QUOTA_S3` | Получение квоты для статуса 3 | Агенты получают операционный билет | Approve массивы | `ops_ticket` | То же что s2, но для статуса 3 |
| 13 | `rtc_quota_clear_s3` | `HL_ENABLE_QUOTA_S3` | Очистка intent для статуса 3 | Очистка intent после распределения | Нет | Intent массивы = 0 | То же что s2, но для статуса 3 |
| 14 | `rtc_status_3_post` | `HL_ENABLE_STATUS_3_POST` | Пост-обработка статуса 3 | Переходы статусов после получения/неполучения квоты | `status_id`, `ops_ticket` | Переходы статусов 3→1 или 3→2 | После всех quota циклов |
| 15 | `rtc_quota_intent_s5` | `HL_ENABLE_QUOTA_S5` | Заявка квоты для статуса 5 | Агенты в статусе 5 подают заявку | `status_id==5` | Intent массивы | Аналогично s2/s3 |
| 16 | `rtc_quota_approve_s5` | `HL_ENABLE_QUOTA_S5` | Одобрение квоты для статуса 5 | Менеджер для статуса 5 | Intent, MP4 квоты | Approve массивы | Аналогично s2/s3 |
| 17 | `rtc_quota_apply_s5` | `HL_ENABLE_QUOTA_S5` | Получение квоты для статуса 5 | Получение билета для статуса 5 | Approve массивы | `ops_ticket` | Аналогично s2/s3 |
| 18 | `rtc_quota_clear_s5` | `HL_ENABLE_QUOTA_S5` | Очистка intent для статуса 5 | Очистка после распределения | Нет | Intent = 0 | Аналогично s2/s3 |
| 19 | `rtc_quota_intent_s1` | `HL_ENABLE_QUOTA_S1` | Заявка квоты для статуса 1 | Агенты в статусе 1 подают заявку | `status_id==1` | Intent массивы | Аналогично другим статусам |
| 20 | `rtc_quota_approve_s1` | `HL_ENABLE_QUOTA_S1` | Одобрение квоты для статуса 1 | Менеджер для статуса 1 | Intent, MP4 | Approve массивы | Аналогично другим статусам |
| 21 | `rtc_quota_apply_s1` | `HL_ENABLE_QUOTA_S1` | Получение квоты для статуса 1 | Получение билета для статуса 1 | Approve массивы | `ops_ticket` | Аналогично другим статусам |
| 22 | `rtc_quota_clear_s1` | `HL_ENABLE_QUOTA_S1` | Очистка intent для статуса 1 | Очистка после распределения | Нет | Intent = 0 | Аналогично другим статусам |
| 23 | `rtc_status_1_post` | `HL_ENABLE_STATUS_1_POST` | Пост-обработка статуса 1 | Переходы из статуса 1 | `status_id==1`, `ops_ticket` | Переходы статусов | После quota циклов |
| 24 | `rtc_status_5_post` | `HL_ENABLE_STATUS_5_POST` | Пост-обработка статуса 5 | Переходы из статуса 5 | `status_id==5`, `ops_ticket` | Переходы статусов | После quota циклов |
| 25 | `rtc_status_2_post` | `HL_ENABLE_STATUS_2_POST` | Пост-обработка статуса 2 | Переход 2→3 при отсутствии билета | `status_id==2`, `ops_ticket==0` | `status_id=3` | После quota циклов |
| 26 | `rtc_log_day` | `HL_ENABLE_MP2_LOG` | Логирование в MP2 | Записывает состояние агента в MP2 лог за день | Все агентные переменные, `day`, `idx` | MP2 массив (SoA) | Каждый шаг, в конце |
| 27 | `rtc_mp2_postprocess` | `export_phase` | Постпроцессинг MP2 | Обрабатывает накопленные логи MP2 | MP2 массивы | Агрегированные данные | Только при `export_phase=2` |
| 28 | `rtc_mp2_copyout` | `HL_ENABLE_MP2_COPY` | Экспорт MP2 в агенты | Копирует данные из MP2 в агентные переменные | MP2 массивы, целевой день | Агентные переменные | Только при `export_phase=1` |
| 29 | `rtc_spawn_mgr` | `HL_ENABLE_SPAWN` | Менеджер спавна | Подготавливает параметры для создания новых агентов | `mp4_new_counter_mi17_seed[day]`, счетчики | MacroProperty spawn параметры | Каждый шаг, если нужен спавн |
| 30 | `rtc_spawn_ticket` | `HL_ENABLE_SPAWN` | Создание новых агентов | Атомарно создает новые агенты MI-17 | Spawn параметры, `ticket` | Новые агенты HELI | Каждый шаг, после spawn_mgr |

## Основные категории функций

### Status обработка (функции 3-5, 23-25)
Управление жизненным циклом агентов - ведение счетчиков дней, проверка триггеров, переходы между статусами.

### Quota система (функции 6-22) 
4 идентичных цикла для статусов 1,2,3,5: intent→approve→apply→clear. Массивное дублирование кода.

### Логирование (функции 26-28)
MP2 система записи и экспорта данных - логирование состояния, постпроцессинг, копирование в host.

### Spawn (функции 29-30)
Создание новых агентов - менеджер подготавливает параметры, тикеты создают агентов.

### Утилиты (функции 1-2)
MP5 probe для чтения часов работы и сброс флагов в начале суток.

Дата: 12-09-2025

## Текущая архитектура RTC пайплайна

### 🎯 СТРУКТУРЫ ДАННЫХ FLAME GPU (консолидировано из transform.md, 29-07-2025)

#### MacroProperty1 (md_components) - 20/20 полей ✅

| **Поле** | **field_id** | **ClickHouse Тип** | **FLAME GPU Тип** | **Назначение** |
|----------|--------------|-------------------|------------------|----------------|
| `assembly_time` | 50 | `UInt8` | `UInt8` | Срок сборки в днях |
| `br_mi8` | 51 | `UInt32` | `UInt32` | Beyond Repair для Ми‑8 (минуты) |
| `br_mi17` | 52 | `UInt32` | `UInt32` | Beyond Repair для Ми‑17 (минуты) |
| `common_restricted1` | 53 | `UInt8` | `UInt8` | Общие ограничения 1 |
| `common_restricted2` | 54 | `UInt8` | `UInt8` | Общие ограничения 2 |
| `comp_number` | 55 | `UInt8` | `UInt8` | Количество одноимённых компонентов на 1 вертолёте |
| `group_by` | 56 | `UInt8` | `UInt8` | Группа взаимозаменяемости: 1=МИ‑8Т, 2=МИ‑17, 3..N — прочие |
| `ll_mi17` | 57 | `UInt32` | `UInt32` | Жизненный лимит МИ-17 |
| `ll_mi8` | 58 | `UInt32` | `UInt32` | Жизненный лимит МИ-8 |
| `oh_mi17` | 59 | `UInt32` | `UInt32` | Межремонтный ресурс МИ-17 |
| `oh_mi8` | 60 | `UInt32` | `UInt32` | Межремонтный ресурс МИ-8 |
| `oh_threshold_mi17` | 61 | `UInt32` | `UInt32` | Порог первого межремонтного цикла МИ-17 |
| `oh_threshold_mi8` | 62 | `UInt32` | `UInt32` | Порог первого межремонтного цикла МИ-8 |
| `partout_time` | 63 | `UInt8` | `UInt8` | Срок разборки в днях |
| `ac_type_mask` | 64 | `UInt8` | `UInt8` | Тип ВС (маска) |
| `repair_time` | 65 | `UInt16` | `UInt16` | Срок ремонта в днях |
| `restrictions_mask` | 66 | `UInt8` | `UInt8` | Маска ограничений |
| `sne_new` | 67 | `UInt32` | `UInt32` | Начальная SNE при закупке |
| `ppr_new` | 68 | `UInt32` | `UInt32` | Начальный PPR при закупке |
| `trigger_interval` | 69 | `UInt32` | `UInt32` | Интервал первого межремонтного цикла |

**Экспорт**: `flame_macroproperty1_export` | **Записей**: 7,113

#### MacroProperty3 (heli_pandas) - 14/14 полей ✅

| **Поле** | **field_id** | **ClickHouse Тип** | **FLAME GPU Тип** | **Назначение** |
|----------|--------------|-------------------|------------------|----------------|
| `partseqno_i` | 32 | `UInt32` | `UInt32` | Серийный номер агрегата |
| `psn` | 33 | `UInt32` | `UInt32` | PSN агрегата |
| `address_i` | 34 | `UInt16` | `UInt16` | Адрес установки |
| `lease_restricted` | 35 | `UInt8` | `UInt8` | Ограничения лизинга |
| `status_id` | 36 | `UInt8` | `UInt8` | Статус агрегата |
| `aircraft_number` | 37 | `UInt32` | `UInt32` | Номер ВС |
| `ac_type_mask` | 38 | `UInt8` | `UInt8` | Тип ВС (маска) |
| `ll` | 39 | `UInt32` | `UInt32` | Жизненный лимит |
| `oh` | 40 | `UInt32` | `UInt32` | Межремонтный ресурс |
| `oh_threshold` | 41 | `UInt32` | `UInt32` | Порог первого межремонтного цикла |
| `sne` | 42 | `UInt32` | `UInt32` | SNE текущий |
| `ppr` | 43 | `UInt32` | `UInt32` | PPR текущий |
| `repair_days` | 44 | `UInt16` | `UInt16` | Дни с начала ремонта |
| `mfg_date` | 45 | `Date` | `UInt16` | Дата производства |

**Экспорт**: `flame_macroproperty3_export` | **Записей**: 7,113

> 30-12-2025 — **ETL-инвариант:** два микросервиса после `heli_pandas_group_by_enricher`:
> - `heli_pandas_component_status.py`: агрегаты на ВС в эксплуатации получают `status_id=2`
> - `heli_pandas_serviceable_status.py`: остальные исправные агрегаты получают `status_id=3`
> Это устраняет «нулевые» статусы в MP3 и обеспечивает корректную инициализацию RTC состояний.

#### MacroProperty4 (flight_program_ac) - 8/8 полей ✅

| **Поле** | **field_id** | **ClickHouse Тип** | **FLAME GPU Тип** | **Назначение** |
|----------|--------------|-------------------|------------------|----------------|
| `dates` | 73 | `Date` | `UInt16` | Даты программы |
| `ops_counter_mi8` | 74 | `UInt16` | `UInt16` | Счетчик требуемых ВС МИ-8 на дату D |
| `ops_counter_mi17` | 75 | `UInt16` | `UInt16` | Счетчик требуемых ВС МИ-17 на дату D |
| `ops_counter_total` | 76 | `UInt16` | `UInt16` | Общий счетчик требуемых ВС |
| `new_counter_mi17` | 77 | `UInt8` | `UInt8` | Триггер рождения МИ-17 |
| `trigger_program_mi8` | 78 | `Int8` | `Int8` | Программный триггер балансировки МИ-8 |
| `trigger_program_mi17` | 79 | `Int8` | `Int8` | Программный триггер балансировки МИ-17 |
| `trigger_program` | 80 | `Int8` | `Int8` | Общий программный триггер |

**Экспорт**: `flame_macroproperty4_export` | **Записей**: 4,000

#### MacroProperty5 (flight_program_fl) - 4/4 полей ✅

| **Поле** | **field_id** | **ClickHouse Тип** | **FLAME GPU Тип** | **Назначение** |
|----------|--------------|-------------------|------------------|----------------|
| `dates` | 81 | `Date` | `UInt16` | Даты налетов |
| `aircraft_number` | 82 | `UInt16` | `UInt16` | Номер ВС |
| `ac_type_mask` | 83 | `UInt8` | `UInt8` | Тип ВС (маска) |
| `daily_hours` | 84 | `UInt32` | `UInt32` | Суточный налет (MP5) |

**Экспорт**: `flame_macroproperty5_export` | **Записей**: 1,116,000

#### Property (версионность) - 2/2 поля ✅

| **Поле** | **field_id** | **ClickHouse Тип** | **FLAME GPU Тип** | **Назначение** |
|----------|--------------|-------------------|------------------|----------------|
| `version_date` | 71 | `Date` | `UInt16` | Дата версии данных (также старт симуляции D0) |
| `version_id` | 72 | `UInt8` | `UInt8` | ID версии данных |

**Экспорт**: `flame_property_export` | **Записей**: 1 (скалярные значения)

#### MacroProperty2 (LoggingLayer Planes) — Результаты симуляции

Выходной слой с состояниями планеров. Для совместимости с BI имена полей, скопированных из MP1/MP3/MP4/MP5, сохраняются без переименования. Новые метрики помечены явно.

**Структура логирования:**
- `dates` — дата симуляции (из MP4)
- `aircraft_number` — номер ВС (из MP3)
- `ac_type_mask` — тип планера (из MP3)
- `status_id` — статус планера (из MP3)
- `daily_flight` — ежедневный налёт (из MP5)
- `ops_counter_mi8` / `ops_counter_mi17` — целевой объём на D (из MP4)
- `ops_current_mi8` / `ops_current_mi17` — фактическая укомплектованность на D (**новая**)

#### Семантика дат и версий (консолидировано 29-07-2025)
- `dates` во всех MP — это календарные дни симуляции, последовательно начиная с `version_date` и далее (план: 4,000 суток).
- `version_date` — не только дата выгрузки/версии, но и стартовая дата симуляции (D0).
- MP5: `mp5_daily_hours` (длина = (DAYS+1)*FRAMES, паддинг D+1 обязателен) → на хосте загружается сразу в MacroProperty `mp5_lin`.

---

## Полный хронологический порядок выполнения RTC функций

#### Фаза: Симуляция (каждый шаг)

**Слой 1**: `rtc_probe_mp5` (условный, если `HL_MP5_PROBE=1`)
- Чтение ежедневных часов работы из MP5
- Запись в агентные переменные `daily_today_u32`, `daily_next_u32`

**Слой 2**: `rtc_quota_begin_day` (всегда)
- Сброс операционных флагов в начале суток
- Обнуление `ops_ticket`, `intent_flag`

**Слой 3**: `rtc_status_6` (условный)
- Обработка агентов в статусе 6 (хранение)
- Счетчик дней, проверка триггеров partout

**Слой 4**: `rtc_status_4` (условный)
- Обработка агентов в статусе 4 (ремонт)
- Счетчик дней ремонта, триггеры завершения

**Слой 5**: `rtc_status_2` (условный)
- Обработка агентов в статусе 2 (эксплуатация)
- Начисление наработки, проверка лимитов

**Слой 6-9**: Quota цикл для статуса 2
- `rtc_quota_intent_s2` → `rtc_quota_approve_s2` → `rtc_quota_apply_s2` → `rtc_quota_clear_s2`

**Слой 10-13**: Quota цикл для статуса 3
- `rtc_quota_intent_s3` → `rtc_quota_approve_s3` → `rtc_quota_apply_s3` → `rtc_quota_clear_s3`

**Слой 14**: `rtc_status_3_post`
- Пост-обработка статуса 3, переходы статусов

**Слой 15-18**: Quota цикл для статуса 5
- `rtc_quota_intent_s5` → `rtc_quota_approve_s5` → `rtc_quota_apply_s5` → `rtc_quota_clear_s5`

**Слой 19-22**: Quota цикл для статуса 1
- `rtc_quota_intent_s1` → `rtc_quota_approve_s1` → `rtc_quota_apply_s1` → `rtc_quota_clear_s1`

**Слой 23**: `rtc_status_1_post`
- Пост-обработка статуса 1

**Слой 24**: `rtc_status_5_post`
- Пост-обработка статуса 5

**Слой 25**: `rtc_status_2_post`
- Пост-обработка статуса 2 (переход 2→3 при отсутствии билета)

**Слой 26**: `rtc_log_day` (условный)
- Логирование состояния агентов в MP2

**Слой 27**: `rtc_spawn_mgr` (условный, если включен spawn)
- Подготовка параметров для создания новых агентов

**Слой 28**: `rtc_spawn_ticket` (условный, если включен spawn)
- Атомарное создание новых агентов MI-17

#### Фаза: Постпроцессинг (только при export_phase=2)

**Слой 29**: `rtc_mp2_postprocess`
- Обработка накопленных логов MP2
- Агрегирование данных по дням

#### Фаза: Экспорт (только при export_phase=1)

**Слой 30**: `rtc_mp2_copyout`
- Копирование данных из MP2 в агентные переменные
- Подготовка для экспорта в host

## Выявленные проблемы

1. **Дублирование кода**: 4 идентичных quota цикла (слои 6-9, 10-13, 15-18, 19-22)
2. **Избыточная сложность**: 30 слоев вместо потенциальных 10-15
3. **Нелогичный порядок**: status_3_post между quota циклами
4. **Жесткая структура**: сложно отключать отдельные компоненты
5. **NVRTC проблемы**: компиляция шаблонов с FRAMES*DAYS

## Предложения по оптимизации

1. **Унификация quota системы**: Один параметризованный цикл вместо 4 копий
2. **Группировка по фазам**: Status → Quota → Post → Log → Spawn
3. **Условные блоки**: Возможность отключения целых групп функций
4. **Модульная архитектура**: Разделение RTC функций по файлам
5. **Профили выполнения**: Предустановленные конфигурации для разных сценариев


## MP2 Device-side Export (обновлено 28-09-2025)

В V2 архитектуре реализован device-side экспорт данных для оптимизации производительности:

### Архитектура MP2 (упрощенная)

1. **MacroProperty MP2**: набор массивов на GPU для хранения снимков состояния агентов
   - **Плотная матрица** без кольца: размер = (DAYS+1) × FRAMES
   - Адресация: `pos = day * FRAMES + idx` (простая, без модуля)
   - Поля: day, idx, aircraft_number, state, intent_state, sne, ppr, repair_days, dt, dn
   - Запись через `exchange()` операцию в RTC функциях

2. **RTC функции записи**: пишут данные напрямую из GPU без копирования на CPU
   - Отдельные функции для каждого состояния (rtc_mp2_write_inactive, rtc_mp2_write_operations, etc.)
   - Гарантируют корректную запись текущего state агента
   - Выполняются в конце каждого шага симуляции
   - **Только запись**, чтение MP2 из RTC в этой версии не используется

3. **Host функция дренажа**: выгружает данные в ClickHouse **один раз в конце симуляции**
   - По умолчанию финальный дренаж: `--mp2-drain-interval=0` (интервальные выключены)
   - Размер батча: 250000 записей (стандарт из sim_master)
   - Таблица: sim_masterv2 (создаётся автоматически)
   - Колоннарные INSERT (`columnar=True`) для снижения накладных расходов драйвера
   - Поле `day_date` вычисляется на стороне ClickHouse (`Date MATERIALIZED addDays(...)`), в Python не считается

4. **Инварианты пайплайна**:
   - MP2 формируется и живёт на GPU всю симуляцию
   - Запись в MP2 — только из RTC-ядер (через exchange/++/+=)
   - Read/Write разводятся по слоям (в текущей версии только Write)
   - Хост прикасается к MP2 один раз — при финальном сливе
   - СУБД — только пакетами (~250k) с сохранением типов

5. **Тайминги выполнения** (как в sim_master):
   - Загрузка модели и данных - время подготовки данных из ClickHouse
   - Обработка на GPU - общее время выполнения всех шагов симуляции
   - Выгрузка в СУБД - время финального дренажа MP2
   - Общее время выполнения - от начала до конца работы скрипта
   - Среднее время на шаг - расчетная метрика производительности

### Параметры запуска (актуально)
- `--enable-mp2` — включает device-side экспорт (MP2)
- `--mp2-drain-interval 0` — финальный дренаж без интервалов (значение по умолчанию)
- При включённом MP2 печатаются сводные метрики дренажа: `rows`, `flushes`, `max_batch`, `flush_time`, `rows/s`

## Инвентаризация Env (Mode A + Spawn, MP2=off)

- **Property (скаляры)**
  - `version_date`: опорная дата (ord) для расчётов в S1/S5 post и логике.
  - `days_total`: горизонт расчёта; гейт для day/индексации в RTC.
  - `frames_total`: размерность FRAMES, параметр шаблонов RTC.
  - `export_phase`: гейт (0 в данной конфигурации); ряд RTC функций ранится только при 0.
  - `export_day`: не используется при MP2=off.
  - `approve_policy`: политика менеджера approve; 0 — по индексу (детерминизм).
  - `mi17_repair_time_const`, `mi17_partout_time_const`, `mi17_assembly_time_const`: нормативы времени из MP1 для новорождённых.
  - `mi17_br_const`, `mi17_ll_const`, `mi17_oh_const`: пороги BR/LL/OH для MI‑17 (для новорождённых).

- **PropertyArray (по дням, длина = DAYS)**
  - `mp4_ops_counter_mi8`, `mp4_ops_counter_mi17`: суточные квоты (используются в approve всех фаз).
  - `mp4_new_counter_mi17_seed`: план спавна Ми‑17 (используется spawn_mgr/spawn_ticket).
  - `month_first_u32`: ord первого дня месяца (mfg_date новорождённых).

- **PropertyArray (по кадрам, длина = FRAMES)**
  - `mp3_mfg_date_days`: дата производства по кадрам (используется в approve S1 приоритетом «самые молодые»).

- **PropertyArray (линейный, длина = (DAYS+1)*FRAMES)**
  - `mp5_daily_hours`: часовые данные MP5; читаются только при включённом `HL_MP5_PROBE=1` (функция `rtc_probe_mp5`). При MP5_PROBE=0 фактически не используются; `daily_today_u32/daily_next_u32` остаются 0.

- **MacroProperty (по кадрам, длина = FRAMES)**
  - Intent/Approve (статусы 2/3/5/1): `mi8_intent`, `mi17_intent`, `mi8_approve`, `mi17_approve`,
    `mi8_approve_s3`, `mi17_approve_s3`, `mi8_approve_s5`, `mi17_approve_s5`, `mi8_approve_s1`, `mi17_approve_s1`.

- **MacroProperty (по дням, длина = DAYS)**
  - Spawn: `spawn_need_u32`, `spawn_base_idx_u32`, `spawn_base_acn_u32`, `spawn_base_psn_u32`.

- **MacroProperty (служебные, длина = 1) — в текущей логике RTC не читаются**
  - `next_idx_spawn`, `next_aircraft_no_mi17`, `next_psn_mi17` (используются агентными переменными `spawn_mgr`, а в Env — как резерв).

- **Неиспользуемые при MP2=off**
  - Все MP2 MacroProperty (`mp2_*`) и фазы `export_phase` 1/2 (copyout/postprocess).
  - `export_day` (активен только при `export_phase=1`).

- **Костыли/хардкод (зафиксировано, без изменений логики)**
  - Spawn новорождённых: `group_by=2`, `ac_type_mask=64u`, `partseqno_i=70482u` заданы константами в `rtc_spawn_mi17_atomic`.
  - Политика approve по умолчанию: `approve_policy=0` (скан по idx) — упрощённая детерминированная стратегия.
  - Лимит тикетов спавна: в текущем сценарии создаётся 16 агентов `spawn_ticket`, что ограничивает суточный спавн 16 шт. (если `need > 16`, часть переносится на следующий шаг; требует явного решения при масштабировании).
  - Заполнение `daily_today_u32/daily_next_u32`: без `HL_MP5_PROBE=1` остаются нулями (агенты 2‑го статуса считают dt=0).

- **Рекомендации для «реальных данных» (без постпроцессинга MP2)**
  - Включить чтение MP5 на GPU: запуск с `HL_MP5_PROBE=1` (и `--seatbelts on`) для заполнения `daily_today_u32/daily_next_u32` из `mp5_daily_hours`.
  - Убедиться, что `mp5_daily_hours` имеет длину `(DAYS+1)*FRAMES` и индексируется формулой `row = day*FRAMES + idx`, паддинг `D+1` присутствует.
  - Контролировать источники порогов для новорождённых: `mi17_*_const` должны заполняться из MP1 (это уже реализовано в менеджере симуляции).
  - Если потребуется приоритизация approve помимо «по idx», согласовать новую политику (например, по `mp3_mfg_date_days` для S2/S3/S5) и включить её явно через `approve_policy`.

## Переходы состояний в V7/V8 (актуально)

В текущей архитектуре переходы выполняются напрямую через RTC-функции с `FunctionCondition` и `setEndState`. Явных функций подтверждения нет: если условие перехода не выполнено, FLAME GPU сохраняет состояние агента.

Детали по актуальным переходам и инвариантам см. в `docs/architecture/limiter_architecture.md` и `docs/architecture/adaptive_steps_logic.md`.
