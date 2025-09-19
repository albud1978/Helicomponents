# RTC Pipeline Architecture

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

### Полный хронологический порядок выполнения RTC функций

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


## V2: Пошаговый пайплайн и загрузка Env

- Нумерация шагов (скрипты `code/sim_v2/`):
  1) 01_setup_env.py — загрузка Env из ClickHouse, построение `frames_index`, строгие валидации
  2) 02_build_model_base.py — базовая модель без RTC, применение Env, smoke
  3) 03_add_probe_mp5.py — подключение `rtc_probe_mp5` (MacroProperty), smoke/trace
  4) 04_add_status_246.py — статусы 2/4/6 с чтением MP5, проверки по validation.md
  5) 05_export_mp5_excel.py — диагностика/экспорт MP5 в Excel (длинный/шахматный вид)
  99) 99_debug_probe_mp5.py — минимальный стенд для компиляции/отладки NVRTC

- **Шаг setup_env (обязательный, первый)**
  - Источник: ClickHouse (готовые массивы MP1/MP3/MP4/MP5).
  - Построение индексов и размерностей:
    - `frames_index`: плотный индекс по `aircraft_number` из объединения (MP3_planes ∪ MP5_planes), порядок: MP3 → MP5-only.
    - `frames_union_no_future`: |MP3_planes ∪ MP5_planes| (без будущих ACN).
    - `first_reserved_idx`: начало блока MP5‑only слотов (заполняется спавном).
    - `first_future_idx`: индекс `base_acn_spawn` если присутствует в union; иначе = `frames_union_no_future`.
    - `frames_total`: при необходимости → `frames_union_no_future + future_buffer`.
  - Формирование Env массивов (по мере потребности RTC):
    - Всегда: `mp4_ops_counter_mi8/mi17`, `mp4_new_counter_mi17_seed`, `month_first_u32` (длина = DAYS).
    - Для MP5‑probe: `mp5_daily_hours` (длина = (DAYS+1)*FRAMES, паддинг D+1 обязателен).
    - Для S1 приоритета: `mp3_mfg_date_days` (длина = FRAMES).
  - Валидации (жёсткие):
    - Длины массивов соответствуют DAYS/FRAMES.
    - `(DAYS+1)*FRAMES` для MP5, индексация `row = day*FRAMES + idx` корректна.
    - `frames_index` плотный, без дырок; новые индексы спавна стартуют с `first_reserved_idx`/`first_future_idx`.

- **Поэтапная загрузка Env**
  - Под RTC‑потребителей данных: включаем только нужные массивы/скаляры на текущем шаге.
  - Примеры:
    - MP5‑probe включён → добавляем только `mp5_daily_hours`.
    - Подключаем S1‑approve (приоритет по дате) → добавляем `mp3_mfg_date_days`.

- **Тест setup_env (smoke)**
  - Печать: FRAMES, DAYS, размеры ключевых массивов, top‑5 `frames_index` и границы блоков.
  - Проверка инвариантов и assert при несоответствии.

