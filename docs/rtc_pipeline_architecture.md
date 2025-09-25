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
  3) 03_add_probe_mp5.py — host‑инициализация `mp5_lin` и/или probe‑smoke MP5 (без копирования RTC)
  4) 04_add_status_246.py — статусы 2/4/6 с чтением MP5 (без копирования), проверки по validation.md
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
    - Для MP5: `mp5_daily_hours` (длина = (DAYS+1)*FRAMES, паддинг D+1 обязателен) → на хосте загружается сразу в MacroProperty `mp5_lin`.
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


## V2: Таблица скриптов и их RTC/функционал (host‑only инициализация MP5)

| № | Скрипт/Шаг | RTC Функции | Назначение | Что делает | Входные данные | Выходные данные | Когда выполняется |
|---|------------|-------------|------------|------------|----------------|-----------------|-------------------|
| 1 | `code/sim_v2/01_setup_env.py` | — | ETL и снапшоты окружения | Загружает из ClickHouse MP1/MP3/MP4/MP5, строит `frames_index`, вычисляет `FRAMES`/`DAYS`, валидирует формы; сохраняет `env_snapshot.json` | ClickHouse: MP1/MP3/MP4/MP5 | `env_snapshot.json`, диагностические логи | Один раз до сборки модели |
| 2 | `code/sim_v2/02_build_model_base.py` | — | Базовая модель без RTC | Объявляет Environment и агент `component` с базовыми переменными; инициализирует скаляры `version_date/frames_total/days_total`; smoke печать | `env_snapshot.json`, массивы Env из ClickHouse | Готовая базовая модель (без RTC), сообщение `V2 Base OK` | Один раз до запуска RTC |
| 3 | `code/sim_v2/03_add_probe_mp5.py` | — (HostFunction `hf_load_mp5`), опц. `rtc_probe_mp5_d{DAYS}` | Host‑инициализация MP5 и/или smoke‑проба | Заполняет `mp5_lin` напрямую на хосте из `mp5_daily_hours_linear` (без `mp5_src` и копирующего RTC); опционально запускает probe‑ядро для верификации `dt/dn` | `env_snapshot.json`, `mp5_daily_hours_linear` | Заполненный `mp5_lin`, логи sample(dt,dn) | Один раз до симуляции / отладка |
| 4 | `code/sim_v2/04_add_status_246.py` | `rtc_probe_mp5`, `rtc_status_2` | Основной симуляционный цикл (минимум) | На каждом дне читает `mp5_lin` в `dt/dn`, выполняет статус‑2 (начисление `sne/ppr`, LL‑порог 2→6); логи и валидации | `env_snapshot.json`, `mp5_lin`, `mp3_ll_by_frame`, `mp3_oh_by_frame` | Обновлённые агентные переменные, счётчики переходов и валидаций | Каждый симуляционный день |
| 5 | `code/sim_v2/05_export_mp5_excel.py` | — | Экспорт MP5 в Excel | Формирует long‑view и матричный сэмпл MP5; пишет в XLSX чанками | `mp5_daily_hours_linear`, `frames_index` | Файл Excel (`tmp/mp5_export.xlsx`) | По необходимости, вне цикла |
| 6 | `code/sim_v2/99_debug_probe_mp5.py` | `rtc_probe_mp5_d{DAYS}` | NVRTC/RTC отладка | Минимальная компиляция probe‑ядра; печать исходника и ошибок NVRTC | `env_snapshot.json`, размеры DAYS/FRAMES | Диагностические логи компиляции/запуска | Ад‑hoc отладка |

Примечания:
- На шаге 04 в текущей реализации включён только `rtc_status_2`; `rtc_status_4`/`rtc_status_6` планируются к добавлению после стабилизации MP5 и валидаторов.
- Fallback‑инициализация через `rtc_mp5_copy_columns` исключена: MP5 загружается напрямую в `mp5_lin` на хосте.

## V2: Оркестрация с state-based архитектурой (обновлено 22.12.2024)

### Ключевые изменения:
- **State-based**: Переход с переменной `status_id` на FLAME GPU States для оптимизации GPU
- **Централизованное квотирование**: Единый менеджер квот вместо распределенной логики
- **Атомарные переходы**: Все изменения состояний в единой RTC функции в конце степа

### Структура выполнения на каждом степе:
1. **Фаза 0**: HostFunction для MP5 (только на step=0)
2. **Фаза 1**: Параллельные RTC по состояниям
   - Отдельная RTC функция для каждого state (0-6)
   - Все функции в одном слое, GPU фильтрует автоматически
   - Параллельное выполнение между состояниями
3. **Фаза 2**: Последовательное квотирование с приоритетами
4. **Фаза 3**: Единая функция переходов состояний

### Ключевые архитектурные принципы:
- **OH из MP1 при создании**: Значения oh определяются при создании агентов, не в RTC
- **Фильтрация планеров**: Только group_by ∈ {1,2} становятся агентами
- **Фиксированные размеры**: MAX_FRAMES из данных, MAX_DAYS=4000
- **Единая загрузка MP5**: Через HostFunction на step=0

Детальное описание: [v2_architecture_consolidated.md](v2_architecture_consolidated.md)
(Старый документ: [v2_state_based_architecture.md](v2_state_based_architecture.md))

ENV‑инварианты:
- `FRAMES`, `DAYS`, `frames_index` берутся из `env_snapshot.json` и не пересчитываются в шагах 03/04.
- MacroProperty размеры: MAX_FRAMES определяется из данных MP3/MP5, MAX_DAYS=4000, MAX_SIZE=MAX_FRAMES*(MAX_DAYS+1). Всегда используется полный размер буфера для единообразной компиляции RTC независимо от периода симуляции.
- После инициализации `mp5_lin` доступен только на чтение в RTC; запись в него запрещена.
- RTC имена фиксированы: `rtc_probe_mp5`, `rtc_status_2`, `rtc_status_4`, `rtc_status_6` (без `rtc_mp5_copy_columns`).

Критерии приёмки и тест‑матрица (host‑only):
- DAYS={5,90,365}; отсутствие падений NVRTC; логи содержат размеры `mp5_lin`, checksum первых 64КБ, первые значения.
- Валидации: неизменность S6, `Δsne_s2 == sum(dt_s2)`; корректная индексация `base = day*MAX_FRAMES + idx`.

## Формат логирования симуляции (V2, минимальный)

Договорённый порядок и формат вывода на экран для оперативной диагностики:

- Сводка по состояниям на КАЖДОМ шаге симуляции:
  - Строка: `Step <N>: counts inactive=<...>, operations=<...>, serviceable=<...>, repair=<...>, reserve=<...>, storage=<...>`
  - Назначение: быстрый контроль численности агентов по state на шаге.

- Логи изменений intent ТОЛЬКО для state=operations, когда intent сменился на значение, отличное от 2:
  - Строка (одна на событие):
    - `[Day <N> | date=<YYYY-MM-DD>] AC <aircraft_number> idx=<idx>: intent <old>-><new> (operations) `
      `sne=<sne_curr>, ppr=<ppr_curr>, dt=<dt_curr>, dn=<dn_curr>, s_next=<sne_old+dn_old>, p_next=<ppr_old+dn_old>, `
      `ll=<ll>, oh=<oh>, br=<br>`
  - Дата: вычисляется как `date = 1970-01-01 + (version_date_u16 + day_u16)` и печатается в ISO (`YYYY-MM-DD`).
  - Прогнозы `s_next/p_next`: считаются ИЗ СОСТОЯНИЯ «ДО шага», то есть без сегодняшнего `dt`:
    - `s_next = sne_old + dn_old`, `p_next = ppr_old + dn_old`.

Примечание:
- Подробные перечни агентов и расширенный отладочный вывод отключены по умолчанию и не печатаются без отдельного запроса.

## State Manager и Intent-based архитектура (обновлено 24.09.2025)

### Концепция intent_state

Архитектура основана на разделении логики определения намерений и фактических переходов состояний:

1. **intent_state** - переменная агента, указывающая желаемое следующее состояние
2. **RTC функции состояний** - определяют intent_state на основе бизнес-логики, НЕ меняют фактическое состояние
3. **State Manager** - централизованно обрабатывает все переходы на основе intent_state

### Принципы работы

#### 1. Установка intent_state в RTC функциях:
```cuda
// В rtc_state_2_operations
if (ppr_next >= oh && sne_next < br) {
    // Хотим перейти в ремонт
    FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);
} else if (sne_next >= ll) {
    // Хотим перейти в хранение
    FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);
} else {
    // Остаёмся в operations
    FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
}
```

#### 2. Обязательность intent_state:
- Каждая RTC функция состояния ДОЛЖНА установить intent_state
- intent_state = 0 считается ошибкой
- Даже если агент остаётся в том же состоянии, нужно явно установить intent_state равным текущему состоянию

#### 3. State Manager обрабатывает переходы:
```cuda
// rtc_state_manager выполняется в отдельном слое после всех RTC состояний
const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");

// Детерминированные переходы (без квот)
if (intent == 4u) {  // Переход в ремонт
    FLAMEGPU->setInitialState("operations");
    FLAMEGPU->setEndState("repair");
}
// ... другие переходы
```

### Преимущества архитектуры

1. **Модульность**: Логика состояний изолирована от логики переходов
2. **Гибкость**: Легко добавлять новые условия переходов
3. **Централизованное квотирование**: Все квоты обрабатываются в одном месте
4. **Отладка**: Можно логировать все намерения перед применением

### Текущая реализация

#### Реализованные модули:
- `rtc_state_1_inactive.py` - обработка неактивных агентов
- `rtc_state_2_operations.py` - обработка агентов в эксплуатации  
- `rtc_state_3_serviceable.py` - обработка исправных агентов
- `rtc_state_4_repair.py` - обработка агентов в ремонте
- `rtc_state_5_reserve.py` - обработка агентов в резерве
- `rtc_state_6_storage.py` - обработка агентов в хранении
- `rtc_state_manager_test.py` - тестовый менеджер для перехода 2→4

#### Правила использования setInitialState/setEndState:
1. В RTC функциях состояний:
   - `setInitialState(state_name)` - обязательно в начале
   - `setEndState(state_name)` - обязательно в конце (тот же state)
   
2. В State Manager:
   - `setInitialState` - исходное состояние для перехода
   - `setEndState` - целевое состояние перехода

### Планы развития

1. **Полный State Manager**: Обработка всех типов переходов
2. **Система квот**: Приоритизация и ограничения на переходы
3. **Логирование переходов**: Детальная отчётность о всех изменениях состояний

## Стандартный паттерн работы с MacroProperty MP5

### 1. Объявление в модели (с динамически определяемыми константами):
```python
# MAX_FRAMES определяется из данных при загрузке
from model_build import MAX_FRAMES, MAX_DAYS, MAX_SIZE, set_max_frames_from_data
# После загрузки данных:
set_max_frames_from_data(frames_count_from_data)
# Затем в модели:
e.newMacroPropertyUInt32("mp5_lin", MAX_SIZE)
```

### 2. Инициализация через HostFunction:
```python
class HF_InitMP5(fg.HostFunction):
    def __init__(self, data: list[int], frames: int, days: int):
        super().__init__()
        self.data = data
        self.frames = frames
        self.days = days

    def run(self, FLAMEGPU):
        mp = FLAMEGPU.environment.getMacroPropertyUInt32("mp5_lin")
        for d in range(self.days + 1):
            for f in range(self.frames):
                src_idx = d * self.frames + f
                dst_idx = d * MAX_FRAMES + f  # Важно: используем MAX_FRAMES
                if src_idx < len(self.data):
                    mp[dst_idx] = self.data[src_idx]
```

### 3. Чтение в RTC (с размерами, определенными на этапе компиляции):
```cuda
const unsigned int base = day * ${MAX_FRAMES}u + frame_idx;  
auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_SIZE}u>("mp5_lin");
const unsigned int value = mp[base];
```
Примечание: `${MAX_FRAMES}` и `${MAX_SIZE}` подставляются при генерации RTC кода из Python.
