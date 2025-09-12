# RTC Pipeline Architecture

## Анализ всех RTC функций системы

### Полная таблица RTC функций

| № | RTC Функция | Флаг активации | Назначение | Что делает | Входные данные | Выходные данные | Когда выполняется |
|---|-------------|----------------|------------|------------|----------------|----------------|-------------------|
| 1 | `rtc_prepare_day` | `HL_ALWAYS_ON` | Подготовка начала суток | Объединяет чтение MP5 (`dt/dn`) и сброс однодневных флагов; НЕ трогает `active_trigger` | MP5 `mp5_daily_hours`, `idx`, `day`; `export_phase` | `daily_today_u32`, `daily_next_u32`, `ops_ticket=0`, `intent_flag=0`, `assembly_trigger=0`, `partout_trigger=0` | Каждый шаг, самым первым |
| 3 | `rtc_status_6` | `HL_ENABLE_STATUS_6` | Обработка статуса хранения | Ведет счетчик дней в статусе 6, проверяет триггер partout | `status_id`, `s6_started`, `s6_days`, `partout_time` | `s6_days++`, `partout_trigger` | Каждый шаг, для агентов в статусе 6 |
| 4 | `rtc_status_4` | `HL_ENABLE_STATUS_4` | Обработка статуса ремонта | Ведет счетчик дней ремонта, проверяет триггеры завершения | `status_id`, `repair_days`, `repair_time`, `assembly_time`, `partout_time` | `repair_days++`, `active_trigger`, `assembly_trigger`, `partout_trigger` | Каждый шаг, для агентов в статусе 4 |
| 5 | `rtc_status_2` | `HL_ENABLE_STATUS_2` | Обработка статуса эксплуатации | Начисляет наработку, проверяет LL/OH/BR | `status_id`, `daily_today_u32`, `sne`, `ppr`, `ll`, `oh`, `br` | `sne+=dt`, `ppr+=dt`, переходы статусов | Каждый шаг, для агентов в статусе 2 |
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
| 27 | `rtc_mp2_postprocess` | `HL_ENABLE_MP2_POST` | Постпроцессинг MP2 | Обрабатывает накопленные логи MP2 | MP2 массивы | Агрегированные данные | Только при `export_phase=2` |
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

### Утилиты (функция 1)
Подготовка суток: чтение MP5 и сброс флагов выполнены одной RTC функцией `rtc_prepare_day`.

Дата: 12-09-2025

## Модульная архитектура RTC пайплайна (обновлено 12-09-2025)

### Новая структура кода

Принцип: Каждая RTC функция в отдельном файле для модульности и упрощения отладки.

#### Структура папок:
```
code/
├── sim/                          # Модуль симуляции
│   ├── __init__.py              # Экспорты модуля  
│   ├── env_setup.py             # Подготовка Environment из ClickHouse
│   ├── pipeline_config.py       # RTC_PIPELINE + профили конфигурации
│   ├── sim_builder.py           # Полный сборщик модели (для production)
│   ├── simple_builder.py        # Упрощенный сборщик (для отладки)
│   └── runners.py               # SmokeRunner + ProductionRunner
├── rtc/                         # RTC функции (отдельные файлы)
│   ├── __init__.py              # BaseRTC базовый класс
│   ├── begin_day.py             # rtc_prepare_day (объединяет probe_mp5 + quota_begin_day)
│   ├── status_2.py              # rtc_status_2 (эксплуатация)
│   ├── status_4.py              # rtc_status_4 (ремонт)
│   ├── status_6.py              # rtc_status_6 (хранение)
│   ├── quota_intent.py          # rtc_quota_intent_* (параметризованно)
│   ├── quota_approve.py         # rtc_quota_approve_* (параметризованно)
│   ├── quota_apply.py           # rtc_quota_apply (универсальный)
│   ├── quota_clear.py           # rtc_quota_clear (универсальный)
│   ├── status_post.py           # rtc_status_*_post (пост-обработка)
│   ├── log_mp2.py               # rtc_log_day, rtc_log_d0, rtc_mp2_copyout
│   ├── spawn_mgr.py             # rtc_spawn_mgr
│   └── spawn_ticket.py          # rtc_spawn_ticket
├── sim_master_v2.py             # Новый упрощенный мастер
├── sim_rtc_step_by_step.py      # Пошаговое добавление RTC (для отладки)
└── test_minimal_env.py          # Минимальный тест Environment
```

### Профили конфигурации

4 предустановленных профиля в `sim/pipeline_config.py`:

| Профиль | Описание | Функций | Назначение |
|-------------|--------------|-------------|----------------|
| `minimal` | Только статусы 2,4,6 без квот | 4/28 | Базовое тестирование |
| `quota_smoke` | Полные квоты без спавна | 26/28 | Тестирование системы квот |
| `status_246` | Статусы 2,4,6 с логированием | 6/28 | Тестирование статусов |
| `production` | Все функции включены | 28/28 | Продуктивные прогоны |

### Пошаговая разработка

Инструменты для отладки:
- `test_minimal_env.py` - тест базового Environment
- `sim_rtc_step_by_step.py` - добавление RTC по одной
- `test_new_architecture.py` - полный тест архитектуры

Команды для пошагового добавления RTC:
```bash
# Базовый Environment (без RTC)
python3 code/test_minimal_env.py

# Добавление одной RTC функции
python3 code/sim_rtc_step_by_step.py --add-begin-day

# Добавление нескольких функций
python3 code/sim_rtc_step_by_step.py --add-begin-day --add-status-2
```

## Правило исполнения RTC
- Ветвления в оркестраторе удалены: RTC выполняются строго последовательно, слой за слоем по `RTC_PIPELINE`.
- Никаких условных веток со смешением порядка: только профили включают/отключают функции, порядок фиксирован.

