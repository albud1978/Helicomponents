# Transform - Преобразование данных для Flame GPU

**Дата создания:** 23-07-2025  
**Последнее обновление:** 24-08-2025

> Статус реализации (10-08-2025): логика RTC, триггеры и GPU-пайплайн реализованы в коде, но ещё не проходили тестирование в целевой среде (ClickHouse/GPU). Требуется прогон и верификация.

## 📊 Обзор процесса Transform

Этап Transform отвечает за преобразование данных из Extract в формат Agent-Based Modeling для Flame GPU.

**Основные компоненты:**
- **MacroProperty1-5**: Загрузка данных из ClickHouse в FLAME GPU Environment Property Arrays
- **Agent Initialization**: Создание агентов планеров с начальными состояниями
- **Simulation Execution**: Запуск GPU симуляции с RTC функциями
- **Results Export**: Выгрузка результатов LoggingLayer в ClickHouse

<!-- Раздел о микросервисной архитектуре удалён в рамках консолидации: Transform — единая точка правды. -->

### ЕДИНЫЙ ИСТОЧНИК ПРАВДЫ ДЛЯ TRANSFORM (24-08-2025)
- Вся логика суточного цикла на GPU, порядок слоёв, правила квотирования и семантика экспорта MP2 закреплены в этом документе (`docs/architecture/transform.md`).
- Разделы в других документах (включая `flame_gpu_architecture.md`) носят вспомогательный характер. При любых расхождениях приоритет у этого документа.
- Терминология обновлена: вместо устаревшего `status_change` используется внутренний `next_status`, применяемый при фиксации «статуса на конец дня».

## 🎯 СТРУКТУРЫ ДАННЫХ FLAME GPU (29-07-2025)

### ✅ **РЕАЛИЗОВАННЫЕ КОМПОНЕНТЫ:**

#### MacroProperty1 (md_components) - 20/20 полей ✅

| **Поле** | **field_id** | **ClickHouse Тип** | **FLAME GPU Тип** | **Назначение** |
|----------|--------------|-------------------|------------------|----------------|
| `assembly_time` | 50 | `UInt8` | `UInt8` | Срок сборки в днях |
| `br_mi8` | 51 | `UInt32` | `UInt32` | Beyond Repair для Ми‑8 (минуты) |
| `br_mi17` | 52 | `UInt32` | `UInt32` | Beyond Repair для Ми‑17 (минуты) |
| `common_restricted1` | 53 | `UInt8` | `UInt8` | Общие ограничения 1 |
| `common_restricted2` | 54 | `UInt8` | `UInt8` | Общие ограничения 2 |
| `comp_number` | 55 | `UInt8` | `UInt8` | Количество одноимённых компонентов на 1 вертолёте (планер — корневой агрегат multiBOM) |
| `group_by` | 56 | `UInt8` | `UInt8` | Группа взаимозаменяемости агрегатов: 1=МИ‑8Т (планер), 2=МИ‑17 (планер), 3..N — прочие группы взаимозаменяемых агрегатов |
| `ll_mi17` | 57 | `UInt32` | `UInt32` | Жизненный лимит МИ-17 |
| `ll_mi8` | 58 | `UInt32` | `UInt32` | Жизненный лимит МИ-8 |
| `oh_mi17` | 59 | `UInt32` | `UInt32` | Межремонтный ресурс МИ-17 |
| `oh_mi8` | 60 | `UInt32` | `UInt32` | Межремонтный ресурс МИ-8 |
| `oh_threshold_mi17` | 61 | `UInt32` | `UInt32` | Порог первого межремонтного цикла для МИ-17 (если применимо) |
| `oh_threshold_mi8` | 62 | `UInt32` | `UInt32` | Порог первого межремонтного цикла для МИ-8 (если применимо) |
| `partout_time` | 63 | `UInt8` | `UInt8` | Срок разборки в днях |
| `ac_type_mask` | 64 | `UInt8` | `UInt8` | Тип ВС (маска) |
| `repair_time` | 65 | `UInt16` | `UInt16` | Срок ремонта в днях |
| `restrictions_mask` | 66 | `UInt8` | `UInt8` | Маска ограничений (multihot: type/common/trigger_interval) |
| `sne_new` | 67 | `UInt32` | `UInt32` | Начальная SNE при закупке (в т.ч. second-hand) |
| `ppr_new` | 68 | `UInt32` | `UInt32` | Начальный PPR при закупке (в т.ч. second-hand) |
| `trigger_interval` | 69 | `UInt32` | `UInt32` | Признак/интервал первого межремонтного цикла (агрегаты со спецификой первого OH) |
| `type_restricted` | 70 | `UInt8` | `UInt8` | Ограничение по типу ВС: при установке на тип агрегат навсегда закрепляется за этим типом |

**Экспорт**: `flame_macroproperty1_export` (реальные имена полей)
> Примечание: поле `br` помечено как DEPRECATED и из загрузки исключено; вместо него используются `br_mi8`, `br_mi17` (в минутах).
**Записей**: 7,113

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
| `oh_threshold` | 41 | `UInt32` | `UInt32` | Порог первого межремонтного цикла (если применимо) |
| `sne` | 42 | `UInt32` | `UInt32` | SNE текущий |
| `ppr` | 43 | `UInt32` | `UInt32` | PPR текущий |
| `repair_days` | 44 | `UInt16` | `UInt16` | Дни с начала ремонта |
| `mfg_date` | 45 | `Date` | `UInt16` | Дата производства |

> Примечание: для задач Transform добавлены аналитические поля `group_by` (`UInt8`) и `status_change` (`UInt8`) — используются при загрузке/экспорте/валидации MacroProperty3. Закрепление `field_id` для них будет оформлено в процессе интеграции Transform-координатора.

**Экспорт**: `flame_macroproperty3_export` (реальные имена полей)
**Записей**: 7,113

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

**Экспорт**: `flame_macroproperty4_export` (реальные имена полей)
**Записей**: 4,000

#### MacroProperty5 (flight_program_fl) - 4/4 полей ✅

| **Поле** | **field_id** | **ClickHouse Тип** | **FLAME GPU Тип** | **Назначение** |
|----------|--------------|-------------------|------------------|----------------|
| `dates` | 81 | `Date` | `UInt16` | Даты налетов |
| `aircraft_number` | 82 | `UInt16` | `UInt16` | Номер ВС |
| `ac_type_mask` | 83 | `UInt8` | `UInt8` | Тип ВС (маска) |
| `daily_hours` | 84 | `UInt32` | `UInt32` | Суточный налет |

**Экспорт**: `flame_macroproperty5_export` (реальные имена полей)
**Записей**: 1,116,000

#### Property (heli_pandas) - 2/2 поля ✅

| **Поле** | **field_id** | **ClickHouse Тип** | **FLAME GPU Тип** | **Назначение** |
|----------|--------------|-------------------|------------------|----------------|
| `version_date` | 71 | `Date` | `UInt16` | Дата версии данных (также старт симуляции D0) |
| `version_id` | 72 | `UInt8` | `UInt8` | ID версии данных |

**Экспорт**: `flame_property_export` (реальные имена полей)
**Записей**: 1 (скалярные значения)

### 📌 Семантика дат и версий
- `dates` во всех MP — это календарные дни симуляции, последовательно начиная с `version_date` и далее (план: 4,000 суток).
- `version_date` — не только дата выгрузки/версии, но и стартовая дата симуляции (D0).

### 🔧 Улучшение экспорта (29-07-2025)

**Проблема:** MacroProperty4, MacroProperty5 и Property экспортировали данные с field_xx именами (field_73, field_74), в то время как MacroProperty1 и MacroProperty3 использовали реальные имена полей.

**Решение:** Унификация экспорта данных для всех компонентов - все экспортные таблицы теперь используют реальные имена полей с field_id в комментариях столбцов.

**Преимущества:**
1. **Удобство тестирования** - понятные имена полей
2. **Консистентность** - единообразный подход во всех экспортерах
3. **Читаемость** - проще анализировать данные в ClickHouse
4. **Совместимость** - field_id сохранены в комментариях столбцов

### ❌ **ОТСУТСТВУЮЩИЕ КОМПОНЕНТЫ:**

#### MacroProperty2 (LoggingLayer Planes)
Результат симуляции FLAME GPU (логирование состояний планеров). Для совместимости с BI и прямых join имена полей, скопированных из MP1/MP3/MP4/MP5, сохраняются без переименования. Новые/производные метрики помечены ниже.

**Структура выходного слоя:**
- `dates` — дата симуляции (из MP4)
- `aircraft_number` — номер ВС планера (из MP3)
- `ac_type_mask` — тип планера (маска) (из MP3)
- `status_id` — статус планера (из MP3)
- `daily_flight` — ежедневный налёт (из MP5)
- `ops_counter_mi8` — целевой объём эксплуатации на D (из MP4)
- `ops_counter_mi17` — целевой объём эксплуатации на D (из MP4)
- `ops_current_mi8` / `ops_current_mi17` — фактическая укомплектованность на D (новые)
- `partout_trigger` — дата триггера разборки (новое)
- `assembly_trigger` — дата триггера сборки (новое)
- `active_trigger` — дата активации (новое)
- `aircraft_age_years` — возраст планера (новое)
- `mfg_date` — дата производства (из MP3)
- `simulation_metadata` — метаданные симуляции (версия, день D, параметры) (новое)

## ✅ **ИТОГОВАЯ СВОДКА FLAME GPU КОМПОНЕНТОВ (29-07-2025)**

### 🎯 **ВСЕ КОМПОНЕНТЫ ЗАГРУЗКИ ГОТОВЫ:**

| **Компонент** | **Поля** | **Записей** | **Время загрузки** | **Статус** | **Тип данных** |
|---------------|-----------|-------------|-------------------|------------|----------------|
| **MacroProperty1** | 20 | 7,113 | ~2.5с | ✅ ГОТОВ | Environment Property Arrays |
| **MacroProperty3** | 14 | 7,113 | ~2.5с | ✅ ГОТОВ | Environment Property Arrays |
| **MacroProperty4** | 8 | 4,000 | ~2.5с | ✅ ГОТОВ | Environment Property Arrays |
| **MacroProperty5** | 4 | 1,116,000 | ~2.5с | ✅ ГОТОВ | Environment Property Arrays |
| **Property** | 2 | 1 (скаляр) | 0.01с | ✅ ГОТОВ | Скалярные Environment Properties |

### 📊 **ОБЩАЯ СТАТИСТИКА:**
- **Общее количество полей:** 48 полей
- **Общее количество записей:** 1,134,227 записей
- **Общее время загрузки:** ~10 секунд
- **Покрытие Transform:** 83% (5 из 6 компонентов)

### 🔧 **СОЗДАННЫЕ КОМПОНЕНТЫ:**
- **15 loader скриптов** (по 3 на каждый MacroProperty/Property)
- **15 exporter скриптов** для визуального контроля (унифицированы 29-07-2025)
- **15 validator скриптов** для проверки целостности данных
- **Все field_id маппинги** готовы и протестированы
- **Автоматическая версионность** для всех компонентов
- **Унифицированный экспорт** - все таблицы используют реальные имена полей

### 📋 **СЛЕДУЮЩИЕ ШАГИ:**

1. **Создание Transform координатора** - объединение всех компонентов в единый цикл
2. **Реализация MacroProperty2** - результат симуляции планеров (LoggingLayer)
3. **Интеграция с основной симуляцией** FLAME GPU
4. **Микросервисная архитектура** - реализация persistent GPU service

### 🎯 **ГОТОВНОСТЬ К PRODUCTION:**
**Transform этап готов на 83%** - все данные успешно загружаются в FLAME GPU, остается интеграция с симуляционным движком и создание выходного слоя MacroProperty2.

## 📚 **Методы доступа к СУБД**

### ClickHouse Connection
```python
from code.utils.config_loader import get_clickhouse_client
client = get_clickhouse_client()
```

### Чтение данных
```python
# Загрузка таблицы
result = client.execute("SELECT * FROM table_name WHERE condition")

# Получение схемы
schema = client.execute("DESCRIBE table_name")
```

### Запись данных
```python
# Создание таблицы
client.execute("CREATE TABLE name (columns) ENGINE = MergeTree() ORDER BY key")

# Вставка данных
client.execute("INSERT INTO table (columns) VALUES", data_list)
```

## РЕЗУЛЬТАТ ЭТАПА 1

✅ **MacroProperty1** (md_components) - загружен и проверен  
✅ **MacroProperty3** (heli_pandas) - загружен и проверен  
✅ **Валидация данных** - roundtrip тесты пройдены  
✅ **JSON конфигурации RTC** - созданы для раздельной архитектуры
✅ **Раздельная архитектура** - 15 JSON файлов для независимых симуляций

**Статус:** ЭТАП 1 ЗАВЕРШЕН ✅  
**Следующий этап:** Инициализация агентов и две параллельные симуляции

## АРХИТЕКТУРА РАЗДЕЛЬНЫХ СИМУЛЯЦИЙ (обновлено 31-07-2025)

### Принцип раздельности:
```
Extract → MacroProperty1-5 (общие данные)
         ↓
Transform → [MI8_Simulation] + [MI17_Simulation] (параллельно)
         ↓
Load → MacroProperty2 (объединенные результаты)
```

### МИ-8Т Симуляция (7 RTC функций):
- `fn_inactive_mi8`, `fn_ops_mi8`, `fn_stock_mi8`
- `fn_repair_mi8`, `fn_reserve_mi8`, `fn_store_mi8`
- `fn_balance_mi8` (БЕЗ spawn - не выпускаются)

### МИ-17 Симуляция (8 RTC функций):
- `fn_inactive_mi17`, `fn_ops_mi17`, `fn_stock_mi17`
- `fn_repair_mi17`, `fn_reserve_mi17`, `fn_store_mi17` 
- `fn_balance_mi17`, `fn_spawn_mi17` (С spawn - новые поставки)

### Преимущества раздельности:
- Параллельные CUDA потоки для каждого типа
- Независимые Environment Property Arrays
- Отсутствие фильтрации ac_type_mask в runtime
- Простая отладка и тестирование 

## Архитектурные намерения для RTC (10-08-2025)

- Термины статусов (фиксированные):
  - 1 — Неактивно; 2 — Эксплуатация; 3 — Исправен; 4 — Ремонт; 5 — Резерв; 6 — Хранение.

- **Слои RTC и host-балансировка** (на каждый день D): repair → ops_check → host_balance → main → change → pass

- Инварианты (без предсказателей статусов):
  - Начисление SNE/PPR выполняется ровно один раз в сутки и только для `status_id=2`.
  - Решения о переходах принимаются и применяются в конце суток D на основе окон LL/OH и квоты D+1.

- Окружение (env):
  - Скаляры: `trigger_pr_final_mi8`, `trigger_pr_final_mi17`, `current_day_index`, `current_day_ordinal`.
  - Массивы (index=agent.idx): `daily_today`, `daily_next`, `partout_time_arr`, `assembly_time_arr`.

- Правила слоёв (без status_change/status_next):
  - rtc_repair:
    - Если `status_id=4`: `repair_days += 1`; если `repair_days >= repair_time` — на конец суток D устанавливается `status_id=5` и выполняются `ppr=0`, `repair_days=0`.
  - rtc_ops_check (только `status_id=2`):
    - Обозначим `dt=daily(D)`, `dn=daily(D+1)`.
    - Окна пригодности к D+1 рассчитываются по счётчикам после начисления (см. раздел «FLAME GPU RTC: атомарная квота без контроллера»).
    - Пригодные к эксплуатации резервируют квоту D+1 атомарным декрементом; при отказе квоты целевое состояние — 3. Применение переходов — в конце суток D.
  - host_balance (по `group_by`=1/2):
    - `trigger = trigger_program_{mi8,mi17}(D)` — дневная квота миграций из MP4;
    - `trigger<0`: top |trigger| из эксплуатации → целевое 3 по приоритету `ppr DESC, sne DESC, mfg_date ASC`.
    - `trigger>0`: Phase1 `3→2`, Phase2 `5→2`, Phase3 `1→2` при `(D−version_date) ≥ repair_time`.
  - rtc_main: если `status_id=2`: `sne += dt`, `ppr += dt`.
  - rtc_change (сайд‑эффекты на конец суток):
    - При входе в ремонт (целевое 4): `repair_days=1`; `partout_trigger = D + partout_time`; `assembly_trigger = D + (repair_time − assembly_time)`.
    - При завершении ремонта (4→5): `ppr=0`, `repair_days=0`.
    - При 1→2: `active_trigger = D − repair_time`; `assembly_trigger = D − assembly_time`.
  - rtc_pass_through: контроль инвариантов суток.

## Модульная архитектура FLAME GPU (12-09-2025)

### Рефакторинг в модульную структуру

**Проблема старой архитектуры:**
- Монолитный `sim_master.py` (1983+ строк)
- 30 RTC функций в одном файле
- Дублирование quota циклов (4 копии)
- Сложная отладка и тестирование

**Новая архитектура:**
- **Модульная структура**: `code/sim/` + `code/rtc/`
- **28 RTC функций** в отдельных файлах
- **Параметризованные quota функции** (вместо 4 копий)
- **4 профиля конфигурации** (minimal → production)
- **Инструменты отладки** без зависания NVRTC

### Созданные компоненты

**Модуль симуляции (code/sim/):**
- `env_setup.py` - подготовка Environment из ClickHouse
- `pipeline_config.py` - RTC_PIPELINE + профили
- `sim_builder.py` - полный сборщик модели
- `simple_builder.py` - упрощенный сборщик для отладки
- `runners.py` - SmokeRunner + ProductionRunner

**RTC функции (code/rtc/):**
- Каждая из 28 функций в отдельном файле
- BaseRTC класс для унификации
- Параметризация для quota функций статусов 1,2,3,5

**Инструменты отладки:**
- `rtc_code_validator.py` - валидация без NVRTC
- `rtc_syntax_checker.py` - проверка синтаксиса
- `sim_rtc_step_by_step.py` - пошаговое добавление
- `test_minimal_env.py` - базовый Environment

### Статус реализации

**✅ Завершено (12-09-2025):**
- Модульная архитектура создана
- RTC функции модуляризованы
- Инструменты отладки работают
- Документация обновлена

**🔄 Следующий этап:**
- Пошаговый перенос RTC ядер из бэкапов
- Тестирование на минимальных параметрах
- Интеграция в production пайплайн 

## Заглушки и переход к реальным тестам (10-08-2025)

- **Заглушки (skeleton):**
  - `code/flame_gpu_helicopter_model.py` — StepFunction/HostFunction без бизнес-логики; цель — зафиксировать порядок слоёв и интерфейсы (`group_by`, `status_change`).
  - `code/pre_simulation_status_change.py` — формирует SQL-план (ops_check + balance шаблоны) в dry-run по умолчанию, без изменений данных.
  - `code/utils/mp3_group_by_filler.py` — заполняет `group_by`, сухой прогон по умолчанию.
- **Хардкод (документирован):**
  - Фильтры по `group_by` (1=МИ‑8Т, 2=МИ‑17) вместо `ac_type_mask`.
  - Правила разметки ops_check: LL/OH/BR по `daily_hours(D|D+1)` — только метки `status_change` (sne/ppr не меняем).
  - В balance Phase3 (1→2) допускается только при `(D - version_date) >= repair_time(partno_comp)`.
  - Сайд-эффект `repair_days=1` перенесён из предсимуляции в `rtc_change` (при `status_change=4`).
- **Переход к реальным тестам:**
  - Включить `--apply` для утилит предсимуляции и выполнить SQL против тестовой ClickHouse.
  - Реализовать логику RTC в `flame_gpu_helicopter_model.py` по описанию раздела «Архитектурные намерения для RTC».
  - Прогнать 1–3 суток симуляции с логированием инвариантов (начисление sne/ppr ровно 1 раз; отсутствие необработанных `status_id=2/4` к `rtc_pass_through`).
  - Сверка с валидацией MacroProperty3/4/5 и отчётами. 

## FLAME GPU RTC: атомарная квота без контроллера (21-08-2025)

### Единый макро‑счётчик квоты на D+1
- Инициализация ровно один раз в начале суток D: квота эксплуатации на D+1 устанавливается значением `ops_counter_mi8(D+1)`.
- Один и тот же счётчик используется всеми слоями в пределах дня; изменяется только атомарными убываниями.
- В квоту «входят» только статусы, претендующие на эксплуатацию на D+1: слои 3 (2), 4 (3), 5 (5), 6 (1) — именно в этом порядке; статусы 4/6 квоту не трогают.

### Порядок слоёв на сутки D (group_by=1)
1) Ремонт (status_id=4):
   - `repair_days := repair_days + 1`.
   - Если `repair_days >= repair_time` → в этот же цикл принять 4→5: `status_id:=5; ppr:=0; repair_days:=0`.
   - Логирование: `daily_flight=0`; перенос остальных полей как есть (для оставшихся в 4 — уже с увеличенным `repair_days`).
2) Хранение (status_id=6):
   - Без изменений счётчиков; в баланс/квоту не участвует. Логирование: перенос полей, `daily_flight=0`.
3) Эксплуатация (status_id=2):
   - Начисление за D: пусть `hD=daily_hours(D)`, `hN=daily_hours(D+1)`.
     - `sne' := sne + hD`, `ppr' := ppr + hD`, `daily_flight := hD`.
   - Проверка готовности к D+1 (окна ресурсов на D+1):
     - `rem_ll := max(ll − sne', 0)`; если `0 ≤ rem_ll < hN` → на D+1 будет 6.
     - `rem_oh := max(oh − ppr', 0)`; если `0 ≤ rem_oh < hN` → если `sne' + hN < br` → на D+1 будет 4, иначе 6.
   - Сайд‑эффекты при выборе 2→4 на D+1: `partout_trigger := current_date + 7; assembly_trigger := current_date + repair_time − 30; repair_days := 1`.
   - Если готов к D+1 и не попадает в окна LL/OH:
     - Пытается занять квоту D+1 атомарным убыванием; успех → на D+1 будет 2; иначе → 3.
4) Исправен (status_id=3):
   - Начислений нет (`daily_flight=0`). Пытается войти в 2 на D+1: атомарно уменьшить квоту D+1; успех → 2, иначе остаётся 3.
5) Резерв (status_id=5):
   - Начислений нет (`daily_flight=0`). Пытается войти в 2 на D+1: атомарно уменьшить квоту D+1; успех → 2, иначе остаётся 5.
6) Неактивно (status_id=1):
   - Начислений нет (`daily_flight=0`). Временное условие допуска на D+1:
     - `t_days := (current_date − version_date).days` (целые дни, совместимо с UInt16 в ClickHouse);
     - требуется `t_days ≥ repair_time` и наличие квоты D+1.
     - При успешном убывании квоты → на D+1 будет 2 и установить `active_trigger := current_date − repair_time; assembly_trigger := current_date + repair_time − 30`; иначе остаётся 1.

### Итоговый экспорт MP2 (единый, в конце дня D)
- Сразу после завершения всех слоёв выполняется один батч‑экспорт строк за D.
- `ops_current_mi8/mi17(D)` считаются отдельным проходом перед экспортом как количество агентов со `status_id=2` на конец D (резервы на D+1 не влияют на факт D).
- Вставляем: `dates=D, psn, partseqno_i, aircraft_number, group_by, ac_type_mask, status_id(конец D), daily_flight, ops_counter_mi8/mi17(D), ops_current_mi8/mi17(D), partout/assembly/active, aircraft_age_years(D), mfg_date, sne, ppr, repair_days, simulation_metadata`.

Примечание: предыдущая архитектура «контроллер на сообщениях» признана избыточной для квотирования и заменена атомарным подходом; контроллер/сообщения не используются.

## 🧩 Архитектура модулей симуляции (без дублирования MP) — 21-08-2025

### Модульная структура (код в `code/`)
- `sim_env_setup.py` — подготовка Environment: `current_date_ord`, `next_date_ord`, `remaining_ops_next_mi8`, массивы `daily_hours_today/next` по индексу агента.
- `sim_agent_factory.py` — создание агентов из MP3; перенос полей MP3 в агентные переменные (инициализация `mfg_date_ord`, `version_date_ord`).
- `sim_layers_mi8.py` — реализация слоёв суток D для `group_by=1` в порядке 4→6→2→3→5→1, атомарное расходование квоты.
- `sim_logging_mp2.py` — подсчёт `ops_current_mi8(D)` и единый батч‑экспорт MP2.
- `sim_runner.py` — дневной цикл: подготовка входов → слои → экспорт.

### Источники данных (только чтение)
- MP1: `repair_time`, `partout_time`, `assembly_time`, `restrictions_mask`.
- MP3: `status_id`, `sne`, `ppr`, `repair_days`, `mfg_date`, `version_date`, `version_id`, `aircraft_number`, `group_by`, `ll`, `oh`, `oh_threshold`, `ac_type_mask`.
- MP4: `ops_counter_mi8/mi17` по датам — квота D+1.
- MP5: `daily_hours` по `(aircraft_number, dates)` — `daily_today/next`.

### Новые Environment свойства (создаются только для симуляции)
- `current_date_ord` UInt16, `next_date_ord` UInt16.
- `remaining_ops_next_mi8` UInt16 — единая квота на D+1; инициализация ровно один раз в начале суток.
- `daily_hours_today[idx]` UInt32, `daily_hours_next[idx]` UInt32 — по `aircraft_number`.

### Агентные переменные (из MP3; изменяются слоями)
- `status_id` UInt8, `sne` UInt32, `ppr` UInt32, `repair_days` UInt16,
  `mfg_date_ord` UInt16, `version_date_ord` UInt16,
  `aircraft_number` UInt32, `group_by` UInt8, `ac_type_mask` UInt8,
  `ll` UInt32, `oh` UInt32, `oh_threshold` UInt32.
- Триггеры: `active_trigger_ord`, `assembly_trigger_ord`, `partout_trigger_ord` (все UInt16, устанавливаются по правилам слоёв).

### Правила недублирования
- Не копируем данные MP в отдельные таблицы; используем MP только как источник при старте дня.
- Новые поля создаём только для нужд симуляции (атомарная квота и суточные массивы), в `Environment`.

### Примечание по BR (24-08-2025)
- Порог Beyond Repair берётся из MP1 раздельно: `br_mi8`/`br_mi17`.
- Единицы измерения: минуты (округление до минут).
- Выбор порога при инициализации агентов: по `ac_type_mask` (бит 32 → `br_mi8`, бит 64 → `br_mi17`; при multihot допускается выбрать один фиксированный порядок).

### Примечание по MP3 экспортерам (25-08-2025)
- Экспорт MP3 в `flame_macroproperty3_export` выполняется с жёстким выбором getter по типу исходного поля (`DESCRIBE heli_pandas`).
- Это исключает «тихие» промахи каскадных getter'ов и гарантирует корректную выгрузку `group_by` (0..11) и других UInt8/UInt16/UInt32 полей.