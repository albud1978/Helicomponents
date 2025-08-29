# GPU модель: архитектура full‑GPU для планеров

**Дата создания:** 27-08-2025

## Цель
Перенести логику эксплуатации и ремонта планеров на GPU, минимизировать host‑костыли, обеспечить атомарное квотирование по программе (MP4) и суточные налёты (MP5) через Environment/MacroProperty.

## Источники данных и окружение (в памяти GPU)

| Источник | Тип в GPU | Ключевые поля | Назначение/использование | Мутируемость |
| --- | --- | --- | --- | --- |
| MP1 (нормативы) | Property Arrays (RO) | partseqno_i, br_mi8, br_mi17, repair_time, partout_time, assembly_time | Ежедневные расчёты порогов и триггеров (читаем на лету в RTC) | Только чтение |
| MP3 (агенты) | Property Arrays (RO) | psn, aircraft_number, ac_type_mask, status_id, sne, ppr, repair_days, ll, oh, mfg_date_days | Источник инициализации агентов и при необходимости прямого чтения RTC | Только чтение |
| MP4 (квоты) | Property Arrays (RO) | dates, ops_counter_mi8, ops_counter_mi17 | Источник значений квот; на основе MP4 создаётся MP6 | Только чтение |
| MP5 (налёт) | Property Arrays (RO) | dates, aircraft_number, daily_hours | Прямое индексирование налёта: base = day * frames_total + idx → dt, dn | Только чтение |
| MP6 (квоты по датам) | MacroProperty UInt32 с размерами (days_total) | mp6_quota_mi8[days], mp6_quota_mi17[days] | Атомарные счётчики квот по типам; на каждый день свой элемент; в RTC: old = atomicSub(mp6_quota_type[D+1], 1) и проверка old>0 | Атомарные (RW) |
| Env: version_date | Property UInt16 | — | Начальная дата симуляции D0 (из СУБД); current_date = version_date + day | RO |

## Переменные
- Агент (минимально необходимый состав):
  - Идентификация: `idx` (плотный индекс агента, привязан к `aircraft_number`; используется для логирования (MP2/MP5, MultiBOM) и для прямого доступа к MP5), `psn`
  - Привязка к планеру: `aircraft_number` (parent_id для MultiBOM), `ac_type_mask` (ежедневно участвует в выборе типа квоты: MI‑8/MI‑17), `ll`, `oh`, (ежедневно участвуют в расчете остатка ресурса)
  - Статус и счётчики: `status_id` (state, меняется не более 1 раза за сутки), `sne`, `ppr`, `repair_days`, `ops_ticket`, `active_trigger`, `partout_trigger`, `assembly_trigger`

- Критерии выбора agent variables
  1) Переменные, которые изменяются внутри агента хотя бы в одном слое (например, `status_id`, `sne`, `ppr`, `repair_days`, триггеры, `ops_ticket`).
  2) Константы, которые участвуют в вычислениях ежедневно хотя бы в одном слое (например, `ll`, `oh`).
  Остальные константы/справочники читаются на лету из MP‑массивов (MP1/MP4/MP5) без хранения в агенте.

### Назначение переменных агента (краткая таблица)

| Переменная | Тип | Источник | Назначение |
| --- | --- | --- | --- |
| `idx` | UInt32 | Инициализация | Плотный индекс агента (по `aircraft_number`); используется для логирования (MP2/MP5, MultiBOM) и прямого доступа к MP5 (`base = day * frames_total + idx`) |
| `psn` | UInt32 | MP3 | Серийный номер агрегата (планера) |
| `aircraft_number` | UInt32 | MP3 | Бортовой номер планера; parent_id для MultiBOM; связка с MP5 |
| `status_id` | UInt32 | MP3/RTC | Текущий статус: 1,2,3,4,5,6; применяется коммитом не более 1 раза за сутки |
| `sne` | UInt32 | MP3/RTC | Наработка (сумма налёта) — инкремент в статусе 2 |
| `ppr` | UInt32 | MP3/RTC | Параллельный счётчик ресурса — инкремент в статусе 2 |
| `ll` | UInt32 | MP3 | Порог по SNE (ежедневно используется в статусе 2) |
| `oh` | UInt32 | MP3 | Порог по PPR (ежедневно используется в статусе 2) |
| `repair_days` | UInt32 | MP3/RTC | Сутки в ремонте (изменяется ежедневно в статусе 4) |
| `ops_ticket` | UInt32 | RTC | Билет допуска на эксплуатацию следующего дня (0/1) |
| `active_trigger` | UInt32 | RTC | Дата‑триггер активации (0 либо дни от 1970‑01‑01); изменяется при входе в эксплуатацию |
| `partout_trigger` | UInt32 | RTC | Дата предполагаемого снятия (0 либо дни от 1970‑01‑01); задаётся при выборе 2→4 |
| `assembly_trigger` | UInt32 | RTC | Дата сборки (0 либо дни от 1970‑01‑01); задаётся при 2→4 или выходе из ремонта |

В `rtc_status_2` значения `dt` (за D) и `dn` (за D+1) берутся напрямую из MP5 по линейной индексации: `base = day * frames_total + idx`, `dt = MP5[base]`, `dn = MP5[base + frames_total]` (MP5 имеет паддинг DAYS+1). Константы `ll`, `oh`, `br`, `repair_time`, а также `mfg_date` не храним в агенте: читаем из окружения при необходимости.

### Список переменных агента и источники заполнения (init из MP3)

| Поле агента | Источник | Как заполняется |
| --- | --- | --- |
| `idx` | Производный (GPU) | Плотный индекс борта по `aircraft_number` при инициализации |
| `psn` | MP3 | Из столбца `psn` |
| `aircraft_number` | MP3 | Из столбца `aircraft_number` |
| `ac_type_mask` | MP3 | Из столбца `ac_type_mask` (маска типа ВС) |
| `status_id` | MP3 | Из столбца `status_id` (начальное состояние) |
| `sne` | MP3 | Из столбца `sne` |
| `ppr` | MP3 | Из столбца `ppr` |
| `repair_days` | MP3 | Из столбца `repair_days` |
| `ops_ticket` | Константа | 0 при инициализации |
| `ll` | MP3 | Из столбца `ll` (по `psn`) |
| `oh` | MP3 | Из столбца `oh` (по `psn`) |
| `active_trigger` | Константа | 0 (если нет известной даты активации) |
| `partout_trigger` | Константа | 0 (будет выставлено при 2→4) |
| `assembly_trigger` | Константа | 0 (будет выставлено при 2→4 или выходе из ремонта) |

## Порядок слоёв (сутки)

0) Загрузка (host)
   - Загрузка MP1/MP4/MP5 из ClickHouse и размещение в Env (Environment Property Arrays)
   - Подготовка индексов (`frames_total`, сопоставления `aircraft_number → idx`)

1) Инициализация модели (build, без RTC)
   - Создание и конфигурация модели: объявление агентных переменных, Env/MacroProperty, создание слоёв
   - Создание популяции агентов из MP3 на GPU (см. «Список переменных и источники заполнения»)
   - Сброс `ops_ticket:=0` (иниц. шаг «init_quota», без JIT‑RTC). Квоты берутся напрямую из MP6 по индексу дня, копий не требуется

2) L1: объединённый слой `status_id ∈ {6,4,2}`
   - `rtc_status_6` (Хранение): пасс‑тру, `daily_flight=0`
   - `rtc_status_4` (Ремонт): `repair_days += 1`; при `repair_days >= repair_time` → `status=5`, `ppr=0`, `repair_days=0`
   - `rtc_status_2` (Эксплуатация):
     - Прямое чтение MP5: вычисляет `base = day * frames_total + idx`; берёт `dt = MP5[base]`, `dn = MP5[base + frames_total]` (где `day` — счётчик суток внутри RTC)
     - Начисление: `sne += dt`, `ppr += dt`
     - Проверки на D+1: СНАЧАЛА LL/OH с учётом `dn` и ремонтопригодности (`sne+dn < br`), затем квота
       - LL → 6; OH → (4 если ремонтопригоден, иначе 6)
     - Если не LL/OH → попытка занять квоту D+1 своей группы: `atomicDec(remaining_ops_next_group)`; успех → остаться 2, иначе пойдёт в 3

3) L2: `rtc_status_3` (Исправен)
   - Быстрый чек: если атомик 0 → пасс‑тру 3
   - Иначе попытка 3→2 через `atomicDec`; успех → 2, иначе 3

4) L3: `rtc_status_5` (Резерв)
   - Аналогично статусу 3: предварительный чек атомика; попытка 5→2; при нуле — пасс‑тру 5

5) L4: `rtc_status_1` (Неактивно)
   - Быстрый чек атомика; затем условия допуска (по правилам проекта)
   - При выполнении условий и наличии квоты — 1→2 через `atomicDec`, иначе пасс‑тру 1

6) Эпилог (опционально в проде, обязателен в отладке)
   - `rtc_log_day (MP2)`: агрегированная запись показателей суток в MacroProperty2 (кол‑ва по статусам, ops_current, quota_claimed, завершённые ремонты)
   - Экспортом батч этого блока в ClickHouse занимается host в конце суток

7) `increment_day`
   - Один агент (idx==0) выполняет `current_day++` на MacroProperty, готовя следующий шаг

### Таблица слоёв

Примечание к обозначениям: `dt` — суточный налёт за день D из MP5 (`daily_hours`), `dn` — суточный налёт за день D+1 из MP5.

Счётчик дня: не хранить и не инкрементировать вручную. В RTC использовать номер шага симуляции (`day = step`); текущая дата рассчитывается как `current_date = version_date + day`.

| Шаг (Слой) | RTC | Исходные данные (MP/поле) | Формулы/логика | Итог: status_id и сайд‑эффекты |
| --- | --- | --- | --- | --- |
| 0 (host) | loaders | MP1/MP4/MP5 | Загрузка MP в Env (Property Arrays), подготовка индексов | — |
| 1 (init) | agent_init + init_quota | MP3 (агенты); MP6 (квоты) | Создание популяции; `ops_ticket := 0`; квоты берутся из MP6[D+1] при обращении | Статус из MP3 |
| 2 (L1, status_id=6) | `rtc_status_6` | — | Пасс‑тру; без начислений | Остаётся 6; без сайд‑эффектов |
| 2 (L1, status_id=4) | `rtc_status_4` | MP1: `repair_time` | `repair_days := repair_days + 1`; если `repair_days >= repair_time` → `status_id := 5; ppr := 0; repair_days := 0` | Либо остаётся 4, либо 4→5 |
| 2 (L1, status_id=2) | `rtc_status_2` | MP5: `daily_hours` → `dt, dn` по линейной индексации; MP3/MP1: `ll, oh, br, partout_time, assembly_time, repair_time`; Env: MP6 (`mp6_quota_mi8/mi17`) | `sne += dt; ppr += dt`; LL/OH на D+1; при `dt>0` — попытка квоты D+1 по типу через `old = atomicSub(mp6_quota_type[D+1], 1)` | Остаётся 2 или 2→4/6/3; триггеры при 2→4 |
| 3 (L2, status_id=3) | `rtc_status_3` | Env: MP6 (`mp6_quota_mi8/mi17`) | По `ac_type_mask` выбрать тип; `old = atomicSub(mp6_quota_type[D+1], 1)`; если `old>0` → 3→2 | Без изменений триггеров |
| 4 (L3, status_id=5) | `rtc_status_5` | Env: MP6 (`mp6_quota_mi8/mi17`) | Аналогично статусу 3 | Без изменений триггеров |
| 5 (L4, status_id=1) | `rtc_status_1` | Env: MP6 (`mp6_quota_mi8/mi17`); MP1: `repair_time`, `assembly_time` | Формула допуска и 1→2 при квоте; `active_trigger := D − repair_time`, `assembly_trigger := D − assembly_time` | 1→2 |
| 6 | `rtc_log_day` | Агент/Env/MP: данные суток | Запись в SoA‑колонки MacroProperty2 по индексу `row = day * frames_total + idx` | Агент без изменений |
| 7 | `increment_day` | Env: `current_day` | `current_day := current_day + 1` | Агент без изменений |
| 8 (host) | export_mp2 | MacroProperty2/популяция | Одно батч‑вставка MP2 в ClickHouse | — |

## Жизненный цикл исполнения
- Build (host) → Load (host) → Init (host/device) → Run (device) → Export (host)

## Правила атомика (MP6)
- Два массива квот: `mp6_quota_mi8[days]`, `mp6_quota_mi17[days]` (UInt32)
- Порядок потребления квоты за день D+1: L1 (остаются в 2) → L2 (3→2) → L3 (5→2) → L4 (1→2)
- При достижении 0 последующие попытки квоты в текущие сутки становятся пасс‑тру (как статус 6), кроме `status_id=2` (уходит в 3)

## Инварианты и валидация
- Квоты: `sum(quota_claimed_mi8, D) ≤ mp6_quota_mi8[D+1]` и аналогично для Ми‑17
- Ограничение смены статуса: ≤ 1 раза/сутки
- Монотонность: `sne/ppr` не убывают; `repair_days` сбрасывается при выходе из ремонта
- `dt==0` → `ops_ticket=0`; `dn` используется только для «завтрашних» LL/OH

## Типы и ограничения
 - daily_hours (MP5) — UInt16 (минуты); паддинг DAYS+1
 - sne/ppr (агент) — UInt32 (сумматоры)
 - mp6_quota_* (MP6) — UInt32 (атомарные счётчики)
 - frames_total — UInt16 (≤ 300)
 - Без Float64; даты как «дни от 1970‑01‑01» (UInt16/UInt32 при необходимости)

## Этапы реализации (пошагово, на реальных данных)

0) Загрузка и чтение MP (RO) на GPU
   - Настроить loaders MP1/MP4/MP5 → Property Arrays; завести `version_date` (Property UInt16).
   - В коде рассчитать `frames_total`, построить `aircraft_number → idx`.
   - Смоук‑тест чтения: выборочные значения из MP1/4/5 доступны на устройстве.

1) Инициализация агентов и базовый экспорт MP2
   - Создать популяцию из MP3; заполнить минимальный набор agent variables.
   - Выполнить «нулевой» проход без RTC и выгрузить текущее состояние в MP2 (D0) как базовую линию.

2) Full‑GPU одного слоя: `rtc_status_4`
   - Реализовать и запустить 1 сутки (D1) только с `rtc_status_4`; логирование MP2 за D1.
   - Расширить до 7 суток; сверка с CPU‑референсом sim_master (для статуса 4).

3) Добавить `rtc_status_6` (хранение) в L1
   - Повторить прогон 1 сутки → 7 суток; MP2 должен совпасть по распределению stat=6.

4) Добавить `rtc_status_2` (эксплуатация)
   - Подключить чтение MP5 (dt/dn) и MP6 (квоты D+1) с `ac_type_mask`.
   - Прогон 1 сутки с MP2‑логом; затем 7 суток; валидировать LL/OH и квоты.

5) Добавить последовательно статусы `3`, `5`, `1`
   - Для каждого: 1 сутки → 7 суток; валидация логики квот (MP6) и итоговых переходов.

6) Инварианты и commit‑семантика
   - Подтвердить порядок потребления квоты (L1→L2→L3→L4) и исключение для `status_id=2` при нуле квоты (уходит в 3).
   - Зафиксировать однократность изменения `status_id` за сутки и корректность триггеров.

7) Расширение горизонта
   - Увеличить период: 7 → 30 → 365 → 1825 → 3650 суток, с контрольными метриками времени и памяти.

8) Итоговый экспорт
   - Убедиться в корректной батч‑вставке MP2 после всех циклов; при необходимости включить MacroProperty2 для агрегатов.

## Таблицы MP и их использование на GPU

### MP1 — свойства по партийным номерам (normatives)
- Источник: Extract → ClickHouse → Loader MP1
- Основные поля: `partseqno_i`, `br_mi8`, `br_mi17`, `repair_time`, `partout_time`, `assembly_time`
- Использование (без хранения в агенте):
  - В `rtc_status_4` читаем `repair_time` на лету для проверки завершения ремонта
  - В `rtc_status_2`/эпилоге читаем `partout_time`/`assembly_time` для расчёта триггеров
  - В `rtc_status_2` получаем `br` как `br_mi8/br_mi17` по `ac_type_mask`

### MP2 — результат симуляции по дням (GPU logger, SoA)
- Источник: Симуляция → MacroProperty2 (SoA) → Exporter (батч)
- Макрополя (колонки, SoA): `mp2_dates[]`, `mp2_psn[]`, `mp2_aircraft[]`, `mp2_status[]`, `mp2_daily_flight[]`, `mp2_ops_counter_mi8[]`, `mp2_ops_counter_mi17[]`, `mp2_ops_current_mi8[]`, `mp2_ops_current_mi17[]`, `mp2_quota_claimed_mi8[]`, `mp2_quota_claimed_mi17[]`, `mp2_sne[]`, `mp2_ppr[]`, `mp2_repair_days[]`, `mp2_active_trigger[]`, `mp2_partout_trigger[]`, `mp2_assembly_trigger[]`, `mp2_mfg_date[]`, `mp2_aircraft_age_years[]`
- Индексация записи: `row = day * frames_total + idx`
- Использование:
  - Логирование: на шаге `rtc_log_day` заполняются соответствующие элементы по текущему `row`
  - Экспорт: после цикла экспортёр читает колонки MacroProperty2 и вставляет батч в ClickHouse

### MP4 — плановая укомплектованность/квоты (Property Arrays, RO)
- Источник: Extract → ClickHouse → Loader MP4
- Основные поля: `dates`, `ops_counter_mi8`, `ops_counter_mi17`
- Использование:
  - Неизменяемый источник квот; значения копируются в MP6 при загрузке (по датам)

### MP5 — налёт по бортам (Property Arrays, RO)
- Источник: Extract → ClickHouse → Loader MP5 (отсортировано по `dates, aircraft_number`)
- Основные поля: `dates`, `aircraft_number`, `daily_hours`
- Использование:
  - В `rtc_status_2`: прямое чтение `dt`/`dn` по индексу `base = day * frames_total + idx`
  - Паддинг: массив должен содержать дополнительный день вперёд (DAYS+1), чтобы индекс `base + frames_total` (для `dn` на D+1) всегда был валиден

### MP6 — атомарные квоты по датам (derived from MP4, MacroProperty Arrays)
- Источник: создаётся при загрузке из MP4
- Поля (в виде MacroProperty Arrays):
  - `mp6_quota_mi8[]`: квота для MI‑8 на каждую дату (UInt32, атомарная)
  - `mp6_quota_mi17[]`: квота для MI‑17 на каждую дату (UInt32, атомарная)
- Использование:
  - В слоях квотирования обращаемся к элементу с индексом `D+1` и выполняем `atomicDec` по типу, выбранному из `ac_type_mask`

### MP3 — индивидуальные свойства (константы и идентификаторы)
- Источник: Extract → ClickHouse → Loader MP3
- Поля для чтения без хранения в агенте: `ll`, `oh`, `mfg_date` (по `psn`)
- Использование:
  - В `rtc_status_2` читаем `ll/oh` (константы) по `psn`
  - `mfg_date` используется только при формировании MP2 (отладка возраста), подтягивается по `psn` на этапе экспорта

## Замеры
- `t_host_fill_env`, `t_sim_step`, `t_export`
- Печать в stdout; при необходимости — запись в служебную таблицу

## Seatbelts и профилирование
- Режимы:
  - Dev: включить «seatbelts» (инварианты, дополнительную валидацию, SoA‑логгер MP2, расширенные логи).
  - Prod: выключить «seatbelts» (минимум проверок; без лишнего логирования), оставить только критические assert/guards.
- Nsight Systems:
  - Цель: подтвердить, что основное время уходит в `sim.step()` (GPU), а не в host‑части.
  - Действия: собрать профиль (`nsys profile …`), убедиться, что на таймлайне доминируют GPU‑kernels слоёв RTC; host‑подготовка и экспорт не доминируют.
  - Проверки: отсутствие частых `get/setPopulationData` между шагами; пакетный экспорт MP2 после цикла.

## Рекомендации по NVRTC/RTC (компиляция и исполнение)
- Избегать «if‑лесов»: состояние агента должно фильтровать исполнение.
  - В каждой RTC‑функции начинать с быстрого предиката, например: `if (status_id != 2u) return ALIVE;`.
  - Разделять логику по слоям/RTC по статусам (L1 для 6/4/2, далее 3/5/1), вместо единой большой функции.
- Использовать ранние выходы и минимальные ветвления; где возможно — предвычисленные флаги (например, `dt>0`, `rem_ll<dn`, `rem_oh<dn`).
- Для выбора по типу планера применять маску `ac_type_mask` с простыми проверками битов; минимизировать вложенность.
- Квоты: единый атомарный вызов `old = atomicSub(mp6_quota_type[day+1], 1)` с проверкой `old>0`, без дополнительных ветвлений.
- Декомпозировать сложные расчёты на короткие RTC, связанных слоёв, чтобы ускорить NVRTC/JIT и улучшить читабельность.

## Мини‑паттерны RTC (эталоны для реализации)

- Чтение MP5 и начисление (rtc_status_2):
```cpp
const uint32_t day = FLAMEGPU->getStepCounter();
const uint16_t N   = FLAMEGPU->environment.getProperty<uint16_t>("frames_total");
const uint16_t i   = FLAMEGPU->getVariable<uint16_t>("idx");

const uint32_t linT = static_cast<uint32_t>(day) * N + i;               // D
const uint32_t linN = static_cast<uint32_t>(day + 1u) * N + i;          // D+1 (MP5 паддинг DAYS+1)

const uint16_t dt = FLAMEGPU->environment.getProperty<uint16_t>("mp5_daily_hours", linT);
const uint16_t dn = FLAMEGPU->environment.getProperty<uint16_t>("mp5_daily_hours", linN);

if (dt) {
  FLAMEGPU->setVariable<uint32_t>("sne", FLAMEGPU->getVariable<uint32_t>("sne") + dt);
  FLAMEGPU->setVariable<uint32_t>("ppr", FLAMEGPU->getVariable<uint32_t>("ppr") + dt);
}
```

- Пороговые проверки LL/OH на завтра (rtc_status_2):
```cpp
const uint32_t ll  = FLAMEGPU->getVariable<uint32_t>("ll");
const uint32_t oh  = FLAMEGPU->getVariable<uint32_t>("oh");
const uint32_t br  = /* получить br по ac_type_mask: br_mi8/br_mi17 из MP1 */ 0u; // читается из MP1 при необходимости
const uint32_t sne = FLAMEGPU->getVariable<uint32_t>("sne");
const uint32_t ppr = FLAMEGPU->getVariable<uint32_t>("ppr");

bool to_6 = false, to_4 = false;
if (dn) {
  if (sne >= ll || (ll - sne) < dn) {
    to_6 = true;
  } else if (ppr >= oh || (oh - ppr) < dn) {
    to_4 = ((sne + dn) < br);
    if (!to_4) to_6 = true;
  }
}
```

- Квота на D+1 (аккуратный атомик):
```cpp
const bool is_mi8  = (FLAMEGPU->getVariable<uint32_t>("ac_type_mask") & 32u) != 0u;
const uint32_t dayp1 = FLAMEGPU->getStepCounter() + 1u;

uint32_t old = is_mi8
  ? FLAMEGPU->environment.getMacroProperty<uint32_t>("mp6_quota_mi8", dayp1).atomicSub(1)
  : FLAMEGPU->environment.getMacroProperty<uint32_t>("mp6_quota_mi17", dayp1).atomicSub(1);

if (old > 0u) {
  FLAMEGPU->setVariable<uint32_t>("ops_ticket", 1u); // остаётся 2
} else {
  FLAMEGPU->setVariable<uint32_t>("ops_ticket", 0u); // уйдёт в 3 по логике переходов
}
```

- Ремонт (rtc_status_4):
```cpp
uint32_t d = FLAMEGPU->getVariable<uint32_t>("repair_days") + 1u;
FLAMEGPU->setVariable<uint32_t>("repair_days", d);
const uint32_t repair_time = /* прочитать repair_time из MP1 при необходимости */ 0u;
if (d >= repair_time) {
  FLAMEGPU->setVariable<uint32_t>("status_id", 5u);
  FLAMEGPU->setVariable<uint32_t>("ppr", 0u);
  FLAMEGPU->setVariable<uint32_t>("repair_days", 0u);
}
```

- Индексация MP2 (SoA‑лог):
```cpp
const uint32_t day = FLAMEGPU->getStepCounter();
const uint16_t N   = FLAMEGPU->environment.getProperty<uint16_t>("frames_total");
const uint16_t i   = FLAMEGPU->getVariable<uint16_t>("idx");
const uint32_t row = static_cast<uint32_t>(day) * N + i;

FLAMEGPU->environment.getMacroProperty<uint16_t>("mp2_daily_flight", row).exchange(dt);
FLAMEGPU->environment.getMacroProperty<uint32_t>("mp2_status",       row).exchange(FLAMEGPU->getVariable<uint32_t>("status_id"));
// ... другие колонки MP2 по необходимости
```

## Примечания
- Валидация и бизнес‑экспорт вне объёма документа; документ описывает архитектуру GPU‑симуляции.
