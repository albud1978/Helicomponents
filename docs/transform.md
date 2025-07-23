# Transform Stage - Имитационная модель планеров
**Дата создания:** 23-07-2025  
**Последнее обновление:** 23-07-2025

## Архитектура Transform этапа

### MacroProperty структура (5 таблиц)
Flame GPU Environment содержит 5 основных таблиц в MacroProperty:

#### MacroProperty1: md_components
- **Источник:** таблица `md_components` из ClickHouse
- **Назначение:** параметры компонентов для расчетов
- **Ключевые поля:**
  - `partno_comp` (field_id: 1) - чертежный номер агрегата в AMOS
  - `ll_mi8` (field_id: 12), `ll_mi17` (field_id: 15) - назначенные ресурсы
  - `oh_mi8` (field_id: 13), `oh_mi17` (field_id: 16) - межремонтные ресурсы
  - `repair_time` (field_id: 17) - срок ремонта в днях
  - `partout_time` (field_id: 18) - срок разборки в днях
  - `assembly_time` (field_id: 19) - срок сборки в днях
  - `br` (field_id: 20) - лимит ремонтопригодности

#### MacroProperty2: LoggingLayer Planes (результат симуляции)
- **Источник:** результат работы агентов планеров
- **Назначение:** итоговый куб аналитики планеров
- **Основные поля:**
  - `dates` (field_id: 21) - массив дат симуляции
  - `aircraft_number` (field_id: 22) - номер ВС планера
  - `daily_flight` (field_id: 24) - суточный налет планера
  - `status_id` (field_id: 25) - статус планера
  - `partout_trigger` (field_id: 26), `assembly_trigger` (field_id: 27) - триггеры
  - `mfg_date` (field_id: 30) - дата производства
  - `active_trigger` (field_id: 31) - дата подъема из Неактивно

#### MacroProperty3: heli_pandas
- **Источник:** таблица `heli_pandas` из ClickHouse
- **Назначение:** начальные данные планеров
- **Ключевые поля:**
  - `partseqno_i` (field_id: 34) - чертежный номер цифровой AMOS
  - `psn` (field_id: 35) - заводской номер цифровой AMOS
  - `address_i` (field_id: 36) - собственник цифровой AMOS
  - `aircraft_number` (field_id: 39) - номер ВС цифровой

#### MacroProperty4: flight_program_ac
- **Источник:** таблица `flight_program_ac` из ClickHouse
- **Назначение:** программы операций и триггеры
- **Ключевые поля:**
  - `ops_counter_mi8`, `ops_counter_mi17` - счетчики операций
  - `new_counter_mi17` - новые поставки МИ-17
  - `trigger_program_mi8`, `trigger_program_mi17` - триггеры программ

#### MacroProperty5: flight_program_fl
- **Источник:** таблица `flight_program_fl` из ClickHouse
- **Назначение:** нормативные налеты планеров
- **Структура:** тензор 4000 дней × количество планеров

### Agent Variables для планеров
**Основные счетчики (динамические):**
- `sne` - наработка с начала эксплуатации
- `ppr` - наработка после ремонта
- `repair_days` - остаток дней ремонта
- `status_id` - статус как state

**Статические переменные:**
- `aircraft_number` - ID планера
- `ac_type_mask` - тип планера (добавляется обогащением)

## RTC Функции планеров

### RTC Step функции (по статусам)
Названия функций для каждого статуса:
- `fn_inactive_ac` (status_id: 1) - планеры в неактивном состоянии
- `fn_ops_ac` (status_id: 2) - планеры в эксплуатации
- `fn_stock_ac` (status_id: 3) - планеры исправные на складе
- `fn_repair_ac` (status_id: 4) - планеры в ремонте
- `fn_reserve_ac` (status_id: 5) - планеры в резерве
- `fn_store_ac` (status_id: 6) - планеры на хранении

### RTC Trigger функции
- `rtc_spawn_ac` - рождение новых планеров
- `rtc_balance_ac` - балансировка статусов планеров

## RTC Balance Architecture

### Глобальные триггеры
- **new_counter_mi17 > 0** - триггер новых поставок МИ-17
- Запускает последовательно: `rtc_spawn_ac` → `rtc_balance_ac`

### rtc_spawn_ac логика (ХАРДКОД)
**Назначение:** Рождение новых агентов-планеров по программе поставок

**Алгоритм:**
1. **Генерация serialno:**
   - Диапазон: 100000-150000
   - Проверка последнего номера среди агентов и в MacroProperty3
   - Генерация первого свободного порядкового номера

2. **Создание записи в MacroProperty3 (heli_pandas):**
   - `serialno` = сгенерированный номер
   - `sne` = 0, `ppr` = 0
   - `mfg_date` = текущая дата симуляции
   - `lease_restricted` = 1
   - `address_i` = 17094 (ХАРДКОД - владелец)
   - `partseqno_i` = 70482 (ХАРДКОД - тип планера МИ-17)
   - `ac_type_mask` = 64 (ХАРДКОД - битовая маска МИ-17)
   - `ll`, `oh` - из MacroProperty1 полей `ll_mi17`, `oh_mi17` для `partno_comp = partseqno_i`
   - `status_id` = 2 (эксплуатация)
   - `aircraft_number` = serialno

3. **Создание агента:** Стандартный набор Agent Variables для планеров

**ХАРДКОД КОНСТАНТЫ:**
- Диапазон номеров: 100000-150000 (резерв для новых МИ-17)
- address_i: 17094 (владелец для новых планеров)
- partseqno_i: 70482 (стандартный тип МИ-17)
- ac_type_mask: 64 (битовая маска МИ-17)
- lease_restricted: 1 (все новые - лизинговые)
- status_id: 2 (сразу в эксплуатацию)

### rtc_balance_ac логика
**Назначение:** Балансировка количества планеров в эксплуатации согласно программе

**Условие запуска:** trigger_program_mi8(mi17) ≠ 0

#### Дефицит планеров (trigger_program < 0):
**Последовательность поиска планеров:**
1. `status_id = 3` (исправные) → `status_id = 2` (эксплуатация)
2. `status_id = 5` (резерв) → `status_id = 2` (эксплуатация)
3. `status_id = 1` (неактивные) с учетом `ac_type_mask = 32(64)` и максимальной `mfg_date`:
   - **Условие готовности:** `(текущая_дата - version_date) >= repair_time`
   - **Если готов:** `status_id = 2`, `active_trigger = текущая_дата`
   - **Если не готов:** `status_id = 4`, `repair_days = repair_time - (текущая_дата - version_date)`
   - **Планирование:** `active_trigger = текущая_дата + repair_days`
   - **RTC отложенный:** запуск `rtc_balance` через `repair_days` шагов

#### Избыток планеров (trigger_program > 0):
- Поиск: `status_id = 2` с учетом `ac_type_mask = 32(64)`
- Действие: `status_id = 3` (на склад)

## Постпроцессинг LoggingLayer Planes

### Функции постпроцессинга
1. **Коррекция триггеров active_trigger:**
   - Проверка наличия `active_trigger` для всех `serialno`
   - Коррекция: `active_trigger - assembly_time = 1`

2. **Формирование partout_trigger и assembly_trigger:**
   - При переходе `status_id = 2` → `status_id = 4`:
   - `partout_trigger = current_date + partout_time`
   - `assembly_trigger = current_date + repair_time - assembly_time`

3. **Обогащение полей:**
   - `ac_type_mask` - из MacroProperty3 в конце цикла
   - `version_date`/`version_id` - из MacroProperty3 в конце цикла
   - `aircraft_age_years` - гибридный расчет из `mfg_date`

### Коррекция repair_days
- **В начале симуляции:** `repair_days` из MacroProperty3 (heli_pandas)
- **В процессе:** при `status_id = 4` выполняется `repair_days -= 1`

## Логика симуляции

### Один шаг = один день
- Индексация по `dates[]` массиву в MacroProperty
- Первая дата заполнена данными из `heli_pandas`
- Последовательность: Variables → Триггеры → Variables (с триггерами)

### Variables (без триггеров) - штатный режим
- Накопление `sne += daily_flight`
- Накопление `ppr += daily_flight`
- Уменьшение `repair_days -= 1` при `status_id = 4`

### Триггеры - условия смены статуса
- RTC balance триггеры
- Программные триггеры из `flight_program_ac`
- Триггеры активации из неактивного состояния

### Variables (с триггерами) - изменения при срабатывании
- Смена `status_id`
- Сброс `ppr = 0` при завершении ремонта
- Установка `repair_days` при входе в ремонт
- Формирование редких триггеров `partout_trigger`, `assembly_trigger`

## Итоговый LoggingLayer Planes
**Структура выходных данных:**
```
dates, aircraft_number, ac_type_mask, daily_flight, status_id,
sne, ppr, repair_days, partout_trigger, assembly_trigger,
version_date, version_id, aircraft_age_years
```

**Назначение:** 
- Экспорт в ClickHouse для аналитики
- Основа для SupersetBI дашбордов
- Вход для дальнейшей симуляции multiBOM агрегатов 