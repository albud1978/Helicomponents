# Transform Stage - Архитектура Flame GPU для планеров
**Дата создания:** 23-07-2025  
**Последнее обновление:** 24-07-2025

## Полная последовательность Transform этапа

### ЭТАП 1: Создание Property и MacroProperty из ClickHouse

#### 1.1 Загрузка таблиц из ClickHouse
```sql
-- Источники данных
SELECT * FROM md_components           -- → MacroProperty1
SELECT * FROM heli_pandas            -- → MacroProperty3  
SELECT * FROM flight_program_ac      -- → MacroProperty4
SELECT * FROM flight_program_fl      -- → MacroProperty5
```

#### 1.2 Матчинг полей через dict_digital_values_flat
```python
# Преобразование полей в field_id
field_mapping = {
    'partno_comp': 1,     'group_by': 10,       'll_mi8': 12,
    'll_mi17': 15,        'oh_mi8': 13,         'oh_mi17': 16,
    'repair_time': 17,    'partout_time': 18,   'assembly_time': 19,
    'br': 20,            'aircraft_number': 32, 'partseqno_i': 34,
    'psn': 35,           'address_i': 36,      'ac_type_mask': 37,
    'sne': 38,           'ppr': 39,            'repair_days': 40,
    'status_id': 25,     'lease_restricted': 41
}
```

#### 1.3 Формирование MacroProperty структур
- **MacroProperty1:** md_components с нормативами (ll, oh, repair_time, br)
- **MacroProperty2:** Пустая структура для LoggingLayer Planes (результат симуляции)
- **MacroProperty3:** heli_pandas с начальными данными агентов
- **MacroProperty4:** flight_program_ac с триггерами (trigger_program_mi8/mi17, new_counter_mi17)
- **MacroProperty5:** flight_program_fl - тензор налетов [4000_дней × количество_планеров]

### ЭТАП 2: Создание агентов планеров

#### 2.1 Инициализация агентов из MacroProperty3
```python
FOR each record IN MacroProperty3:
    CREATE Agent:
        agent_id = record.serialno
        
        # Динамические переменные (будут изменяться)
        status_id = record.status_id
        sne = record.sne  
        ppr = record.ppr
        repair_days = record.repair_days
        
        # Статические переменные (не изменяются)
        aircraft_number = record.aircraft_number
        ac_type_mask = record.ac_type_mask
        partseqno_i = record.partseqno_i
        mfg_date = record.mfg_date
        
        # Нормативы из MacroProperty1 (по partseqno_i)
        ll = MacroProperty1[partno_comp=partseqno_i].ll_mi8/mi17
        oh = MacroProperty1[partno_comp=partseqno_i].oh_mi8/mi17  
        br = MacroProperty1[partno_comp=partseqno_i].br
        repair_time = MacroProperty1[partno_comp=partseqno_i].repair_time
        partout_time = MacroProperty1[partno_comp=partseqno_i].partout_time
        assembly_time = MacroProperty1[partno_comp=partseqno_i].assembly_time
        
        # Триггерные переменные (формируются при событиях)
        active_trigger = 0
        partout_trigger = 0
        assembly_trigger = 0
```

#### 2.2 Разделение агентов по Agent States
```python
# Автоматическая группировка агентов по status_id
inactive_agents = filter(agents, status_id=1)    # Layer 1
ops_agents = filter(agents, status_id=2)         # Layer 2  
stock_agents = filter(agents, status_id=3)       # Layer 3
repair_agents = filter(agents, status_id=4)      # Layer 4
reserve_agents = filter(agents, status_id=5)     # Layer 5
store_agents = filter(agents, status_id=6)       # Layer 6
```

## Полная последовательность Transform этапа

### ЭТАП 1: Создание Property и MacroProperty из ClickHouse

#### 1.1 Загрузка таблиц из ClickHouse
```sql
-- Источники данных для Transform
SELECT * FROM md_components           -- → MacroProperty1
SELECT * FROM heli_pandas            -- → MacroProperty3  
SELECT * FROM flight_program_ac      -- → MacroProperty4
SELECT * FROM flight_program_fl      -- → MacroProperty5
```

#### 1.2 Матчинг полей через dict_digital_values_flat
```python
# Преобразование названий полей в field_id
field_mapping = {
    'partno_comp': 1,     'group_by': 10,       'll_mi8': 12,
    'll_mi17': 15,        'oh_mi8': 13,         'oh_mi17': 16,
    'repair_time': 17,    'partout_time': 18,   'assembly_time': 19,
    'br': 20,            'dates': 21,          'aircraft_number': 22,
    'daily_flight': 24,   'status_id': 25,      'partout_trigger': 26,
    'assembly_trigger': 27, 'mfg_date': 30,     'active_trigger': 31,
    'partseqno_i': 34,    'psn': 35,           'address_i': 36,
    'ac_type_mask': 37,   'sne': 38,           'ppr': 39,
    'repair_days': 40,    'lease_restricted': 41, 'trigger_program_mi8': 42,
    'trigger_program_mi17': 43, 'new_counter_mi17': 44
}
```

#### 1.3 Формирование MacroProperty структур
- **MacroProperty1:** md_components с нормативами компонентов
- **MacroProperty2:** Пустая структура для LoggingLayer Planes (результат симуляции)
- **MacroProperty3:** heli_pandas с данными агентов + новорожденные
- **MacroProperty4:** flight_program_ac с глобальными триггерами
- **MacroProperty5:** flight_program_fl - тензор налетов [dates × aircraft_numbers]

### ЭТАП 2: Создание агентов планеров

#### 2.1 Инициализация агентов из MacroProperty3
```python
FOR each record IN MacroProperty3:
    CREATE Agent:
        agent_id = record.serialno
        
        # Динамические переменные (изменяются в симуляции)
        status_id = record.status_id
        sne = record.sne  
        ppr = record.ppr
        repair_days = record.repair_days
        
        # Статические переменные (не изменяются)
        aircraft_number = record.aircraft_number
        ac_type_mask = record.ac_type_mask
        partseqno_i = record.partseqno_i
        mfg_date = record.mfg_date
        
        # Нормативы из MacroProperty1 (JOIN по partseqno_i)
        ll = MacroProperty1[partno_comp=partseqno_i].ll_mi8/mi17
        oh = MacroProperty1[partno_comp=partseqno_i].oh_mi8/mi17
        br = MacroProperty1[partno_comp=partseqno_i].br
        repair_time = MacroProperty1[partno_comp=partseqno_i].repair_time
        partout_time = MacroProperty1[partno_comp=partseqno_i].partout_time
        assembly_time = MacroProperty1[partno_comp=partseqno_i].assembly_time
        
        # Триггерные переменные (формируются при событиях)
        active_trigger = 0
        partout_trigger = 0  
        assembly_trigger = 0
```

#### 2.2 Автоматическая группировка по Agent States
```python
# Flame GPU автоматически распределяет агентов по слоям
inactive_layer = filter(agents, status_id=1)    # fn_inactive_layer
ops_layer = filter(agents, status_id=2)         # fn_ops_layer  
stock_layer = filter(agents, status_id=3)       # fn_stock_layer
repair_layer = filter(agents, status_id=4)      # fn_repair_layer
reserve_layer = filter(agents, status_id=5)     # fn_reserve_layer
store_layer = filter(agents, status_id=6)       # fn_store_layer
```

### ЭТАП 3: Вычисления на Flame GPU (основная симуляция)

## Архитектура множественных слоев Flame GPU

### Принцип state-based архитектуры
Каждый Agent State = отдельный слой = специализированная RTC функция
- **6 слоев по статусам планеров** (status_id 1-6)
- **2 глобальных RTC слоя** (spawn + balance)
- **Параллельное выполнение** слоев без зависимостей
- **Автоматические переходы** между слоями через dependency analysis

### MacroProperty структура (5 таблиц)
Flame GPU Environment содержит 5 основных таблиц в MacroProperty:

#### MacroProperty1: md_components
- **Источник:** таблица `md_components` из ClickHouse
- **Назначение:** параметры компонентов для расчетов
- **Ключевые поля:**
  - `partno_comp` (field_id: 1) - чертежный номер агрегата в AMOS
  - `group_by` (field_id: 10) - группировка планеров (1=МИ-8, 2=МИ-17)
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
  - `partout_trigger` (field_id: 26), `assembly_trigger` (field_id: 27) - триггеры ремонта
  - `mfg_date` (field_id: 30) - дата производства
  - `active_trigger` (field_id: 31) - дата активации из неактивного статуса

#### MacroProperty3: heli_pandas
- **Источник:** таблица `heli_pandas` из ClickHouse
- **Назначение:** начальные данные планеров + данные новорожденных
- **Ключевые поля:**
  - `serialno` (agent_id) - уникальный идентификатор агента
  - `aircraft_number` (field_id: 32) - номер ВС (для планеров = serialno)
  - `partseqno_i` (field_id: 34) - чертежный номер цифровой AMOS
  - `psn` (field_id: 35) - заводской номер цифровой AMOS
  - `address_i` (field_id: 36) - собственник цифровой AMOS
  - `ac_type_mask` (field_id: 37) - маска типа ВС (32=МИ-8, 64=МИ-17)
  - `sne` (field_id: 38) - наработка с начала эксплуатации
  - `ppr` (field_id: 39) - наработка после последнего ремонта
  - `repair_days` (field_id: 40) - счетчик дней ремонта (начальное значение=0)
  - `status_id` (field_id: 25) - статус планера
  - `lease_restricted` (field_id: 41) - ограничения лизинга

#### MacroProperty4: flight_program_ac
- **Источник:** таблица `flight_program_ac` из ClickHouse
- **Назначение:** программы операций и глобальные триггеры
- **Ключевые поля:**
  - `trigger_program_mi8` (field_id: 42) - триггер балансировки МИ-8
  - `trigger_program_mi17` (field_id: 43) - триггер балансировки МИ-17
  - `new_counter_mi17` (field_id: 44) - триггер рождения новых МИ-17
  - `trigger_program_mi8`, `trigger_program_mi17` - триггеры программ

#### MacroProperty5: flight_program_fl
- **Источник:** таблица `flight_program_fl` из ClickHouse
- **Назначение:** нормативные налеты планеров
- **Структура:** тензор 4000 дней × количество планеров

### Agent Variables для планеров
**Динамические переменные (изменяются в процессе симуляции):**
- `status_id` (field_id: 25) - текущий статус планера
- `serialno` (agent_id) - уникальный идентификатор агента
- `sne` (field_id: 38) - наработка с начала эксплуатации 
- `ppr` (field_id: 39) - наработка после последнего ремонта
- `repair_days` (field_id: 40) - счетчик дней в ремонте

**Статические переменные (загружаются из MacroProperty3):**
- `aircraft_number` (field_id: 32) - номер ВС (для планеров = serialno)
- `ac_type_mask` (field_id: 37) - тип планера (32=МИ-8, 64=МИ-17)
- `partseqno_i` (field_id: 34) - чертежный номер
- `mfg_date` (field_id: 30) - дата производства
- `ll`, `oh`, `br`, `repair_time` - нормативы из MacroProperty1

**Триггерные переменные (формируются при событиях):**
- `active_trigger` (field_id: 31) - дата активации из неактивного статуса
- `partout_trigger` (field_id: 26) - дата начала разборки при ремонте
- `assembly_trigger` (field_id: 27) - дата начала сборки при ремонте

### Разделение планеров по типам ВС
**Логика массивов через ac_type_mask:**
1. **МИ-8:** ac_type_mask = 32, обрабатывается trigger_program_mi8
2. **МИ-17:** ac_type_mask = 64, обрабатывается trigger_program_mi17

## Flame GPU Layers Architecture

### Agent State Layers (6 слоев по статусам)
**LAYER 1:** `fn_inactive_layer` (status_id: 1) - "Неактивен"
- **Триггеры:** отсутствуют
- **Переходы:** через rtc_balance_ac → статус 2
- **Нагрузка:** минимальная

**LAYER 2:** `fn_ops_layer` (status_id: 2) - "Эксплуатация"  
- **Триггеры:** 3 ресурсных триггера
- **Переходы:** статус 4 (ремонт) или статус 6 (хранение)
- **Нагрузка:** максимальная (накопление наработки + проверка ресурсов)

**LAYER 3:** `fn_stock_layer` (status_id: 3) - "Исправен"
- **Триггеры:** отсутствуют  
- **Переходы:** через rtc_balance_ac → статус 2 (первый приоритет)
- **Нагрузка:** минимальная

**LAYER 4:** `fn_repair_layer` (status_id: 4) - "Ремонт"
- **Триггеры:** 1 agent триггер (завершение ремонта)
- **Переходы:** статус 5 (резерв) при repair_days == repair_time
- **Нагрузка:** средняя (инкремент repair_days)

**LAYER 5:** `fn_reserve_layer` (status_id: 5) - "Резерв"
- **Триггеры:** отсутствуют
- **Переходы:** через rtc_balance_ac → статус 2 (второй приоритет)
- **Нагрузка:** минимальная

**LAYER 6:** `fn_store_layer` (status_id: 6) - "Хранение"
- **Триггеры:** отсутствуют
- **Переходы:** отсутствуют (конечное состояние)
- **Нагрузка:** нулевая

### Global RTC Layers (2 триггерных слоя)
**RTC_SPAWN_LAYER:** `rtc_spawn_layer` - рождение новых МИ-17
**RTC_BALANCE_LAYER:** `rtc_balance_layer` - балансировка количества в эксплуатации

## Система триггеров

### Global Triggers (6 глобальных триггеров)
1. **new_counter_mi17 > 0** → `rtc_spawn_layer`
2. **trigger_program_mi8 ≠ 0** → `rtc_balance_layer` 
3. **trigger_program_mi17 ≠ 0** → `rtc_balance_layer`
4. **(ll-sne) < daily_flight** → статус 6 + `rtc_balance_layer`
5. **(oh-ppr) < daily_flight AND sne >= br** → статус 6 + `rtc_balance_layer`
6. **(oh-ppr) < daily_flight AND sne < br** → статус 4 + `rtc_balance_layer`

### Agent Triggers (1 локальный триггер)
- **repair_days == repair_time** → статус 4→5 (завершение ремонта)

## RTC Balance Architecture

### rtc_spawn_ac логика (УПРОЩЕННАЯ)
**Условие запуска:** `new_counter_mi17 > 0`  
**Назначение:** Рождение новых агентов-планеров в статусе "исправен"

**Алгоритм (ХАРДКОД МИ-17):**
```
FOR i = 1 TO new_counter_mi17:
    1. serialno = find_next_free_in_range(100000, 150000)
    2. Проверить serialno в агентах И MacroProperty3
    3. Создать запись MacroProperty3:
       - serialno = новый номер
       - sne = 0, ppr = 0, repair_days = 0
       - mfg_date = current_simulation_date
       - lease_restricted = 1
       - address_i = 17094  # ХАРДКОД: владелец новых МИ-17
       - partseqno_i = 70482  # ХАРДКОД: тип МИ-17
       - ac_type_mask = 64  # ХАРДКОД: битовая маска МИ-17
       - ll = MacroProperty1[partno_comp=70482].ll_mi17
       - oh = MacroProperty1[partno_comp=70482].oh_mi17
       - status_id = 3  # ИСПРАВЕН (не в эксплуатацию)
       - aircraft_number = serialno
    4. Создать агента с Agent Variables
END FOR
```

**ХАРДКОД КОНСТАНТЫ:**
- Диапазон номеров: 100000-150000 (резерв для новых МИ-17)
- address_i: 17094 (владелец для новых планеров)
- partseqno_i: 70482 (стандартный тип МИ-17)
- ac_type_mask: 64 (битовая маска МИ-17)
- lease_restricted: 1 (все новые - лизинговые)
- status_id: 3 (на склад исправными, НЕ в эксплуатацию)

### rtc_balance_layer алгоритм (УПРОЩЕННАЯ ЛОГИКА)
**Принцип:** Триггеры содержат готовое количество планеров для перевода

**Обработка триггеров:**
```
# Получение готовых значений триггеров
trigger_mi8 = MacroProperty4.trigger_program_mi8
trigger_mi17 = MacroProperty4.trigger_program_mi17

# Обработка МИ-8 (ac_type_mask=32)
IF trigger_mi8 ≠ 0:
    process_balance_for_type(ac_type_mask=32, trigger_value=trigger_mi8)

# Обработка МИ-17 (ac_type_mask=64)  
IF trigger_mi17 ≠ 0:
    process_balance_for_type(ac_type_mask=64, trigger_value=trigger_mi17)
```

**Универсальная функция балансировки:**
```
FUNCTION process_balance_for_type(ac_type_mask, trigger_value):
    abs_count = ABS(trigger_value)       # Количество планеров
    direction = SIGN(trigger_value)      # Направление: +1 или -1
    
    IF direction > 0:  # ДЕФИЦИТ (положительный триггер)
        transfer_to_ops(ac_type_mask, abs_count)
    ELSE:  # ИЗБЫТОК (отрицательный триггер)  
        transfer_from_ops(ac_type_mask, abs_count)
```

**Алгоритм дефицита (положительный триггер):**
```
FUNCTION transfer_to_ops(ac_type_mask, count_needed):
    transferred = 0
    
    # Приоритет 1: Исправен (3) → Эксплуатация (2)
    WHILE transferred < count_needed:
        agent = find_first_agent(status_id=3, ac_type_mask)
        IF agent FOUND:
            agent.status_id = 2
            transferred++
        ELSE BREAK
    
    # Приоритет 2: Резерв (5) → Эксплуатация (2)
    WHILE transferred < count_needed:
        agent = find_first_agent(status_id=5, ac_type_mask)
        IF agent FOUND:
            agent.status_id = 2
            transferred++
        ELSE BREAK
    
    # Приоритет 3: Неактивен (1) → Эксплуатация (2)
    WHILE transferred < count_needed:
        agent = find_first_agent(status_id=1, ac_type_mask, ORDER_BY="mfg_date DESC")
        IF agent FOUND:
            agent.status_id = 2
            agent.active_trigger = current_date
            transferred++
        ELSE BREAK
```

**Алгоритм избытка (отрицательный триггер):**
```
FUNCTION transfer_from_ops(ac_type_mask, count_excess):
    transferred = 0
    
    # Эксплуатация (2) → Исправен (3)
    WHILE transferred < count_excess:
        agent = find_first_agent(status_id=2, ac_type_mask)
        IF agent FOUND:
            agent.status_id = 3
            transferred++
        ELSE BREAK
        ELSE: BREAK
    
    # Balance step 2: Резерв
    WHILE corrected < balance_needed:
        agent = find_first(status_id=5, ac_type_mask=target_mask)
        IF agent FOUND:
            agent.status_id = 2
            corrected++
        ELSE: BREAK
    
    # Balance step 3: Неактивные (упрощенная логика)
    WHILE corrected < balance_needed:
        agent = find_first(status_id=1, ac_type_mask=target_mask, ORDER BY mfg_date DESC)
        IF agent FOUND:
            repair_time = MacroProperty1[partno_comp=agent.partseqno_i].repair_time
            time_since_start = current_date - version_date
            
            IF time_since_start >= repair_time:
                agent.status_id = 2
                agent.active_trigger = current_date
                corrected++
            ELSE:
                BREAK  # Оставляем дефицит
        ELSE: BREAK

ELIF balance_needed < 0:  # Избыток
    # Последовательное уменьшение для целевого типа
    corrected = 0
    excess = ABS(balance_needed)
    
    WHILE corrected < excess:
        agent = find_first(status_id=2, ac_type_mask=target_mask)
        IF agent FOUND:
            agent.status_id = 3
            corrected++
        ELSE: BREAK
```

## Layer Functions (детализация по слоям)

### Layer 1: fn_inactive_layer (status_id = 1)
**Статус:** "Неактивен"  
**Логика:** Минимальная - планеры ожидают активации
```
FOR each agent WHERE status_id = 1:
    # Штатные переменные без накопления наработки
    daily_flight = 0
    # Переходы только через rtc_balance_layer
END FOR
```

### Layer 2: fn_ops_layer (status_id = 2)  
**Статус:** "Эксплуатация"  
**Логика:** Максимальная - накопление наработки + 3 ресурсных триггера
```
FOR each agent WHERE status_id = 2:
    # Получение нормативного налета из MacroProperty5
    daily_flight = MacroProperty5[aircraft_number][current_date]
    
    # Накопление наработки
    agent.sne += daily_flight
    agent.ppr += daily_flight
    
    # Получение нормативов из переменных агента
    ll = agent.ll        # назначенный ресурс
    oh = agent.oh        # межремонтный ресурс  
    br = agent.br        # граница ремонтопригодности
    
    # Ресурсный триггер 1: Исчерпание назначенного ресурса
    IF (ll - agent.sne) < daily_flight:
        agent.status_id = 6  # → "Хранение"
        trigger_rtc_balance_layer()
        CONTINUE
    
    # Ресурсный триггер 2: МРР исчерпан + НЕ ремонтопригодный
    IF (oh - agent.ppr) < daily_flight AND agent.sne >= br:
        agent.status_id = 6  # → "Хранение"  
        trigger_rtc_balance_layer()
        CONTINUE
    
    # Ресурсный триггер 3: МРР исчерпан + ремонтопригодный
    IF (oh - agent.ppr) < daily_flight AND agent.sne < br:
        agent.status_id = 4  # → "Ремонт"
        agent.repair_days = 0  # Начинаем с 0
        
        # Формирование триггеров ремонта
        agent.partout_trigger = current_date + agent.partout_time
        agent.assembly_trigger = current_date + agent.repair_time - agent.assembly_time
        
        trigger_rtc_balance_layer()
END FOR
```

### Layer 3: fn_stock_layer (status_id = 3)
**Статус:** "Исправен"  
**Логика:** Минимальная - готовность к эксплуатации
```
FOR each agent WHERE status_id = 3:
    # Штатные переменные без накопления наработки
    daily_flight = 0
    # Первый приоритет для перевода в эксплуатацию через rtc_balance_layer
END FOR
```

### Layer 4: fn_repair_layer (status_id = 4)
**Статус:** "Ремонт"  
**Логика:** Средняя - инкремент счетчика + agent триггер завершения
```
FOR each agent WHERE status_id = 4:
    # Увеличение счетчика дней ремонта
    agent.repair_days += 1
    
    # Agent триггер: завершение ремонта
    IF agent.repair_days == repair_time:  # РАВЕНСТВО, не больше
        agent.status_id = 5  # → "Резерв" (НЕ исправен!)
        agent.ppr = 0  # Сброс наработки после ремонта
        agent.repair_days = 0  # Сброс счетчика
        agent.partout_trigger = 0
        agent.assembly_trigger = 0
    
    daily_flight = 0  # Нет наработки в ремонте
END FOR
```

### Layer 5: fn_reserve_layer (status_id = 5)
**Статус:** "Резерв"  
**Логика:** Минимальная - стратегический запас после ремонта
```
FOR each agent WHERE status_id = 5:
    # Штатные переменные без накопления наработки
    daily_flight = 0
    # Второй приоритет для перевода в эксплуатацию через rtc_balance_layer
    # Сюда попадают планеры ПОСЛЕ РЕМОНТА
END FOR
```

### Layer 6: fn_store_layer (status_id = 6)
**Статус:** "Хранение"  
**Логика:** Нулевая - конечное состояние
```
FOR each agent WHERE status_id = 6:
    # Штатные переменные без изменений
    daily_flight = 0
    # КОНЕЧНОЕ СОСТОЯНИЕ - никаких переходов
    # Списанные/законсервированные планеры
END FOR
```

## Постпроцессинг LoggingLayer Planes

### Функции постпроцессинга
**Порядок выполнения после всех RTC функций:**

#### 1. Обработка триггеров с датами
```
FOR each agent:
    # 1.1 active_trigger: ретроспективная настройка входа в ремонт
    IF agent.active_trigger > 1:  # Значение = дата
        target_date = agent.active_trigger
        repair_time = MacroProperty1[partno_comp=agent.partseqno_i].repair_time
        assembly_time = MacroProperty1[partno_comp=agent.partseqno_i].assembly_time
        
        # Устанавливаем статус ремонт на целевую дату
        LoggingLayer[target_date][agent.serialno].status_id = 4
        
        # Устанавливаем assembly_trigger на (repair_time - assembly_time) дней позже
        assembly_date = target_date + (repair_time - assembly_time)
        LoggingLayer[assembly_date][agent.serialno].assembly_trigger = 1
        
        # Обнуляем исходный триггер
        agent.active_trigger = 0
    
    # 1.2 partout_trigger: установка триггера на будущую дату
    IF agent.partout_trigger > 1:  # Значение = дата
        target_date = agent.partout_trigger
        LoggingLayer[target_date][agent.serialno].partout_trigger = 1
        agent.partout_trigger = 0
    
    # 1.3 assembly_trigger: установка триггера на будущую дату
    IF agent.assembly_trigger > 1:  # Значение = дата
        target_date = agent.assembly_trigger
        LoggingLayer[target_date][agent.serialno].assembly_trigger = 1
        agent.assembly_trigger = 0
END FOR
```

#### 2. Обогащение полей из MacroProperty3
```
FOR each agent:
    # 2.1 ac_type_mask из MacroProperty3
    agent.ac_type_mask = MacroProperty3[agent.serialno].ac_type_mask
    
    # 2.2 version_date/version_id из MacroProperty3
    agent.version_date = MacroProperty3[agent.serialno].version_date
    agent.version_id = MacroProperty3[agent.serialno].version_id
    
    # 2.3 mfg_date из MacroProperty3
    agent.mfg_date = MacroProperty3[agent.serialno].mfg_date
END FOR
```

#### 3. Расчет aircraft_age_years
```
FOR each agent:
    # Гибридный расчет возраста
    mfg_date = agent.mfg_date  # Из MacroProperty3 или dynamic birth_date
    age_days = current_date - mfg_date
    
    # Возраст в полных годах (округление вниз)
    agent.aircraft_age_years = FLOOR(age_days / 365.25)  # uint8
    
    # Для планеров: aircraft_number = serialno (Macroproperty, НЕ Variable)
    IF agent_type == "планер":
        agent.aircraft_number = agent.serialno  # Обогащение из MacroProperty3
END FOR
```

## Архитектура слоев

### Вопрос раздельных слоев по статусам
**Анализ возможностей:**

#### Вариант 1: Единый слой + RTC функции (текущий)
- **Преимущества:** Простота управления, единая логика
- **Недостатки:** Сложные RTC функции, смешанная логика

#### Вариант 2: Раздельные слои по статусам
- **Преимущества:** Четкое разделение логики, простые функции слоя
- **Недостатки:** Смерть/рождение агентов, межслойная синхронизация

#### Вариант 3: Гибридный подход
- **Слой "Эксплуатация"** (status_id = 2) - активные агенты с накоплением
- **Слой "Склад"** (status_id = 3,5) - пассивные агенты без накопления  
- **Слой "Ремонт"** (status_id = 4) - агенты с календарной логикой
- **Слой "Хранение"** (status_id = 1,6) - неактивные агенты

**Рекомендация:** Текущий подход с RTC функциями проще для реализации и поддержки.

## Логика симуляции

### Порядок выполнения в симуляции

```
1. RTC Triggers (если условия):
   - rtc_spawn_ac (при new_counter_mi17 > 0)
   - rtc_balance_ac (при trigger_program ≠ 0)
   - fn_balance_ac (при ресурсных триггерах)

2. RTC Step Functions (векторизация по статусам):
   - fn_inactive_ac (status_id = 1)
   - fn_ops_ac (status_id = 2)  
   - fn_stock_ac (status_id = 3)
   - fn_repair_ac (status_id = 4)
   - fn_reserve_ac (status_id = 5)
   - fn_store_ac (status_id = 6)

3. Постпроцессинг LoggingLayer:
   - Обработка триггеров с датами
   - Обогащение полей из MacroProperty3
   - Расчет aircraft_age_years
```

### Векторизация выполнения
- **RTC Step Functions** повторяются каждый день до наступления Global Trigger
- **Global Triggers** останавливают векторизацию и запускают глобальную балансировку
- **Календарные функции** (ремонт, хранение) работают без остановки

## Итоговый LoggingLayer Planes
**Структура выходных данных:**
```
dates, aircraft_number, ac_type_mask, daily_flight, status_id,
sne, ppr, repair_days, partout_trigger, assembly_trigger, active_trigger,
version_date, version_id, mfg_date, aircraft_age_years
```

**Назначение:** 
- Экспорт в ClickHouse для аналитики
- Основа для SupersetBI дашбордов
- Вход для дальнейшей симуляции multiBOM агрегатов 

## Схема переходов между слоями

```
РОЖДЕНИЕ → 3 (Исправен) ←─── rtc_balance ←─── 1 (Неактивен)
              ↓                                      ↑
              ↓ rtc_balance (приоритет 1)            │
              ↓                                      │
              ↓                                      │
              ↓                                      │
          2 (Эксплуатация) ←── rtc_balance ←── 5 (Резерв)
              ↓                  (приоритет 2)       ↑
              ↓ ресурсные триггеры                   │
              ↓                                      │
              ↓                                      │
              ↓                                      │
    ┌─── 4 (Ремонт) ──── repair_days==repair_time ──┘
    │
    └─── 6 (Хранение) ← КОНЕЧНОЕ СОСТОЯНИЕ
```

## Хардкод константы

### rtc_spawn_layer (МИ-17)
- **Диапазон serialno:** 100000-150000
- **address_i:** 17094 (поставщик новых МИ-17)
- **partseqno_i:** 70482 (стандартный чертежный номер МИ-17)
- **ac_type_mask:** 64 (битовая маска МИ-17)  
- **lease_restricted:** 1 (все новые планеры лизинговые)
- **Начальный статус:** 3 ("Исправен")

### Приоритеты rtc_balance_layer
**При дефиците (положительный триггер):**
1. **Исправен (3)** → Эксплуатация (2) - новые и готовые
2. **Резерв (5)** → Эксплуатация (2) - отремонтированные
3. **Неактивен (1)** → Эксплуатация (2) - крайняя мера

**При избытке (отрицательный триггер):**
- **Эксплуатация (2)** → Исправен (3) - возврат в готовность

### Типы ВС
- **МИ-8:** ac_type_mask = 32, trigger_program_mi8
- **МИ-17:** ac_type_mask = 64, trigger_program_mi17

## Dependency Graph

### Execution Layers
**Layer 1:** Agent State Functions (параллельно)
- fn_inactive_layer, fn_ops_layer, fn_stock_layer, fn_repair_layer, fn_reserve_layer, fn_store_layer

**Layer 2:** Global RTC Functions (после State Functions)
- rtc_spawn_layer, rtc_balance_layer

**Layer 3:** Post-processing
- Обогащение полей, коррекция триггеров 

### ЭТАП 4: Формирование и обогащение LoggingLayer Planes

#### 4.1 Сбор данных симуляции в MacroProperty2
```python
# После каждого дня симуляции
FOR each agent:
    LoggingLayer_record = {
        'dates': current_simulation_date,
        'aircraft_number': agent.aircraft_number,
        'daily_flight': agent.daily_flight,
        'status_id': agent.status_id,
        'sne': agent.sne,
        'ppr': agent.ppr,
        'repair_days': agent.repair_days,
        'partout_trigger': agent.partout_trigger,
        'assembly_trigger': agent.assembly_trigger,
        'active_trigger': agent.active_trigger,
        'mfg_date': agent.mfg_date
    }
    
    MacroProperty2.append(LoggingLayer_record)
```

#### 4.2 Постпроцессинг (после завершения симуляции)
```python
# Обогащение полей в LoggingLayer Planes
FOR each record IN MacroProperty2:
    
    # 4.2.1 Обработка active_trigger (ретроспективная коррекция)
    IF record.active_trigger > 0:
        target_date = record.active_trigger
        assembly_time = MacroProperty1[partno_comp=record.partseqno_i].assembly_time
        
        # Коррекция входа в ремонт
        LoggingLayer[target_date][record.aircraft_number].status_id = 4
        LoggingLayer[target_date - assembly_time][record.aircraft_number].assembly_trigger = 1
    
    # 4.2.2 Обогащение version_date и version_id
    record.version_date = simulation_start_date
    record.version_id = simulation_run_id
    
    # 4.2.3 Расчет aircraft_age_years
    record.aircraft_age_years = (record.dates - record.mfg_date) / 365.25
    
    # 4.2.4 Проверка ac_type_mask (из MacroProperty3)
    record.ac_type_mask = MacroProperty3[serialno=record.aircraft_number].ac_type_mask
```

#### 4.3 Финальная структура LoggingLayer Planes
```python
# Итоговые поля для экспорта в ClickHouse
LoggingLayer_Planes_fields = [
    'dates',              # (field_id: 21) - дата симуляции
    'aircraft_number',    # (field_id: 22) - номер планера
    'daily_flight',       # (field_id: 24) - суточный налет
    'status_id',          # (field_id: 25) - статус планера
    'partout_trigger',    # (field_id: 26) - триггер разборки
    'assembly_trigger',   # (field_id: 27) - триггер сборки
    'mfg_date',           # (field_id: 30) - дата производства
    'active_trigger',     # (field_id: 31) - триггер активации
    'ac_type_mask',       # (field_id: 37) - тип ВС
    'sne',                # (field_id: 38) - наработка СНЭ
    'ppr',                # (field_id: 39) - наработка ППР
    'repair_days',        # (field_id: 40) - дни в ремонте
    'version_date',       # дата запуска симуляции
    'version_id',         # ID прогона симуляции
    'aircraft_age_years'  # возраст планера в годах
]
```

### ЭТАП 5: Проверка результатов симуляции

#### 5.1 Валидация данных LoggingLayer Planes
```python
# Проверка целостности данных
validation_checks = {
    # 5.1.1 Проверка непрерывности дат
    'dates_continuity': check_sequential_dates(LoggingLayer.dates),
    
    # 5.1.2 Проверка переходов статусов
    'status_transitions': validate_status_changes(LoggingLayer.status_id),
    
    # 5.1.3 Проверка накопления наработки
    'flight_accumulation': validate_sne_ppr_growth(LoggingLayer.sne, LoggingLayer.ppr),
    
    # 5.1.4 Проверка триггеров ремонта
    'repair_triggers': validate_repair_flow(LoggingLayer.partout_trigger, LoggingLayer.assembly_trigger),
    
    # 5.1.5 Проверка балансировки
    'balance_verification': check_ops_count_vs_program(LoggingLayer.status_id, MacroProperty4.trigger_program)
}
```

#### 5.2 Контроль качества по планерам
```python
# Агрегированная проверка результатов
quality_metrics = {
    # 5.2.1 Соответствие программе полетов
    'program_compliance': {
        'mi8_ops_count': count_daily_ops(ac_type_mask=32, status_id=2),
        'mi17_ops_count': count_daily_ops(ac_type_mask=64, status_id=2),
        'target_vs_actual': compare_with_flight_program()
    },
    
    # 5.2.2 Реалистичность ремонтных циклов
    'repair_cycles': {
        'average_repair_time': calculate_avg_repair_duration(),
        'repair_completion_rate': count_completed_repairs(),
        'stuck_in_repair': find_overdue_repairs()
    },
    
    # 5.2.3 Ресурсное планирование
    'resource_utilization': {
        'sne_exhaustion_rate': count_ll_exhausted(),
        'ppr_exhaustion_rate': count_oh_exhausted(),
        'premature_disposal': find_early_store_transitions()
    },
    
    # 5.2.4 Новые поставки
    'spawn_verification': {
        'mi17_spawned_count': count_new_agents(ac_type_mask=64),
        'spawn_trigger_response': verify_spawn_triggers(),
        'new_agents_status': check_spawned_initial_status()
    }
}
```

#### 5.3 Экспорт результатов в ClickHouse
```sql
-- Финальная загрузка LoggingLayer Planes
INSERT INTO LoggingLayer_Planes (
    dates, aircraft_number, daily_flight, status_id,
    partout_trigger, assembly_trigger, mfg_date, active_trigger,
    ac_type_mask, sne, ppr, repair_days,
    version_date, version_id, aircraft_age_years
) 
SELECT * FROM flame_gpu_output;

-- Проверочные запросы
SELECT 
    status_id,
    COUNT(*) as agent_count,
    AVG(sne) as avg_sne,
    AVG(ppr) as avg_ppr
FROM LoggingLayer_Planes 
WHERE dates = (SELECT MAX(dates) FROM LoggingLayer_Planes)
GROUP BY status_id;
```

## Связь этапов Transform с общим ETL

### Входящие данные (из Extract)
- ✅ **md_components** → готов из словарных загрузчиков
- ✅ **heli_pandas** → готов из dual_loader.py
- ✅ **flight_program_ac** → готов из program_ac_status_processor.py
- ✅ **flight_program_fl** → готов из flight_program_fl_loader.py

### Исходящие данные (в Load)
- 🎯 **LoggingLayer_Planes** → результаты симуляции по каждому дню для всех агентов (включая новорожденных МИ-17)
- 🎯 **Логи симуляции** → для отладки и мониторинга Transform процесса  
- ⚠️ **heli_pandas остается неизменной** → только источник данных, НЕ результат симуляции

#### Принцип разделения входных и выходных данных
```python
# ВХОДНЫЕ данные (READ-ONLY)
heli_pandas = source_data        # Исходное состояние парка на start_date
md_components = normatives       # Нормативы компонентов
flight_program_ac = triggers     # Программы и триггеры
flight_program_fl = flight_plan  # Плановые налеты

# ВЫХОДНЫЕ данные (результат симуляции)
LoggingLayer_Planes = {
    # Исходные агенты на каждый день симуляции
    existing_agents_daily_state: heli_pandas → daily_tracking,
    
    # Новорожденные агенты (когда spawn_trigger срабатывает)
    spawned_agents_daily_state: new_mi17_agents → daily_tracking,
    
    # Полная траектория каждого агента
    complete_simulation_timeline: all_agents × all_simulation_days
}
```

#### Структура новорожденных агентов в LoggingLayer_Planes
```python
# Когда срабатывает new_counter_mi17 (MacroProperty4)
spawned_mi17_agent = {
    'dates': spawn_date,                    # День рождения агента
    'aircraft_number': generate_new_id(),   # Новый уникальный номер
    'ac_type_mask': 64,                     # МИ-17
    'status_id': 3,                         # Начальный статус = склад
    'sne': 0,                               # Нулевая наработка СНЭ
    'ppr': 0,                               # Нулевая наработка ППР
    'mfg_date': spawn_date,                 # Дата производства = дата рождения
    'partseqno_i': 'MI17_BASELINE',         # Базовая конфигурация МИ-17
    
    # Эти записи появляются в LoggingLayer_Planes
    # и отслеживаются каждый день после spawn_date
}
```

### Интеграция с BI (финальная цель)
- 📊 **SupersetBI дашборды** на основе LoggingLayer_Planes
- 📈 **Аналитика ремонтных циклов** через status_id переходы
- 🔍 **Прогнозирование ресурсов** через sne/ppr накопление
- ⚡ **Real-time мониторинг** программы полетов через version_id 

## ДОРАБОТКИ RTC ФУНКЦИЙ (25-07-2025)

### rtc_ops_layer - Исправлена логика ресурсных триггеров

**ПРОБЛЕМА:** Упрощенная логика `ppr >= oh or sne >= ll` всегда переводила в ремонт.

**РЕШЕНИЕ:** Реализованы 3 точных ресурсных триггера согласно документации:

#### Триггер 1: Исчерпание жизненного лимита (ll)
```
Условие: (ll - agent['sne']) < daily_flight
Действие: status_id = 6 (Хранение)
```

#### Триггер 2: Исчерпание межремонтного ресурса при превышении списания (oh+br)  
```
Условие: (oh - agent['ppr']) < daily_flight AND agent['sne'] >= br
Действие: status_id = 6 (Хранение)
```

#### Триггер 3: Исчерпание межремонтного ресурса до списания (oh)
```
Условие: (oh - agent['ppr']) < daily_flight AND agent['sne'] < br  
Действие: status_id = 4 (Ремонт), repair_days = 1
Установка: partout_trigger, assembly_trigger (timestamp)
```

### rtc_repair_layer - Исправлен переход после ремонта

**ПРОБЛЕМА:** Неправильный переход в status_id = 3 (Склад).

**РЕШЕНИЕ:** 
- Переход в status_id = 5 (Резерв) согласно документации
- Изменено условие с `>=` на `==` для точного завершения
- Сброс триггеров в 0 при завершении ремонта

### rtc_balance_layer - Исправлены приоритеты активации

**ПРОБЛЕМА:** Неправильный порядок приоритетов и логика времени.

**РЕШЕНИЕ:**
- Приоритеты: Склад (3) → Резерв (5) → Неактивный (1)  
- Для неактивных: сортировка по mfg_date (новейшие первые)
- Логика времени: `(current_simulation_date - version_date).days >= repair_time`

### Значения repair_time - Исправлены по умолчанию

**ПРОБЛЕМА:** repair_time = 45 дней для планеров неверно.

**РЕШЕНИЕ:** repair_time = 180 дней согласно md_components.

### Форматирование триггеров - Добавлена функция _format_trigger_date

**ПРОБЛЕМА:** Ошибки типов при логировании timestamp триггеров.

**РЕШЕНИЕ:** Конвертация timestamp в строки 'YYYY-MM-DD' или None для логирования.

## РЕЗУЛЬТАТЫ ДОРАБОТОК

### Корректная симуляция 7 дней:
- Неактивных: 117 → 116 (активирован 1)
- Эксплуатация: 154 → 2 (переведено 152 по ресурсным триггерам)  
- Ремонт: 8 → 75 (+67 новых)
- Резерв: 0 → 59 (+59 завершивших ремонт - НЕ реальность для 7 дней!)
- Хранение: 0 → 27 (+27 по триггерам списания)

### Статус балансировки:
- Ежедневная активация 1 неактивного планера
- Невозможность активации в первые 180 дней (правильно)
- Дефицит планеров отражает реальную ситуацию с поздним планированием ремонтов 