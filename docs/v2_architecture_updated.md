# V2 State-based Architecture - Обновленная версия

## Дата: 2024-12-23

## Основные изменения

1. **Упрощение триггеров**: 
   - Убран `partout_trigger` 
   - `assembly_trigger` используется только как индикатор для постпроцессинга
   - Добавлен `active_trigger` для spawn логики

2. **Трёхэтапная обработка**:
   - Этап 1: Счетчики, сайд-эффекты и определение intent (RTC функции состояний)
   - Этап 2: Квотирование и смена state (менеджер состояний)
   - Этап 3: Постпроцессинг (после симуляции)

## Этап 1: RTC функции состояний

### State_1 (inactive)
```
Логика:
if (days - repair_time) > version_date then 
    intent = 2
else 
    intent = 1
```

### State_2 (operations)
```
Счетчики:
sne += dt
ppr += dt

Сайд-эффекты:
if active_trigger == 1 then active_trigger = 0
if assembly_trigger == 0 then assembly_trigger = 1

Определение intent:
if ppr >= oh - dt & sne < br then 
    intent = 4
elif sne >= ll - dt or (ppr >= oh - dt & sne >= br) then 
    intent = 6
else 
    intent = 2
```

### State_3 (serviceable)
```
intent = 2
```

### State_4 (repair)
```
Счетчики:
repair_days += 1

Сайд-эффекты:
if repair_days >= repair_time - assembly_time then 
    assembly_trigger = 1

Определение intent:
if repair_days == repair_time then 
    intent = 5
else 
    intent = 4
```

### State_5 (reserve)
```
intent = 2
```

### State_6 (storage)
```
intent = 6
```

## Этап 2: Менеджер состояний (квотирование и переходы)

### State_1 -> 2
```
if quota > 0 & intent == 2 then 
    state = 2
    assembly_trigger = 1
    active_trigger = current_date
    // Приоритет по min(mfg_date)
else 
    state = intent
```

### State_2 -> 4, 6, 3
```
if quota > 0 & intent == 2 then 
    state = 2
elif intent != 2 then 
    state = intent
    assembly_trigger = 0
else 
    state = 3
    assembly_trigger = 0
    // Приоритет по max(mfg_date) для перехода в 3
```

### State_3 -> 2
```
if quota > 0 & intent == 2 then 
    state = 2
    assembly_trigger = 1
    // Приоритет по min(mfg_date)
else 
    state = 3
```

### State_4 -> 5
```
state = intent
assembly_trigger = 0
```

### State_5 -> 2
```
if quota > 0 & intent == 2 then 
    state = 2
    assembly_trigger = 1
    // Приоритет по min(mfg_date)
else 
    state = 5
```

### State_6
```
state = intent
```

## Этап 3: Постпроцессинг

После завершения симуляции:

1. **Логирование в dataframe**
   - Сохранение всех переменных агентов
   - Трекинг переходов состояний

2. **Обработка active_trigger**
   ```
   if active_trigger > 0 then:
       state = 4
       repair_days = random(1..repair_time) 
       // в диапазоне от (date - repair_time) до date
       
       assembly_trigger = 1 
       // в диапазоне от (repair_time - assembly_time) до date
   ```
   где `date` - дата, когда `active_trigger > 0`

3. **Запись в СУБД**
   - Финальные состояния агентов
   - История переходов

## Ключевые особенности

1. **assembly_trigger**:
   - В state_2: всегда = 1 (планер в эксплуатации)
   - В state_4: = 1 только в конце ремонта (период сборки)
   - При переходе из других состояний: сбрасывается в 0

2. **active_trigger**:
   - Используется для spawn логики
   - Устанавливается при переходе 1->2 в текущую дату
   - Сбрасывается в state_2

3. **Приоритеты квотирования**:
   - Для входа в эксплуатацию (->2): по min(mfg_date) - старые первые
   - Для выхода из эксплуатации (2->3): по max(mfg_date) - новые первые

## Переменные агента

```cpp
// Идентификаторы
unsigned int idx
unsigned int aircraft_number
unsigned int partseqno_i
unsigned int group_by

// Наработки
unsigned int sne
unsigned int ppr
unsigned int cso

// Нормативы
unsigned int ll
unsigned int oh
unsigned int br

// Времена и счетчики
unsigned int repair_time
unsigned int assembly_time
unsigned int repair_days
unsigned int mfg_date

// Триггеры
unsigned int assembly_trigger
unsigned int active_trigger

// MP5 данные
unsigned int daily_today_u32
unsigned int daily_next_u32

// Intent для менеджера состояний
unsigned int intent_state
```
