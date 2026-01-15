# Логика определения адаптивных шагов

> **Статус:** Временный файл для анализа  
> **Дата:** 15-01-2026

---

## Часть 1: Детерминированные данные (до симуляции)

**Источники, известные при загрузке:**

| Источник | Откуда | Что содержит |
|----------|--------|--------------|
| **Program changes** | `flight_program_fl` | Даты изменения лётной программы (Mi-8/Mi-17 targets) |
| **Ресурсные лимиты** | `heli_pandas` | LL (life limit), OH (overhaul), BR (beyond repair) для каждого агента |
| **Начальные SNE/PPR** | `heli_pandas` | Текущая наработка и межремонтный ресурс |
| **Repair time** | константа | 180 дней ожидания "ремонта" |
| **MP5 cumsum** | `flight_program_fl` | Кумулятивные налёты по дням для бинарного поиска |

**Вычисляется при инициализации:**

1. **`program_change_days[]`** — массив дней, когда меняется target (ops count)
2. **`exit_date`** для агентов в repair/reserve — день выхода из состояния
3. **`limiter`** для агентов в operations — дней до ближайшего ресурсного лимита

---

## Часть 2: Цикл динамического определения длины шага

**На каждом шаге симуляции собираются три минимума:**

### 2.1. Минимум по ресурсам (операционные агенты)

```
Для каждого агента в operations:
    limiter = min(
        дней до SNE >= LL,
        дней до PPR >= OH
    )
    
mp_min_limiter = MIN(все limiter агентов в ops)
```

**Как вычисляется limiter:**
- Бинарный поиск в `mp5_cumsum`: найти день D, когда `cumsum[D] - cumsum[today] >= остаток_ресурса`
- `limiter = D - current_day`

### 2.2. Минимум по детерминированным выходам

```
Для агентов в repair:
    exit_date = день входа в repair + repair_time
    
Для агентов в unserviceable:
    exit_date = день входа в unsvc + repair_time
    
Для агентов в reserve (плановый spawn):
    exit_date = запланированный день активации

min_exit_date = MIN(все exit_date)
```

### 2.3. Следующее изменение программы

```
next_program_change = первый день из program_change_days[], который > current_day
days_to_program_change = next_program_change - current_day
```

---

## Часть 3: Определение следующего шага

**Формула:**

```
adaptive_days = MIN(
    mp_min_limiter,               // ближайший ресурсный лимит
    min_exit_date - current_day,  // ближайший выход из repair/spawn
    days_to_program_change        // ближайшее изменение программы
)

// Ограничения:
if adaptive_days < 1:  adaptive_days = 1
if adaptive_days > (end_day - current_day):  adaptive_days = end_day - current_day
```

**Переход к следующему дню:**

```
current_day = current_day + adaptive_days
```

**Что происходит после перехода:**

| Если adaptive_days определён | Что происходит |
|------------------------------|----------------|
| **Ресурсным лимитом** | Агент(ы) переходят ops→storage или ops→unsvc |
| **Exit date** | Агент(ы) переходят repair→svc или reserve→ops или unsvc→ops |
| **Program change** | Пересчёт квот (target изменился), возможны promote/demote |

---

## Пример последовательности

```
День 0:
  - mp_min_limiter = 28 (агент A достигнет OH через 28 дней)
  - min_exit_date = 180 (агент B в repair, выйдет через 180)
  - next_program_change = 89
  → adaptive_days = MIN(28, 180, 89) = 28

День 28:
  - Агент A: ops → unserviceable (PPR >= OH)
  - exit_date[A] = 28 + 180 = 208
  - mp_min_limiter = 45 (агент C)
  - min_exit_date = MIN(180, 208) = 180
  - next_program_change = 89
  → adaptive_days = MIN(45, 152, 61) = 61

День 89:
  - Program change: target изменился
  - Квотирование: promote/demote по новому target
  - mp_min_limiter = 12
  - min_exit_date = 91
  → adaptive_days = MIN(12, 91, 28) = 12

...и так далее
```

---

## Схема потока данных

```
┌─────────────────────────────────────────────────────────────────┐
│                    ИНИЦИАЛИЗАЦИЯ (1 раз)                        │
├─────────────────────────────────────────────────────────────────┤
│  program_change_days[] ← flight_program_fl                      │
│  mp5_cumsum[][] ← flight_program_fl                             │
│  агенты.exit_date ← repair_time / spawn_date                    │
│  агенты.limiter ← бинарный поиск по mp5_cumsum                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ЦИКЛ СИМУЛЯЦИИ                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─── ШАГ N ───────────────────────────────────────────────┐   │
│  │                                                          │   │
│  │  1. СБОР МИНИМУМОВ                                       │   │
│  │     mp_min_limiter ← atomicMin(ops.limiter)              │   │
│  │     min_exit_date ← atomicMin(repair/spawn/unsvc.exit)   │   │
│  │     next_pc ← program_change_days[idx > current]         │   │
│  │                                                          │   │
│  │  2. ВЫЧИСЛЕНИЕ adaptive_days                             │   │
│  │     adaptive = MIN(limiter, exit-day, pc-day)            │   │
│  │                                                          │   │
│  │  3. ПРИМЕНЕНИЕ СОБЫТИЙ (что произошло за adaptive дней)  │   │
│  │     - Инкремент SNE/PPR для ops агентов                  │   │
│  │     - Переходы по ресурсам (ops→storage, ops→unsvc)      │   │
│  │     - Выходы из repair/spawn (repair→svc, spawn→ops)     │   │
│  │     - Квотирование (если program change)                 │   │
│  │                                                          │   │
│  │  4. ОБНОВЛЕНИЕ СОСТОЯНИЯ                                 │   │
│  │     current_day += adaptive_days                         │   │
│  │     Пересчёт limiter для новых ops агентов               │   │
│  │                                                          │   │
│  └──────────────────────────────────────────────────────────┘   │
│                              │                                  │
│                              ▼                                  │
│                    current_day < end_day ?                      │
│                         │         │                             │
│                        YES        NO → КОНЕЦ                    │
│                         │                                       │
│                         ▼                                       │
│                    ШАГ N+1                                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Заметки для доработки

<!-- Здесь можно добавить свои заметки -->


