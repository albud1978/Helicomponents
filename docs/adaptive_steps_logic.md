# Логика определения адаптивных шагов

> **Статус:** Рабочий документ  
> **Дата:** 15-01-2026  
> **Версия:** 3.0

---

## Эталон из Baseline

| Метрика | Значение |
|---------|----------|
| Всего ресурсных переходов | 204 |
| Уникальных дней с переходами | **183** |
| ops→repair (PPR >= OH) | 173 перехода, 155 дней |
| ops→storage (SNE >= BR/LL) | 31 переход, 31 день |

**Важно:** В baseline при PPR >= OH агент идёт в **repair**, в V8 — в **unserviceable**.

---

## Часть 1: Детерминированные даты (MacroProperty)

Один отсортированный массив всех фиксированных дат.

**⚠️ ВСЕ даты — в днях симуляции (0, 1, 2, ..., end_day)!**

```
deterministic_dates[] = sorted([
    0,                                    # День 0 симуляции
    repair_exits[],                       # День выхода из repair (см. формулу ниже)
    spawn_dates[],                        # День spawn (индекс из mp4_new_counter)
    program_change_days[],                # День изменения программы (индекс)
    end_day                               # Последний день (3650)
])
```

**Формулы для repair_exits (день симуляции):**
```
Для каждого агента в repair на загрузке:
  repair_time = 180 (или из MP по group_by)
  repair_days = X   (сколько дней уже в ремонте, из heli_pandas)
  
  exit_day = repair_time - repair_days
  // Это ДЕНЬ СИМУЛЯЦИИ когда агент выйдет из repair
  
Пример:
  repair_time=180, repair_days=30
  exit_day = 180 - 30 = 150
  // На день 150 симуляции агент выйдет из repair
```

**⚠️ ВАЖНО: Сравнение baseline и limiter ТОЛЬКО на одном датасете!**
- Если baseline на 2025-07-04, то limiter тоже на 2025-07-04
- Нельзя смешивать датасеты (04.07 vs 30.12)

**Пример для датасета 2025-07-04:**
```
deterministic_dates = [0, 28, 89, 103, 120, 150, 181, ..., 3649, 3650]
Всего: ~94 даты (все — дни симуляции)
```

---

## Часть 2: Динамические лимитеры агентов

### 2.1. Переменные по состояниям

| Статус | Переменная | Что означает |
|--------|------------|--------------|
| **operations** | `limiter` | Дней до ресурсного лимита (MIN(LL-SNE, OH-PPR)) |
| **repair** | `repair_days` | Дней до конца ремонта |
| **unserviceable** | — | НЕ используется (квота через RepairAgent) |
| **остальные** | — | Не участвуют в min_dynamic |

### 2.2. RepairAgent — агент ремонтной мощности

**Назначение:** Управление квотой ремонта через счётчик агрегато-дней

| Переменная | Тип | Описание |
|------------|-----|----------|
| `capacity` | UInt32 | Накопленные агрегато-дни доступные для ремонта |
| `repair_quota` | UInt16 | Дневная квота ремонта (кол-во слотов) |

**Инициализация (день 0):**
```
capacity = repair_quota - count(repair)
// Разница между квотой и агентами уже в repair
```

**Инкремент (каждый шаг):**
```
capacity += (repair_quota - count(repair))
// Инкремент на разницу (свободные слоты за текущий день)
```

**Роли агентов:**
```
RepairAgent:
  - Накапливает мощность (capacity)
  - Выдаёт capacity на квотирование
  - Списывает по команде QuotaManager

QuotaManager:
  - Проверяет условия
  - Принимает решения о переводах
  - Отправляет команду списания
```

**Алгоритм квотирования P2/P3 (QuotaManager):**
```
ВХОД: capacity от RepairAgent, дефицит ops

1. Проверка условия времени:
   if current_day < repair_time:
     // Недостаточно времени от начала симуляции
     // P2/P3 недоступны → сразу к spawn
     return

2. Определить slots = floor(capacity / repair_time)
   if slots == 0:
     // Нет ремонтной мощности → к spawn
     return

3. Определить дефицит = target_ops - current_ops
   approved = MIN(дефицит, slots)

4. P2: Отфильтровать unsvc по idx (mfg_date) — первые approved
   approved_unsvc = MIN(count(unsvc), approved)
   remaining = approved - approved_unsvc

5. P3: Если remaining > 0:
   Отфильтровать inactive по idx — первые remaining
   approved_inactive = MIN(count(inactive), remaining)

6. P4 (Spawn): Если ещё есть дефицит:
   to_spawn = дефицит - approved_unsvc - approved_inactive

7. Отправить RepairAgent команду списания:
   to_deduct = (approved_unsvc + approved_inactive) * repair_time
```

**Протокол обмена сообщениями (внутри одного шага):**

```
Слой 1: RepairAgent → QuotaManager (адресно)
  - RepairAgent считает capacity и отправляет доступную мощность
  - Сообщение: { capacity, slots = floor(capacity / repair_time) }

Слой 2: QuotaManager принимает решение
  - Получает capacity от RepairAgent
  - Считает дефицит ops, одобряет unsvc/inactive по idx
  - Определяет to_deduct = approved * repair_time

Слой 3: QuotaManager → RepairAgent (адресно)
  - Отправляет команду списания: { to_deduct }

Слой 4: RepairAgent списывает
  - capacity -= to_deduct
```

**⚠️ Адресные сообщения — НЕ brute-force!**

### 2.3. Инициализация (день 0)

```
Для агентов в operations:
    limiter = бинарный_поиск_по_mp5_cumsum(LL - SNE, OH - PPR)
    // Возвращает MIN(дней_до_LL, дней_до_OH)

Для агентов в repair:
    repair_days = repair_time - repair_days  // Остаток до выхода
    // repair_time из MP по group_by, repair_days из heli_pandas

Для RepairAgent:
    capacity = repair_quota - count(repair)
    // Начальная мощность = свободные слоты
```

---

## Часть 3: Цикл вычисления adaptive_days

```
На каждом шаге N:

1. СБОР min_dynamic
   min_dynamic = MIN(
       ops.limiter,           // Все агенты в operations
       repair.repair_days     // Все агенты в repair
   )
   // unsvc НЕ участвует в min_dynamic!

2. БЛИЖАЙШАЯ ДЕТЕРМИНИРОВАННАЯ ДАТА
   next_det = первая дата из deterministic_dates[] > current_day
   days_to_det = next_det - current_day

3. ВЫЧИСЛЕНИЕ adaptive_days
   adaptive_days = MIN(min_dynamic, days_to_det)

4. ДЕКРЕМЕНТЫ
   Для ops: limiter -= adaptive_days
   Для repair: repair_days -= adaptive_days
   // unsvc НЕ декрементируется!

5. ИНКРЕМЕНТ РЕМОНТНОЙ МОЩНОСТИ
   RepairAgent.capacity += (repair_quota - count(repair))

6. ОБНОВЛЕНИЕ ДНЯ
   current_day += adaptive_days
```

---

## Часть 4: Обработка событий (limiter=0)

### 4.1. operations с limiter = 0

**⚠️ ГАРАНТИЯ: если limiter = 0, ОБЯЗАТЕЛЬНО выполняется одно из условий выхода!**

```
Проверка условий НА СЛЕДУЮЩИЙ ДЕНЬ (в порядке приоритета):
  1. SNE + dt >= LL           → storage (списание по ресурсу)
  2. PPR + dt >= OH AND SNE + dt >= BR → storage (ремонт нерентабелен)
  3. PPR + dt >= OH AND SNE + dt < BR  → unserviceable (ждёт ремонта)

где dt = налёт СЛЕДУЮЩЕГО дня (чтобы избежать переналёта!)

При переходе ops → unserviceable:
  repair_days = repair_time - 1   // НЕ хардкод 180!
  // repair_time берётся из MP по group_by агента
  // Минус 1 потому что декремент идёт до 0, а не до 1
  limiter остаётся 0 до входа в operations
```

### 4.2. repair с repair_days = 0

```
Автоматический переход: repair → serviceable
PPR = 0 (после ремонта)
```

### 4.3. unserviceable — ожидание промоута

```
НЕ автоматический переход!
Агент в unserviceable ожидает решения QuotaManager

Переход unsvc → ops возможен при:
  1. current_day >= repair_time (прошло достаточно времени от начала симуляции)
  2. RepairAgent.capacity >= repair_time (есть ремонтная мощность)
  3. Есть дефицит в программе ops

Если условия НЕ выполнены → переход к spawn (покупка)
```

**⚠️ ВАЖНО: repair_days для unserviceable НЕ используется!**
- Вместо отслеживания repair_days каждого unsvc
- Используем RepairAgent с общим счётчиком мощности

### 4.4. Вход в operations

```
ТОЛЬКО для агента, который ВХОДИТ в operations:
  limiter = бинарный_поиск_по_mp5_cumsum(LL - SNE, OH - PPR)

НЕ пересчитывать для всех агентов в operations!
```

---

## Часть 5: Текущие результаты V8

### Конфигурация прогона
- Датасет: 2025-07-04
- end_day: 3650
- State transitions: V7 (ops→storage, ops→unsvc, repair→svc, P1/P2/P3)
- Spawn: НЕ включён

### Результаты
| Метрика | Baseline | V8 |
|---------|----------|-----|
| Всего шагов | 3650 | 987 |
| Время GPU | ~80с* | 4.7с |
| Ускорение | 1x | ~17x* |

### Итоговые состояния (day 3649/3650)
| Состояние | Baseline | V8 | Примечание |
|-----------|----------|-----|------------|
| operations | 163 | 138 | V8 без spawn |
| inactive | 93 | 94 | |
| storage | 31 | 37 | Похоже |
| repair | 8 | 0 | V8: unsvc вместо repair |
| unserviceable | 0 | 10 | V8 логика |
| serviceable | 8 | 0 | |
| reserve | 14 | 0 | V8 без spawn |
| **TOTAL** | **317** | **279** | Разница из-за spawn |

### Причины шагов V8
- deterministic: 93 (program changes + end_day)
- limiter: 922 (ресурсные события)
- end_day: 2

---

## Часть 6: Валидация

### Что сравнивать

| Метрика | Baseline | V8 |
|---------|----------|-----|
| Ресурсные переходы | ops→repair/storage | ops→unsvc/storage |
| Уникальных дней | **183** | **?** |

### Проблема текущей метрики

V8 считает **922 "limiter шага"** — это шаги где adaptive_days определился по limiter.
Но это НЕ то же самое что **дни с переходами**.

Один шаг может не иметь переходов (limiter близок, но агент остаётся в ops).
Несколько переходов могут произойти в один день.

### Правильная метрика

Нужно считать **уникальные дни** когда хотя бы один агент сделал переход ops→storage или ops→unsvc.

Для этого нужно добавить логирование переходов в V8.

---

## Часть 7: TODO

- [x] Собрать эталон из baseline (183 дня)
- [x] Реализовать V8 с декрементом limiter/repair_days
- [x] Интегрировать V7 state transitions
- [ ] Добавить логирование дней переходов
- [ ] Сравнить уникальные дни переходов V8 vs baseline (183)
- [ ] Добавить spawn и циклы P2
- [ ] Финальная валидация

---

## Схема (финальная)

```
┌─────────────────────────────────────────────────────────────────┐
│                    ИНИЦИАЛИЗАЦИЯ                                │
├─────────────────────────────────────────────────────────────────┤
│  deterministic_dates[] ← [0, repair_exits, spawns, PC, end_day] │
│  ops.limiter ← бинарный_поиск(LL-SNE, OH-PPR)                   │
│  repair.repair_days ← остаток до выхода                         │
│  RepairAgent.capacity ← repair_quota - count(repair)            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ЦИКЛ: ШАГ N                                  │
├─────────────────────────────────────────────────────────────────┤
│  1. min_dynamic = MIN(ops.limiter, repair.repair_days,          │
│                       unsvc.repair_days)                        │
│                                                                 │
│  2. days_to_det = next(deterministic_dates) - current_day       │
│                                                                 │
│  3. adaptive_days = MIN(min_dynamic, days_to_det)               │
│                                                                 │
│  4. ДЕКРЕМЕНТ:                                                  │
│     ops.limiter -= adaptive_days                                │
│     repair.repair_days -= adaptive_days                         │
│     // unsvc НЕ декрементируется!                               │
│                                                                 │
│  4b. ИНКРЕМЕНТ РЕМОНТНОЙ МОЩНОСТИ:                              │
│     RepairAgent.capacity += (quota - count(repair))             │
│                                                                 │
│  5. ИНКРЕМЕНТЫ (ops только):                                    │
│     ops.sne += dt, ops.ppr += dt                                │
│                                                                 │
│  6. ПЕРЕХОДЫ (проверка на СЛЕДУЮЩИЙ день!):                     │
│     ops(limiter=0): SNE+dt>=LL → storage                        │
│                     PPR+dt>=OH AND SNE+dt>=BR → storage         │
│                     PPR+dt>=OH AND SNE+dt<BR → unsvc            │
│     repair(repair_days=0) → serviceable                         │
│     unsvc(repair_days=0) → ПРАВО на ops                         │
│                                                                 │
│  7. КВОТИРОВАНИЕ:                                               │
│     Демоут если ops > target                                    │
│     P1: serviceable → ops                                       │
│     P2: unsvc → ops (если capacity >= repair_time)              │
│     P3: inactive → ops (если capacity >= repair_time)           │
│     P4: spawn (если P2/P3 недоступны)                           │
│     RepairAgent.capacity -= approved * repair_time              │
│                                                                 │
│  8. ВХОД В OPS:                                                 │
│     new_ops.limiter = бинарный_поиск(...)                       │
│                                                                 │
│  9. current_day += adaptive_days                                │
└─────────────────────────────────────────────────────────────────┘
```

---

## Архитектурные отличия V8 vs Baseline

| Аспект | Baseline | V8 |
|--------|----------|-----|
| PPR >= OH | → repair | → unserviceable |
| Цикл агента | ops→repair→serviceable→ops | ops→unsvc→ops |
| Ожидание ремонта | В repair 180 дней | В unsvc repair_time-1 дней |
| Spawn | Динамический (reserve→ops) | Пока не включён |

Эти отличия — архитектурные решения V7, не баги.

---

## ⚠️ КРИТИЧЕСКИЕ ТРЕБОВАНИЯ

1. **Проверка ресурса на СЛЕДУЮЩИЙ день**
   - Условие: `SNE + dt >= LL` (не `SNE >= LL`)
   - dt = налёт следующего дня
   - Иначе возможен переналёт!

2. **Гарантия выхода при limiter = 0**
   - Если limiter = 0, ОБЯЗАТЕЛЬНО выполняется условие выхода
   - Ситуация `limiter=0 AND агент остаётся в ops` — **ОШИБКА!**
   - При обнаружении → выбросить exception, остановить симуляцию

3. **repair_days = repair_time - 1**
   - Не хардкод 180!
   - repair_time берётся из MP по group_by агента
   - Минус 1 потому что декремент до 0, а не до 1

4. **Один датасет для сравнения**
   - Baseline и Limiter на ОДНОЙ дате
   - Нельзя: baseline на 04.07, limiter на 30.12
