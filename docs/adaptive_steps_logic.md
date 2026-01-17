# Логика определения адаптивных шагов

> **Статус:** Рабочий документ  
> **Дата:** 16-01-2026  
> **Версия:** 4.0

---

## ⚠️ АРХИТЕКТУРНЫЕ РЕШЕНИЯ V8 (КАНОН)

| Вопрос | Решение | Обоснование |
|--------|---------|-------------|
| **Механизм ремонта** | RepairLine.free_days | общий пул, число линий = repair_number из MP |
| **exit_date для unsvc** | УДАЛЁН | Заменён на RepairLine‑проверку |
| **repair_days для unsvc** | НЕ ИСПОЛЬЗУЕТСЯ | unsvc не декрементируется |
| **Правило ресурса** | next-day dt (`SNE + dt >= LL`) | Предотвращение переналёта |
| **limiter=0** | Обязательный выход (иначе EXCEPTION) | Гарантия корректности |
| **unsvc в min_dynamic** | НЕ участвует | Управляется через RepairLine |
| **Инициализация limiter** | Разрешён 0 (убрать max(1,...)) | Консистентность с RTC |

**Отличие от V7:** V8 НЕ эквивалентен V7 по переходам — это осознанное решение

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
**⚠️ Ограничение:** `MAX_DETERMINISTIC_DATES=500` (RTC). Если дат больше, лишние даты **отбрасываются**, события будут потеряны.

---

## Часть 2: Динамические лимитеры агентов

### 2.1. Переменные по состояниям

| Статус | Переменная | Что означает |
|--------|------------|--------------|
| **operations** | `limiter` | Дней до ресурсного лимита (MIN(LL-SNE, OH-PPR)) |
| **repair** | `repair_days` | Дней до конца ремонта |
| **unserviceable** | — | НЕ используется (квота через RepairLine) |
| **остальные** | — | Не участвуют в min_dynamic |

### 2.2. RepairLine (repair_number → число линий)

**Назначение:** Управление квотой ремонта через линии (free_days), общий пул линий, число линий = repair_number из MP

**RepairLine (для каждой линии):**
```
  - free_days += adaptive_days (всегда)
  - прием в ремонт только если free_days >= repair_time
  - при приёме: free_days = 0, aircraft_number = acn (однократно)
```
**Day‑0:** агенты, пришедшие уже в repair, выходят по детерминированной exit‑дате; после day‑0 ремонт идёт только через RepairLine.

**Алгоритм квотирования P2/P3 (QuotaManager):**
```
ВХОД: repair_number (число линий), дефицит ops

1. Готовность агента:
   if current_day < status_change_day + repair_time:
     // Агент ещё не готов к ремонту
     return

2. Отфильтровать RepairLine:
   free_days >= repair_time

3. Определить дефицит = target_ops - current_ops
   approved = MIN(дефицит, available_lines)

4. P2: Отфильтровать unsvc по idx — первые approved
   approved_unsvc = MIN(count(unsvc), approved, available_lines)
   remaining = approved - approved_unsvc
   remaining_lines = available_lines - approved_unsvc

5. P3: Если remaining > 0:
   Отфильтровать inactive по idx — первые remaining
   approved_inactive = MIN(count(inactive), remaining, remaining_lines)

6. P4 (Spawn): Если ещё есть дефицит:
   to_spawn = дефицит - approved_unsvc - approved_inactive

7. При подтверждении линии:
   free_days = 0, aircraft_number = acn (однократно)
```

**Протокол обмена сообщениями (внутри одного шага):**

```
Слой 1: RepairLine → QuotaManager (адресные MessageArray)
  - Каждая линия публикует free_days и aircraft_number

Слой 2: QuotaManager формирует слоты (до P2/P3)
  - Отбирает линии с free_days >= repair_time по типу
  - Сохраняет списки слотов в MacroProperty (Mi-8 / Mi-17)

Слой 3: P2/P3 использует слоты
  - Для rank < needed берёт line_id из списка слотов
```

**⚠️ Сообщения — только адресные (MessageArray), без brute-force.**

### 2.3. Инициализация (день 0)

```
Для агентов в operations:
    limiter = бинарный_поиск_по_mp5_cumsum(LL - SNE, OH - PPR)
    // Возвращает MIN(дней_до_LL, дней_до_OH)

Для агентов в repair:
    repair_days = repair_time - repair_days  // Остаток до выхода
    // repair_time из MP по group_by, repair_days из heli_pandas

Для RepairLine:
    free_days = 1, aircraft_number = 0
```

---

## Часть 3: Цикл вычисления adaptive_days

```
На каждом шаге N:

1. СБОР min_dynamic
   min_dynamic = MIN(
       ops.limiter,           // Все агенты в operations
       repair.repair_days     // Только day‑0 ремонт
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

5. ИНКРЕМЕНТ ЛИНИЙ РЕМОНТА
  RepairLine.free_days += adaptive_days (для всех линий)

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
  PPR = 0                         // Сброс межремонтной наработки
  limiter = 0                     // Не участвует в min_dynamic
  // НЕТ repair_days! Ожидание через RepairLine.free_days
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
  1. Есть RepairLine с free_days >= repair_time
  2. Есть дефицит в программе ops

Если условия НЕ выполнены → переход к spawn (покупка)
```

**⚠️ ВАЖНО: repair_days для unserviceable НЕ используется!**
- Вместо отслеживания repair_days каждого unsvc
- Используем RepairLine (free_days)

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
│  RepairLine.free_days ← 1, aircraft_number ← 0                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ЦИКЛ: ШАГ N                                  │
├─────────────────────────────────────────────────────────────────┤
│  1. min_dynamic = MIN(ops.limiter, repair.repair_days)          │
│     // unsvc НЕ участвует в min_dynamic!                        │
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
│  4b. ИНКРЕМЕНТ ЛИНИЙ РЕМОНТА:                                   │
│     RepairLine.free_days += adaptive_days                       │
│                                                                 │
│  5. ИНКРЕМЕНТЫ (ops только):                                    │
│     ops.sne += dt, ops.ppr += dt                                │
│                                                                 │
│  6. ПЕРЕХОДЫ (проверка на СЛЕДУЮЩИЙ день!):                     │
│     ops(limiter=0): SNE+dt>=LL → storage                        │
│                     PPR+dt>=OH AND SNE+dt>=BR → storage         │
│                     PPR+dt>=OH AND SNE+dt<BR → unsvc            │
│     repair(repair_days=0) → serviceable                         │
│     unsvc → ПРАВО на ops (через RepairLine)                     │
│                                                                 │
│  7. КВОТИРОВАНИЕ:                                               │
│     Демоут если ops > target                                    │
│     P1: serviceable → ops                                       │
│     P2: unsvc → ops (если есть RepairLine.free_days >= repair_time) │
│     P3: inactive → ops (если есть RepairLine.free_days >= repair_time)│
│     P4: spawn (если P2/P3 недоступны)                           │
│     free_days = 0, aircraft_number = acn                        │
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

3. **repair_days используется только для day‑0 ремонта**
   - repair_time берётся из MP по group_by агента
   - day‑0 exit_day = repair_time - repair_days (из heli_pandas)

4. **Один датасет для сравнения**
   - Baseline и Limiter на ОДНОЙ дате
   - Нельзя: baseline на 04.07, limiter на 30.12
