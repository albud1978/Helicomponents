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

Один отсортированный массив всех фиксированных дат:

```
deterministic_dates[] = sorted([
    0,                           # Начало симуляции
    repair_exits[],              # repair_time - repair_days для агентов в repair на загрузке
    spawn_dates[],               # Даты детерминированного spawn
    program_change_days[],       # Даты изменения программы (~92 за 10 лет)
    end_day                      # Последний день (3650)
])
```

**Пример для датасета 2025-07-04:**
```
deterministic_dates = [0, 28, 89, 103, 120, 150, 181, ..., 3649, 3650]
Всего: ~94 даты
```

---

## Часть 2: Динамические лимитеры агентов

### 2.1. Переменные по состояниям

| Статус | Переменная | Что означает |
|--------|------------|--------------|
| **operations** | `limiter` | Дней до ресурсного лимита (MIN(LL-SNE, OH-PPR)) |
| **repair** | `repair_days` | Дней до конца ремонта |
| **unserviceable** | `repair_days` | Дней до права на вход в operations |
| **остальные** | — | Не участвуют в min_dynamic |

### 2.2. Инициализация (день 0)

```
Для агентов в operations:
    limiter = бинарный_поиск_по_mp5_cumsum(LL - SNE, OH - PPR)
    // Возвращает MIN(дней_до_LL, дней_до_OH)

Для агентов в repair:
    repair_days = repair_time - repair_days  // Остаток до выхода

Для агентов в unserviceable (если есть):
    repair_days = repair_time  // 180 дней ожидания
```

---

## Часть 3: Цикл вычисления adaptive_days

```
На каждом шаге N:

1. СБОР min_dynamic
   min_dynamic = MIN(
       ops.limiter,           // Все агенты в operations
       repair.repair_days,    // Все агенты в repair
       unsvc.repair_days      // Все агенты в unserviceable
   )

2. БЛИЖАЙШАЯ ДЕТЕРМИНИРОВАННАЯ ДАТА
   next_det = первая дата из deterministic_dates[] > current_day
   days_to_det = next_det - current_day

3. ВЫЧИСЛЕНИЕ adaptive_days
   adaptive_days = MIN(min_dynamic, days_to_det)

4. ДЕКРЕМЕНТЫ
   Для ops: limiter -= adaptive_days
   Для repair: repair_days -= adaptive_days
   Для unsvc: repair_days -= adaptive_days

5. ОБНОВЛЕНИЕ ДНЯ
   current_day += adaptive_days
```

---

## Часть 4: Обработка событий (limiter=0)

### 4.1. operations с limiter = 0

```
Проверка условий (в порядке приоритета):
  1. SNE >= LL           → storage (списание по ресурсу)
  2. PPR >= OH AND SNE >= BR → storage (ремонт нерентабелен)
  3. PPR >= OH AND SNE < BR  → unserviceable (ждёт ремонта)

При переходе ops → unserviceable:
  repair_days = repair_time (180 дней)
  limiter остаётся 0 до входа в operations
```

### 4.2. repair с repair_days = 0

```
Автоматический переход: repair → serviceable
PPR = 0 (после ремонта)
```

### 4.3. unserviceable с repair_days = 0

```
НЕ автоматический переход!
Агент получает ПРАВО на промоут в operations
Решает QuotaManager по приоритету P2

В квотировании P2:
  Учитывать ТОЛЬКО unsvc с repair_days = 0
```

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
│  unsvc.repair_days ← repair_time                                │
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
│     unsvc.repair_days -= adaptive_days                          │
│                                                                 │
│  5. ИНКРЕМЕНТЫ (ops только):                                    │
│     ops.sne += dt, ops.ppr += dt                                │
│                                                                 │
│  6. ПЕРЕХОДЫ:                                                   │
│     ops(limiter=0 AND условие) → storage/unsvc                  │
│     repair(repair_days=0) → serviceable                         │
│     unsvc(repair_days=0) → ПРАВО на ops                         │
│                                                                 │
│  7. КВОТИРОВАНИЕ:                                               │
│     Демоут если ops > target                                    │
│     P1: serviceable → ops                                       │
│     P2: unsvc(repair_days=0) → ops                              │
│     P3: inactive → ops                                          │
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
| Ожидание ремонта | В repair 180 дней | В unsvc 180 дней |
| Spawn | Динамический (reserve→ops) | Пока не включён |

Эти отличия — архитектурные решения V7, не баги.
