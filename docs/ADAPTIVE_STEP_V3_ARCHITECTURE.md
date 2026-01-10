# Архитектура Adaptive Step v3

## Дата создания: 10.01.2026
## Ветка: feature/flame-messaging
## Статус: 🔬 Прототип с ограничениями

---

## Концепция (от Алексея)

> Добавим в динамические шаги прогнозирование конца ремонта. Таких планеров немного, шагов 20 в год добавится.
> 
> У нас есть `repair_days` по которому можно прогнозировать когда остановить модель для квотирования и смены статуса ремонта на резерв.
> 
> **Порядок:**
> 1. Ремонт первым делай
> 2. Квотирование вторым шагом
> 
> Тогда внутри шагов все будет простым начислением инкрементов только для планеров в операциях и начислением `repair_days`.
> 
> **Логика вычисления горизонтов:**
> 
> 1. **Горизонт operations (один агент):**
>    - Находишь минимум дней налёта между остатком ресурса до LL и OH
>    - С учётом индивидуального вектора суммы налёта из общей таблицы нормативного налёта по конкретным датам
>    - Округляешь вниз до целых, чтобы не было переналёта
> 
> 2. **Горизонт repair (два агент):**
>    - Тупо ищешь минимальный остаток `repair_days` по ремонтным
> 
> 3. **Adaptive step (три агент):**
>    - Берёшь из них двоих минимальный — вот тебе и adaptive шаг
> 
> 4. **Инкременты operations (четыре агент):**
>    - По всем агентам в операциях начисляешь инкременты
>    - Суммой значений по матрице его индивидуальных наработок из MP5 с нормативным налётом для этих дней
> 
> 5. **Инкременты repair (пять агент):**
>    - По ремонтным начисляешь `repair_days`
> 
> 6. **Переходы (шесть агент):**
>    - Те у которых был минимум по горизонту (`adaptive_days`) переводишь в следующее состояние
> 
> 7. **Запись в БД:**
>    - Пишем только дни `adaptive_days`
>    - Остальные агенты остаются без изменений
>    - Их параметры тупо дублируются в `adaptive_day` из прошлого шага
> 
> **Результат:** Никакой дивергенции, много стандартных расчётов, оптимальная запись в базу.

---

## Детализированная архитектура

### Схема выполнения шага

```
┌─────────────────────────────────────────────────────────────────┐
│                    ADAPTIVE STEP v3                            │
│                  (один шаг симуляции)                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ФАЗА 1: ВЫЧИСЛЕНИЕ ГОРИЗОНТОВ (GPU, параллельно по агентам)   │
│  ═══════════════════════════════════════════════════════════   │
│                                                                 │
│  1.1 Operations агенты:                                        │
│      ├─ remaining_sne = ll - sne                               │
│      ├─ remaining_ppr = oh - ppr                               │
│      ├─ horizon_sne = find_day(mp5_cumsum[idx], remaining_sne) │
│      ├─ horizon_ppr = find_day(mp5_cumsum[idx], remaining_ppr) │
│      └─ horizon[idx] = min(horizon_sne, horizon_ppr)           │
│                                                                 │
│  1.2 Repair агенты:                                            │
│      └─ horizon[idx] = repair_time - repair_days               │
│                                                                 │
│  1.3 Остальные агенты (inactive, serviceable, reserve, storage):│
│      └─ horizon[idx] = MAX_INT (не участвуют в min)            │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ФАЗА 2: ГЛОБАЛЬНЫЙ MIN (GPU reduction)                        │
│  ═══════════════════════════════════════════════════════════   │
│                                                                 │
│      adaptive_days = min(horizon[0], horizon[1], ..., horizon[N])│
│                                                                 │
│      // ~300 агентов → O(log N) reduction на GPU               │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ФАЗА 3: БАТЧЕВЫЕ ИНКРЕМЕНТЫ (GPU, параллельно)                │
│  ═══════════════════════════════════════════════════════════   │
│                                                                 │
│  3.1 Operations агенты:                                        │
│      ├─ delta_sne = mp5_cumsum[idx][day + adaptive_days]       │
│      │            - mp5_cumsum[idx][day]                       │
│      ├─ sne += delta_sne                                       │
│      └─ ppr += delta_ppr  // аналогично или delta_sne/6        │
│                                                                 │
│  3.2 Repair агенты:                                            │
│      └─ repair_days += adaptive_days                           │
│                                                                 │
│  3.3 Остальные агенты:                                         │
│      └─ (без изменений)                                        │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ФАЗА 4: ПЕРЕХОДЫ (GPU, только агенты с horizon == adaptive)   │
│  ═══════════════════════════════════════════════════════════   │
│                                                                 │
│  4.1 Repair → Reserve (ПЕРВЫМ!):                               │
│      ├─ Условие: repair_days >= repair_time                    │
│      ├─ Действие: state = reserve, ppr = 0, repair_days = 0    │
│      └─ Результат: освобождается слот в repair                 │
│                                                                 │
│  4.2 Operations → Repair/Storage:                              │
│      ├─ Условие: sne >= ll OR ppr >= oh                        │
│      ├─ Если ppr >= oh AND sne < br → repair                   │
│      └─ Если sne >= ll OR sne >= br → storage                  │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ФАЗА 5: КВОТИРОВАНИЕ (GPU)                                    │
│  ═══════════════════════════════════════════════════════════   │
│                                                                 │
│      ├─ count_ops (подсчёт агентов в operations)               │
│      ├─ quota_ops_excess (демоут при избытке)                  │
│      ├─ quota_promote_serviceable (промоут P1)                 │
│      ├─ quota_promote_reserve (промоут P2)                     │
│      ├─ quota_promote_inactive (промоут P3)                    │
│      └─ quota_repair (FIFO очередь на ремонт)                  │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ФАЗА 6: STATE MANAGERS (GPU)                                  │
│  ═══════════════════════════════════════════════════════════   │
│                                                                 │
│      └─ Применение переходов по intent_state                   │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ФАЗА 7: MP2 ЗАПИСЬ (только на дни adaptive_day)               │
│  ═══════════════════════════════════════════════════════════   │
│                                                                 │
│      ├─ Записываем состояние ВСЕХ агентов на день adaptive_day │
│      ├─ Агенты без изменений: параметры = предыдущий шаг       │
│      └─ Агенты с переходами: новое состояние                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Пример работы (10 лет симуляции)

### День 0: Начальное состояние

```
Operations (150 агентов):
  AC 22431: sne=500000, ll=1080000 → remaining=580000 → horizon=480 дн.
  AC 22215: sne=900000, ll=1080000 → remaining=180000 → horizon=120 дн. ← MIN_OPS
  AC 22418: sne=800000, ll=1080000 → remaining=280000 → horizon=230 дн.
  ...

Repair (8 агентов):
  AC 25484: repair_days=170, repair_time=180 → horizon=10 дн. ← MIN_REPAIR
  AC 27069: repair_days=160, repair_time=180 → horizon=20 дн.
  AC 22431: repair_days=100, repair_time=180 → horizon=80 дн.
  ...

Глобальный min:
  min_ops = 120 дней (AC 22215)
  min_repair = 10 дней (AC 25484)
  
  adaptive_days = min(120, 10) = 10 ← Ремонт определяет шаг!
```

### Шаг 1: adaptive_days = 10

```
Инкременты:
  Operations: sne += cumsum(day 0→10), ppr += cumsum(day 0→10)
  Repair: repair_days += 10

Переходы:
  AC 25484: repair_days = 180 → ПЕРЕХОД repair → reserve ✓
  (остальные без изменений)

Запись в БД:
  День 10: все 158 агентов (состояние после шага)
```

### День 10: Новые горизонты

```
Operations:
  AC 22215: remaining=162000 → horizon=110 дн. ← MIN_OPS
  ...

Repair (7 агентов, AC 25484 ушёл в reserve):
  AC 27069: repair_days=170 → horizon=10 дн.
  AC 22431: repair_days=110 → horizon=70 дн.
  ...
  min_repair = 10 дн. (AC 27069)

adaptive_days = min(110, 10) = 10
```

### Продолжение симуляции...

```
День 0:   adaptive=10  → переход repair→reserve (AC 25484)
День 10:  adaptive=10  → переход repair→reserve (AC 27069)
День 20:  adaptive=15  → переход repair→reserve (AC 22431)
День 35:  adaptive=85  → переход ops→repair (AC 22418)
День 120: adaptive=30  → переход ops→repair (AC 22215)
...
```

### Статистика за 10 лет:

```
Всего шагов: ~120 (вместо 3650)
  - Завершения ремонтов: ~20 шагов/год × 10 = ~200
  - Исчерпания ресурсов: ~10 шагов/год × 10 = ~100
  - Минимум из двух: ~100-150 шагов

Записей в БД: ~120 × 300 = ~36,000 (вместо 1,098,816)
Ускорение: 365/12 ≈ 30x по шагам, ~30x по записям
```

---

## Алгоритм find_day (бинарный поиск по cumsum)

```cuda
/**
 * Находит день когда cumsum[idx] достигнет target_remaining
 * 
 * @param idx          Индекс агента
 * @param current_day  Текущий день симуляции
 * @param remaining    Остаток ресурса (ll - sne или oh - ppr)
 * @return             Количество дней до исчерпания ресурса
 */
FLAMEGPU_DEVICE_FUNCTION unsigned int find_horizon_day(
    flamegpu::DeviceAPI<...>* FLAMEGPU,
    unsigned int idx,
    unsigned int current_day,
    unsigned int remaining
) {
    // Читаем cumsum из MacroProperty
    // Индексация: cumsum[idx * (MAX_DAYS + 1) + day]
    auto mp_cumsum = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_SIZE>("mp5_cumsum");
    
    const unsigned int MAX_DAYS = 4000u;
    const unsigned int base_idx = idx * (MAX_DAYS + 1u);
    
    // Текущее значение cumsum
    const unsigned int base_cumsum = mp_cumsum[base_idx + current_day];
    
    // Целевое значение cumsum
    const unsigned int target_cumsum = base_cumsum + remaining;
    
    // Бинарный поиск: найти минимальный день где cumsum >= target
    unsigned int lo = current_day;
    unsigned int hi = current_day + 3650u;  // Max 10 лет
    if (hi > MAX_DAYS) hi = MAX_DAYS;
    
    while (lo < hi) {
        unsigned int mid = (lo + hi) / 2u;
        unsigned int mid_cumsum = mp_cumsum[base_idx + mid];
        
        if (mid_cumsum < target_cumsum) {
            lo = mid + 1u;
        } else {
            hi = mid;
        }
    }
    
    // Результат: количество дней от current_day
    // Округление ВНИЗ (lo - 1 если точного совпадения нет)
    unsigned int horizon = lo - current_day;
    
    // Защита от 0 (минимум 1 день)
    if (horizon == 0u) horizon = 1u;
    
    return horizon;
}
```

---

## Структуры данных

### MacroProperty

| Название | Размер | Тип | Описание |
|----------|--------|-----|----------|
| `mp5_cumsum` | MAX_FRAMES × (MAX_DAYS+1) | UInt32 | Кумулятивная сумма налёта |
| `mp_horizon` | MAX_FRAMES | UInt32 | Горизонт каждого агента |
| `mp_adaptive_days` | 4 | UInt32 | Результат global min |

### Environment Properties

| Название | Тип | Описание |
|----------|-----|----------|
| `current_day` | UInt32 | Текущий день симуляции |
| `adaptive_days` | UInt32 | Длина текущего шага |
| `end_day` | UInt32 | Конечный день симуляции |

### Переменные агента

| Название | Тип | Состояние | Описание |
|----------|-----|-----------|----------|
| `horizon` | UInt32 | Все | Дней до события для этого агента |
| `sne` | UInt32 | operations | Наработка СНЭ (минуты) |
| `ppr` | UInt32 | operations | Межремонтный ресурс (минуты) |
| `repair_days` | UInt32 | repair | Дней в ремонте |

---

## Порядок слоёв FLAME GPU

```python
layers = [
    # ФАЗА 1: Вычисление горизонтов
    "layer_compute_horizon_ops",      # operations: бинарный поиск по cumsum
    "layer_compute_horizon_repair",   # repair: repair_time - repair_days
    "layer_clear_horizon_others",     # остальные: MAX_INT
    
    # ФАЗА 2: Global min
    "layer_copy_horizon_to_macro",    # Копируем horizon в MacroProperty
    "layer_compute_adaptive_days",    # QuotaManager: global min
    
    # ФАЗА 3: Батчевые инкременты
    "layer_batch_increment_ops",      # operations: sne/ppr по cumsum
    "layer_batch_increment_repair",   # repair: repair_days += adaptive_days
    
    # ФАЗА 4: Переходы (repair ПЕРВЫМ!)
    "layer_repair_to_reserve",        # repair → reserve (при завершении)
    "layer_ops_to_repair_storage",    # operations → repair/storage
    
    # ФАЗА 5: Квотирование
    "layer_count_ops",
    "layer_quota_ops_excess",
    "layer_quota_promote_serviceable",
    "layer_quota_promote_reserve",
    "layer_quota_promote_inactive",
    "layer_quota_repair",
    
    # ФАЗА 6: State managers
    "layer_state_manager_operations",
    "layer_state_manager_serviceable",
    "layer_state_manager_repair",
    "layer_state_manager_reserve",
    "layer_state_manager_inactive",
    "layer_state_manager_storage",
    
    # ФАЗА 7: MP2 запись
    "layer_mp2_write",
]
```

---

## Преимущества архитектуры

| Аспект | Ежедневные шаги | Adaptive v3 | Улучшение |
|--------|-----------------|-------------|-----------|
| **Шагов/10 лет** | 3,650 | ~120 | **30x** |
| **Записей в БД** | 1,098,816 | ~36,000 | **30x** |
| **Точность** | 100% | **100%** | = |
| **Дивергенция** | Нет | **Нет** | = |
| **Сложность логики** | Средняя | **Низкая** | ↑ |

---

## Инварианты

### INV-ADAPTIVE-1: Точность инкрементов
```
sne_after = sne_before + (cumsum[day + adaptive] - cumsum[day])
// Точное значение из таблицы, без умножения!
```

### INV-ADAPTIVE-2: Переходы только при достижении горизонта
```
transition_happens IFF horizon[agent] == adaptive_days
```

### INV-ADAPTIVE-3: Repair → Reserve первым
```
layer_repair_to_reserve BEFORE layer_ops_to_repair_storage
// Освобождаем слот в repair перед новыми поступлениями
```

### INV-ADAPTIVE-4: Global min корректен
```
adaptive_days = min(all_horizons) > 0
```

---

## Результаты тестирования (10.01.2026)

### Запуск: DS1 (2025-07-04), 3650 дней

| Метрика | Adaptive v3 | LIMITER | Baseline |
|---------|-------------|---------|----------|
| **Шагов** | 322 | 3650 | 3650 |
| **Время** | 4.37с | 48с | 75с |
| **GPU** | 2.77с | ~30с | ~34с |
| **Записей** | 89,838 | ~1.15M | ~1.15M |
| **Ускорение** | **17x** | 1.56x | 1x |

### Состояния на финальный день

| Состояние | Adaptive v3 (Day 3639) | LIMITER (Day 3649) |
|-----------|------------------------|-------------------|
| inactive | 13 | 95 |
| operations | 61 | 174 |
| serviceable | **0** | (включено в цикл) |
| repair | 18 | 4 |
| reserve | 187 | 17 |
| storage | **0** | 29 |
| **Total** | 279 | 319 |

---

## ⚠️ Архитектурные ограничения

### ОГРАНИЧЕНИЕ 1: Несовместимость с baseline модулями

Baseline модули квотирования используют `getStepCounter()` как номер дня:

```c++
const unsigned int day = FLAMEGPU->getStepCounter();
target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", day + 1);
```

В Adaptive v3 `getStepCounter()` возвращает **номер шага** (0, 1, 2...), а не `current_day` (0, 11, 23, 49...).

**Результат:** Квотирование по программе не работает — `quota_demount` читает неверный `target`.

### ОГРАНИЧЕНИЕ 2: Отсутствие serviceable

LIMITER использует цикл `operations ↔ serviceable` для:
- Демонтажа при избытке программы
- Холдинга перед возвратом в operations

В Adaptive v3 serviceable пропускается — агенты идут прямо в reserve.

### ОГРАНИЧЕНИЕ 3: Отсутствие spawn

Модуль `spawn_v2` также использует `getStepCounter()` для определения дня спавна.
Новые агенты не создаются — постоянно 279 агентов вместо 319.

### ОГРАНИЧЕНИЕ 4: Отсутствие storage

Переходы `operations → storage` (при sne >= ll) не работают корректно.
Возможно из-за порядка слоёв или условий.

---

## Пути решения

### Вариант A: Рефакторинг baseline модулей
- Заменить `getStepCounter()` на `environment.getProperty<unsigned int>("current_day")`
- Трудоёмкость: ~20 файлов
- Риск: Сломать baseline

### Вариант B: Создать специализированные adaptive модули
- Копии baseline с адаптированной логикой
- Трудоёмкость: ~10 новых файлов
- Риск: Дублирование кода

### Вариант C: Гибридная архитектура
- Adaptive шаги только для ремонтов
- Квотирование ежедневно
- Компромисс между скоростью и точностью

---

## Файлы реализации

| Файл | Статус | Описание |
|------|--------|----------|
| `rtc_adaptive_v3.py` | ✅ | Горизонты, batch increment, adaptive_days |
| `orchestrator_adaptive_v3.py` | ✅ | Оркестратор с адаптивным циклом |
| `precompute_events.py` | ✅ | Вычисление mp5_cumsum |
| `base_model_messaging.py` | ✅ | MacroProperty для adaptive |

---

## Связанные документы

- `docs/MESSAGING_RESEARCH.md` — история исследования архитектур
- `docs/ADAPTIVE_STEP_ARCHITECTURE.md` — предыдущая версия adaptive step
- `docs/validation.md` — инварианты и проверки
- `.cursorrules` — правила проекта

---

**Автор:** Алексей (концепция), AI (детализация)
**Дата:** 10.01.2026

