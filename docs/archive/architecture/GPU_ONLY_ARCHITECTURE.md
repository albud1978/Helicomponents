# Архитектура: Полностью GPU-side симуляция

## Текущие ограничения FLAME GPU

| Операция | CPU (HostFunction) | GPU (Agent Function) |
|----------|-------------------|---------------------|
| Environment Property read | ✅ | ✅ |
| Environment Property write | ✅ | ❌ |
| MacroProperty read | ✅ | ✅ |
| MacroProperty atomic write | ✅ | ✅ (atomicAdd, exchange) |
| MacroProperty atomicMin | ? | ✅ (CUDA native) |
| Exit condition | ✅ | ❌ |

## Проблема

В текущей архитектуре:
```python
# Python (CPU) — вызывается 173 раза
for step in range(max_steps):
    step_days = compute_step_days(current_day)  # CPU
    simulation.setEnvironmentProperty("step_days", step_days)  # CPU→GPU
    simulation.step()  # GPU
    current_day += step_days
```

**173 переключения CPU↔GPU** даже при 970x ускорении!

## Решение: MacroProperty + Минимальный HostFunction

### Архитектура

```
┌─────────────────────────────────────────────────────────────┐
│  CPU: Только загрузка и выгрузка                            │
├─────────────────────────────────────────────────────────────┤
│  1. Загрузка env_data, mp3, mp5_cumsum                      │
│  2. simulation.simulate(MAX_STEPS)                          │
│  3. Выгрузка результатов в СУБД                             │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  GPU: Вся логика симуляции                                  │
├─────────────────────────────────────────────────────────────┤
│  MacroProperty:                                             │
│    mp_current_day[1]     — текущий день                     │
│    mp_step_days[1]       — длина шага                       │
│    mp_min_limiter[1]     — минимальный лимитер              │
│    mp_program_days[1]    — дней до программы                │
│                                                             │
│  Layer 1: rtc_compute_limiters (все агенты)                 │
│    → atomicMin(mp_min_limiter, my_limiter)                  │
│                                                             │
│  Layer 2: rtc_set_step_days (1 агент QuotaManager)          │
│    → mp_step_days = min(mp_min_limiter, mp_program_days)    │
│                                                             │
│  Layer 3: rtc_batch_increment (все агенты)                  │
│    → sne += cumsum[day+step] - cumsum[day]                  │
│                                                             │
│  Layer 4: rtc_update_day (1 агент)                          │
│    → mp_current_day += mp_step_days                         │
│    → mp_min_limiter = MAX_INT (reset)                       │
└─────────────────────────────────────────────────────────────┘
```

### RTC: Вычисление лимитеров (GPU)

```cuda
FLAMEGPU_AGENT_FUNCTION(rtc_compute_limiters, ...) {
    // Только для агентов в operations
    auto mp_min = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("mp_min_limiter");
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("mp_current_day");
    
    const unsigned int current_day = mp_day[0];
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int remaining = ll - sne;
    
    // Ищем день, когда cumsum достигнет remaining
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    auto cumsum = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_SIZE>("mp5_cumsum");
    
    unsigned int my_limiter = 365;  // MAX
    const unsigned int base = idx * (DAYS + 1);
    const unsigned int start = cumsum[base + current_day];
    
    for (unsigned int d = 1; d <= 365 && current_day + d < DAYS; d++) {
        if (cumsum[base + current_day + d] - start >= remaining) {
            my_limiter = d;
            break;
        }
    }
    
    // atomicMin — обновляем глобальный минимум
    atomicMin(&mp_min[0], my_limiter);
    
    return flamegpu::ALIVE;
}
```

### Проблема: Exit Condition

FLAME GPU **не поддерживает** остановку симуляции из device-side!

**Варианты:**

1. **Фиксированное количество шагов** — не идеально
   ```python
   simulation.simulate(500)  # MAX возможных шагов
   ```

2. **Минимальный HostFunction** — только проверка exit
   ```python
   class MinimalExitCheck(fg.HostFunction):
       def run(self, FLAMEGPU):
           current_day = FLAMEGPU.environment.getMacroPropertyUInt("mp_current_day", 0)
           if current_day >= end_day:
               FLAMEGPU.setExitCondition()  # Если есть такой API
   ```

3. **Агент-sentinel** — устанавливает флаг в MacroProperty
   ```cuda
   // Последний слой каждого шага
   if (mp_current_day[0] >= END_DAY) {
       mp_simulation_done[0] = 1;
   }
   ```
   Но всё равно нужен HostFunction для проверки этого флага!

## Реалистичная архитектура

```
┌─────────────────────────────────────────────────────────────┐
│  CPU (минимум):                                             │
│  • Загрузка данных (1 раз)                                  │
│  • HostFunction: проверка exit (~1мкс/шаг)                  │
│  • HostFunction: опционально логирование                    │
│  • Выгрузка результатов (1 раз)                             │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  GPU (99.99% времени):                                      │
│  • Все вычисления лимитеров                                 │
│  • Все батчевые инкременты                                  │
│  • Всё квотирование                                         │
│  • Все state transitions                                    │
└─────────────────────────────────────────────────────────────┘
```

## Ожидаемый прирост

| Подход | CPU↔GPU переключений | Время |
|--------|---------------------|-------|
| Текущий (Python loop) | 173 | ~0.39с |
| GPU-only + exit check | 173 (но ~1мкс каждое) | ~0.35с |
| Теоретический максимум | 1 (загрузка+выгрузка) | ~0.30с |

Прирост: **~10-15%** — не критично при текущих 0.39с.

## Вывод

1. **Полностью GPU-only невозможно** в FLAME GPU из-за exit condition
2. **Можно минимизировать CPU** до проверки условия выхода
3. **Основной выигрыш уже достигнут** — 970x от адаптивного шага
4. **Дополнительная оптимизация** даст ~10-15%, не критично

## Рекомендация

Текущая архитектура с Python loop достаточно эффективна:
- 173 шага × ~2мс overhead = ~0.35с overhead
- GPU время: ~0.04с
- Общее: ~0.39с

Переход на GPU-only даст ~0.30-0.35с — **несущественно**.

---
**Вывод:** Оставить текущую архитектуру. 970x ускорение достаточно.

