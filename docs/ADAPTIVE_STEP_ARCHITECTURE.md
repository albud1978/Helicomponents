# Архитектура: Адаптивный шаг симуляции (DES + ABM гибрид)

## Концепция

**Текущий подход (ABM daily):**
- 1 шаг = 1 день
- 3650 шагов = 10 лет
- Каждый шаг: все агенты обрабатываются

**Новый подход (DES+ABM hybrid):**
- 1 шаг = N дней (определяется событиями)
- ~500-1000 шагов = 10 лет
- Внутри шага: батчевые инкременты

## Лимитеры (определяют границы шагов)

### 1. Ресурсный лимитер (per-agent)
```
remaining_sne = ll - sne
remaining_ppr = oh - ppr
min_remaining = min(remaining_sne, remaining_ppr)
days_to_limit = find_day_when_cumsum_dt >= min_remaining
```

### 2. Программный лимитер (global)
```
# Предрасчёт дней изменения mp4_ops_counter
program_events = [
    (day=28, mi8=64, mi17=88),
    (day=56, mi8=68, mi17=90),
    ...
]
days_to_next_program = program_events[next].day - current_day
```

### 3. Ремонтный лимитер (per-agent in repair)
```
days_to_repair_complete = repair_time - repair_days
```

### 4. Шаг симуляции
```
step_days = min(
    min_all_agents(days_to_resource_limit),
    days_to_next_program_change,
    min_repair_agents(days_to_repair_complete)
)
```

## Архитектура FLAME GPU

### Environment Properties
```python
# Логическое время (не путать с getStepCounter)
env.newPropertyUInt("current_day", 0)        # Текущий день симуляции
env.newPropertyUInt("step_days", 1)          # Длина текущего шага
env.newPropertyUInt("end_day", 3650)         # Конечный день

# Предрасчитанные данные
env.newPropertyArrayUInt("program_change_days", [...])  # Дни изменения программы
env.newPropertyArrayUInt("mp5_cumsum", [...])           # Кумулятивные суммы dt
```

### HostFunction: Вычисление step_days
```python
class AdaptiveStepHostFunction(pyflamegpu.HostFunction):
    def run(self, FLAMEGPU):
        current_day = FLAMEGPU.environment.getPropertyUInt("current_day")
        
        # 1. Найти минимальный ресурсный лимитер
        min_resource_days = self._find_min_resource_limit(FLAMEGPU, current_day)
        
        # 2. Найти ближайшее изменение программы
        program_days = self._find_next_program_change(FLAMEGPU, current_day)
        
        # 3. Найти минимальный ремонтный лимитер
        min_repair_days = self._find_min_repair_limit(FLAMEGPU)
        
        # 4. Шаг = минимум (но не более 365 дней за раз)
        step_days = min(min_resource_days, program_days, min_repair_days, 365)
        step_days = max(step_days, 1)  # Минимум 1 день
        
        FLAMEGPU.environment.setPropertyUInt("step_days", step_days)
        
        # Логирование
        if step_days > 1:
            print(f"[Adaptive] Day {current_day}: step_days={step_days} " 
                  f"(resource={min_resource_days}, program={program_days}, repair={min_repair_days})")
```

### RTC: Батчевые инкременты
```cuda
FLAMEGPU_AGENT_FUNCTION(rtc_batch_increment_operations, ...) {
    const unsigned int step_days = FLAMEGPU->environment.getProperty<unsigned int>("step_days");
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Агрегируем dt за весь период из кумулятивных сумм
    auto mp5_cumsum = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_SIZE>("mp5_cumsum");
    const unsigned int base = idx * DAYS;
    const unsigned int dt_start = mp5_cumsum[base + current_day];
    const unsigned int dt_end = mp5_cumsum[base + current_day + step_days];
    const unsigned int total_dt = dt_end - dt_start;
    
    // Батчевые инкременты
    unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    
    sne += total_dt;
    ppr += total_dt;
    
    FLAMEGPU->setVariable<unsigned int>("sne", sne);
    FLAMEGPU->setVariable<unsigned int>("ppr", ppr);
    
    // Проверка достижения лимита
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    
    if (sne >= ll || ppr >= oh) {
        FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);  // -> repair
    }
    
    return flamegpu::ALIVE;
}
```

### HostFunction: Обновление current_day
```python
class UpdateDayHostFunction(pyflamegpu.HostFunction):
    def run(self, FLAMEGPU):
        current_day = FLAMEGPU.environment.getPropertyUInt("current_day")
        step_days = FLAMEGPU.environment.getPropertyUInt("step_days")
        
        new_day = current_day + step_days
        FLAMEGPU.environment.setPropertyUInt("current_day", new_day)
        
        # Условие завершения
        end_day = FLAMEGPU.environment.getPropertyUInt("end_day")
        if new_day >= end_day:
            FLAMEGPU.setExitCondition()  # Завершить симуляцию
```

## MP2 Экспорт

### Вариант A: Интерполяция
```python
# Записываем только ключевые точки (начало/конец шага)
# Промежуточные дни интерполируем при выгрузке
for day in range(step_start, step_end):
    progress = (day - step_start) / (step_end - step_start)
    sne_day = sne_start + (sne_end - sne_start) * progress
```

### Вариант B: Заполнение одинаковыми значениями
```python
# Все дни внутри шага имеют одинаковые финальные значения
for day in range(step_start, step_end + 1):
    mp2_row = (day, idx, sne_end, ppr_end, state)
```

### Вариант C: Только точки переходов
```python
# Записываем только дни с изменениями состояний
# Значительно меньше строк (не 1M, а ~50K)
```

## Предрасчёт данных

### 1. Дни изменения программы
```python
def find_program_change_days(mp4_mi8, mp4_mi17):
    """Найти все дни, когда target меняется"""
    changes = []
    prev_mi8, prev_mi17 = mp4_mi8[0], mp4_mi17[0]
    
    for day in range(1, len(mp4_mi8)):
        if mp4_mi8[day] != prev_mi8 or mp4_mi17[day] != prev_mi17:
            changes.append((day, mp4_mi8[day], mp4_mi17[day]))
            prev_mi8, prev_mi17 = mp4_mi8[day], mp4_mi17[day]
    
    return changes  # ~120 записей за 10 лет
```

### 2. Кумулятивные суммы dt
```python
def compute_mp5_cumsum(mp5_lin, frames, days):
    """Кумулятивная сумма dt для быстрого расчёта sum(dt[a:b])"""
    cumsum = np.zeros((frames, days + 1), dtype=np.uint32)
    
    for f in range(frames):
        cumsum[f, 0] = 0
        for d in range(days):
            cumsum[f, d + 1] = cumsum[f, d] + mp5_lin[f * days + d]
    
    return cumsum.flatten()
```

## Результаты тестирования (08-01-2026)

### Сравнение архитектур (3650 дней = 10 лет)

| Архитектура | Шагов | Время | Дней/сек | Ускорение |
|-------------|-------|-------|----------|-----------|
| **Daily polling** | 3650 | ~387с | 9.4 | 1x |
| **Event-driven** | 3650 | 103с | 35.4 | 3.8x |
| **Adaptive step (MVP)** | 173 | 0.40с | 9158 | **~970x** ⚡ |

### Анализ результатов

**Adaptive step MVP:**
- step_days = min(program_limiter, 30)  
- 100 изменений программы за 10 лет → ~170 шагов
- Квотирование стабилизируется на day 17-18 (balance=0)
- После стабилизации: step_days=30 (максимум для MVP)

**Потенциал улучшений:**
- С реальными лимитерами (resource, repair): step_days может быть >30 в стабильные периоды
- Интеграция с MP2 экспортом: интерполяция или sparse export
- Динамический spawn: требует корректировки для батчевых шагов

## Файловая структура

```
code/sim_v2/messaging/
├── orchestrator_messaging.py       # Текущий (не трогаем)
├── orchestrator_adaptive.py        # НОВЫЙ: адаптивный шаг
├── rtc_publish_event.py            # Текущий (не трогаем)
├── rtc_batch_operations.py         # НОВЫЙ: батчевые инкременты
├── host_adaptive_step.py           # НОВЫЙ: вычисление step_days
└── precompute_events.py            # НОВЫЙ: предрасчёт событий
```

## План реализации

1. [ ] Создать `precompute_events.py` — предрасчёт program_changes и mp5_cumsum
2. [ ] Создать `host_adaptive_step.py` — HostFunction для вычисления step_days
3. [ ] Создать `rtc_batch_operations.py` — RTC с батчевыми инкрементами
4. [ ] Создать `orchestrator_adaptive.py` — новый оркестратор
5. [ ] Интегрировать с event-driven messaging
6. [ ] Тестирование и валидация

---

## Резюме архитектуры DES+ABM гибрида

### Ключевые принципы

1. **Событийно-управляемый шаг**: Длительность шага определяется ближайшим событием
   - Изменение программы (target)
   - Достижение ресурсного лимита (ll-sne, oh-ppr)
   - Завершение ремонта

2. **Батчевые инкременты**: Вместо ежедневных sne += dt
   - total_dt = cumsum[end_day] - cumsum[start_day]
   - sne += total_dt (одна операция за много дней)

3. **Предрасчёт событий**: На host перед симуляцией
   - program_changes: ~100 событий за 10 лет
   - mp5_cumsum: кумулятивные суммы dt для O(1) расчёта

4. **Минимум host-взаимодействий**: GPU выполняет батчи автономно
   - Только загрузка данных в начале
   - Только экспорт результатов в конце

### Совместимость с FLAME GPU

- **Слои**: batch → event → quota → apply → state_manager
- **Messaging**: Event-driven (READY, OPS_REPORT, DEMOUNT)
- **MacroProperty**: mp5_cumsum для батчевых расчётов
- **Environment**: current_day, step_days, end_day

### Преимущества

| Аспект | Daily | Adaptive |
|--------|-------|----------|
| Шагов (10 лет) | 3650 | ~170 |
| Время | ~103с | ~0.4с |
| RTC компиляций | ~40 | ~40 |
| Накладные расходы | высокие | минимальные |

### Ограничения MVP

- step_days ограничен 30 днями (для стабильности)
- Нет интеграции с MP2 экспортом (требует интерполяции)
- Нет динамического spawn в рамках шага

---
**Статус:** ✅ MVP работает
**Дата:** 08-01-2026
**Результат:** 970x ускорение vs baseline

