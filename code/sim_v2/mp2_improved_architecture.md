# Улучшенная архитектура MP2 (на основе sim_master)

## Проблемы текущей реализации MP2:

1. **Частый дренаж** (каждые 30 дней) - неэффективно для ClickHouse
2. **Кольцевой буфер** - риск потери данных при длинных симуляциях
3. **Маленькие батчи** - при 286 агентах и дренаже раз в 30 дней получаем батчи ~8580 строк

## Рекомендуемый подход (как в sim_master):

### Вариант 1: Полный экспорт после симуляции

```python
class MP2FinalExportHostFunction(fg.HostFunction):
    """Экспортирует ВСЕ данные MP2 в конце симуляции"""
    
    def __init__(self, client, table_name='sim_masterv2', batch_size=250000):
        super().__init__()
        self.client = client
        self.table_name = table_name
        self.batch_size = batch_size
        
    def run(self, FLAMEGPU):
        # Вызывается ОДИН раз в конце симуляции
        if FLAMEGPU.getStepCounter() != self.simulation_steps - 1:
            return
            
        # Экспортируем ВСЕ дни из MP2
        total_days = FLAMEGPU.getStepCounter() + 1
        frames = FLAMEGPU.environment.getPropertyUInt("frames_total")
        
        batch = []
        for day in range(total_days):
            for idx in range(frames):
                # Читаем из линейного MP2 (не кольцевого!)
                pos = day * frames + idx
                # ... собираем данные ...
                batch.append(row)
                
                if len(batch) >= self.batch_size:
                    self._flush_batch(batch)
                    
        # Финальный flush
        if batch:
            self._flush_batch(batch)
```

### Вариант 2: Увеличить размер буфера и интервал дренажа

```python
# В rtc_mp2_writer.py
MP2_RING_DAYS = 400  # Хранить 400 дней вместо 30
MP2_SIZE = MAX_FRAMES * MP2_RING_DAYS

# В mp2_drain_host.py
drain_interval = 365  # Дренаж раз в год
batch_size = 250000   # Оптимальный размер для ClickHouse
```

### Вариант 3: Линейный буфер вместо кольцевого

```python
# Выделяем память на ВСЮ симуляцию
MP2_MAX_DAYS = 4000  # Как MP5
MP2_SIZE = MAX_FRAMES * MP2_MAX_DAYS

# В RTC функции - линейная адресация
const unsigned int pos = step_day * MAX_FRAMES + idx;  // Без % операции

# Дренаж только в конце или раз в год
```

## Преимущества подхода sim_master:

1. **Нет потерь данных** - все хранится до конца
2. **Эффективные батчи** - 250к строк оптимально для ClickHouse
3. **Минимум операций GPU↔CPU** - только в конце симуляции
4. **Простота** - нет сложной логики кольцевых буферов

## Выводы:

Текущий MP2 с дренажом каждые 30 дней - это overengineering для задачи. 
Лучше использовать подход sim_master:
- Хранить все на GPU до конца симуляции
- Экспортировать одним большим батчем в конце
- Или увеличить интервал дренажа до 365+ дней

## Результаты применения Варианта 2 (25-09-2025)

Реализован Вариант 2 с увеличенными параметрами:
- MP2_RING_DAYS = 400 (кольцевой буфер на 400 дней)
- drain_interval = 365 (дренаж раз в год)
- batch_size = 250000 (оптимально для ClickHouse)

### Тестирование на 3650 дней (10 лет):
- **Производительность**: 54.44с общее время, 13.4мс на шаг
- **MP2 дренаж**: 21.67с (параллельно с GPU), 11 операций
- **Объем данных**: ~275K записей экспортировано
- **Корректность**: state/intent_state/dt/dn записываются правильно
- **Стабильность**: нет потерь данных, кольцевой буфер не переполнился

Вариант 2 оказался оптимальным компромиссом между памятью GPU и эффективностью.
