# V2 Архитектура: Карта RTC модулей и распределение ядер

**Дата**: 30-09-2025  
**Статус**: Планирование рефакторинга

---

## 🎯 Цели распределения

1. **Модульность** — каждый RTC модуль в отдельном файле
2. **Переиспользуемость** — общие компоненты вынесены в утилиты
3. **Тестируемость** — можно тестировать модули изолированно
4. **Масштабируемость** — легко добавлять новые состояния/модули

---

## 📂 Структура директорий (текущая)

```
code/sim_v2/
├── orchestrator_v2.py          (638 строк) — главный оркестратор
├── base_model.py                — базовая модель агента
├── sim_env_setup.py             — загрузка данных из ClickHouse
│
├── rtc_mp5_probe.py             — MP5 host function
├── rtc_mp2_writer.py            — MP2 device-side export
│
├── rtc_state_2_operations.py   — RTC для operations
├── rtc_quota_ops_excess.py      — квотирование operations
├── rtc_states_stub.py           — заглушки для states 1,3,4,5,6
├── rtc_state_manager_*.py       — менеджеры переходов (3 файла)
│
└── (будущие модули после рефакторинга)
```

---

## 📂 Структура после рефакторинга

```
code/sim_v2/
│
├── orchestrator_core.py         (~150 строк) — ТОЛЬКО оркестрация
│
├── components/                   — Компоненты инфраструктуры
│   ├── agent_population.py       — инициализация агентов
│   ├── telemetry_collector.py    — логирование и телеметрия
│   ├── mp5_strategy.py           — стратегии загрузки MP5
│   ├── data_adapters.py          — адаптеры MP1/MP3/MP5 → AgentRecord
│   └── validation_rules.py       — валидаторы данных
│
├── rtc_modules/                  — RTC функции (CUDA код)
│   ├── mp5/
│   │   ├── rtc_mp5_probe.py      — чтение MP5 в агента
│   │   └── host_mp5_init.py      — host function MP5
│   │
│   ├── states/                   — RTC функции состояний
│   │   ├── rtc_state_1_inactive.py
│   │   ├── rtc_state_2_operations.py
│   │   ├── rtc_state_3_serviceable.py
│   │   ├── rtc_state_4_repair.py
│   │   ├── rtc_state_5_reserve.py
│   │   └── rtc_state_6_storage.py
│   │
│   ├── quota/                    — Квотирование
│   │   ├── rtc_quota_ops_excess.py
│   │   ├── rtc_quota_manager.py   (будущее — унифицированный)
│   │   └── quota_common.py        (общие утилиты)
│   │
│   ├── transitions/              — Менеджеры переходов
│   │   ├── rtc_state_manager_operations.py
│   │   ├── rtc_state_manager_repair.py
│   │   ├── rtc_state_manager_storage.py
│   │   ├── rtc_state_manager_serviceable.py (будущее)
│   │   └── transition_common.py   (общие функции)
│   │
│   └── export/                   — Экспорт данных
│       ├── rtc_mp2_writer.py     — MP2 device-side writer
│       └── mp2_drain_host.py     — host drain в ClickHouse
│
├── utils/                        — Общие утилиты
│   ├── rtc_common.py             — общие CUDA функции
│   ├── constants.py              — константы (MAX_FRAMES, MAX_DAYS)
│   └── types.py                  — типы данных (AgentRecord, etc)
│
└── base_model.py                 — определение агента и модели
```

---

## 🔧 Детальное распределение RTC ядер

### 1. MP5 Модуль

**Файл**: `rtc_modules/mp5/rtc_mp5_probe.py`

```python
"""
RTC функция для чтения MP5 данных в агента
"""

def create_rtc_mp5_probe(frames_total: int, days_total: int) -> str:
    """
    Генерирует RTC код для чтения mp5_lin в daily_today_u32/daily_next_u32
    
    Параметры:
        frames_total: количество кадров
        days_total: горизонт симуляции
    
    Возвращает:
        CUDA код RTC функции
    """
    return f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_mp5_probe, flamegpu::MessageNone, flamegpu::MessageNone) {{
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
        const unsigned int step_day = FLAMEGPU->getStepCounter();
        
        const unsigned int FRAMES = {frames_total}u;
        const unsigned int base = step_day * FRAMES + idx;
        const unsigned int base_next = base + FRAMES;
        
        auto mp5 = FLAMEGPU->environment.getMacroProperty<unsigned int>("mp5_lin");
        
        const unsigned int dt = mp5[base];
        const unsigned int dn = mp5[base_next];
        
        FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
        FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
        
        return flamegpu::ALIVE;
    }}
    """
```

**Файл**: `rtc_modules/mp5/host_mp5_init.py`

```python
"""
Host Function для инициализации MP5
"""

def create_host_function_mp5_init(mp5_data: List[int], frames: int, days: int):
    """
    Создаёт HostFunction для заполнения mp5_lin
    
    Стратегия: host-only (без RTC копирования)
    """
    class HF_InitMP5(fg.HostFunction):
        def __init__(self, data, frames_val, days_val):
            super().__init__()
            self.data = data
            self.frames = frames_val
            self.days = days_val
        
        def run(self, FLAMEGPU):
            mp = FLAMEGPU.environment.getMacroPropertyUInt32("mp5_lin")
            # Заполнение напрямую из Python
            for i, val in enumerate(self.data):
                mp[i] = val
    
    return HF_InitMP5(mp5_data, frames, days)
```

---

### 2. States Модули

**Каждое состояние — отдельный файл с одной RTC функцией**

#### `rtc_modules/states/rtc_state_2_operations.py`

```python
"""
RTC функция для состояния operations (status_id=2)

Ответственность:
- Начисление наработки sne/ppr на основе daily_today_u32
- Проверка лимитов LL/OH/BR
- Установка intent_state для переходов (2→2, 2→3, 2→4, 2→6)
"""

def create_rtc_state_2_operations(frames: int) -> str:
    return f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_state_2_operations, flamegpu::MessageNone, flamegpu::MessageNone) {{
        // Текущие значения
        unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
        unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
        const unsigned int dt = FLAMEGPU->getVariable<unsigned int>("daily_today_u32");
        const unsigned int dn = FLAMEGPU->getVariable<unsigned int>("daily_next_u32");
        
        // Нормативы
        const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
        const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
        const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
        
        // Начисление наработки
        sne += dt;
        ppr += dt;
        
        FLAMEGPU->setVariable<unsigned int>("sne", sne);
        FLAMEGPU->setVariable<unsigned int>("ppr", ppr);
        
        // Прогноз на завтра
        const unsigned int sne_next = sne + dn;
        const unsigned int ppr_next = ppr + dn;
        
        // Определение intent_state
        unsigned int intent = 2u;  // по умолчанию остаёмся в operations
        
        if (sne_next >= br) {{
            intent = 6u;  // storage (Beyond Repair)
        }} else if (sne_next >= ll) {{
            intent = 6u;  // storage (LL limit)
        }} else if (ppr_next >= oh) {{
            intent = 4u;  // repair (OH limit)
        }} else if (dt == 0u) {{
            intent = 3u;  // serviceable (нет налёта → квоты нет)
        }}
        
        FLAMEGPU->setVariable<unsigned int>("intent_state", intent);
        
        return flamegpu::ALIVE;
    }}
    """
```

#### `rtc_modules/states/rtc_state_4_repair.py`

```python
"""
RTC функция для состояния repair (status_id=4)

Ответственность:
- Инкремент repair_days
- Проверка завершения ремонта (repair_days >= repair_time)
- Установка intent_state для перехода 4→5 (reserve)
"""

def create_rtc_state_4_repair() -> str:
    return """
    FLAMEGPU_AGENT_FUNCTION(rtc_state_4_repair, flamegpu::MessageNone, flamegpu::MessageNone) {
        unsigned int rd = FLAMEGPU->getVariable<unsigned int>("repair_days");
        const unsigned int rt = FLAMEGPU->getVariable<unsigned int>("repair_time");
        
        // Инкремент дней в ремонте
        rd += 1u;
        FLAMEGPU->setVariable<unsigned int>("repair_days", rd);
        
        // Проверка завершения
        unsigned int intent = 4u;  // по умолчанию остаёмся в repair
        
        if (rd >= rt) {
            intent = 5u;  // reserve (ремонт завершён)
        }
        
        FLAMEGPU->setVariable<unsigned int>("intent_state", intent);
        
        return flamegpu::ALIVE;
    }
    """
```

#### `rtc_modules/states/rtc_state_6_storage.py`

```python
"""
RTC функция для состояния storage (status_id=6)

Ответственность:
- Пассивное состояние (хранение)
- intent_state всегда = 6 (остаёмся в storage)
"""

def create_rtc_state_6_storage() -> str:
    return """
    FLAMEGPU_AGENT_FUNCTION(rtc_state_6_storage, flamegpu::MessageNone, flamegpu::MessageNone) {
        // Хранение — пассивное состояние
        FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);
        return flamegpu::ALIVE;
    }
    """
```

**Аналогично для states 1, 3, 5** — каждый в своём файле.

---

### 3. Quota Модуль

#### `rtc_modules/quota/rtc_quota_ops_excess.py`

```python
"""
Квотирование для operations с демоутом избытка

Трёхфазный процесс:
1. rtc_quota_intent_ops     — сбор заявок от operations (intent=2)
2. rtc_quota_decide_ops     — менеджер решает кого демоутить
3. rtc_quota_demote_ops     — применение демоута (intent 2→3)
"""

def create_rtc_quota_ops_excess(frames: int) -> Tuple[str, str, str]:
    """
    Возвращает 3 RTC функции: intent, decide, demote
    """
    
    rtc_intent = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_intent_ops, ...) {{
        // Сбор заявок от operations (только те, у кого intent=2)
        ...
    }}
    """
    
    rtc_decide = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_decide_ops, ...) {{
        // Менеджер (idx=0) считает избыток и ранжирует
        ...
    }}
    """
    
    rtc_demote = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_demote_ops, ...) {{
        // Применение демоута: intent 2→3 для выбранных
        ...
    }}
    """
    
    return rtc_intent, rtc_decide, rtc_demote
```

---

### 4. Transitions Модуль

#### `rtc_modules/transitions/rtc_state_manager_operations.py`

```python
"""
State Manager для operations (2→X переходы)

Ответственность:
- Чтение intent_state от operations агентов
- Выполнение переходов:
  - 2→2 (остаёмся в operations)
  - 2→3 (serviceable, нет квоты)
  - 2→4 (repair, OH лимит)
  - 2→6 (storage, LL/BR лимит)
"""

def create_rtc_state_manager_operations() -> Tuple[str, str, str, str]:
    """
    Возвращает 4 RTC функции для каждого типа перехода
    """
    
    rtc_ops_to_ops = """
    FLAMEGPU_AGENT_FUNCTION(rtc_mgr_ops_to_ops, ...) {
        const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
        if (intent == 2u) {
            FLAMEGPU->setInitialState("operations");
            FLAMEGPU->setEndState("operations");
        }
        return flamegpu::ALIVE;
    }
    """
    
    rtc_ops_to_serviceable = """
    FLAMEGPU_AGENT_FUNCTION(rtc_mgr_ops_to_serviceable, ...) {
        const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
        if (intent == 3u) {
            FLAMEGPU->setInitialState("operations");
            FLAMEGPU->setEndState("serviceable");
        }
        return flamegpu::ALIVE;
    }
    """
    
    # Аналогично для 2→4 и 2→6
    
    return rtc_ops_to_ops, rtc_ops_to_serviceable, rtc_ops_to_repair, rtc_ops_to_storage
```

---

### 5. Export Модуль

#### `rtc_modules/export/rtc_mp2_writer.py`

```python
"""
MP2 device-side writer

Записывает состояние агента в MacroProperty MP2 на каждом шаге
"""

def create_rtc_mp2_writer(frames: int) -> str:
    return f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_mp2_write, ...) {{
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int pos = day * {frames}u + idx;
        
        // Запись в MP2 SoA
        auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int>("mp2_day");
        auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int>("mp2_idx");
        auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int>("mp2_sne");
        // ...
        
        mp2_day[pos] = day;
        mp2_idx[pos] = idx;
        mp2_sne[pos] = FLAMEGPU->getVariable<unsigned int>("sne");
        // ...
        
        return flamegpu::ALIVE;
    }}
    """
```

---

## 🔄 Оркестрация: как модули подключаются

### `orchestrator_core.py` (после рефакторинга)

```python
class V2OrchestratorCore:
    """
    Упрощённый оркестратор — ТОЛЬКО управление пайплайном
    """
    
    def __init__(self, env_data, config):
        self.env_data = env_data
        self.config = config
        
        # Компоненты (делегаты)
        self.population_builder = AgentPopulationBuilder(env_data)
        self.telemetry = TelemetryCollector() if config.telemetry else None
        self.mp5_strategy = HostFunctionMP5Init(env_data)
        
    def build_model(self, rtc_modules: List[str]):
        """Собирает модель из указанных модулей"""
        model = BaseModel()
        
        # 1. MP5 инициализация
        self.mp5_strategy.register(model)
        
        # 2. Подключение RTC модулей
        for module_name in rtc_modules:
            module = import_rtc_module(f"rtc_modules/{module_name}")
            module.register(model)
        
        return model
    
    def run(self, steps: int):
        """Запуск симуляции"""
        for step in range(steps):
            self.simulation.step()
            
            # Телеметрия (опционально)
            if self.telemetry:
                self.telemetry.track_step(step, self.simulation)
```

---

## 📊 Сравнение: до/после рефакторинга

| Компонент | Было | Станет | Выигрыш |
|-----------|------|--------|---------|
| **Оркестратор** | 638 строк | ~150 строк | 76% ↓ |
| **Инициализация агентов** | в оркестраторе | `agent_population.py` | Изоляция |
| **Телеметрия** | в `run()` | `telemetry_collector.py` | Опциональна |
| **MP5 стратегия** | жёстко в оркестраторе | `mp5_strategy.py` | Гибкость |
| **RTC модули** | 6 файлов | 15+ файлов | Модульность |
| **Общие утилиты** | дубли в RTC | `rtc_common.py` | DRY |

---

## 🎯 Преимущества новой архитектуры

### 1. **Тестируемость**
```python
# Можно тестировать модули изолированно
def test_state_2_operations():
    rtc_code = create_rtc_state_2_operations(frames=286)
    # Проверка генерации кода, парсинг, unit-тесты
```

### 2. **Переиспользуемость**
```python
# Общие функции вынесены в rtc_common
from rtc_modules.utils.rtc_common import calculate_mp5_index

# Используется в rtc_mp5_probe и других модулях
```

### 3. **Расширяемость**
```python
# Добавить новое состояние — просто новый файл
rtc_modules/states/rtc_state_7_maintenance.py  # новое состояние
```

### 4. **Конфигурируемость**
```bash
# Запуск с разными наборами модулей
python orchestrator_core.py --modules state_2 quota_ops state_manager_ops
python orchestrator_core.py --modules state_2 state_4 state_6  # минимум
```

---

## 📝 Порядок выполнения рефакторинга

**Согласно Tasktracker**:

1. ✅ **Шаг 1**: `agent_population.py` — без изменений RTC
2. ✅ **Шаг 2**: `telemetry_collector.py` — без изменений RTC
3. ✅ **Шаг 3**: `mp5_strategy.py` — перенос host function
4. ⏭ **Шаг 4**: `data_adapters.py` — без изменений RTC
5. ⏭ **Шаг 5**: `validation_rules.py` — без изменений RTC
6. ⏭ **Шаг 6**: Реорганизация RTC в `rtc_modules/` (опционально, P3)

**Критично**: Шаги 1-5 не трогают RTC код, только переносят Python-логику!

---

*Обновлено: 30-09-2025*  
*Автор: V2 Refactoring Team*

