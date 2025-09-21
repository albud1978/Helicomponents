# V2 Микросервисная архитектура FLAME GPU

## Обзор

Новая архитектура V2 следует принципам микросервисной разработки для FLAME GPU симуляций:

1. **Базовая модель** (`base_model.py`) - определяет структуру данных и окружение
2. **RTC модули** (`rtc_*.py`) - содержат изолированную бизнес-логику 
3. **Оркестратор** (`orchestrator_v2.py`) - управляет композицией и выполнением

## Компоненты

### 1. Базовая модель (base_model.py)

Отвечает за:
- Создание FLAME GPU модели и окружения
- Определение структуры агентов (переменные)
- Настройку Environment properties (скаляры, массивы, MacroProperty)
- Динамическую загрузку RTC модулей

```python
base_model = V2BaseModel()
model = base_model.create_model(env_data)
base_model.add_rtc_module('mp5_probe')
base_model.add_rtc_module('status_246')
```

### 2. RTC модули

Каждый модуль содержит:
- RTC функции (CUDA C++ код)
- Регистрацию функций и слоев
- Вспомогательные функции (например, HostFunction)

Пример структуры модуля:
```python
def register_rtc(model, agent):
    """Регистрирует RTC функции и слои"""
    rtc_code = """
    FLAMEGPU_AGENT_FUNCTION(...) {
        // Бизнес-логика
    }
    """
    agent.newRTCFunction("function_name", rtc_code)
    layer = model.newLayer()
    layer.addAgentFunction(agent.getFunction("function_name"))
    return layer
```

### 3. Оркестратор (orchestrator_v2.py)

Управляет всем процессом:
- Загрузка данных из ClickHouse
- Построение модели с нужными модулями
- Инициализация симуляции
- Запуск и контроль выполнения
- Сбор результатов

## Использование

### Базовый запуск
```bash
python orchestrator_v2.py
```

### С выбором модулей
```bash
python orchestrator_v2.py --modules mp5_probe status_246 quotas
```

### С указанием количества шагов
```bash
python orchestrator_v2.py --steps 365
```

## Создание нового RTC модуля

1. Создайте файл `rtc_module_name.py`
2. Реализуйте функцию `register_rtc(model, agent)`
3. Добавьте RTC функции и слои
4. Модуль автоматически станет доступен для подключения

Пример минимального модуля:
```python
def register_rtc(model, agent):
    rtc_code = """
    FLAMEGPU_AGENT_FUNCTION(rtc_my_function, flamegpu::MessageNone, flamegpu::MessageNone) {
        // Ваша логика
        return flamegpu::ALIVE;
    }
    """
    agent.newRTCFunction("rtc_my_function", rtc_code)
    layer = model.newLayer()
    layer.addAgentFunction(agent.getFunction("rtc_my_function"))
    return layer
```

## Преимущества архитектуры

1. **Модульность** - каждый аспект логики изолирован в своем модуле
2. **Переиспользование** - модули можно комбинировать в разных конфигурациях
3. **Тестируемость** - каждый модуль можно тестировать отдельно
4. **Масштабируемость** - легко добавлять новую функциональность
5. **Читаемость** - код организован логично и понятно

## Соответствие принципам FLAME GPU

- **Environment** используется для глобальных данных
- **MacroProperty** для больших массивов с GPU-оптимизированным доступом
- **HostFunction** для инициализации данных на CPU
- **RTC функции** компилируются в GPU код для максимальной производительности
- **Слои (Layers)** обеспечивают синхронизацию между шагами
