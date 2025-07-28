# FLAME GPU Микросервисная Архитектура

**Дата создания:** 28-07-2025  
**Последнее обновление:** 28-07-2025  
**Автор:** Helicopter Component Lifecycle Project Team

## 🎯 **НАЗНАЧЕНИЕ ДОКУМЕНТА**

Описание архитектурного решения для организации FLAME GPU симуляций в микросервисной архитектуре с persistent состоянием и интеграцией в ETL пайплайн проекта моделирования жизненного цикла вертолетных компонентов.

---

## 📋 **ПРОБЛЕМАТИКА И ЦЕЛИ**

### **Основная проблема**
FLAME GPU Environment существует только во время выполнения Python скрипта - GPU память стирается после завершения процесса.

### **Бизнес-требования**
1. **Persistent симуляция**: Загрузка MacroProperty → создание агентов → симуляция → формирование MacroProperty2 → выгрузка с field_id
2. **Отключение промежуточных выгрузок** в СУБД для оптимизации производительности
3. **Логика агентов** должна сохраняться между этапами
4. **Интеграция с BI** через словари с field_id маппингом
5. **Поэтапное тестирование** каждого компонента архитектуры

### **Технические ограничения**
- FLAME GPU использует CUDA память, которая не персистентна
- Python процесс завершается → GPU контекст освобождается
- Необходимость в state management между микросервисами
- Высокая нагрузка на GPU память при больших данных

---

## 🏗️ **АРХИТЕКТУРНОЕ РЕШЕНИЕ**

### **Выбранный паттерн: Persistent GPU Service + State Checkpoints**

Комбинированный подход, основанный на лучших практиках GPU микросервисов:

1. **Persistent FLAME GPU Service** - долгоживущий сервис
2. **State Checkpoint Manager** - сохранение промежуточных состояний
3. **Pipeline Orchestrator** - управление этапами симуляции
4. **ClickHouse Integration Layer** - интеграция с СУБД

---

## 🔧 **КОМПОНЕНТЫ АРХИТЕКТУРЫ**

### **1. FLAME GPU Persistent Service**
```
flame_gpu_service.py
├── GPU Memory Manager
├── Environment State Controller  
├── Agent Population Manager
├── Simulation Engine Controller
└── Checkpoint Save/Restore
```

**Функции:**
- Поддержание активного FLAME GPU контекста
- Управление GPU памятью и агентами
- Обработка команд от оркестратора
- Сохранение/восстановление состояний

### **2. Pipeline Orchestrator**
```
simulation_orchestrator.py
├── Stage Controller
├── Service Communication
├── Error Recovery
├── Progress Monitoring
└── Resource Management
```

**Этапы пайплайна:**
1. **Load Stage**: Загрузка MacroProperty1-5 + Property
2. **Agent Creation**: Создание популяции агентов
3. **Simulation Stage**: Запуск симуляции
4. **Result Export**: Формирование MacroProperty2
5. **BI Integration**: Выгрузка с field_id маппингом

### **3. State Checkpoint Manager**
```
checkpoint_manager.py
├── GPU State Serializer
├── ClickHouse State Storage
├── Recovery Controller
├── Version Management
└── Integrity Checker
```

**Checkpoint типы:**
- **Environment Checkpoint**: MacroProperty, Property, настройки
- **Agent Population Checkpoint**: Состояния всех агентов
- **Simulation Progress**: Промежуточные результаты симуляции
- **Result Checkpoint**: MacroProperty2 перед выгрузкой

### **4. ClickHouse Integration Layer**
```
clickhouse_integration.py
├── Dictionary Manager
├── Field ID Mapper
├── Batch Processor
├── BI Connector
└── Data Validator
```

---

## 🚀 **ПОЭТАПНАЯ РЕАЛИЗАЦИЯ**

### **Этап 1: Базовая архитектура (1-2 недели)**
```bash
# Создание основных компонентов
code/simulation/
├── flame_gpu_service.py          # Persistent GPU сервис
├── simulation_orchestrator.py    # Оркестратор пайплайна
├── checkpoint_manager.py         # Управление состояниями
└── config/
    ├── service_config.yaml       # Конфигурация сервисов
    └── simulation_params.yaml    # Параметры симуляции
```

**Тестирование Этапа 1:**
- Запуск/остановка Persistent Service
- Загрузка MacroProperty без выгрузки
- Сохранение/восстановление checkpoint'ов
- Базовая коммуникация между компонентами

### **Этап 2: Agent Management (1-2 недели)**
```bash
# Расширение для управления агентами
code/simulation/agents/
├── agent_factory.py             # Создание агентов
├── population_manager.py        # Управление популяцией
├── agent_state_controller.py    # Состояния агентов
└── lifecycle_logic.py           # Логика жизненного цикла
```

**Тестирование Этапа 2:**
- Создание агентов из MacroProperty
- Валидация начальных состояний
- Checkpoint агентской популяции
- Memory management для больших популяций

### **Этап 3: Simulation Engine (2-3 недели)**
```bash
# Реализация симуляции
code/simulation/engine/
├── simulation_controller.py     # Контроллер симуляции
├── step_processor.py            # Обработка шагов
├── result_collector.py          # Сбор результатов
└── macroproperty2_builder.py    # Формирование MacroProperty2
```

**Тестирование Этапа 3:**
- Простые симуляционные сценарии
- Корректность MacroProperty2
- Performance тестирование
- Checkpoint промежуточных результатов

### **Этап 4: BI Integration (1-2 недели)**
```bash
# Интеграция с BI
code/simulation/bi/
├── field_id_mapper.py           # Маппинг field_id
├── bi_exporter.py               # Экспорт в BI формат
├── dictionary_updater.py        # Обновление словарей
└── quality_validator.py         # Валидация качества
```

**Тестирование Этапа 4:**
- Корректность field_id маппинга
- Интеграция с существующими словарями
- End-to-end тестирование пайплайна
- Performance полного цикла

---

## 🔄 **КОММУНИКАЦИОННЫЕ ПРОТОКОЛЫ**

### **Inter-Service Communication**
```python
# RESTful API между сервисами
POST /flame-gpu/load-macroproperty    # Загрузка данных
POST /flame-gpu/create-agents         # Создание агентов  
POST /flame-gpu/run-simulation        # Запуск симуляции
POST /flame-gpu/export-results        # Экспорт результатов
GET  /flame-gpu/status                # Статус сервиса
POST /flame-gpu/checkpoint/save       # Сохранение состояния
POST /flame-gpu/checkpoint/restore    # Восстановление состояния
```

### **Message Queue (опционально)**
```python
# Для асинхронной обработки больших задач
Queue: simulation.macroproperty.load
Queue: simulation.agents.create
Queue: simulation.run.execute  
Queue: simulation.results.export
```

---

## 📊 **МОНИТОРИНГ И МЕТРИКИ**

### **Key Performance Indicators**
- **GPU Memory Usage**: % использования GPU памяти
- **Agent Population Size**: Количество активных агентов
- **Simulation Steps/sec**: Скорость симуляции
- **Checkpoint Size/Time**: Размер и время checkpoint'ов
- **End-to-End Latency**: Время полного пайплайна

### **Logging Strategy**
```python
# Структурированное логирование
logs/simulation/
├── flame_gpu_service_{timestamp}.log
├── orchestrator_{timestamp}.log
├── checkpoint_manager_{timestamp}.log
└── bi_integration_{timestamp}.log
```

---

## 🛡️ **ERROR RECOVERY & RESILIENCE**

### **Failure Scenarios**
1. **GPU Service Crash**: Restart + восстановление из checkpoint
2. **Out of Memory**: Batch processing + memory optimization
3. **Simulation Divergence**: Rollback к предыдущему checkpoint
4. **ClickHouse Connection Loss**: Retry with exponential backoff

### **Recovery Strategies**
```python
# Автоматическое восстановление
try:
    result = flame_gpu_service.run_simulation()
except GPUMemoryError:
    checkpoint_manager.save_current_state()
    flame_gpu_service.optimize_memory()
    flame_gpu_service.restore_from_checkpoint()
    result = flame_gpu_service.run_simulation(batch_mode=True)
```

---

## 🔗 **ИНТЕГРАЦИЯ С СУЩЕСТВУЮЩИМ ETL**

### **Integration Points**
1. **Extract Stage**: Данные подготавливаются как обычно
2. **Transform Stage**: 
   - Отключение промежуточных выгрузок MacroProperty
   - Активация Persistent Service
   - Запуск оркестратора симуляции
3. **Load Stage**: Загрузка MacroProperty2 с field_id в ClickHouse

### **Обратная совместимость**
```python
# Fallback на текущую архитектуру
if ENABLE_PERSISTENT_SIMULATION:
    orchestrator = SimulationOrchestrator()
    result = orchestrator.run_full_pipeline()
else:
    # Текущий подход: отдельные loader/exporter
    loader = FlameMacroPropertyLoader()
    result = loader.run_loading_process()
```

---

## 📈 **EXPECTED BENEFITS**

### **Performance Gains**
- **Устранение повторных загрузок**: ~60% экономии времени
- **Persistent GPU контекст**: ~40% экономии на инициализации
- **Batch processing**: ~80% улучшение throughput для больших данных

### **Operational Benefits**
- **Упрощение deployment**: Один persistent сервис вместо множества скриптов
- **Лучший мониторинг**: Централизованные метрики и логирование
- **Easier debugging**: Checkpoint'ы для воспроизведения проблем
- **Scalability**: Горизонтальное масштабирование GPU workers

---

## 🎯 **NEXT STEPS**

1. **Утверждение архитектуры** с командой проекта
2. **Создание MVP** (Этап 1) для proof of concept
3. **Интеграционное тестирование** с текущим ETL пайплайном
4. **Поэтапная миграция** существующих компонентов
5. **Production deployment** с мониторингом и метриками

---

## 📚 **REFERENCES**

- FLAME GPU 2.0 Documentation: Environment Management
- GPU Computing Best Practices: Persistent Services
- Microservices Patterns: State Management in Distributed Systems
- ClickHouse Integration: High-Performance Data Pipelines
- CUDA Programming Guide: Memory Management Strategies

---

*Документ будет обновляться по мере развития архитектуры и получения feedback от команды разработки.* 