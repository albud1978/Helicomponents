# Статус интеграции Spawn - 30.09.2025

## Текущая проблема

**NVRTC Error 425** при компиляции `rtc_spawn_ticket_simple` (и полного `rtc_spawn_ticket`)

### Симптомы
- Ошибка возникает на этапе `create_simulation()` при вызове `sim.setEnvironmentPropertyUInt()`
- FLAME GPU пытается скомпилировать RTC функцию и выдаёт:
  ```
  (InvalidAgentFunc) Error compiling runtime agent function ('rtc_spawn_ticket_simple'): 
  function had compilation errors (see std::cout)
  ```
- Детальные логи NVRTC **не выводятся** в stdout, даже с `FLAMEGPU_VERBOSE=1`

### Что проверено и исключено

✅ **Переменные агента**: Все переменные (`idx`, `aircraft_number`, `partseqno_i`, `group_by`, `ll`, `oh`, `br`, `sne`, `ppr`, `cso`, `repair_days`, `s6_started`, `s6_days`, `intent_state`, `prev_intent_state`, `daily_today_u32`, `daily_next_u32`, `mfg_date`, `repair_time`, `assembly_time`, `partout_time`, `assembly_trigger`, `active_trigger`, `partout_trigger`) объявлены в `base_model.py`

✅ **State инициализация**: Все 6 states (`inactive`, `operations`, `serviceable`, `repair`, `reserve`, `storage`) инициализируются в `agent_population.py`, даже если пустые

✅ **MacroProperty**: Упрощённые макросвойства используются (`spawn_need_u32`, `spawn_base_idx_u32`)

✅ **Упрощённый RTC код**: `rtc_spawn_simple.py` содержит минимальный RTC без сложной логики

✅ **Populация spawn агентов**: Добавлена инициализация `spawn_mgr` и `spawn_ticket` через `initialize_simple_spawn_population()`

❌ **Целевой state для agent_out**: Пробовали `"serviceable"` и `"operations"` - обе дают ту же ошибку

❌ **Изоляция модуля**: Запуск только `spawn_simple` без других модулей даёт ту же ошибку

### Рабочий код для сравнения

Коммит `b6bc62c8` содержит рабочий spawn (`code/sim_v2/rtc_spawn.py`), где:
- Использовался `status_id = 2u` (старая архитектура до States)
- `setAgentOutput(agent, "operations")`
- Те же переменные и логика MacroProperty

**НО**: Неизвестно как именно запускался тест (возможно через orchestrator с полным набором модулей)

### Текущие файлы

#### `/home/budnik_an/cube linux/cube/code/sim_v2/rtc_modules/rtc_spawn_simple.py`
- Упрощённая версия spawn для отладки
- 2 RTC функции: `rtc_spawn_mgr_simple`, `rtc_spawn_ticket_simple`
- Минимальные MacroProperty для обмена данными
- `initialize_simple_spawn_population()` для инициализации популяции

#### `/home/budnik_an/cube linux/cube/code/sim_v2/rtc_modules/rtc_spawn_integration.py`
- Полная версия spawn из `rtc_spawn.py`
- Убран `status_id` (используются States)
- `setAgentOutput(agent, "serviceable")` с `intent_state=2`
- `initialize_spawn_population()` для инициализации

#### `/home/budnik_an/cube linux/cube/code/sim_v2/orchestrator_v2.py`
- Обновлён для поддержки обоих модулей spawn
- Условная инициализация популяции в зависимости от модуля
- Флаг `self.spawn_enabled` устанавливается для `"spawn"` или `"spawn_simple"`

#### `/home/budnik_an/cube linux/cube/code/sim_v2/base_model.py`
- Добавлены все необходимые переменные агента
- `partout_time`, `partout_trigger` для Mi-17
- Условная регистрация модулей `spawn` и `spawn_simple`

#### `/home/budnik_an/cube linux/cube/code/sim_v2/components/agent_population.py`
- Инициализация **всех** states, даже пустых (для spawn agent_out)
- Инициализация `partout_time` для Mi-8 и Mi-17

### Гипотезы для дальнейшей отладки

1. **Недостающие #include или headers в RTC**: Возможно NVRTC не видит определения каких-то типов
2. **Конфликт имён переменных**: Возможно какая-то переменная пересекается с зарезервированным словом
3. **Проблема с agent_out в конкретном state**: Может быть ограничение FLAME GPU на создание агентов в определённых states
4. **Отсутствие других RTC модулей**: Возможно spawn зависит от каких-то определений из других модулей (хотя это маловероятно)
5. **Версия FLAME GPU**: Возможно в рабочем коде использовалась другая версия библиотеки
6. **Проблема с порядком слоёв**: Возможно spawn нужно регистрировать в определённом порядке относительно других модулей

### Следующие шаги

1. **Получить детальные логи NVRTC**: Попробовать другие способы включения verbose режима компиляции
2. **Сравнить с рабочим sim_master**: Проверить как spawn интегрировался в старом монолитном коде
3. **Попробовать создание агентов через HostFunction**: Альтернативный подход без agent_out в RTC
4. **Минимальный воспроизводимый пример**: Создать отдельный скрипт FLAME GPU только со spawn (без всей инфраструктуры)
5. **Проверка зависимостей**: Убедиться что все env properties доступны на момент компиляции RTC

## История изменений

### 30.09.2025 - Попытки решения NVRTC 425

- Создан `rtc_spawn_simple.py` с минимальным RTC кодом
- Добавлена инициализация всех states в `agent_population.py`
- Добавлена `initialize_simple_spawn_population()` для spawn агентов
- Обновлён `orchestrator_v2.py` для поддержки `spawn_simple`
- Пробовали разные target states: `serviceable`, `operations`
- Пробовали запуск в изоляции (только spawn_simple)
- Пробовали запуск с базовыми модулями (state_2_operations + states_stub)

**Результат**: Все попытки дают NVRTC Error 425 при компиляции `rtc_spawn_ticket_simple`

### Что работает

✅ Рефакторинг V2 без spawn (state managers, quota, operations, MP2 export)
✅ Полный 3650-дневный тест без spawn
✅ Модульная архитектура orchestrator_v2

### Что НЕ работает

❌ Компиляция любой RTC функции spawn с `agent_out`
❌ Интеграция spawn в V2 архитектуру

## Контекст

Spawn интегрируется для создания новых агентов Mi-17 согласно плану `mp4_new_counter_mi17_seed`. В старом коде `sim_master` spawn работал отдельно, но при интеграции с полным пайплайном возникали конфликты компиляции NVRTC. Это стало одной из причин рефакторинга в микросервисную V2 архитектуру.

Текущая блокировка на NVRTC 425 требует глубокой диагностики или альтернативного подхода к созданию агентов.
