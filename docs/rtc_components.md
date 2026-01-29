# RTC Components Architecture (Агрегаты)

> **Актуальная версия:** v2.0 (15-01-2026)  
> **Файл оркестратора:** `code/sim_v2/units/orchestrator_units.py`

---

## ⚠️ Известные проблемы (15-01-2026)

### 1. Дефицит Mi-17 двигателей (-105)

**Проблема:** Mi-17 имеет дефицит двигателей на финале (153 vs 258 нужных).

**Причина:** 
- Spawn активирует ~1 агрегат за шаг на группу
- Флот Mi-17 **растёт** (86→129 ВС), требуя больше двигателей

**Симптомы:**
- День 0: deficit=0 (полная комплектация)
- День 3639: 153 двигателей на 129 ВС (дефицит 105)
- Spawn создал 75 новых двигателей — недостаточно

**Статус:** Требует увеличения скорости spawn.

### 2. Многократное назначение агрегатов на планер ✅ ИСПРАВЛЕНО (15-01-2026)

**Проблема:** На одном планере Mi-8 назначалось **до 29 двигателей** вместо 2!

**Корень проблемы:** Неправильный API для PropertyArray в CUDA:
```cuda
// НЕПРАВИЛЬНО:
getProperty<unsigned int>("comp_numbers", group_by);

// ПРАВИЛЬНО:
getProperty<unsigned int, MAX_GROUPS>("comp_numbers", group_by);
```

**Исправлено в:**
- `rtc_units_assembly.py` (2 места)
- `rtc_units_fifo_priority.py` (4 места)

**Результат:** Mi-8: MAX=2, Mi-17: MAX=3 (race condition на 1 планере).

### 3. 10 проходов assembly

**Проблема:** Для полной комплектации требуется **10 последовательных проходов** assembly.

**Причина:** Атомарные операции на GPU конкурируют за слоты.

**Следствие:** Увеличенное время симуляции (~150с на 3650 дней).

**Статус:** Workaround. Возможна оптимизация.

### 4. Неполная запись данных

**Проблема:** Последний drain не записывает дни 3640-3649.

**Причина:** Drain происходит каждые 10 дней, но финальный буфер не сбрасывается.

**Статус:** TODO — добавить финальный drain после симуляции.

### 5. storage баг исправлен ✅ (14-01-2026)

**Проблема:** Агрегаты уходили в storage при `SNE >= BR` без проверки `PPR >= OH`.

**Решение:** Условие исправлено на `(PPR >= OH AND SNE >= BR)`.

---

## 📊 Таблица RTC модулей (42+ функций)

> **⚠️ ВАЖНО: Порядок = хронология выполнения**
> 
> Номера (#) соответствуют **порядку регистрации слоёв в модели**.

| # | Фаза | Модуль | Функция | State | Описание |
|---|------|--------|---------|-------|----------|
| **ФАЗА 0: Инициализация** |||||
| 0 | Init | `init_planer_dt.py` | `InitPlanerDtHostFunction` | Host | Загрузка dt, assembly, history в MacroProperty |
| 0b | Init | `init_fifo_queues.py` | `InitFifoQueuesFunction` | Host | Инициализация FIFO очередей |
| **ФАЗА 0.5: Детекция выхода планера** |||||
| 1 | planer_exit | `rtc_units_planer_exit.py` | `rtc_units_planer_exit_check` | ops | Проверка `mp_planer_in_ops_history` — планер ещё в ops? |
| 2 | planer_exit | `rtc_units_planer_exit.py` | `rtc_units_planer_exit_decrement` | ops | Декремент `mp_planer_slots`, агрегат → serviceable |
| **ФАЗА 0.6: Расчёт дефицита (HostFunction)** |||||
| 3 | demand | `rtc_units_demand_host.py` | `DemandHostFunction` | Host | Расчёт `mp_request_count` по группам |
| **ФАЗА 1: States Stub** |||||
| 4 | states_stub | `rtc_units_states_stub.py` | `rtc_units_stub_serviceable` | svc | `intent_state = 3` |
| 5 | states_stub | `rtc_units_states_stub.py` | `rtc_units_stub_reserve` | rsv | `intent_state = 5` |
| 6 | states_stub | `rtc_units_states_stub.py` | `rtc_units_stub_storage` | stor | `intent_state = 6` |
| **ФАЗА 2: Check Limits** |||||
| 7 | check_limits | `rtc_units_increment.py` | `rtc_units_check_limits` | ops | `if ppr >= oh → intent=4`, `if sne >= ll → intent=6` |
| **ФАЗА 3: State Repair** |||||
| 8 | state_repair | `rtc_units_state_repair.py` | `rtc_units_repair_increment` | rep | `repair_days++`, проверка выхода |
| 9 | state_repair | `rtc_units_state_repair.py` | `rtc_units_repair_to_reserve` | 4→5 | Выход из ремонта: `ppr=0`, `mp_rsv_count++` |
| **ФАЗА 4: Transitions из Operations** |||||
| 10 | transition_ops | `rtc_units_transition_ops.py` | `rtc_units_apply_2_to_2` | ops→ops | Остаёмся в ops |
| 11 | transition_ops | `rtc_units_transition_ops.py` | `rtc_units_apply_2_to_3` | ops→svc | → serviceable, `svc_tail++` |
| 12 | transition_ops | `rtc_units_transition_ops.py` | `rtc_units_apply_2_to_4` | ops→rep | → repair, `mp_replacement_request` |
| 13 | transition_ops | `rtc_units_transition_ops.py` | `rtc_units_apply_2_to_6` | ops→stor | → storage (SNE >= LL) |
| **ФАЗА 5: Трёхуровневая FIFO (приоритет svc→rsv→spawn)** |||||
| 14 | fifo_return | `rtc_units_fifo_priority.py` | `rtc_fifo_return_to_rsv` | rsv | Возврат после ремонта: `rsv_tail++` |
| 15 | fifo_svc_check | `rtc_units_fifo_priority.py` | `rtc_fifo_assign_svc_check` | svc | Phase1: чтение `mp_request_count` |
| 16 | fifo_rsv_check | `rtc_units_fifo_priority.py` | `rtc_fifo_assign_rsv_check` | rsv | Phase1: чтение очередей |
| 17 | fifo_spawn_check | `rtc_units_fifo_priority.py` | `rtc_fifo_spawn_check` | rsv(0) | Phase1: поиск планера с дефицитом |
| 18 | fifo_svc_activate | `rtc_units_fifo_priority.py` | `rtc_fifo_assign_svc_activate` | svc | Phase2: назначение, `svc_head++` |
| 19 | fifo_rsv_activate | `rtc_units_fifo_priority.py` | `rtc_fifo_assign_rsv_activate` | rsv | Phase2: назначение, `rsv_head++`, `mp_rsv_count--` |
| 20 | fifo_spawn_activate | `rtc_units_fifo_priority.py` | `rtc_fifo_spawn_activate` | rsv(0) | Phase2: активация spawn, `active=1` |
| **ФАЗА 6: Assembly (10 проходов)** |||||
| 21-30 | assembly | `rtc_units_assembly.py` | `rtc_assembly_check` | svc/rsv | Phase1: поиск планера в ops |
| 21-30 | assembly | `rtc_units_assembly.py` | `rtc_assembly_activate` | svc/rsv | Phase2: назначение на планер |
| **ФАЗА 7: Transitions из других состояний** |||||
| 31 | transition_rsv | `rtc_units_transition_reserve.py` | `rtc_units_apply_5_to_5` | rsv→rsv | Остаёмся в reserve |
| 32 | transition_rsv | `rtc_units_transition_reserve.py` | `rtc_units_apply_5_to_2` | rsv→ops | → operations |
| 33 | transition_svc | `rtc_units_transition_serviceable.py` | `rtc_units_apply_3_to_3` | svc→svc | Остаёмся в svc |
| 34 | transition_svc | `rtc_units_transition_serviceable.py` | `rtc_units_apply_3_to_2` | svc→ops | → operations |
| 35 | transition_stor | `rtc_units_transition_storage.py` | `rtc_units_apply_6_to_6` | stor→stor | Терминальное состояние |
| **ФАЗА 8: Инкремент наработки** |||||
| 36 | increment | `rtc_units_increment.py` | `rtc_units_increment` | ops | `sne += dt`, `ppr += dt` |
| **ФАЗА 9: Запись результатов** |||||
| 37 | mp2_writer | `rtc_units_mp2_writer.py` | `rtc_units_write_mp2` | all | Запись в MacroProperty буфер |
| **StepFunctions (после каждого шага)** |||||
| S1 | debug | `debug_step.py` | `DebugQueueStepFunction` | Host | Отладка очередей |
| S2 | deficit | `deficit_check_step.py` | `DeficitCheckStepFunction` | Host | Проверка дефицита |
| S3 | drain | `mp2_drain_units.py` | `MP2DrainUnitsHostFunction` | Host | Дренаж в ClickHouse |

---

## 🏗️ Архитектурные принципы

### Двухфазная архитектура (Two-Phase)

**Ключевое отличие от Limiter V7:** Используется `intent_state` для отложенных переходов.

```cpp
// Phase 1: CHECK (только чтение MacroProperty)
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_check, ...) {
    // Читаем mp_planer_in_ops_history, mp_slots
    // Устанавливаем want_assign = 1 если нашли планер
}

// Phase 2: ACTIVATE (только атомарные записи)
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_activate, ...) {
    if (want_assign == 1) {
        mp_slots[pos]++;  // Атомарно захватываем слот
        aircraft_number = target_ac;
    }
}
```

**Причина:** FLAME GPU запрещает смешивать read и atomic write в одном слое.

### Трёхуровневая приоритетная FIFO

```
┌─────────────────────────────────────────────────────────────────┐
│  ПРИОРИТЕТ 1: SERVICEABLE (state=3, active=1)                    │
│  ─────────────────────────────────────────────────────────────── │
│  mp_svc_head/tail[group_by]                                      │
│  Готовые агрегаты на складе                                      │
│                                                                  │
│  ПРИОРИТЕТ 2: RESERVE (state=5, active=1)                        │
│  ─────────────────────────────────────────────────────────────── │
│  mp_rsv_head/tail[group_by], mp_rsv_count[group_by]              │
│  Агрегаты после ремонта                                          │
│                                                                  │
│  ПРИОРИТЕТ 3: SPAWN (state=5, active=0)                          │
│  ─────────────────────────────────────────────────────────────── │
│  Активация нового агрегата: sne=0, ppr=0                         │
└─────────────────────────────────────────────────────────────────┘
```

### Детекция выхода планера (planer_exit)

Когда планер уходит из operations (2→4, 2→6, 2→3...):
1. `mp_planer_in_ops_history[day * MAX_PLANERS + planer_idx]` становится `0`
2. Агрегат на этом планере детектит изменение
3. Агрегат отцепляется (`aircraft_number = 0`) и уходит в serviceable
4. `mp_planer_slots` декрементируется

### Состояния агрегатов

| ID | State | Описание | Переходы |
|----|-------|----------|----------|
| 2 | operations | На планере в эксплуатации | →3, →4, →6 |
| 3 | serviceable | Исправен, на складе | →2 |
| 4 | repair | В ремонте (PPR >= OH) | →5 |
| 5 | reserve | После ремонта / spawn-слот | →2 |
| 6 | storage | Списан (SNE >= LL) | терминал |

**Отличие от планеров:** Нет `inactive` (1). Новые агрегаты создаются в `reserve`.

### MacroProperty

| Переменная | Размер | Описание |
|------------|--------|----------|
| `mp_planer_dt[MAX_PLANERS * MAX_DAYS]` | UInt32 | dt планеров по дням |
| `mp_planer_in_ops_history[MAX_PLANERS * MAX_DAYS]` | UInt8 | История состояний планеров |
| `mp_planer_type[MAX_PLANERS]` | UInt8 | Тип планера (1=Mi-8, 2=Mi-17) |
| `mp_planer_slots[MAX_GROUPS * MAX_PLANERS]` | UInt32 | Занятые слоты по группам |
| `mp_svc_head/tail[MAX_GROUPS]` | UInt32 | FIFO serviceable |
| `mp_rsv_head/tail[MAX_GROUPS]` | UInt32 | FIFO reserve |
| `mp_rsv_count[MAX_GROUPS]` | UInt32 | Счётчик свободных в reserve |
| `mp_request_count[MAX_GROUPS]` | UInt32 | Запросы на замену |
| `mp_replacement_request[MAX_FRAMES]` | UInt32 | aircraft_number для замены |
| `mp_ac_to_idx[MAX_AC_NUMBER]` | UInt32 | Маппинг AC → planer_idx |
| `mp_idx_to_ac[MAX_PLANERS]` | UInt32 | Маппинг planer_idx → AC |

### Условия переходов

```cuda
// operations → repair (2→4)
if (oh > 0u && ppr >= oh) {
    intent_state = 4u;
    // Создаём запрос замены в mp_replacement_request
}

// operations → storage (2→6)  
// FIX 14.01.2026: Добавлена проверка ppr >= oh!
if ((ll > 0u && sne >= ll) || (ppr >= oh && br > 0u && sne >= br)) {
    intent_state = 6u;
}

// repair → reserve (4→5)
if (repair_days >= repair_time) {
    intent_state = 5u;
    ppr = 0u;  // Обнуление после ремонта
    mp_rsv_count[group_by]++;
}
```

---

## 📁 Файлы

| Файл | Описание |
|------|----------|
| `orchestrator_units.py` | Главный оркестратор |
| `base_model_units.py` | Модель агента, MacroProperty |
| `agent_population_units.py` | Инициализация популяции |
| `rtc_units_planer_exit.py` | Детекция выхода планера |
| `rtc_units_demand_host.py` | HostFunction расчёта дефицита |
| `rtc_units_states_stub.py` | Инициализация intent |
| `rtc_units_increment.py` | Инкремент SNE/PPR, check_limits |
| `rtc_units_state_repair.py` | Логика ремонта |
| `rtc_units_transition_ops.py` | Переходы из operations |
| `rtc_units_fifo_priority.py` | Трёхуровневая FIFO |
| `rtc_units_assembly.py` | Комплектация (10 проходов) |
| `rtc_units_transition_reserve.py` | Переходы из reserve |
| `rtc_units_transition_serviceable.py` | Переходы из serviceable |
| `rtc_units_transition_storage.py` | Терминальное состояние |
| `rtc_units_mp2_writer.py` | Запись результатов |
| `planer_dt_loader.py` | Загрузка dt из ClickHouse |
| `init_planer_dt.py` | InitFunction для dt |
| `init_fifo_queues.py` | InitFunction для FIFO |
| `mp2_drain_units.py` | Дренаж в ClickHouse |

---

## 📈 Результаты (DS1, 2025-07-04)

| Метрика | Значение |
|---------|----------|
| Шаги | **3650** |
| Время | **149.56с** |
| Агентов | **1989** (905 существующих + 1084 spawn-слотов) |
| RTC функций | **42+** |

**Финальная статистика (день 3639):**

| State | Mi-8 (g=3) | Mi-17 (g=4) |
|-------|------------|-------------|
| operations | 193 | 153 |
| serviceable | 0 | 0 |
| repair | 0 | 1 |
| reserve | 0 | 0 |
| storage | 327 | 248 |
| spawn | 2 | 75 |
| **ВС в ops** | **47** | **129** |
| **deficit** | **+99** | **-105** |

**Общий итог:** 346 двигателей в ops для 352 нужных = **дефицит -6**.

**Spawn работает!** 77 новых двигателей создано (2 Mi-8 + 75 Mi-17).

---

## 🔗 Связанная документация

- `docs/rtc_pipeline_architecture.md` — Архитектура планеров (baseline)
- `Helicomponents-messaging/docs/limiter_architecture.md` — V7 Limiter
- `.cursor/rules/*.mdc` — Правила проекта

---

*Документ обновлён: 15-01-2026*  
*Статус: ✅ Исправлен баг с многократным назначением агрегатов (PropertyArray API)*  
*Тест: DS1, 50 дней, MAX=2 (Mi-8), MAX=3 (Mi-17 — race condition)*
