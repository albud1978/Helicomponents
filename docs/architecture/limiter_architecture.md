# LIMITER Architecture (V8 — RepairLine)

> **Актуальная версия:** V8 (16-01-2026)  
> **Файл оркестратора:** `code/sim_v2/messaging/orchestrator_limiter_v8.py`
> **Резервная версия:** V7 (`orchestrator_limiter_v7.py`)

---

## 🔑 АРХИТЕКТУРНЫЕ РЕШЕНИЯ V8 (КАНОН)

> **Источник истины для V8. Любые противоречия в других документах — устаревшие.**

| Вопрос | V7 | V8 | Обоснование V8 |
|--------|-----|-----|----------------|
| **Механизм ремонта** | exit_date для каждого unsvc | RepairLine (free_days) | общий пул, число линий = repair_number из MP |
| **exit_date для unsvc** | ✅ Используется | ❌ УДАЛЁН | Заменён на RepairLine‑проверку |
| **repair_days для unsvc** | — | ✅ ИСПОЛЬЗУЕТСЯ | unsvc декрементирует repair_days до 0; готовность исключает уже назначенные `repair_line_id` |
| **repair_days для inactive** | — | ✅ ВСЕГДА 0 | inactive не декрементируется |
| **unsvc в min_dynamic** | ✅ Да | ❌ НЕТ | Управляется через RepairLine |
| **Правило ресурса** | post-increment (`sne += dt; if sne >= ll`) | next-day dt (`if sne + dt >= ll`) | Предотвращение переналёта |
| **limiter=0** | min(1, ...) в Python | **Обязательный выход** (иначе EXCEPTION) | Гарантия корректности |
| **limiter инициализация** | max(1, ...) | Разрешён 0 | Консистентность с RTC |

**⚠️ ВАЖНО: V8 НЕ эквивалентен V7 по переходам — это осознанное архитектурное решение.**

**⚠️ Ограничение deterministic_dates:** `MAX_DETERMINISTIC_DATES=500`. При превышении лимита лишние даты **отбрасываются**, события могут быть потеряны.

**⚠️ V8 квоты (сообщения):** один QM шлёт broadcast через MessageBucket (`QuotaBucket`), агенты принимают решения по rank.

### Канонические артефакты V8
- `config/transitions/transitions_rules.json` — единая матрица переходов state→state
- `config/transitions/quota_rules.json` — логика квотирования (MessageBucket/RepairLine)
- `tools/transitions_viewer/index.html` — визуализация переходов и квот

---

## 📊 Таблица слоёв модели (фактический порядок V8)

> **⚠️ ВАЖНО: Порядок = хронология выполнения**
> 
> Номера (#) в таблице соответствуют **порядку регистрации слоёв в модели** — функции выполняются именно в этом порядке. Это критически важно для понимания алгоритма:
> 
> 1. **Зависимости данных** — слой N читает данные, записанные слоем N-1
> 2. **Reset перед сбором** — `reset_exit_date` (→MAX) должен быть ДО `copy_exit_date` (atomicMin), иначе останутся данные предыдущего шага
> 3. **Квотирование** — строгая последовательность: подсчёт → решения QM → применение к агентам
> 4. **Adaptive steps** — источники `min_dynamic` и `deterministic_dates` должны быть готовы ДО вычисления `adaptive_days`

> **Оптимизации:**
> - Функции `_stay` удалены — FLAME GPU автоматически оставляет агентов в состоянии
> - `clear_limiter_on_exit` удалён — обнуление уже в функциях 2→3, 2→6, 2→7
> - `limiter_on_entry` упрощён — проверяет только `limiter==0` (без intent)

| # | Слой | Функция | State | Логика |
|---|------|---------|-------|--------|
| **ФАЗА -1: Инициализация (Host)** |||||
| 0 | layer_init_mp5_cumsum | `HF_InitMP5Cumsum` | Host | Готовит кумулятивные часы MP5 для лимитера |
| 1 | layer_init_repair_lines | `HF_InitRepairLines` | Host | Инициализирует RepairLine по `repair_number` |
| **ФАЗА 0: Детерминированные переходы** |||||
| 2 | v8_repair_line_assign_repair | `rtc_repair_line_assign_repair_v8` | 4→4 | Привязка линий для day0‑ремонта |
| 3 | v7_repair_to_svc | `rtc_repair_to_svc_v7` | 4→3 | Завершение day0‑ремонта по `exit_date` |
| 4 | v7_spawn_to_ops | `rtc_spawn_to_ops_v7` | 5→2 | Плановый spawn по `exit_date` |
| **ФАЗА 0.5: Сбор min_exit_date (совместимость)** |||||
| 5 | v7_reset_exit_date | `rtc_reset_exit_date_v7` | QM | Сброс min_exit_date перед сбором |
| 6 | v7_copy_exit_date_repair | `rtc_copy_exit_date_repair_v7` | 4 | Сбор ближайшего выхода из ремонта |
| 7 | v7_copy_exit_date_spawn | `rtc_copy_exit_date_spawn_v7` | 5 | Сбор ближайшего spawn‑события |
| 8 | v7_copy_exit_date_unsvc | `rtc_copy_exit_date_unsvc_v7` | 7 | Сбор exit_date из unsvc (в V8 не влияет на шаг) |
| **ФАЗА 1: Operations и ремонтные счётчики** |||||
| 9 | v8_ops_increment | `rtc_ops_increment_v8` | 2→2 | Начисление налёта и ресурса, шаговый декремент лимитера |
| 10 | v8_unsvc_decrement | `rtc_unsvc_decrement_v8` | 7→7 | Декремент `repair_days` для unserviceable |
| 11 | v8_ops_to_storage | `rtc_ops_to_storage_v8` | 2→6 | Списание по LL/BR |
| 12 | v8_ops_to_unsvc | `rtc_ops_to_unsvc_v8` | 2→7 | Уход в unserviceable по OH (limiter=0 → выход) |
| **ФАЗА 1.25: V8 pre‑quota adaptive (min_dynamic)** |||||
|  |  |  |  | `min_dynamic` кодируется с источником (limiter/repair_days) для явной причины шага |
| 13 | v8_init | `HF_InitV8` | Host | Подготовка `deterministic_dates` и синхронизация состояния |
| 14 | v8_collect_min_ops | `rtc_collect_min_dynamic_ops_v8` | 2 | Сбор минимального лимитера по ops |
| 15 | v8_collect_min_repair | `rtc_collect_min_dynamic_repair_v8` | 4 | Сбор минимальных `repair_days` для day0‑ремонта |
| 16 | v8_compute_global_min | `rtc_compute_global_min_v8` | QM | Вычисление `adaptive_days` и сброс `min_dynamic` |
| **ФАЗА 1.5: RepairLine (pre‑quota)** |||||
| 17 | v8_repair_line_sync_pre | `rtc_repair_line_sync_v8` | RepairLine | Синхронизация линии из MacroProperty |
| 18 | v8_repair_line_increment | `rtc_repair_line_increment_v8` | RepairLine | Наращивание `free_days` на шаг |
| 19 | v8_repair_line_write | `rtc_repair_line_write_v8` | RepairLine | Запись состояния линий в MacroProperty |
| 20 | v8_repair_line_publish_status | `rtc_repair_line_publish_status_v8` | RepairLine | Сообщение линий в QM (готовность/занятость) |
| **ФАЗА 2: Квотирование** |||||
| 21 | v8_reset_flags | `rtc_reset_flags_v7` | all | Сброс флагов промоута/демоута |
| 22 | v8_reset_buffers | `rtc_reset_buffers_v7` | all | Сброс буферов подсчёта |
| 23 | v8_count_agents | `rtc_count_*` | all | Подсчёт по состояниям + готовность unsvc/inactive |
| 24 | v8_repair_line_slots | `rtc_repair_line_slots_v8` | QM | Сбор доступных RepairLine‑слотов |
| 25 | v8_debug_p2 | `rtc_quota_debug_p2_v8` | QM | Debug‑метрики P2 (ops/target/deficit/needed/slots) |
| 26 | v8_demote | `rtc_demote_ops_v7` | QM | Решение демоута ops→svc |
| 27 | v8_promote_svc | `rtc_promote_svc_v7` | QM | Решение P1: svc→ops |
| 28 | v8_promote_unsvc_decide | `rtc_promote_unsvc_v8` | QM | Решение P2: отбор unsvc по RepairLine (без повторного `repair_line_id`) |
| 29 | v8_promote_unsvc_commit | `rtc_promote_unsvc_commit_v8` | QM | Бронирование линии и фиксация P2 (fallback на следующий слот) |
| 30 | v8_promote_inactive_decide | `rtc_promote_inactive_v8` | QM | Решение P3: отбор inactive по условиям RepairLine |
| 31 | v8_promote_inactive_commit | `rtc_promote_inactive_commit_v8` | QM | Бронирование линии и фиксация P3 (fallback на следующий слот) |
| **ФАЗА 3: Применение квот** |||||
| 32 | v7_ops_demote | `rtc_ops_demote_v7` | 2→3 | Применение демоута |
| 33 | v7_svc_to_ops | `rtc_svc_to_ops_v7` | 3→2 | Применение P1 |
| 34 | v7_unsvc_to_ops | `rtc_unsvc_to_ops_v7` | 7→2 | Применение P2, обнуление PPR |
| 35 | v7_inactive_to_ops | `rtc_inactive_to_ops_v7` | 1→2 | Применение P3 |
| **ФАЗА 3.5: RepairLine (post‑quota)** |||||
| 36 | v8_repair_line_sync_post | `rtc_repair_line_sync_v8` | RepairLine | Синхронизация линий после квот |
| **ФАЗА 4: Динамический спавн** |||||
| 37 | v8_spawn_dynamic_mgr | `rtc_spawn_dynamic_mgr_v8` | SpawnMgr | Дефицит = target − curr_ops − used (P1/P2/P3 approve) |
| 38 | v8_spawn_dynamic_ticket | `rtc_spawn_dynamic_ticket_v7` | Ticket→ops | Создание новых агентов |
| **ФАЗА 5: Limiter (min_limiter)** |||||
| 39 | L_limiter_entry | `rtc_compute_limiter_on_entry` | 2→2 | Пересчёт limiter при входе/нулевом значении |
| 40 | L_limiter_min | `rtc_compute_min_limiter` | 2→2 | Сбор минимального limiter по ops |
| **ФАЗА 6: Update day** |||||
| 41 | v8_update_day | `HF_UpdateDayV8` | Host | Обновление `current_day` по `adaptive_days` |

---

## 🏗️ Архитектурные принципы V7

### Однофазная архитектура (Single-Phase)

**Ключевое отличие от baseline:** Переходы состояний выполняются **напрямую** через `setInitialState/setEndState` с `FunctionCondition`, без промежуточной переменной `intent_state`.

```cpp
// V7: Прямой переход с FunctionCondition
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_ops_to_unsvc_v7) {
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    return ppr >= oh;  // Condition: PPR >= OH
}

FLAMEGPU_AGENT_FUNCTION(rtc_ops_to_unsvc_v7, ...) {
    // Устанавливаем exit_date для ожидания repair_time
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int repair_time = FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    FLAMEGPU->setVariable<unsigned int>("exit_date", current_day + repair_time);
    // PPR сохраняется — обнуление при возврате в ops (unsvc→ops)
    return flamegpu::ALIVE;
}
// Регистрация: fn.setInitialState("operations"); fn.setEndState("unserviceable");
```

### Детерминированные переходы

Агенты в `repair` (4) и `reserve` (5) имеют переменную `exit_date` — день выхода из состояния:

- **repair → serviceable (4→3):** При `current_day >= exit_date`
- **reserve → operations (5→2):** При `current_day >= exit_date` (плановый spawn)

`exit_date` включается в расчёт `adaptive_days` через `min_exit_date_mp`.

### Состояния V7

| ID | State | Описание |
|----|-------|----------|
| 1 | inactive | Неактивный (ожидание комплектации) |
| 2 | operations | В эксплуатации |
| 3 | serviceable | Исправен, на складе |
| 4 | repair | В ремонте |
| 5 | reserve | Резерв / плановый spawn |
| 6 | storage | Хранение (списан) |
| 7 | unserviceable | Неисправен (PPR >= OH, ждёт ремонта) |

### Квотирование

**Приоритеты промоута:**
1. **P1:** serviceable → operations (самый высокий)
2. **P2:** unserviceable → operations (PPR=0)
3. **P3:** inactive → operations (самый низкий)
4. **P4:** dynamic spawn (покупка вертолёта)

**V7: Ожидание repair_time через exit_date:**
- При переходе `ops → unserviceable` устанавливается `exit_date = current_day + repair_time`
- P2 промоут проверяет `current_day >= exit_date` перед возвратом в ops

**V8: Ожидание через RepairLine:**
- unsvc декрементирует `repair_days` до 0
- P2/P3 проверяют одновременно: `day >= repair_time`, `repair_days == 0`, линия с `free_days >= repair_time`
- Если условия не выполнены → P4 (spawn)
- См. `docs/architecture/adaptive_steps_logic.md` для деталей

**Демоут:** operations → serviceable (при избытке)

### Адаптивные шаги

**V7:**
```

---

## ✅ Финальная архитектура квотирования V8 (message‑only, единый модуль)

### Базовые принципы
- Target‑квоты **по типам** (Mi‑8/Mi‑17 считаются отдельно).
- RepairLine‑квота **общая** для обоих типов.
- QM читает RepairLine из MacroProperty (`repair_line_free_days_mp`/`repair_line_acn_mp`) после pre‑quota sync.
- Приоритет промоутов по idx: **больше idx → моложе → выше приоритет**.
- Storage — терминальный: **не участвует** и не шлёт сообщений.
- Spawn‑индексы выдаёт **один SpawnMgr** (плотный выделенный диапазон).
- Ранний выход: **если 0 агентов/сообщений или квота=0**, модуль сразу завершает работу.
- Выбор RepairLine: **минимальный free_days при условии free_days ≥ repair_time** (анти‑фрагментация).

### Каскад квот (единый модуль, несколько слоёв)
1) **Reset/Prepare** — сброс флагов у агентов.  
2) **Announce (messages)** — агенты шлют состояние; RepairLine шлёт line_id/free_days.  
3) **Balance (QM)** — считает `quota_left_mi8/mi17` и `repair_quota_left`.  
4) **Demote (по типам)** — демоут при избытке.  
5) **P1 (svc→ops, по типам)** — уменьшает `quota_left_*`.  
6) **P2 (unsvc→ops, общий пул)** — единая очередь по idx + назначение линий.  
7) **P3 (inactive→ops, общий пул после P2)** — аналогично P2, слой выполняется после P2.  
8) **Spawn (P4, по типам)** — остатки `quota_left_*`.
adaptive_days = min(min_limiter, days_to_program_change, days_to_exit_date)
```

**V8 (упрощённо):**
```
adaptive_days = min(min_dynamic, days_to_deterministic)
```

| V7 Источник | V8 Источник | Что означает |
|-------------|-------------|--------------|
| `min_limiter` | `min_dynamic` | MIN(limiter) для ops + repair |
| `days_to_program_change` | `deterministic_dates[]` | Один MacroProperty со всеми датами |
| `days_to_exit_date` (repair/spawn/**unsvc**) | `deterministic_dates[]` | **V8: unsvc НЕ участвует** |

### Логика reset-функций (критично для корректности)

**Проблема:** Если на шаге N нет агентов в repair, то `atomicMin` не вызывается и в `min_exit_date_mp` остаётся старое значение с шага N-1.

**Решение:** Reset перед сбором → MAX означает "нет данных".

```
┌─────────────────────────────────────────────────────────────────────────┐
│ ШАГ N                                                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ФАЗА 0: ДЕТЕРМИНИРОВАННЫЕ ПЕРЕХОДЫ                                     │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ [0] repair_to_svc      → агенты exit_date <= current_day: 4→3     │ │
│  │ [1] spawn_to_ops       → агенты exit_date <= current_day: 5→2     │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  ФАЗА 0.5: СБРОС + СБОР min_exit_date (ПОСЛЕ переходов!)                │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ [2] reset_exit_date    → min_exit_date_mp = 0xFFFFFFFF (MAX)       │ │
│  │ [3] copy_exit_repair   → ОСТАВШИЕСЯ repair: atomicMin(exit_date)  │ │
│  │ [4] copy_exit_spawn    → ОСТАВШИЕСЯ reserve: atomicMin(exit_date) │ │
│  │                                                                    │ │
│  │ Результат: min_exit_date_mp = MIN(exit_date оставшихся) или MAX   │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  ФАЗА 4: СБОР min_limiter                                               │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ [20] min_limiter       → агенты ops: atomicMin(limiter)           │ │
│  │                                                                    │ │
│  │ Результат: mp_min_limiter = MIN(все limiter) или MAX              │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  ФАЗА 5: ВЫЧИСЛЕНИЕ adaptive_days                                       │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ [22] compute_global    → ЧИТАЕТ: mp_min_limiter, min_exit_date_mp │ │
│  │                        → ПИШЕТ:  adaptive_days                    │ │
│  │                                                                    │ │
│  │ [23] reset_min_limiter → mp_min_limiter = MAX (для шага N+1)      │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**Пример без reset (БАГ):**
```
Шаг 10: Агент A в repair, exit_date=150 → min_exit_date_mp = 150
Шаг 11: Агент A вышел (4→3), нет агентов в repair
        БЕЗ RESET: min_exit_date_mp = 150 (старое!)
        → adaptive_days некорректно = 150 - current_day
```

**Пример с reset (КОРРЕКТНО):**
```
Шаг 10: Агент A в repair, exit_date=150 → min_exit_date_mp = 150
Шаг 11: RESET → min_exit_date_mp = MAX
        Нет агентов в repair → atomicMin не вызывается
        min_exit_date_mp = MAX → игнорируется в compute_global_min
```

**MAX = "источник не влияет на adaptive_days"**

---

## 📁 Файлы V7

| Файл | Описание |
|------|----------|
| `orchestrator_limiter_v7.py` | Главный оркестратор V7 |
| `rtc_state_transitions_v7.py` | RTC переходы состояний |
| `rtc_quota_v7.py` | RTC квотирование |
| `rtc_spawn_dynamic_v7.py` | **Динамический спавн Mi-17** |
| `rtc_limiter_v5.py` | GPU-only адаптивные шаги |
| `rtc_limiter_optimized.py` | Бинарный поиск limiter |
| `base_model_messaging.py` | Модель агента |

---

## 📈 Результаты V7

| Метрика | Значение |
|---------|----------|
| Шаги | **192** |
| Время GPU | **1.85с** |
| Скорость | **1976 дней/сек** |
| GPU | 100% |
| Архитектура | Single-phase |
| RTC функций | **29** (27 + 2 dynamic spawn) |

**Финальная статистика (3650 дней, датасет 2025-07-04):**
| State | Агентов | Примечание |
|-------|---------|------------|
| inactive | 94 | P3 промоутит по дефициту |
| operations | **173** | Target покрыт |
| serviceable | 0 | P1 полностью использован |
| repair | 0 | Все вышли из ремонта |
| reserve | 0 | Spawn активирован |
| storage | 35 | Достигли LL/BR |
| unserviceable | 11 | Ожидают repair_time |
| **ВСЕГО** | **313** (279 + 34 dynamic spawn) |

**Program changes:**
- Всего program_changes: **92** (за 10 лет)
- Попадают точно в adaptive steps: **7** (остальные "перепрыгиваются" более близкими limiter/exit_date событиями)
- Это **корректное поведение** — V7 прыгает к ближайшему событию

**Динамический спавн Mi-17:**
- Менеджер следит за дефицитом Mi-17 (target vs ops_count)
- При дефиците выдаёт "тикеты" на создание новых агентов
- Тикеты создают агентов через `agent_out` напрямую в `operations`
- Резерв: 50 динамических слотов

**Исправленные баги (14-01-2026):**
1. **Race condition в `rtc_compute_global_min`** — только group_by=1 (Mi-8) выполняет вычисления
2. **`num_program_changes` не обновлялся** — добавлен `setPropertyUInt` в except блок
3. **`v7_reset_buffers` для всех состояний** — регистрация для 7 состояний (не только operations)

---

## 🔗 Связанная документация

- `docs/architecture/rtc_pipeline_architecture.md` — Baseline архитектура (intent-based)
- `docs/architecture/validation_rules.md` — Инварианты и тесты
- `.cursor/rules/` — Правила проекта

---

## ✅ V8: RepairLine (АКТУАЛЬНАЯ АРХИТЕКТУРА)

> **Статус:** основная архитектура  
> **Документация:** `docs/architecture/adaptive_steps_logic.md`  
> **Цель:** квотирование ремонта через RepairLine + адаптивные шаги

### Ключевые изменения V8 vs V7

| Аспект | V7 | V8 |
|--------|-----|-----|
| unsvc в min_dynamic | ✅ Да (exit_date) | ❌ Нет |
| unsvc декремент | ✅ repair_days | ✅ repair_days |
| P2/P3 условие | `current_day >= exit_date` | day >= repair_time, repair_days == 0, RepairLine.free_days ≥ repair_time |
| Квота ремонта | Через exit_date каждого unsvc | repair_number из MP (число линий) |

### RepairLine (repair_number → число линий)

**Назначение:** Управление квотой ремонта через линии (free_days), число линий = repair_number из MP

```
```
RepairLine (для каждой линии):
  free_days += adaptive_days   // всегда
  доступна при free_days >= repair_time
  при назначении: free_days = 0, aircraft_number = acn (однократно)
  слоты: aircraft_number == 0 И free_days >= repair_time, без повтора acn в соседние дни
```

### Протокол сообщений (адресные, внутри одного шага)

```
Слой 2: QuotaManager решает
  - Определяет дефицит, approved = MIN(дефицит, lines_available)
  - P2: unsvc по idx (первые approved), только если free_days >= repair_time
  - P3: inactive по idx (если остался дефицит), только если free_days >= repair_time
  - P4: spawn (если P2/P3 недоступны)
  - подтверждённым линиям: free_days = 0, aircraft_number = acn
```

### Мотивация (из V7)

V7 использует большие MacroProperty буферы `promote[MAX_AGENTS]` для передачи решений QM → агенты. V8 заменяет их на адресные сообщения `MessageBucket`.

### Типы сообщений FLAME GPU

| Тип | Сложность | Применение | Использование в V8 |
|-----|-----------|------------|-------------------|
| **MessageNone** | O(1) | Без коммуникации | Текущий подход |
| **MessageBruteForce** | O(N²) | Все → Все | ❌ Запрещён |
| **MessageBucket** | O(N/K) | По ключу | ✅ Квотирование |
| **MessageArray** | O(1) | По индексу | Возможно для spawn |
| **MessageSpatial** | O(K) | По радиусу | Не применимо |

### Архитектура V8

```
┌─────────────────────────────────────────────────────────────────────────┐
│ КВОТИРОВАНИЕ V8 (MessageBucket)                                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Слой 1: СБОР (MacroProperty — оставляем)                               │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ count_agents  → atomicAdd счётчики по state/group                  │ │
│  │ collect_idx   → запись idx в очереди для вычисления threshold     │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  Слой 2: РЕШЕНИЯ QM → MessageBucket                                     │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ QM_mi8  → MessageBucket[key=1] = {quotas, thresholds}              │ │
│  │ QM_mi17 → MessageBucket[key=2] = {quotas, thresholds}              │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  Слой 3: ПРИМЕНЕНИЕ (агенты читают сообщения)                           │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ Агенты читают MessageBucket[свой group_by]                         │ │
│  │ Берут threshold по своему state                                    │ │
│  │ if (my_idx >= threshold) → promote                                 │ │
│  │ if (my_idx < demote_threshold) → demote                            │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  Слой 4: ДИНАМИЧЕСКИЙ SPAWN (отдельное сообщение)                       │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ SpawnMgr → MessageBucket[key=100+group] = {need, base_idx}         │ │
│  │ SpawnTickets читают сообщение, создают агентов                     │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Сообщения V8

#### 1. Квотирование (promote/demote)

```cpp
// Определение
MessageBucket quota_msg;
quota_msg.setBounds(1, 2);  // ключи 1=Mi-8, 2=Mi-17
quota_msg.newVariable<int>("quota_s3");      // serviceable
quota_msg.newVariable<int>("quota_s7");      // unserviceable
quota_msg.newVariable<int>("quota_s5");      // inactive
quota_msg.newVariable<uint>("threshold_s3"); // min idx для promote из s3
quota_msg.newVariable<uint>("threshold_s7"); // min idx для promote из s7
quota_msg.newVariable<uint>("threshold_s5"); // min idx для promote из s5
quota_msg.newVariable<uint>("demote_threshold"); // max idx для demote

// QM отправляет
FLAMEGPU->message_out.setKey(my_group_by);  // 1 или 2
FLAMEGPU->message_out.setVariable<int>("quota_s3", quota_serviceable);
// ...

// Агент читает
for (auto &msg : FLAMEGPU->message_in(my_group_by)) {
    uint threshold = msg.getVariable<uint>("threshold_s3");
    if (my_state == 3 && my_idx >= threshold) {
        // Promote: serviceable → operations
    }
}
```

#### 2. Динамический spawn (отдельное сообщение)

```cpp
// Определение
MessageBucket spawn_msg;
spawn_msg.setBounds(101, 102);  // ключи 101=Mi-8 spawn, 102=Mi-17 spawn
spawn_msg.newVariable<int>("need");       // сколько нужно создать
spawn_msg.newVariable<uint>("base_idx");  // базовый idx для новых агентов

// SpawnMgr отправляет
FLAMEGPU->message_out.setKey(100 + my_group_by);
FLAMEGPU->message_out.setVariable<int>("need", deficit);
// ...

// SpawnTicket читает
for (auto &msg : FLAMEGPU->message_in(100 + my_group_by)) {
    int need = msg.getVariable<int>("need");
    // Создаём агентов
}
```

### Преимущества V8

| Аспект | V7 (MacroProperty) | V8 (MessageBucket) |
|--------|-------------------|-------------------|
| Буферы promote[idx] | MAX_AGENTS × 4 байт | Не нужны |
| Передача решений | Запись в глобальную память | Сообщения между агентами |
| Модель | Глобальные массивы | Агентное взаимодействие |
| Адресация | По idx в буфере | По group_by в сообщении |

### Риски V8

1. **Один QM = одно сообщение** — QM не может отправить несколько сообщений за вызов
2. **Threshold вычисление** — QM должен знать распределение idx по состояниям
3. **Синхронизация** — возможны проблемы как с MacroProperty в V7

### Файлы V8

| Файл | Статус | Описание |
|------|--------|----------|
| `orchestrator_limiter_v8.py` | ✅ Создан | Оркестратор V8 |
| `rtc_limiter_v8.py` | ✅ Создан | Adaptive steps с deterministic_dates |
| `rtc_state_transitions_v8.py` | ✅ Создан | Next-day dt проверка (SNE + dt >= LL) |
| `rtc_repair_lines_v8.py` | ✅ Создан | RepairLine (free_days + aircraft_number) |
| `rtc_quota_v8.py` | ✅ Создан | P2/P3 через RepairLine |

### Результаты тестирования V8 (16-01-2026)

| Метрика | V8 | V7 | Изменение |
|---------|-----|-----|-----------|
| Шаги | **186** | 192 | -3% |
| GPU время | **1.80с** | 1.85с | -3% |
| Скорость GPU | **2027 д/с** | 1976 д/с | +3% |
| Агентов | 294 | 313 | -6% |
| Общее время | 5.60с | ~6с | — |

**Примечания:**
- V8 использует RepairLine вместо MacroProperty exit_date для unsvc
- Меньше шагов из-за более агрессивного adaptive step (deterministic_dates)
- 9 агентов spawn (Mi-17)

---

*Документ обновлён: 16-01-2026*  
*Статус V8: ✅ Актуальная архитектура (RepairLine)*
*Статус V7: 📦 Резервная архитектура*

