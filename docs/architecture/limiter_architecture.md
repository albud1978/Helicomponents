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
> 5. **RepairLine slots** — `v8_repair_line_slots` обязан идти ПОСЛЕ `v8_repair_line_publish_status` и ДО P2/P3 (`v8_promote_*`), иначе слоты будут устаревшими

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
| 13 | v8_init | `HF_InitV8` | Host | Подготовка `deterministic_dates` и синхронизация состояния |
| 14 | v8_collect_min_ops | `rtc_collect_min_dynamic_ops_v8` | 2 | Сбор минимального лимитера по ops |
| 15 | v8_collect_min_repair | `rtc_collect_min_dynamic_repair_v8` | 4 | Сбор минимальных `repair_days` для day0‑ремонта |
| 16 | v8_compute_global_min | `rtc_compute_global_min_v8` | QM | Вычисление `adaptive_days` и сброс `min_dynamic` |
| **ФАЗА 1.3: Update day (вариант B)** |||||
| 17 | v8_update_day | `HF_UpdateDayV8` | Host | Update day ДО квотирования |
| **ФАЗА 1.5: RepairLine (pre‑quota)** |||||
| 18 | v8_repair_line_sync_pre | `rtc_repair_line_sync_v8` | RepairLine | Синхронизация линии из MacroProperty |
| 19 | v8_repair_line_increment | `rtc_repair_line_increment_v8` | RepairLine | Наращивание `free_days` на шаг |
| 20 | v8_repair_line_write | `rtc_repair_line_write_v8` | RepairLine | Запись состояния линий в MacroProperty |
| 21 | v8_repair_line_publish_status | `rtc_repair_line_publish_status_v8` | RepairLine | Сообщение линий в QM (RepairLineStatus) |
| **ФАЗА 2: Квотирование (MessageBucket)** |||||
| 22 | v8_reset_flags | `rtc_reset_flags_v8_*` | all | Сброс флагов промоута/демоута |
| 23 | v8_reset_buffers | `rtc_reset_quota_v8_*` | all | Сброс буферов подсчёта |
| 24 | v8_count_ops | `rtc_count_ops_v8` | 2 | Подсчёт ops |
| 25 | v8_count_svc | `rtc_count_svc_v8` | 3 | Подсчёт serviceable |
| 26 | v8_count_unsvc | `rtc_count_unsvc_v8` | 7 | Подсчёт unserviceable (readiness по `repair_days`) |
| 27 | v8_count_inactive | `rtc_count_inactive_v8` | 1 | Подсчёт inactive |
| 28 | v8_quota_manager_bucket | `rtc_quota_manager_v8_bucket` | QM | QuotaManager → QuotaBucket (key=0) |
| 29 | v8_demote | `rtc_demote_ops_v8` | 2 | Решение демоута ops→svc |
| 30 | v8_promote_svc_bucket | `rtc_promote_svc_bucket_v8` | 3 | P1 решение по rank (MessageBucket) |
| 31 | v8_promote_unsvc_bucket | `rtc_promote_unsvc_bucket_v8` | 7 | P2 решение по rank (MessageBucket) |
| 32 | v8_promote_inactive_bucket | `rtc_promote_inactive_bucket_v8` | 1 | P3 решение по rank (MessageBucket) |
| 33 | v8_promote_unsvc_commit | `rtc_promote_unsvc_commit_v8` | 7 | Commit P2: бронирование RepairLine |
| 34 | v8_promote_inactive_commit | `rtc_promote_inactive_commit_v8` | 1 | Commit P3: бронирование RepairLine |
| **ФАЗА 3: Применение квот** |||||
| 35 | v7_ops_demote | `rtc_ops_demote_v7` | 2→3 | Применение демоута |
| 36 | v7_svc_to_ops | `rtc_svc_to_ops_v7` | 3→2 | Применение P1 |
| 37 | v7_unsvc_to_ops | `rtc_unsvc_to_ops_v7` | 7→2 | Применение P2, обнуление PPR |
| 38 | v7_inactive_to_ops | `rtc_inactive_to_ops_v7` | 1→2 | Применение P3 |
| **ФАЗА 3.5: RepairLine (post‑quota)** |||||
| 39 | v8_repair_line_sync_post | `rtc_repair_line_sync_post_v8` | RepairLine | Синхронизация линий после квот |
| **ФАЗА 3.7: Post‑quota пересчёт** |||||
| 40 | v8_reset_buffers_post_quota | `rtc_reset_quota_v8_post_*` | all | Сброс буферов после пост‑квотных переходов |
| 41 | v8_count_agents_post_quota | `rtc_count_*_v8_post` | all | Подсчёт ops/svc/unsvc/inactive после квот |
| 42 | v8_promote_inactive_post | `rtc_promote_inactive_post_v8` | 1 | Доп. добор из inactive после пост‑квотных переходов |
| 43 | v8_inactive_to_ops_post | `rtc_inactive_to_ops_post_v8` | 1→2 | Применение post‑добора inactive |
| **ФАЗА 3.8: Spawn counts** |||||
| 44 | v8_reset_buffers_spawn | `rtc_reset_quota_v8_spawn_*` | all | Сброс буферов перед spawn |
| 45 | v8_count_agents_spawn | `rtc_count_*_v8_spawn` | all | Подсчёт ops/svc/unsvc/inactive перед spawn |
| **ФАЗА 4: Динамический спавн** |||||
| 46 | v8_spawn_dynamic_mgr | `rtc_spawn_dynamic_mgr_v8` | SpawnMgr | Дефицит = target − curr_ops − used (P1/P2/P3 commit) |
| 47 | v8_spawn_dynamic_ticket | `rtc_spawn_dynamic_ticket_v8` | Ticket→ops | Создание новых агентов (Mi‑17) |
| 48 | v8_spawn_dynamic_ticket_mi8 | `rtc_spawn_dynamic_ticket_v8_mi8` | Ticket→ops | Создание новых агентов (Mi‑8) |
| **ФАЗА 5: Limiter (бинарный поиск)** |||||
| 49 | L_limiter_entry | `rtc_compute_limiter_on_entry` | 2→2 | Пересчёт limiter при входе/нулевом значении |
| 50 | L_limiter_min | `rtc_compute_min_limiter` | 2→2 | Сбор минимального limiter по ops |

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
- Используется **только message‑only квотирование** (`register_quota_v8_messages`). Альтернативные agent‑based P2/P3 (`rtc_promote_unsvc_v8`/`rtc_promote_inactive_v8` из legacy‑ветки) **не применять**.

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

V7 использует большие MacroProperty буферы `promote[MAX_AGENTS]` для передачи решений QM → агенты. V8 заменяет их на один broadcast MessageBucket (`QuotaBucket`, key=0). Динамический spawn остаётся на MacroProperty (`spawn_dynamic_*`).

### Типы сообщений FLAME GPU

| Тип | Сложность | Применение | Использование в V8 |
|-----|-----------|------------|-------------------|
| **MessageNone** | O(1) | Без коммуникации | Текущий подход |
| **MessageBruteForce** | O(N²) | Все → Все | ⚠️ PlanerReport (legacy, дорогой) |
| **MessageBucket** | O(N/K) | По ключу | ✅ QuotaBucket (key=0) |
| **MessageArray** | O(1) | По индексу | ✅ RepairLineStatus; QuotaDecisionArray (legacy) |
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
│  Слой 2: РЕШЕНИЯ QM → MessageBucket (QuotaBucket)                       │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ QM → MessageBucket[key=0] = {promote_p1/p2/p3, deficit}            │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  Слой 3: ПРИМЕНЕНИЕ (агенты читают сообщения)                           │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ Агенты читают MessageBucket[key=0]                                 │ │
│  │ Берут свой promote_* по group_by и rank                            │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  Слой 4: ДИНАМИЧЕСКИЙ SPAWN (MacroProperty)                             │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ SpawnMgr → MacroProperty spawn_dynamic_* (need/base_idx/base_acn)  │ │
│  │ SpawnTickets читают MacroProperty, создают агентов                 │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Сообщения V8

#### 1. Квотирование (promote/demote)

```cpp
// Определение
MessageBucket quota_msg;
quota_msg.setBounds(0, 1);  // один broadcast (key=0)
quota_msg.newVariable<uint>("promote_p1_mi8");
quota_msg.newVariable<uint>("promote_p1_mi17");
quota_msg.newVariable<uint>("promote_p2_mi8");
quota_msg.newVariable<uint>("promote_p2_mi17");
quota_msg.newVariable<uint>("promote_p3_mi8");
quota_msg.newVariable<uint>("promote_p3_mi17");
quota_msg.newVariable<uint>("deficit_mi8");
quota_msg.newVariable<uint>("deficit_mi17");

// QM отправляет
FLAMEGPU->message_out.setKey(0);
FLAMEGPU->message_out.setVariable<uint>("promote_p1_mi8", p1_8);
// ...

// Агент читает
for (auto &msg : FLAMEGPU->message_in(0)) {
    uint promote = (group_by == 1u)
        ? msg.getVariable<uint>("promote_p1_mi8")
        : msg.getVariable<uint>("promote_p1_mi17");
    if (rank < promote) {
        // Promote: serviceable → operations
    }
    break;
}
```

#### 2. Динамический spawn (MacroProperty)

```cpp
// SpawnMgr пишет параметры по дню
spawn_dynamic_need_*[day] = need;
spawn_dynamic_base_idx_*[day] = next_idx;
spawn_dynamic_base_acn_*[day] = next_acn;

// SpawnTicket читает параметры
need = spawn_dynamic_need_*[day];
base_idx = spawn_dynamic_base_idx_*[day];
base_acn = spawn_dynamic_base_acn_*[day];
if (ticket < need) {
    // Создаём агента
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

