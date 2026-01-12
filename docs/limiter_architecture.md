# LIMITER Architecture (V7 — Однофазная архитектура)

> **Актуальная версия:** V7 (12-01-2026)  
> **Файл оркестратора:** `code/sim_v2/messaging/orchestrator_limiter_v7.py`

---

## 📊 Таблица RTC модулей (32 функции)

| # | Слой | Функция | State | Описание |
|---|------|---------|-------|----------|
| **ФАЗА -1: Копирование exit_date для adaptive_days** |||||
| 0 | v7_reset_exit_date | `rtc_reset_exit_date_v7` | QM | Сброс min_exit_date_mp = MAX |
| 1 | v7_copy_exit_date_repair | `rtc_copy_exit_date_repair_v7` | 4 | repair → atomicMin |
| 2 | v7_copy_exit_date_spawn | `rtc_copy_exit_date_spawn_v7` | 5 | reserve → atomicMin |
| **ФАЗА 0: Детерминированные переходы** |||||
| 3 | v7_repair_to_svc | `rtc_repair_to_svc_v7` | 4→3 | Выход из ремонта при exit_date, PPR=0 |
| 4 | v7_spawn_to_ops | `rtc_spawn_to_ops_v7` | 5→2 | Spawn при exit_date |
| **ФАЗА 1: Operations — инкременты и определение перехода** |||||
| 5 | v7_ops_increment | `rtc_ops_increment_v7` | 2→2 | sne/ppr += dt |
| 6 | v7_ops_to_storage | `rtc_ops_to_storage_v7` | 2→6 | SNE >= BR/LL |
| 7 | v7_ops_to_unsvc | `rtc_ops_to_unsvc_v7` | 2→7 | PPR >= OH, PPR=0 |
| 8 | v7_ops_stay | `rtc_ops_stay_v7` | 2→2 | Остаться в ops |
| 9 | v7_svc_stay | `rtc_svc_stay_v7` | 3→3 | Остаться в svc |
| 10 | v7_sto_stay | `rtc_sto_stay_v7` | 6→6 | Остаться в storage |
| 11 | v7_unsvc_stay | `rtc_unsvc_stay_v7` | 7→7 | Остаться в unsvc |
| 12 | v7_ina_stay | `rtc_ina_stay_v7` | 1→1 | Остаться в inactive |
| **ФАЗА 2: Квотирование (сброс, подсчёт, демоут, P1/P2/P3)** |||||
| 13 | v7_reset_flags | `rtc_reset_flags_v7` | all | Сброс promoted/needs_demote |
| 14 | v7_reset_buffers | `rtc_reset_buffers_v7` | all | Обнуление MacroProperty буферов |
| 15 | v7_count_agents | `rtc_count_agents_v7` | all | Подсчёт агентов в состояниях |
| 16 | v7_demote | `rtc_demote_v7` | QM | Демоут ops→svc |
| 17 | v7_promote_p1 | `rtc_promote_p1_v7` | QM | P1: svc→ops |
| 18 | v7_promote_p2 | `rtc_promote_p2_v7` | QM | P2: unsvc→ops |
| 19 | v7_promote_p3 | `rtc_promote_p3_v7` | QM | P3: ina→ops |
| **ФАЗА 3: Применение решений квотирования** |||||
| 20 | v7_apply_demote | `rtc_apply_demote_v7` | 2→3 | Применение демоута |
| 21 | v7_apply_promote_p1 | `rtc_apply_promote_p1_v7` | 3→2 | Применение P1 |
| 22 | v7_apply_promote_p2 | `rtc_apply_promote_p2_v7` | 7→2 | Применение P2, PPR=0 |
| 23 | v7_apply_promote_p3 | `rtc_apply_promote_p3_v7` | 1→2 | Применение P3 |
| **ФАЗА 4: Limiter V3** |||||
| 24 | limiter_on_entry | `rtc_compute_limiter_on_entry` | 2 | Бинарный поиск limiter при входе в ops |
| 25 | decrement_limiter | `rtc_decrement_limiter` | 2 | limiter -= adaptive_days |
| 26 | clear_limiter | `rtc_clear_limiter_on_exit` | 2 | limiter=0 при выходе |
| 27 | min_limiter | `rtc_compute_min_limiter` | 2 | atomicMin |
| **ФАЗА 5: V5 GPU-only** |||||
| 28 | copy_limiter_v5 | `rtc_copy_limiter_v5` | 2 | limiter → limiter_buffer |
| 29 | compute_global_min | `rtc_compute_global_min_v5` | QM | min(limiters, program, exit_date) |
| 30 | reset_min | `rtc_reset_min_limiter_v5` | QM | mp_min = MAX |
| 31 | clear_limiter_v5 | `rtc_clear_limiter_v5` | non-ops | limiter_buffer = MAX |
| 32 | save_adaptive | `rtc_save_adaptive_v5` | HELI | adaptive → agent var |
| 33 | save_adaptive_qm | `rtc_save_adaptive_v5_qm` | QM | adaptive → agent var |
| 34 | update_day | `rtc_update_day_v5` | QM | current_day += adaptive |

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
    FLAMEGPU->setVariable<unsigned int>("ppr", 0u);  // PPR обнуляется
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
2. **P2:** unserviceable → operations (+ PPR=0)
3. **P3:** inactive → operations (самый низкий)

**Демоут:** operations → serviceable (при избытке)

### Адаптивные шаги

```
adaptive_days = min(min_limiter, days_to_program_change, days_to_exit_date)
```

- `min_limiter` — минимальный limiter среди агентов в operations
- `days_to_program_change` — дней до изменения программы полётов
- `days_to_exit_date` — дней до ближайшего детерминированного события

---

## 📁 Файлы V7

| Файл | Описание |
|------|----------|
| `orchestrator_limiter_v7.py` | Главный оркестратор V7 |
| `rtc_state_transitions_v7.py` | RTC переходы состояний |
| `rtc_quota_v7.py` | RTC квотирование |
| `rtc_limiter_v5.py` | GPU-only адаптивные шаги |
| `rtc_limiter_optimized.py` | Бинарный поиск limiter |
| `base_model_messaging.py` | Модель агента |

---

## 📈 Результаты V7

| Метрика | Значение |
|---------|----------|
| Шаги | 266 |
| Время | 1.96с |
| Скорость | 1862 дней/сек |
| GPU | 100% |
| Архитектура | Single-phase |

**Сравнение с V5:**
| Метрика | V7 | V5 |
|---------|-----|-----|
| Шаги | 266 | 332 |
| Время | 1.96с | 3.71с |
| Ускорение | **1.9x** | — |

---

## 🔗 Связанная документация

- `docs/rtc_pipeline_architecture.md` — Baseline архитектура (intent-based)
- `docs/validation.md` — Инварианты и тесты
- `.cursorrules` — Правила проекта

---

*Документ создан: 13-01-2026*  
*Статус: ✅ Актуальная архитектура*

