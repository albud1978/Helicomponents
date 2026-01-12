# RTC Pipeline Architecture — LIMITER V6

> **Актуальная версия:** V6 (12-01-2026)  
> **Основной оркестратор:** `code/sim_v2/messaging/orchestrator_limiter_v6.py`  
> **Архивные архитектуры:** `docs/archive/architecture/`

---

## Таблица RTC функций V6 — Порядок исполнения

| # | Слой | Функция | State | Описание |
|---|------|---------|-------|----------|
| **ФАЗА 0: Детерминированные переходы** |||||
| 1 | L_repair_to_serviceable | `rtc_repair_to_serviceable` | 4→3 | Выход из ремонта при exit_date |
| 2 | L_repair_stay | `rtc_repair_stay` | 4→4 | Остаться в repair |
| 3 | L_spawn_to_operations | `rtc_spawn_to_operations` | 5→2 | Spawn при exit_date |
| 4 | L_spawn_stay | `rtc_spawn_stay` | 5→5 | Ожидание spawn |
| **ФАЗА 1: Инкременты состояний** |||||
| 5 | state_2_operations | `rtc_state_2_operations` | 2→2 | sne/ppr += dt, intent=7 при OH |
| 6 | (auto) | `rtc_state_3_serviceable_v2` | 3→3 | intent=3, dt=0 |
| 7 | (auto) | `rtc_state_4_repair_v6` | 4→4 | intent=4, dt=0 |
| 8 | (auto) | `rtc_state_5_reserve_v6` | 5→5 | intent=5, dt=0 |
| 9 | (auto) | `rtc_state_6_storage_v6` | 6→6 | intent=6, dt=0 |
| 10 | (auto) | `rtc_state_7_unserviceable_v6` | 7→7 | intent=7, dt=0 |
| **ФАЗА 2: Подсчёт и квотирование** |||||
| 11 | reset_quota_buffers | `rtc_reset_quota_buffers` | all | Обнуление буферов (idx=0) |
| 12 | count_ops | `rtc_count_ops` | 2 | mi*_ops_count[idx]=1 |
| 13 | count_serviceable | `rtc_count_serviceable` | 3 | mi*_svc_count[idx]=1 |
| 14 | count_reserve | `rtc_count_reserve` | 5 | mi*_reserve_count[idx]=1 |
| 15 | count_inactive | `rtc_count_inactive` | 1 | mi*_inactive_count[idx]=1 |
| 16 | count_unserviceable | `rtc_count_unserviceable` | 7 | mi*_unsvc_count[idx]=1 |
| 17 | count_reserve_queue | `rtc_count_reserve_queue` | 5 | reserve_queue если intent=0 |
| 18 | count_ops_to_unsrv | `rtc_count_ops_to_unserviceable` | 2 | ops_repair если intent=7 |
| 19 | log_mp4_targets | `rtc_log_mp4_targets` | QM | Логирование target |
| 20 | log_quota_gap | `rtc_log_quota_gap` | QM | Логирование gap |
| 21 | quota_demount | `rtc_quota_demount` | 2 | Демоут ops→svc при избытке |
| 22 | quota_promote_svc_p1 | `rtc_quota_promote_serviceable` | 3 | **P1**: svc→ops |
| 23 | quota_promote_unsvc_p2 | `rtc_quota_promote_unserviceable` | 7 | **P2**: unsvc→ops |
| 24 | quota_promote_ina_p3 | `rtc_quota_promote_inactive` | 1 | **P3**: ina→ops |
| **ФАЗА 3: State Managers** |||||
| 25 | svc_holding | `rtc_serviceable_holding_confirm` | 3→3 | Остаться в svc |
| 26 | svc_to_ops | `rtc_serviceable_to_operations` | 3→2 | P1 переход |
| 27 | ops_2_to_2 | `rtc_apply_2_to_2` | 2→2 | Остаться в ops |
| 28 | ops_2_to_3 | `rtc_apply_2_to_3` | 2→3 | Демоут в svc |
| 29 | ops_2_to_6 | `rtc_apply_2_to_6` | 2→6 | В storage (BR) |
| 30 | storage_stay | `rtc_apply_storage_stay` | 6→6 | Неизменяемый |
| 31 | inactive_1_to_1 | `rtc_apply_1_to_1` | 1→1 | Остаться |
| 32 | inactive_1_to_2 | `rtc_apply_1_to_2` | 1→2 | P3 переход |
| 33 | state7_stay | `rtc_apply_state7_stay` | 7→7 | Остаться в unsvc |
| 34 | state7_to_ops | `rtc_apply_state7_to_ops` | 7→2 | P2 переход, PPR=0 |
| **ФАЗА 4: Limiter V3** |||||
| 35 | limiter_on_entry | `rtc_compute_limiter_on_entry` | 2 | Бинарный поиск при входе |
| 36 | decrement_limiter | `rtc_decrement_limiter` | 2 | limiter -= adaptive_days |
| 37 | clear_limiter | `rtc_clear_limiter_on_exit` | 2 | limiter=0 при выходе |
| 38 | min_limiter | `rtc_compute_min_limiter` | 2 | atomicMin |
| **ФАЗА 5: V5 GPU-only** |||||
| 39 | copy_limiter_v5 | `rtc_copy_limiter_v5` | 2 | limiter → limiter_buffer |
| 40 | compute_global_min | `rtc_compute_global_min_v5` | QM | min(limiters, program) |
| 41 | reset_min | `rtc_reset_min_limiter_v5` | QM | mp_min = MAX |
| 42 | clear_limiter_v5 | `rtc_clear_limiter_v5` | non-ops | limiter_buffer = MAX |
| 43 | save_adaptive | `rtc_save_adaptive_v5` | HELI | adaptive → agent var |
| 44 | save_adaptive_qm | `rtc_save_adaptive_v5_qm` | QM | adaptive → agent var |
| 45 | update_day | `rtc_update_day_v5` | QM | current_day += adaptive |

---

## Состояния агентов V6

| ID | State | Описание | Переходы ИЗ | Переходы В |
|----|-------|----------|-------------|------------|
| 1 | `inactive` | Давно запаркованные | 1 | 2 (P3) |
| 2 | `operations` | В эксплуатации | 3,5,7,1 | 3,6,7 |
| 3 | `serviceable` | Исправные в холдинге | 2 | 2 (P1) |
| 4 | `repair` | В ремонте (детерм. exit_date) | — | 3 |
| 5 | `reserve` | Spawn tickets | — | 2 |
| 6 | `storage` | Списание (BR) | 2 | — |
| 7 | `unserviceable` | После OH, ждут P2 | 2 | 2 (P2) |

---

## Приоритеты квотирования

| Приоритет | Переход | Логика PPR |
|-----------|---------|------------|
| **P1** | serviceable → operations | PPR сохраняется |
| **P2** | unserviceable → operations | PPR = 0 (после ремонта) |
| **P3** | inactive → operations | PPR по group_by |

---

## Ключевые архитектурные принципы V6

### 1. Адаптивные шаги (LIMITER)
- `limiter` вычисляется **один раз** при входе в operations (бинарный поиск по `mp5_cumsum`)
- На каждом шаге: `limiter -= adaptive_days`
- `adaptive_days = min(min_limiter, next_program_change, end_day - current_day)`

### 2. 100% GPU-only
- Весь расчёт `adaptive_days` и `current_day` выполняется на GPU
- Единственный HF: `HF_SyncDayV5` — синхронизация `current_day_mp` → Environment

### 3. Детерминированные переходы
- `repair` (state 4): агенты с `exit_date` — автовыход в `serviceable`
- `reserve` (state 5): spawn tickets с `exit_date` — автовыход в `operations`
- Функции: `rtc_repair_to_serviceable`, `rtc_spawn_to_operations`

### 4. Ремонт в постпроцессинге
- В V6 ремонт **не симулируется**, только фиксируется `exit_date` при загрузке
- Полный расчёт ремонтов планируется в постпроцессинге (не реализовано)

---

## Производительность V6

| Метрика | DS1 (2025-07-04) | DS2 (2025-12-30) |
|---------|------------------|------------------|
| Дней | 3650 | 3650 |
| Шагов | 316 | ~320 |
| Время | 3.00с | ~3с |
| Агентов | 279 | 285 |

---

## Файловая структура

```
code/sim_v2/messaging/
├── orchestrator_limiter_v6.py      # Основной оркестратор V6
├── base_model_messaging.py         # Модель агентов (7 состояний)
├── rtc_deterministic_exit.py       # Детерм. переходы (repair, spawn)
├── rtc_state_2_operations.py       # Инкременты operations
├── rtc_states_stub_v2.py           # Stub для не-operations состояний
├── rtc_quota_count_ops.py          # Подсчёт агентов
├── rtc_quota_ops_excess.py         # Демоут при избытке
├── rtc_quota_promote_serviceable.py  # P1
├── rtc_quota_promote_unserviceable.py # P2 (V6)
├── rtc_quota_promote_inactive.py   # P3
├── rtc_state_manager_*.py          # State managers
├── rtc_limiter_optimized.py        # Limiter V3
└── rtc_limiter_v5.py               # GPU-only adaptive
```

---

## История версий

| Версия | Дата | Описание |
|--------|------|----------|
| V6 | 12-01-2026 | +state 7 (unserviceable), детерм. exit, убран repair из оборота |
| V5 | 11-01-2026 | 100% GPU-only, simulate() вместо while step() |
| V3 | 10-01-2026 | LIMITER с адаптивными шагами, бинарный поиск |
| V2 | 2025 | Baseline с пошаговой симуляцией |

---

## Архивные документы

Перемещены в `docs/archive/architecture/`:
- `ADAPTIVE_2_0_ARCHITECTURE.md` — концепция GPU-only
- `ADAPTIVE_STEP_ARCHITECTURE.md` — ранняя версия адаптивных шагов
- `ADAPTIVE_STEP_V3_ARCHITECTURE.md` — V3 архитектура
- `GPU_ONLY_ARCHITECTURE.md` — GPU-only концепция
- `LIMITER_V2_ARCHITECTURE.md` — LIMITER V2
- `MESSAGING_RESEARCH.md` — исследование messaging

---

*Последнее обновление: 12-01-2026*
