# LIMITER V7 — Однофазная архитектура

**Дата:** 12.01.2026  
**Статус:** Реализовано и протестировано

## Ключевые отличия от V5/V6

| Аспект | V5/V6 (двухфазная) | V7 (однофазная) |
|--------|-------------------|-----------------|
| Переходы | intent → apply | Прямой переход через `setEndState` |
| Переменная intent | Используется | **НЕ используется** |
| Флаги квотирования | intent_state | `promoted`, `needs_demote` |
| Количество RTC | ~45 | ~25 |
| Производительность | 3.71с | **1.97с** (1.9x быстрее) |

## Результаты тестирования

```
✅ Симуляция завершена (3650 дней):
   Шагов: 284
   Время GPU: 1.97с
   Скорость: 1849 дней/сек
```

## Состояния агентов (7 штук)

| ID | Состояние | Описание |
|----|-----------|----------|
| 1 | `inactive` | Давно запаркованные |
| 2 | `operations` | В эксплуатации |
| 3 | `serviceable` | Исправные в холдинге |
| 4 | `repair` | Детерминированный выход по exit_date |
| 5 | `reserve` | Spawn tickets |
| 6 | `storage` | Списанные (BR/LL) |
| 7 | `unserviceable` | После OH, ждёт промоут P2 |

## Порядок слоёв RTC

### ФАЗА 0: Детерминированные переходы

| # | Слой | RTC функция | Переход | Условие |
|---|------|-------------|---------|---------|
| 1 | `v7_repair_to_svc` | `rtc_repair_to_svc_v7` | 4→3 | `current_day >= exit_date` |
| 2 | `v7_repair_stay` | `rtc_repair_stay_v7` | 4→4 | `current_day < exit_date` |
| 3 | `v7_spawn_to_ops` | `rtc_spawn_to_ops_v7` | 5→2 | `current_day >= exit_date` |
| 4 | `v7_spawn_stay` | `rtc_spawn_stay_v7` | 5→5 | `current_day < exit_date` |

### ФАЗА 1: Operations — инкременты и переходы

| # | Слой | RTC функция | Переход | Описание |
|---|------|-------------|---------|----------|
| 5 | `v7_ops_increment` | `rtc_ops_increment_v7` | 2→2 | SNE/PPR += dt (из mp5_cumsum) |
| 6 | `v7_ops_to_storage` | `rtc_ops_to_storage_v7` | 2→6 | SNE >= BR или SNE >= LL |
| 7 | `v7_ops_to_unsvc` | `rtc_ops_to_unsvc_v7` | 2→7 | PPR >= OH |

### ФАЗА 2: Квотирование

| # | Слой | RTC функция | Состояние | Описание |
|---|------|-------------|-----------|----------|
| 8 | `v7_reset_flags` | `rtc_reset_flags_v7_*` | все | Сброс promoted, needs_demote |
| 9 | `v7_reset_buffers` | `rtc_reset_quota_v7` | operations | Сброс MacroProperty буферов |
| 10 | `v7_count_agents` | `rtc_count_ops_v7` | operations | mi*_ops_count[idx] = 1 |
| 11 | `v7_count_agents` | `rtc_count_svc_v7` | serviceable | mi*_svc_count[idx] = 1 |
| 12 | `v7_count_agents` | `rtc_count_unsvc_v7` | unserviceable | mi*_unsvc_count[idx] = 1 |
| 13 | `v7_count_agents` | `rtc_count_inactive_v7` | inactive | mi*_inactive_count[idx] = 1 |
| 14 | `v7_demote` | `rtc_demote_ops_v7` | operations | needs_demote = 1 если избыток |
| 15 | `v7_promote_svc` | `rtc_promote_svc_v7` | serviceable | P1: promoted = 1 если дефицит |
| 16 | `v7_promote_unsvc` | `rtc_promote_unsvc_v7` | unserviceable | P2: promoted = 1 если дефицит |
| 17 | `v7_promote_inactive` | `rtc_promote_inactive_v7` | inactive | P3: promoted = 1 если дефицит |

### ФАЗА 3: Переходы после квотирования

| # | Слой | RTC функция | Переход | Условие |
|---|------|-------------|---------|---------|
| 18 | `v7_ops_demote` | `rtc_ops_demote_v7` | 2→3 | needs_demote == 1 |
| 19 | `v7_ops_stay` | `rtc_ops_stay_v7` | 2→2 | PPR < OH && SNE < BR && !demote |
| 20 | `v7_svc_to_ops` | `rtc_svc_to_ops_v7` | 3→2 | promoted == 1 (P1) |
| 21 | `v7_svc_stay` | `rtc_svc_stay_v7` | 3→3 | promoted != 1 |
| 22 | `v7_unsvc_to_ops` | `rtc_unsvc_to_ops_v7` | 7→2 | promoted == 1 (P2), **PPR = 0** |
| 23 | `v7_unsvc_stay` | `rtc_unsvc_stay_v7` | 7→7 | promoted != 1 |
| 24 | `v7_inactive_to_ops` | `rtc_inactive_to_ops_v7` | 1→2 | promoted == 1 (P3) |
| 25 | `v7_inactive_stay` | `rtc_inactive_stay_v7` | 1→1 | promoted != 1 |
| 26 | `v7_storage_stay` | `rtc_storage_stay_v7` | 6→6 | Неизменяемый |

### ФАЗА 4: Limiter V3

| # | Слой | RTC функция | Описание |
|---|------|-------------|----------|
| 27 | `limiter_entry` | `rtc_compute_limiter_on_entry` | Вычисление limiter при входе в ops |
| 28 | `limiter_decrement` | `rtc_decrement_limiter` | limiter -= adaptive_days |
| 29 | `limiter_copy` | `rtc_copy_limiter_to_mp` | limiter → mp_min_limiter (atomicMin) |

### ФАЗА 5: GPU-only Adaptive (V5)

| # | Слой | RTC функция | Агент | Описание |
|---|------|-------------|-------|----------|
| 30 | `v5_compute_global_min` | `rtc_compute_global_min_v5` | QuotaManager | adaptive_days = min(limiter, program_change) |
| 31 | `v5_save_adaptive` | `rtc_save_adaptive_v5` | все | computed_adaptive_days = adaptive_days |
| 32 | `v5_update_day` | `rtc_update_day_v5` | QuotaManager | current_day += adaptive_days |

## Файловая структура

```
code/sim_v2/messaging/
├── orchestrator_limiter_v7.py     # Главный оркестратор V7
├── rtc_state_transitions_v7.py    # Однофазные переходы
├── rtc_quota_v7.py                # Квотирование без intent
├── rtc_limiter_optimized.py       # Limiter V3 (бинарный поиск)
├── rtc_limiter_v5.py              # GPU-only adaptive
└── base_model_messaging.py        # Модель с promoted/needs_demote
```

## Переменные агента (V7)

### Флаги квотирования (НОВЫЕ)
- `promoted` (UInt) — 1 = получил промоут в этом шаге
- `needs_demote` (UInt) — 1 = должен выйти из operations

### Transition флаги
- `transition_2_to_3` — демоут (ops → serviceable)
- `transition_2_to_6` — списание (ops → storage)
- `transition_2_to_7` — OH (ops → unserviceable)
- `transition_3_to_2` — P1 промоут
- `transition_7_to_2` — P2 промоут (PPR = 0)
- `transition_1_to_2` — P3 промоут
- `transition_4_to_3` — repair → serviceable
- `transition_5_to_2` — spawn → operations

## Приоритеты промоута

| Приоритет | Источник | Назначение | PPR |
|-----------|----------|------------|-----|
| P1 | serviceable | operations | Сохраняется |
| P2 | unserviceable | operations | **Обнуляется** |
| P3 | inactive | operations | По правилам group_by |

## Запуск

```bash
cd code/sim_v2/messaging && python3 orchestrator_limiter_v7.py \
  --version-date 2025-07-04 \
  --end-day 3650
```

## Связанные документы

- `docs/rtc_pipeline_architecture.md` — Baseline архитектура
- `docs/limiter_v6_architecture.md` — V6 (двухфазная)
- `.cursorrules` — Правила проекта

