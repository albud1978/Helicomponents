# Исследование: Альтернативная архитектура с FLAME GPU Messaging

## Цель

Исследовать альтернативный подход к симуляции планеров с использованием **нативных сообщений FLAME GPU** вместо текущего подхода через MacroProperty.

## Результаты (08-01-2026)

### ✅ УСПЕШНО: Event-driven гибридная архитектура

**Производительность:**

| Архитектура | Время (3650 шагов) | Шагов/сек | Ускорение |
|-------------|-------------------|-----------|-----------|
| Polling (MacroProperty) | ~387с | ~9.4 | 1.0x |
| **Event-driven (hybrid)** | **103с** | **35.4** | **~3.8x** |

**Ключевые файлы:**
- `code/sim_v2/messaging/rtc_publish_event.py` — публикация событий агентами
- `code/sim_v2/messaging/rtc_quota_manager_event.py` — обработка событий QuotaManager
- `code/sim_v2/messaging/orchestrator_messaging.py` — оркестратор с `--event-driven` флагом

### Архитектура EVENT-DRIVEN (гибридная)

```
┌─────────────────────────────────────────────────────────────┐
│                    ГИБРИДНЫЙ ПОДХОД                        │
├─────────────────────────────────────────────────────────────┤
│  Operations агенты:                                        │
│    └─ ВСЕГДА публикуют OPS_REPORT (event_type=3)          │
│    └─ При выходе публикуют DEMOUNT (event_type=1)         │
│                                                             │
│  Serviceable агенты:                                       │
│    └─ ВСЕГДА публикуют READY (event_type=2)               │
│                                                             │
│  Repair/Reserve/Inactive:                                  │
│    └─ Публикуют ТОЛЬКО при изменении intent               │
│                                                             │
│  QuotaManager:                                             │
│    └─ Читает target из mp4_ops_counter[day]               │
│    └─ Подсчитывает curr_ops из OPS_REPORT сообщений       │
│    └─ Вычисляет balance = curr - target                   │
│    └─ При balance > 0 → DEMOTE (oldest first)             │
│    └─ При balance < 0 → PROMOTE из ready-пулов (P1→P2→P3) │
└─────────────────────────────────────────────────────────────┘
```

### Типы событий

| event_type | Название | Источник | Описание |
|------------|----------|----------|----------|
| 1 | DEMOUNT | operations | Агент выходит из operations (intent != 2) |
| 2 | READY | serviceable/reserve/inactive | Агент готов к промоуту |
| 3 | OPS_REPORT | operations | Агент остаётся в operations (для учёта и ранжирования) |

### Ранжирование

- **Демоут:** oldest first (больший mfg_date → старше → демоутить первым)
- **Промоут:** youngest first (меньший mfg_date → моложе → промоутить первым)

### Каскад промоута

1. **P1:** serviceable → operations
2. **P2:** reserve → operations  
3. **P3:** inactive → operations

## Сравнение архитектур

### Polling (MacroProperty)

**Плюсы:**
- Простая логика
- Один источник истины (MacroProperty счётчики)

**Минусы:**
- Много слоёв (14+) для count/quota/promote
- Каждый агент читает MacroProperty каждый шаг
- Медленнее (~9.4 шагов/сек)

### Event-driven (Messaging)

**Плюсы:**
- **3.8x быстрее** (35.4 шагов/сек)
- Централизованная логика в QuotaManager
- Меньше слоёв (6 publish + 1 quota + 6 apply)
- Масштабируется для следующих уровней (агрегаты)

**Минусы:**
- Сложнее отладка (асинхронные сообщения)
- Требует буферов для ранжирования (MAX_EVENTS=200)

## Запуск

```bash
# Event-driven (рекомендуется)
python3 code/sim_v2/messaging/orchestrator_messaging.py \
  --version-date 2025-07-04 \
  --steps 3650 \
  --enable-mp2 \
  --drop-table \
  --event-driven

# Polling (для сравнения)
python3 code/sim_v2/messaging/orchestrator_messaging.py \
  --version-date 2025-07-04 \
  --steps 3650 \
  --enable-mp2 \
  --drop-table
```

## Выводы

1. **Event-driven архитектура работает** и даёт значительное ускорение
2. **Гибридный подход оптимален:** operations и serviceable ВСЕГДА публикуют для точного подсчёта
3. **QuotaManager агенты (2 шт)** эффективно централизуют логику квотирования
4. **Готово к расширению** на агрегаты и следующие уровни сборки

---

**Статус:** ✅ Исследование завершено, архитектура работает
**Дата завершения:** 08-01-2026
