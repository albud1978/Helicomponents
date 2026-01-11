# LIMITER V2 — Упрощённая архитектура

## Дата: 11.01.2026
## Ветка: feature/flame-messaging
## Статус: ✅ Работает (постпроцессинг TODO)

---

## Ключевая идея

**Убрать статус `repair` из оборота модели.**

Ремонт — календарно предсказуем, его можно добавить постпроцессингом.
Это упрощает модель и убирает адаптивные шаги на выход из ремонта.

---

## Состояния агентов

| Статус | ID | Назначение |
|--------|-----|------------|
| `inactive` | 1 | Долгий простой / запарковано |
| `operations` | 2 | В эксплуатации |
| `serviceable` | 3 | Исправный, ожидает promote |
| `unserviceable` | 4 | **НОВЫЙ**: Неисправный после OH, ремонтопригодный |
| `reserve` | 5 | **Только для spawn** |
| `storage` | 6 | Списание (SNE >= LL или BR) |

### Что изменилось:
- ❌ `repair` (ID=4 в baseline) → `unserviceable` (тот же ID, другая семантика)
- ❌ `reserve` после ремонта — больше нет
- ✅ `reserve` только для spawn агентов

---

## Таблица переходов

| Из | В | Условие | Примечание |
|----|---|---------|------------|
| operations | serviceable | Demount (квота) | Исправный, снят с линии |
| operations | unserviceable | PPR >= OH | Требует ремонта |
| operations | storage | SNE >= LL или SNE >= BR | Списание |
| serviceable | operations | Promote P1 | Первый приоритет |
| unserviceable | operations | Promote P2 | **TODO: постпроцессинг добавит repair!** |
| inactive | operations | Promote P3 | Третий приоритет |
| reserve | operations | Spawn активация | Только для новых агентов |

### Убрано из оборота:
- ❌ `repair → reserve` (был выход из ремонта)
- ❌ `repair_days` инкременты
- ❌ Очередь ремонтов (`repair_number`)
- ❌ Адаптивные шаги на выход из ремонта
- ❌ Active флаги на reserve

---

## Очередь квотирования (promote)

При дефиците в operations (curr < target):

```
1. serviceable (P1) — исправные, быстрый ввод
2. unserviceable (P2) — требуют ремонта, но доступны
3. inactive (P3) — долгий простой, последний приоритет
```

**Принцип:** Вчера вышедший вертолёт проще ремонтировать, чем давно запаркованный.

---

## Загрузка дня 0

| Источник (heli_pandas) | Статус в модели |
|------------------------|-----------------|
| status_id = 2 (ИСПРАВНЫЙ) | `operations` |
| status_id = 4 (НЕИСПРАВНЫЙ) | `unserviceable` ← НЕ repair! |
| status_id = 1 (inactive) | `inactive` |

---

## TODO: Постпроцессинг (на GPU!)

После основного оборота:
1. Для каждого перехода `unserviceable → operations`:
   - Добавить `repair_time` дней как статус `repair`
   - Вычислить дату начала ремонта = дата_promote - repair_time

2. Реализовать **на GPU** (не выходя на host):
   - MacroProperty для истории переходов
   - RTC функция постпроцессинга

3. Квота ремонта:
   - TODO: придумать как ограничить одновременные ремонты

---

## Преимущества V2

| Аспект | V1 (baseline) | V2 (новый) |
|--------|---------------|------------|
| Состояний в обороте | 6 | 5 (без repair) |
| repair_days инкременты | ✅ | ❌ |
| Адаптив шаги repair | ~150/год | 0 |
| Очередь ремонтов | Сложная | Нет в обороте |
| Постпроцессинг | Host-side | GPU |

---

## Файлы реализации

| Файл | Изменения |
|------|-----------|
| `orchestrator_limiter.py` | Убрать repair логику |
| `rtc_modules/rtc_batch_operations.py` | Убрать repair_days инкременты |
| `rtc_modules/rtc_quota_repair.py` | Отключить |
| `rtc_modules/rtc_state_manager_repair.py` | → `rtc_state_manager_unserviceable.py` |
| `components/agent_population.py` | status_id=4 → unserviceable |

---

---

## Результаты теста (365 дней)

| Метрика | V2 |
|---------|-----|
| Время | 4.8с |
| GPU | 2.75с (57%) |
| Drain | 2.05с (43%) |
| Строк | 103,407 |
| Дней/сек | 76.1 |

### Распределение (день 364):
| Состояние | Агентов |
|-----------|---------|
| inactive | 522 |
| operations | 755 |
| unserviceable (repair) | 57 |
| reserve (spawn) | 12 |
| serviceable | 79 |

### Что работает ✅:
- Переход `ops → unserviceable` (PPR >= OH)
- Promote P2 из unserviceable
- Reserve только для spawn
- Нет инкрементов repair_days
- Нет выхода repair→reserve

### TODO ⏸️:
- Постпроцессинг на GPU (добавить repair период)
- Квота ремонта (ограничение одновременных)

---

**Автор:** Алексей (концепция), AI (реализация)
**Дата:** 11.01.2026

