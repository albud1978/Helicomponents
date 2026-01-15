# Валидация симуляции Heli

> **Версия:** 2026-01-15  
> **Архив предыдущих версий:** `docs/archive/validation_legacy/`

## Оглавление

1. [Общие принципы](#1-общие-принципы)
2. [Валидация входных данных (heli_pandas)](#2-валидация-входных-данных-heli_pandas)
3. [Валидация симуляции планеров](#3-валидация-симуляции-планеров)
4. [Валидация симуляции агрегатов](#4-валидация-симуляции-агрегатов)
5. [Скрипты валидации](#5-скрипты-валидации)
6. [SQL-проверки и инварианты](#6-sql-проверки-и-инварианты)
7. [Матрица переходов состояний](#7-матрица-переходов-состояний)

---

## 1. Общие принципы

### 1.1 Приоритет источников данных

| Приоритет | Источник | Назначение |
|-----------|----------|------------|
| **1 (высший)** | СУБД (ClickHouse) | Основной контроль результатов |
| **2** | Логи симуляции | Только для диагностики по согласованию |
| **3** | Логи JIT | Контроль отсутствия warning'ов |

### 1.2 Запрет JIT warning'ов

**Правило:** Никакие warning'и в JIT логе NVRTC компиляции НЕ допускаются.

| Warning | Причина | Решение |
|---------|---------|---------|
| `#117-D: non-void function should return a value` | `return;` вместо `return flamegpu::ALIVE;` | Заменить на `return flamegpu::ALIVE;` |
| `#177-D: variable was declared but never referenced` | Неиспользуемая переменная | Удалить или использовать |

### 1.3 Правила источников данных

- Все тесты выполняются **ТОЛЬКО** на реальных данных из ClickHouse
- Синтетические данные запрещены без явного разрешения
- Отступления документируются с указанием причины

---

## 2. Валидация входных данных (heli_pandas)

### 2.1 Колонки валидации

| Колонка | Тип | Источник | Описание |
|---------|-----|----------|----------|
| `ll_mi8` | Nullable(UInt32) | md_components | Life Limit (назначенный ресурс) |
| `oh_mi8` | Nullable(UInt32) | md_components | Overhaul (межремонтный ресурс) |
| `br_mi8` | Nullable(UInt32) | md_components | Beyond Repair (порог неремонтопригодности) |
| `error_flags` | UInt8 | расчётный | Битовая маска ошибок |

### 2.2 Битовая маска error_flags

| Бит | Значение | Status | Константа | Условие SQL |
|-----|----------|--------|-----------|-------------|
| 0 | 1 | 10 | `FLAG_NO_DATA` | `ll_mi8 IS NULL OR ll_mi8 = 0` |
| 1 | 2 | 11 | `FLAG_DATE_PAST` | `target_date < version_date AND target_date IS NOT NULL` |
| 2 | 4 | 12 | `FLAG_SNE_ZERO` | `condition != 'ИСПРАВНЫЙ' AND sne = 0` |
| 3 | 8 | 13 | `FLAG_OVER_LIMIT` | `ll_mi8 > 0 AND (sne > ll_mi8 OR ppr > oh_mi8)` |
| 4 | 16 | 14 | `FLAG_BAD_COND` | `condition NOT IN ('ИСПРАВНЫЙ', 'НЕИСПРАВНЫЙ', 'ДОНОР', 'ВОЗМОЖНОЕ ПРОДЛЕНИЕ НР')` |
| 5 | 32 | 15 | `FLAG_EARLY_DONOR` | `condition = 'ДОНОР' AND br_mi8 > 0 AND sne < br_mi8` |

### 2.3 Рабочие статусы (только при error_flags = 0)

| Status | ID | Название | Условие |
|--------|-----|----------|---------|
| inactive | 1 | Неактивный | Исправный, не в эксплуатации |
| operations | 2 | Эксплуатация | Исправный, установлен на борт |
| serviceable | 3 | Исправный на складе | Исправный, готов к установке |
| repair | 4 | Ремонт | Неисправный, в ремонте |
| reserve | 5 | Резерв | После ремонта, ожидает установки |
| storage | 6 | Хранение | sne >= ll или (ppr >= oh и sne >= br) |

### 2.4 Валидные значения condition

| Значение | Тип | Описание |
|----------|-----|----------|
| `ИСПРАВНЫЙ` | основной | Агрегат исправен |
| `НЕИСПРАВНЫЙ` | основной | Агрегат неисправен |
| `ДОНОР` | ⚠️ warning | Агрегат-донор (требует внимания) |
| `ВОЗМОЖНОЕ ПРОДЛЕНИЕ НР` | ⚠️ warning | На продлении ресурса |

### 2.5 Скрипт валидации heli_pandas

**Файл:** `code/analysis/validate_heli_pandas.py`

```bash
# Анализ без изменений
python code/analysis/validate_heli_pandas.py --analyze

# Обновить ресурсы и пересчитать error_flags
python code/analysis/validate_heli_pandas.py --update

# Всё вместе
python code/analysis/validate_heli_pandas.py --all
```

**Функция update_resources:**
1. LEFT JOIN с `md_components` по `partseqno_i = partno_comp`
2. Заполняет `ll_mi8`, `oh_mi8`, `br_mi8`
3. Сбрасывает `error_flags = 0`
4. Последовательно устанавливает флаги через `bitOr()`

---

## 3. Валидация симуляции планеров

### 3.1 Оркестратор валидации

**Файл:** `code/analysis/sim_validation_runner.py`

```bash
python code/analysis/sim_validation_runner.py --version-date YYYY-MM-DD
```

Запускает три основных валидатора и генерирует отчёт.

### 3.2 Валидатор квот (QuotaValidator)

**Файл:** `code/analysis/sim_validation_quota.py`

| Параметр | Значение | Описание |
|----------|----------|----------|
| `TOLERANCE` | 1 | Допустимое отклонение |
| `CRITICAL_DEFICIT` | 3 | Критический дефицит |

**Логика:**
```python
delta = ops_count - quota_target
if delta == 0:
    status = 'ok'
elif abs(delta) <= TOLERANCE:
    status = 'minor'
elif delta < -CRITICAL_DEFICIT:
    status = 'critical'
elif delta < 0:
    status = 'warning'
else:
    status = 'excess'
```

**SQL-запрос:**
```sql
SELECT 
    day_u16,
    group_by,
    countIf(state = 'operations') as ops_count
FROM sim_masterv2
WHERE version_date = {version_date}
GROUP BY day_u16, group_by
```

### 3.3 Валидатор переходов (TransitionsValidator)

**Файл:** `code/analysis/sim_validation_transitions.py`

#### Разрешённые переходы (ALLOWED_TRANSITIONS)

| Переход | Описание |
|---------|----------|
| (0, 2) | spawn → operations |
| (0, 3) | spawn → serviceable |
| (1, 1) | inactive → inactive |
| (1, 2) | inactive → operations |
| (1, 4) | inactive → repair |
| (2, 2) | operations → operations |
| (2, 3) | operations → serviceable (демоут) |
| (2, 4) | operations → repair |
| (2, 5) | operations → reserve |
| (2, 6) | operations → storage |
| (3, 2) | serviceable → operations (промоут) |
| (3, 3) | serviceable → serviceable |
| (4, 2) | repair → operations |
| (4, 4) | repair → repair |
| (4, 5) | repair → reserve |
| (5, 2) | reserve → operations (промоут) |
| (5, 5) | reserve → reserve |
| (6, 6) | storage → storage |

#### Валидация длительности ремонта

```sql
SELECT
    aircraft_number,
    countIf(transition_1_to_4 = 1) as entries,
    countIf(transition_4_to_2 = 1) as exits
FROM sim_masterv2
WHERE version_date = {version_date} AND group_by = {gb}
GROUP BY aircraft_number
HAVING entries > 0 OR exits > 0
```

### 3.4 Валидатор инкрементов (IncrementsValidator)

**Файл:** `code/analysis/sim_validation_increments.py`

#### Инвариант dt (налёт)

| Состояние | dt > 0 | Статус |
|-----------|--------|--------|
| operations | Да | ✅ Ожидаемо |
| Другое + переход из ops | Да | 📝 День перехода |
| Другое | Да | ❌ Ошибка |

**Логика:**
```python
if state != 'operations' and with_dt > 0:
    # Это день перехода ИЗ operations
    # dt записан корректно = налёт в день перехода
    status = "📝 (дн. перех.)"
```

#### Инвариант PPR reset после ремонта

**Для Mi-8 (group_by=1):**
- PPR должен быть 0 после выхода из ремонта

**Для Mi-17 (group_by=2):**
- PPR может быть > 0 (комплектация, не ремонт планера)
- Если ppr < br — это ожидаемо

```python
br2_mi17 = 973750  # BR для Mi-17
if ppr < br2_mi17:
    expected_mi17.append(...)  # Ожидаемо
else:
    violations_mi17_real.append(...)  # Нарушение
```

### 3.5 Правила валидации (validation_rules.py)

**Файл:** `code/sim_v2/components/validation_rules.py`

#### StateTransitionValidator

```python
ALLOWED_TRANSITIONS = {
    (1, 1), (2, 2), (2, 3), (2, 4), (2, 6), (3, 3), (3, 2),
    (4, 4), (4, 5), (5, 5), (5, 2), (6, 6),
}
```

#### Контекстные проверки переходов

**Переход 2→4 (operations → repair):**
```python
# Условие: ppr_next >= oh AND sne_next < br
if from_state == 2 and to_state == 4:
    assert p_next >= oh and s_next < br
```

**Переход 2→6 (operations → storage):**
```python
# Условие: sne_next >= ll OR (ppr_next >= oh AND sne_next >= br)
if from_state == 2 and to_state == 6:
    assert (s_next >= ll) or (p_next >= oh and s_next >= br)
```

---

## 4. Валидация симуляции агрегатов

### 4.1 FIFO очереди

| Очередь | Приоритет | Источник |
|---------|-----------|----------|
| serviceable | P1 (высший) | Исправные на складе |
| reserve | P2 | После ремонта |
| spawn | P3 (низший) | Динамический spawn |

### 4.2 Условия перехода в storage

```
sne >= ll  (исчерпан назначенный ресурс)
ИЛИ
ppr >= oh AND sne >= br  (требуется ремонт, но неремонтопригоден)
```

### 4.3 Привязка к планерам

- Агрегаты привязываются к планерам через `aircraft_number`
- При выходе планера из operations агрегаты переходят в `serviceable`
- `comp_numbers[group_by]` — количество агрегатов на планер по группе

---

## 5. Скрипты валидации

### 5.1 Сводная таблица скриптов

| Скрипт | Назначение | Ключевые проверки |
|--------|------------|-------------------|
| `sim_validation_runner.py` | Оркестратор | Запуск всех валидаторов |
| `sim_validation_quota.py` | Квоты | ops_count vs quota_target |
| `sim_validation_transitions.py` | Переходы | Допустимость, длительность ремонта |
| `sim_validation_increments.py` | Инкременты | dt invariant, SNE consistency, PPR reset |
| `validate_heli_pandas.py` | Входные данные | error_flags, ресурсы |
| `validation_rules.py` | Правила | Матрица переходов, контекст |

### 5.2 Запуск валидации

```bash
# Полная валидация симуляции
python code/analysis/sim_validation_runner.py --version-date 2025-07-04

# Валидация входных данных
python code/analysis/validate_heli_pandas.py --all

# Отдельные валидаторы
python code/analysis/sim_validation_quota.py --version-date 2025-07-04
python code/analysis/sim_validation_transitions.py --version-date 2025-07-04
python code/analysis/sim_validation_increments.py --version-date 2025-07-04
```

---

## 6. SQL-проверки и инварианты

### 6.1 Инварианты состояний

#### INV-STATE-1: Intent всегда определён
```sql
SELECT COUNT(*) FROM sim_masterv2 
WHERE intent_state = 0 AND day_u16 > 0;
-- Ожидается: 0
```

#### INV-STATE-2: Storage неизменяем
```sql
WITH storage_entries AS (
  SELECT aircraft_number, MIN(day_u16) AS first_storage_day
  FROM sim_masterv2 WHERE state = 'storage'
  GROUP BY aircraft_number
)
SELECT s.aircraft_number, s.first_storage_day, m.day_u16, m.state
FROM storage_entries s
JOIN sim_masterv2 m ON s.aircraft_number = m.aircraft_number
WHERE m.day_u16 >= s.first_storage_day AND m.state != 'storage';
-- Ожидается: пусто
```

### 6.2 Инварианты квотирования

#### INV-QUOTA-1: Демоут = Balance
```sql
SELECT day_u16, group_by,
    SUM(CASE WHEN intent_state = 2 THEN 1 ELSE 0 END) as curr_i2,
    SUM(CASE WHEN intent_state = 3 THEN 1 ELSE 0 END) as demount_i3
FROM sim_masterv2
WHERE state = 'operations'
GROUP BY day_u16, group_by;
```

#### INV-QUOTA-2: XOR свойство
Агент либо демотируется, либо может быть промотирован, но не оба одновременно.

### 6.3 Инварианты ремонта

#### INV-REPAIR-1: repair_days не превышает repair_time
```sql
SELECT COUNT(*) FROM sim_masterv2 
WHERE state = 'repair' AND repair_days > repair_time;
-- Ожидается: 0
```

#### INV-REPAIR-2: Квота ремонта соблюдена
```sql
SELECT day_u16, COUNT(*) as in_repair
FROM sim_masterv2
WHERE state = 'repair' AND group_by IN (1, 2)
GROUP BY day_u16
HAVING in_repair > 18;  -- repair_number
-- Ожидается: 0
```

### 6.4 Инварианты spawn

#### INV-SPAWN-1: Динамический spawn после repair_time
```sql
SELECT COUNT(*) FROM sim_masterv2 
WHERE aircraft_number >= 100006 
  AND group_by = 2
  AND day_u16 < 180;
-- Ожидается: 0
```

#### INV-SPAWN-2: Задержка вступления = 0
```sql
WITH spawn_days AS (
    SELECT aircraft_number,
        MIN(day_u16) as birth_day,
        MIN(day_u16) FILTER (WHERE state = 'operations') as first_ops_day
    FROM sim_masterv2
    WHERE aircraft_number >= 100006 AND group_by = 2
    GROUP BY aircraft_number
)
SELECT COUNT(*) FROM spawn_days 
WHERE first_ops_day - birth_day != 0;
-- Ожидается: 0
```

### 6.5 Инварианты MP5

#### INV-MP5-1: Индексация
```
base = step_day * MAX_FRAMES + idx
```

#### INV-MP5-2: Read-only после инициализации
MP5 не изменяется RTC функциями.

---

## 7. Матрица переходов состояний

### 7.1 Планеры (group_by 1, 2)

```
         TO:
FROM:    1(inactive)  2(operations)  3(serviceable)  4(repair)  5(reserve)  6(storage)
─────────────────────────────────────────────────────────────────────────────────────
1        ✅ (hold)    ✅ (P3)        ❌              ❌         ❌          ❌
2        ❌           ✅ (stay)      ✅ (демоут)     ✅ (ремонт) ❌         ✅ (списание)
3        ❌           ✅ (P1)        ✅ (hold)       ❌         ❌          ❌
4        ❌           ❌             ❌              ✅ (stay)   ✅ (выход) ❌
5        ❌           ✅ (P2)        ❌              ❌         ✅ (hold)   ❌
6        ❌           ❌             ❌              ❌         ❌          ✅ (вечный)
spawn    ❌           ✅ (dynamic)   ✅ (v2)         ❌         ❌          ❌
```

### 7.2 Условия переходов

| Переход | Условие | Модуль |
|---------|---------|--------|
| 1→2 | `step_day >= repair_time` + quota P3 | `quota_promote_inactive` |
| 2→3 | `curr > target` | `quota_ops_excess` |
| 2→4 | `ppr_next >= oh AND sne_next < br` | `state_2_operations` |
| 2→6 | `sne_next >= ll` или BR ветка | `state_2_operations` |
| 3→2 | `curr < target` + quota P1 | `quota_promote_serviceable` |
| 4→5 | `repair_days >= repair_time` | `states_stub` |
| 5→2 | `curr < target` + quota P2 | `quota_promote_reserve` |

### 7.3 Ранжирование

| Операция | Критерий | Направление |
|----------|----------|-------------|
| Демоут | mfg_date | Oldest first (минимум) |
| Промоут | mfg_date | Youngest first (максимум) |
| Ремонт | mfg_date | Youngest first |

---

## Связанные документы

| Документ | Описание |
|----------|----------|
| `.cursorrules` | Главный источник правил |
| `docs/rtc_pipeline_architecture.md` | Архитектура модулей |
| `docs/rtc_components.md` | Архитектура агрегатов |
| `docs/spawn_dynamic_architecture.md` | Динамический spawn |
| `docs/changelog.md` | История изменений |

---

## История версий

| Дата | Изменение |
|------|-----------|
| 2026-01-15 | Консолидация документации, новая структура |
| 2025-12-30 | Добавлены инварианты агрегатов |
| 2025-11-20 | Добавлен модуль quota_repair |
| 2025-11-08 | Динамический spawn планеров |
| 2025-10-21 | Багфиксы active_trigger, MAX_FRAMES, quota_target |
| 2025-10-17 | Валидация Full Pipeline |
