# Архитектура системы квотирования: Полный цикл работы

**Дата:** 19-11-2025  
**Цель:** Документирование полного цикла от загрузки данных до логирования переходов для создания модуля quota_repair

---

## 📦 1. Загрузка данных в модель

### 1.1. Источники данных (ClickHouse → Python)

**Файл:** `code/sim_v2/base_model.py`

Данные загружаются из ClickHouse через `env_data` dictionary:

```python
# MP1: repair_number (квоты на ремонт)
mp1_repair_number = list(env_data.get('mp1_repair_number', []))
if mp1_repair_number:
    self.env.newPropertyArrayUInt8("mp1_repair_number", mp1_repair_number)
```

**Ключевые поля:**
- `mp1_repair_number` (UInt8): Квота на количество агентов в ремонте (18 для планеров Mi-8/Mi-17)
- `mp1_oh_mi8`, `mp1_oh_mi17` (UInt32): Overhau hours в минутах
- `mp1_repair_time`, `mp1_assembly_time` (UInt32): Длительности ремонтов
- `mp4_ops_counter_mi8`, `mp4_ops_counter_mi17` (UInt32): Целевые квоты по дням
- `mp3_mfg_date_days` (UInt32): Даты производства для приоритизации

---

### 1.2. Создание MacroProperty массивов

**Файл:** `code/sim_v2/base_model.py` (lines 150-185)

Все MacroProperty создаются **единожды** при инициализации модели:

```python
def _setup_macro_properties(self):
    # Подсчёт агентов по состояниям
    self.env.newMacroPropertyUInt32("mi8_ops_count", MAX_FRAMES)
    self.env.newMacroPropertyUInt32("mi17_ops_count", MAX_FRAMES)
    self.env.newMacroPropertyUInt32("mi8_svc_count", MAX_FRAMES)
    self.env.newMacroPropertyUInt32("mi17_svc_count", MAX_FRAMES)
    self.env.newMacroPropertyUInt32("mi8_reserve_count", MAX_FRAMES)
    self.env.newMacroPropertyUInt32("mi17_reserve_count", MAX_FRAMES)
    self.env.newMacroPropertyUInt32("mi8_inactive_count", MAX_FRAMES)
    self.env.newMacroPropertyUInt32("mi17_inactive_count", MAX_FRAMES)
    
    # Approve буферы для квотирования
    self.env.newMacroPropertyUInt32("mi8_approve", MAX_FRAMES)          # Демоут
    self.env.newMacroPropertyUInt32("mi17_approve", MAX_FRAMES)
    self.env.newMacroPropertyUInt32("mi8_approve_s3", MAX_FRAMES)       # Промоут P1 (serviceable)
    self.env.newMacroPropertyUInt32("mi17_approve_s3", MAX_FRAMES)
    self.env.newMacroPropertyUInt32("mi8_approve_s5", MAX_FRAMES)       # Промоут P2 (reserve)
    self.env.newMacroPropertyUInt32("mi17_approve_s5", MAX_FRAMES)
    self.env.newMacroPropertyUInt32("mi8_approve_s1", MAX_FRAMES)       # Промоут P3 (inactive)
    self.env.newMacroPropertyUInt32("mi17_approve_s1", MAX_FRAMES)
    
    # Spawn pending флаги
    self.env.newMacroPropertyUInt32("mi8_spawn_pending", MAX_FRAMES)
    self.env.newMacroPropertyUInt32("mi17_spawn_pending", MAX_FRAMES)
```

**⚠️ КРИТИЧНО:**
- `MAX_FRAMES` = 286 (количество планеров Mi-8 + Mi-17 в MP3 с group_by ∈ {1,2})
- Все массивы имеют **фиксированный размер** для всей симуляции
- Данные НЕ перезагружаются между шагами

---

## 🔄 2. Цикл квотирования (каждый день симуляции)

### 2.1. Обнуление буферов (слой 1)

**Модуль:** `rtc_quota_count_ops.py` → `rtc_reset_quota_buffers`

**Кто выполняет:** Только первый агент (`idx=0`)

**Что обнуляется:**
- Подсчёт по состояниям: `mi8_ops_count`, `mi17_ops_count`, `mi8_svc_count`, `mi17_svc_count`, `mi8_reserve_count`, `mi17_reserve_count`, `mi8_inactive_count`, `mi17_inactive_count`
- Approve флаги: `mi8_approve`, `mi17_approve`, `mi8_approve_s3`, `mi17_approve_s3`, `mi8_approve_s5`, `mi17_approve_s5`, `mi8_approve_s1`, `mi17_approve_s1`
- Spawn pending: `mi8_spawn_pending`, `mi17_spawn_pending`

```cpp
// Только первый агент (idx=0) обнуляет буферы
if (idx == 0u) {
    auto mi8_ops = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_FRAMES>("mi8_ops_count");
    auto mi17_ops = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_FRAMES>("mi17_ops_count");
    // ... аналогично для всех буферов
    
    for (unsigned int i = 0u; i < MAX_FRAMES; ++i) {
        mi8_ops[i].exchange(0u);
        mi17_ops[i].exchange(0u);
        // ... аналогично для всех буферов
    }
}
```

---

### 2.2. Подсчёт агентов по состояниям (слои 2-5)

**Модуль:** `rtc_quota_count_ops.py` → `rtc_count_ops`, `rtc_count_serviceable`, `rtc_count_reserve`, `rtc_count_inactive`

**Кто выполняет:** ВСЕ агенты в соответствующих состояниях

**Алгоритм для operations:**
```cpp
// Фильтр: только state=operations
// Считаем только агентов с intent=2 (хотят остаться в operations)
if (intent == 2u) {
    if (group_by == 1u) {
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_FRAMES>("mi8_ops_count");
        ops_count[idx].exchange(1u);  // ✅ Атомарная операция
    } else if (group_by == 2u) {
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_FRAMES>("mi17_ops_count");
        ops_count[idx].exchange(1u);
    }
}
```

**Аналогично для других состояний:**
- `serviceable`: Записывают в `mi8_svc_count` / `mi17_svc_count`
- `reserve`: Записывают в `mi8_reserve_count` / `mi17_reserve_count`
- `inactive`: Записывают в `mi8_inactive_count` / `mi17_inactive_count`

**⚠️ КРИТИЧНО:**
- Используем `exchange()` для атомарной записи (избегаем race conditions)
- Буферы — это **битовые маски**: 0 = не в состоянии, 1 = в состоянии
- Фильтрация по `state` обеспечивается через `setInitialState()` в регистрации функции

---

### 2.3. Квотирование (демоут и промоуты)

#### 2.3.1. Демоут: operations → serviceable

**Модуль:** `rtc_quota_ops_excess.py` → `rtc_quota_demount`

**Фильтр:** `state=operations & intent=2`

**Логика:**
1. Подсчитать `curr` (агенты в operations с intent=2)
2. Прочитать `target` из `mp4_ops_counter_mi*[safe_day]` (D+1)
3. Рассчитать `balance = curr - target`
4. **Early exit** если `balance <= 0` (нет избытка)
5. Ранжировать агентов: **oldest first** (меньший `idx` = старше)
6. Демоутить `K = balance` самых старых → `intent=3`
7. Пометить в `mi*_approve[idx] = 1`

**Ключевой код:**
```cpp
// Ранжирование: oldest first (idx УЖЕ отсортирован по mfg_date)
unsigned int rank = 0u;
auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_FRAMES>("mi8_ops_count");
for (unsigned int i = 0u; i < frames; ++i) {
    if (i == idx) continue;
    if (ops_count[i] != 1u) continue;  // ✅ Только агенты в operations
    
    // Oldest first: rank растёт если other (i) СТАРШЕ меня (меньший idx)
    if (i < idx) {
        ++rank;
    }
}

if (rank < K) {
    // Я в числе K самых старых → демоут
    FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
    auto approve = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_FRAMES>("mi8_approve");
    approve[idx].exchange(1u);
}
```

---

#### 2.3.2. Промоут P1: serviceable → operations

**Модуль:** `rtc_quota_promote_serviceable.py` → `rtc_quota_promote_serviceable`

**Фильтр:** `state=serviceable & intent=3`

**Логика:**
1. Подсчитать `curr` (агенты в operations с intent=2)
2. Прочитать `target` из `mp4_ops_counter_mi*[safe_day]` (D+1)
3. Рассчитать `deficit = target - curr`
4. **Early exit** если `deficit <= 0`
5. Ранжировать агентов: **youngest first** (больший `idx` = моложе)
6. Промоутить `K = deficit` самых молодых → `intent=2`
7. Пометить в `mi*_approve_s3[idx] = 1`

**Ключевой код:**
```cpp
// Ранжирование: youngest first
unsigned int rank = 0u;
auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_FRAMES>("mi8_svc_count");
for (unsigned int i = 0u; i < frames; ++i) {
    if (i == idx) continue;
    if (svc_count[i] != 1u) continue;  // ✅ Только агенты в serviceable
    
    // Youngest first: rank растёт если other (i) МОЛОЖЕ меня (больший idx)
    if (i > idx) {
        ++rank;
    }
}

if (rank < K) {
    FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
    auto approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_FRAMES>("mi8_approve_s3");
    approve_s3[idx].exchange(1u);
    
    printf("  [PROMOTE P1→2 Day %u] AC %u (idx %u): rank=%u/%u serviceable->operations\\n", 
           day, aircraft_number, idx, rank, K);
}
```

---

#### 2.3.3. Промоут P2: reserve → operations

**Модуль:** `rtc_quota_promote_reserve.py` → `rtc_quota_promote_reserve`

**Фильтр:** `state=reserve & intent=5`

**Логика:**
1. Подсчитать `curr` (агенты в operations)
2. Подсчитать `used` (уже одобрено из P1): `sum(mi*_approve_s3[i])`
3. Рассчитать `deficit = target - curr - used` (каскадное вычитание)
4. **Early exit** если `deficit <= 0`
5. Ранжировать: **youngest first**
6. Промоутить `K = deficit` → `intent=2`
7. Пометить в `mi*_approve_s5[idx] = 1`

**Ключевой код (каскадное квотирование):**
```cpp
// Считаем сколько уже одобрено из serviceable (P1)
unsigned int used = 0u;
auto approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_FRAMES>("mi8_approve_s3");
for (unsigned int i = 0u; i < frames; ++i) {
    if (approve_s3[i] == 1u) ++used;
}

// Расчёт дефицита с учётом P1
const int deficit = (int)target - (int)curr - (int)used;
```

---

#### 2.3.4. Промоут P3: inactive → operations

**Модуль:** `rtc_quota_promote_inactive.py` → `rtc_quota_promote_inactive`

**Фильтр:** `state=inactive & intent=1 & step_day >= repair_time`

**Логика:**
1. Подсчитать `curr` + `used` (P1 + P2)
2. Рассчитать `deficit = target - curr - used`
3. **Публиковать deficit** в `quota_deficit_mi*_u32[safe_day]` для динамического spawn
4. **Early exit** если `deficit <= 0`
5. Ранжировать: **youngest first**
6. Промоутить `K = deficit` → `intent=2`
7. Пометить в `mi*_approve_s1[idx] = 1`

**⚠️ Может остаться deficit > 0** (допустимо по бизнес-логике)

---

## 📊 3. Логирование и переходы

### 3.1. Логирование переходов

**Модуль:** `rtc_state_manager_*.py` (для каждого состояния)

**Формат логирования:**
```cpp
printf("  [TRANSITION X→Y Day %u] AC %u (idx %u): state_name -> state_name\\n", 
       step_day, aircraft_number, idx);
```

**Примеры:**
- `[TRANSITION 2→4 Day 128] AC 22485 (idx 42): operations -> repair`
- `[TRANSITION 5→2 Day 149] AC 22268 (idx 163): reserve -> operations`
- `[PROMOTE P1→2 Day 180] AC 24113 (idx 92): rank=5/10 serviceable->operations`

**⚠️ ВАЖНО:**
- Логи пишутся в `stdout` (перенаправляются в файл через `tee`)
- Используются для валидации, т.к. `transition_*` в СУБД не работает в baseline
- Формат должен быть **единообразным** для парсинга

---

### 3.2. Чтение repair_number из MP1

**Источник:** `code/sim_v2/rtc_repair_number_example.py`

**Алгоритм:**
```cpp
// 1. Получить partseqno агента
const unsigned int partseqno = FLAMEGPU->getVariable<unsigned int>("partseqno");

// 2. Найти индекс в mp1_index (Environment array)
auto mp1_index = FLAMEGPU->environment.getProperty<unsigned int, MP1_SIZE>("mp1_index");
int pidx = -1;
for (unsigned int i = 0; i < MP1_SIZE; i++) {
    if (mp1_index[i] == partseqno) {
        pidx = static_cast<int>(i);
        break;
    }
}

// 3. Прочитать repair_number из mp1_repair_number
auto mp1_repair_number = FLAMEGPU->environment.getProperty<unsigned char, MP1_SIZE>("mp1_repair_number");
const unsigned char repair_number = mp1_repair_number[pidx];

// 4. Проверить на sentinel (255 = NULL)
if (repair_number != 255u) {
    // Использовать repair_number для квотирования
}
```

**⚠️ SENTINEL:**
- `255` (0xFF) = NULL в исходных данных
- Для планеров (group_by=1,2) значение = 18

---

## 🎯 4. Применение для quota_repair

### 4.1. MacroProperty для quota_repair

**Что нужно создать в base_model.py:**
```python
# Подсчёт агентов в repair
self.env.newMacroPropertyUInt32("mi8_repair_count", MAX_FRAMES)
self.env.newMacroPropertyUInt32("mi17_repair_count", MAX_FRAMES)

# Кандидаты на ремонт (reserve & intent=0)
self.env.newMacroPropertyUInt32("mi8_repair_queue_count", MAX_FRAMES)
self.env.newMacroPropertyUInt32("mi17_repair_queue_count", MAX_FRAMES)

# Кандидаты на ремонт (operations & intent=4)
self.env.newMacroPropertyUInt32("mi8_repair_request_count", MAX_FRAMES)
self.env.newMacroPropertyUInt32("mi17_repair_request_count", MAX_FRAMES)
```

### 4.2. Модуль quota_repair

**Регистрация:** `rtc_modules/rtc_quota_repair.py`

**Слои:**
1. **Подсчёт агентов в repair** (state=repair)
2. **Подсчёт очереди** (state=reserve & intent=0)
3. **Подсчёт запросов** (state=operations & intent=4)
4. **Квотирование** (каскадное: сначала очередь, потом запросы)

**Логика квотирования:**
```cpp
// 1. Прочитать repair_number из MP1 (через mp1_index)
// 2. Подсчитать curr_in_repair (агенты с тем же repair_number в repair)
// 3. Рассчитать available = quota - curr_in_repair
// 4. Early exit если available <= 0
// 5. Ранжировать ВСЕХ кандидатов (очередь + запросы): youngest first
// 6. Одобрить топ-K кандидатов: intent=4 (для обоих типов)
// 7. Отклонить остальных: intent=0 (очередь) или intent=5 (запросы)
```

### 4.3. Логирование для отладки

**Критичные логи:**
```cpp
// Одобрение из очереди
printf("  [REPAIR APPROVE QUEUE Day %u] AC %u (idx %u): rank=%u/%u reserve->repair\\n", 
       day, aircraft_number, idx, rank, K);

// Одобрение новых запросов
printf("  [REPAIR APPROVE NEW Day %u] AC %u (idx %u): rank=%u/%u operations->repair\\n", 
       day, aircraft_number, idx, rank, K);

// Отклонение (только в дни отладки)
if (day == 180u || day == 181u || day == 182u) {
    printf("  [REPAIR REJECT Day %u] AC %u (idx %u): rank=%u, available=%d\\n", 
           day, aircraft_number, idx, rank, available);
}
```

---

## 📋 5. Чек-лист для разработки quota_repair

- [ ] Создать MacroProperty в `base_model.py`:
  - [ ] `mi8_repair_count`, `mi17_repair_count`
  - [ ] `mi8_repair_queue_count`, `mi17_repair_queue_count`
  - [ ] `mi8_repair_request_count`, `mi17_repair_request_count`

- [ ] Создать модуль `rtc_quota_repair.py`:
  - [ ] Слой 1: `rtc_count_repair` (подсчёт агентов в repair)
  - [ ] Слой 2: `rtc_count_repair_queue` (подсчёт reserve & intent=0)
  - [ ] Слой 3: `rtc_count_repair_requests` (подсчёт operations & intent=4)
  - [ ] Слой 4: `rtc_quota_repair` (квотирование)

- [ ] Реализовать логику:
  - [ ] Чтение `repair_number` из MP1
  - [ ] Подсчёт `curr_in_repair` по группам `repair_number`
  - [ ] Каскадное квотирование (очередь → запросы)
  - [ ] Ранжирование: youngest first для ВСЕХ кандидатов

- [ ] Логирование:
  - [ ] Одобрения (APPROVE QUEUE / APPROVE NEW)
  - [ ] Отклонения (REJECT) — только в дни отладки
  - [ ] Переходы в state_managers (2→4, 5→4)

- [ ] Тестирование:
  - [ ] 90 дней: проверка базовой логики
  - [ ] 3650 дней: сравнение с baseline
  - [ ] Инварианты: max_in_repair ≤ 18

---

## 🔗 Связанные документы

- `docs/baseline_no_quota_repair.md` — baseline для сравнения
- `docs/repair_quota_design.md` — архитектура модуля quota_repair
- `docs/rtc_pipeline_architecture.md` — общая архитектура RTC pipeline
- `code/sim_v2/rtc_quota_*.py` — существующие модули квотирования (примеры)
- `code/sim_v2/base_model.py` — создание MacroProperty
- `code/sim_v2/rtc_repair_number_example.py` — пример чтения repair_number

---

**Следующий шаг:** Разработка модуля `quota_repair` на основе этой архитектуры.


