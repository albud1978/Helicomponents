# Полная схема квотирования и баланса

**Дата:** 10.10.2025  
**Цель:** Детальный разбор использования MP4, счётчиков и ранжирования

---

## 📊 **1. Где используются данные из MP (MacroProperty) и счётчики**

### **Источник данных: MP4 (flight_program_ac)**

**Таблица СУБД:**
```sql
SELECT dates, ops_counter_mi8, ops_counter_mi17, trigger_program_mi8, trigger_program_mi17, new_counter_mi17
FROM flight_program_ac
ORDER BY dates
```

**Загрузка в симуляцию:**
```python
# code/sim_env_setup.py
mp4_by_day = preload_mp4_by_day(client)  # Dict[date, Dict[str,int]]
mp4_ops8, mp4_ops17 = build_mp4_arrays(mp4_by_day, days_sorted)

# Применение к Environment:
sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8", mp4_ops8)
sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi17", mp4_ops17)
```

**Размерность:** `mp4_ops_counter_mi8[days_total]` — по одному значению на каждый день симуляции

---

### **Использование MP4 в квотировании:**

#### **1️⃣ Demotion (quota_ops_excess.py)**

```cpp
// Чтение Target из MP4
const unsigned int safe_day = (day >= days_total) ? (days_total - 1u) : day;
target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);

// Подсчёт Curr из счётчика ops_count (заполненного в count_ops)
auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_ops_count");
unsigned int curr = 0u;
for (unsigned int i = 0u; i < frames; ++i) {
    if (ops_count[i] == 1u) ++curr;  // ✅ Считаем флаги из буфера
}

// Баланс = Избыток
const int balance = (int)curr - (int)target;

printf("[DEMOUNT BALANCE Day %u] Mi-17: Curr=%u, Target=%u, Balance=%d\n", 
       day, curr, target, balance);
```

**Где используется:**
- `target` — **MP4 из СУБД** (`flight_program_ac.ops_counter_mi17[day]`)
- `curr` — **счётчик из MP** (`mi17_ops_count`, заполняется в `count_ops`)
- `balance` — **вычисляется** на основе MP4 и счётчика

---

#### **2️⃣ Promotion P1 (quota_promote_serviceable.py)**

```cpp
// Чтение Target из MP4
target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);

// Подсчёт Curr из счётчика ops_count
auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_ops_count");
unsigned int curr = 0u;
for (unsigned int i = 0u; i < frames; ++i) {
    if (ops_count[i] == 1u) ++curr;
}

// Used = 0 (первый промоут слой)
unsigned int used = 0u;

// Deficit = Дефицит
const int deficit = (int)target - (int)(curr + used);

printf("[PROMOTE P1 DEFICIT Day %u] Mi-17: Curr=%u, Used=%u, Target=%u, Deficit=%d\n",
       day, curr, used, target, deficit);
```

**Где используется:**
- `target` — **MP4 из СУБД**
- `curr` — **счётчик из MP** (`mi17_ops_count`)
- `used` — `0` (для P1, нет предшественников)
- `deficit` — **вычисляется**

---

#### **3️⃣ Promotion P2 (quota_promote_reserve.py)**

```cpp
// Чтение Target из MP4
target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);

// Подсчёт Curr из счётчика ops_count
auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_ops_count");
unsigned int curr = 0u;
for (unsigned int i = 0u; i < frames; ++i) {
    if (ops_count[i] == 1u) ++curr;
}

// Used = одобренные в P1 (serviceable)
auto approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_approve_s3");
unsigned int used = 0u;
for (unsigned int i = 0u; i < frames; ++i) {
    if (approve_s3[i] == 1u) ++used;  // ✅ Считаем из буфера P1
}

// Deficit = Остаток дефицита после P1
const int deficit = (int)target - (int)(curr + used);

printf("[PROMOTE P2 DEFICIT Day %u] Mi-17: Curr=%u, Used=%u, Target=%u, Deficit=%d\n",
       day, curr, used, target, deficit);
```

**Где используется:**
- `target` — **MP4 из СУБД**
- `curr` — **счётчик из MP** (`mi17_ops_count`)
- `used` — **буфер промоута P1** (`mi17_approve_s3`)
- `deficit` — **остаток** после P1

---

#### **4️⃣ Promotion P3 (quota_promote_inactive.py)**

```cpp
// Чтение Target из MP4
target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);

// Подсчёт Curr из счётчика ops_count
auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_ops_count");
unsigned int curr = 0u;
for (unsigned int i = 0u; i < frames; ++i) {
    if (ops_count[i] == 1u) ++curr;
}

// Used = одобренные в P1 + P2
auto approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_approve_s3");
auto approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_approve_s5");
unsigned int used = 0u;
for (unsigned int i = 0u; i < frames; ++i) {
    if (approve_s3[i] == 1u) ++used;  // P1 (serviceable)
    if (approve_s5[i] == 1u) ++used;  // P2 (reserve)
}

// Deficit = Остаток дефицита после P1 + P2
const int deficit = (int)target - (int)(curr + used);

printf("[PROMOTE P3 DEFICIT Day %u] Mi-17: Curr=%u, Used=%u, Target=%u, Deficit=%d\n",
       day, curr, used, target, deficit);
```

**Где используется:**
- `target` — **MP4 из СУБД**
- `curr` — **счётчик из MP** (`mi17_ops_count`)
- `used` — **буферы промоута P1+P2** (`mi17_approve_s3 + mi17_approve_s5`)
- `deficit` — **финальный остаток** после P1+P2

---

## 🎯 **2. Ранжирование по mfg_date (приоритеты)**

### **2.1. Demotion (operations → serviceable) — Oldest First**

**Файл:** `rtc_quota_ops_excess.py`

```cpp
// Ранжирование по mfg_date (oldest_first) среди агентов в operations
const unsigned int my_mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", idx);
unsigned int rank = 0u;

auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_ops_count");
for (unsigned int i = 0u; i < frames; ++i) {
    if (i == idx) continue;
    if (ops_count[i] != 1u) continue;  // ✅ Только агенты в operations (фильтр через буфер)
    
    const unsigned int other_mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", i);
    if (other_mfg < my_mfg || (other_mfg == my_mfg && i < idx)) {
        ++rank;  // Старше меня → ранг растёт
    }
}

if (rank < K) {
    // Я в числе K самых старых → демоунт в serviceable
    FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
}
```

**Где используется:**
- `ops_count` — **буфер из count_ops** (фильтр: только agенты в operations)
- `mp3_mfg_date_days` — **MP3 из СУБД** (дата производства в днях)
- `rank` — **позиция в приоритете** (меньше rank → старше → приоритет выше)

---

### **2.2. Promotion (serviceable → operations) — Youngest First**

**Файл:** `rtc_quota_promote_serviceable.py`

```cpp
// Ранжирование: youngest first среди РЕАЛЬНЫХ агентов в serviceable
const unsigned int my_mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", idx);
unsigned int rank = 0u;

// Используем svc_count буфер для фильтрации
auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_svc_count");
for (unsigned int i = 0u; i < frames; ++i) {
    if (i == idx) continue;
    if (svc_count[i] != 1u) continue;  // ✅ Только агенты в serviceable (фильтр через буфер)
    
    const unsigned int other_mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", i);
    // Youngest first: rank растёт если other МОЛОЖЕ меня
    if (other_mfg > my_mfg || (other_mfg == my_mfg && i < idx)) {
        ++rank;
    }
}

if (rank < K) {
    // Я в числе K самых молодых → промоут в operations
    FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);  // Подтверждаем intent
    
    // Записываем в буфер одобрения (для передачи в P2)
    auto approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_approve_s3");
    approve_s3[idx].exchange(1u);
}
```

**Где используется для ранжирования:**
- `svc_count` — **буфер из count_ops** (фильтр: только агенты в serviceable)
- `mp3_mfg_date_days` — **MP3 из СУБД** (дата производства)
- `rank` — **позиция в приоритете** (меньше rank → моложе → приоритет выше)

**Где используется для перехода 3→2:**
- `intent_state` — агент с `intent=2` переходит в operations в `state_manager_serviceable`

---

### **2.3. Переход 3→2 (serviceable → operations)**

**Файл:** `rtc_state_manager_serviceable.py`

```cpp
FLAMEGPU_AGENT_FUNCTION(rtc_apply_3_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent_state = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // Если intent=2 (промоут одобрен в quota_promote_serviceable), переходим в operations
    if (intent_state == 2u) {
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
        
        // Логирование
        if (aircraft_number >= 100000u || day == 226u || day == 227u || day == 228u) {
            printf("  [TRANSITION 3→2 Day %u] AC %u (idx %u): serviceable -> operations (intent=2 preserved)\\n", 
                   day, aircraft_number, idx);
        }
        
        // Переход в operations через setEndState()
        return flamegpu::ALIVE;  // ✅ Агент перейдёт в operations
    }
    
    // Если intent!=2, агент остаётся в serviceable
    return flamegpu::DEAD;  // ❌ НЕ переходим
}
```

**Регистрация:**
```python
rtc_func_3_to_2.setInitialState("serviceable")  # Обрабатываем только serviceable
rtc_func_3_to_2.setEndState("operations")       # Переход в operations
```

**Где используется:**
- `intent_state` — **устанавливается** в `quota_promote_serviceable` (rank < K → intent=2)
- **Переход происходит** в `state_manager_serviceable` (проверка intent=2)

---

## 📋 **3. Итоговая схема использования данных**

```
┌──────────────────────────────────────────────────────────────────┐
│ MP4 (flight_program_ac) — СУБД                                   │
│   ├─ ops_counter_mi8[day]  → Target для Mi-8                    │
│   └─ ops_counter_mi17[day] → Target для Mi-17                   │
├──────────────────────────────────────────────────────────────────┤
│ MP3 (helicopter_sim_master) — СУБД                               │
│   └─ mfg_date_days → Дата производства (приоритеты)             │
├──────────────────────────────────────────────────────────────────┤
│ count_ops (RTC модуль)                                           │
│   ├─ mi8_ops_count[idx] = 1  → Агент в operations (Mi-8)       │
│   ├─ mi17_ops_count[idx] = 1 → Агент в operations (Mi-17)      │
│   ├─ mi8_svc_count[idx] = 1  → Агент в serviceable (Mi-8)      │
│   └─ mi17_svc_count[idx] = 1 → Агент в serviceable (Mi-17)     │
├──────────────────────────────────────────────────────────────────┤
│ Квотирование (Demotion)                                          │
│   ├─ Target ← MP4 (из СУБД)                                     │
│   ├─ Curr ← sum(ops_count) (из count_ops)                       │
│   ├─ Balance = Curr - Target                                     │
│   └─ Ранжирование: oldest first (mfg_date ← MP3)               │
├──────────────────────────────────────────────────────────────────┤
│ Квотирование (Promotion P1/P2/P3)                                │
│   ├─ Target ← MP4 (из СУБД)                                     │
│   ├─ Curr ← sum(ops_count) (из count_ops)                       │
│   ├─ Used ← sum(approve_s3 + approve_s5 + ...) (каскад)        │
│   ├─ Deficit = Target - (Curr + Used)                           │
│   └─ Ранжирование: youngest first (mfg_date ← MP3)             │
│      + Фильтрация через svc_count (serviceable)                 │
└──────────────────────────────────────────────────────────────────┘
```

---

## 🎯 **4. Ответы на вопросы**

### **Q1: Где используются данные MP и счётчики ops_counter?**

**A1:**
- **MP4 (ops_counter_mi8/mi17)** — используется во **ВСЕХ** модулях квотирования для расчёта `target`:
  - `quota_ops_excess` (demotion)
  - `quota_promote_serviceable` (P1)
  - `quota_promote_reserve` (P2)
  - `quota_promote_inactive` (P3)

- **Счётчики (mi8/mi17_ops_count)** — используются во **ВСЕХ** модулях квотирования для расчёта `curr`:
  - Заполняются в `count_ops`
  - Считываются в `quota_*` модулях через цикл `for`

### **Q2: Где используется ранжирование по mfg_date для serviceable?**

**A2:**
- **В модуле `quota_promote_serviceable`** — для выбора K молодых агентов (youngest first)
- **Фильтрация:** через буфер `svc_count` (только агенты в serviceable)
- **Переход 3→2:** в `state_manager_serviceable` по `intent=2`

### **Q3: Нужны ли отдельные слои для избежания гонок?**

**A3:**
✅ **ДА!** Уже реализовано:
- `count_ops` — 3 слоя (reset, count_ops, count_serviceable)
- `quota_promote_serviceable` — записывает в `approve_s3`
- `quota_promote_reserve` — читает `approve_s3`, записывает в `approve_s5`
- `quota_promote_inactive` — читает `approve_s3 + approve_s5`, записывает в `approve_s1`

**Никаких гонок!** Каждый слой работает со своим буфером.

---

## ✅ **Резюме:**

1. **MP4 (Target)** — из СУБД `flight_program_ac`, используется для расчёта balance/deficit
2. **Счётчики (Curr)** — из `count_ops` → `ops_count`, используется для подсчёта агентов в operations
3. **Ранжирование serviceable** — по `mfg_date` (youngest first) через буфер `svc_count`
4. **Переход 3→2** — в `state_manager_serviceable` по `intent=2`
5. **Каскад Used** — через буферы `approve_s3/s5/s1` (без гонок)

