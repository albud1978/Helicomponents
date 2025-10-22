# 📋 Добавлено детальное логирование всех переходов состояний

**Дата:** 08-10-2025  
**Цель:** Отследить движение новых агентов (ACN 100000+) через систему квотирования

---

## ✅ Добавлено логирование для всех переходов

### 1. **Переходы из operations (state=2)**

**Файл:** `code/sim_v2/rtc_state_manager_operations.py`

| Переход | Код | Описание | Формат лога |
|---------|-----|----------|-------------|
| 2→2 | operations → operations | Остаёмся в операциях | `[TRANSITION 2→2 Day X] AC Y (idx Z): staying in operations, sne=A, ppr=B` |
| 2→3 | operations → serviceable | Квотный демоут | `[TRANSITION 2→3 Day X] AC Y (idx Z): operations -> serviceable (DEMOUNT), sne=A, ppr=B` |
| 2→4 | operations → repair | Переход в ремонт | `[TRANSITION 2→4 Day X] AC Y (idx Z): operations -> repair, sne=A, ppr=B, oh=C, br=D` |
| 2→6 | operations → storage | Переход в хранение | `[TRANSITION 2→6 Day X] AC Y (idx Z): operations -> storage, sne=A, ppr=B, ll=C, oh=D, br=E` |

**Условие логирования:** 
- Для новых агентов (ACN >= 100000): ВСЕГДА
- Для остальных: дни 226-228 (вокруг spawn)

---

### 2. **Переходы из repair (state=4)**

**Файл:** `code/sim_v2/rtc_state_manager_repair.py`

| Переход | Код | Описание | Формат лога |
|---------|-----|----------|-------------|
| 4→4 | repair → repair | Продолжается ремонт | Без логов (информативности мало) |
| 4→5 | repair → reserve | Ремонт завершён | `[TRANSITION 4→5 Day X] AC Y (idx Z): repair -> reserve, repair_days=A/B` |

---

### 3. **Промоуты через квотирование**

#### 3a. **Приоритет 1: serviceable → operations (3→2)**

**Файл:** `code/sim_v2/rtc_quota_promote_serviceable.py`

| Результат | Формат лога |
|-----------|-------------|
| ✅ Одобрен | `[PROMOTE P1→2 Day X] AC Y (idx Z): rank=A/B serviceable->operations, deficit=C` |
| ❌ Отклонён | `[PROMOTE P1 REJECT Day X] AC Y (idx Z): rank=A >= K=B, staying in serviceable` |

**Когда срабатывает:**
- Агент в state=serviceable, intent=2
- Проверяется deficit, ранжирование FCFS
- Если прошёл → intent подтверждается (intent=2), записывается в `mi8_approve_s3`
- Если НЕ прошёл → intent=3 (остаётся в serviceable)

---

#### 3b. **Приоритет 2: reserve → operations (5→2)**

**Файл:** `code/sim_v2/rtc_quota_promote_reserve.py`

| Результат | Формат лога |
|-----------|-------------|
| ✅ Одобрен | `[PROMOTE P2→2 Day X] AC Y (idx Z): rank=A/B reserve->operations, deficit=C` |
| ❌ Отклонён | `[PROMOTE P2 REJECT Day X] AC Y (idx Z): rank=A >= K=B, staying in reserve` |

---

#### 3c. **Приоритет 3: inactive → operations (1→2)**

**Файл:** `code/sim_v2/rtc_quota_promote_inactive.py`

| Результат | Формат лога |
|-----------|-------------|
| ✅ Одобрен | `[PROMOTE P3→2 Day X] AC Y (idx Z): rank=A/B inactive->operations, deficit=C` |
| ❌ Отклонён | `[PROMOTE P3 REJECT Day X] AC Y (idx Z): rank=A >= K=B, staying in inactive` |

---

### 4. **Spawn (создание новых агентов)**

**Файл:** `code/sim_v2/rtc_modules/rtc_spawn_v2.py`

| Событие | Формат лога |
|---------|-------------|
| Создание агента | `[SPAWN Day X] Creating AC Y (idx Z), state=serviceable, intent=2 (wants operations)` |
| Общая сводка | `[SPAWN Day X] Creating N agents Mi-17: idx A-B, acn C-D` |

---

## 🔍 Логика фильтрации логов

**Логи выводятся для:**
1. **Новых агентов:** ACN >= 100000 (ВСЕГДА)
2. **Критичные дни:** 226-230 (дни около spawn, для ВСЕХ агентов)

**Цель:** Минимизировать шум в логах, но отследить:
- Создание новых агентов
- Их попытки войти в operations
- Причины отказа (если есть)
- Переходы между состояниями

---

## 📊 Ожидаемый flow для новых агентов

### Нормальный сценарий:
```
Day 226:
  [SPAWN Day 226] Creating AC 100000 (idx 279), state=serviceable, intent=2
  [SPAWN Day 226] Creating AC 100001 (idx 280), state=serviceable, intent=2
  ...
  [SPAWN Day 226] Creating 7 agents Mi-17: idx 279-285, acn 100000-100006

Day 227 (первый промоут):
  [PROMOTE P1 DEFICIT Day 227] Mi-17: Used=150, Target=157, Deficit=7
  [PROMOTE P1→2 Day 227] AC 100000 (idx 279): rank=0/7 serviceable->operations, deficit=7
  [PROMOTE P1→2 Day 227] AC 100001 (idx 280): rank=1/7 serviceable->operations, deficit=7
  ...
  [TRANSITION 2→2 Day 227] AC 100000 (idx 279): staying in operations, sne=0, ppr=0

Day 228+:
  [TRANSITION 2→2 Day 228] AC 100000 (idx 279): staying in operations, sne=X, ppr=X
  (наработка растёт)
```

### Проблемный сценарий (если deficit=0):
```
Day 226:
  [SPAWN Day 226] Creating 7 agents...

Day 227:
  [PROMOTE P1 DEFICIT Day 227] Mi-17: Used=150, Target=150, Deficit=0
  (Early exit! Промоут не запускается)
  
  (Новые агенты остаются в serviceable)

Day 228-230:
  [PROMOTE P1 REJECT Day 228] AC 100000 (idx 279): rank=0 >= K=0, staying in serviceable
  (Агенты НЕ попадают в operations!)
```

---

## 🧪 Команда для тестового прогона

```bash
cd "/home/budnik_an/cube linux/cube" && \
rm -rf code/sim_v2/__pycache__ code/sim_v2/rtc_modules/__pycache__ code/sim_v2/components/__pycache__ && \
export CUDA_PATH=/usr/local/cuda-12.8 CUBE_CONFIG_PATH="/home/budnik_an/cube linux/cube" && \
python3 code/sim_v2/orchestrator_v2.py \
  --modules spawn_v2 state_2_operations quota_ops_excess quota_promote_serviceable quota_promote_reserve quota_promote_inactive states_stub state_manager_operations state_manager_repair state_manager_storage \
  --steps 300 \
  --enable-mp2 \
  --drop-table \
  2>&1 | tee /tmp/spawn_transitions_300.log
```

---

## 📋 Проверка логов

```bash
# Все события spawn
grep "SPAWN" /tmp/spawn_transitions_300.log | head -20

# Промоуты новых агентов
grep "AC 10000[0-6]" /tmp/spawn_transitions_300.log | grep "PROMOTE"

# Переходы новых агентов
grep "AC 10000[0-6]" /tmp/spawn_transitions_300.log | grep "TRANSITION"

# Проверка deficit на критичные дни
grep "DEFICIT Day 22[6-9]" /tmp/spawn_transitions_300.log

# Полный flow для конкретного агента
grep "AC 100000" /tmp/spawn_transitions_300.log | head -50
```

---

## ✅ Итоги

**Добавлено логирование для:**
- ✅ 2→2 (operations → operations)
- ✅ 2→3 (operations → serviceable, демоут)
- ✅ 2→4 (operations → repair)
- ✅ 2→6 (operations → storage)
- ✅ 4→5 (repair → reserve)
- ✅ 3→2 (serviceable → operations, промоут P1)
- ✅ 5→2 (reserve → operations, промоут P2)
- ✅ 1→2 (inactive → operations, промоут P3)
- ✅ Spawn (создание агентов)

**НЕ логируются** (малая информативность):
- 1→1 (inactive → inactive)
- 3→3 (serviceable → serviceable)
- 4→4 (repair → repair, внутри ремонта)
- 5→5 (reserve → reserve)
- 6→6 (storage → storage)

---

**Автор:** AI Assistant  
**Дата:** 08-10-2025

