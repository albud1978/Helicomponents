# Исправление семантики intent_state

**Дата:** 09-10-2025  
**Статус:** ✅ ИСПРАВЛЕНО

---

## Проблема

В СУБД на Day 226 (день spawn) новые агенты показывали:
```
state=operations, intent=0
```

**Это неправильно,** потому что:
- `intent=0` означает "нет намерений" (агент уже в нужном state)
- Но агенты **только что** перешли 3→2 (serviceable→operations)
- Должно быть: `intent=2` ("хочу в operations")

---

## Причина

### Старый код `rtc_state_manager_serviceable.py`:
```cpp
if (intent_state == 2u) {
    FLAMEGPU->setVariable<unsigned int>("intent_state", 0u);  // ❌ Сброс сразу
    return flamegpu::ALIVE;
}
```

**Проблема:** `intent` сбрасывался **до** MP2 экспорта, поэтому в СУБД на Day 226 уже было `intent=0`.

---

## Решение

### 1. `rtc_state_manager_serviceable.py` - НЕ сбрасывать intent
```cpp
if (intent_state == 2u) {
    // НЕ сбрасываем intent! Он будет установлен в 0 на следующем шаге
    // ВАЖНО: intent=2 сохраняется, чтобы в MP2 было видно что агент хотел в operations
    return flamegpu::ALIVE;  // Агент перейдёт в operations с intent=2
}
```

### 2. `rtc_state_2_operations.py` - установить intent=0
```cpp
// 3. Остаёмся в operations
// intent=0 означает "нет намерений" (агент уже в нужном состоянии)
FLAMEGPU->setVariable<unsigned int>("intent_state", 0u);
```

**Было:** `intent=2` (хочу в operations) для **всех** агентов в operations  
**Стало:** `intent=0` (нет намерений) для агентов, **остающихся** в operations

---

## Результат

### До исправления:
| День | State | Intent | Объяснение |
|------|-------|--------|------------|
| 226 | operations | **0** ❌ | Неправильно: intent сброшен до экспорта |
| 227 | operations | 2 | Неправильно: все в operations имеют intent=2 |

### После исправления:
| День | State | Intent | Объяснение |
|------|-------|--------|------------|
| **226** | **operations** | **2** ✅ | Правильно: агенты "хотели в operations" и попали |
| **227** | operations | **0** ✅ | Правильно: "нет намерений", уже в operations |
| 228+ | operations | 0 ✅ | Продолжают работать |

---

## Семантика intent_state

| Intent | Значение | Когда устанавливается |
|--------|----------|----------------------|
| **0** | Нет намерений (агент в нужном state) | `state_2_operations` для агентов, остающихся в operations |
| **2** | Хочу в operations | - `state_2_operations` для агентов на Day 0<br>- При переходе 3→2, 5→2, 1→2 (сохраняется из промоута) |
| **3** | Хочу в serviceable | Демоут (2→3) |
| **4** | Хочу в repair | `state_2_operations` при PPR >= OH |
| **6** | Хочу в storage | `state_2_operations` при SNE >= LL или BR |

---

## Тайминг событий (Day 226)

1. **Spawn:** Создание агентов в `serviceable` + `intent=2`
2. **Промоут:** Одобрение → `intent=2` остаётся
3. **Переход 3→2:** Агенты переходят в `operations` + **intent=2 сохраняется**
4. **MP2 экспорт:** Записывает `state=operations, intent=2` ✅
5. **Day 227 - state_2_operations:** Устанавливает `intent=0`
6. **MP2 экспорт Day 227:** Записывает `state=operations, intent=0` ✅

---

## Проверка

```sql
SELECT day_u16, aircraft_number, state, intent_state, sne, ppr 
FROM sim_masterv2 
WHERE aircraft_number >= 100000 AND day_u16 IN (226, 227, 228)
```

**Результат:**
```
Day 226: AC 100000-100006, state=operations, intent=2, sne=0, ppr=0     ✅
Day 227: AC 100000-100006, state=operations, intent=0, sne=83, ppr=83   ✅
Day 228: AC 100000-100006, state=operations, intent=0, sne=166, ppr=166 ✅
```

---

## Выводы

1. ✅ `intent=2` на Day 226 показывает что агенты **хотели** в operations и сразу туда попали
2. ✅ `intent=0` на Day 227+ показывает что агенты **уже** в operations и намерений нет
3. ✅ MP2 экспорт корректно фиксирует состояние **после всех переходов**
4. ✅ Семантика intent теперь логична и соответствует реальности

---

**Статус:** ✅ ГОТОВО  
**Файлы изменены:**
- `code/sim_v2/rtc_state_manager_serviceable.py`
- `code/sim_v2/rtc_state_2_operations.py`

