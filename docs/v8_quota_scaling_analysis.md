# Анализ ограничения квотирования V8 и рекомендации FLAME GPU

> **Дата:** 20.01.2026
> **Версия:** V8 (message-only)
> **Статус:** Архитектурное предложение

---

## 📋 Проблема

### Текущее ограничение V8

В коммите `e06d0fbc` зафиксировано ограничение:

> **message‑only квоты P2/P3 ограничены 1 решением на шаг (одно сообщение от QM‑агента)**

### Корневая причина (FLAME GPU constraint)

Из официальной документации FLAME GPU:

> "Agents may output a single message from an agent function"

Каждый агент может выдать **только одно сообщение** за вызов функции. В текущей реализации `rtc_quota_manager_v8_msg` пытается вызывать `setIndex()` в цикле:

```cpp
for (unsigned int i = 0u; i < K; ++i) {
    FLAMEGPU->message_out.setIndex(ops_mi8[i]);  // ⚠️ Перезаписывается!
    FLAMEGPU->message_out.setVariable<unsigned char>("action", 1u);
    ...
}
```

**Проблема:** Каждый вызов `setIndex()` перезаписывает предыдущий. Сохраняется только последнее значение.

---

## 🔍 Исследование FLAME GPU Community

### Issue #295: Message input/output to same list from multiple functions in same layer

> "For shared message output; it requires tracking sharing an offset into the message output buffer"
> "Not worth implementing until concurrent functions within layers is operational."

**Статус:** Open, Priority: Low — FLAME GPU не планирует поддержку multi-output от одного агента.

### Discussion #1124: Same output message in the same layer with different agents types

Пользователь столкнулся с аналогичной проблемой — несколько агентов пытаются писать в один MessageList в одном слое.

**Ответ разработчиков:** Это ограничение архитектуры. Рекомендуется использовать:
- Разные слои для разных функций
- Распределённое принятие решений

---

## 💡 Рекомендованные паттерны FLAME GPU

### Паттерн 1: Distributed Decision Making (DDM) — **РЕКОМЕНДУЕТСЯ**

Вместо централизованного QM, **каждый агент сам принимает решение** на основе глобального состояния.

```
┌─────────────────────────────────────────────────────────────────────────┐
│ DISTRIBUTED DECISION MAKING                                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Слой 1: ANNOUNCE (агенты → MacroProperty)                               │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ Каждый агент: atomicAdd(count[state][group])                       │ │
│  │ Записывает idx в sorted_candidates[state][rank]                    │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  Слой 2: COMPUTE (QM → MacroProperty)                                    │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ QM читает counts, вычисляет:                                       │ │
│  │   - quota_left_mi8/mi17                                            │ │
│  │   - threshold_p1/p2/p3 (idx порог для промоута)                    │ │
│  │   - demote_threshold (idx порог для демоута)                       │ │
│  │ Записывает thresholds в MacroProperty                              │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  Слой 3: APPLY (каждый агент сам)                                        │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ Каждый агент читает threshold для своего state                     │ │
│  │ if (my_rank < threshold) → apply decision                          │ │
│  │ Использует atomicExchange для "захвата" RepairLine                 │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**Преимущества:**
- Полностью параллельно (масштабируется на тысячи агентов)
- Детерминированный порядок (по idx)
- Нет ограничения "1 сообщение на агента"

**Недостатки:**
- Больше слоёв (latency)
- Сложнее координация RepairLine

### Паттерн 2: Threshold-based Messaging

QM публикует **один** threshold в сообщении. Каждый агент проверяет свой rank.

```cpp
// QM: одно сообщение с thresholds
FLAMEGPU->message_out.setVariable<unsigned int>("promote_threshold_mi8", threshold);

// Агент: проверяет свой rank
unsigned int my_rank = /* вычисляется через MacroProperty prefix sum */;
if (my_rank < threshold) {
    FLAMEGPU->setVariable<unsigned int>("promoted", 1u);
}
```

**Преимущества:**
- Один QM = одно сообщение (укладывается в ограничение)
- Простая реализация

**Недостатки:**
- Требует prefix sum для вычисления rank
- Сложнее с RepairLine (нужен atomic capture)

### Паттерн 3: Multi-QM Pool

Создать **пул QM-агентов** (по одному на каждое потенциальное решение).

```
QM_Slot[0] → Decision для агента с idx = candidates[0]
QM_Slot[1] → Decision для агента с idx = candidates[1]
...
QM_Slot[N-1] → Decision для агента с idx = candidates[N-1]
```

**Преимущества:**
- Каждый QM_Slot выдаёт своё сообщение (нет ограничения)
- Параллельная обработка

**Недостатки:**
- Накладные расходы на создание/управление агентами
- Сложнее масштабирование (нужно заранее знать max decisions)

---

## 🏗️ Рекомендованная архитектура для V9 (масштабирование)

### Концепция: "Ranked Atomic Capture"

Гибрид DDM + threshold, оптимизированный для RepairLine.

```
┌─────────────────────────────────────────────────────────────────────────┐
│ V9: RANKED ATOMIC CAPTURE                                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Phase 1: COUNT & RANK (параллельно)                                     │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ Каждый агент:                                                      │ │
│  │   rank = atomicAdd(candidates_count[state][group], 1)              │ │
│  │   candidates_idx[state][group][rank] = my_idx                      │ │
│  │   candidates_repair_time[state][group][rank] = my_repair_time      │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  Phase 2: QM DECISION (один QM на группу)                                │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ QM_mi8: читает candidates, вычисляет:                              │ │
│  │   - deficit = target - ops_count                                   │ │
│  │   - K = min(deficit, candidates_count, repair_lines_available)     │ │
│  │   - approve_count[state] = K                                       │ │
│  │   - Сортирует candidates по idx (youngest first)                   │ │
│  │   - Записывает approve_idx[0..K-1] в MacroProperty                 │ │
│  │ QM публикует MessageBucket[key=group]: approve_count               │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  Phase 3: ATOMIC CAPTURE RepairLine (параллельно)                        │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ Каждый approved агент:                                             │ │
│  │   line_id = atomicExchange(repair_line_slots[my_rank], 0xFFFF)     │ │
│  │   if (line_id != 0xFFFF) → captured = true                         │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  Phase 4: APPLY (параллельно)                                            │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ Каждый агент с captured:                                           │ │
│  │   promoted = 1                                                     │ │
│  │   repair_line_id = line_id                                         │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Масштабирование на агрегаты

Для агрегатов (следующий уровень) та же архитектура применима:

| Уровень | Агенты | Ресурс | Quota |
|---------|--------|--------|-------|
| **Планеры (V8)** | Helicopter | RepairLine | repair_number |
| **Агрегаты** | Unit | UnitRepairSlot | unit_repair_capacity |
| **Сборки** | Assembly | AssemblyLine | assembly_capacity |

**Ключевой принцип:** Каждый уровень использует свой "pool" слотов, агенты конкурируют за них через atomic capture.

---

## 💡 ИСПРАВЛЕНИЕ: MessageBucket + Threshold (20-01-2026)

**Уточнение от Алексея:** MessageBucket поддерживает broadcast — один QM отправляет сообщение с threshold, и **все агенты** в bucket его читают.

### Корректная схема V9 с MessageBucket

```
┌─────────────────────────────────────────────────────────────────────────┐
│ MessageBucket: THRESHOLD-BASED PROMOTION                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Phase 1: COUNT (MacroProperty, параллельно)                             │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ Каждый unsvc агент (ready):                                        │ │
│  │   atomicAdd(unsvc_ready_count[group], 1)                           │ │
│  │   // idx уже есть у агента, rank не нужен!                         │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  Phase 2: QM → MessageBucket (ОДНО сообщение на группу!)                 │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ QM_mi8 (group_by=1):                                               │ │
│  │   // Собирает idx всех unsvc_ready в локальный массив              │ │
│  │   // Сортирует по idx DESC (youngest first = больший idx)          │ │
│  │   deficit = target - ops_count                                     │ │
│  │   approve_p2 = min(deficit, unsvc_ready_count, slots_available)    │ │
│  │   // Находит threshold: idx N-го по молодости                      │ │
│  │   threshold_idx = sorted_idx[approve_p2 - 1]  // граница           │ │
│  │                                                                    │ │
│  │   message_out.setKey(1)  // bucket key = Mi-8                      │ │
│  │   message_out.setVariable("threshold_idx", threshold_idx)          │ │
│  │   message_out.setVariable("approve_count", approve_p2)             │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  Phase 3: Агенты читают ИЗ СВОЕГО bucket + Atomic Capture               │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ Каждый unsvc_ready агент:                                          │ │
│  │   for (msg : message_in(my_group_by)) {                            │ │
│  │       threshold = msg.getVariable("threshold_idx")                 │ │
│  │       approve = msg.getVariable("approve_count")                   │ │
│  │                                                                    │ │
│  │       if (my_idx >= threshold) {                                   │ │
│  │           // Я в числе N "молодых"!                                │ │
│  │           // Захватываю слот RepairLine через atomic               │ │
│  │           for (s = 0; s < slots_count; ++s) {                      │ │
│  │               old = atomicExch(slot_taken[s], 1)                   │ │
│  │               if (old == 0) {                                      │ │
│  │                   my_line = slot_ids[s]                            │ │
│  │                   break                                            │ │
│  │               }                                                    │ │
│  │           }                                                        │ │
│  │           if (my_line != INVALID) promoted = 1                     │ │
│  │       }                                                            │ │
│  │   }                                                                │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Преимущества над текущим V8

| Аспект | V8 (текущий, сломан) | V9 (MessageBucket) |
|--------|----------------------|-------------------|
| Сообщений от QM | N (не работает!) | **1** (broadcast) |
| Ограничение FLAME GPU | Нарушено | Соблюдено |
| Назначение линий | В QM (не работает) | Atomic capture |
| Параллелизм | Нет | Полный |
| Масштабирование | Плохое | **Отличное** |

### Ключевые изменения для реализации

1. **Добавить MessageBucket** вместо MessageArray для QuotaDecision:
   ```python
   msg_quota_threshold = model.newMessageBucket("QuotaThreshold")
   msg_quota_threshold.setBounds(1, 2)  # 1=Mi-8, 2=Mi-17
   msg_quota_threshold.newVariableUInt("threshold_idx_p2")
   msg_quota_threshold.newVariableUInt("threshold_idx_p3")
   msg_quota_threshold.newVariableUInt("approve_p2")
   msg_quota_threshold.newVariableUInt("approve_p3")
   ```

2. **QM собирает idx и сортирует** в локальном массиве (уже делает!)
3. **QM публикует threshold** через `setKey(group_by)`
4. **Агенты проверяют свой idx** против threshold
5. **Atomic capture** для RepairLine слотов

### Для масштабирования на агрегаты

Та же схема работает на любом уровне:

```cpp
// Агрегат проверяет свой idx против threshold
for (msg : message_in(my_unit_type)) {
    threshold = msg.getVariable("threshold_idx");
    if (my_idx >= threshold) {
        // Захватываю слот UnitRepairLine
        ...
    }
}
```

---

## 📊 Сравнение подходов

| Аспект | V8 (текущий) | DDM | Threshold | Multi-QM |
|--------|--------------|-----|-----------|----------|
| Сообщений на шаг | 1 ⚠️ | 0 (MacroProperty) | 1 | N |
| Параллелизм | Нет | Полный | Частичный | Полный |
| Сложность | Низкая | Средняя | Средняя | Высокая |
| Масштабируемость | Плохая | Отличная | Хорошая | Хорошая |
| RepairLine coord | Сложно | Atomic capture | Atomic capture | Сообщения |

---

## ✅ Рекомендация

### Для V9 (ближайший шаг):

**Паттерн DDM (Distributed Decision Making)** с atomic capture для RepairLine.

**Почему:**
1. Полностью укладывается в ограничения FLAME GPU
2. Масштабируется на тысячи агентов
3. Единый паттерн для планеров, агрегатов, сборок
4. Детерминированный результат (сортировка по idx)

### Изменения для V9:

1. **Удалить цикл setIndex в QM** — он не работает
2. **Добавить MacroProperty для кандидатов:**
   - `candidates_unsvc_idx[MAX_FRAMES]`
   - `candidates_inactive_idx[MAX_FRAMES]`
   - `candidates_count[2]` (mi8, mi17)
3. **Добавить MacroProperty для одобренных:**
   - `approve_unsvc_count[2]`
   - `approve_inactive_count[2]`
4. **Реализовать atomic capture для RepairLine:**
   - Агенты с rank < approve_count захватывают слоты
   - Использовать `atomicExchange(slot, 0xFFFFFFFF)`

### Временное решение для V8:

До реализации V9 можно использовать **итеративный подход**:
- Несколько шагов квотирования (P2 шаг 1, P2 шаг 2, ...)
- Каждый шаг обрабатывает 1 решение
- Медленнее, но корректно

---

## 🔗 Связанные документы

- `docs/limiter_architecture.md` — текущая архитектура V8
- `docs/rtc_pipeline_architecture.md` — baseline архитектура
- FLAME GPU Issue #295 — multi-output limitations
- FLAME GPU Discussion #1124 — same message list multiple agents

---

*Документ создан: 20.01.2026*
*Автор: AI Analysis*
