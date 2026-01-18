# Архитектура симуляции агрегатов на односторонних сообщениях (groups 3 и 4)

> **Статус:** draft v0.1 (15-01-2026)  
> **Цель:** новая модель оборота агрегатов (двигатели групп 3 и 4)  
> **Ограничения:** НЕ трогать загрузку данных и симуляцию планеров  
> **Источник nalетa:** `sim_masterv2` (планеры)  

---

## 1. Контекст

### 1.1 Базовая цепочка
1) Программа полетов → формирует потребность по планерам  
2) Симуляция планеров → `sim_masterv2` (day, aircraft, state, dt)  
3) Новая модель агрегатов → **читает `sim_masterv2`** и управляет оборотом агрегатов

### 1.2 Цель новой модели
Перейти от сложной MacroProperty+FIFO+assembly логики к **односторонним сообщениям**:
- **PlanerAgent → UnitAgent** (только в одну сторону)
- Агрегаты сами обеспечивают: комплектование, ремонт, списание

---

## 2. Объекты и состояния

### 2.1 PlanerAgent (пассивный источник)
PlanerAgent не имеет собственных переходов в этой модели, он лишь **публикует сообщение**:
- `aircraft_number`
- `planer_idx`
- `day`
- `in_ops` (0/1)
- `dt` (налет текущего дня)
- `planer_type` (1=Mi-8, 2=Mi-17)
- `required_slots[group]` (для групп 3 и 4 = 2)

### 2.2 UnitAgent (двигатель, group_by = 3 или 4)
Состояния:
- `operations` (2)
- `serviceable` (3)
- `repair` (4)
- `reserve` (5)
- `storage` (6)

---

## 3. Сообщения

### 3.1 Односторонние сообщения
**Только PlanerAgent → UnitAgent.**  
Агрегаты НЕ отправляют сообщений обратно.

### 3.2 Содержание сообщения
```
PlanerMessage {
  aircraft_number,
  planer_idx,
  in_ops,
  dt,
  planer_type,
  required_slots_g3,
  required_slots_g4
}
```

### 3.3 Слоты
Для каждого planera и группы:
- `mp_planer_slots[group, idx]` — занято
- `mp_planer_need[group, idx]` — сколько еще нужно

---

## 4. Основные правила

### 4.0 Матрица переходов планера ↔ агрегатов (rule)

- Неисправные агрегаты (`repair`/`storage`) уходят по своему циклу независимо от статуса планера.
- На планер в `operations`/`serviceable` **нельзя** устанавливать агрегаты не `operations`/`serviceable`.

### 4.1 Табличная матрица 7×7 (планер → планер)

| Из \\ В | operations | repair | serviceable | reserve | storage | inactive | unserviceable |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **operations** | state=operations; aircraft_number=planer; need=0 | if assembly_trigger=1 → state=operations; aircraft_number=planer; else state=serviceable; aircraft_number=0 | state=serviceable; aircraft_number=0 | state=serviceable; aircraft_number=0 | state=serviceable; aircraft_number=0 | state=serviceable; aircraft_number=0 | state=serviceable; aircraft_number=0 |
| **repair** | if assembly_trigger=1 → state=operations; aircraft_number=planer; else state=serviceable; aircraft_number=0 | if assembly_trigger=1 → state=serviceable; aircraft_number=planer; else state=serviceable; aircraft_number=0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 |
| **serviceable** | if need>0 → state=operations; aircraft_number=planer; else state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 |
| **reserve** | if need>0 → state=operations; aircraft_number=planer; else state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 |
| **storage** | if need>0 → state=operations; aircraft_number=planer; else state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 |
| **inactive** | if need>0 → state=operations; aircraft_number=planer; else state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 |
| **unserviceable** | if need>0 → state=operations; aircraft_number=planer; else state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 | state=serviceable; aircraft_number=planer/0 |

### 4.1 Назначение агрегатов
Агрегат в `serviceable` или `reserve`:
1) Читает сообщения планеров  
2) Если `in_ops=1` и есть `need>0`, пытается занять слот (atomic)  
3) Если слот захвачен → `aircraft_number` планера и `state=operations`

### 4.2 Зимовка
Если `in_ops=1`, но `dt=0` — агрегат **НЕ снимается**, остается в ops (ожидаемо).

### 4.3 Выход планера из ops
Если планер `in_ops` сменился на 0:
1) Все агрегаты на этом планере отцепляются
2) Переходят в `serviceable`
3) `mp_planer_slots--`

Исключение:
- агрегаты в `repair`/`storage` **не разукомплектовываются**.

### 4.4 Ремонт и хранение
Правила аналогичны планерам:
- **В ремонт**: `ppr >= oh` и `sne < br`
- **В storage**: `sne >= ll` или `(ppr >= oh AND sne >= br)`

### 4.5 Repair_time
`repair_time` берется из `md_components` **по group_by (3/4)**.

### 4.6 Spawn
Динамический spawn:
- **Запрещен** до `repair_time` дней (для двигателей)  
- Разрешается, если **svc и rsv пусты** и `need > 0`

---

## 5. Макропроперти и индексы

Обязательные:
- `mp_planer_in_ops_history[day * MAX_PLANERS + idx]`
- `mp_planer_dt[day * MAX_PLANERS + idx]`
- `mp_planer_type[idx]`
- `mp_planer_slots[group * MAX_PLANERS + idx]`
- `mp_planer_need[group * MAX_PLANERS + idx]`

Служебные:
- `mp_request_count[group]` (для планеров, которым не хватило агрегатов)
- `mp_spawn_block_days[group]` (период запрета спавна = repair_time)

---

## 6. Фазы шага (proposal)

1) **PlanerMessages**: публикация сообщений на основе `sim_masterv2`
2) **ResetNeeds**: `mp_planer_need = required_slots`
3) **Detach**: агрегаты на планерах `in_ops=0` → `serviceable`
4) **Assign**: svc/rsv пытаются занять слоты (atomic)
5) **OpsIncrement**: `sne/ppr += dt`
6) **Transitions**:
   - `operations → repair` (ppr >= oh, sne < br)
   - `operations → storage` (sne >= ll или br-ветка)
   - `repair → reserve` (repair_days >= repair_time)
7) **Spawn** (если svc+rsv пусты и day >= repair_time)
8) **Write MP2**

---

## 7. Инварианты

- На каждом планере в `operations` **ровно 2 двигателя** (groups 3/4).
- `dt=0` в operations не разукомплектовывает планер.
- `spawn` не происходит до `repair_time`.
- `storage` — терминальное состояние.

---

## 8. Границы модели

На этапе v0.1:
- Только группы **3 и 4**.
- Остальные группы не моделируются.
- Никакой модификации `sim_masterv2`.

---

## 9. Следующие шаги

1) Создать `code/sim_v2/units_msg/` (отдельный контур)  
2) Реализовать PlanerMessage Reader (из `sim_masterv2`)  
3) Реализовать базовую логику Assign/Detach/Repair/Storage  
4) Поднять валидатор для групп 3/4 аналогично планерам  


