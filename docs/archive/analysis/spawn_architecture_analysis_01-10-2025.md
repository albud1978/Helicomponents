# Анализ архитектуры Spawn из sim_master (b6bc62c8)

**Дата**: 01-10-2025  
**Цель**: Понять КАК spawn работал в sim_master для корректной интеграции в orchestrator_v2

## 1. Режим запуска spawn в sim_master

### Критические особенности:
```python
os.environ['HL_RTC_MODE'] = 'spawn_only'    # ТОЛЬКО spawn, БЕЗ квот!
os.environ['HL_ENV_MINIMAL'] = '1'           # БЕЗ MP1/MP5!
```

**Вывод**: spawn в sim_master работал **ИЗОЛИРОВАННО** от основной симуляции!

## 2. Структура spawn агентов

### 2.1 spawn_mgr (менеджер, 1 экземпляр)
**Переменные:**
- `next_idx` - следующий свободный idx в FRAMES
- `next_acn` - следующий aircraft_number
- `next_psn` - следующий partseqno

**Логика:**
- Читает `mp4_new_counter_mi17_seed[safe_day]` - сколько создать в день
- Публикует в MacroProperty: `spawn_need_u32[safe_day]`, `spawn_base_idx_u32[safe_day]`, etc
- Сдвигает курсоры: `next_idx += need`

### 2.2 spawn_ticket (тикеты, 16 экземпляров)
**Переменные:**
- `ticket` - номер тикета (0..15)

**Логика:**
- Читает из MacroProperty: `need_mp[safe_day]`, `bidx_mp[safe_day]`, etc
- Если `ticket >= need` → ничего не делает
- Создаёт агента через `agent_out` с idx = `base_idx + ticket`

## 3. MacroProperty для spawn

**КРИТИЧНО**: В model_build используются **МАССИВЫ размера MAX_DAYS**:

```python
env.newMacroPropertyUInt("spawn_need_u32", MAX_DAYS)      # Массив по дням!
env.newMacroPropertyUInt("spawn_base_idx_u32", MAX_DAYS)
env.newMacroPropertyUInt("spawn_base_acn_u32", MAX_DAYS)
env.newMacroPropertyUInt("spawn_base_psn_u32", MAX_DAYS)
```

**Индексация в RTC:**
```cpp
auto need_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_need_u32");
const unsigned int need = need_mp[safe_day];  // Индекс [safe_day], НЕ [0]!
```

## 4. Переменные новорожденного агента (ПОРЯДОК из model_build)

```cpp
// 1. Идентификаторы
out.setVariable<unsigned int>("idx", idx);
out.setVariable<unsigned int>("psn", psn);
out.setVariable<unsigned int>("aircraft_number", acn);
out.setVariable<unsigned int>("ac_type_mask", 64u);
out.setVariable<unsigned int>("group_by", 2u);
out.setVariable<unsigned int>("partseqno_i", 70482u);

// 2. Времена из Environment
out.setVariable<unsigned int>("repair_time", FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const"));
out.setVariable<unsigned int>("assembly_time", ...);
out.setVariable<unsigned int>("partout_time", ...);

// 3. Дата производства
out.setVariable<unsigned int>("mfg_date", mfg);

// 4. Статус и наработки
out.setVariable<unsigned int>("status_id", 3u);  // ❌ В orchestrator_v2 НЕТ status_id!
out.setVariable<unsigned int>("sne", 0u);
out.setVariable<unsigned int>("ppr", 0u);
out.setVariable<unsigned int>("repair_days", 0u);
out.setVariable<unsigned int>("ops_ticket", 0u);
out.setVariable<unsigned int>("intent_flag", 0u);

// 5. Нормативы из Environment
out.setVariable<unsigned int>("ll", FLAMEGPU->environment.getProperty<unsigned int>("mi17_ll_const"));
out.setVariable<unsigned int>("oh", ...);
out.setVariable<unsigned int>("br", ...);

// 6. MP5 переменные
out.setVariable<unsigned int>("daily_today_u32", 0u);
out.setVariable<unsigned int>("daily_next_u32", 0u);

// 7. Триггеры
out.setVariable<unsigned int>("active_trigger", 0u);
out.setVariable<unsigned int>("assembly_trigger", 0u);
out.setVariable<unsigned int>("partout_trigger", 0u);

// 8. S6 счётчики
out.setVariable<unsigned int>("s6_days", 0u);
out.setVariable<unsigned int>("s6_started", 0u);
```

## 5. Environment properties для spawn

### Создаются в model_build:
```python
# Скаляры
env.newPropertyUInt("version_date", 0)
env.newPropertyUInt("frames_total", 0)
env.newPropertyUInt("days_total", 0)
env.newPropertyUInt("frames_initial", 0)  # Для spawn: откуда начинать idx
env.newPropertyUInt("mi17_repair_time_const", 0)
env.newPropertyUInt("mi17_assembly_time_const", 0)
env.newPropertyUInt("mi17_partout_time_const", 0)
env.newPropertyUInt("mi17_ll_const", 0)
env.newPropertyUInt("mi17_oh_const", 0)
env.newPropertyUInt("mi17_br_const", 0)

# PropertyArrays
env.newPropertyArrayUInt32("mp4_new_counter_mi17_seed", [0] * MAX_DAYS)
env.newPropertyArrayUInt32("month_first_u32", [0] * MAX_DAYS)
```

### Устанавливаются в sim_master ПОСЛЕ создания симуляции:
```python
sim = hm.build_simulation()  # Создание
sim.setEnvironmentPropertyUInt("version_date", ...)  # Установка!
sim.setEnvironmentPropertyArrayUInt32("mp4_new_counter_mi17_seed", ...)
```

## 6. Порядок слоёв spawn

В model_build spawn добавляется **В КОНЦЕ**:
```python
# После всех RTC (квоты, логгер)
l_spawn_mgr = model.newLayer()
l_spawn_mgr.addAgentFunction(spawn_mgr.getFunction("rtc_spawn_mgr"))

l_spawn_ticket = model.newLayer()
l_spawn_ticket.addAgentFunction(spawn_ticket.getFunction("rtc_spawn_mi17_atomic"))
```

## 7. Ключевые отличия orchestrator_v2 от sim_master

| Аспект | sim_master | orchestrator_v2 |
|--------|-----------|-----------------|
| Режим | spawn_only (ИЗОЛИРОВАННЫЙ) | Интеграция в полный цикл |
| MP5 | ОТКЛЮЧЁН (HL_ENV_MINIMAL=1) | ВКЛЮЧЁН (HostFunction) |
| status_id | ЕСТЬ переменная | НЕТ (используем States) |
| Environment setup | setEnvironmentProperty ПОСЛЕ симуляции | setEnvironmentProperty в base_model ДО симуляции |
| Агент spawn → | "component" (универсальный) | "heli" (со States) |
| PropertyArrays | Устанавливаются через sim.set* | Создаются в base_model.create_model() |

## 8. Проблемы при интеграции spawn в orchestrator_v2

### ❌ Не работает (подтверждено тестами):
1. spawn PropertyArrays в base_model ломают компиляцию (причина неизвестна)
2. Повторная установка Environment триггерит NVRTC Error 425
3. status_id не существует в orchestrator_v2

### ✅ Работает (подтверждено):
- Базовый orchestrator БЕЗ spawn: 3650 дней, MP2 export, квоты
- MacroProperty размера 1 = скаляр, обращение БЕЗ [0]
- agent_out со States (протестировано в standalone)

## 9. Рекомендации для интеграции spawn

### Вариант A: Host-spawn (рекомендуется для первой итерации)
- Создание агентов через `HostFunction` с `HostAgentAPI.newAgent()`
- БЕЗ RTC компиляции → БЕЗ Error 425
- Простая интеграция в существующий пайплайн

### Вариант B: RTC-spawn (требует отладки)
- Нужно решить проблему с PropertyArrays в base_model
- Адаптировать переменные (убрать status_id, добавить новые)
- State для новорожденных: `serviceable` (нейтральное)
- MacroProperty: МАССИВЫ [MAX_DAYS], индекс [safe_day]

## 10. Следующие шаги

1. ✅ **Зафиксировать**: Базовый orchestrator РАБОТАЕТ (тест 3650 дней успешен)
2. **Решить**: Какой вариант spawn выбрать (Host или RTC)
3. **Если RTC**: Выяснить почему spawn PropertyArrays ломают компиляцию
4. **Адаптировать**: Набор переменных под orchestrator_v2 (без status_id)
5. **Протестировать**: standalone spawn → интеграция в orchestrator
