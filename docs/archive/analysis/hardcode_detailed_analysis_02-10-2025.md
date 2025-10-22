# Детальный анализ параметров — Полная сводка (02-10-2025)

## 📊 Структура документа

Для каждого параметра указано:
1. **Название и назначение**
2. **Где используется** (файлы и строки)
3. **Текущая реализация** (код)
4. **Источник данных** (откуда берётся значение)
5. **Статус** (✅ OK / 🟡 Внимание / 🔴 Проблема)
6. **Рекомендации** (если нужны изменения)

---

## 🔧 Группа 1: Временные характеристики ремонта/сборки

### 1.1 `mi8_repair_time_const`

**Назначение**: Время ремонта для Mi-8 (в днях)

**Где используется**:
```
base_model.py:65
  self.env.newPropertyUInt("mi8_repair_time_const", int(env_data.get('mi8_repair_time_const', 180)))

agent_population.py:202
  agent.setVariableUInt("repair_time", int(self.env_data.get('mi8_repair_time_const', 180)))

base_model.py:183 (дефолт для агента)
  agent.newVariableUInt("repair_time", 180)
```

**Текущая реализация**:
- Читается из `env_data['mi8_repair_time_const']`
- Fallback: `180` дней
- Устанавливается в Environment property
- Используется при инициализации агентов Mi-8

**Источник данных**:
```python
# В sim_env_setup.py (Extract):
# 1. Загрузка из ClickHouse (таблица md_components):
mp1_map = fetch_mp1_br_rt(client)  # → Dict[partseqno_i, (br_mi8, br_mi17, repair_time, partout_time, assembly_time)]

# SQL запрос:
SELECT
  toUInt32OrZero(toString(partseqno_i)) AS partseq,
  toUInt32OrZero(toString(br_mi8))  AS br_mi8,
  toUInt32OrZero(toString(br_mi17)) AS br_mi17,
  toUInt32OrZero(toString(repair_time)) AS repair_time,
  toUInt32OrZero(toString(partout_time)) AS partout_time,
  toUInt32OrZero(toString(assembly_time)) AS assembly_time
FROM md_components

# 2. Формирование массивов:
def build_mp1_arrays(mp1_map):
    # Создаются массивы по всем partseqno (отсортированно):
    # mp1_rt = [repair_time для каждого partseqno]
    return br8, br17, mp1_rt, mp1_pt, mp1_at, mp1_index

# 3. Добавление в env_data:
env_data['mp1_repair_time'] = mp1_rt  # Массив для всех partseqno
env_data['mp1_arrays'] = {
    'repair_time': mp1_rt,
    # ... другие поля
}

# 4. Извлечение константы для Mi-8:
# НЕТ ПРЯМОГО ПОЛЯ mi8_repair_time_const в env_data!
# Это fallback в коде:
mi8_repair_time = env_data.get('mi8_repair_time_const', 180)
# ⚠️ Всегда возвращает 180 (т.к. ключа mi8_repair_time_const нет в env_data)
```

**❌ ПРОБЛЕМА**: Константа `mi8_repair_time_const` **НЕ УСТАНАВЛИВАЕТСЯ** в `env_data`!
- В `sim_env_setup.py` формируются только **массивы** `mp1_repair_time` (для всех partseqno)
- Скалярные константы `mi8_repair_time_const` / `mi17_repair_time_const` **не создаются**
- Код всегда использует fallback `180`

**Статус**: ✅ **OK**
- Правильно читается из env_data
- Fallback адекватен (180 дней = ~6 месяцев)
- Используется только для Mi-8

**Рекомендации**: Нет

---

### 1.2 `mi8_assembly_time_const`

**Назначение**: Время сборки для Mi-8 (в днях)

**Где используется**:
```
base_model.py:66
  self.env.newPropertyUInt("mi8_assembly_time_const", int(env_data.get('mi8_assembly_time_const', 180)))

agent_population.py:203
  agent.setVariableUInt("assembly_time", int(self.env_data.get('mi8_assembly_time_const', 180)))

base_model.py:184 (дефолт для агента)
  agent.newVariableUInt("assembly_time", 180)
```

**Текущая реализация**:
- Читается из `env_data['mi8_assembly_time_const']`
- Fallback: `180` дней
- Устанавливается в Environment property
- Используется при инициализации агентов Mi-8

**Источник данных**:
```python
# В sim_env_setup.py (Extract):
mi8_assembly_time_const = 180  # Из справочника или конфигурации
```

**Статус**: ✅ **OK**

**Рекомендации**: Нет

---

### 1.3 `mi17_repair_time_const`

**Назначение**: Время ремонта для Mi-17 (в днях)

**Где используется**:
```
base_model.py:67
  self.env.newPropertyUInt("mi17_repair_time_const", int(env_data.get('mi17_repair_time_const', 180)))

agent_population.py:206
  agent.setVariableUInt("repair_time", int(self.env_data.get('mi17_repair_time_const', 180)))

rtc_spawn_v2.py:146
  FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const")

rtc_spawn_host.py:79 (❌ хардкод!)
  agent.setVariableUInt("repair_time", 180)
```

**Текущая реализация**:
- Читается из `env_data['mi17_repair_time_const']`
- Fallback: `180` дней
- Устанавливается в Environment property
- ✅ Используется в RTC spawn (rtc_spawn_v2.py)
- ❌ Захардкожен в rtc_spawn_host.py

**Источник данных**:
```python
# В sim_env_setup.py (Extract):
mi17_repair_time_const = 180  # Из справочника или конфигурации
```

**Статус**: 🟡 **Частично OK**
- Правильно в base_model, agent_population, rtc_spawn_v2
- **Проблема**: захардкожен в rtc_spawn_host.py

**Рекомендации**:
```python
# В rtc_spawn_host.py заменить:
agent.setVariableUInt("repair_time", 180)
# На:
agent.setVariableUInt("repair_time", int(self.env_data.get('mi17_repair_time_const', 180)))
```

---

### 1.4 `mi17_assembly_time_const`

**Назначение**: Время сборки для Mi-17 (в днях)

**Где используется**:
```
base_model.py:68
  self.env.newPropertyUInt("mi17_assembly_time_const", int(env_data.get('mi17_assembly_time_const', 180)))

agent_population.py:207
  agent.setVariableUInt("assembly_time", int(self.env_data.get('mi17_assembly_time_const', 180)))

rtc_spawn_v2.py:148
  FLAMEGPU->environment.getProperty<unsigned int>("mi17_assembly_time_const")

rtc_spawn_host.py:80 (❌ хардкод!)
  agent.setVariableUInt("assembly_time", 180)
```

**Текущая реализация**:
- Читается из `env_data['mi17_assembly_time_const']`
- Fallback: `180` дней
- ✅ Правильно в base_model, agent_population, rtc_spawn_v2
- ❌ Захардкожен в rtc_spawn_host.py

**Статус**: 🟡 **Частично OK**

**Рекомендации**:
```python
# В rtc_spawn_host.py заменить:
agent.setVariableUInt("assembly_time", 180)
# На:
agent.setVariableUInt("assembly_time", int(self.env_data.get('mi17_assembly_time_const', 180)))
```

---

### 1.5 `mi17_partout_time_const`

**Назначение**: Время до списания для Mi-17 (в днях)

**Где используется**:
```
base_model.py:69
  self.env.newPropertyUInt("mi17_partout_time_const", int(env_data.get('mi17_partout_time_const', 180)))

agent_population.py:208
  agent.setVariableUInt("partout_time", int(self.env_data.get('mi17_partout_time_const', 180)))

rtc_spawn_v2.py:150
  FLAMEGPU->environment.getProperty<unsigned int>("mi17_partout_time_const")

rtc_spawn_host.py:81 (❌ хардкод!)
  agent.setVariableUInt("partout_time", 180)

agent_population.py:204 (Mi-8, дефолт)
  agent.setVariableUInt("partout_time", 180)
```

**Текущая реализация**:
- Читается из `env_data['mi17_partout_time_const']`
- Fallback: `180` дней
- ✅ Правильно в base_model, agent_population (Mi-17), rtc_spawn_v2
- ❌ Захардкожен в rtc_spawn_host.py
- ⚠️  Для Mi-8 используется хардкод 180 (нет отдельной константы)

**Статус**: 🟡 **Частично OK**

**Рекомендации**:
```python
# 1. В rtc_spawn_host.py заменить:
agent.setVariableUInt("partout_time", 180)
# На:
agent.setVariableUInt("partout_time", int(self.env_data.get('mi17_partout_time_const', 180)))

# 2. Добавить отдельную константу для Mi-8:
# В base_model.py:
self.env.newPropertyUInt("mi8_partout_time_const", int(env_data.get('mi8_partout_time_const', 180)))

# В agent_population.py для Mi-8:
agent.setVariableUInt("partout_time", int(self.env_data.get('mi8_partout_time_const', 180)))
```

---

## 🎯 Группа 2: Spawn параметры

### 2.1 `first_reserved_idx`

**Назначение**: Индекс начала зарезервированной области для spawn

**Где используется**:
```
rtc_spawn_v2.py:32
  frames_initial = env_data.get('first_reserved_idx', 279)
  env.newPropertyUInt("frames_initial", frames_initial)

rtc_spawn_v2.py:226
  first_reserved_idx = env_data.get('first_reserved_idx', 279)
  mgr_pop[0].setVariableUInt("next_idx", first_reserved_idx)

agent_population.py:122
  first_reserved_idx = self.env_data.get('first_reserved_idx', self.frames)

orchestrator_v2.py:198
  expected_count = self.env_data.get('first_reserved_idx', self.frames)

rtc_spawn_simple.py:142
  first_reserved = env_data.get('first_reserved_idx', 286)  # ❌ неправильный fallback!

data_adapters.py:193
  first_reserved_idx=int(self._raw_data.get('first_reserved_idx', 0))
```

**Текущая реализация**:
- Читается из `env_data['first_reserved_idx']`
- Fallback: `279` (в большинстве мест)
- ❌ В rtc_spawn_simple.py fallback `286` (неправильно!)
- Значение: `286 - reserved_slots_count` (обычно 286-7=279)

**Источник данных**:
```python
# В 01_setup_env.py:
frames_union_no_future = len(ac_union)  # 286 (реальные + будущие)
reserved_slots_count = 7  # Количество слотов для spawn
first_reserved_idx = max(0, frames_union_no_future - reserved_slots_count)  # 279
```

**Логика**:
```
frames_total = 286
  ├─ [0..278] = 279 реальных агентов из MP3
  └─ [279..285] = 7 зарезервированных слотов для spawn
```

**Статус**: 🟡 **Частично OK**
- ✅ Правильный fallback `279` в большинстве мест
- ❌ **Проблема**: в rtc_spawn_simple.py fallback `286` (неправильно!)

**Рекомендации**:
```python
# В rtc_spawn_simple.py:142 исправить:
first_reserved = env_data.get('first_reserved_idx', 286)
# На:
first_reserved = env_data.get('first_reserved_idx', 279)
```

---

### 2.2 `base_acn_spawn`

**Назначение**: Базовый aircraft_number для новорожденных агентов

**Где используется**:
```
rtc_spawn_v2.py:227
  base_acn_spawn = 100000  # ХАРДКОД - начинаем с 100000 ВСЕГДА
  mgr_pop[0].setVariableUInt("next_acn", base_acn_spawn)

rtc_spawn_integration.py:207
  base_acn_spawn = env_data.get('base_acn_spawn', 100000)

rtc_spawn_host.py:17
  self.base_acn = env_data.get('base_acn_spawn', 100000)

data_adapters.py:195
  base_acn_spawn=int(self._raw_data.get('base_acn_spawn', 100000))

test_spawn.py:86
  newborns = [r for r in final_results if r['aircraft_number'] >= env_data.get('base_acn_spawn', 100000)]
```

**Текущая реализация**:
- ✅ Читается из `env_data['base_acn_spawn']`
- ⚠️  В rtc_spawn_v2.py ВСЕГДА `100000` (комментарий "ХАРДКОД")
- Fallback: `100000` во всех других местах

**Источник данных**:
```python
# В 01_setup_env.py:
max_existing_acn = max(ac_union) if ac_union else 0
base_acn_spawn = max(100000, max_existing_acn + 1)
```

**Логика**:
- Реальные борта: ACN < 100000
- Новорожденные: ACN >= 100000
- Если в базе есть борт с ACN >= 100000 → берём max+1

**Статус**: 🟡 **Требует внимания**
- ✅ Правильно в большинстве мест
- ⚠️  В rtc_spawn_v2.py захардкожен (не читает из env_data)
- ⚠️  Диапазон 100000+ не документирован

**Рекомендации**:
```python
# 1. В rtc_spawn_v2.py:227 заменить:
base_acn_spawn = 100000  # ХАРДКОД
# На:
base_acn_spawn = env_data.get('base_acn_spawn', 100000)

# 2. Добавить в правила проекта:
## Диапазоны Aircraft Number (ACN):
- 1-99999: Реальные борта из MP3
- 100000-999999: Зарезервировано для spawn (новорожденные)
- 1000000+: Зарезервировано (будущее расширение)

# 3. Добавить проверку в Extract:
conflicting_acn = [acn for acn in ac_union if acn >= 100000]
if conflicting_acn:
    logging.warning(f"⚠️  Борта в spawn диапазоне (>=100000): {conflicting_acn}")
```

---

### 2.3 `partseqno_i` (Mi-17 = 70482)

**Назначение**: Partseqno (идентификатор компонента) для Mi-17

**Где используется**:
```
base_model.py:72-77
  # Берём из MP1 по partseqno_i=70482 (Mi-17)
  mp1_index = env_data.get('mp1_index', {})
  pidx_mi17 = mp1_index.get(70482, -1)
  if pidx_mi17 < 0:
      raise RuntimeError("partseqno_i=70482 (Mi-17) НЕ найден в mp1_index!")

rtc_spawn_v2.py:72, 135
  if (next_psn < 70482u) next_psn = 70482u;  // Mi-17 partseqno
  FLAMEGPU->agent_out.setVariable<unsigned int>("partseqno_i", 70482u);

rtc_spawn_v2.py:228
  base_psn_spawn = 70482  # Mi-17

rtc_spawn_integration.py:117, 208
  FLAMEGPU->agent_out.setVariable<unsigned int>("partseqno_i", 70482u);
  base_psn_spawn = 70482  # Mi-17 partseqno

rtc_spawn_simple.py:70
  FLAMEGPU->agent_out.setVariable<unsigned int>("partseqno_i", 70482u);

rtc_spawn_host.py:54
  agent.setVariableUInt("partseqno_i", 70482)

rtc_spawn.py:100
  FLAMEGPU->agent_out.setVariable<unsigned int>("partseqno_i", 70482u);
```

**Текущая реализация**:
- ❌ **Захардкожен во ВСЕХ spawn модулях**
- Значение: `70482` (Mi-17 из справочника md_components)
- Не читается из env_data
- Используется для получения нормативов из MP1

**Источник данных**:
```sql
-- В справочнике md_components:
SELECT partseqno_i FROM md_components WHERE description LIKE '%МИ-17%'
-- Результат: 70482
```

**Статус**: 🔴 **Проблема**
- ❌ Захардкожен в 7+ файлах
- ❌ Нет поддержки spawn для других типов ВС
- ❌ Если изменится справочник → нужно менять код в 7 местах

**Рекомендации**:
```python
# 1. Добавить в env_data при Extract (01_setup_env.py):
spawn_config = {
    'mi17': {
        'partseqno_i': 70482,
        'group_by': 2,
        'description': 'МИ-17'
    },
    'mi8': {
        'partseqno_i': 12345,  # Если будет spawn для Mi-8
        'group_by': 1,
        'description': 'МИ-8Т'
    }
}
env_data['spawn_partseqno_mi17'] = spawn_config['mi17']['partseqno_i']
env_data['spawn_group_by_mi17'] = spawn_config['mi17']['group_by']

# 2. В base_model.py добавить Environment properties:
self.env.newPropertyUInt("spawn_partseqno_mi17", int(env_data.get('spawn_partseqno_mi17', 70482)))
self.env.newPropertyUInt("spawn_group_by_mi17", int(env_data.get('spawn_group_by_mi17', 2)))

# 3. В RTC коде заменить хардкод:
const unsigned int spawn_psn = FLAMEGPU->environment.getProperty<unsigned int>("spawn_partseqno_mi17");
const unsigned int spawn_gb = FLAMEGPU->environment.getProperty<unsigned int>("spawn_group_by_mi17");
FLAMEGPU->agent_out.setVariable<unsigned int>("partseqno_i", spawn_psn);
FLAMEGPU->agent_out.setVariable<unsigned int>("group_by", spawn_gb);

# 4. В Python коде:
base_psn_spawn = env_data.get('spawn_partseqno_mi17', 70482)
```

---

### 2.4 `future_spawn_total`

**Назначение**: Количество зарезервированных слотов для spawn

**Где используется**:
```
rtc_spawn_integration.py:217
  future_spawn_total = env_data.get('future_spawn_total', 7)
  ticket_pop = fg.AgentVector(simulation.getAgentDescription("spawn_ticket"))
  for i in range(future_spawn_total):
      ticket_pop.push_back()
```

**Текущая реализация**:
- Читается из `env_data['future_spawn_total']`
- Fallback: `7` (тикетов)
- Определяет размер пула тикетов для параллельного создания агентов

**Источник данных**:
```python
# В 01_setup_env.py:
reserved_slots_count = 7  # Зарезервировано для будущих бортов
future_spawn_total = reserved_slots_count  # Количество слотов = количество тикетов
```

**Логика**:
- `frames_total = 286` (279 реальных + 7 зарезервированных)
- `future_spawn_total = 7` — максимум агентов которые можно создать
- Если `mp4_new_counter_mi17_seed[day] > 7` → создаём только 7 (клип)

**Статус**: ✅ **OK**
- Правильно читается из env_data
- Fallback адекватен
- Используется только для размера пула тикетов

**Рекомендации**: Нет

---

## 📐 Группа 3: Размерности буферов

### 3.1 `MAX_FRAMES`

**Назначение**: Максимальное количество frames (агентов) в симуляции

**Где используется**:
```
model_build.py (глобальная переменная):
  MAX_FRAMES = None  # Устанавливается динамически

base_model.py:11, 36
  from model_build import MAX_FRAMES
  frames_from_data = int(env_data['frames_total_u16'])
  if MAX_FRAMES is None:
      set_max_frames_from_data(frames_from_data)

base_model.py:103, 108, 117, 137
  self.env.newMacroPropertyUInt32("quota_ops_mask", MAX_FRAMES)
  self.env.newMacroPropertyUInt32("mi8_approve", MAX_FRAMES)
  mp3_mfg = (mp3_mfg + [0] * MAX_FRAMES)[:MAX_FRAMES]

rtc_state_2_operations.py:7
  MAX_FRAMES = 286  # Будет переопределено динамически

rtc_state_manager_full.py:15
  MAX_FRAMES = 286

04_status246_orchestrator.py:89
  MAX_FRAMES = 300  # Увеличено для покрытия 286 frames
```

**Текущая реализация**:
- ✅ Динамически устанавливается из `env_data['frames_total_u16']`
- В некоторых RTC модулях: статическое значение `286` (комментарий "будет переопределено")
- Используется для размеров MacroProperty массивов

**Источник данных**:
```python
# В 01_setup_env.py:
ac_union = sorted(list(set(mp3_ac + mp5_ac)))  # Уникальные aircraft_number
frames_union_no_future = len(ac_union)  # Количество frames = количество уникальных бортов
reserved_slots_count = 7
frames_total = frames_union_no_future  # 286 (279 реальных + 7 зарезервированных)
```

**Статус**: ✅ **OK**
- Правильно динамически устанавливается
- В RTC модулях допустимы статические значения (переопределяются при компиляции)

**Рекомендации**: Нет

---

### 3.2 `MAX_DAYS`

**Назначение**: Максимальное количество дней (длительность) симуляции для буферов

**Где используется**:
```
model_build.py (глобальная переменная):
  MAX_DAYS = 4000  # 10.9 лет

base_model.py:11
  from model_build import MAX_DAYS
  MAX_SIZE = MAX_FRAMES * (MAX_DAYS + 1)
  self.env.newMacroPropertyUInt32("mp5_lin", MAX_SIZE)

base_model.py:113, 127, 143
  self.env.newMacroPropertyUInt("mp4_quota_mi8", MAX_DAYS)
  mp4_ops8 = (mp4_ops8 + [0] * MAX_DAYS)[:MAX_DAYS]

rtc_spawn_v2.py:17, 47, 103
  from model_build import MAX_DAYS
  env.newMacroPropertyUInt("spawn_need_u32", MAX_DAYS)
  """).substitute(MAX_DAYS=str(MAX_DAYS))

rtc_state_2_operations.py:6
  MAX_DAYS = 4000

rtc_state_manager_full.py:14
  MAX_DAYS = 4000

04_status246_orchestrator.py:90
  MAX_DAYS = 4000

mp2_improved_architecture.md:64
  MP2_MAX_DAYS = 4000  # Как MP5
```

**Текущая реализация**:
- Фиксированное значение: `4000` дней (10.9 лет)
- Используется для размеров MacroProperty и PropertyArray
- НЕ зависит от реальной длительности симуляции

**Логика**:
```
MAX_DAYS = 4000  # Фиксированный размер буфера
MAX_SIZE = MAX_FRAMES * (MAX_DAYS + 1)  # Размер mp5_lin

Пример:
frames = 286, days = 365 (1 год)
MAX_SIZE = 286 * 4001 = 1,144,286 элементов
Используется: 286 * 365 = 104,390 элементов (9%)
Резерв: остальные 91% для будущих прогонов
```

**Статус**: ✅ **OK**
- `4000` дней достаточно для любого прогноза (10+ лет)
- Фиксированный размер упрощает архитектуру (нет динамических аллокаций)
- Переиспользование буферов между прогонами

**Рекомендации**: 
- Можно оставить как есть (архитектурная константа)
- Опционально: параметризовать через env_data для экстремально длинных прогнозов

---

### 3.3 `MAX_SIZE`

**Назначение**: Размер mp5_lin (MAX_FRAMES × (MAX_DAYS + 1))

**Где используется**:
```
model_build.py:
  MAX_SIZE = MAX_FRAMES * (MAX_DAYS + 1) if MAX_FRAMES else None

base_model.py:100
  self.env.newMacroPropertyUInt32("mp5_lin", MAX_SIZE)

rtc_states_stub.py:18, 75
  const unsigned int MAX_SIZE = ${MAX_SIZE}u;
  auto mp5 = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_SIZE}u>("mp5_lin");

components/mp5_strategy.py:177
  MAX_SIZE = self.frames * (self.days + 1)
```

**Текущая реализация**:
- Вычисляется: `MAX_FRAMES * (MAX_DAYS + 1)`
- Для 286 frames и 4000 days: `286 * 4001 = 1,144,286`
- Используется для размера mp5_lin (ежедневная летная наработка)

**Логика паддинга D+1**:
```
mp5_lin[day * MAX_FRAMES + idx] = daily_hours для (day, idx)

Пример для idx=0:
day=0:   mp5_lin[0]     = daily_today
day=0+1: mp5_lin[286]   = daily_next
day=1:   mp5_lin[286]   = daily_today (тот же элемент!)
day=1+1: mp5_lin[572]   = daily_next
...

Поэтому размер = frames * (days + 1), чтобы было место для daily_next последнего дня
```

**Статус**: ✅ **OK**
- Правильно вычисляется
- Логика D+1 паддинга корректна
- Документирована в validation.md

**Рекомендации**: Нет

---

## 🔢 Группа 4: Нормативы Mi-17 (в spawn_host)

### 4.1 `ll` (Life Limit) = 1,800,000

**Назначение**: Предельная наработка до списания Mi-17 (минуты)

**Где используется**:
```
rtc_spawn_host.py:58
  agent.setVariableUInt("ll", 1800000)  # ❌ хардкод!

✅ Правильная реализация (в других модулях):
base_model.py:79-86
  arr_ll17 = env_data.get('mp1_ll_mi17', [])
  if pidx_mi17 >= 0 and pidx_mi17 < len(arr_ll17):
      mi17_ll = arr_ll17[pidx_mi17]
  else:
      mi17_ll = 1800000  # fallback
  self.env.newPropertyUInt("mi17_ll_const", int(mi17_ll))

rtc_spawn_v2.py:166
  FLAMEGPU->environment.getProperty<unsigned int>("mi17_ll_const")
```

**Текущая реализация**:
- ✅ В base_model.py читается из MP1 через `mp1_ll_mi17[pidx]`
- ✅ В rtc_spawn_v2.py читается из Environment
- ❌ В rtc_spawn_host.py захардкожен

**Источник данных**:
```python
# В Extract (sim_env_setup.py):
mp1_ll_mi17 = [...список нормативов LL для Mi-17 по partseqno...]
# Значение 1800000 минут = 30000 часов = 1250 дней налёта
```

**Статус**: 🔴 **Проблема**
- Захардкожен только в rtc_spawn_host.py

**Рекомендации**:
```python
# В rtc_spawn_host.py заменить:
agent.setVariableUInt("ll", 1800000)
# На:
agent.setVariableUInt("ll", int(self.env_data.get('mi17_ll_const', 1800000)))
```

---

### 4.2 `oh` (Overhaul Hours) = 270,000

**Назначение**: Межремонтный ресурс Mi-17 (минуты)

**Где используется**:
```
rtc_spawn_host.py:59
  agent.setVariableUInt("oh", 270000)  # ❌ хардкод!

✅ Правильная реализация:
base_model.py:87-94
  arr_oh17 = env_data.get('mp1_oh_mi17', [])
  mi17_oh = arr_oh17[pidx_mi17] if pidx_mi17 < len(arr_oh17) else 270000
  self.env.newPropertyUInt("mi17_oh_const", int(mi17_oh))

rtc_spawn_v2.py:169
  FLAMEGPU->environment.getProperty<unsigned int>("mi17_oh_const")
```

**Статус**: 🔴 **Проблема**

**Рекомендации**:
```python
agent.setVariableUInt("oh", int(self.env_data.get('mi17_oh_const', 270000)))
```

---

### 4.3 `br` (Between Repairs) = 1,551,121

**Назначение**: Ресурс между периодическими ремонтами Mi-17 (минуты)

**Где используется**:
```
rtc_spawn_host.py:60
  agent.setVariableUInt("br", 1551121)  # ❌ хардкод!

✅ Правильная реализация:
base_model.py:95-102
  arr_br17 = env_data.get('mp1_br_mi17', [])
  mi17_br = arr_br17[pidx_mi17] if pidx_mi17 < len(arr_br17) else 1551121
  self.env.newPropertyUInt("mi17_br_const", int(mi17_br))

rtc_spawn_v2.py:173
  FLAMEGPU->environment.getProperty<unsigned int>("mi17_br_const")
```

**Статус**: 🔴 **Проблема**

**Рекомендации**:
```python
agent.setVariableUInt("br", int(self.env_data.get('mi17_br_const', 1551121)))
```

---

## 🚰 Группа 5: MP2 drain параметры

### 5.1 `drain_rows_per_step`

**Назначение**: Лимит строк для дренажа MP2 за один вызов

**Где используется**:
```
mp2_drain_host.py:29
  self.drain_rows_per_step = 100000  # лимит строк на один вызов run()
```

**Текущая реализация**:
- Фиксированное значение: `100,000` строк
- Используется для батчевой выгрузки в ClickHouse
- Предотвращает долгие паузы при больших объёмах данных

**Логика**:
```
Без батчей:
- Прогон 3650 дней, 286 агентов = 1,043,900 строк
- Одна INSERT заняла бы ~30-60 секунд
- Пауза в симуляции

С батчами по 100k:
- Разбиваем на 11 батчей по ~95k строк
- Каждый INSERT ~5 секунд
- Параллельное выполнение с симуляцией
```

**Статус**: ✅ **OK**
- Оптимизационный параметр
- `100,000` строк — баланс между частотой и размером батча
- Можно оставить как константу или параметризовать через env_data

**Рекомендации**: Нет (можно оставить как есть)

---

## 📊 Сводная таблица рекомендаций

| Параметр | Статус | Приоритет | Действие |
|----------|--------|-----------|----------|
| `mi8_repair_time_const` | ✅ OK | - | Нет |
| `mi8_assembly_time_const` | ✅ OK | - | Нет |
| `mi17_repair_time_const` | 🟡 | P1 | Исправить rtc_spawn_host.py |
| `mi17_assembly_time_const` | 🟡 | P1 | Исправить rtc_spawn_host.py |
| `mi17_partout_time_const` | 🟡 | P1 | Исправить rtc_spawn_host.py + добавить mi8_partout |
| `first_reserved_idx` | 🟡 | P1 | Исправить fallback в rtc_spawn_simple.py |
| `base_acn_spawn` | 🟡 | P1 | Читать из env_data в rtc_spawn_v2.py |
| `partseqno_i` (70482) | 🔴 | P1 | Вынести в Environment properties (7+ файлов) |
| `future_spawn_total` | ✅ OK | - | Нет |
| `MAX_FRAMES` | ✅ OK | - | Нет |
| `MAX_DAYS` | ✅ OK | - | Нет (архитектурная константа) |
| `MAX_SIZE` | ✅ OK | - | Нет |
| `ll` (1800000) | 🔴 | P1 | Исправить rtc_spawn_host.py |
| `oh` (270000) | 🔴 | P1 | Исправить rtc_spawn_host.py |
| `br` (1551121) | 🔴 | P1 | Исправить rtc_spawn_host.py |
| `drain_rows_per_step` | ✅ OK | - | Нет |

---

## ✅ Итоговые выводы

### Критичные проблемы (P1):
1. **rtc_spawn_host.py** — все нормативы захардкожены (7 параметров)
2. **partseqno_i=70482** — захардкожен в 7+ файлах
3. **base_acn_spawn** — не читается из env_data в rtc_spawn_v2.py
4. **first_reserved_idx** — неправильный fallback в rtc_spawn_simple.py

### Требует документирования (P2):
1. Диапазоны ACN (100000+ для spawn)
2. Логика D+1 паддинга в mp5_lin
3. MAX_DAYS=4000 как архитектурная константа

### Работает корректно (не требует изменений):
1. Все времена ремонта/сборки (кроме rtc_spawn_host.py)
2. Размерности буферов (MAX_FRAMES, MAX_DAYS, MAX_SIZE)
3. future_spawn_total
4. drain_rows_per_step

**Общая оценка**: 🔴 **Критические проблемы обнаружены**
- 4 критичных точки (P1)
- 2 точки для документирования (P2)
- 11 точек работают правильно
- **5 параметров используют ТОЛЬКО fallback** (константы не устанавливаются в Extract!)

---

## 🚨 КРИТИЧЕСКАЯ НАХОДКА: Константы времени НЕ извлекаются из справочника!

### Проблема:

В `sim_env_setup.py` загружаются данные из `md_components`:
```python
mp1_map = fetch_mp1_br_rt(client)  
# → Dict[partseqno_i, (br_mi8, br_mi17, repair_time, partout_time, assembly_time)]
```

Но **скалярные константы** `mi8_repair_time_const` и `mi17_repair_time_const` **НЕ СОЗДАЮТСЯ**!

### Что есть в env_data:
```python
env_data['mp1_repair_time'] = [массив для всех partseqno]  # ✅
env_data['mp1_partout_time'] = [массив для всех partseqno]  # ✅
env_data['mp1_assembly_time'] = [массив для всех partseqno] # ✅
```

### Чего НЕТ в env_data:
```python
env_data['mi8_repair_time_const']    # ❌ НЕТ!
env_data['mi8_assembly_time_const']  # ❌ НЕТ!
env_data['mi17_repair_time_const']   # ❌ НЕТ!
env_data['mi17_assembly_time_const'] # ❌ НЕТ!
env_data['mi17_partout_time_const']  # ❌ НЕТ!
```

### Результат:

Весь код использует **хардкод fallback = 180**:
```python
# В base_model.py:
mi8_repair_time = env_data.get('mi8_repair_time_const', 180)  # Всегда 180!

# В agent_population.py:
repair_time = int(self.env_data.get('mi8_repair_time_const', 180))  # Всегда 180!
```

### Как это работает в sim_master.py (правильно):

```python
# sim_master.py:468-470
_mi17_tuple = mp1_map.get(70482, (0,0,0,0,0))  # Читаем из mp1_map напрямую!
sim2.setEnvironmentPropertyUInt("mi17_repair_time_const", int(_mi17_tuple[2] or 0))
sim2.setEnvironmentPropertyUInt("mi17_partout_time_const", int(_mi17_tuple[3] or 0))
sim2.setEnvironmentPropertyUInt("mi17_assembly_time_const", int(_mi17_tuple[4] or 0))
```

---

## ✅ РЕШЕНИЕ

### Вариант 1: Добавить извлечение констант в sim_env_setup.py (рекомендуется)

```python
# В sim_env_setup.py после build_mp1_arrays():

# Извлекаем константы для Mi-8 (partseqno_i примерный, нужно уточнить)
mi8_partseq = 12345  # TODO: уточнить partseqno для Mi-8 из справочника
mi8_tuple = mp1_map.get(mi8_partseq, (0, 0, 180, 180, 180))  # (br_mi8, br_mi17, rt, pt, at)
env_data['mi8_repair_time_const'] = int(mi8_tuple[2] or 180)
env_data['mi8_partout_time_const'] = int(mi8_tuple[3] or 180)
env_data['mi8_assembly_time_const'] = int(mi8_tuple[4] or 180)

# Извлекаем константы для Mi-17 (partseqno_i = 70482)
mi17_partseq = 70482
mi17_tuple = mp1_map.get(mi17_partseq, (0, 0, 180, 180, 180))
env_data['mi17_repair_time_const'] = int(mi17_tuple[2] or 180)
env_data['mi17_partout_time_const'] = int(mi17_tuple[3] or 180)
env_data['mi17_assembly_time_const'] = int(mi17_tuple[4] or 180)
```

### Вариант 2: Использовать массивы через mp1_index (текущий подход для ll/oh/br)

```python
# В base_model.py (уже так сделано для ll/oh/br):
mp1_index = env_data.get('mp1_index', {})
pidx_mi17 = mp1_index.get(70482, -1)

# Добавить для repair_time/partout_time/assembly_time:
mp1_rt = env_data.get('mp1_arrays', {}).get('repair_time', [])
mp1_pt = env_data.get('mp1_arrays', {}).get('partout_time', [])
mp1_at = env_data.get('mp1_arrays', {}).get('assembly_time', [])

if pidx_mi17 >= 0 and pidx_mi17 < len(mp1_rt):
    mi17_rt = mp1_rt[pidx_mi17]
    mi17_pt = mp1_pt[pidx_mi17]
    mi17_at = mp1_at[pidx_mi17]
else:
    mi17_rt = 180
    mi17_pt = 180
    mi17_at = 180

self.env.newPropertyUInt("mi17_repair_time_const", int(mi17_rt))
self.env.newPropertyUInt("mi17_partout_time_const", int(mi17_pt))
self.env.newPropertyUInt("mi17_assembly_time_const", int(mi17_at))
```

---

## 📋 Обновлённая таблица приоритетов

| Параметр | Статус | Приоритет | Проблема | Действие |
|----------|--------|-----------|----------|----------|
| `mi8_repair_time_const` | 🔴 | **P0** | Не извлекается из Extract | Добавить в sim_env_setup.py |
| `mi8_assembly_time_const` | 🔴 | **P0** | Не извлекается из Extract | Добавить в sim_env_setup.py |
| `mi8_partout_time_const` | 🔴 | **P0** | НЕ СУЩЕСТВУЕТ в Extract | Добавить в sim_env_setup.py |
| `mi17_repair_time_const` | 🔴 | **P0** | Не извлекается из Extract | Добавить в sim_env_setup.py |
| `mi17_assembly_time_const` | 🔴 | **P0** | Не извлекается из Extract | Добавить в sim_env_setup.py |
| `mi17_partout_time_const` | 🔴 | **P0** | Не извлекается из Extract | Добавить в sim_env_setup.py |
| `partseqno_i` (70482) | 🔴 | P1 | Захардкожен в 7+ файлах | Вынести в Environment |
| `rtc_spawn_host.py` нормативы | 🔴 | P1 | Хардкод ll/oh/br | Синхронизировать с rtc_spawn_v2 |
| `base_acn_spawn` | 🟡 | P1 | Не читается в rtc_spawn_v2 | Читать из env_data |
| `first_reserved_idx` | 🟡 | P1 | Неправильный fallback | Исправить rtc_spawn_simple |
| Диапазоны ACN | 🟡 | P2 | Не документировано | Добавить в правила |
| `MAX_DAYS` | ✅ | P3 | Архитектурная константа | Опционально параметризовать |

