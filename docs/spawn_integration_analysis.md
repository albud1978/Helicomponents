# Анализ интеграции спавна Mode B в Mode A

## Критическая архитектура: frames_total = |MP3 ∪ MP5|

### Ключевой принцип
- **MP5 уже содержит записи для будущих вертолетов** (которые будут рождены через спавн)
- **frames_total** должен быть равен количеству уникальных aircraft_number в объединении MP3 и MP5
- **frames_initial = |MP3|** - только существующие агенты на старте
- **Спавн только создает агентов** с правильными переменными, их места в MP5 уже зарезервированы

### Последовательность индексации
1. Индексы 0..frames_initial-1: существующие агенты из MP3
2. Индексы frames_initial..frames_total-1: будущие агенты из MP5 (для спавна)

### Размерности массивов
- **MP2 (MacroProperty)**: размер = frames_total × days_total (включает будущих)
- **MP5 daily_hours**: размер = (days_total + 1) × frames_total
- **Популяция агентов**: начинается с frames_initial, растет до frames_total

## Сравнение Environment переменных

### Общие переменные (используются в обоих режимах)
| Переменная | Тип | Размер | Описание |
|------------|-----|--------|----------|
| version_date | UInt | scalar | Дата версии |
| frames_total | UInt | scalar | Общее количество кадров |
| days_total | UInt | scalar | Общее количество дней |
| mp4_ops_counter_mi8 | Array[UInt32] | days_total | Квоты операций МИ-8 |
| mp4_ops_counter_mi17 | Array[UInt32] | days_total | Квоты операций МИ-17 |

### Переменные только в Mode A
| Переменная | Тип | Размер | Описание |
|------------|-----|--------|----------|
| mp5_daily_hours | Array[UInt16] | (days+1)*frames | Часы налета |
| mp1_br_mi8/mi17 | Array[UInt32] | mp1_len | Beyond Repair значения |
| mp1_repair_time | Array[UInt32] | mp1_len | Время ремонта |
| mp3_* arrays | Array[UInt32] | mp3_count | Данные MP3 (SoA) |
| approve_policy | UInt | scalar | Политика одобрения квот |

### Переменные только в Mode B (spawn)
| Переменная | Тип | Размер | Описание |
|------------|-----|--------|----------|
| frames_initial | UInt | scalar | Начальное количество агентов |
| mp4_new_counter_mi17_seed | Array[UInt32] | days_total | План поставок новых МИ-17 |
| month_first_u32 | Array[UInt32] | days_total | Первый день месяца для mfg_date |
| spawn_need_u32 | MacroProperty[UInt] | days_total | Потребность в спавне по дням |
| spawn_base_idx_u32 | MacroProperty[UInt] | days_total | Базовый индекс для спавна |
| spawn_base_acn_u32 | MacroProperty[UInt] | days_total | Базовый aircraft_number |
| spawn_base_psn_u32 | MacroProperty[UInt] | days_total | Базовый PSN |
| next_idx_spawn | MacroProperty[UInt] | 1 | Следующий индекс для спавна |
| next_aircraft_no_mi17 | MacroProperty[UInt] | 1 | Следующий номер борта |
| next_psn_mi17 | MacroProperty[UInt] | 1 | Следующий PSN |

### MacroProperty для квот (используются в обоих)
| Переменная | Тип | Размер | Описание |
|------------|-----|--------|----------|
| mi8_intent | MacroProperty[UInt32] | frames_total | Намерения МИ-8 |
| mi17_intent | MacroProperty[UInt32] | frames_total | Намерения МИ-17 |
| mi8_approve | MacroProperty[UInt32] | frames_total | Одобрения МИ-8 |
| mi17_approve | MacroProperty[UInt32] | frames_total | Одобрения МИ-17 |

## Ключевые различия в настройке

### Mode A (status12456-smoke-real)
```python
# Полный набор Environment из БД
sim.setEnvironmentPropertyUInt16("mp5_daily_hours", list(env_data['mp5_daily_hours_linear']))
sim.setEnvironmentPropertyArrayUInt32("mp1_br_mi8", list(env_data['mp1_br_mi8']))
sim.setEnvironmentPropertyArrayUInt32("mp1_br_mi17", list(env_data['mp1_br_mi17']))
# ... и так далее все MP1, MP3 массивы
```

### Mode B (spawn-smoke-real)
```python
# Минимальный набор + специфичные для спавна
os.environ['HL_ENV_MINIMAL'] = '1'  # Минимизирует окружение
sim.setEnvironmentPropertyUInt("frames_initial", int(env_data.get('mp3_count', 0)))
sim.setEnvironmentPropertyArrayUInt32("mp4_new_counter_mi17_seed", _seed.tolist())
sim.setEnvironmentPropertyArrayUInt32("month_first_u32", _m1.tolist())
```

## Конфликты и решения

### 1. Флаг HL_ENV_MINIMAL
- **Конфликт**: Mode B устанавливает `HL_ENV_MINIMAL=1`, что отключает MP1/MP5 в model_build
- **Решение**: В Mode A с интегрированным спавном НЕ устанавливать этот флаг

### 2. Размер MacroProperty для спавна
- **Конфликт**: В Mode B spawn MacroProperty имеют размер `days_total`, в документации для Mode A рекомендуются скалярные
- **Решение**: Использовать размер согласно GPUarc.md - скалярные MacroProperty для Mode A

### 3. Агенты spawn_ticket и spawn_mgr
- **Mode B**: Создает только 16 spawn_ticket и 1 spawn_mgr
- **Mode A**: Должен создавать полную популяцию component + spawn агенты

### 4. RTC слои
- **Mode B**: Использует `HL_RTC_MODE='spawn_only'`
- **Mode A**: Должен добавлять spawn слои ПОСЛЕ rtc_log_day

## План интеграции

### Этап 1: Подготовка model_build.py
В функции `build_model()` класса `HeliSimModel`:

1. **После объявления основных Environment** (строка ~100) добавить блок для спавна:
```python
# Spawn-specific environment (только если включен)
enable_spawn = os.environ.get('HL_ENABLE_SPAWN', '0') == '1'
if enable_spawn:
    # frames_initial уже объявлен на строке 49
    # mp4_new_counter_mi17_seed уже объявлен на строке 64
    # month_first_u32 уже объявлен на строке 76
    # spawn MacroProperty уже объявлены на строках 67-74
    # Агенты spawn_ticket и spawn_mgr уже созданы на строках 290-352
    pass  # Все уже есть!
```

2. **Добавить слои спавна ПОСЛЕ rtc_log_day** (после строки 1146):
```python
# Spawn layers (должны быть последними!)
if enable_spawn:
    spawn_mgr_fn = model.getAgent("spawn_mgr").getFunction("rtc_spawn_mgr")
    spawn_fn = model.getAgent("spawn_ticket").getFunction("rtc_spawn_mi17_atomic")
    l_spawn_mgr = model.newLayer()
    l_spawn_mgr.addAgentFunction(spawn_mgr_fn)
    l_spawn = model.newLayer()
    l_spawn.addAgentFunction(spawn_fn)
```

### Этап 2: Модификация sim_master.py (Mode A)
В блоке `if getattr(a, 'status12456_smoke_real', False):` (строка ~1170):

1. **Установить флаг спавна** (после строки 1175):
```python
# Включаем спавн для Mode A
os.environ.setdefault('HL_ENABLE_SPAWN', '1')
```

2. **После создания основной популяции** (после строки ~1300):
```python
# Создаем популяции спавна
if os.environ.get('HL_ENABLE_SPAWN', '0') == '1':
    spawn_ticket_desc = model2.model.getAgent("spawn_ticket")
    spawn_tickets = pyflamegpu.AgentVector(spawn_ticket_desc, 64)  # HL_SPAWN_MAX_PER_DAY
    for i in range(64):
        spawn_tickets[i].setVariableUInt("ticket", i)
    sim2.setPopulationData(spawn_tickets)
    
    spawn_mgr_desc = model2.model.getAgent("spawn_mgr")
    spawn_mgr = pyflamegpu.AgentVector(spawn_mgr_desc, 1)
    spawn_mgr[0].setVariableUInt("next_idx", int(env_data.get('mp3_count', 0)))
    spawn_mgr[0].setVariableUInt("next_acn", 100000)
    spawn_mgr[0].setVariableUInt("next_psn", 2000000)
    sim2.setPopulationData(spawn_mgr)
```

### Этап 3: Изменение sim_env_setup.py

#### 3.1 Модификация build_frames_index() для учета MP5
Текущая функция использует только MP3, нужно добавить MP5:

```python
def build_frames_index(mp3_rows, mp3_fields: List[str], mp5_by_day: Dict[date, Dict[int, int]] = None) -> Tuple[Dict[int, int], int]:
    idx = {name: i for i, name in enumerate(mp3_fields)}
    ac_set = set()
    
    # Существующая логика для MP3
    for r in mp3_rows:
        ac = int(r[idx['aircraft_number']] or 0)
        if ac <= 0:
            continue
        is_plane = False
        if 'group_by' in idx:
            gb = int(r[idx['group_by']] or 0)
            is_plane = gb in (1, 2)
        elif 'ac_type_mask' in idx:
            m = int(r[idx['ac_type_mask']] or 0)
            is_plane = (m & (32 | 64)) != 0
        else:
            is_plane = False
        if is_plane:
            ac_set.add(ac)
    
    # НОВОЕ: Добавляем aircraft_number из MP5
    if mp5_by_day:
        for day_data in mp5_by_day.values():
            for ac in day_data.keys():
                if ac > 0:  # Исключаем 0
                    ac_set.add(ac)
    
    # Сортировка для детерминированной индексации
    ac_list = sorted([ac for ac in ac_set if ac > 0])
    frames_index = {ac: i for i, ac in enumerate(ac_list)}
    return frames_index, len(ac_list)
```

#### 3.2 Обновление вызова в prepare_env_arrays()
В строке 246:
```python
# Было:
frames_index, frames_total = build_frames_index(mp3_rows, mp3_fields)

# Стало:
frames_index, frames_total = build_frames_index(mp3_rows, mp3_fields, mp5_by_day)
```

#### 3.3 Добавление spawn Environment в apply_env_to_sim()
После строки 328:
```python
# Spawn-specific environment
if 'mp4_new_counter_mi17_seed' in env_data:
    sim.setEnvironmentPropertyArrayUInt32("mp4_new_counter_mi17_seed", 
                                         list(env_data['mp4_new_counter_mi17_seed']))
if 'month_first_u32' in env_data:
    sim.setEnvironmentPropertyArrayUInt32("month_first_u32", 
                                         list(env_data['month_first_u32']))
if 'frames_initial' in env_data:
    sim.setEnvironmentPropertyUInt("frames_initial", 
                                  int(env_data.get('mp3_count', 0)))
```

### Этап 4: Тестирование
1. **Базовый тест** - Mode A без спавна:
   ```bash
   HL_ENABLE_SPAWN=0 python3 sim_master.py --status12456-smoke-real --status12456-days 30
   ```

2. **Тест с интегрированным спавном**:
   ```bash
   HL_ENABLE_SPAWN=1 python3 sim_master.py --status12456-smoke-real --status12456-days 30
   ```

3. **Сравнение с Mode B**:
   ```bash
   python3 sim_master.py --spawn-smoke-real --spawn-days 30
   ```

## Критические моменты
1. **Порядок объявления**: Все Environment должны быть объявлены в model_build ДО первого setEnvironmentProperty
2. **Изоляция RTC**: Spawn RTC должны быть последними в цепочке слоев
3. **Индексация**: frames_initial должен соответствовать |MP3|, новые агенты получают idx >= frames_initial
4. **frames_total = |MP3 ∪ MP5|**: Критически важно, что frames_total включает ВСЕ уникальные aircraft_number из обеих таблиц

## Последовательность реализации

### Фаза 1: Подготовка данных (sim_env_setup.py)
1. Модифицировать `build_frames_index()` для учета MP5
2. Обновить вызов с передачей mp5_by_day
3. Убедиться, что frames_initial корректно установлен

### Фаза 2: Модель (model_build.py)
1. Добавить условную компиляцию spawn слоев по флагу HL_ENABLE_SPAWN
2. Разместить spawn слои после rtc_log_day

### Фаза 3: Симуляция (sim_master.py)
1. Установить HL_ENABLE_SPAWN=1 для Mode A
2. Создать популяции spawn_ticket и spawn_mgr
3. Инициализировать с правильными начальными значениями

### Фаза 4: Валидация
1. Проверить, что frames_total включает всех будущих агентов
2. Убедиться, что spawn создает агентов с idx >= frames_initial
3. Валидировать, что новые агенты корректно участвуют в симуляции с D+1
