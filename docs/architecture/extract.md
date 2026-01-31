# Extract - Извлечение данных

## 📥 Обзор процесса Extract

Этап Extract отвечает за извлечение данных из Excel файлов, первичную загрузку в ClickHouse и обогащение данных.

## 📊 Итоги последнего тестового Extract (21-11-2025)

| Таблица | Назначение | Кол-во записей (version 2025-07-04 v1) |
|---------|------------|----------------------------------------|
| `md_components` | Мастер-данные агрегатов (MP1) | **64** |
| `status_overhaul` | Статусы и капремонт | **46** |
| `program_ac` | Реестр эксплуатируемых ВС | **189** |
| `heli_pandas` | Центральная таблица компонентов (MP3) | **10 736** |
| `flight_program_ac` | Тензор операций (4000 дней) | **4 000** |
| `flight_program_fl` | Нормативные налёты (4000×279) | **1 140 000** |
| `dict_aircraft_number_flat` | Справочник планеров | **279** |
| `dict_digital_values_flat` | Мета-словарь Extract | **152** |

> Сумма первых четырёх таблиц = **11 035** — это итог из логов `extract_master`.  
> Из них **10 736** строк приходится непосредственно на `heli_pandas`. Детальная разбивка по `partno` сохраняется в `output/heli_pandas_partno_stats_2025-07-04.md` (см. `heli_pandas_partno_stats.py`, параметр `--md-path`).
> Аналитику по летящим бортам (`status_id=2`) и установленным агрегатам см. в `output/heli_pandas_ops_inventory_YYYY-MM-DD.md`.

## 🔍 Проверка комплектности вертолётов

После загрузки датасета рекомендуется проверить комплектность агрегатов на планерах в эксплуатации.

### Основная проверка (какие вертолёты с некомплектом)

```bash
python3 code/heli_pandas_ops_other_groups.py --version-date YYYY-MM-DD --version-id 1 --skip-pdf
```

**Выход:** `output/heli_pandas_ops_other_groups_YYYY-MM-DD.md`

**Ключевые колонки:**
| Колонка | Описание |
|---------|----------|
| `inst` | Установлено агрегатов |
| `norm` | Норматив (32 для Ми-8, 35 для Ми-17) |
| `Δ` | **Дефицит** (отрицательное = некомплект!) |
| `shortages` | Детализация недостающих групп |

### Детальная инвентаризация

```bash
python3 code/heli_pandas_ops_inventory.py --version-date YYYY-MM-DD --version-id 1
```

**Выход:** `output/heli_pandas_ops_inventory_YYYY-MM-DD.md`

## Намерения по расширению Extract (10-08-2025)

- **MP3.group_by**: добавить поле `group_by` в `heli_pandas` через связь `partseqno_i (MP3)` = `partno_comp (MP1)`. 
  - Планеры: Ми‑8Т → `group_by=1`, Ми‑17 → `group_by=2`.
  - Экспортер MacroProperty3 будет включать `group_by` для RTC.
- **Pre-simulation status_change**: задача по начальному `status_change` зафиксирована в Tasktracker и будет выполнена после интеграции `group_by` (инициализация на D согласно RTC-логике).

## 🎯 Единый пайплайн версионности (Дата: 27-07-2025)

### Архитектура централизованной версионности

**Принцип:** ВСЕ компоненты ETL используют единую версию из `Status_Components.xlsx`

| **Компонент** | **Источник версии** | **Статус** | **Описание** |
|---------------|-------------------|-----------|--------------|
| **Extract Master** | `Status_Components.xlsx` | ✅ **КОРНЕВОЙ** | Читает метаданные Excel (дата создания/модификации) |
| **Все загрузчики** | CLI параметры Extract | ✅ **ПРИОРИТЕТ** | `--version-date 2025-07-04 --version-id 1` |
| **Fallback загрузчиков** | `Status_Components.xlsx` | ✅ **ЕДИНЫЙ ИСТОЧНИК** | `utils.version_utils.extract_unified_version_date()` |
| **Словари** | `heli_pandas` | ✅ **ЦЕПОЧКА** | `get_version_from_heli_pandas()` |
| **Тензоры** | CLI параметры Extract | ✅ **НАСЛЕДОВАНИЕ** | Получают версию из Extract Master |

### Критическая логика версионности

⚠️ **ВАЖНО:** Сохранена логика проверки года создания файла

| **Приоритет** | **Источник** | **Условие** | **Назначение** |
|---------------|-------------|-------------|----------------|
| **1** | Дата создания Excel | `abs(created_year - current_year) <= 1` | Основной источник версии |
| **2** | Дата модификации Excel | Если создание > 1 года | Резервный источник |
| **3** | Время модификации ОС | Если Excel метаданные недоступны | Последний fallback |

### Технические детали

**Подробности реализации и устраненные проблемы смотрите в `docs/changelog.md` от 27-07-2025**

## 📋 Перечень скриптов Extract (актуализировано 28-07-2025)

### Основные загрузчики данных (Extract Pipeline)

| **№** | **Скрипт** | **Назначение** | **Версионность** |
|-------|-----------|----------------|------------------|
| **1** | `md_components_loader.py` | MD Components (мастер-данные компонентов) - **ОПТИМИЗИРОВАН** | ✅ Единый источник |
| **2** | `status_overhaul_loader.py` | Status & Overhaul (статусы и капремонт) | ✅ Единый источник |
| **3** | `program_ac_loader.py` | Program AC (связка программ и ВС) | ✅ Единый источник |
| **4** | `dual_loader.py` | Status Components (основные данные + процессинг) | ✅ Единый источник |

### Процессоры обогащения данных (Extract Pipeline)

| **№** | **Скрипт** | **Назначение** | **Версионность** |
|-------|-----------|----------------|------------------|
| **5** | `enrich_heli_pandas.py` | Обогащение ac_type_mask | ✅ Наследует от heli_pandas |
| **6** | `calculate_beyond_repair.py` | Расчет Beyond Repair (br_mi8/br_mi17, минуты) - **ИСПРАВЛЕН** | ✅ Наследует от md_components |
| **7** | `md_components_enricher.py` | Обогащение MD Components | ✅ Наследует от словарей |
| **8** | `dictionary_creator.py` | Все справочники (статусы, партномера, серийники, владельцы, типы ВС, номера ВС) | ✅ Получает из heli_pandas |

### Генераторы тензоров (в конце пайплайна)

| **№** | **Скрипт** | **Назначение** | **Версионность** |
|-------|-----------|----------------|------------------|
| **9** | `program_fl_direct_loader.py` | Прямой тензор программ полетов на 4000 дней | ✅ CLI параметры Extract |
| **10** | `program_ac_direct_loader.py` | Прямой тензор операций ВС с постпроцессингом | ✅ CLI параметры Extract |
| **11** | `heli_pandas_group_by_enricher.py` | Обогащение `heli_pandas.group_by` по `md_components.partno_comp` (идемпотентно, запускается с `--apply`) | ✅ Наследует от heli_pandas/md_components |
| **12** | `digital_values_dictionary_creator.py` | Аддитивный словарь всех полей для Flame GPU - **ИСПРАВЛЕН** | ✅ Получает из heli_pandas |

### Финальные расчеты (критичные зависимости)

| **№** | **Скрипт** | **Назначение** | **Версионность** |
|-------|-----------|----------------|------------------|
| **12** | `repair_days_calculator.py` | Расчет repair_days для ВС в ремонте - **ПЕРЕМЕЩЕН В КОНЕЦ** | ✅ Наследует от heli_pandas |

## 🚀 Команды запуска Extract

### **Основной Extract цикл (интерактивный)**
```bash
# Тестовый режим (очистка всех таблиц + единая версионность)
python3 code/extract_master.py
# Выбрать: 1 (датасет), затем 1 (ТЕСТ)

# Продуктивный режим (добавление новых версий данных + единая версионность)  
python3 code/extract_master.py
# Выбрать: 2 (датасет), затем 2 (ПРОД)
```

### **Автоматический режим (для скриптов/CI)**
```bash
# Датасет 1 (2025-07-04), режим TEST (полная очистка)
printf "1\n1\n" | python3 code/extract_master.py

# Датасет 2 (2025-12-30), режим PROD (сохраняет данные других версий)
printf "2\n2\n" | python3 code/extract_master.py
```

> **Важно:** Первый датасет грузить в режиме TEST, последующие — в режиме PROD!

### **Первая настройка проекта**
```bash
# Автоматическая настройка окружения (один раз)
python3 code/utils/auto_config.py

# Проверка подключения к базе данных
python3 code/utils/test_db_connection.py
```

### **Диагностика и обслуживание**
```bash
# Информация о системе и настройках
python3 code/utils/auto_config.py --info

# Тестирование отображения эмодзи
python3 code/utils/display_manager.py

# Принудительный текстовый режим (для Windows)
EMOJI_MODE=text python3 code/extract_master.py

# Очистка аддитивной грязи в словарях
python3 code/utils/cleanup_dictionaries.py

# Полная очистка всех таблиц проекта
python3 code/utils/database_cleanup.py
```

## 🔄 Жизненный цикл данных

### **Ежедневный/еженедельный запуск**
1. **Обновление исходных Excel файлов** в `data_input/source_data/`
2. **Запуск Extract в продуктивном режиме**: `python3 code/extract_master.py` → выбор 2
3. **Проверка результатов** через валидацию в extract_master.py

### **При разработке/отладке**  
1. **Запуск Extract в тестовом режиме**: `python3 code/extract_master.py` → выбор 1
2. **Анализ результатов** и отладка проблем
3. **Повторный запуск** после исправлений

### **При проблемах**
1. **Диагностика**: `python3 code/utils/test_db_connection.py`
2. **Очистка словарей**: `python3 code/utils/cleanup_dictionaries.py` 
3. **Полная очистка**: `python3 code/utils/database_cleanup.py`
4. **Перезапуск Extract**: `python3 code/extract_master.py`

## 🎯 Результирующие таблицы Extract

### **Основные данные**
- **`heli_pandas`** - Центральная таблица всех компонентов (2.4M записей)
- **`md_components`** - Мастер-данные компонентов с обогащением (group_by, beyond_repair)
- **`status_overhaul`** - Статусы и капремонт компонентов
- **`program_ac`** - Связка программ и ВС в эксплуатации

### **Справочники (dictGet)**
- **`dict_status_flat`** - Справочник статусов компонентов
- **`dict_partno_flat`** - Справочник партномеров компонентов
- **`dict_serialno_flat`** - Справочник серийных номеров
- **`dict_owner_flat`** - Справочник владельцев ВС
- **`dict_ac_type_flat`** - Справочник типов ВС (МИ-8, МИ-17)
- **`dict_aircraft_number_flat`** - Справочник номеров планеров (279 ВС)

### **Тензоры для ABM**
- **`flight_program_fl`** - Программы полетов (279 планеров × 4000 дней = ~1.1M записей)
- **`flight_program_ac`** - Операции ВС с trigger полями (поля × типы × 4000 дней)

### Интегрированные процессоры (встроены в dual_loader.py)

*Следующие процессоры НЕ являются отдельными скриптами, а интегрированы в СКРИПТ 4:*
- **`aircraft_number_processor.py`** - определение номеров ВС → `heli_pandas.aircraft_number`
- **`overhaul_status_processor.py`** - обработка статусов капремонта → `heli_pandas.status_id`
- **`program_ac_status_processor.py`** - обработка статусов эксплуатации → `heli_pandas.status_id`
- **`inactive_planery_processor.py`** - обработка неактивных планеров → `heli_pandas.status_id`

### Утилиты Extract

16. **`utils/create_all_dictionaries.py`** - утилита-обертка для создания всех словарей
17. **`utils/test_db_connection.py`** - тестирование подключений к СУБД  
18. **`utils/cleanup_dictionaries.py`** - принудительная очистка всех словарей (для решения проблем с аддитивной грязью)
19. **`code/archive/aircraft_number_dict_creator.py`** - устаревший создатель справочника номеров ВС (заархивирован)
20. **`heli_pandas_partno_stats.py`** - агрегирует `heli_pandas` по `partno`, выводит статистику (`components`, `aircrafts`) и сохраняет отчёт в `output/heli_pandas_partno_stats_<version>.md` (через `--md-path`).
21. **`heli_pandas_ops_inventory.py`** - инвентаризация агрегатов на бортах в статусе `status_id=2`, считает количество установленных компонентов по каждому `aircraft_number` и сохраняет отчёт `docs/heli_pandas_ops_inventory_<version>.md`.
22. **`heli_pandas_ops_other_groups.py`** - для планеров `status_id=2` подсчитывает агрегаты `group_by>2`, сравнивает их количество с нормой `md_components.comp_number`, подсвечивает дефицитные группы и сохраняет отчёт `docs/heli_pandas_ops_other_groups_<version>.md`.

## 🔄 Порядок работы скриптов Extract (по Extract Master Pipeline)

### Управление пайплайном

**Оркестратор:** `extract_master.py`
- Выбор режима (тест/прод)
- **Единая версионность** из `Status_Components.xlsx`
- Обработка зависимостей
- Валидация результатов

### Этап 1-4: Базовые загрузчики (критичные)

| **Этап** | **Скрипт** | **Таблица СУБД** | **Источник версии** |
|----------|-----------|------------------|-------------------|
| **1** | `md_components_loader.py` | `md_components` | ✅ Единый источник |
| **2** | `status_overhaul_loader.py` | `status_overhaul` | ✅ Единый источник |
| **3** | `program_ac_loader.py` | `program_ac` | ✅ Единый источник |
| **4** | `dual_loader.py` | `heli_pandas` + `heli_raw` | ✅ Единый источник |

### Этап 5-8: Обогащение данных (некритичные)

| **Этап** | **Скрипт** | **Обогащаемые поля** | **Источник версии** |
|----------|-----------|---------------------|-------------------|
| **5** | `enrich_heli_pandas.py` | `heli_pandas.ac_type_mask` | ✅ Наследует от heli_pandas |
| **6** | `calculate_beyond_repair.py` | `md_components.br_mi8/br_mi17` | ✅ Наследует от md_components |
| **7** | `md_components_enricher.py` | `md_components.partno_comp` | ✅ Наследует от словарей |
| **8** | `dictionary_creator.py` | Все справочники | ✅ Получает из heli_pandas |

### Встроенные процессоры (интегрированы в ЭТАП 4)

⚠️ **Следующие процессоры НЕ являются отдельными скриптами:**

| **Процессор** | **Обогащаемое поле** | **Описание** |
|---------------|---------------------|--------------|
| `aircraft_number_processor.py` | `heli_pandas.aircraft_number` | Определение номеров ВС |
| `overhaul_status_processor.py` | `heli_pandas.status_id` | Обработка статусов капремонта |
| `program_ac_status_processor.py` | `heli_pandas.status_id` | Обработка статусов эксплуатации |
| `inactive_planery_processor.py` | `heli_pandas.status_id` | Обработка неактивных планеров |

### Этап 9-14: Тензоры для Flame GPU + precheck + финальные расчеты

| **Этап** | **Скрипт** | **Таблица СУБД** | **Источник версии** |
|----------|-----------|------------------|-------------------|
| **9** | `program_fl_direct_loader.py` | `flight_program_fl` | ✅ CLI параметры Extract |
| **10** | `program_ac_direct_loader.py` | `flight_program_ac` | ✅ CLI параметры Extract |
| **11** | `heli_pandas_group_by_enricher.py` | `heli_pandas` | ✅ Наследует от heli_pandas/md_components |
| **12** | `program_ac_precheck_runner.py` | `heli_pandas` | ✅ Наследует от heli_pandas/md_components/flight_program_fl |
| **13** | `digital_values_dictionary_creator.py` | `dict_digital_values_flat` | ✅ Получает из heli_pandas |
| **14** | `repair_days_calculator.py` | `heli_pandas.repair_days` | ✅ Наследует от heli_pandas |
## 📚 Матрица чтение/запись по этапам Extract (актуально на 04-09-2025)

Ниже указано для каждого этапа: какие таблицы/поля читаются, какие пишутся/обновляются, и инварианты порядка (почему этап на своём месте).

1) md_components_loader.py (Этап 1)
- Читает: Excel MD_Components (листы, сырьё)
- Пишет: `md_components` поля: partno, comp_number, group_by, ac_type_mask, type/common_restricted*, trigger_interval, partout_time, assembly_time, repair_time, ll_mi8/mi17, oh_mi8/ми17, repair_price, purchase_price, version_date, version_id
- Инварианты: корневой справочник для фильтрации в dual_loader; нужен до любых обогащений

2) status_overhaul_loader.py (Этап 2)
- Читает: Excel Status_Overhaul
- Пишет: `status_overhaul` поля: ac_registr, wpno, status, sched_*/act_* даты, owner/operator, version_date, version_id
- Инварианты: источник статусов ремонта для process_status_field

3) program_ac_loader.py (Этап 3)
- Читает: Excel Program_AC
- Пишет: `program_ac` поля: ac_registr, ac_typ, owner/operator, homebase*, directorate, version_date, version_id
- Инварианты: источник статусов эксплуатации

4) dual_loader.py (Этап 4)
- Читает: Excel Status_Components; `md_components.partno`, `status_overhaul`, `program_ac`
- Пишет: `heli_raw` базовые поля; `heli_pandas` базовые + обогащённые: status_id (через процессоры), repair_days (init/коррекции), aircraft_number, ac_type_mask(позже), group_by(колонка, значение позже)
- Инварианты: центральная таблица; требует готового `md_components`

5) enrich_heli_pandas.py (Этап 5)
- Читает: `heli_pandas`, при необходимости словари типов
- Пишет: `heli_pandas.ac_type_mask`
- Инварианты: маска типов нужна до словарей и тензоров

6) calculate_beyond_repair.py (Этап 6)
- Читает: `md_components` поля цен и ресурсов, `ac_type_mask`
- Пишет: `md_components.br_mi8/br_mi17`
- Инварианты: BR используется precheck/аналитикой; безопасно до словарей

7) md_components_enricher.py (Этап 7)
- Читает: `dict_partno_flat` (если существует) или исходники; `md_components.partno`
- Пишет: `md_components.partno_comp`
- Инварианты: связывает MD с MP3; нужен до final расчётов/тензоров

8) dictionary_creator.py (Этап 8)
- Читает: `heli_pandas` (distinct partseqno_i/psn/address_i/ac_typ/aircraft_number), `md_components`
- Пишет: `dict_partno_flat`, `dict_serialno_flat`, `dict_owner_flat`, `dict_ac_type_flat`, `dict_status_flat`, `dict_aircraft_number_flat` (+ Dictionary объекты)
- Инварианты: словари требуются тензорам и мета‑словарю

9) program_fl_direct_loader.py (Этап 9)
- Читает: Excel Program.xlsx, `dict_aircraft_number_flat`
- Пишет: `flight_program_fl` (dates, aircraft_number, daily_hours, ac_type_mask, version_date, version_id)
- Инварианты: FL нужен для D1 precheck

10) program_ac_direct_loader.py (Этап 10)
- Читает: Excel Program_heli.xlsx; `heli_pandas`, `md_components`
- Пишет: `flight_program_ac`
- Инварианты: используется мета‑словарём; независим от precheck

11) heli_pandas_group_by_enricher.py (Этап 11)
- Читает: `md_components.partno_comp`, `heli_pandas.partseqno_i`
- Пишет: `heli_pandas.group_by`
- Инварианты: group_by требуется для фильтрации планеров/агрегатов

12) heli_pandas_component_status.py (Этап 12)
- Читает: `heli_pandas` (group_by, aircraft_number, condition, status_id)
- Пишет: `heli_pandas.status_id = 2` для агрегатов на ВС в эксплуатации
- Условия: group_by > 2, aircraft_number > 0, condition = 'ИСПРАВНЫЙ', связанный планер имеет status_id = 2
- Инварианты: выполняется после group_by_enricher; требует status_id планеров (из dual_loader)

13) heli_pandas_serviceable_status.py (Этап 13)
- Читает: `heli_pandas` (group_by, condition, status_id)
- Пишет: `heli_pandas.status_id = 3` для исправных агрегатов НЕ на ВС в эксплуатации
- Условия: group_by > 2, condition = 'ИСПРАВНЫЙ', status_id = 0
- Инварианты: выполняется ПОСЛЕ heli_pandas_component_status; обрабатывает остаток

14) digital_values_dictionary_creator.py (Этап 14)
- Читает: `DESCRIBE` всех таблиц Extract, включая `flight_program_*`
- Пишет: `dict_digital_values_flat` + Dictionary `digital_values_dict_flat`
- Инварианты: выполняется после формирования всех таблиц

15) repair_days_calculator.py (Этап 15)
- Читает: `md_components.repair_time`, `heli_pandas` (status_id=4, target_date), `status_overhaul`
- Пишет: `heli_pandas.repair_days`
- Инварианты: финальный расчёт после всех обогащений

## 📊 Основные таблицы после Extract

### heli_pandas (основная таблица компонентов)
*Краткое описание основных полей (полная таблица в разделе "СКРИПТ 4")*
- **partno** Nullable(String) - чертежный номер компонента
- **serialno** Nullable(String) - серийный номер компонента  
- **status_id** UInt8 - статус компонента (1-неактивно, 2-эксплуатация, 3-исправен, 4-ремонт, 5-резерв, 6-хранение)
- **oh** Nullable(UInt32) - overhaul ресурс
- **ll** Nullable(UInt32) - life limit ресурс
- **repair_days** Nullable(UInt16) - дни ремонта уже прошедшие
- **aircraft_number** UInt32 - номер ВС
- **ac_type_mask** UInt8 - битовая маска типа ВС (multihot)

### md_components (справочник компонентов)
*Краткое описание основных полей (полная таблица в разделе "СКРИПТ 1")*
- **partno** Nullable(String) - чертежный номер
- **comp_number** Nullable(UInt8) - количество на ВС
- **group_by** Nullable(UInt8) - группировка (1-МИ8, 2-МИ17)
- **repair_time** Nullable(UInt16) - время ремонта в днях
- **ll_mi8**, **ll_mi17**, **oh_mi8**, **oh_mi17**, **br2_mi17** — ресурсы из Excel конвертируются из часов в МИНУТЫ при загрузке (`md_components_loader.py`).
- **br_mi8** Nullable(UInt32), **br_mi17** Nullable(UInt32) — Beyond Repair (экономический порог списания), ЕДИНИЦЫ: МИНУТЫ; расчёт в `calculate_beyond_repair.py` выполняется в минутах (без дополнительного ×60), инвариант: `br <= ll`.
- **br2_mi17** Nullable(UInt32) — порог межремонтного ресурса для подъёма из inactive (минуты). Используется в симуляции: если `ppr >= br2_mi17`, выполняется ремонт с обнулением ppr; иначе — комплектация без ремонта (ppr сохраняется).
- **partno_comp** Nullable(UInt32) - component ID для связи с heli_pandas

### Справочные таблицы
*Краткие описания (детальные таблицы в соответствующих разделах СКРИПТ 2-12)*
- **status_overhaul** - данные капремонта и статусов ВС (13 полей)
- **program_ac** - реестр ВС в программах эксплуатации (11 полей)
- **dict_status_flat** - справочник статусов компонентов (6 значений)
- **dict_aircraft_number_flat** - справочник номеров ВС с ac_type_mask (279 ВС)
- **dict_digital_values_flat** - мета-словарь всех полей Extract системы

### **Тензоры для Flame GPU (Дата: 2025-07-20)**
- **`flight_program_fl`** - Нормативные налеты планеров (~1.1M записей)
  - **Поля**: aircraft_number, dates, daily_hours, ac_type_mask
  - **daily_hours**: НОРМАТИВНЫЙ налет (потенциальный при эксплуатации)
  - **daily_flight** (планируется): РЕАЛЬНЫЙ налет после прогноза статусов
  - **MultiBOM**: Планеры как корневые элементы для утилизации агрегатов
- **`flight_program_ac`** - Операции и поставки ВС (4000 записей)
  - **Структура**: Оптимизированная flat с легкими типами (UInt16/UInt8/Int8)
  - **Поля**: dates, ops_counter_*, trigger_program_*, new_counter_mi17
  - **Особенности**: Trigger логика с корректировкой первой даты

## 📁 Управление входными данными

### Структура data_input

**Размещение Excel файлов:**
- Основные файлы компонентов размещаются в `data_input/`
- Поддерживаемые форматы: .xlsx, .xls
- Кодировка: UTF-8 или CP1251

**Требования к структуре Excel:**
- Обязательные колонки: partno, serialno, name
- Рекомендуемые колонки: oh, ll, sne, ppr, oh_threshold
- Первая строка - заголовки колонок
- Данные начинаются со второй строки

**Именование файлов:**
Рекомендуется использовать шаблон: `Status_Components_YYYYMMDD.xlsx`

**Размер файлов:**
- Оптимальный размер: до 100MB
- Поддерживается до 1M записей в одном файле
- При больших объемах рекомендуется разбиение

### Примеры входных данных

**Минимальная структура Excel:**
- partno, serialno, name

**Полная структура Excel:**
- partno, serialno, name, oh, ll, sne, ppr, oh_threshold

## ⚙️ Конфигурация Extract

### Настройки загрузки
- BATCH_SIZE=10000 (размер пакета для массовой вставки)
- MAX_RETRIES=3 (количество повторных попыток)
- TIMEOUT_SECONDS=300 (таймаут операций)

## 🔍 Мониторинг Extract

### Контроль качества данных

**Метрики после загрузки:**
- Количество загруженных записей
- Проверка обязательных полей
- Статистика по типам записей
- Распределение статусов после обработки

### Типичные проблемы Extract

1. **Дублирующие записи** - проверка ключей (partno, serialno)
2. **Некорректные числовые данные** - замена на NULL
3. **Проблемы кодировки** - автоматическое определение CP1251/UTF-8
4. **Большие файлы** - разбиение на пакеты при загрузке

## 🚁 Специальная обработка aircraft_number с ac_type_mask

### Исправленная логика словаря aircraft_number

**Проблема (исправлено 19-07-2025):** Словарь `aircraft_number_dict_flat` содержал нулевые значения `ac_type_mask` из-за неправильного порядка ETL этапов.

**Решение:**
1. **Порядок этапов**: `enrich_heli_pandas.py` перемещен ПЕРЕД `dictionary_creator.py`
2. **Строгая фильтрация**: Только ВС с планерными partno включаются в словарь
3. **Корректный запрос**: ac_type_mask извлекается от любых записей ВС через JOIN

**Планерные partno для фильтрации:**
- МИ-8Т, МИ-8П, МИ-8ПС, МИ-8ТП, МИ-8АМТ, МИ-8МТВ (ac_type_mask = 32)
- МИ-17, МИ-26 (ac_type_mask = 64)

**Финальный результат:**
- ✅ **279 ВС** в словаре (отфильтровано по планерам)
- ✅ **163 ВС с ac_type_mask = 32** (Ми-8 семейство)
- ✅ **116 ВС с ac_type_mask = 64** (Ми-17 семейство)
- ✅ **dictGet('aircraft_number_dict_flat', 'ac_type_mask', aircraft_number)** работает для Flame GPU

### Использование ac_type_mask в аналитике

Поддерживаются запросы для получения типа ВС по номеру через dictGet:
- Получение ac_type_mask по aircraft_number
- Получение registration_code по aircraft_number  
- Объединение данных heli_pandas с dict_aircraft_number_flat
- Статистика по типам ВС с использованием CASE конструкций

## 🔧 Проблемы с исходными данными (исправлено 19-07-2025)

### Некорректные регистрационные коды OB-/OM-

**Проблема:** В исходном Excel были записи с регистрационными кодами `OB-` и `OM-` вместо стандартных `RA-`.

**Влияние на ETL:**
- `aircraft_number_processor.py` обрабатывает только `RA-` формат
- Записи с `OB-/OM-` получали `aircraft_number = 0` и `location = ''`
- Результат: 3100 записей "мусора" в heli_pandas

**Решение:**
1. **Ручная очистка Excel** - исключены все записи с `OB-/OM-` регистрацией
2. **Исключены связанные агрегаты** этих ВС
3. **Результат:** корректные 279 ВС вместо 284

### Анализ агрегатов без ВС

**Обнаружено:** 3100 записей с `aircraft_number = 0` в heli_pandas - это НЕ грязь, а корректные данные:
- **Агрегаты на складе** (не установленные на ВС)
- **Запасные части** в ремонте  
- **Компоненты в резерве**

**Важно:** Эти записи должны оставаться в системе для учета всех компонентов, но исключаются из словаря aircraft_number.

### Утилита очистки словарей

**Файл:** `code/utils/cleanup_dictionaries.py`

**Назначение:** Принудительная очистка всех словарей при накоплении "аддитивной грязи"

**Использование:**
Запуск из корня проекта с подтверждением yes для принудительной очистки всех словарей.

**Что удаляет:**
- Все Dictionary объекты (aircraft_number_dict_flat, status_dict_flat и др.)
- Все словарные таблицы (dict_aircraft_number_flat, dict_status_flat и др.)
- НЕ трогает основные данные (heli_pandas, md_components и др.)

**Когда использовать:**
- При проблемах с устаревшими данными в словарях
- Перед чистой перезагрузкой ETL
- При изменении логики создания словарей

## 🧹 Принципы безопасной уборки

*Принципы безопасной уборки перенесены в `.cursorrules` раздел "Автоматическая уборка рабочего стола"*

## 🔧 Исправления repair_days и циклических зависимостей (Дата: 26-07-2025)

## 🎯 Оптимизация полей MacroProperty1 для аналитики (Дата: 28-07-2025)

### Завершенная оптимизация типов данных GPU

**Цель:** Переход от Float64 к оптимизированным GPU-совместимым типам данных в таблице `md_components`.

#### Ключевые изменения в полях:

| **Поле** | **Было** | **Стало** | **Причина изменения** |
|----------|----------|-----------|----------------------|
| `ac_typ` | `Nullable(String)` | `ac_type_mask` `Nullable(UInt8)` | **Переименование + оптимизация:** String→битовая маска для GPU |
| `sne` | `Nullable(Float64)` | `sne_new` `Nullable(UInt32)` | **Переименование + оптимизация:** Float64→UInt32 для производительности |
| `ppr` | `Nullable(Float64)` | `ppr_new` `Nullable(UInt32)` | **Переименование + оптимизация:** Float64→UInt32 для производительности |
| все остальные | `Float64` | `UInt8/UInt16/UInt32/Float32` | **Массовая оптимизация:** типы данных под GPU архитектуру |

#### Исправленные зависимости:

**1. `calculate_beyond_repair.py`:**
- **Проблема:** Использовал устаревшее поле `ac_typ`
- **Решение:** Обновлен на `ac_type_mask`

**2. `digital_values_dictionary_creator.py`:**
- **Проблема:** Неправильный `field_key` создавал конфликты полей
- **Решение:** Изменен на `(table_name, field_name)` для уникальности

**3. `flame_macroproperty1_loader.py`:**
- **Проблема:** Загружал все поля вместо оптимизированного набора
- **Решение:** Фильтрация на 20 полей аналитики включая `ac_type_mask`, `sne_new`, `ppr_new`

#### Результат оптимизации:

✅ **Все поля MacroProperty1 оптимизированы для GPU**  
✅ **Extract пайплайн работает стабильно (12/12 этапов)**  
✅ **Версионность едина: 2025-07-04 v1 по всем таблицам**  
✅ **Flame GPU готов к использованию оптимизированных данных**

### Проблема циклической зависимости

**Исходная проблема:**
- `dual_loader.py` (ЭТАП 4) рассчитывал `repair_days`, но нуждался в `repair_time` из `md_components`
- `md_components_enricher.py` (ЭТАП 7) заполнял `repair_time`, но выполнялся ПОСЛЕ `dual_loader.py`
- Результат: циклическая зависимость и негативные значения `repair_days`

### Решение: Вынос расчета repair_days в отдельный этап

**Новый этап 8: `repair_days_calculator.py`**
- **Зависимости**: md_components (с repair_time), heli_pandas (с status_id=4), status_overhaul
- **Позиция**: ПОСЛЕ md_components_enricher.py
- **Формула**: `repair_days = repair_time - (target_date - version_date)`

### Новая формула repair_days

**Было (некорректно):**
```python
repair_days = (target_date - version_date).days  # Дни до окончания ремонта
```

**Стало (правильно):**
```python
repair_days = repair_time - (target_date - version_date).days  # Дни ремонта уже прошедшие
```

**Логика:**
- `repair_time` - общее время ремонта (из md_components, например 180 дней)
- `(target_date - version_date)` - дни до завершения ремонта
- `repair_days` - сколько дней ремонта уже прошло

**Примеры расчета:**
- ВС 22215: `repair_days = 180 - (2025-09-05 - 2025-07-04) = 180 - 63 = 117` ✅
- ВС 22431: `repair_days = 180 - (2025-07-27 - 2025-07-04) = 180 - 23 = 157` ✅

### Улучшенные фильтры для status=4

**Новые условия установки status=4 (Ремонт):**
1. `sched_start_date` не пустая И меньше `version_date` ИЛИ
2. `act_start_date` не пустая И меньше `version_date`
3. При обеих пустых датах → НЕ устанавливать status=4
4. При датах больше `version_date` → НЕ устанавливать status=4

**Результат:** Исключены ВС с будущими датами начала ремонта (например, ВС 24116)

### Обновленный порядок Extract пайплайна (актуализировано 28-07-2025)

```
ЭТАП 7: md_components_enricher.py              → заполняет repair_time
ЭТАП 8: dictionary_creator.py                  → создает словари
ЭТАП 9-11: program_fl/ac/digital_values       → тензоры и мета-словари
ЭТАП 12: repair_days_calculator.py            → рассчитывает repair_days (ФИНАЛЬНЫЙ)
```

**Файлы изменений:**
- `code/repair_days_calculator.py` - новый скрипт расчета
- `code/overhaul_status_processor.py` - убран расчет repair_days  
- `code/extract_master.py` - repair_days_calculator.py перемещен в конец (этап 12)
- `docs/extract.md` - обновлена документация

**Тестирование:** ✅ Все 7 ВС в ремонте получили корректные положительные значения repair_days

---

## 📊 Детальный анализ ETL скриптов

### СКРИПТ 1: `md_components_loader.py` (обновлено 02-01-2026)

| **Характеристика** | **Значение** |
|-------------------|--------------|
| **Порядковый номер** | 1 (первый в Extract пайплайне) |
| **Создает таблицу СУБД** | ✅ `md_components` (MergeTree) |
| **Создает датафрейм** | ✅ `df` (pandas from Excel) |
| **Источник данных** | `data_input/master_data/MD_Сomponents.xlsx` |
| **Лист Excel** | "Агрегаты" (header=1, вторая строка) |
| **ENGINE ClickHouse** | `MergeTree()` |
| **ORDER BY** | `(version_date, version_id)` |
| **PARTITION BY** | `toYYYYMM(version_date)` |

#### ⚡ ВАЖНО: Единый справочник (02-01-2026)

`md_components` — **единый справочник номенклатур** для всех датасетов:

- **Без дублирования**: 77 записей (не 77×N для N датасетов)
- **UPSERT логика**: проверка по `partno`, добавляются только НОВЫЕ номенклатуры
- **Аддитивная таблица**: НЕ удаляется при политике "перезаписать"
- **version_date**: дата первого добавления (timestamp)

```python
# Логика загрузки:
existing_partnos = {row[0] for row in client.execute("SELECT DISTINCT partno FROM md_components")}
df_new = df[~df['partno'].isin(existing_partnos)]  # Только новые
# Если df_new пустой — ничего не добавляем
```

#### Поля таблицы `md_components` (оптимизировано для MacroProperty1):

| **№** | **Поле** | **Тип ClickHouse** | **Тип cuDF** | **MacroProperty1** | **Источник** | **Описание** |
|-------|----------|-------------------|--------------|-------------------|--------------|--------------|
| 1 | `partno` | `Nullable(String)` | - | ❌ | Excel | Чертежный номер |
| 2 | `comp_number` | `Nullable(UInt8)` | `uint8` | ✅ | Excel | Количество на ВС (оптимизировано Float64→UInt8) |
| 3 | `group_by` | `Nullable(UInt8)` | `uint8` | ✅ | Excel | Группировка (оптимизировано Float64→UInt8) |
| 4 | `ac_type_mask` | `Nullable(UInt8)` | `uint8` | ✅ | Excel | **Тип ВС маска (переименовано из ac_typ, String→UInt8)** |
| 5 | `type_restricted` | `Nullable(UInt8)` | `multihot[u8]` | ✅ | Excel | Ограничение по типу (оптимизировано Float64→UInt8) |
| 6 | `common_restricted1` | `Nullable(UInt8)` | `uint8` | ✅ | Excel | Общее ограничение 1 (оптимизировано Float64→UInt8) |
| 7 | `common_restricted2` | `Nullable(UInt8)` | `uint8` | ✅ | Excel | Общее ограничение 2 (оптимизировано Float64→UInt8) |
| 8 | `trigger_interval` | `Nullable(UInt8)` | `uint8` | ✅ | Excel | Интервал срабатывания (оптимизировано Float64→UInt8) |
| 9 | `partout_time` | `Nullable(UInt8)` | `uint8` | ✅ | Excel | Время снятия (оптимизировано Float64→UInt8) |
| 10 | `assembly_time` | `Nullable(UInt8)` | `uint8` | ✅ | Excel | Время установки (оптимизировано Float64→UInt8) |
| 11 | `repair_time` | `Nullable(UInt16)` | `uint16` | ✅ | Excel | Время ремонта (оптимизировано Float64→UInt16) |
| 12 | `ll_mi8` | `Nullable(UInt32)` | `uint32` | ✅ | Excel | LL МИ-8 (оптимизировано Float64→UInt32) |
| 13 | `oh_mi8` | `Nullable(UInt32)` | `uint32` | ✅ | Excel | OH МИ-8 (оптимизировано Float64→UInt32) |
| 14 | `oh_threshold_mi8` | `Nullable(UInt32)` | `uint32` | ✅ | Excel | Порог OH МИ-8 (оптимизировано Float64→UInt32) |
| 15 | `ll_mi17` | `Nullable(UInt32)` | `uint32` | ✅ | Excel | LL МИ-17 (оптимизировано Float64→UInt32) |
| 16 | `oh_mi17` | `Nullable(UInt32)` | `uint32` | ✅ | Excel | OH МИ-17 (оптимизировано Float64→UInt32) |
| 17 | `repair_price` | `Nullable(Float32)` | - | ❌ | Excel | Цена ремонта (оптимизировано Float64→Float32) |
| 18 | `purchase_price` | `Nullable(Float32)` | - | ❌ | Excel | Цена покупки (оптимизировано Float64→Float32) |
| 19 | `sne_new` | `Nullable(UInt32)` | `uint32` | ✅ | Excel | **SNE (переименовано из sne, Float64→UInt32)** |
| 20 | `ppr_new` | `Nullable(UInt32)` | `uint32` | ✅ | Excel | **PPR (переименовано из ppr, Float64→UInt32)** |
| 21 | `version_date` | `Date` | `Date` | ❌ | Метаданные | Дата версии |
| 22 | `version_id` | `UInt8` | `uint8` | ❌ | Метаданные | ID версии |
| 23 | `br_mi8`/`br_mi17` | `Nullable(UInt32)` | `uint32` | ✅ | **Обогащение** | Beyond Repair по типам (минуты, calculate_beyond_repair.py) |
| 24 | `br2_mi17` | `Nullable(UInt32)` | `uint32` | ✅ | Excel | **Порог межремонтного ресурса для подъёма из inactive (минуты, для Mi-17)** |
| 25 | `partno_comp` | `Nullable(UInt32)` | `uint32` | ✅ | **Обогащение** | Component ID (md_components_enricher.py) |
| 26 | `restrictions_mask` | `UInt8` | `multihot[u8]` | ✅ | **Расчет** | Битовая маска ограничений (type_restricted*1+common_restricted1*2+common_restricted2*4+trigger_interval*8) |

### СКРИПТ 2: `status_overhaul_loader.py`

| **Характеристика** | **Значение** |
|-------------------|--------------|
| **Порядковый номер** | 2 (второй в Extract пайплайне) |
| **Создает таблицу СУБД** | ✅ `status_overhaul` (MergeTree) |
| **Создает датафрейм** | ✅ `df` (pandas from Excel) |
| **Источник данных** | `data_input/source_data/Status_Overhaul.xlsx` |
| **Лист Excel** | "Status_Overhaul" (header=0, первая строка) |
| **ENGINE ClickHouse** | `MergeTree()` |
| **ORDER BY** | `(ac_registr, wpno, status, version_date, version_id)` |
| **PARTITION BY** | `toYYYYMM(version_date)` |

#### Поля таблицы `status_overhaul`:

| **№** | **Поле** | **Тип ClickHouse** | **Тип cuDF** | **MacroProperty** | **Источник** | **Описание** |
|-------|----------|-------------------|--------------|-------------------|--------------|--------------|
| 1 | `ac_registr` | `UInt32` | - | ❌ | Excel | Регистрационный номер ВС |
| 2 | `ac_typ` | `String` | - | ❌ | Excel | Тип ВС (МИ8, МИ8АМТ) |
| 3 | `wpno` | `String` | - | ❌ | Excel | Номер рабочего пакета |
| 4 | `description` | `String` | - | ❌ | Excel | Описание работ |
| 5 | `sched_start_date` | `Nullable(Date)` | - | ❌ | Excel | Плановая дата начала |
| 6 | `sched_end_date` | `Nullable(Date)` | - | ❌ | Excel | Плановая дата окончания |
| 7 | `act_start_date` | `Nullable(Date)` | - | ❌ | Excel | Фактическая дата начала |
| 8 | `act_end_date` | `Nullable(Date)` | - | ❌ | Excel | Фактическая дата окончания |
| 9 | `status` | `String` | - | ❌ | Excel | Статус (Закрыто, В процессе, Открыто) |
| 10 | `owner` | `String` | - | ❌ | Excel | Собственник |
| 11 | `operator` | `String` | - | ❌ | Excel | Оператор |
| 12 | `version_date` | `Date` | - | ❌ | Метаданные | Дата версии данных |
| 13 | `version_id` | `UInt8` | - | ❌ | Метаданные | ID версии |

### СКРИПТ 3: `program_ac_loader.py`

| **Характеристика** | **Значение** |
|-------------------|--------------|
| **Порядковый номер** | 3 (третий в Extract пайплайне) |
| **Создает таблицу СУБД** | ✅ `program_ac` (MergeTree) |
| **Создает датафрейм** | ✅ `df` (pandas from Excel) |
| **Источник данных** | `data_input/source_data/Program_AC.xlsx` |
| **Лист Excel** | "Program_AC" (header=0, первая строка) |
| **ENGINE ClickHouse** | `MergeTree()` |
| **ORDER BY** | `(ac_registr, ac_typ, version_date, version_id)` |
| **PARTITION BY** | `toYYYYMM(version_date)` |

#### Поля таблицы `program_ac`:

| **№** | **Поле** | **Тип ClickHouse** | **Тип cuDF** | **MacroProperty** | **Источник** | **Описание** |
|-------|----------|-------------------|--------------|-------------------|--------------|--------------|
| 1 | `ac_registr` | `UInt32` | - | ❌ | Excel | Регистрационный номер ВС |
| 2 | `ac_typ` | `String` | - | ❌ | Excel | Тип ВС (350B3, 355NP, МИ8МТВ, МИ26Т) |
| 3 | `object_type` | `String` | - | ❌ | Excel | Тип объекта (HELICOPTER) |
| 4 | `description` | `String` | - | ❌ | Excel | Полное описание модели ВС |
| 5 | `owner` | `String` | - | ❌ | Excel | Собственник (ЮТ-ВУ, CHOPPER LL, ГТЛК) |
| 6 | `operator` | `String` | - | ❌ | Excel | Эксплуатант (ЮТ-ВУ) |
| 7 | `homebase` | `String` | - | ❌ | Excel | Код базы приписки (ТЮМ, СУР, НОЯ) |
| 8 | `homebase_name` | `String` | - | ❌ | Excel | Полное наименование базы приписки |
| 9 | `directorate` | `String` | - | ❌ | Excel | Дирекция (ЗАПАДНО-СИБИРСКАЯ ДИРЕКЦИЯ) |
| 10 | `version_date` | `Date` | - | ❌ | Метаданные | Дата версии данных |
| 11 | `version_id` | `UInt8` | - | ❌ | Метаданные | ID версии |

### СКРИПТ 4: `dual_loader.py`

| **Характеристика** | **Значение** |
|-------------------|--------------|
| **Порядковый номер** | 4 (четвертый в Extract пайплайне) |
| **Создает таблицы СУБД** | ✅ `heli_raw` + `heli_pandas` (MergeTree) |
| **Создает датафрейм** | ✅ `df` (pandas from Excel) |
| **Источник данных** | `data_input/source_data/Status_Components.xlsx` |
| **Лист Excel** | "Status_Components" (header=0, первая строка) |
| **ENGINE ClickHouse** | `MergeTree()` |
| **ORDER BY** | `(version_date, version_id)` |
| **PARTITION BY** | `toYYYYMM(version_date)` |

#### Фильтрация по owner (только heli_pandas)

При загрузке в `heli_pandas` применяется двухэтапный фильтр по собственнику.

**Разрешённые owner:**
```python
ALLOWED_OWNERS = {'ЮТ-ВУ', 'UTE', 'ГТЛК', 'ВТК-АВИА', 'РЕГ ЛИЗИНГ', 'СБЕР ЛИЗИНГ', 'АК ЮТЭЙР', 'PL PANORAMA'}
```

**Этап 1: Определение "наших" бортов**
- Фильтруем планеры (`partno` начинается с `МИ-8`) по двум критериям:
  - `owner` в `ALLOWED_OWNERS`
  - `location` начинается с `RA-` (российская регистрация)
- Планеры без `RA-*` регистрации (иностранные, без номера) исключаются
- Получаем список RA-номеров "наших" бортов

**Этап 2: Фильтрация всех записей**

Запись **остаётся** в `heli_pandas` если выполняется **любое** из условий:
1. **Планер** (`partno` начинается с `МИ-8`) с `owner` в `ALLOWED_OWNERS` **И** `location` начинается с `RA-`
2. **Агрегат на "нашем" борту** — `location` = один из RA-номеров "наших" бортов (независимо от `owner`)
3. **Агрегат на складе** с `owner` в `ALLOWED_OWNERS`

**Важно:**
- Чужие планеры **полностью исключаются** со всеми их агрегатами
- Планеры без российской регистрации (`RA-*`) **полностью исключаются**
- Гарантийные агрегаты на НАШИХ бортах **остаются** для участия в симуляции
- Данные **сохраняются в `heli_raw`** без фильтрации для полного архива

#### Поля таблицы `heli_pandas` (основная таблица):

| **№** | **Поле** | **Тип ClickHouse** | **Тип cuDF** | **MacroProperty** | **Источник** | **Описание** |
|-------|----------|-------------------|--------------|-------------------|--------------|--------------|
| 1 | `partno` | `Nullable(String)` | - | ❌ | Excel | Чертежный номер компонента |
| 2 | `serialno` | `Nullable(String)` | - | ❌ | Excel | Серийный номер компонента |
| 3 | `ac_typ` | `Nullable(String)` | - | ❌ | Excel | Тип ВС |
| 4 | `location` | `Nullable(String)` | - | ❌ | Excel | Местоположение компонента |
| 5 | `mfg_date` | `Nullable(Date)` | `Date` | ✅ MacroProperty3 | Excel | Дата изготовления |
| 6 | `removal_date` | `Nullable(Date)` | - | ❌ | Excel | Дата снятия |
| 7 | `target_date` | `Nullable(Date)` | - | ❌ | Excel | Целевая дата |
| 8 | `condition` | `Nullable(String)` | - | ❌ | Excel | Состояние компонента |
| 9 | `owner` | `Nullable(String)` | - | ❌ | Excel | Владелец |
| 10 | `lease_restricted` | `UInt8` | `uint8` | ✅ MacroProperty3 | Excel | Ограничение лизинга |
| 11 | `oh` | `Nullable(UInt32)` | `uint32` | ✅ MacroProperty3 | Excel | Overhaul ресурс |
| 12 | `oh_threshold` | `Nullable(UInt32)` | `uint32` | ✅ MacroProperty3 | Excel | Порог Overhaul |
| 13 | `ll` | `Nullable(UInt32)` | `uint32` | ✅ MacroProperty3 | Excel | Life limit ресурс |
| 14 | `sne` | `Nullable(UInt32)` | `uint32` | ✅ MacroProperty3 | Excel | SNE ресурс |
| 15 | `ppr` | `Nullable(UInt32)` | `uint32` | ✅ MacroProperty3 | Excel | PPR ресурс |
| 16 | `version_date` | `Date` | `Date` | ✅ Property | Метаданные | Дата версии |
| 17 | `version_id` | `UInt8` | `uint8` | ✅ Property | Метаданные | ID версии |
| 18 | `partseqno_i` | `Nullable(UInt32)` | `uint16` | ✅ MacroProperty3 | Excel | ID партномера из Excel |
| 19 | `psn` | `Nullable(UInt32)` | `uint32` | ✅ MacroProperty3 | Excel | ID серийного номера из Excel |
| 20 | `address_i` | `Nullable(UInt16)` | `uint16` | ✅ MacroProperty3 | Excel | ID владельца из Excel |
| 21 | `ac_type_i` | `Nullable(UInt16)` | - | ❌ | Excel | ID типа ВС из Excel |
| 22 | `status_id` | `UInt8` | `uint8` | ✅ MacroProperty3 | **Обогащение** | Статус компонента (status_processor.py) |
| 23 | `repair_days` | `Nullable(UInt16)` | `uint16` | ✅ MacroProperty3 | **Обогащение** | Дни ремонта (repair_days_calculator.py) |
| 24 | `aircraft_number` | `UInt32` | `uint32` | ✅ MacroProperty3 | **Обогащение** | Номер ВС (aircraft_number_processor.py) |
| 25 | `ac_type_mask` | `UInt8` | `multihot[u8]` | ✅ MacroProperty3 | **Обогащение** | Битовая маска типа ВС (enrich_heli_pandas.py) |

### СКРИПТ 5: `enrich_heli_pandas.py`

| **Характеристика** | **Значение** |
|-------------------|--------------|
| **Порядковый номер** | 5 (пятый в Extract пайплайне) |
| **Создает таблицу СУБД** | ❌ (обогащает существующую `heli_pandas`) |
| **Создает датафрейм** | ❌ (работает с ClickHouse напрямую) |
| **Источник данных** | `dict_ac_type_flat` или встроенные маски |
| **Назначение** | Заполнение поля `ac_type_mask` битовыми масками |
| **Операция** | `ALTER TABLE heli_pandas UPDATE` |

#### Обогащаемое поле:

| **№** | **Поле** | **Тип ClickHouse** | **Тип cuDF** | **MacroProperty** | **Источник** | **Описание** |
|-------|----------|-------------------|--------------|-------------------|--------------|--------------|
| 1 | `ac_type_mask` | `UInt8` | `multihot[u8]` | ✅ MacroProperty3 | **Обогащение** | Битовые маски типов ВС для multihot операций |

#### Битовые маски типов ВС (ХАРДКОД):

⚠️ **ХАРДКОД КОНСТАНТЫ** в `code/enrich_heli_pandas.py` (строки 32-48):

| **Тип ВС** | **Маска** | **Двоичное** | **Описание** |
|------------|-----------|--------------|--------------|
| `Ми-26`, `МИ26Т` | 128 | `0b10000000` | Тяжелый транспортный вертолет |
| `Ми-17`, `МИ171`, `171А2`, `МИ171Е` | 64 | `0b01000000` | Транспортно-боевой вертолет |
| `Ми-8Т`, `МИ8МТВ`, `МИ8`, `МИ8АМТ` | 32 | `0b00100000` | Многоцелевой вертолет |
| `КА32Т` | 16 | `0b00010000` | Палубный вертолет Камов |
| `350B3` | 8 | `0b00001000` | Airbus H350 |
| `355NP`, `355N` | 4 | `0b00000100` | Airbus H355 |
| `R44`, `R44I`, `R44II` | 2 | `0b00000010` | Robinson R44 |



### СКРИПТ 6: `calculate_beyond_repair.py` (исправлено 28-07-2025)

| **Характеристика** | **Значение** |
|-------------------|--------------|
| **Порядковый номер** | 6 (шестой в Extract пайплайне) |
| **Создает таблицу СУБД** | ❌ (обогащает существующую `md_components`) |
| **Создает датафрейм** | ❌ (работает с ClickHouse напрямую) |
| **Источник данных** | Поля `md_components`: `repair_price`, `purchase_price`, `ll_mi8/mi17`, `oh_threshold_mi8/mi17`, **`ac_type_mask`** |
| **Назначение** | Расчет экономического порога списания (Beyond Repair) — теперь по типам и в минутах |
| **Операция** | `ALTER TABLE md_components UPDATE` |

#### Обогащаемое поле:

| **№** | **Поле** | **Тип ClickHouse** | **Тип cuDF** | **MacroProperty** | **Источник** | **Описание** |
|-------|----------|-------------------|--------------|-------------------|--------------|--------------|
| 1 | `br_mi8`/`br_mi17` | `Nullable(UInt32)` | `uint32` | ✅ MacroProperty1 | **Расчет** | Beyond Repair по типам (минуты) — пороговая наработка для списания |

#### Формула расчета Beyond Repair:

**Математическая формула:**
```
BR = NR - (RepairPrice / ((PurchasePrice - RepairPrice) / NR + RepairPrice / MRR))
```

**Где:**
- `NR` = Назначенный ресурс (`ll_mi8` или `ll_mi17`)
- `MRR` = Межремонтный ресурс (`oh_threshold_mi8` или `oh_threshold_mi17`) 
- `RepairPrice` = Стоимость ремонта (`repair_price`)
- `PurchasePrice` = Стоимость покупки (`purchase_price`)

Единицы в таблице: минуты. Формула считается в часах и умножается на 60 с округлением до минут. Ограничения: `repair_price >= purchase_price` → 0; зажатие [0, 60*NR].

### СКРИПТ 7: `md_components_enricher.py`

| **Характеристика** | **Значение** |
|-------------------|--------------|
| **Порядковый номер** | 7 (седьмой в Extract пайплайне) |
| **Создает таблицу СУБД** | ❌ (обогащает существующую `md_components`) |
| **Создает датафрейм** | ❌ (работает с ClickHouse напрямую) |
| **Источник данных** | `dict_partno_flat` (словарь партномеров с реальными ID из AMOS) |
| **Назначение** | Обогащение поля `partno_comp` цифровыми ID партномеров |
| **Операция** | `ALTER TABLE md_components UPDATE` |

#### Обогащаемое поле:

| **№** | **Поле** | **Тип ClickHouse** | **Тип cuDF** | **MacroProperty** | **Источник** | **Описание** |
|-------|----------|-------------------|--------------|-------------------|--------------|--------------|
| 1 | `partno_comp` | `Nullable(UInt32)` | `uint32` | ✅ MacroProperty1 | **Словарь** | Component ID партномера из AMOS (partseqno_i) |

#### Логика обогащения:

**Источник данных:**
```sql
SELECT partseqno_i, partno FROM dict_partno_flat
```


**Назначение `partno_comp`:**
- Связывает `md_components.partno` с `heli_pandas.partseqno_i`
- Используется в `repair_days_calculator.py` для поиска `repair_time`
- Обеспечивает единую нумерацию партномеров в Flame GPU MacroProperty1

### СКРИПТ 8: `repair_days_calculator.py` (исправлено 28-07-2025)

| **Характеристика** | **Значение** |
|-------------------|--------------|
| **Порядковый номер** | 12 (перемещен в конец пайплайна) |
| **Создает таблицу СУБД** | ❌ (обогащает существующую `heli_pandas`) |
| **Создает датафрейм** | ❌ (работает с ClickHouse напрямую) |
| **Источник данных** | `md_components.repair_time`, `heli_pandas.target_date`, `status_overhaul.sched_end_date` |
| **Назначение** | Расчет дней ремонта для ВС в статусе 4 (Ремонт) |
| **Операция** | `ALTER TABLE heli_pandas UPDATE` |

#### Обогащаемое поле:

| **№** | **Поле** | **Тип ClickHouse** | **Тип cuDF** | **MacroProperty** | **Источник** | **Описание** |
|-------|----------|-------------------|--------------|-------------------|--------------|--------------|
| 1 | `repair_days` | `Nullable(UInt16)` | `uint16` | ✅ MacroProperty3 | **Расчет** | Количество дней ремонта (было отрицательное исправлено) |

#### Формула расчета repair_days:

**Математическая формула:**
```
repair_days = repair_time - (sched_end_date - version_date)
```

**Где:**
- `repair_time` = Нормативное время ремонта из `md_components` (дни)
- `sched_end_date` = Плановая дата окончания ремонта из `target_date` в `heli_pandas`
- `version_date` = Текущая дата версии данных

**Логика расчета:**
1. Выбираются только ВС в статусе 4 (Ремонт)
2. Поиск `repair_time` по `partno_comp` в `md_components`
3. Если `repair_time` найдено → расчет по формуле
4. Если `repair_time` не найдено → `repair_days = NULL`

### СКРИПТ 9: `dictionary_creator.py` (исправлено 28-07-2025)

| **Характеристика** | **Значение** |
|-------------------|--------------|
| **Порядковый номер** | 8 (переместился после исправления repair_days) |
| **Создает таблицы СУБД** | ✅ `dict_partno_flat`, `dict_serialno_flat`, `dict_owner_flat`, `dict_ac_type_flat`, `dict_status_flat`, `dict_aircraft_number_flat` |
| **Создает датафрейм** | ❌ (работает с ClickHouse напрямую) |
| **Источник данных** | DISTINCT значения из `heli_pandas` и `md_components` |
| **Назначение** | Создание всех словарей для аналитики и dictGet операций |
| **ENGINE** | `MergeTree()` (аддитивные) |

#### Создаваемые словарные таблицы:

| **№** | **Таблица** | **Ключ** | **Значение** | **MacroProperty** | **Тип** | **Описание** |
|-------|-------------|----------|--------------|-------------------|---------|--------------|
| 1 | `dict_partno_flat` | `partseqno_i` | `partno` | ❌ | Аддитивная | Партномера компонентов |
| 2 | `dict_serialno_flat` | `psn` | `serialno` | ❌ | Аддитивная | Серийные номера |
| 3 | `dict_owner_flat` | `address_i` | `owner` | ❌ | Аддитивная | Владельцы ВС |
| 4 | `dict_ac_type_flat` | `ac_type_mask` | `ac_typ` | ❌ | Аддитивная | Типы ВС с битовыми масками |
| 5 | `dict_status_flat` | `status_id` | `status_name` | ❌ | Не-аддитивная | Статусы компонентов |
| 6 | `dict_aircraft_number_flat` | `aircraft_number` | `ac_type_mask` | ❌ | Не-аддитивная | Номера ВС с масками типов |

#### Словарь 1: `dict_partno_flat`

| **Поле** | **Тип** | **Назначение** |
|----------|---------|----------------|
| `partseqno_i` | `UInt32` | ID партномера из Excel (ключ) |
| `partno` | `String` | Чертежный номер компонента |
| `load_timestamp` | `DateTime` | Время загрузки записи |

**Источник:** `SELECT DISTINCT partseqno_i, partno FROM heli_pandas`
**Логика:** Аддитивное накопление, MergeTree

#### Словарь 2: `dict_serialno_flat`

| **Поле** | **Тип** | **Назначение** |
|----------|---------|----------------|
| `psn` | `UInt32` | ID серийного номера из Excel (ключ) |
| `serialno` | `String` | Серийный номер компонента |
| `load_timestamp` | `DateTime` | Время загрузки записи |

**Источник:** `SELECT DISTINCT psn, serialno FROM heli_pandas`
**Логика:** Аддитивное накопление, MergeTree

#### Словарь 3: `dict_owner_flat`

| **Поле** | **Тип** | **Назначение** |
|----------|---------|----------------|
| `address_i` | `UInt32` | ID владельца из Excel (ключ) |
| `owner` | `String` | Наименование владельца ВС |
| `load_timestamp` | `DateTime` | Время загрузки записи |

**Источник:** `SELECT DISTINCT address_i, owner FROM heli_pandas`
**Логика:** Аддитивное накопление, MergeTree

#### Словарь 4: `dict_ac_type_flat`

| **Поле** | **Тип** | **Назначение** |
|----------|---------|----------------|
| `ac_type_mask` | `UInt8` | Битовая маска типа ВС (ключ) |
| `ac_typ` | `String` | Название типа ВС |
| `load_timestamp` | `DateTime` | Время загрузки записи |

**Источник:** `SELECT DISTINCT ac_typ FROM heli_pandas` + **ХАРДКОД масок**
**Логика:** Аддитивное накопление + битовые маски, MergeTree
**Хардкод:** МИ-8→32, МИ-17→64, МИ-26→128, Airbus→2/4/8/16

#### Словарь 5: `dict_status_flat`

| **Поле** | **Тип** | **Назначение** |
|----------|---------|----------------|
| `status_id` | `UInt8` | ID статуса компонента (ключ) |
| `status_name` | `String` | Название статуса |

**Источник:** **ХАРДКОД констант** (статические значения)
**Логика:** Полная перезапись, MergeTree
**Хардкод:** 1-Неактивно, 2-Эксплуатация, 3-Исправен, 4-Ремонт, 5-Резерв, 6-Хранение

#### Словарь 6: `dict_aircraft_number_flat`

| **Поле** | **Тип** | **Назначение** |
|----------|---------|----------------|
| `aircraft_number` | `UInt32` | Номер ВС (ключ) |
| `ac_type_mask` | `UInt8` | Битовая маска типа ВС |

**Источник:** `SELECT DISTINCT aircraft_number FROM heli_pandas` + обогащение масками
**Логика:** Полная перезапись, MergeTree

---

### СКРИПТ 10: `program_fl_direct_loader.py` (исправлено 28-07-2025)

| **Характеристика** | **Значение** |
|-------------------|--------------|
| **Порядок** | 9 (переместился после исправления repair_days) |
| **Таблица в СУБД** | ✅ Создает `flight_program_fl` (MergeTree) |
| **DataFrame** | ✅ Создает `df` (pandas из Excel `Program.xlsx`, лист "2025") |
| **Зависимости** | `dict_aircraft_number_flat` |
| **Источник Excel** | `Program.xlsx` |
| **Назначение** | Прямое создание тензора программы полетов на 4000 дней (~1.1M записей) |

#### Поля таблицы `flight_program_fl`:

| **Поле** | **Тип ClickHouse** | **Тип cuDF** | **MacroProperty5** | **Назначение** |
|----------|-------------------|--------------|-------------------|----------------|
| `aircraft_number` | `UInt16` | `uint16` | ✅ | Номер ВС |
| `dates` | `Date` | `Date` | ✅ | Дата программы полетов |
| `daily_hours` | `UInt32` | `uint32` | ✅ | Дневной налет в минутах |
| `ac_type_mask` | `UInt8` | `uint8` | ✅ | Битовая маска типа ВС |
| `version_date` | `Date` | `Date` | ❌ | Дата версии данных |
| `version_id` | `UInt8` | `uint8` | ❌ | ID версии |

#### Логика обработки:

- **Приоритеты:** 1-serialno (экземпляры), 2-ac_type_mask (типы)

**Размножение данных:**
- **Базовая дата:** последняя `version_date` из `heli_pandas`
- **Период:** 4000 дней вперед
- **Заполнение:** если нет данных на дату → берем по дню/месяцу из последнего известного года

**Валидация тензора:**
- Проверка планеров по serialno
- Проверка общего количества заполненных планеров
- Отсутствие пустых и нулевых полей в массиве

---

### СКРИПТ 11: `program_ac_direct_loader.py` (исправлено 28-07-2025)

| **Характеристика** | **Значение** |
|-------------------|--------------|
| **Порядок** | 10 (переместился после исправления repair_days) |
| **Таблица в СУБД** | ✅ Создает `flight_program_ac` (MergeTree) |
| **DataFrame** | ✅ Создает `df` (pandas из Excel `Program_heli.xlsx`, лист "2025") |
| **Зависимости** | `heli_pandas`, `md_components` |
| **Источник Excel** | `Program_heli.xlsx` |
| **Назначение** | Прямое создание тензора программы операций ВС на 4000 дней с постпроцессингом |

#### Поля таблицы `flight_program_ac`:

| **Поле** | **Тип ClickHouse** | **Тип cuDF** | **MacroProperty4** | **Назначение** |
|----------|-------------------|--------------|-------------------|----------------|
| `dates` | `Date` | `Date` | ✅ | Дата программы операций |
| `ops_counter_mi8` | `UInt16` | `uint16` | ✅ | Счетчик операций МИ-8 |
| `ops_counter_mi17` | `UInt16` | `uint16` | ✅ | Счетчик операций МИ-17 |
| `ops_counter_total` | `UInt16` | `uint16` | ✅ | Общий счетчик операций (вычисляемое) |
| `new_counter_mi17` | `UInt8` | `uint8` | ✅ | Новые поставки МИ-17 |
| `trigger_program_mi8` | `Int8` | `int8` | ✅ | Триггер программы МИ-8 |
| `trigger_program_mi17` | `Int8` | `int8` | ✅ | Триггер программы МИ-17 |
| `trigger_program` | `Int8` | `int8` | ✅ | Общий триггер программы (вычисляемое) |
| `version_date` | `Date` | `Date` | ❌ | Дата версии данных |
| `version_id` | `UInt8` | `uint8` | ❌ | ID версии |

#### Логика обработки:

**Источники данных Excel:**
- **Строка с "Год":** годы по месяцам/колонкам
- **`ops_counter_*`:** количество ВС в программе (равномерно по дням месяца)
- **`new_counter_*`:** новые поставки (только в последний день месяца)

**Постпроцессинг:**
- **`ops_counter_total`** = `ops_counter_mi8` + `ops_counter_mi17`
- **`trigger_program_*`** = триггер изменения программы относительно предыдущего дня
- **Корректировка первых значений** по фактическим компонентам в статусе 2 из `heli_pandas`

**Валидация:**
- Проверка 4000 дат
- Статистика по всем полям
- Контроль вычисляемых полей

---

### СКРИПТ: `program_ac_precheck_runner.py` (добавлено 04-09-2025)

| **Характеристика** | **Значение** |
|-------------------|--------------|
| **Порядок** | 12 (после FL и group_by) |
| **Таблица в СУБД** | ❌ Не создает (обновляет `heli_pandas.status_id`) |
| **DataFrame** | ✅ Загружает `heli_pandas` в память |
| **Зависимости** | `heli_pandas`, `md_components`, `flight_program_fl` |
| **Назначение** | Безопасный D1 precheck для записей `status_id=2` |

#### Логика

- Читает D1 `daily_hours` из `flight_program_fl` по `aircraft_number`.
- Для `group_by∈{1,2}` и `status_id=2` рассчитывает остатки `ll/oh` на вечер D0.
- Если остаток < D1, корректирует `status_id` на 6 (хранение) или 4 (ремонт) с учетом BR (`br_mi8/br_mi17`).
- При отсутствии зависимостей шаг пропускается, обеспечивая устойчивость первичной загрузки.

#### Результат

- Точечные `ALTER ... UPDATE` по `serialno` в `heli_pandas`.
- Разрывает цикл ожидания FL на ранних этапах: precheck выполняется после формирования FL.

### СКРИПТ 12: `digital_values_dictionary_creator.py` (исправлено 28-07-2025)

| **Характеристика** | **Значение** |
|-------------------|--------------|
| **Порядок** | 11 (переместился после исправления repair_days) |
| **Таблица в СУБД** | ✅ Создает `dict_digital_values_flat` (MergeTree) + Dictionary объект |
| **DataFrame** | ❌ Не создает (работает с метаданными СУБД) |
| **Зависимости** | `heli_pandas`, `md_components`, `flight_program_ac`, `flight_program_fl` |
| **Источник данных** | Метаданные ClickHouse (`DESCRIBE TABLE`) |
| **Назначение** | Создание аддитивного мета-словаря всех полей Extract системы для Flame GPU |

#### Поля таблицы `dict_digital_values_flat`:

| **Поле** | **Тип ClickHouse** | **Тип cuDF** | **Назначение** |
|----------|-------------------|--------------|----------------|
| `field_id` | `UInt16` | `uint16` | Уникальный ID поля (1-65535) |
| `primary_table` | `String` | `string` | Основная таблица поля |
| `field_name` | `String` | `string` | Название поля |
| `field_description` | `String` | `string` | Описание назначения поля |
| `data_type` | `String` | `string` | Реальный тип данных ClickHouse |
| `is_nullable` | `UInt8` | `uint8` | Может ли быть NULL (0/1) |
| `load_timestamp` | `DateTime` | `datetime` | Время загрузки (аддитивность) |

#### Логика обработки:

**Анализ метаданных:**
- **Сканирование таблиц:** все 12 Extract таблиц через `DESCRIBE TABLE`
- **DISTINCT поля:** уникальные имена полей с реальными типами из ClickHouse
- **Аддитивность:** новые поля добавляются, существующие сохраняются

**Dictionary объект:**
- **`digital_values_dict_flat`** для быстрого `dictGet()` доступа
- **PRIMARY KEY:** `field_id`
- **LAYOUT:** FLAT() для максимальной производительности

**Использование:**
- **Flame GPU macroproperty:** маппинг полей по `field_id`
- **Аналитические запросы:** `dictGet('digital_values_dict_flat', 'field_description', field_id)`
- **Direct join:** прямое соединение по `field_id`

---

## Краткая матрица чтение/запись по этапам Extract (таблица, 04-09-2025)
| Этап | Скрипт | Читает | Пишет | Примечание |
|-----:|--------|--------|-------|-----------|
| 1 | `md_components_loader.py` | Excel MD_Components | `md_components` (база, ресурсы, цены, version_*) | База для фильтрации в Этапе 4 |
| 2 | `status_overhaul_loader.py` | Excel Status_Overhaul | `status_overhaul` | Источник ремонтов |
| 3 | `program_ac_loader.py` | Excel Program_AC | `program_ac` | Источник эксплуатации |
| 4 | `dual_loader.py` | Excel Status_Components; `md_components`; `status_overhaul`; `program_ac` | `heli_raw`; `heli_pandas` (status_id, repair_days init, aircraft_number, …) | Центральная таблица |
| 5 | `enrich_heli_pandas.py` | `heli_pandas` | `heli_pandas.ac_type_mask` | До словарей/тензоров |
| 6 | `calculate_beyond_repair.py` | `md_components` | `md_components.br_mi8/br_mi17` | BR в минутах |
| 7 | `md_components_enricher.py` | `dict_partno_flat`; `md_components` | `md_components.partno_comp` | Связь MP1↔MP3 |
| 8 | `dictionary_creator.py` | `heli_pandas`; `md_components` | `dict_*` таблицы | Словари для join/dictGet |
| 9 | `program_fl_direct_loader.py` | `dict_aircraft_number_flat`; Excel Program.xlsx | `flight_program_fl` | Нужен для precheck |
| 10 | `program_ac_direct_loader.py` | `heli_pandas`; `md_components`; Excel Program_heli.xlsx | `flight_program_ac` | Независим от precheck |
| 11 | `heli_pandas_group_by_enricher.py` | `md_components`; `heli_pandas` | `heli_pandas.group_by` | Для фильтрации планеров/агрегатов |
| 12 | `heli_pandas_component_status.py` | `heli_pandas` (group_by, aircraft_number, condition) | `heli_pandas.status_id=2` (агрегаты на ВС) | После group_by |
| 13 | `heli_pandas_serviceable_status.py` | `heli_pandas` (group_by, condition, status_id) | `heli_pandas.status_id=3` (исправные агрегаты) | После component_status |
| 14 | `digital_values_dictionary_creator.py` | DESCRIBE всех таблиц Extract | `dict_digital_values_flat` (+Dictionary) | После всех таблиц |
| 15 | `repair_days_calculator.py` | `md_components.repair_time`; `heli_pandas`; `status_overhaul` | `heli_pandas.repair_days` | Финальный расчёт |

## СВОДКА ВЕРСИОННОСТИ ТАБЛИЦ СУБД

### Версионность таблиц ClickHouse в Extract системе

#### ОСНОВНЫЕ ТАБЛИЦЫ ДАННЫХ (7 таблиц)

| **Таблица СУБД** | **Версионные поля** | **Статус** | **Тип данных** | **Назначение** |
|------------------|---------------------|------------|----------------|----------------|
| `heli_pandas` | ✅ `version_date`, `version_id` | **ВЕРСИОНИРОВАНА** | `Date`, `UInt8` | **КОРНЕВАЯ ТАБЛИЦА** компонентов |
| `heli_raw` | ✅ `version_date`, `version_id` | **ВЕРСИОНИРОВАНА** | `Date`, `UInt8` | Сырые данные компонентов |
| `md_components` | ✅ `version_date`, `version_id` | **ВЕРСИОНИРОВАНА** | `Date`, `UInt8` | Справочник компонентов |
| `status_overhaul` | ✅ `version_date`, `version_id` | **ВЕРСИОНИРОВАНА** | `Date`, `UInt8` | Статусы капремонтов |
| `program_ac` | ✅ `version_date`, `version_id` | **ВЕРСИОНИРОВАНА** | `Date`, `UInt8` | Реестр ВС в эксплуатации |
| `flight_program_fl` | ✅ `version_date`, `version_id` | **ВЕРСИОНИРОВАНА** | `Date`, `UInt8` | Тензор программ полетов |
| `flight_program_ac` | ✅ `version_date`, `version_id` | **ВЕРСИОНИРОВАНА** | `Date`, `UInt8` | Тензор операций ВС |

#### АДДИТИВНЫЕ СЛОВАРИ (6 таблиц) - ОБНОВЛЕНО 27-07-2025

| **Таблица СУБД** | **Версионные поля** | **Статус** | **Поле времени** | **Назначение** |
|------------------|---------------------|------------|------------------|----------------|
| `dict_partno_flat` | ✅ `version_date`, `version_id` | **ВЕРСИОНИРОВАНА** | `load_timestamp` | Аддитивный словарь партномеров |
| `dict_serialno_flat` | ✅ `version_date`, `version_id` | **ВЕРСИОНИРОВАНА** | `load_timestamp` | Аддитивный словарь серийников |
| `dict_owner_flat` | ✅ `version_date`, `version_id` | **ВЕРСИОНИРОВАНА** | `load_timestamp` | Аддитивный словарь владельцев |
| `dict_ac_type_flat` | ✅ `version_date`, `version_id` | **ВЕРСИОНИРОВАНА** | `load_timestamp` | Аддитивный словарь типов ВС |
| `dict_aircraft_number_flat` | ✅ `version_date`, `version_id` | **ВЕРСИОНИРОВАНА** | `load_timestamp` | Аддитивный словарь номеров ВС |
| `dict_digital_values_flat` | ✅ `version_date`, `version_id` | **ВЕРСИОНИРОВАНА** | `load_timestamp` | Аддитивный мета-словарь полей |

#### НЕ-АДДИТИВНЫЕ СЛОВАРИ (1 таблица)

| **Таблица СУБД** | **Версионные поля** | **Статус** | **Поле времени** | **Назначение** |
|------------------|---------------------|------------|------------------|----------------|
| `dict_status_flat` | ❌ НЕТ полей | **ПЕРЕЗАПИСЫВАЕТСЯ** | `load_timestamp` | Словарь статусов (пересоздается каждый раз) |

### Принципы версионности Extract:

**✅ ОСНОВНЫЕ ТАБЛИЦЫ ДАННЫХ (7 из 14 таблиц):**
- Все содержат поля `version_date` (Date) и `version_id` (UInt8)
- Корневая таблица `heli_pandas` - полная версионность
- Поддержка множественных загрузок в рамках одного цикла ETL
- Партиционирование по `toYYYYMM(version_date)` для производительности

**✅ АДДИТИВНЫЕ СЛОВАРИ (6 из 14 таблиц) - ВЕРСИОННОСТЬ ДОБАВЛЕНА 27-07-2025:**
- Добавлены поля `version_date` и `version_id` для единообразия архитектуры
- Сохранен накопительный дизайн + версионная привязка к `heli_pandas`
- `load_timestamp` + версионные поля для полной трассируемости
- Защищены от удаления в ТЕСТ режиме Extract

**✅ НЕ-АДДИТИВНЫЕ СЛОВАРИ (1 из 14 таблиц):**
- `dict_status_flat` - **ЯДРО АРХИТЕКТУРЫ МОДЕЛИ** 
- БЕЗ версионности (не предполагает изменений)
- Статический справочник с фиксированными значениями
- Пересоздается каждый Extract цикл

**🔧 ОБОГАЩЕНИЕ ВНУТРИ ЦИКЛА:**
- UPDATE операции (ac_type_mask, br_mi8/br_mi17, partno_comp, repair_days) выполняются внутри одного Extract цикла
- НЕ требуют отдельной версионности - используют уже установленные поля `version_date`/`version_id`
- Дополнения к таблицам в рамках одного цикла логически связаны

**📋 ЗАКЛЮЧЕНИЕ ЕДИНОГО ПАЙПЛАЙНА ВЕРСИОННОСТИ (27-07-2025):**
- **Архитектура ЗАВЕРШЕНА**: ВСЕ основные данные версионированы (100% критичных таблиц)
- **Словари УНИФИЦИРОВАНЫ**: 6 аддитивных версионированных + 1 перезаписываемый
- **Источник ЦЕНТРАЛИЗОВАН**: единая версия из `Status_Components.xlsx`
- **Цепочка НАДЕЖНАЯ**: Extract Master → CLI параметры → heli_pandas → словари
- **Общий объем**: 14 таблиц в полностью синхронизированной Extract системе

---


