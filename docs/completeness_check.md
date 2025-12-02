# Проверка комплектности вертолётов (Completeness Check)

## Связи документации
- **Источник правил**: `.cursorrules`
- **История изменений**: `docs/changelog.md`
- **Номенклатура агрегатов**: `md_components` (СУБД)

---

## Назначение

Аналитический слой проверки комплектности вертолётов Mi-8/Mi-17 в эксплуатации (`status_id=2`).

**Задачи:**
1. Проверка наличия всех обязательных агрегатов на каждом планере
2. Сравнение с нормами из справочника `md_components`
3. Выявление дефицитов (недостающих агрегатов)
4. Формирование отчётов для принятия решений

---

## Входные данные

### Таблица `heli_pandas`
| Поле | Тип | Описание |
|------|-----|----------|
| `aircraft_number` | UInt32 | Бортовой номер планера |
| `partseqno_i` | UInt32 | Код позиции агрегата (ссылка на md_components.partno_comp) |
| `partno` | String | Наименование агрегата |
| `group_by` | UInt8 | Группа агрегата (1=Mi-8 планер, 2=Mi-17 планер, 3+=агрегаты) |
| `status_id` | UInt8 | Статус (2=эксплуатация) |
| `ac_type_mask` | UInt8 | Маска типа ВС (32=Mi-8, 64=Mi-17, 96=оба) |
| `condition` | String | Техническое состояние |
| `serialno` | String | Серийный номер агрегата |

### Таблица `md_components`
| Поле | Тип | Описание |
|------|-----|----------|
| `partno_comp` | UInt32 | Код позиции (ключ) |
| `partno` | String | Наименование |
| `group_by` | UInt8 | Группа агрегата |
| `comp_number` | UInt8 | **Норма** — требуемое количество на борт |
| `ac_type_mask` | UInt8 | Применимость к типам ВС |

---

## Алгоритм проверки

### Шаг 1: Выборка планеров в эксплуатации
```sql
SELECT DISTINCT aircraft_number, group_by, ac_type_mask
FROM heli_pandas
WHERE status_id = 2 AND group_by IN (1, 2)
```

**Результат:** Список планеров Mi-8 (group_by=1) и Mi-17 (group_by=2)

### Шаг 2: Загрузка норм из md_components
```sql
SELECT group_by, partno_comp, partno, comp_number, ac_type_mask
FROM md_components
WHERE group_by > 2
ORDER BY group_by
```

**Ключевой принцип:** Норма определяется по **группе** (`group_by`), а не по конкретной позиции (`partno_comp`).

Для каждой группы берётся `MAX(comp_number)` — это требуемое количество агрегатов данной группы на борту.

### Шаг 3: Подсчёт установленных агрегатов по группам
```sql
SELECT 
    aircraft_number,
    group_by,
    COUNT(*) as installed_count
FROM heli_pandas
WHERE aircraft_number IN (<список планеров>)
  AND group_by > 2
GROUP BY aircraft_number, group_by
```

### Шаг 4: Сравнение с нормами
```
Для каждого планера P и каждой группы G:
    норма = md_components.comp_number для группы G (с учётом ac_type_mask)
    установлено = COUNT агрегатов группы G на планере P
    delta = установлено - норма
    
    если delta < 0:
        ДЕФИЦИТ: не хватает |delta| агрегатов группы G
```

### Шаг 5: Фильтрация по ac_type_mask
Норма применяется к планеру только если:
```
ac_type_mask == 0  -- применимо ко всем типам
OR
(ac_type_mask & plane_ac_type_mask) > 0  -- битовое совпадение
```

| ac_type_mask | Применимость |
|--------------|--------------|
| 0 | Все типы ВС |
| 32 | Только Mi-8 |
| 64 | Только Mi-17 |
| 96 | Mi-8 и Mi-17 |

---

## Номенклатура агрегатов

### Группы по системам вертолёта

| Группа | Норма | Тип ВС | Название | Система |
|--------|-------|--------|----------|---------|
| 1-2 | 1 | Планер | МИ-8/МИ-17 | Планер |
| 3-4 | 2 | Двигатель | ТВ2-117/ТВ3-117 | Силовая установка |
| **5** | 1 | Оба | 8-1950/1960-000 | **Автомат перекоса** |
| 6 | 5 | Оба | 8АТ-2710 | Колонка НВ |
| 7-9 | 1 | Оба | Колонки управления | Управление |
| 10-11 | 1 | Оба | Панели | Управление |
| 12-13 | 1 | Mi-8/17 | ВР-8А/ВР-14 | Главный редуктор |
| 14-16 | 1 | Mi-17 | АИ-9В/SAFIR/ТА-14 | ВСУ |
| 17-21 | 1 | Разные | Хвостовой редуктор | Трансмиссия |
| **22** | **3** | Оба | **КАУ-30Б** | **Гидравлика** |
| 23 | 1 | Оба | РА-60Б | Гидравлика |
| 24 | 4 | Mi-17 | КАУ-115АМ | Гидравлика |
| **25** | 1 | Оба | **8А-4101-00Б-1** | **Вал трансмиссии** |
| **26** | 1 | Оба | **8А-4101-00Б-2** | **Вал трансмиссии** |
| 27 | 1 | Оба | 8А-4201-00А | Вал привода |
| 28-31 | 1 | Mi-17 | Топливные агрегаты | Топливная |
| 32-34 | 1 | Разные | Хвостовая трансмиссия | Трансмиссия |
| 35-37 | 2 | Разные | АГБ (авиагоризонты) | Приборы |
| 38 | 1 | Оба | АК-50Т1 | Аккумулятор |
| **39** | 1 | Оба | **АРМ-406** | **Аварийный радиомаяк** |
| 40-42 | 2 | Разные | НР (насосы-регуляторы) | Топливная |

---

## Скрипты проверки

### 1. `heli_pandas_ops_inventory.py`
**Назначение:** Инвентаризация агрегатов на планерах в эксплуатации

**Запуск:**
```bash
python3 code/heli_pandas_ops_inventory.py [--skip-md]
```

**Выход:**
- Консольный отчёт: количество компонентов на каждом борту
- Markdown: `docs/heli_pandas_ops_inventory_<version>.md`

### 2. `heli_pandas_ops_other_groups.py`
**Назначение:** Проверка комплектности по группам агрегатов (group_by > 2)

**Запуск:**
```bash
python3 code/heli_pandas_ops_other_groups.py [--skip-md]
```

**Выход:**
- Консольный отчёт с дефицитами (delta < 0)
- PDF: `docs/heli_pandas_ops_other_groups_<version>.pdf`
- Markdown: `docs/heli_pandas_ops_other_groups_<version>.md`

**Формат вывода дефицитов:**
```
   24120  Т       1988-07-14     32     32      1   -1  **22:2/3(КАУ-30Б:3)**
         │       │              │      │       │    │   └── группа:установлено/норма(название:норма)
         │       │              │      │       │    └── delta (отклонение)
         │       │              │      │       └── optional (опциональных)
         │       │              │      └── norm (норма)
         │       │              └── inst (установлено)
         │       └── mfg_date (дата производства)
         └── variant (модификация)
```

---

## Порядок проверки комплектности

### Быстрая проверка (только дефициты)
```bash
cd "/home/budnik_an/cube linux/cube"
source config/load_env.sh
python3 code/heli_pandas_ops_other_groups.py --skip-md 2>&1 | grep "\*\*"
```

### Полная проверка
```bash
cd "/home/budnik_an/cube linux/cube"
source config/load_env.sh

# 1. Инвентаризация (общая статистика)
python3 code/heli_pandas_ops_inventory.py --skip-md

# 2. Проверка комплектности по группам (детальная)
python3 code/heli_pandas_ops_other_groups.py
```

---

## Типичные дефициты и причины

| Тип дефицита | Пример | Причина | Решение |
|--------------|--------|---------|---------|
| **Неверный partseqno_i** | AC 24479: Б-2 с partseqno=35713 (должен 35714) | Ошибка в исходных данных Excel | Исправить partseqno_i в Excel |
| **Отсутствует агрегат** | AC 22571: нет группы 5 (автомат перекоса) | Агрегат не привязан к борту | Добавить запись в heli_pandas |
| **Недокомплект** | AC 24120: КАУ-30Б 2/3 | Установлено меньше нормы | Установить недостающий агрегат |

---

## Инварианты проверки

### INV-COMP-1: Все планеры в эксплуатации должны быть укомплектованы
```sql
-- Проверка: нет дефицитов по обязательным группам
SELECT aircraft_number, group_by, installed, required, installed - required as delta
FROM (
    -- подзапрос сравнения установленных с нормами
)
WHERE delta < 0
-- Ожидается: 0 строк (или только известные исключения)
```

### INV-COMP-2: partseqno_i должен соответствовать partno
```sql
-- Проверка: partseqno_i в heli_pandas соответствует md_components
SELECT hp.aircraft_number, hp.partseqno_i, hp.partno, md.partno as expected_partno
FROM heli_pandas hp
LEFT JOIN md_components md ON hp.partseqno_i = md.partno_comp
WHERE hp.partno != md.partno
-- Ожидается: 0 строк
```

### INV-COMP-3: group_by в heli_pandas соответствует md_components
```sql
-- Проверка: group_by корректно присвоен из md_components
SELECT hp.aircraft_number, hp.partseqno_i, hp.group_by as hp_group, md.group_by as md_group
FROM heli_pandas hp
JOIN md_components md ON hp.partseqno_i = md.partno_comp
WHERE hp.group_by != md.group_by AND hp.group_by > 2
-- Ожидается: 0 строк
```

---

## Интеграция с симуляцией

После исправления дефицитов в исходных данных:

1. **Перезагрузить данные (Extract):**
   ```bash
   python3 code/extract_master.py --mode TEST
   ```

2. **Запустить симуляцию (Transform):**
   ```bash
   python3 orchestrator_v2.py --modules ... --steps 3650 --enable-mp2 --enable-mp2-postprocess --drop-table
   ```

3. **Повторить проверку комплектности:**
   ```bash
   python3 code/heli_pandas_ops_other_groups.py --skip-md 2>&1 | grep "\*\*"
   ```

---

## Версия документа
- **Создано:** 02-12-2025
- **Автор:** AI Assistant + Алексей
- **Статус:** Готово для продакшена

