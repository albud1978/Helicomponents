# Validation Rules

## Назначение
Методология валидации симуляции и данных `heli_pandas`.

**SSoT инвариантов**: `config/transitions/invariants.json` — формализованный реестр всех инвариантов (INV-1..INV-9), temporal-контрактов (TEMP-1..TEMP-4) и GPU-ограничений (GPU-1..GPU-6).

Данный документ описывает **методологию** (SQL-first, JIT-правила, типы данных), а не перечень инвариантов.

Источники: `docs/archive/validation_legacy.md`, `docs/archive/data_validation_legacy.md`.

## Принципы валидации
- **SQL-first**: итоговая проверка — только по данным СУБД (MP2 экспорт).
- **Логирование в RTC** — только по явному согласованию; после фикса логирование удаляется и тест повторяется.
- **Критерий успеха**: результаты подтверждены в СУБД без избыточных логов.
- **JIT warning'и запрещены**: при каждой компиляции проверять NVRTC лог.
- **Данные только реальные**: синтетика/заглушки — только по явному разрешению владельца.
- **Baseline sim_masterv2 заморожен**: сравнение с baseline — только по явному запросу.

## NVRTC / JIT правила
**Правило:** warning'и в JIT логе недопустимы.

Типичные warning'и:
| Warning | Причина | Решение |
|---------|---------|---------|
| `#117-D: non-void function should return a value` | `return;` вместо `return flamegpu::ALIVE;` | Заменить `return;` на `return flamegpu::ALIVE;` |
| `#177-D: variable was declared but never referenced` | Неиспользуемая переменная | Удалить или использовать |

**Действия при обнаружении:**
1. Исправить RTC код.
2. Очистить кэш изменённых ядер (или полностью: `rm -rf .rtc_cache/*`).
3. Повторить компиляцию и проверить лог.

## Методология валидации
- Добавлять модули по одному.
- Делать полный прогон на 3650 дней (для компиляции всех ядер).
- Проверять инварианты SQL-запросами к СУБД.
- Процесс: факты → гипотеза → согласование → изменение.

## Источники данных и типы
- **MP5**: линейный массив `(DAYS + 1) * FRAMES` в `MacroProperty mp5_lin`.
- **MP3**: пороги `ll/oh/br` сопоставлены по `frames_index`.
- **MP1**: нормы `mp1_br_*`, `mp1_oh_*`, `mp1_ll_mi17`, `mp1_second_ll` (UInt32, минуты).
- `second_ll`: пустые значения заполняются sentinel `0xFFFFFFFF`, чтобы отличать NULL от 0.

## Базовые инварианты
- Индексация MP5: `row = day * FRAMES + idx`, `row_next = row + FRAMES`.
- `mp5_lin` — read-only после инициализации.
- Типы согласованы: UInt32 для MP5 и агентных накопителей.

### Status 2 (operations)
- Инкременты: `sne += dt`, `ppr += dt`, где `dt = mp5(day, idx)`.
- Прогноз: `s_next = sne + dn`, `p_next = ppr + dn`.
- Переходы:
  - 2→6 при `s_next >= ll` (LL порог).
  - 2→4 при `p_next >= oh` и `s_next < br` (ветка ремонта).

### Status 4 (repair)
- `repair_days` растёт до `repair_time`.
- Переход 4→5 при `repair_days >= repair_time`.
- `assembly_time` влияет на `assembly_trigger` (планово D-`assembly_time` до конца ремонта).

### Status 6 (storage)
- Терминальное состояние: значения повторяются.
- `s6_days` растёт только если `s6_started = 1`.

### Инварианты переходов
- За один шаг агент делает максимум один переход (без каскадов).

## Валидация `heli_pandas`
**Общий принцип:** все правила проверяются одновременно (без приоритетов).

### Контроль агрегатов на ВС в эксплуатации
Ожидается, что для текущей версии агрегаты на бортах (group_by > 2) при `condition = 'ИСПРАВНЫЙ'`
имеют `status_id` в {2, 3}.

```sql
SELECT count()
FROM heli_pandas
WHERE version_date = ?
  AND version_id = ?
  AND aircraft_number > 0
  AND group_by > 2
  AND upperUTF8(replaceRegexpAll(ifNull(condition,''), '^\\s+|\\s+$', '')) = 'ИСПРАВНЫЙ'
  AND status_id NOT IN (2, 3);
```

Ожидаемый результат — `0`.

### Колонки
| Колонка | Тип | Источник | Описание |
|---------|-----|----------|----------|
| `ll_mi8` | Nullable(UInt32) | `md_components` | Life Limit |
| `oh_mi8` | Nullable(UInt32) | `md_components` | Overhaul |
| `br_mi8` | Nullable(UInt32) | `md_components` | Before Repair |
| `error_flags` | UInt8 | расчётный | Битовая маска ошибок |

### Битовая маска `error_flags` (статусы 10–15)
| Бит | Значение | Status | Условие |
|-----|----------|--------|---------|
| 0 | 1 | 10 | `ll_mi8 IS NULL OR ll_mi8 = 0` |
| 1 | 2 | 11 | `target_date < version_date AND target_date IS NOT NULL` |
| 2 | 4 | 12 | `condition != 'ИСПРАВНЫЙ' AND sne = 0` |
| 3 | 8 | 13 | `ll_mi8 > 0 AND (sne > ll_mi8 OR ppr > oh_mi8)` |
| 4 | 16 | 14 | `condition NOT IN ('ИСПРАВНЫЙ', 'НЕИСПРАВНЫЙ', 'ДОНОР', 'ВОЗМОЖНОЕ ПРОДЛЕНИЕ НР')` |
| 5 | 32 | 15 | `condition = 'ДОНОР' AND br_mi8 > 0 AND sne < br_mi8` |

### Рабочие статусы (1–6)
Назначаются **только** при `error_flags = 0`.

| Status | Название | Условие |
|--------|----------|---------|
| 1 | ЭКСПЛУАТАЦИЯ | Исправный, установлен на борт |
| 2 | ХРАНЕНИЕ | Исправный, на складе, `sne >= br` |
| 3 | РЕМОНТ | Неисправный, в ремонте |
| 4 | ОЖИДАНИЕ РЕМОНТА | Неисправный, ремфонд (`sne < br`) |
| 5 | ПОСЛЕ РЕМОНТА | После ремонта, ожидает установки |
| 6 | СПИСАНИЕ | Списан, не участвует в расчётах |

### Валидные `condition`
Допустимые значения: `ИСПРАВНЫЙ`, `НЕИСПРАВНЫЙ`, `ДОНОР`, `ВОЗМОЖНОЕ ПРОДЛЕНИЕ НР`.

### Скрипт валидации
`code/analysis/validate_heli_pandas.py`

```bash
python code/analysis/validate_heli_pandas.py --analyze
python code/analysis/validate_heli_pandas.py --update
python code/analysis/validate_heli_pandas.py --all
```

## Архив
- `docs/archive/validation_legacy.md`
- `docs/archive/data_validation_legacy.md`
