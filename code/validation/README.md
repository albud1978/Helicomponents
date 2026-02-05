# Валидационные скрипты

Скрипты для проверки корректности работы RTC модулей через анализ данных в ClickHouse.

## Принципы валидации

✅ **Основной контроль:** Итоговая выгрузка в СУБД (MP2)  
⚠️ **Логирование в RTC коде:** Только по явному согласованию  
🧹 **Очистка:** После устранения проблемы логирование удаляется  
✅ **Критерий успеха:** Результаты подтверждены в СУБД БЕЗ избыточного логирования  

## Структура

```
code/validation/
├── README.md                              # Этот файл
├── validate_state2ops_transitions.py      # Метод 1: Проверка переходов intent
├── validate_state2ops_increments.py       # Метод 2: Проверка инкрементов
└── [будущие скрипты для других модулей]
```

## ЭТАП 1: state_2_operations ✅ ЗАВЕРШЁН

**Статус:** Все проверки пройдены. Модуль работает корректно.

### Метод 1: Проверка переходов intent_state / state (V8)

**Скрипт:** `validate_state2ops_transitions.py`

**Что проверяет (V7 intent_state):**
1. Количество агентов в operations на начало (день 0)
2. Количество агентов с intent=2 на конец (день 3649)
3. Количество реальных переходов 2→4 и 2→6
4. Баланс: начало - конец = количество переходов
5. Корректность логики intent для state='operations':
   - intent=4: `(ppr + dn) >= oh AND (sne + dn) < br`
   - intent=6: `(sne + dn) >= ll OR ((ppr + dn) >= oh AND (sne + dn) >= br)`

**Режим V8 (state-based):**
- Если таблица `sim_masterv2_v8` или нет `intent_state`, проверка ведётся по смене `state` через `lagInFrame`
- Переходы считаются как ops→storage/unsvc при `prev_state='operations'`
- Условия корректности:
  - storage: `prev_sne+prev_dt_next>=prev_ll OR (prev_ppr+prev_dt_next>=prev_oh AND prev_sne+prev_dt_next>=prev_br)`
  - unsvc: `prev_ppr+prev_dt_next>=prev_oh AND prev_sne+prev_dt_next<prev_br`
- `prev_dt_next` берётся из `daily_next_u32`

**Использование:**
```bash
cd "/home/budnik_an/cube linux/cube"
python3 code/validation/validate_state2ops_transitions.py --table sim_masterv2_v8
```
Параметр `--table` опционален (по умолчанию `sim_masterv2_v8`).

**Фактический результат (3650 дней):**
- Начало: 154 агента в operations ✅
- Конец: 2 агента с intent=2 ✅
- Переходов: 152 (134 в intent=4 + 18 в intent=6) ✅
- Логика intent: 0 ошибок ✅
- Баланс: 154 - 2 = 152 ✅

**Детальная проверка всех 152 переходов:**
- ✅ intent=4: Все 134 перехода происходят ТОЧНО при `ppr_next >= oh AND sne_next < br`
- ✅ intent=6: Все 18 переходов корректны
  - 5 по достижению LL (`sne_next >= ll`)
  - 13 по условию BR (`ppr_next >= oh AND sne_next >= br`, но `sne_next < ll`)
  - Минимальный запас до LL: 7,566 минут (idx=82, Mi-17, день 544)

---

### Метод 2: Проверка инкрементов

**Скрипт:** `validate_state2ops_increments.py`

**Что проверяет:**
1. Baseline (таблицы с `dt`): `Δsne = Σdt` и `Δppr = Σdt` по агентам
2. V8 (без `dt`): построчная проверка по `lagInFrame`
3. Для V8: `sne - prev_sne == daily_today_u32` и `ppr - prev_ppr == daily_today_u32`
4. Порядок для V8: `debug_step` (если есть), иначе `day_u16`
5. Учитывает особенность дня 0 в baseline режиме

**Использование:**
```bash
cd "/home/budnik_an/cube linux/cube"
python3 code/validation/validate_state2ops_increments.py --table sim_masterv2_v8
```
Параметры:
- `--table` опционален (по умолчанию `sim_masterv2_v8`)
- `--dt-column` опционален (по умолчанию `daily_today_u32` для V8, иначе `dt`)

**Ожидаемый результат:**
- Типичная разница: 103-153 минут (= dt дня 0)
- Монотонность: 0 ошибок
- Инкременты корректны для всех проверенных агентов

**Пояснение разницы:**
Начальное значение `sne` в день 0 уже включает наработку. Модуль прибавляет `dt` дня 0, но при расчёте `Δsne = sne_end - sne_start` мы берём `sne_start` с дня 0. Поэтому разница ровно равна `dt(день 0)`. Это **ожидаемое поведение**, а не ошибка.

---

## Запуск всех проверок ЭТАП 1

```bash
cd "/home/budnik_an/cube linux/cube"

# Метод 1: Переходы
python3 code/validation/validate_state2ops_transitions.py --table sim_masterv2_v8

# Метод 2: Инкременты
python3 code/validation/validate_state2ops_increments.py --table sim_masterv2_v8
```

Оба скрипта должны завершиться с кодом 0 и сообщением "✅ ВАЛИДАЦИЯ УСПЕШНА".

---

## Результаты валидации ЭТАП 1

**Дата:** 13.10.2025  
**Прогон:** 3650 дней  
**Модули:** state_2_operations (изолированно)  
**Строк в СУБД:** 1,018,350  

### ✅ Итоги:

1. **Логика intent:** 100% корректно для state='operations'
2. **Инкременты:** Корректны с учётом дня 0
3. **Баланс переходов:** Сходится (154 - 2 = 152)
4. **Данные в СУБД:** Полная выгрузка, нет пропусков

### Подробный отчёт:

См. `docs/validation_stage1_state2ops_13-10-2025.md`

---

## Следующие этапы

- ЭТАП 2: state_2_operations + states_stub
- ЭТАП 3: + count_ops
- ЭТАП 4: + quota_ops_excess
- И т.д. по списку модулей

Для каждого этапа будут созданы соответствующие валидационные скрипты.

