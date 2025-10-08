# Инструкция по использованию каскадной архитектуры квотирования

**Дата:** 07-10-2025  
**Статус:** Реализовано, готово к тестированию

---

## 📦 Реализованные модули

### 1. Демоут (operations → serviceable)
**Файл:** `code/sim_v2/rtc_quota_ops_excess.py`  
**Слой:** `quota_demount`  
**Логика:**
- Считает `Curr` (агенты в operations с intent=2, кроме 4/6)
- Считает `Target` (из mp4_ops_counter на D+1)
- Balance = Curr - Target
- **ЕСЛИ Balance <= 0:** Early exit (все агенты)
- **ИНАЧЕ:** Демоут K самых старых → intent=3 + mi*_approve=1

### 2. Промоут приоритет 1 (serviceable → operations)
**Файл:** `code/sim_v2/rtc_quota_promote_serviceable.py`  
**Слой:** `quota_promote_serviceable`  
**Логика:**
- Считает `used` (одобрено в демоуте)
- deficit = Target - used
- **ЕСЛИ deficit <= 0:** Early exit
- **ИНАЧЕ:** Промоут deficit агентов (FCFS) → intent=2 + mi*_approve=1

### 3. Промоут приоритет 2 (reserve → operations)
**Файл:** `code/sim_v2/rtc_quota_promote_reserve.py`  
**Слой:** `quota_promote_reserve`  
**Логика:**
- Считает `used` (демоут + serviceable)
- deficit = Target - used
- **ЕСЛИ deficit <= 0:** Early exit
- **ИНАЧЕ:** Промоут deficit агентов (FCFS) → intent=2 + mi*_approve=1

### 4. Промоут приоритет 3 (inactive → operations)
**Файл:** `code/sim_v2/rtc_quota_promote_inactive.py`  
**Слой:** `quota_promote_inactive`  
**Логика:**
- Считает `used` (демоут + serviceable + reserve)
- deficit = Target - used
- **ЕСЛИ deficit <= 0:** Early exit
- **ИНАЧЕ:** Промоут deficit агентов (FCFS, с условием) → intent=2 + mi*_approve=1
- ⚠️ **Может остаться deficit > 0** (допустимо по бизнес-логике)

---

## 🚀 Запуск

### Базовый запуск (без квотирования)
```bash
cd /home/budnik_an/cube\ linux/cube/code/sim_v2
python orchestrator_v2.py --modules state_manager_full --steps 5
```

### Запуск с каскадным квотированием (полная цепочка)
```bash
cd /home/budnik_an/cube\ linux/cube/code/sim_v2
python orchestrator_v2.py \
  --modules state_manager_full quota_ops_excess quota_promote_serviceable quota_promote_reserve quota_promote_inactive \
  --steps 5
```

### Параметры
- `--modules`: Список RTC модулей (порядок важен!)
- `--steps`: Количество дней симуляции (по умолчанию из HL_V2_STEPS)
- `--enable-mp2`: Включить MP2 device-side export
- `--drop-table`: Дропнуть таблицу sim_masterv2 перед запуском

---

## 📊 Порядок слоёв (важно!)

Согласно каскадной архитектуре, модули должны подключаться в следующем порядке:

1. **state_manager_full** (или другой state manager) — устанавливает intent для всех состояний
2. **quota_ops_excess** — демоут (operations → serviceable)
3. **quota_promote_serviceable** — промоут приоритет 1 (serviceable → operations)
4. **quota_promote_reserve** — промоут приоритет 2 (reserve → operations)
5. **quota_promote_inactive** — промоут приоритет 3 (inactive → operations)
6. **rtc_state_transitions** (если есть) — применение intent → смена state

⚠️ **Нарушение порядка приведёт к некорректной работе каскада!**

---

## 🧪 Тестирование

### Smoke test (DAYS=5)
```bash
python orchestrator_v2.py \
  --modules state_manager_full quota_ops_excess quota_promote_serviceable quota_promote_reserve quota_promote_inactive \
  --steps 5
```

### Интеграционный тест (DAYS=90)
```bash
python orchestrator_v2.py \
  --modules state_manager_full quota_ops_excess quota_promote_serviceable quota_promote_reserve quota_promote_inactive \
  --steps 90
```

### Полный прогон (DAYS=365)
```bash
python orchestrator_v2.py \
  --modules state_manager_full quota_ops_excess quota_promote_serviceable quota_promote_reserve quota_promote_inactive \
  --steps 365
```

### Проверка детерминизма
Запустить дважды с одинаковыми параметрами и сравнить результаты:
```bash
python orchestrator_v2.py --modules ... --steps 90 > run1.log
python orchestrator_v2.py --modules ... --steps 90 > run2.log
diff run1.log run2.log
```

---

## 🔍 Диагностика

### Логи квотирования
Модули выводят диагностику на дни 180, 181, 182:
- `[DEMOUNT Day X]` — демоут агента
- `[PROMOTE P1 Day X]` — промоут приоритет 1 (serviceable)
- `[PROMOTE P2 Day X]` — промоут приоритет 2 (reserve)
- `[PROMOTE P3 Day X]` — промоут приоритет 3 (inactive)

### Проверка каскада
Для проверки корректности каскада:
1. Запустить с `--steps 182`
2. Проверить логи на день 181:
   - Должен быть подсчёт `Curr`, `Target`, `Balance` (демоут)
   - Должен быть подсчёт `used`, `deficit` (промоут)
   - Должна быть передача остатка между приоритетами

---

## 📝 TODO (после тестирования)

- [ ] Добавить условие для inactive (repair_time - repair_days > assembly_time)
- [ ] Оптимизировать подсчёт `curr` (использовать reduction вместо цикла)
- [ ] Добавить поддержку других приоритетов (oldest_first для промоута)
- [ ] Интегрировать с универсальным QuotaManager (этап 2)

---

## 🔗 Связанные документы

- [Каскадная архитектура](quota_cascade_architecture_06-10-2025.md)
- [План рефакторинга](refactoring_plan_quota_optimization.md)
- [Универсальный QuotaManager](universal_quota_manager_design.md)



