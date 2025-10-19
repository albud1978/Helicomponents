# Экспорт чата от 19-10-2025

## Основные темы чата

### 1. Анализ логирования флагов квотирования
- Обнаружено, что флаги демоута и промоутов все нулевые в базе
- `quota_demount`, `quota_promote_p1/p2/p3` не логируются в СУБД
- Целевые значения и gap логируются корректно ✓

### 2. Причина проблемы
- В `rtc_mp2_write_inactive` флаги логируются ✓
- В `rtc_mp2_write_operations, serviceable, repair, reserve, storage` флаги НЕ логируются ❌
- Старый код просто копирует значения квотирования без записи флагов в MP2

### 3. Результаты теста на 3650 дней
- GPU обработка: 154.22с
- Выгрузка в СУБД: 74.64с (параллельно)
- Строк в MP2: 1,042,318
- Лог сохранён: `/logs/full_test_logging_3650days_20251018_203706.log`

---

## Решенные задачи

### ✅ Задача 1: Перенос лога в папку проекта
**Проблема:** Лог теста лежал в `/tmp`, вне структуры проекта.

**Решение:**
- Перемещён в `/home/budnik_an/cube_linux/cube/logs/`
- Имя: `full_test_logging_3650days_20251018_203706.log` (2.2 MB)

**Результат:** ✅ Завершено

---

### ✅ Задача 2: Проверка квотирования в базе данных
**Проблема:** Нужно было убедиться, что флаги квотирования логируются.

**Решение:**
Написаны SQL запросы для AC 24113:
```sql
SELECT day_u16, state, quota_target_mi8, quota_gap_mi8,
       quota_demount, quota_promote_p1, quota_promote_p2, quota_promote_p3
FROM sim_masterv2
WHERE aircraft_number = 24113 AND day_u16 BETWEEN 24 AND 30
```

**Результат:** ❌ Все флаги = 0 (не логируются)

---

### ⏳ Задача 3: Добавление логирования флагов во все функции mp2_writer
**Статус:** Ожидает реализации

**Проблема:** 5 функций не логируют флаги квотирования.

**План решения:**
1. Добавить блок логирования в `rtc_mp2_write_operations` (строки 318-330)
2. Добавить блок логирования в `rtc_mp2_write_serviceable`
3. Добавить блок логирования в `rtc_mp2_write_repair`
4. Добавить блок логирования в `rtc_mp2_write_reserve`
5. Добавить блок логирования в `rtc_mp2_write_storage`

**Блок для копирования:**
```cuda
// ═══════════════════════════════════════════════════════════════════
// Логирование флагов квотирования (демоут и промоут)
// ═══════════════════════════════════════════════════════════════════
unsigned int demount_flag = 0u;
unsigned int promote_p1_flag = 0u;
unsigned int promote_p2_flag = 0u;
unsigned int promote_p3_flag = 0u;

if (FLAMEGPU->getVariable<unsigned int>("group_by") == 1u) {{
    auto mi8_approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve");
    auto mi8_approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s3");
    auto mi8_approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s5");
    auto mi8_approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s1");
    
    demount_flag = mi8_approve[idx];
    promote_p1_flag = mi8_approve_s3[idx];
    promote_p2_flag = mi8_approve_s5[idx];
    promote_p3_flag = mi8_approve_s1[idx];
}} else if (FLAMEGPU->getVariable<unsigned int>("group_by") == 2u) {{
    auto mi17_approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve");
    auto mi17_approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s3");
    auto mi17_approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s5");
    auto mi17_approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s1");
    
    demount_flag = mi17_approve[idx];
    promote_p1_flag = mi17_approve_s3[idx];
    promote_p2_flag = mi17_approve_s5[idx];
    promote_p3_flag = mi17_approve_s1[idx];
}}

// Логируем флаги в MP2
auto mp2_demount = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_demount");
auto mp2_p1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p1");
auto mp2_p2 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p2");
auto mp2_p3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p3");

mp2_demount[pos].exchange(demount_flag);
mp2_p1[pos].exchange(promote_p1_flag);
mp2_p2[pos].exchange(promote_p2_flag);
mp2_p3[pos].exchange(promote_p3_flag);
```

---

## Технические детали

### AC 24113 (Mi-8) на дни 26-27
- **День 24-26:** target_mi8=68, gap_mi8=-4, demount=0, p1=0, p2=0, p3=0
- **День 27:** target_mi8=64, gap_mi8=0, demount=0, p1=0, p2=0, p3=0
- **Проблема:** Все флаги нулевые - они не читаются и не пишутся в MP2

### Структура MP2
- Позиция: `pos = day * MAX_FRAMES + idx`
- Плотная матрица с D+1 паддингом
- MAX_FRAMES=286, MAX_DAYS=4000, MAX_SIZE=1,144,286

### Проблемы с редактированием
- `edit_file` tool не работает на этом файле
- Возможная причина: f-string с `{{}}` экранированием
- Решение: ручное редактирование или использование terminal скриптов

---

## Документация

### Обновлено
- `docs/validation.md` — добавлен раздел о логировании квотирования (19.10.2025)

### Удалено
- `docs/QUOTA_LOGGING_ANALYSIS_2025-10-19.md` (было неправильное имя/место)
- `test_edit.txt`, `test_edit.py` (файлы для тренировки)

---

## Ключевые выводы

1. **Квотирование работает** - approve флаги устанавливаются корректно в буферах `mi*_approve*`
2. **Проблема в логировании** - флаги не пишутся в MP2 MacroProperties из 5 функций
3. **Нужно добавить 40+ строк кода** - скопировать блок логирования из `inactive` в остальные 5 функций
4. **После исправления** - повторить тест на 3650 дней и проверить флаги в базе

