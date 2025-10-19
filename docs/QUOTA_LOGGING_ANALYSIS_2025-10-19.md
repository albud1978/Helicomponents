# Анализ системы логирования квотирования (2025-10-19)

## Проблема

При проверке данных в ClickHouse после теста на 3650 дней обнаружено:
- **Флаги квотирования все нулевые**: `quota_demount`, `quota_promote_p1`, `quota_promote_p2`, `quota_promote_p3` = 0
- **Целевые значения логируются**: `quota_target_mi8`, `quota_target_mi17` ✓
- **Gap логируется**: `quota_gap_mi8`, `quota_gap_mi17` ✓

## Причина

В файле `code/sim_v2/rtc_mp2_writer.py`:

1. **rtc_mp2_write_inactive** (строки 94-235): Флаги **логируются** ✓
   - Читает из `mi8_approve`, `mi8_approve_s3/s5/s1` и `mi17_approve*`
   - Пишет в `mp2_quota_demount`, `mp2_quota_promote_p1/p2/p3`

2. **rtc_mp2_write_operations** (строки 238-334): Флаги **НЕ логируются** ❌
   - Старый код просто копирует квотирование, но не пишет флаги в MP2

3. **rtc_mp2_write_serviceable** (строки 337-462): Флаги **НЕ логируются** ❌

4. **rtc_mp2_write_repair** (строки 465-589): Флаги **НЕ логируются** ❌

5. **rtc_mp2_write_reserve** (строки 592-716): Флаги **НЕ логируются** ❌

6. **rtc_mp2_write_storage** (строки 719-843): Флаги **НЕ логируются** ❌

## Решение

Необходимо добавить блок логирования флагов во все 5 функций (`operations`, `serviceable`, `repair`, `reserve`, `storage`), аналогично `inactive`:

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

Это заменит старый код "просто копирования" на строках 318-328 в `operations` и аналогичные блоки в других функциях.

## Результаты текста на 3650 дней

- **Загрузка модели**: 3.63с
- **Обработка на GPU**: 154.22с
- **Выгрузка в СУБД**: 74.64с (параллельно)
- **Общее время**: 158.84с
- **Строк выгружено**: 1,042,318 (286 агентов × 3650 дней + spawn новые)
- **Лог**: `/home/budnik_an/cube_linux/cube/logs/full_test_logging_3650days_20251018_203706.log`

## Важные замечания

1. **AC 24113** на дни 26-27:
   - День 26: target_mi8=68, gap_mi8=-4, demount=0, p1=0, p2=0, p3=0 (все нулевые - проблема!)
   - День 27: target_mi8=64, gap_mi8=0, demount=0, p1=0, p2=0, p3=0

2. **Логирование квотирования должно показать**:
   - Когда агент одобрен в демоут (demount=1)
   - Когда агент одобрен в промоут P1/P2/P3 (соответствующий флаг=1)

3. **MP2 структура**: pos = day * MAX_FRAMES + idx (плотная матрица с D+1 паддингом)

## Статус

- Откачено: 1 попытка редактирования через Python скрипт
- Откачено: 5+ попыток редактирования через `edit_file`
- **ОЖИДАЕТ**: Ручное редактирование файла или использование другого подхода

