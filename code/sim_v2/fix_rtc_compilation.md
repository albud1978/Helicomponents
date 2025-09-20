# Исправление ошибки компиляции rtc_mp5_copy_columns

## Проблема
Ошибка компиляции RTC функции `rtc_mp5_copy_columns` из-за неправильного синтаксиса в шаблонном параметре MacroProperty.

## Причина
NVRTC не может правильно обработать выражение в шаблонном параметре вида:
```cpp
getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp5_lin")
```

## Решение
Вычислить размер заранее и использовать константу:
```cpp
static const unsigned int TOTAL_SIZE = {(DAYS+1)*FRAMES}u;
// ...
getMacroProperty<unsigned int, TOTAL_SIZE>("mp5_lin")
```

## Внесенные изменения

### В файле `03_add_probe_mp5.py`:
1. Добавлена константа `TOTAL_SIZE` в обе RTC функции
2. Заменены выражения в шаблонных параметрах на константу
3. Улучшено форматирование для лучшей читаемости

### В файле `04_add_status_246.py`:
1. Аналогичные изменения в функции `rtc_status_246_run`

## Применение исправлений в вашем orchestrator

Если у вас есть файл `04_status246_orchestrator.py`, внесите аналогичные изменения:

```python
# Найдите строку с rtc_mp5_copy_columns и замените на:
rtc_copy = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mp5_copy_columns, flamegpu::MessageNone, flamegpu::MessageNone) {{
    static const unsigned int FRAMES = {FRAMES}u;
    static const unsigned int DAYS = {DAYS}u;
    static const unsigned int TOTAL_SIZE = {(DAYS+1)*FRAMES}u;
    
    const unsigned int i = FLAMEGPU->getVariable<unsigned int>("idx");
    if (i >= FRAMES) return flamegpu::ALIVE;
    
    auto dst = FLAMEGPU->environment.getMacroProperty<unsigned int, TOTAL_SIZE>("mp5_lin");
    
    for (unsigned int d = 0u; d <= DAYS; ++d) {{
        const unsigned int base = d * FRAMES + i;
        const unsigned int v = FLAMEGPU->environment.getProperty<unsigned int>("mp5_src", base);
        dst[base].exchange(v);
    }}
    return flamegpu::ALIVE;
}}
"""
```

## Тестирование
После внесения изменений запустите:
```bash
cd /home/budnik_an/cube_linux/cube && PYTHONUNBUFFERED=1 HL_V2_STEPS=365 HL_V2_CHECK_ACN=22418 python3 -u code/sim_v2/04_status246_orchestrator.py
```