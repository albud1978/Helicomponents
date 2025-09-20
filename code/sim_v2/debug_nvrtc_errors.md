# Отладка ошибок компиляции NVRTC в FLAME GPU

## Типичные причины ошибок компиляции RTC функций

### 1. Проблемы с шаблонными параметрами
**Проблема:**
```cpp
getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp5_lin")
```

**Решение:**
```cpp
static const unsigned int TOTAL_SIZE = FRAMES * (DAYS + 1u);
getMacroProperty<unsigned int, TOTAL_SIZE>("mp5_lin")
```

### 2. Отсутствующие переменные агента
**Проблема:** Использование переменной, которая не была объявлена:
```cpp
FLAMEGPU->getVariable<unsigned int>("daily_today_u32")
```

**Решение:** Убедитесь, что переменная объявлена в модели:
```python
a.newVariableUInt("daily_today_u32", 0)
```

### 3. Неправильный синтаксис C++
**Частые ошибки:**
- Отсутствующие точки с запятой
- Неправильные скобки в выражениях
- Использование Python f-string синтаксиса внутри C++ кода

### 4. Проблемы с константами
**Проблема:** Использование больших чисел без суффикса:
```cpp
const unsigned int val = 1000000;  // Может вызвать предупреждения
```

**Решение:**
```cpp
const unsigned int val = 1000000u;  // Явно указываем unsigned
```

## Как включить подробную диагностику

### 1. Включите вывод ошибок NVRTC:
```bash
export FLAMEGPU_VERBOSE=1
export PYTHONUNBUFFERED=1
```

### 2. Добавьте отладочный вывод RTC кода:
```python
rtc_src = f"""
// Ваш RTC код
"""
print("=== RTC SOURCE ===")
print(rtc_src)
print("=== END SOURCE ===")
```

### 3. Используйте простые тестовые функции:
```python
# Минимальная RTC функция для проверки компиляции
rtc_test = """
FLAMEGPU_AGENT_FUNCTION(test_compile, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}
"""
```

## Пошаговая отладка

1. **Создайте минимальный воспроизводимый пример**
2. **Постепенно добавляйте функциональность**
3. **Проверяйте компиляцию после каждого изменения**
4. **Используйте статические константы вместо выражений в шаблонах**

## Полезные команды

### Проверка синтаксиса без запуска симуляции:
```python
try:
    model = fg.ModelDescription("TestModel")
    a = model.newAgent("test")
    a.newRTCFunction("test_fn", rtc_src)
    print("✅ RTC компиляция успешна")
except Exception as e:
    print(f"❌ Ошибка компиляции: {e}")
```

### Вывод полного стека ошибок:
```python
import traceback
try:
    # Ваш код
except Exception as e:
    print("Полный стек ошибки:")
    traceback.print_exc()
```