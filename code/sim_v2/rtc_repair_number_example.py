"""
Пример использования repair_number в RTC функциях
Демонстрирует как получить repair_number для агента по его partseqno
"""

from string import Template

def register_rtc(model):
    """
    Регистрирует пример RTC функции для работы с repair_number
    
    Использование:
    1. Получить partseqno агента
    2. Найти индекс в mp1_index через Environment
    3. Прочитать repair_number из mp1_repair_number[pidx]
    4. Проверить на sentinel (255 = NULL)
    """
    
    # Пример RTC функции для чтения repair_number
    rtc_code = Template("""
FLAMEGPU_AGENT_FUNCTION(rtc_example_read_repair_number, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Получаем partseqno текущего агента
    const unsigned int partseqno = FLAMEGPU->getVariable<unsigned int>("partseqno");
    
    // Получаем индекс в mp1_index (Environment array)
    auto mp1_index = FLAMEGPU->environment.getProperty<unsigned int, ${MP1_SIZE}>("mp1_index");
    
    // Находим позицию partseqno в mp1_index
    int pidx = -1;
    for (unsigned int i = 0; i < ${MP1_SIZE}; i++) {
        if (mp1_index[i] == partseqno) {
            pidx = static_cast<int>(i);
            break;
        }
    }
    
    // Если partseqno не найден - ошибка
    if (pidx < 0) {
        // В реальном коде можно использовать дефолтное значение или пропустить агента
        return flamegpu::ALIVE;
    }
    
    // Читаем repair_number из mp1_repair_number
    auto mp1_repair_number = FLAMEGPU->environment.getProperty<unsigned char, ${MP1_SIZE}>("mp1_repair_number");
    const unsigned char repair_number = mp1_repair_number[pidx];
    
    // Проверяем на sentinel (255 = NULL)
    const unsigned char SENTINEL_U8 = 255u;
    
    if (repair_number == SENTINEL_U8) {
        // repair_number = NULL в исходных данных
        // Можно использовать дефолтное значение или пропустить агента
        // Например, для не-планеров это нормально
    } else {
        // repair_number имеет значение (для планеров это должно быть 18)
        // Здесь можно использовать repair_number для квотирования ремонтов
        
        // Пример: сохраняем в переменную агента для отладки
        // FLAMEGPU->setVariable<unsigned int>("debug_repair_number", static_cast<unsigned int>(repair_number));
    }
    
    return flamegpu::ALIVE;
}
""")
    
    # Получаем размер MP1 из модели
    mp1_size = model.Environment().getPropertyUInt("mp1_size")
    
    # Компилируем RTC функцию
    rtc_func = model.newRTCFunction("rtc_example_read_repair_number", rtc_code.substitute(MP1_SIZE=mp1_size))
    
    return rtc_func


def create_example_layer(model, layer_name="example_repair_number"):
    """
    Создает пример слоя для демонстрации использования repair_number
    
    Args:
        model: FLAME GPU ModelDescription
        layer_name: имя слоя
    
    Returns:
        layer: FLAME GPU LayerDescription
    """
    # Регистрируем RTC функцию
    rtc_func = register_rtc(model)
    
    # Создаем слой
    layer = model.newLayer(layer_name)
    
    # Добавляем функцию для агентов в состоянии "repair"
    # (можно изменить на любое другое состояние или убрать фильтр)
    agent_func = layer.AgentFunction("helicopter", rtc_func)
    agent_func.setInitialState("repair")
    agent_func.setEndState("repair")
    
    return layer


# Документация по использованию
"""
ИСПОЛЬЗОВАНИЕ repair_number В RTC ФУНКЦИЯХ:

1. ЧТЕНИЕ repair_number:
   ```cpp
   // Получить partseqno агента
   const unsigned int partseqno = FLAMEGPU->getVariable<unsigned int>("partseqno");
   
   // Найти индекс в mp1_index
   auto mp1_index = FLAMEGPU->environment.getProperty<unsigned int, MP1_SIZE>("mp1_index");
   int pidx = -1;
   for (unsigned int i = 0; i < MP1_SIZE; i++) {
       if (mp1_index[i] == partseqno) {
           pidx = static_cast<int>(i);
           break;
       }
   }
   
   // Прочитать repair_number
   auto mp1_repair_number = FLAMEGPU->environment.getProperty<unsigned char, MP1_SIZE>("mp1_repair_number");
   const unsigned char repair_number = mp1_repair_number[pidx];
   
   // Проверить на NULL (sentinel = 255)
   if (repair_number != 255u) {
       // Использовать repair_number
   }
   ```

2. ИСПОЛЬЗОВАНИЕ В КВОТИРОВАНИИ:
   - repair_number определяет номер квоты для ремонта
   - Для планеров (group_by=1,2) значение = 18
   - Для других агрегатов может быть NULL (255) или другое значение
   - Можно использовать для группировки агентов по типам ремонта

3. SENTINEL ЗНАЧЕНИЯ:
   - 255 (0xFF) = NULL в исходных данных
   - Для UInt8 это максимальное значение
   - Проверяйте на sentinel перед использованием

4. ОПТИМИЗАЦИЯ:
   - Поиск в mp1_index можно кэшировать в переменной агента
   - Для планеров можно использовать прямой доступ по известным индексам
   - Для массовых операций лучше использовать MacroProperty
"""


