"""
RTC модуль для простого менеджера состояний (без квотирования)
"""

import pyflamegpu as fg
from model_build import MAX_FRAMES, MAX_DAYS

# Простой подход - используем переменную status_id для отслеживания текущего состояния
# и меняем состояние агента на основе intent_state

# Функция для агентов, которые хотят перейти в operations (из 1, 3, 5)
RTC_TO_OPERATIONS = """
FLAMEGPU_AGENT_FUNCTION(rtc_to_operations, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // Только для агентов с intent = 2 (operations)
    if (intent != 2u) {
        return flamegpu::ALIVE;
    }
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    // Обновляем status_id для отслеживания
    FLAMEGPU->setVariable<unsigned int>("status_id", 2u);
    
    printf("  [Step %u] AC %u: TRANSITION -> operations\\n", 
           step_day, aircraft_number);
    
    return flamegpu::ALIVE;
}
"""

# Функция для агентов из operations, которые хотят перейти в repair
RTC_TO_REPAIR = """
FLAMEGPU_AGENT_FUNCTION(rtc_to_repair, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // Только для агентов с intent = 4 (repair)
    if (intent != 4u) {
        return flamegpu::ALIVE;
    }
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    // Обновляем status_id для отслеживания
    FLAMEGPU->setVariable<unsigned int>("status_id", 4u);
    
    printf("  [Step %u] AC %u: TRANSITION -> repair\\n", 
           step_day, aircraft_number);
    
    return flamegpu::ALIVE;
}
"""

# Функция для агентов из operations, которые хотят перейти в storage
RTC_TO_STORAGE = """
FLAMEGPU_AGENT_FUNCTION(rtc_to_storage, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // Только для агентов с intent = 6 (storage)
    if (intent != 6u) {
        return flamegpu::ALIVE;
    }
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    // Обновляем status_id для отслеживания
    FLAMEGPU->setVariable<unsigned int>("status_id", 6u);
    
    printf("  [Step %u] AC %u: TRANSITION -> storage\\n", 
           step_day, aircraft_number);
    
    return flamegpu::ALIVE;
}
"""

# Функция для агентов из repair, которые хотят перейти в reserve
RTC_TO_RESERVE = """
FLAMEGPU_AGENT_FUNCTION(rtc_to_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // Только для агентов с intent = 5 (reserve)
    if (intent != 5u) {
        return flamegpu::ALIVE;
    }
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    // Обновляем status_id для отслеживания
    FLAMEGPU->setVariable<unsigned int>("status_id", 5u);
    
    // Сбрасываем счетчики при выходе из ремонта
    FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
    FLAMEGPU->setVariable<unsigned int>("assembly_trigger", 0u);
    
    printf("  [Step %u] AC %u: TRANSITION -> reserve\\n", 
           step_day, aircraft_number);
    
    return flamegpu::ALIVE;
}
"""

def register_state_manager_simple(model: fg.ModelDescription, agent: fg.AgentDescription, layer):
    """
    Регистрирует простой менеджер состояний в модели
    
    Args:
        model: FLAME GPU ModelDescription
        agent: FLAME GPU AgentDescription
        layer: слой для добавления функций (не используется, создаём свои слои)
    """
    print("  Регистрация функций переходов состояний:")
    
    # Создаем функции переходов с правильными состояниями
    transitions = [
        # (from_states, to_state, rtc_code, func_name)
        (["inactive", "serviceable", "reserve"], "operations", RTC_TO_OPERATIONS, "rtc_to_operations"),
        (["operations"], "repair", RTC_TO_REPAIR, "rtc_to_repair"),
        (["operations"], "storage", RTC_TO_STORAGE, "rtc_to_storage"),
        (["repair"], "reserve", RTC_TO_RESERVE, "rtc_to_reserve"),
    ]
    
    for from_states, to_state, rtc_code, func_name in transitions:
        # Создаем RTC функцию
        rtc_func = agent.newRTCFunction(func_name, rtc_code)
        
        # Создаем новый слой для каждой функции
        new_layer = model.newLayer(f"transition_to_{to_state}")
        
        # Добавляем функцию для каждого исходного состояния
        for from_state in from_states:
            # Создаем копию функции для каждого исходного состояния
            state_func = agent.newRTCFunction(f"{func_name}_from_{from_state}", rtc_code)
            state_func.setInitialState(from_state)
            state_func.setEndState(to_state)
            
            new_layer.addAgentFunction(state_func)
            print(f"    - {from_state} -> {to_state}")
    
    print("  RTC модуль state_manager зарегистрирован")
