"""
RTC модуль для тестового state manager - только один переход 2->4
"""

import pyflamegpu as fg

# Функция для перехода 2->4
RTC_TEST_2_TO_4 = """
FLAMEGPU_AGENT_FUNCTION(rtc_test_2_to_4, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // Проверяем, что агент хочет перейти в repair (intent_state == 4)
    if (intent != 4u) {
        return flamegpu::ALIVE;
    }
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    printf("  [Step %u] AC %u: TEST TRANSITION operations -> repair (intent=%u)\\n", 
           step_day, aircraft_number, intent);
    
    // После перехода состояние автоматически изменится благодаря setEndState
    
    return flamegpu::ALIVE;
}
"""

def register_state_manager_test(model: fg.ModelDescription, agent: fg.AgentDescription):
    """
    Регистрирует тестовый state manager для одного перехода
    
    Args:
        model: FLAME GPU ModelDescription
        agent: FLAME GPU AgentDescription
    """
    print("  Регистрация тестового state manager (только 2->4)")
    
    # Создаем слой для перехода
    layer = model.newLayer("test_transition_2_to_4")
    
    # Создаем RTC функцию
    rtc_func = agent.newRTCFunction("rtc_test_2_to_4", RTC_TEST_2_TO_4)
    
    # Устанавливаем состояния для перехода
    rtc_func.setInitialState("operations")  # Работает только для агентов в состоянии operations
    rtc_func.setEndState("repair")          # Переводит их в состояние repair
    
    # Добавляем функцию в слой
    layer.addAgentFunction(rtc_func)
    
    print("  RTC модуль state_manager_test зарегистрирован (переход 2->4)")
