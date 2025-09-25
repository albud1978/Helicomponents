"""
RTC модуль для тестового state manager - только переход 2->4
"""

import pyflamegpu as fg

# Условие выполнения функции: только intent==4
RTC_COND_INTENT_4 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_4) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 4u;
}
"""

# Функция для перехода 2->4 (теперь без внутренней проверки - она в условии)
RTC_TEST_2_TO_4 = """
FLAMEGPU_AGENT_FUNCTION(rtc_test_2_to_4, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    printf("  [Step %u] AC %u: TRANSITION operations -> repair (intent=4)\\n", step_day, aircraft_number);
    return flamegpu::ALIVE;
}
"""

def register_state_manager_test(model: fg.ModelDescription, agent: fg.AgentDescription):
    """
    Регистрирует тестовый state manager для перехода 2->4
    
    Args:
        model: FLAME GPU ModelDescription
        agent: FLAME GPU AgentDescription
    """
    print("  Регистрация тестового state manager (только 2->4)")
    
    # Слой: Переход 2->4 (operations -> repair)
    layer_2_to_4 = model.newLayer("test_transition_2_to_4")
    rtc_func_2_to_4 = agent.newRTCFunction("rtc_test_2_to_4", RTC_TEST_2_TO_4)
    
    # Применяем условие через setRTCFunctionCondition
    rtc_func_2_to_4.setRTCFunctionCondition(RTC_COND_INTENT_4)
    
    rtc_func_2_to_4.setInitialState("operations")   # Работает для агентов в состоянии operations
    rtc_func_2_to_4.setEndState("repair")           # Переводит их в состояние repair
    layer_2_to_4.addAgentFunction(rtc_func_2_to_4)
    
    print("  RTC модуль state_manager_test зарегистрирован (только 2->4)")
