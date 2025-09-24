"""
RTC модуль для менеджера состояний с условными переходами
Использует одну функцию для обработки всех переходов в каждое состояние
"""

import pyflamegpu as fg

def register_state_manager_conditional(model: fg.ModelDescription, agent: fg.AgentDescription):
    """
    Регистрирует менеджер состояний с условными переходами
    
    Args:
        model: FLAME GPU ModelDescription
        agent: FLAME GPU AgentDescription
    """
    print("  Регистрация state manager с условными переходами")
    
    # Создаём слои для переходов
    # Слой 1: Все переходы В operations (из 1,3,5)
    layer_to_ops = model.newLayer("transitions_to_operations")
    
    # Единая функция для всех переходов в operations
    rtc_to_operations = agent.newRTCFunction("rtc_all_to_operations", """
FLAMEGPU_AGENT_FUNCTION(rtc_all_to_operations, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    const unsigned int status_id = FLAMEGPU->getVariable<unsigned int>("status_id");
    
    // Проверяем, хочет ли агент в operations
    if (intent != 2u) {
        return flamegpu::ALIVE;
    }
    
    // Проверяем, что агент в одном из состояний, откуда можно перейти в operations
    if (status_id != 1u && status_id != 3u && status_id != 5u) {
        return flamegpu::ALIVE;
    }
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    const char* getStateName(unsigned int state) {
        switch(state) {
            case 1: return "inactive";
            case 3: return "serviceable";
            case 5: return "reserve";
            default: return "unknown";
        }
    }
    
    printf("  [Step %u] AC %u: TRANSITION %s -> operations\\n", 
           step_day, aircraft_number, getStateName(status_id));
    
    // Обновляем status_id
    FLAMEGPU->setVariable<unsigned int>("status_id", 2u);
    
    return flamegpu::ALIVE;
}
""")
    # Эта функция работает для ВСЕХ агентов, проверяя условия внутри
    layer_to_ops.addAgentFunction(rtc_to_operations)
    
    # Слой 2: Переходы ИЗ operations (в 4,6)
    layer_from_ops = model.newLayer("transitions_from_operations")
    
    # Переход operations -> repair
    rtc_ops_to_repair = agent.newRTCFunction("rtc_ops_to_repair", """
FLAMEGPU_AGENT_FUNCTION(rtc_ops_to_repair, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    const unsigned int status_id = FLAMEGPU->getVariable<unsigned int>("status_id");
    
    // Проверяем условия перехода
    if (status_id != 2u || intent != 4u) {
        return flamegpu::ALIVE;
    }
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    printf("  [Step %u] AC %u: TRANSITION operations -> repair\\n", 
           step_day, aircraft_number);
    
    // Обновляем status_id
    FLAMEGPU->setVariable<unsigned int>("status_id", 4u);
    
    return flamegpu::ALIVE;
}
""")
    layer_from_ops.addAgentFunction(rtc_ops_to_repair)
    
    # Переход operations -> storage
    rtc_ops_to_storage = agent.newRTCFunction("rtc_ops_to_storage", """
FLAMEGPU_AGENT_FUNCTION(rtc_ops_to_storage, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    const unsigned int status_id = FLAMEGPU->getVariable<unsigned int>("status_id");
    
    // Проверяем условия перехода
    if (status_id != 2u || intent != 6u) {
        return flamegpu::ALIVE;
    }
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    printf("  [Step %u] AC %u: TRANSITION operations -> storage\\n", 
           step_day, aircraft_number);
    
    // Обновляем status_id
    FLAMEGPU->setVariable<unsigned int>("status_id", 6u);
    
    return flamegpu::ALIVE;
}
""")
    layer_from_ops.addAgentFunction(rtc_ops_to_storage)
    
    # Слой 3: Переход repair -> reserve
    layer_repair = model.newLayer("transitions_from_repair")
    
    rtc_repair_to_reserve = agent.newRTCFunction("rtc_repair_to_reserve", """
FLAMEGPU_AGENT_FUNCTION(rtc_repair_to_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    const unsigned int status_id = FLAMEGPU->getVariable<unsigned int>("status_id");
    
    // Проверяем условия перехода
    if (status_id != 4u || intent != 5u) {
        return flamegpu::ALIVE;
    }
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    printf("  [Step %u] AC %u: TRANSITION repair -> reserve\\n", 
           step_day, aircraft_number);
    
    // Обновляем status_id
    FLAMEGPU->setVariable<unsigned int>("status_id", 5u);
    
    // Сбрасываем счётчики
    FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
    FLAMEGPU->setVariable<unsigned int>("assembly_trigger", 0u);
    
    return flamegpu::ALIVE;
}
""")
    layer_repair.addAgentFunction(rtc_repair_to_reserve)
    
    print("  RTC модуль state_manager с условными переходами зарегистрирован")
