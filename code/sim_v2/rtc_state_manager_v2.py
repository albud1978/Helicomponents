"""
RTC модуль для простого менеджера состояний (версия 2)
Использует одну функцию для всех агентов, которая обновляет status_id
"""

import pyflamegpu as fg

# Единая функция обработки переходов для всех агентов
RTC_APPLY_TRANSITIONS = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_transitions, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    const unsigned int current_status = FLAMEGPU->getVariable<unsigned int>("status_id");
    
    // Если intent совпадает с текущим статусом, ничего не делаем
    if (intent == current_status || intent == 0u) {
        return flamegpu::ALIVE;
    }
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    // Обновляем status_id
    FLAMEGPU->setVariable<unsigned int>("status_id", intent);
    
    // Логирование перехода
    const char* getStateName(unsigned int state) {
        switch(state) {
            case 1: return "inactive";
            case 2: return "operations"; 
            case 3: return "serviceable";
            case 4: return "repair";
            case 5: return "reserve";
            case 6: return "storage";
            default: return "unknown";
        }
    }
    
    printf("  [Step %u] AC %u: TRANSITION %s -> %s\\n", 
           step_day, aircraft_number, 
           getStateName(current_status), getStateName(intent));
    
    // Особые действия при переходах
    if (current_status == 4u && intent == 5u) {  // repair -> reserve
        FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
        FLAMEGPU->setVariable<unsigned int>("assembly_trigger", 0u);
    }
    
    // Важно: сбрасываем intent_state после применения перехода
    FLAMEGPU->setVariable<unsigned int>("intent_state", 0u);
    
    return flamegpu::ALIVE;
}
"""

def register_state_manager_v2(model: fg.ModelDescription, agent: fg.AgentDescription):
    """
    Регистрирует простой менеджер состояний (версия 2)
    
    Args:
        model: FLAME GPU ModelDescription
        agent: FLAME GPU AgentDescription
    """
    print("  Регистрация state manager v2")
    
    # Создаем новый слой для менеджера состояний
    layer = model.newLayer("state_manager")
    
    # Создаем RTC функцию для всех агентов
    rtc_func = agent.newRTCFunction("rtc_apply_transitions", RTC_APPLY_TRANSITIONS)
    
    # Добавляем функцию в слой
    layer.addAgentFunction(rtc_func)
    
    print("  RTC модуль state_manager_v2 зарегистрирован")
    
    # Теперь нужно обновить RTC функции состояний, чтобы они проверяли status_id
    # вместо использования FLAME GPU States
