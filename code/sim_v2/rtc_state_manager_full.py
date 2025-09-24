"""
Полный state manager для обработки всех детерминированных переходов
Каждый переход в отдельном слое для избежания конфликтов
"""

try:
    import pyflamegpu as fg
except ImportError:
    class fg:
        class ModelDescription: pass
        class AgentDescription: pass

# Константы
MAX_DAYS = 4000
MAX_FRAMES = 286
MAX_SIZE = MAX_FRAMES * (MAX_DAYS + 1)


def register_state_manager_full(model: fg.ModelDescription, agent: fg.AgentDescription):
    """
    Регистрирует полный state manager с отдельными слоями для каждого перехода
    """
    print("  Регистрация полного state manager")
    
    # Слой 1: Переход 2->6 (operations -> storage)
    rtc_2_to_6 = """
FLAMEGPU_AGENT_FUNCTION(rtc_transition_2_to_6, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // Только для агентов с intent = 6
    if (intent == 6u) {
        const unsigned int step_day = FLAMEGPU->getStepCounter();
        const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        
        printf("  [Step %u] AC %u: TRANSITION operations -> storage (intent=6)\\n", 
               step_day, aircraft_number);
        
        return flamegpu::ALIVE;
    }
    
    return flamegpu::ALIVE;
}
"""
    
    # Слой 2: Переход 2->4 (operations -> repair)
    rtc_2_to_4 = """
FLAMEGPU_AGENT_FUNCTION(rtc_transition_2_to_4, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // Только для агентов с intent = 4
    if (intent == 4u) {
        const unsigned int step_day = FLAMEGPU->getStepCounter();
        const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        
        printf("  [Step %u] AC %u: TRANSITION operations -> repair (intent=4)\\n", 
               step_day, aircraft_number);
        
        // Инициализация repair_days при входе в ремонт
        FLAMEGPU->setVariable<unsigned int>("repair_days", 1u);
        
        return flamegpu::ALIVE;
    }
    
    return flamegpu::ALIVE;
}
"""
    
    # Слой 3: Переход 4->5 (repair -> reserve)
    rtc_4_to_5 = """
FLAMEGPU_AGENT_FUNCTION(rtc_transition_4_to_5, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // Только для агентов с intent = 5
    if (intent == 5u) {
        const unsigned int step_day = FLAMEGPU->getStepCounter();
        const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        
        printf("  [Step %u] AC %u: TRANSITION repair -> reserve (intent=5)\\n", 
               step_day, aircraft_number);
        
        // Сброс уже сделан в RTC функции state_4
        
        return flamegpu::ALIVE;
    }
    
    return flamegpu::ALIVE;
}
"""
    
    # Слой 4: Остающиеся в том же состоянии (1->1, 2->2, 3->3, 4->4, 5->5, 6->6)
    rtc_stay = """
FLAMEGPU_AGENT_FUNCTION(rtc_stay_in_state, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    const unsigned int current_status = FLAMEGPU->getVariable<unsigned int>("status_id");
    
    // Проверяем что intent совпадает с текущим статусом (остаёмся)
    if (intent == current_status) {
        // Ничего не делаем, агент остаётся в своём состоянии
        return flamegpu::ALIVE;
    }
    
    return flamegpu::ALIVE;
}
"""
    
    try:
        # Создаём функции и слои
        
        # Слой 1: 2->6
        func_2_to_6 = agent.newRTCFunction("rtc_transition_2_to_6", rtc_2_to_6)
        func_2_to_6.setInitialState("operations")
        func_2_to_6.setEndState("storage")
        
        layer_2_to_6 = model.newLayer("state_manager_2_to_6")
        layer_2_to_6.addAgentFunction(func_2_to_6)
        
        # Слой 2: 2->4
        func_2_to_4 = agent.newRTCFunction("rtc_transition_2_to_4", rtc_2_to_4)
        func_2_to_4.setInitialState("operations")
        func_2_to_4.setEndState("repair")
        
        layer_2_to_4 = model.newLayer("state_manager_2_to_4")
        layer_2_to_4.addAgentFunction(func_2_to_4)
        
        # Слой 3: 4->5
        func_4_to_5 = agent.newRTCFunction("rtc_transition_4_to_5", rtc_4_to_5)
        func_4_to_5.setInitialState("repair")
        func_4_to_5.setEndState("reserve")
        
        layer_4_to_5 = model.newLayer("state_manager_4_to_5")
        layer_4_to_5.addAgentFunction(func_4_to_5)
        
        # Слои для остающихся в том же состоянии
        states = [
            ("inactive", 1),
            ("operations", 2),
            ("serviceable", 3),
            ("repair", 4),
            ("reserve", 5),
            ("storage", 6)
        ]
        
        for state_name, state_id in states:
            func_name = f"rtc_stay_{state_name}"
            func_code = f"""
FLAMEGPU_AGENT_FUNCTION({func_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // Только для агентов с intent = {state_id} (остаются в {state_name})
    if (intent == {state_id}u) {{
        return flamegpu::ALIVE;
    }}
    
    return flamegpu::ALIVE;
}}
"""
            func = agent.newRTCFunction(func_name, func_code)
            func.setInitialState(state_name)
            func.setEndState(state_name)
            
            layer = model.newLayer(f"state_manager_stay_{state_name}")
            layer.addAgentFunction(func)
        
        print("  RTC модуль state_manager_full зарегистрирован (9 слоёв)")
        
    except Exception as e:
        print(f"  Ошибка регистрации state_manager_full: {e}")
