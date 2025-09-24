"""
RTC модуль для менеджера состояний с промежуточными состояниями
Это правильное решение в рамках архитектуры FLAME GPU
"""

import pyflamegpu as fg

# Функции для перехода в промежуточные состояния
# Эти функции устанавливают промежуточное состояние на основе intent_state

def register_state_manager_intermediate(model: fg.ModelDescription, agent: fg.AgentDescription):
    """
    Регистрирует менеджер состояний с промежуточными состояниями
    
    Args:
        model: FLAME GPU ModelDescription
        agent: FLAME GPU AgentDescription
    """
    print("  Регистрация state manager с промежуточными состояниями")
    
    # Сначала нужно добавить промежуточные состояния в модель
    intermediate_states = [
        "pending_operations_from_inactive",
        "pending_operations_from_serviceable", 
        "pending_operations_from_reserve",
        "pending_repair",
        "pending_storage",
        "pending_reserve"
    ]
    
    for state_name in intermediate_states:
        agent.newState(state_name)
        print(f"    Добавлено промежуточное состояние: {state_name}")
    
    # Слой 1: Переходы в промежуточные состояния
    layer1 = model.newLayer("to_intermediate_states")
    
    # 1->pending_operations_from_inactive
    func_1_to_pending = agent.newRTCFunction("rtc_1_to_pending_ops", """
FLAMEGPU_AGENT_FUNCTION(rtc_1_to_pending_ops, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    if (intent == 2u) {  // хочет в operations
        const unsigned int step_day = FLAMEGPU->getStepCounter();
        const unsigned int ac = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        printf("  [Step %u] AC %u: inactive -> pending_operations\\n", step_day, ac);
    }
    return flamegpu::ALIVE;
}
""")
    func_1_to_pending.setInitialState("inactive")
    func_1_to_pending.setEndState("pending_operations_from_inactive")
    layer1.addAgentFunction(func_1_to_pending)
    
    # 2->pending_repair
    func_2_to_repair = agent.newRTCFunction("rtc_2_to_pending_repair", """
FLAMEGPU_AGENT_FUNCTION(rtc_2_to_pending_repair, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    if (intent == 4u) {  // хочет в repair
        const unsigned int step_day = FLAMEGPU->getStepCounter();
        const unsigned int ac = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        printf("  [Step %u] AC %u: operations -> pending_repair\\n", step_day, ac);
    }
    return flamegpu::ALIVE;
}
""")
    func_2_to_repair.setInitialState("operations")
    func_2_to_repair.setEndState("pending_repair")
    layer1.addAgentFunction(func_2_to_repair)
    
    # 2->pending_storage
    func_2_to_storage = agent.newRTCFunction("rtc_2_to_pending_storage", """
FLAMEGPU_AGENT_FUNCTION(rtc_2_to_pending_storage, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    if (intent == 6u) {  // хочет в storage
        const unsigned int step_day = FLAMEGPU->getStepCounter();
        const unsigned int ac = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        printf("  [Step %u] AC %u: operations -> pending_storage\\n", step_day, ac);
    }
    return flamegpu::ALIVE;
}
""")
    func_2_to_storage.setInitialState("operations")
    func_2_to_storage.setEndState("pending_storage")
    layer1.addAgentFunction(func_2_to_storage)
    
    # 3->pending_operations_from_serviceable
    func_3_to_pending = agent.newRTCFunction("rtc_3_to_pending_ops", """
FLAMEGPU_AGENT_FUNCTION(rtc_3_to_pending_ops, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    if (intent == 2u) {  // хочет в operations
        const unsigned int step_day = FLAMEGPU->getStepCounter();
        const unsigned int ac = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        printf("  [Step %u] AC %u: serviceable -> pending_operations\\n", step_day, ac);
    }
    return flamegpu::ALIVE;
}
""")
    func_3_to_pending.setInitialState("serviceable")
    func_3_to_pending.setEndState("pending_operations_from_serviceable")
    layer1.addAgentFunction(func_3_to_pending)
    
    # 4->pending_reserve
    func_4_to_reserve = agent.newRTCFunction("rtc_4_to_pending_reserve", """
FLAMEGPU_AGENT_FUNCTION(rtc_4_to_pending_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    if (intent == 5u) {  // хочет в reserve
        const unsigned int step_day = FLAMEGPU->getStepCounter();
        const unsigned int ac = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        printf("  [Step %u] AC %u: repair -> pending_reserve\\n", step_day, ac);
        // Сбрасываем счетчики
        FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
        FLAMEGPU->setVariable<unsigned int>("assembly_trigger", 0u);
    }
    return flamegpu::ALIVE;
}
""")
    func_4_to_reserve.setInitialState("repair")
    func_4_to_reserve.setEndState("pending_reserve")
    layer1.addAgentFunction(func_4_to_reserve)
    
    # 5->pending_operations_from_reserve
    func_5_to_pending = agent.newRTCFunction("rtc_5_to_pending_ops", """
FLAMEGPU_AGENT_FUNCTION(rtc_5_to_pending_ops, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    if (intent == 2u) {  // хочет в operations
        const unsigned int step_day = FLAMEGPU->getStepCounter();
        const unsigned int ac = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        printf("  [Step %u] AC %u: reserve -> pending_operations\\n", step_day, ac);
    }
    return flamegpu::ALIVE;
}
""")
    func_5_to_pending.setInitialState("reserve")
    func_5_to_pending.setEndState("pending_operations_from_reserve")
    layer1.addAgentFunction(func_5_to_pending)
    
    # Слой 2: Переходы из промежуточных в финальные состояния
    layer2 = model.newLayer("from_intermediate_states")
    
    # pending_operations_* -> operations
    for source in ["inactive", "serviceable", "reserve"]:
        func_name = f"rtc_pending_ops_from_{source}_to_ops"
        func = agent.newRTCFunction(func_name, """
FLAMEGPU_AGENT_FUNCTION(%s, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int ac = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    printf("  [Step %%u] AC %%u: TRANSITION completed -> operations\\n", step_day, ac);
    return flamegpu::ALIVE;
}
""" % func_name)
        func.setInitialState(f"pending_operations_from_{source}")
        func.setEndState("operations")
        layer2.addAgentFunction(func)
    
    # pending_repair -> repair
    func_to_repair = agent.newRTCFunction("rtc_pending_to_repair", """
FLAMEGPU_AGENT_FUNCTION(rtc_pending_to_repair, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int ac = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    printf("  [Step %u] AC %u: TRANSITION completed -> repair\\n", step_day, ac);
    return flamegpu::ALIVE;
}
""")
    func_to_repair.setInitialState("pending_repair")
    func_to_repair.setEndState("repair")
    layer2.addAgentFunction(func_to_repair)
    
    # pending_storage -> storage
    func_to_storage = agent.newRTCFunction("rtc_pending_to_storage", """
FLAMEGPU_AGENT_FUNCTION(rtc_pending_to_storage, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int ac = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    printf("  [Step %u] AC %u: TRANSITION completed -> storage\\n", step_day, ac);
    return flamegpu::ALIVE;
}
""")
    func_to_storage.setInitialState("pending_storage")
    func_to_storage.setEndState("storage")
    layer2.addAgentFunction(func_to_storage)
    
    # pending_reserve -> reserve
    func_to_reserve = agent.newRTCFunction("rtc_pending_to_reserve", """
FLAMEGPU_AGENT_FUNCTION(rtc_pending_to_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int ac = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    printf("  [Step %u] AC %u: TRANSITION completed -> reserve\\n", step_day, ac);
    return flamegpu::ALIVE;
}
""")
    func_to_reserve.setInitialState("pending_reserve")
    func_to_reserve.setEndState("reserve")
    layer2.addAgentFunction(func_to_reserve)
    
    print("  RTC модуль state_manager с промежуточными состояниями зарегистрирован")
