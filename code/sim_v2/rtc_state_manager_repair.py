"""
State Manager для переходов из состояния repair
Обрабатывает: 4->4 (остаться в ремонте), 4->2 (переход в operations)
"""

import pyflamegpu as fg

# Условие для intent_state == 4 (остаться в repair)
RTC_COND_INTENT_4 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_4) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 4u;
}
"""

# Условие для intent_state == 2 (переход в operations)
RTC_COND_INTENT_2 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_2) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
}
"""

# Функция для агентов, остающихся в repair (4->4)
RTC_APPLY_4_TO_4 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_4_to_4, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Агенты остаются в repair
    return flamegpu::ALIVE;
}
"""

# Функция для перехода repair -> operations (4->2)
RTC_APPLY_4_TO_2 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_4_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    // Логирование перехода (4→5) с типом вертолёта
    const char* type = (group_by == 1u) ? "Mi-8" : (group_by == 2u) ? "Mi-17" : "Unknown";
    printf("  [TRANSITION 4→2 Day %u] AC %u (idx %u, %s): repair -> operations, repair_days=%u/%u\\n", 
           step_day, aircraft_number, idx, type, repair_days, repair_time);
    
    // Сбрасываем счетчики при переходе в operations
    FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
    FLAMEGPU->setVariable<unsigned int>("assembly_trigger", 0u);
    
    // ✅ Инкремент s4_days (продолжаем счёт repair+reserve)
    unsigned int s4_days = FLAMEGPU->getVariable<unsigned int>("s4_days");
    s4_days++;
    FLAMEGPU->setVariable<unsigned int>("s4_days", s4_days);
    
    return flamegpu::ALIVE;
}
"""

def register_state_manager_repair(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует state manager для переходов из repair"""
    print("  Регистрация state manager для repair (4→4, 4→2)")
    
    # Layer 1: Transition 4->4 (stay in repair)
    layer_4_to_4 = model.newLayer("transition_4_to_4")
    rtc_func_4_to_4 = agent.newRTCFunction("rtc_apply_4_to_4", RTC_APPLY_4_TO_4)
    rtc_func_4_to_4.setRTCFunctionCondition(RTC_COND_INTENT_4)
    rtc_func_4_to_4.setInitialState("repair")
    rtc_func_4_to_4.setEndState("repair")
    layer_4_to_4.addAgentFunction(rtc_func_4_to_4)
    
    # Layer 2: Transition 4->2 (repair -> operations)
    layer_4_to_2 = model.newLayer("transition_4_to_2")
    rtc_func_4_to_2 = agent.newRTCFunction("rtc_apply_4_to_2", RTC_APPLY_4_TO_2)
    rtc_func_4_to_2.setRTCFunctionCondition(RTC_COND_INTENT_2)
    rtc_func_4_to_2.setInitialState("repair")
    rtc_func_4_to_2.setEndState("operations")
    layer_4_to_2.addAgentFunction(rtc_func_4_to_2)
    
    print("  RTC модуль state_manager_repair зарегистрирован")
