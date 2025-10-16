"""
State Manager для переходов из состояния repair
Обрабатывает: 4->4 (остаться в ремонте), 4->5 (переход в резерв)
"""

import pyflamegpu as fg

# Условие для intent_state == 4 (остаться в repair)
RTC_COND_INTENT_4 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_4) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 4u;
}
"""

# Условие для intent_state == 5 (переход в reserve)
RTC_COND_INTENT_5 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_5) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 5u;
}
"""

# Функция для агентов, остающихся в repair (4->4)
RTC_APPLY_4_TO_4 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_4_to_4, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Агенты остаются в repair
    return flamegpu::ALIVE;
}
"""

# Функция для перехода repair -> reserve (4->5)
RTC_APPLY_4_TO_5 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_4_to_5, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    
    printf("  [TRANSITION 4→5 Day %u] AC %u (idx %u): repair -> reserve, repair_days=%u/%u\\n", 
           step_day, aircraft_number, idx, repair_days, repair_time);
    
    // ✅ Устанавливаем intent=5 (reserve) - это сигнал что из ремонта вышли
    FLAMEGPU->setVariable<unsigned int>("intent_state", 5u);
    
    // Сбрасываем счетчики при переходе в резерв
    FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
    FLAMEGPU->setVariable<unsigned int>("assembly_trigger", 0u);
    
    return flamegpu::ALIVE;
}
"""

def register_state_manager_repair(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует state manager для переходов из repair"""
    print("  Регистрация state manager для repair (4→4, 4→5)")
    
    # Layer 1: Transition 4->4 (stay in repair)
    layer_4_to_4 = model.newLayer("transition_4_to_4")
    rtc_func_4_to_4 = agent.newRTCFunction("rtc_apply_4_to_4", RTC_APPLY_4_TO_4)
    rtc_func_4_to_4.setRTCFunctionCondition(RTC_COND_INTENT_4)
    rtc_func_4_to_4.setInitialState("repair")
    rtc_func_4_to_4.setEndState("repair")
    layer_4_to_4.addAgentFunction(rtc_func_4_to_4)
    
    # Layer 2: Transition 4->5 (repair -> reserve)
    layer_4_to_5 = model.newLayer("transition_4_to_5")
    rtc_func_4_to_5 = agent.newRTCFunction("rtc_apply_4_to_5", RTC_APPLY_4_TO_5)
    rtc_func_4_to_5.setRTCFunctionCondition(RTC_COND_INTENT_5)
    rtc_func_4_to_5.setInitialState("repair")
    rtc_func_4_to_5.setEndState("reserve")
    layer_4_to_5.addAgentFunction(rtc_func_4_to_5)
    
    print("  RTC модуль state_manager_repair зарегистрирован")
