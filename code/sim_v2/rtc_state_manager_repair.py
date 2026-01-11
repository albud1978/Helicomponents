"""
State Manager для переходов из состояния repair/unserviceable

V3 архитектура:
- 4→4: остаться в repair (ожидание завершения repair_time)
- 4→2: unserviceable → operations (НАПРЯМУЮ, с PPR=0)
- 4→5: repair → reserve (только для baseline совместимости)

В V3: reserve используется ТОЛЬКО для spawn pool!
"""

import pyflamegpu as fg

# Условие для intent_state == 4 (остаться в repair)
RTC_COND_INTENT_4 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_4) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 4u;
}
"""

# Условие для intent_state == 2 (переход в operations) — V3!
RTC_COND_INTENT_2 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_2_from_repair) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
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

# Функция для перехода repair/unserviceable -> operations (4->2) — V3!
# КРИТИЧНО: При этом переходе PPR обнуляется (после ремонта)
RTC_APPLY_4_TO_2 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_4_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int ppr_old = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    // ✅ V3: PPR обнуляется при выходе из ремонта!
    FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
    
    // Сбрасываем счётчики ремонта
    FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
    FLAMEGPU->setVariable<unsigned int>("assembly_trigger", 0u);
    
    // Логирование перехода (4→2) с обнулением PPR
    const char* type = (group_by == 1u) ? "Mi-8" : (group_by == 2u) ? "Mi-17" : "Unknown";
    printf("  [TRANSITION 4→2 Day %u] AC %u (idx %u, %s): unserviceable -> operations, PPR: %u -> 0\\n", 
           step_day, aircraft_number, idx, type, ppr_old);
    
    return flamegpu::ALIVE;
}
"""

# Функция для перехода repair -> reserve (4->5)
RTC_APPLY_4_TO_5 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_4_to_5, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V3: используем current_day из Environment для совместимости с адаптивными шагами
    const unsigned int step_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    // Логирование перехода (4→5) с типом вертолёта
    const char* type = (group_by == 1u) ? "Mi-8" : (group_by == 2u) ? "Mi-17" : "Unknown";
    printf("  [TRANSITION 4→5 Day %u] AC %u (idx %u, %s): repair -> reserve, repair_days=%u/%u\\n", 
           step_day, aircraft_number, idx, type, repair_days, repair_time);
    
    // Сбрасываем счетчики при переходе в резерв
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
    """Регистрирует state manager для переходов из repair/unserviceable
    
    V3 архитектура:
    - 4→4: остаться в repair
    - 4→2: unserviceable → operations (V3! с PPR=0)
    - 4→5: repair → reserve (baseline совместимость, НЕ используется в V3)
    """
    print("  Регистрация state manager для repair (4→4, 4→2, 4→5)")
    
    # Layer 1: Transition 4->4 (stay in repair)
    layer_4_to_4 = model.newLayer("transition_4_to_4")
    rtc_func_4_to_4 = agent.newRTCFunction("rtc_apply_4_to_4", RTC_APPLY_4_TO_4)
    rtc_func_4_to_4.setRTCFunctionCondition(RTC_COND_INTENT_4)
    rtc_func_4_to_4.setInitialState("repair")
    rtc_func_4_to_4.setEndState("repair")
    layer_4_to_4.addAgentFunction(rtc_func_4_to_4)
    
    # Layer 2: Transition 4->2 (unserviceable -> operations) — V3!
    layer_4_to_2 = model.newLayer("transition_4_to_2")
    rtc_func_4_to_2 = agent.newRTCFunction("rtc_apply_4_to_2", RTC_APPLY_4_TO_2)
    rtc_func_4_to_2.setRTCFunctionCondition(RTC_COND_INTENT_2)
    rtc_func_4_to_2.setInitialState("repair")
    rtc_func_4_to_2.setEndState("operations")
    layer_4_to_2.addAgentFunction(rtc_func_4_to_2)
    
    # Layer 3: Transition 4->5 (repair -> reserve) — для baseline совместимости
    layer_4_to_5 = model.newLayer("transition_4_to_5")
    rtc_func_4_to_5 = agent.newRTCFunction("rtc_apply_4_to_5", RTC_APPLY_4_TO_5)
    rtc_func_4_to_5.setRTCFunctionCondition(RTC_COND_INTENT_5)
    rtc_func_4_to_5.setInitialState("repair")
    rtc_func_4_to_5.setEndState("reserve")
    layer_4_to_5.addAgentFunction(rtc_func_4_to_5)
    
    print("  ✅ RTC модуль state_manager_repair зарегистрирован (V3: 4→4, 4→2, 4→5)")
