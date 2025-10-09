#!/usr/bin/env python3
"""
State Manager для serviceable (state=3)

Обрабатывает переход: 3→2 (serviceable → operations) на основе intent_state
"""

import pyflamegpu as fg

def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """
    Регистрирует RTC функции для управления переходами из serviceable
    """
    
    # ═════════════════════════════════════════════════════════════════════════
    # Переход 3→2: serviceable → operations (по одобрению промоута)
    # ═════════════════════════════════════════════════════════════════════════
    RTC_APPLY_3_TO_2 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_3_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent_state = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // Если intent=2 (промоут одобрен), переходим в operations
    if (intent_state == 2u) {
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
        
        // Логирование для новых spawn агентов и ключевых дней
        if (aircraft_number >= 100000u || day == 226u || day == 227u || day == 228u) {
            printf("  [TRANSITION 3→2 Day %u] AC %u (idx %u): serviceable -> operations\\n", 
                   day, aircraft_number, idx);
        }
        
        // Переход в operations происходит через setEndState()
        // НЕ меняем state напрямую!
        FLAMEGPU->setVariable<unsigned int>("intent_state", 0u);  // Сброс intent
        return flamegpu::ALIVE;  // Агент перейдёт в operations автоматически
    }
    
    // Если intent!=2, агент остаётся в serviceable
    return flamegpu::DEAD;  // НЕ переходим
}
"""
    
    rtc_func_3_to_2 = agent.newRTCFunction("rtc_apply_3_to_2", RTC_APPLY_3_TO_2)
    rtc_func_3_to_2.setInitialState("serviceable")
    rtc_func_3_to_2.setEndState("operations")
    rtc_func_3_to_2.setAllowAgentDeath(False)
    
    layer_3_to_2 = model.newLayer("transition_3_to_2")
    layer_3_to_2.addAgentFunction(rtc_func_3_to_2)
    
    print("  Регистрация state manager для serviceable (3→2)")

