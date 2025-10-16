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
    # Переход 3→2: serviceable → operations
    # Обрабатывает оба случая:
    #   - intent=2: агент выбран на промоут (из quota_promote_serviceable)
    #   - intent=3: демутированный агент возвращается в operations
    # ═════════════════════════════════════════════════════════════════════════
    RTC_APPLY_3_TO_2 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_3_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent_state = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // ✅ Работаем ТОЛЬКО с intent=2 (промутированные агенты, одобренные на операции)
    // intent=3 (холдинг) остаются в serviceable
    if (intent_state == 2u) {
        // Переходим в operations
        return flamegpu::ALIVE;  // Агент перейдёт в operations с intent=2
    }
    
    // Если intent!=2, агент остаётся в serviceable
    return flamegpu::ALIVE;
}
"""
    
    rtc_func_3_to_2 = agent.newRTCFunction("rtc_apply_3_to_2", RTC_APPLY_3_TO_2)
    rtc_func_3_to_2.setInitialState("serviceable")
    rtc_func_3_to_2.setEndState("operations")
    rtc_func_3_to_2.setAllowAgentDeath(False)
    
    layer_3_to_2 = model.newLayer("transition_3_to_2")
    layer_3_to_2.addAgentFunction(rtc_func_3_to_2)
    
    print("  Регистрация state manager для serviceable (3→2, оба случая)")

