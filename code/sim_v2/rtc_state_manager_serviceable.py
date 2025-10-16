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
    # Обрабатывает ТОЛЬКО intent=3 (холдинг в serviceable)
    # ═════════════════════════════════════════════════════════════════════════
    RTC_SERVICEABLE_HOLDING = """
FLAMEGPU_AGENT_FUNCTION(rtc_serviceable_holding_confirm, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent_state = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // ✅ Работаем ТОЛЬКО с intent=3 (агенты в холдинге в serviceable)
    // intent=2 обрабатывается в других слоях (quota_promote уже ставит intent=2)
    if (intent_state != 3u) {
        // intent!=3 - пропускаем
        return flamegpu::ALIVE;
    }
    
    // ✅ intent=3 в serviceable - остаёмся в serviceable
    // Эта RTC функция подтверждает, что агент остаётся в serviceable
    // (не переводит его в operations)
    return flamegpu::ALIVE;
}
"""
    
    rtc_func_holding = agent.newRTCFunction("rtc_serviceable_holding_confirm", RTC_SERVICEABLE_HOLDING)
    rtc_func_holding.setInitialState("serviceable")
    rtc_func_holding.setEndState("serviceable")  # ✅ Остаёмся в serviceable!
    rtc_func_holding.setAllowAgentDeath(False)
    
    layer_holding = model.newLayer("serviceable_holding_confirm")
    layer_holding.addAgentFunction(rtc_func_holding)
    
    print("  Регистрация state manager для serviceable (3→serviceable, холдинг)")

