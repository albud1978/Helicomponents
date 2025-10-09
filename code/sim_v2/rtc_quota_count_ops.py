#!/usr/bin/env python3
"""
RTC модуль для подсчёта агентов в operations и serviceable
Должен выполняться ПЕРЕД квотированием

ВАЖНО: Первый агент (idx=0) сбрасывает ВСЕ буферы перед подсчётом!
"""

import pyflamegpu as fg

def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Подсчёт агентов в operations для каждого group_by"""
    
    # =========================================================================
    # Слой 1: Обнуление буферов (выполняет только первый агент)
    # =========================================================================
    RTC_RESET_BUFFERS = """
FLAMEGPU_AGENT_FUNCTION(rtc_reset_quota_buffers, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Только первый агент (idx=0) обнуляет буферы
    if (idx == 0u) {
        auto mi8_ops = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi8_ops_count");
        auto mi17_ops = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_ops_count");
        auto mi8_svc = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi8_svc_count");
        auto mi17_svc = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_svc_count");
        for (unsigned int i = 0u; i < 286u; ++i) {
            mi8_ops[i].exchange(0u);
            mi17_ops[i].exchange(0u);
            mi8_svc[i].exchange(0u);
            mi17_svc[i].exchange(0u);
        }
    }
    
    return flamegpu::ALIVE;
}
"""
    
    rtc_reset = agent.newRTCFunction("rtc_reset_quota_buffers", RTC_RESET_BUFFERS)
    rtc_reset.setInitialState("operations")
    rtc_reset.setEndState("operations")
    
    layer_reset = model.newLayer("reset_quota_buffers")
    layer_reset.addAgentFunction(rtc_reset)
    
    # =========================================================================
    # Слой 2: Подсчёт агентов в operations
    # =========================================================================
    RTC_COUNT_OPS = """
FLAMEGPU_AGENT_FUNCTION(rtc_count_ops, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Записываем флаг что этот агент в operations
    if (group_by == 1u) {
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi8_ops_count");
        ops_count[idx].exchange(1u);
    } else if (group_by == 2u) {
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_ops_count");
        ops_count[idx].exchange(1u);
    }
    
    return flamegpu::ALIVE;
}
"""
    
    rtc_func = agent.newRTCFunction("rtc_count_ops", RTC_COUNT_OPS)
    rtc_func.setInitialState("operations")
    rtc_func.setEndState("operations")
    
    layer_count = model.newLayer("count_ops")
    layer_count.addAgentFunction(rtc_func)
    
    # =========================================================================
    # Слой 3: Подсчёт агентов в serviceable
    # =========================================================================
    RTC_COUNT_SVC = """
FLAMEGPU_AGENT_FUNCTION(rtc_count_serviceable, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Записываем флаг что этот агент в serviceable
    if (group_by == 1u) {
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi8_svc_count");
        svc_count[idx].exchange(1u);
    } else if (group_by == 2u) {
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_svc_count");
        svc_count[idx].exchange(1u);
    }
    
    return flamegpu::ALIVE;
}
"""
    
    rtc_func_svc = agent.newRTCFunction("rtc_count_serviceable", RTC_COUNT_SVC)
    rtc_func_svc.setInitialState("serviceable")
    rtc_func_svc.setEndState("serviceable")
    
    layer_count_svc = model.newLayer("count_serviceable")
    layer_count_svc.addAgentFunction(rtc_func_svc)
    
    print("  RTC модуль count_ops зарегистрирован (обнуление + подсчёт operations + serviceable)")

