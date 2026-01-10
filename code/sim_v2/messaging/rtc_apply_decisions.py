#!/usr/bin/env python3
"""
RTC модуль: Применение QuotaDecision планерами

Каждый планер читает QuotaDecision сообщения и применяет решение:
- action=1 (DEMOTE): intent → 3
- action=2 (PROMOTE): intent → 2
"""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

import pyflamegpu as fg


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции применения QuotaDecision для всех состояний"""
    
    print("  📥 Регистрация модуля: apply_decisions (QuotaDecision сообщения)")
    
    # ═══════════════════════════════════════════════════════════════════════
    # RTC функция применения решений
    # ═══════════════════════════════════════════════════════════════════════
    
    RTC_APPLY_DECISIONS = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_decision, flamegpu::MessageBruteForce, flamegpu::MessageNone) {
    const unsigned int my_idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    
    // Читаем все сообщения и ищем своё
    for (const auto& msg : FLAMEGPU->message_in) {
        const unsigned short msg_idx = msg.getVariable<unsigned short>("idx");
        
        if (msg_idx == (unsigned short)my_idx) {
            const unsigned char action = msg.getVariable<unsigned char>("action");
            
            if (action == 1u) {
                // DEMOTE: intent → 3 (serviceable)
                FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
                
                if (step_day < 10u || step_day == 180u) {
                    const unsigned int acn = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
                    printf("  [APPLY Day %u] AC %u (idx=%u): DEMOTE intent→3\\n", 
                           step_day, acn, my_idx);
                }
            }
            else if (action == 2u) {
                // PROMOTE: intent → 2 (operations)
                FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
                
                if (step_day < 10u || step_day == 180u) {
                    const unsigned int acn = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
                    printf("  [APPLY Day %u] AC %u (idx=%u): PROMOTE intent→2\\n", 
                           step_day, acn, my_idx);
                }
            }
            
            break;  // Нашли своё решение
        }
    }
    
    return flamegpu::ALIVE;
}
"""
    
    # Регистрируем для каждого состояния
    # ВАЖНО: FLAME GPU не позволяет нескольким функциям в одном слое
    # читать из одного MessageList. Используем отдельные слои.
    states = ["inactive", "operations", "serviceable", "repair", "reserve", "storage"]
    
    for i, state_name in enumerate(states):
        layer = model.newLayer(f"apply_decisions_{state_name}")
        func_name = f"rtc_apply_decision_{state_name}"
        rtc_func = agent.newRTCFunction(func_name, RTC_APPLY_DECISIONS)
        rtc_func.setInitialState(state_name)
        rtc_func.setEndState(state_name)
        rtc_func.setMessageInput("QuotaDecision")
        layer.addAgentFunction(rtc_func)
    
    print(f"    ✅ Зарегистрировано {len(states)} функций применения решений (каждая в своём слое)")

