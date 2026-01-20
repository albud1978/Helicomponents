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


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription, message_name: str = "QuotaDecision"):
    """Регистрирует RTC функции применения QuotaDecision для всех состояний"""
    
    print("  📥 Регистрация модуля: apply_decisions (QuotaDecision сообщения)")
    
    # ═══════════════════════════════════════════════════════════════════════
    # RTC функция применения решений
    # ═══════════════════════════════════════════════════════════════════════
    
    RTC_APPLY_DECISIONS = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_decision, flamegpu::MessageArray, flamegpu::MessageNone) {
    const unsigned int my_idx = FLAMEGPU->getVariable<unsigned int>("idx");
    auto msg = FLAMEGPU->message_in.at(my_idx);
    const unsigned char action = msg.getVariable<unsigned char>("action");
    const unsigned int line_id = msg.getVariable<unsigned int>("line_id");
    
    if (action == 1u) {
        // DEMOTE: ops -> svc
        FLAMEGPU->setVariable<unsigned int>("needs_demote", 1u);
    } else if (action == 2u) {
        // P1: svc -> ops
        FLAMEGPU->setVariable<unsigned int>("promoted", 1u);
    } else if (action == 3u) {
        // P2: unsvc -> ops (repair line assigned)
        FLAMEGPU->setVariable<unsigned int>("repair_candidate", 1u);
        FLAMEGPU->setVariable<unsigned int>("repair_line_id", line_id);
        FLAMEGPU->setVariable<unsigned int>("decision_p2", 1u);
    } else if (action == 4u) {
        // P3: inactive -> ops (repair line assigned)
        FLAMEGPU->setVariable<unsigned int>("repair_candidate", 1u);
        FLAMEGPU->setVariable<unsigned int>("repair_line_id", line_id);
        FLAMEGPU->setVariable<unsigned int>("decision_p3", 1u);
    }
    
    return flamegpu::ALIVE;
}
    """
    
    # Регистрируем для каждого состояния
    # ВАЖНО: FLAME GPU не позволяет нескольким функциям в одном слое
    # читать из одного MessageList. Используем отдельные слои.
    states = ["inactive", "operations", "serviceable", "repair", "reserve", "unserviceable"]
    
    for i, state_name in enumerate(states):
        layer = model.newLayer(f"apply_decisions_{state_name}")
        func_name = f"rtc_apply_decision_{state_name}"
        rtc_func = agent.newRTCFunction(func_name, RTC_APPLY_DECISIONS)
        rtc_func.setInitialState(state_name)
        rtc_func.setEndState(state_name)
        rtc_func.setMessageInput(message_name)
        layer.addAgentFunction(rtc_func)
    
    print(f"    ✅ Зарегистрировано {len(states)} функций применения решений (каждая в своём слое)")

