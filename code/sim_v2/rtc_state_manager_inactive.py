#!/usr/bin/env python3
"""
State Manager для состояния inactive (1).
Обрабатывает переход 1→1 без побочных эффектов.
Просто подтверждает, что агент остаётся в inactive.
"""

import pyflamegpu as fg


# Условие для intent_state == 1 (остаться в inactive)
RTC_COND_INTENT_1 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_1) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 1u;
}
"""


# Функция для агентов, остающихся в inactive (1->1)
RTC_APPLY_1_TO_1 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_1_to_1, flamegpu::MessageNone, flamegpu::MessageNone) {
    // В состоянии inactive (замороженные) изменений не происходит; агент остаётся в inactive
    return flamegpu::ALIVE;
}
"""


def register_state_manager_inactive(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует state manager для состояния inactive (1→1)."""
    print("  Регистрация state manager для inactive (1→1)")

    layer_1_to_1 = model.newLayer("transition_1_to_1")
    rtc_func_1_to_1 = agent.newRTCFunction("rtc_apply_1_to_1", RTC_APPLY_1_TO_1)
    rtc_func_1_to_1.setRTCFunctionCondition(RTC_COND_INTENT_1)
    rtc_func_1_to_1.setInitialState("inactive")
    rtc_func_1_to_1.setEndState("inactive")
    layer_1_to_1.addAgentFunction(rtc_func_1_to_1)

    print("  RTC модуль state_manager_inactive зарегистрирован")
