#!/usr/bin/env python3
"""
State Manager для состояния reserve (5).
Обрабатывает переход 5→5 без побочных эффектов.
Просто подтверждает, что агент остаётся в reserve.
"""

import pyflamegpu as fg


# Условие для intent_state == 5 (остаться в reserve)
RTC_COND_INTENT_5 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_5) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 5u;
}
"""


# Функция для агентов, остающихся в reserve (5->5)
RTC_APPLY_5_TO_5 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_5_to_5, flamegpu::MessageNone, flamegpu::MessageNone) {
    // В состоянии reserve (холдинг) изменений не происходит; агент остаётся в reserve
    return flamegpu::ALIVE;
}
"""


def register_state_manager_reserve(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует state manager для состояния reserve (5→5)."""
    print("  Регистрация state manager для reserve (5→5)")

    layer_5_to_5 = model.newLayer("transition_5_to_5")
    rtc_func_5_to_5 = agent.newRTCFunction("rtc_apply_5_to_5", RTC_APPLY_5_TO_5)
    rtc_func_5_to_5.setRTCFunctionCondition(RTC_COND_INTENT_5)
    rtc_func_5_to_5.setInitialState("reserve")
    rtc_func_5_to_5.setEndState("reserve")
    layer_5_to_5.addAgentFunction(rtc_func_5_to_5)

    print("  RTC модуль state_manager_reserve зарегистрирован")
