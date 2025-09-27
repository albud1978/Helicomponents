"""
State Manager для состояния storage (6).
Обрабатывает тривиальный переход 6 -> 6 без побочных эффектов.
"""

import pyflamegpu as fg


# Условие для intent_state == 6 (остаться в storage)
RTC_COND_INTENT_6 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_6) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 6u;
}
"""


# Функция для агентов, остающихся в storage (6->6)
RTC_APPLY_6_TO_6 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_6_to_6, flamegpu::MessageNone, flamegpu::MessageNone) {
    // В состоянии storage изменений не происходит; агент остаётся в storage
    return flamegpu::ALIVE;
}
"""


def register_state_manager_storage(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует state manager для состояния storage (6→6)."""
    print("  Регистрация state manager для storage (6→6)")

    layer_6_to_6 = model.newLayer("transition_6_to_6")
    rtc_func_6_to_6 = agent.newRTCFunction("rtc_apply_6_to_6", RTC_APPLY_6_TO_6)
    rtc_func_6_to_6.setRTCFunctionCondition(RTC_COND_INTENT_6)
    rtc_func_6_to_6.setInitialState("storage")
    rtc_func_6_to_6.setEndState("storage")
    layer_6_to_6.addAgentFunction(rtc_func_6_to_6)

    print("  RTC модуль state_manager_storage зарегистрирован")


