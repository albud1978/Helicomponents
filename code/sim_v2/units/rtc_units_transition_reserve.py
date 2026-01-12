"""
RTC модуль переходов из reserve (агрегаты)
Аналог rtc_state_manager_reserve планеров

Переходы:
- 5→5 (reserve → reserve) при intent=5 или если нет запроса
- 5→2 (reserve → operations) при FIFO-выборе
- 5→3 (reserve → serviceable) при spawn (intent=3)

Дата: 05.01.2026
"""

import pyflamegpu as fg

MAX_FRAMES = 10000  # Будет заменено из env


RTC_CODE_5_TO_5 = """
// 5→5 (остаёмся в reserve)
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_5_to_5, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}
"""


RTC_CODE_5_TO_2 = """
// 5→2 (reserve → operations)
// Основная логика в rtc_fifo_assign_reserve — здесь только transition
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_5_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {
    // aircraft_number и intent_state уже установлены в rtc_fifo_assign_reserve
    FLAMEGPU->setVariable<unsigned int>("transition_5_to_2", 1u);
    return flamegpu::ALIVE;
}
"""


RTC_CODE_5_TO_3 = """
// 5→3 (reserve → serviceable) для spawn-агентов
// Срабатывает после rtc_fifo_spawn_activate, который установил intent=3
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_5_to_3, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Сбрасываем intent после перехода
    FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
    return flamegpu::ALIVE;
}
"""


RTC_COND_RESERVE_STAY = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_reserve_stay) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    return intent == 5u || intent == 0u;  // stay если intent=5 или не определён
}
"""

RTC_COND_RESERVE_TO_OPS = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_reserve_to_ops) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
}
"""

RTC_COND_RESERVE_TO_SVC = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_reserve_to_svc) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 3u;
}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription, max_frames: int = 10000):
    """Регистрирует RTC функции переходов из reserve"""
    
    # 5→5 (остаёмся)
    fn_5_to_5 = agent.newRTCFunction("rtc_units_apply_5_to_5", RTC_CODE_5_TO_5)
    fn_5_to_5.setRTCFunctionCondition(RTC_COND_RESERVE_STAY)
    fn_5_to_5.setInitialState("reserve")
    fn_5_to_5.setEndState("reserve")
    
    # 5→2 (reserve → operations)
    fn_5_to_2 = agent.newRTCFunction("rtc_units_apply_5_to_2", RTC_CODE_5_TO_2)
    fn_5_to_2.setRTCFunctionCondition(RTC_COND_RESERVE_TO_OPS)
    fn_5_to_2.setInitialState("reserve")
    fn_5_to_2.setEndState("operations")  # СМЕНА СОСТОЯНИЯ!
    
    # 5→3 (reserve → serviceable) для spawn
    fn_5_to_3 = agent.newRTCFunction("rtc_units_apply_5_to_3", RTC_CODE_5_TO_3)
    fn_5_to_3.setRTCFunctionCondition(RTC_COND_RESERVE_TO_SVC)
    fn_5_to_3.setInitialState("reserve")
    fn_5_to_3.setEndState("serviceable")  # СМЕНА СОСТОЯНИЯ!
    
    # Слои — сначала выходы, потом stay
    layer_5_to_2 = model.newLayer("layer_units_reserve_5_to_2")
    layer_5_to_2.addAgentFunction(fn_5_to_2)
    
    layer_5_to_3 = model.newLayer("layer_units_reserve_5_to_3")
    layer_5_to_3.addAgentFunction(fn_5_to_3)
    
    layer_5_to_5 = model.newLayer("layer_units_reserve_5_to_5")
    layer_5_to_5.addAgentFunction(fn_5_to_5)
    
    print("  RTC модуль units_transition_reserve зарегистрирован (3 слоя: 5→2, 5→3, 5→5)")

