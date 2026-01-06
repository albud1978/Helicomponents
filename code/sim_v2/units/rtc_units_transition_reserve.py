"""
RTC модуль переходов из reserve (агрегаты)
Аналог rtc_state_manager_reserve планеров

Переходы:
- 5→5 (reserve → reserve) при intent=5 или если нет запроса
- 5→2 (reserve → operations) при FIFO-выборе

Дата: 05.01.2026
"""

import pyflamegpu as fg

MAX_FRAMES = 10000  # Будет заменено из env


def get_rtc_code(max_frames: int) -> str:
    """Возвращает CUDA код для переходов из reserve"""
    return f"""
// 5→5 (остаёмся в reserve)
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_5_to_5, flamegpu::MessageNone, flamegpu::MessageNone) {{
    return flamegpu::ALIVE;
}}

// 5→2 (reserve → operations)
// Основная логика в rtc_fifo_assign_reserve — здесь только transition
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_5_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // aircraft_number и intent_state уже установлены в rtc_fifo_assign_reserve
    FLAMEGPU->setVariable<unsigned int>("transition_5_to_2", 1u);
    return flamegpu::ALIVE;
}}
"""


RTC_COND_RESERVE_STAY = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_reserve_stay) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 5u;
}
"""

RTC_COND_RESERVE_TO_OPS = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_reserve_to_ops) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription, max_frames: int = 10000):
    """Регистрирует RTC функции переходов из reserve"""
    rtc_code = get_rtc_code(max_frames)
    
    # 5→5
    fn_5_to_5 = agent.newRTCFunction("rtc_units_apply_5_to_5", rtc_code)
    fn_5_to_5.setRTCFunctionCondition(RTC_COND_RESERVE_STAY)
    fn_5_to_5.setInitialState("reserve")
    fn_5_to_5.setEndState("reserve")
    
    # 5→2
    fn_5_to_2 = agent.newRTCFunction("rtc_units_apply_5_to_2", rtc_code)
    fn_5_to_2.setRTCFunctionCondition(RTC_COND_RESERVE_TO_OPS)
    fn_5_to_2.setInitialState("reserve")
    fn_5_to_2.setEndState("operations")  # СМЕНА СОСТОЯНИЯ!
    
    # Слои
    layer_5_to_5 = model.newLayer("layer_units_reserve_5_to_5")
    layer_5_to_5.addAgentFunction(fn_5_to_5)
    
    layer_5_to_2 = model.newLayer("layer_units_reserve_5_to_2")
    layer_5_to_2.addAgentFunction(fn_5_to_2)
    
    print("  RTC модуль units_transition_reserve зарегистрирован (2 слоя)")

