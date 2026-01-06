"""
RTC модуль переходов из serviceable (агрегаты)
Аналог rtc_state_manager_serviceable планеров

Переходы:
- 3→3 (serviceable → serviceable) при intent=3
- 3→2 (serviceable → operations) при FIFO-выборе

Дата: 05.01.2026
"""

import pyflamegpu as fg

MAX_FRAMES = 10000


def get_rtc_code(max_frames: int) -> str:
    """Возвращает CUDA код для переходов из serviceable"""
    return f"""
// 3→3 (остаёмся в serviceable)
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_3_to_3, flamegpu::MessageNone, flamegpu::MessageNone) {{
    return flamegpu::ALIVE;
}}

// 3→2 (serviceable → operations)
// Основная логика в rtc_fifo_assign_serviceable — здесь только transition
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_3_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // aircraft_number и intent_state уже установлены в rtc_fifo_assign_serviceable
    FLAMEGPU->setVariable<unsigned int>("transition_3_to_2", 1u);
    return flamegpu::ALIVE;
}}
"""


RTC_COND_SERVICEABLE_STAY = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_serviceable_stay) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 3u;
}
"""

RTC_COND_SERVICEABLE_TO_OPS = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_serviceable_to_ops) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription, max_frames: int = 10000):
    """Регистрирует RTC функции переходов из serviceable"""
    rtc_code = get_rtc_code(max_frames)
    
    # 3→3
    fn_3_to_3 = agent.newRTCFunction("rtc_units_apply_3_to_3", rtc_code)
    fn_3_to_3.setRTCFunctionCondition(RTC_COND_SERVICEABLE_STAY)
    fn_3_to_3.setInitialState("serviceable")
    fn_3_to_3.setEndState("serviceable")
    
    # 3→2
    fn_3_to_2 = agent.newRTCFunction("rtc_units_apply_3_to_2", rtc_code)
    fn_3_to_2.setRTCFunctionCondition(RTC_COND_SERVICEABLE_TO_OPS)
    fn_3_to_2.setInitialState("serviceable")
    fn_3_to_2.setEndState("operations")  # СМЕНА СОСТОЯНИЯ!
    
    # Слои
    layer_3_to_3 = model.newLayer("layer_units_serviceable_3_to_3")
    layer_3_to_3.addAgentFunction(fn_3_to_3)
    
    layer_3_to_2 = model.newLayer("layer_units_serviceable_3_to_2")
    layer_3_to_2.addAgentFunction(fn_3_to_2)
    
    print("  RTC модуль units_transition_serviceable зарегистрирован (2 слоя)")

