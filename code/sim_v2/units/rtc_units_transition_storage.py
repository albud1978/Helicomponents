"""
RTC модуль переходов из storage (агрегаты)

Storage — терминальное состояние!
Переходы:
- 6→6 (storage → storage) — всегда остаёмся

Дата: 06.01.2026
"""

import pyflamegpu as fg


def get_rtc_code() -> str:
    """Возвращает CUDA код для storage"""
    return """
// 6→6 (остаёмся в storage — терминальное состояние)
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_6_to_6, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Storage — терминальное состояние, агрегат здесь навсегда
    return flamegpu::ALIVE;
}
"""


# Условие: всегда истинно для storage
RTC_COND_STORAGE_STAY = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_storage_stay) {
    return true;  // Всегда остаёмся в storage
}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции переходов из storage"""
    rtc_code = get_rtc_code()
    
    # 6→6
    fn_6_to_6 = agent.newRTCFunction("rtc_units_apply_6_to_6", rtc_code)
    fn_6_to_6.setRTCFunctionCondition(RTC_COND_STORAGE_STAY)
    fn_6_to_6.setInitialState("storage")
    fn_6_to_6.setEndState("storage")
    
    # Слой
    layer_6_to_6 = model.newLayer("layer_units_storage_6_to_6")
    layer_6_to_6.addAgentFunction(fn_6_to_6)
    
    print("  RTC модуль units_transition_storage зарегистрирован (1 слой)")












