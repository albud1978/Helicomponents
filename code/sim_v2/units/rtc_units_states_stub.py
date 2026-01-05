"""
RTC модуль инициализации intent для состояний reserve, serviceable, storage
Аналог rtc_states_stub планеров

Функционал:
- reserve: intent = 2 (хотят в operations)
- serviceable: intent = 2 (хотят в operations)
- storage: intent = 6 (остаются в storage)

Дата: 05.01.2026
"""

import pyflamegpu as fg


def get_rtc_code() -> str:
    """Возвращает CUDA код для stub модулей"""
    return """
// Reserve: хотят перейти в operations (через FIFO-очередь)
FLAMEGPU_AGENT_FUNCTION(rtc_units_stub_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {
    FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);  // хотим в operations
    return flamegpu::ALIVE;
}

// Serviceable: хотят перейти в operations (через FIFO-очередь)
FLAMEGPU_AGENT_FUNCTION(rtc_units_stub_serviceable, flamegpu::MessageNone, flamegpu::MessageNone) {
    FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);  // хотим в operations
    return flamegpu::ALIVE;
}

// Storage: терминальное состояние
FLAMEGPU_AGENT_FUNCTION(rtc_units_stub_storage, flamegpu::MessageNone, flamegpu::MessageNone) {
    FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);  // остаёмся
    return flamegpu::ALIVE;
}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции stub для состояний"""
    rtc_code = get_rtc_code()
    
    # Reserve
    fn_reserve = agent.newRTCFunction("rtc_units_stub_reserve", rtc_code)
    fn_reserve.setInitialState("reserve")
    fn_reserve.setEndState("reserve")
    
    # Serviceable
    fn_serviceable = agent.newRTCFunction("rtc_units_stub_serviceable", rtc_code)
    fn_serviceable.setInitialState("serviceable")
    fn_serviceable.setEndState("serviceable")
    
    # Storage
    fn_storage = agent.newRTCFunction("rtc_units_stub_storage", rtc_code)
    fn_storage.setInitialState("storage")
    fn_storage.setEndState("storage")
    
    # Один слой для всех stub-функций
    layer = model.newLayer("layer_units_states_stub")
    layer.addAgentFunction(fn_reserve)
    layer.addAgentFunction(fn_serviceable)
    layer.addAgentFunction(fn_storage)
    
    print("  RTC модуль units_states_stub зарегистрирован (1 слой)")

