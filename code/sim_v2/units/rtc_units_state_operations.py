"""
RTC модуль для агентов в состоянии operations (агрегаты)
Аналог rtc_state_2_operations планеров

Функционал:
- Вычисление intent_state на основе ресурсов (ppr, oh, sne, ll, br)
- Инкремент наработки (sne, ppr) на dt из MP2 планера

Дата: 05.01.2026
"""

import pyflamegpu as fg

MAX_GROUPS = 50


def get_rtc_code() -> str:
    """Возвращает CUDA код для модуля operations"""
    return """
FLAMEGPU_AGENT_FUNCTION(rtc_units_state_operations_intent, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Получаем текущие значения ресурсов
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    // Условия переходов (приоритет от высшего к низшему):
    // 1. sne >= ll → storage (исчерпан назначенный ресурс)
    // 2. sne >= br → storage (выгоднее списать чем ремонтировать)
    // 3. ppr >= oh → repair (исчерпан межремонтный) — КРОМЕ лопастей (group_by=6)
    // 4. Иначе → operations (остаёмся)
    
    unsigned int intent = 2u;  // По умолчанию: остаёмся в operations
    
    // Проверка назначенного ресурса
    if (ll > 0u && sne >= ll) {
        intent = 6u;  // storage
    }
    // Проверка breakeven
    else if (br > 0u && sne >= br) {
        intent = 6u;  // storage
    }
    // Проверка межремонтного (кроме лопастей)
    else if (oh > 0u && ppr >= oh && group_by != 6u) {
        intent = 4u;  // repair
    }
    
    FLAMEGPU->setVariable<unsigned int>("intent_state", intent);
    
    return flamegpu::ALIVE;
}

// Инкремент наработки (вызывается ПОСЛЕ вычисления intent)
FLAMEGPU_AGENT_FUNCTION(rtc_units_state_operations_increment, flamegpu::MessageNone, flamegpu::MessageNone) {
    unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    
    // TODO: Читать dt из MP2 планера по aircraft_number
    // Пока используем фиксированное значение 90 минут/день
    const unsigned int dt = 90u;
    
    // Инкремент наработки
    sne += dt;
    ppr += dt;
    
    FLAMEGPU->setVariable<unsigned int>("sne", sne);
    FLAMEGPU->setVariable<unsigned int>("ppr", ppr);
    
    return flamegpu::ALIVE;
}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции для operations"""
    rtc_code = get_rtc_code()
    
    # Функция вычисления intent
    fn_intent = agent.newRTCFunction("rtc_units_state_operations_intent", rtc_code)
    fn_intent.setInitialState("operations")
    fn_intent.setEndState("operations")
    
    # Функция инкремента
    fn_increment = agent.newRTCFunction("rtc_units_state_operations_increment", rtc_code)
    fn_increment.setInitialState("operations")
    fn_increment.setEndState("operations")
    
    # Слои (порядок важен!)
    layer_intent = model.newLayer("layer_units_ops_intent")
    layer_intent.addAgentFunction(fn_intent)
    
    layer_increment = model.newLayer("layer_units_ops_increment")
    layer_increment.addAgentFunction(fn_increment)
    
    print("  RTC модуль units_state_operations зарегистрирован (2 слоя)")

