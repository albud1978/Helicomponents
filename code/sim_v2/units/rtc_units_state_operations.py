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

// DEPRECATED: Инкремент перенесён в rtc_units_increment.py
// Этот модуль использует реальный dt от планера, а не хардкод 90u
// FLAMEGPU_AGENT_FUNCTION(rtc_units_state_operations_increment, ...) - УДАЛЁН
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции для operations (только intent)"""
    rtc_code = get_rtc_code()
    
    # Функция вычисления intent
    fn_intent = agent.newRTCFunction("rtc_units_state_operations_intent", rtc_code)
    fn_intent.setInitialState("operations")
    fn_intent.setEndState("operations")
    
    # DEPRECATED: fn_increment удалён — используется rtc_units_increment.py
    
    # Слой intent
    layer_intent = model.newLayer("layer_units_ops_intent")
    layer_intent.addAgentFunction(fn_intent)
    
    print("  RTC модуль units_state_operations зарегистрирован (1 слой: intent only)")

