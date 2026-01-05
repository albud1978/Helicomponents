"""
RTC модуль переходов из operations (агрегаты)
Аналог rtc_state_manager_operations планеров

Переходы:
- 2→2 (operations → operations) при intent=2
- 2→4 (operations → repair) при intent=4
- 2→6 (operations → storage) при intent=6

Дата: 05.01.2026
"""

import pyflamegpu as fg


def get_rtc_code() -> str:
    """Возвращает CUDA код для переходов из operations"""
    return """
// Условия по intent
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_ops_stay) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
}

FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_ops_to_repair) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 4u;
}

FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_ops_to_storage) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 6u;
}

// 2→2 (остаёмся в operations)
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_2_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Агент остаётся в operations
    return flamegpu::ALIVE;
}

// 2→4 (operations → repair)
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_2_to_4, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int psn = FLAMEGPU->getVariable<unsigned int>("psn");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    
    // Флаг перехода
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_4", 1u);
    
    // Логирование
    if (psn < 100000u) {
        printf("  [UNIT 2→4 Day %u] PSN %u (group %u): ppr=%u >= oh=%u\\n", 
               step_day, psn, group_by, ppr, oh);
    }
    
    return flamegpu::ALIVE;
}

// 2→6 (operations → storage)
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_2_to_6, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int psn = FLAMEGPU->getVariable<unsigned int>("psn");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    
    // Флаг перехода
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_6", 1u);
    FLAMEGPU->setVariable<unsigned int>("aircraft_number", 0u);  // Снят с планера
    
    // Логирование
    if (psn < 100000u) {
        printf("  [UNIT 2→6 Day %u] PSN %u (group %u): sne=%u >= ll=%u\\n", 
               step_day, psn, group_by, sne, ll);
    }
    
    return flamegpu::ALIVE;
}
"""


RTC_COND_OPS_STAY = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_ops_stay) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
}
"""

RTC_COND_OPS_TO_REPAIR = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_ops_to_repair) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 4u;
}
"""

RTC_COND_OPS_TO_STORAGE = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_ops_to_storage) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 6u;
}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции переходов из operations"""
    rtc_code = get_rtc_code()
    
    # 2→2
    fn_2_to_2 = agent.newRTCFunction("rtc_units_apply_2_to_2", rtc_code)
    fn_2_to_2.setRTCFunctionCondition(RTC_COND_OPS_STAY)
    fn_2_to_2.setInitialState("operations")
    fn_2_to_2.setEndState("operations")
    
    # 2→4
    fn_2_to_4 = agent.newRTCFunction("rtc_units_apply_2_to_4", rtc_code)
    fn_2_to_4.setRTCFunctionCondition(RTC_COND_OPS_TO_REPAIR)
    fn_2_to_4.setInitialState("operations")
    fn_2_to_4.setEndState("repair")  # СМЕНА СОСТОЯНИЯ!
    
    # 2→6
    fn_2_to_6 = agent.newRTCFunction("rtc_units_apply_2_to_6", rtc_code)
    fn_2_to_6.setRTCFunctionCondition(RTC_COND_OPS_TO_STORAGE)
    fn_2_to_6.setInitialState("operations")
    fn_2_to_6.setEndState("storage")  # СМЕНА СОСТОЯНИЯ!
    
    # Слои
    layer_2_to_2 = model.newLayer("layer_units_ops_2_to_2")
    layer_2_to_2.addAgentFunction(fn_2_to_2)
    
    layer_2_to_4 = model.newLayer("layer_units_ops_2_to_4")
    layer_2_to_4.addAgentFunction(fn_2_to_4)
    
    layer_2_to_6 = model.newLayer("layer_units_ops_2_to_6")
    layer_2_to_6.addAgentFunction(fn_2_to_6)
    
    print("  RTC модуль units_transition_ops зарегистрирован (3 слоя)")

