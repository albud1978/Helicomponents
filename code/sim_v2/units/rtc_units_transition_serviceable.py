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

// 3→2 (serviceable → operations через FIFO)
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_3_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int psn = FLAMEGPU->getVariable<unsigned int>("psn");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int queue_position = FLAMEGPU->getVariable<unsigned int>("queue_position");
    
    // Читаем aircraft_number из запроса замены
    auto requests = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_request");
    const unsigned int my_idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int target_ac = requests[my_idx];
    
    if (target_ac > 0u) {{
        // Есть запрос — устанавливаемся на планер
        FLAMEGPU->setVariable<unsigned int>("aircraft_number", target_ac);
        FLAMEGPU->setVariable<unsigned int>("transition_3_to_2", 1u);
        
        // Очищаем запрос через atomicExch
        requests[my_idx].exchange(0u);
        
        // Логирование
        printf("  [UNIT 3→2 Day %u] PSN %u (group %u): serviceable -> operations, AC=%u, FIFO pos=%u\\n", 
               step_day, psn, group_by, target_ac, queue_position);
    }}
    
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

