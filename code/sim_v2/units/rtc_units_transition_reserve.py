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

// 5→2 (reserve → operations через FIFO)
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_5_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {{
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
        FLAMEGPU->setVariable<unsigned int>("transition_5_to_2", 1u);
        
        // Очищаем запрос через atomicExch
        requests[my_idx].exchange(0u);
        
        // Логирование
        printf("  [UNIT 5→2 Day %u] PSN %u (group %u): reserve -> operations, AC=%u, FIFO pos=%u\\n", 
               step_day, psn, group_by, target_ac, queue_position);
    }}
    
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

