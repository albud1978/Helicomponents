#!/usr/bin/env python3
"""
RTC модуль FIFO-назначения замены

Упрощённая версия: агенты в пуле проверяют свою позицию.
Первый агент с позицией == head "выигрывает" через atomicAdd.

Дата: 05.01.2026
"""

import pyflamegpu as fg

MAX_GROUPS = 50
UNITS_MAX_FRAMES = 12000


def get_rtc_code(max_frames: int) -> str:
    """Возвращает CUDA код для FIFO-назначения"""
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_fifo_check_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int my_position = FLAMEGPU->getVariable<unsigned int>("queue_position");
    
    // Пропускаем агентов не в очереди
    if (my_position == 0xFFFFFFFFu || group_by >= {MAX_GROUPS}u) {{
        return flamegpu::ALIVE;
    }}
    
    auto queue_head = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_queue_head");
    auto requests = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_request");
    auto req_groups = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_group");
    
    // Читаем текущую голову очереди
    unsigned int current_head = queue_head[group_by];
    
    // Если моя позиция == голова — я кандидат на выдачу
    if (my_position == current_head) {{
        // Ищем запрос для моей группы
        for (unsigned int i = 0; i < {max_frames}u; ++i) {{
            unsigned int req_group = req_groups[i];
            unsigned int req_ac = requests[i];
            
            if (req_group == group_by && req_ac > 0u) {{
                // Атомарно инкрементируем head (первый кто сделает exchange выиграет)
                unsigned int old_head = queue_head[group_by].exchange(current_head + 1u);
                
                // Если мы были первыми (old_head == current_head), то мы выиграли
                if (old_head == current_head) {{
                    // Назначаюсь на этот планер
                    FLAMEGPU->setVariable<unsigned int>("aircraft_number", req_ac);
                    FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);  // operations
                    FLAMEGPU->setVariable<unsigned int>("transition_5_to_2", 1u);  // reserve → operations
                    FLAMEGPU->setVariable<unsigned int>("queue_position", 0xFFFFFFFFu);  // Выходим из очереди
                    
                    // Очищаем запрос
                    requests[i].exchange(0u);
                    req_groups[i].exchange(0u);
                }}
                break;  // Обработали один запрос
            }}
        }}
    }}
    
    return flamegpu::ALIVE;
}}

FLAMEGPU_AGENT_FUNCTION(rtc_units_fifo_check_serviceable, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int my_position = FLAMEGPU->getVariable<unsigned int>("queue_position");
    
    if (my_position == 0xFFFFFFFFu || group_by >= {MAX_GROUPS}u) {{
        return flamegpu::ALIVE;
    }}
    
    auto queue_head = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_queue_head");
    auto requests = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_request");
    auto req_groups = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_group");
    
    unsigned int current_head = queue_head[group_by];
    
    if (my_position == current_head) {{
        for (unsigned int i = 0; i < {max_frames}u; ++i) {{
            unsigned int req_group = req_groups[i];
            unsigned int req_ac = requests[i];
            
            if (req_group == group_by && req_ac > 0u) {{
                unsigned int old_head = queue_head[group_by].exchange(current_head + 1u);
                
                if (old_head == current_head) {{
                    FLAMEGPU->setVariable<unsigned int>("aircraft_number", req_ac);
                    FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
                    FLAMEGPU->setVariable<unsigned int>("transition_3_to_2", 1u);  // serviceable → operations
                    FLAMEGPU->setVariable<unsigned int>("queue_position", 0xFFFFFFFFu);
                    
                    requests[i].exchange(0u);
                    req_groups[i].exchange(0u);
                }}
                break;
            }}
        }}
    }}
    
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription, max_frames: int = UNITS_MAX_FRAMES):
    """Регистрирует RTC функции FIFO-назначения"""
    rtc_code = get_rtc_code(max_frames)
    
    # Функции для агентов в пуле
    fn_reserve = agent.newRTCFunction("rtc_units_fifo_check_reserve", rtc_code)
    fn_reserve.setInitialState("reserve")
    fn_reserve.setEndState("reserve")
    
    fn_serviceable = agent.newRTCFunction("rtc_units_fifo_check_serviceable", rtc_code)
    fn_serviceable.setInitialState("serviceable")
    fn_serviceable.setEndState("serviceable")
    
    # Слой
    layer = model.newLayer("layer_units_fifo_assignment")
    layer.addAgentFunction(fn_reserve)
    layer.addAgentFunction(fn_serviceable)
    
    print("  RTC модуль units_fifo_assignment зарегистрирован (1 слой)")
