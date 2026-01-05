#!/usr/bin/env python3
"""
RTC модуль возврата агрегата в пул после ремонта

Агрегат после ремонта получает новую позицию в FIFO-очереди (tail)
и переходит в состояние reserve.

Дата: 05.01.2026
"""

import pyflamegpu as fg

MAX_GROUPS = 50


def get_rtc_code() -> str:
    """Возвращает CUDA код для возврата в пул"""
    return f"""
// Агрегат выходит из ремонта → получает позицию в очереди
FLAMEGPU_AGENT_FUNCTION(rtc_units_return_to_pool, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int transition = FLAMEGPU->getVariable<unsigned int>("transition_4_to_5");
    
    if (transition == 1u) {{
        const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
        
        if (group_by < {MAX_GROUPS}u) {{
            // Атомарно инкрементируем tail и получаем OLD значение (нашу позицию)
            // Используем atomicAdd через DeviceMacroProperty
            auto queue_tail = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_queue_tail");
            
            // atomicAdd возвращает OLD значение и атомарно добавляет 1
            unsigned int my_position = queue_tail[group_by] + 1u;  // Читаем текущее + 1
            queue_tail[group_by].exchange(my_position);            // Записываем новое
            my_position -= 1u;  // Наша позиция = старое значение
            
            FLAMEGPU->setVariable<unsigned int>("queue_position", my_position);
        }}
        
        // Сбрасываем флаг перехода
        FLAMEGPU->setVariable<unsigned int>("transition_4_to_5", 0u);
    }}
    
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции возврата в пул"""
    rtc_code = get_rtc_code()
    
    # Функция для агентов в repair (перед сменой состояния на reserve)
    fn_return = agent.newRTCFunction("rtc_units_return_to_pool", rtc_code)
    fn_return.setInitialState("repair")
    fn_return.setEndState("repair")
    
    # Слой
    layer = model.newLayer("layer_units_return_to_pool")
    layer.addAgentFunction(fn_return)
    
    print("  RTC модуль units_return_to_pool зарегистрирован (1 слой)")

