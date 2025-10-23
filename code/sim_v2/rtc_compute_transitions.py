#!/usr/bin/env python3
"""
RTC модуль для вычисления переходов между состояниями.
Выполняется ПЕРЕД state_managers, используя current state и intent_state.

Логика:
- current state = текущее состояние (день D-1, из StateName)
- intent_state = желаемое состояние (день D, установлено intent слоями)
- Если state ≠ intent_state → вычисляем и записываем переход в агента
- После этого state_managers применит intent → state
"""

import pyflamegpu as fg


def register_compute_transitions(model, agent):
    """Регистрирует RTC модуль compute_transitions в модель - отдельные функции для каждого состояния"""
    
    print("  Подключение модуля: compute_transitions")
    
    # Создаём функции для каждого состояния (это упрощает работу со StateName)
    state_map = {
        'inactive': (1, 'FLAMEGPU_STORE_FUNCTION_STATE_INACTIVE'),
        'operations': (2, 'FLAMEGPU_STORE_FUNCTION_STATE_OPERATIONS'),
        'serviceable': (3, 'FLAMEGPU_STORE_FUNCTION_STATE_SERVICEABLE'),
        'repair': (4, 'FLAMEGPU_STORE_FUNCTION_STATE_REPAIR'),
        'reserve': (5, 'FLAMEGPU_STORE_FUNCTION_STATE_RESERVE'),
        'storage': (6, 'FLAMEGPU_STORE_FUNCTION_STATE_STORAGE'),
    }
    
    try:
        layer = model.newLayer("compute_transitions")
        
        # Создаём RTC функцию для каждого состояния
        for state_name, (state_id, state_const) in state_map.items():
            rtc_code = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_compute_transitions_{state_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Пропускаем агентов с aircraft_number=0 (запас под спавн)
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Текущее состояние - {state_name} (={state_id})
    const unsigned int state = {state_id}u;
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // Если состояние не меняется - выходим
    if (state == intent) {{
        return flamegpu::ALIVE;
    }}
    
    // Инициализируем все флаги в 0
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_4", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_6", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_3", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_3_to_2", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_5_to_2", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_1_to_2", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_4_to_5", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_1_to_4", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_4_to_2", 0u);
    
    // Затем устанавливаем нужный флаг в зависимости от (state, intent)
    if (state == 2u && intent == 4u) {{  // operations → repair
        FLAMEGPU->setVariable<unsigned int>("transition_2_to_4", 1u);
    }} else if (state == 2u && intent == 6u) {{  // operations → storage
        FLAMEGPU->setVariable<unsigned int>("transition_2_to_6", 1u);
    }} else if (state == 2u && intent == 3u) {{  // operations → serviceable
        FLAMEGPU->setVariable<unsigned int>("transition_2_to_3", 1u);
    }} else if (state == 3u && intent == 2u) {{  // serviceable → operations
        FLAMEGPU->setVariable<unsigned int>("transition_3_to_2", 1u);
    }} else if (state == 5u && intent == 2u) {{  // reserve → operations
        FLAMEGPU->setVariable<unsigned int>("transition_5_to_2", 1u);
    }} else if (state == 1u && intent == 2u) {{  // inactive → operations
        FLAMEGPU->setVariable<unsigned int>("transition_1_to_2", 1u);
    }} else if (state == 4u && intent == 5u) {{  // repair → reserve
        FLAMEGPU->setVariable<unsigned int>("transition_4_to_5", 1u);
    }} else if (state == 1u && intent == 4u) {{  // inactive → repair
        FLAMEGPU->setVariable<unsigned int>("transition_1_to_4", 1u);
    }} else if (state == 4u && intent == 2u) {{  // repair → operations
        FLAMEGPU->setVariable<unsigned int>("transition_4_to_2", 1u);
    }}
    
    return flamegpu::ALIVE;
}}
"""
            
            fn = agent.newRTCFunction(f"rtc_compute_transitions_{state_name}", rtc_code)
            fn.setInitialState(state_name)
            fn.setEndState(state_name)
            layer.addAgentFunction(fn)
        
        print("  RTC модуль compute_transitions зарегистрирован (6 функций для 6 состояний)")
        return layer
        
    except Exception as e:
        print(f"  ❌ Ошибка при регистрации compute_transitions: {e}")
        raise
