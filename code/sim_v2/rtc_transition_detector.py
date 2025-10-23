#!/usr/bin/env python3
"""
⚠️ DEPRECATED (23-10-2025): Этот файл больше НЕ используется!

Причина: Transition detection перенесён в rtc_compute_transitions.py с прямой записью в MacroProperty.
Этот модуль остаётся для исторической справки.

---

СТАРАЯ ВЕРСИЯ:
GPU RTC слой для постпроцессинга MP2: вычисление переходов между состояниями
Выполняется на каждом шаге симуляции (ПОСЛЕ всех state managers)
Читает mp2_state (UInt), сравнивает день D с днём D-1 и заполняет transition флаги для дня D
"""

import pyflamegpu as fg

class ComputeTransitionsRTCFunction:
    """RTC функция для вычисления переходов на GPU"""
    
    @staticmethod
    def get_rtc_code(max_frames: int):
        """Генерирует RTC код для вычисления переходов"""
        return f"""
// GPU RTC функция: вычисление переходов между состояниями (incremental, на каждый день)
// Выполняется на КАЖДОМ шаге после всех state managers
// Вычисляет переходы ТОЛЬКО для текущего дня путём сравнения состояний день D и день D-1

FLAMEGPU_AGENT_FUNCTION(rtc_compute_transitions, flamegpu::MessageNone, flamegpu::MessageNone) {{
    
    // Пропускаем агентов с aircraft_number=0 (запас под спавн)
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Получаем текущий день (шаг симуляции) и индекс
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    const unsigned int MAX_FRAMES = {max_frames}u;
    
    // Вычисляем MAX_SIZE dynamically (это вычислится в runtime)
    // На самом деле используем только две позиции: current_day и prev_day
    const unsigned int MAX_SIZE_CALC = MAX_FRAMES * 4001u;  // MAX_DAYS+1 = 4001
    const unsigned int pos_current = day * MAX_FRAMES + idx;
    const unsigned int pos_prev = (day > 0u) ? ((day - 1u) * MAX_FRAMES + idx) : pos_current;
    
    // Для дня 0 pos_prev == pos_current (нет предшественника, поэтому переходов не будет)
    // Это нормально - просто не будет никаких изменений state между одинаковыми позициями
    
    // Получаем MP2 MacroProperty для ВСЕХ переходов
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_SIZE_CALC>("mp2_state");
    auto mp2_transition_2_to_4 = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_SIZE_CALC>("mp2_transition_2_to_4");
    auto mp2_transition_2_to_6 = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_SIZE_CALC>("mp2_transition_2_to_6");
    auto mp2_transition_2_to_3 = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_SIZE_CALC>("mp2_transition_2_to_3");
    auto mp2_transition_3_to_2 = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_SIZE_CALC>("mp2_transition_3_to_2");
    auto mp2_transition_5_to_2 = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_SIZE_CALC>("mp2_transition_5_to_2");
    auto mp2_transition_1_to_2 = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_SIZE_CALC>("mp2_transition_1_to_2");
    auto mp2_transition_4_to_5 = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_SIZE_CALC>("mp2_transition_4_to_5");
    auto mp2_transition_1_to_4 = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_SIZE_CALC>("mp2_transition_1_to_4");
    auto mp2_transition_4_to_2 = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_SIZE_CALC>("mp2_transition_4_to_2");
    
    // Читаем состояния текущего и предыдущего дня
    unsigned int state_current = mp2_state[pos_current];
    unsigned int state_prev = mp2_state[pos_prev];
    
    // Сравниваем состояния и устанавливаем флаги переходов для текущего дня
    if (state_current != state_prev) {{
        // Определяем переход и устанавливаем соответствующий флаг
        if (state_prev == 2u && state_current == 4u) {{  // operations → repair
            mp2_transition_2_to_4[pos_current].exchange(1u);
        }} else if (state_prev == 2u && state_current == 6u) {{  // operations → storage
            mp2_transition_2_to_6[pos_current].exchange(1u);
        }} else if (state_prev == 2u && state_current == 3u) {{  // operations → serviceable (demount)
            mp2_transition_2_to_3[pos_current].exchange(1u);
        }} else if (state_prev == 3u && state_current == 2u) {{  // serviceable → operations
            mp2_transition_3_to_2[pos_current].exchange(1u);
        }} else if (state_prev == 5u && state_current == 2u) {{  // reserve → operations
            mp2_transition_5_to_2[pos_current].exchange(1u);
        }} else if (state_prev == 1u && state_current == 2u) {{  // inactive → operations
            mp2_transition_1_to_2[pos_current].exchange(1u);
        }} else if (state_prev == 4u && state_current == 5u) {{  // repair → reserve
            mp2_transition_4_to_5[pos_current].exchange(1u);
        }} else if (state_prev == 1u && state_current == 4u) {{  // inactive → repair
            mp2_transition_1_to_4[pos_current].exchange(1u);
        }} else if (state_prev == 4u && state_current == 2u) {{  // repair → operations
            mp2_transition_4_to_2[pos_current].exchange(1u);
        }}
    }}
    
    return flamegpu::ALIVE;
}}
"""


def register_compute_transitions_layer(model, agent, max_frames: int, max_days: int):
    """Регистрирует RTC функцию compute_transitions в модель"""
    
    print("  🔧 Регистрация GPU слоя compute_transitions для постпроцессинга переходов")
    print("     (выполняется на КАЖДОМ шаге, вычисляет переходы incrementally)")
    
    try:
        # Регистрируем RTC функцию на агента
        rtc_code = ComputeTransitionsRTCFunction.get_rtc_code(max_frames)
        fn = agent.newRTCFunction("rtc_compute_transitions", rtc_code)
        
        # Создаем новый слой для post-processing (ПОСЛЕ всех state managers, ДО дренажа)
        layer = model.newLayer("compute_transitions")
        layer.addAgentFunction(fn)
        
        print(f"  ✅ RTC слой compute_transitions зарегистрирован")
        print(f"     (MAX_FRAMES={max_frames}, будет работать на каждом из {max_days} дней)")
        
        return layer
    except Exception as e:
        print(f"  ❌ Ошибка при регистрации compute_transitions: {e}")
        raise
