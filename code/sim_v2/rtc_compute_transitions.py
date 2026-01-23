#!/usr/bin/env python3
"""
RTC модуль для вычисления переходов между состояниями.
Выполняется ПЕРЕД state_managers, используя current state и intent_state.

Логика:
- current state = текущее состояние (день D-1, из StateName)
- intent_state = желаемое состояние (день D, установлено intent слоями)
- Если state ≠ intent_state → вычисляем переход и записываем напрямую в MP2
- После этого state_managers применит intent → state

Архитектура:
- Записывает transition флаги НАПРЯМУЮ в MacroProperty (mp2_transition_X_to_Y)
- НЕ использует agent variables (transition_X_to_Y не нужны)
- Флаг ставится однократно в день перехода (день D)

⚠️ ИСКЛЮЧЕНИЯ:
- transition_1_to_4 (inactive→repair) — заполняется ПОСТПРОЦЕССИНГОМ (mp2_postprocess_active)
- transition_4_to_2 (repair→operations) — заполняется ПОСТПРОЦЕССИНГОМ (mp2_postprocess_active)

Эти переходы НЕ происходят напрямую в симуляции, а восстанавливаются по active_trigger.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import model_build

import pyflamegpu as fg


def register_compute_transitions(model, agent):
    """Регистрирует RTC модуль compute_transitions в модель - отдельные функции для каждого состояния"""
    
    print("  Подключение модуля: compute_transitions")
    
    # ФИКСИРОВАННЫЕ размеры для RTC кэширования
    MAX_FRAMES = model_build.RTC_MAX_FRAMES
    MAX_DAYS = model_build.MAX_DAYS
    MP2_SIZE = MAX_FRAMES * (MAX_DAYS + 1)
    
    # Создаём функции для каждого состояния (это упрощает работу со StateName)
    state_map = {
        'inactive': (1, 'FLAMEGPU_STORE_FUNCTION_STATE_INACTIVE'),
        'operations': (2, 'FLAMEGPU_STORE_FUNCTION_STATE_OPERATIONS'),
        'serviceable': (3, 'FLAMEGPU_STORE_FUNCTION_STATE_SERVICEABLE'),
        'repair': (4, 'FLAMEGPU_STORE_FUNCTION_STATE_REPAIR'),
        'reserve': (5, 'FLAMEGPU_STORE_FUNCTION_STATE_RESERVE'),
        'storage': (6, 'FLAMEGPU_STORE_FUNCTION_STATE_STORAGE'),
        'unserviceable': (7, 'FLAMEGPU_STORE_FUNCTION_STATE_UNSERVICEABLE'),
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
    
    // Вычисляем позицию в MP2 (плотная матрица day × frames)
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int pos = step_day * {MAX_FRAMES}u + idx;
    
    // Получаем MacroProperty для transition флагов
    auto mp2_transition_2_to_4 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_transition_2_to_4");
    auto mp2_transition_2_to_6 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_transition_2_to_6");
    auto mp2_transition_2_to_3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_transition_2_to_3");
    auto mp2_transition_3_to_2 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_transition_3_to_2");
    auto mp2_transition_7_to_4 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_transition_7_to_4");
    auto mp2_transition_7_to_2 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_transition_7_to_2");
    // ⚠️ transition_1_to_4, transition_4_to_2 НЕ обрабатываются здесь
    // Они заполняются ПОСТПРОЦЕССИНГОМ (mp2_postprocess_active)
    
    // Записываем нужный флаг в зависимости от (state, intent)
    if (state == 2u && intent == 4u) {{  // operations → repair
        mp2_transition_2_to_4[pos].exchange(1u);
    }} else if (state == 2u && intent == 6u) {{  // operations → storage
        mp2_transition_2_to_6[pos].exchange(1u);
    }} else if (state == 2u && intent == 3u) {{  // operations → serviceable
        mp2_transition_2_to_3[pos].exchange(1u);
    }} else if (state == 3u && intent == 2u) {{  // serviceable → operations
        mp2_transition_3_to_2[pos].exchange(1u);
    }} else if (state == 7u && intent == 4u) {{  // unserviceable → repair
        mp2_transition_7_to_4[pos].exchange(1u);
    }} else if (state == 7u && intent == 2u) {{  // unserviceable → operations
        mp2_transition_7_to_2[pos].exchange(1u);
    }}
    // НЕ обрабатываем:
    // - state==1 && intent==2 (inactive→operations) - заполняется как transition_4_to_2 постпроцессингом
    // - state==1 && intent==4 (inactive→repair) - не происходит в симуляции
    // - state==4 && intent==2 (repair→operations) - заполняется постпроцессингом
    
    return flamegpu::ALIVE;
}}
"""
            
            fn = agent.newRTCFunction(f"rtc_compute_transitions_{state_name}", rtc_code)
            fn.setInitialState(state_name)
            fn.setEndState(state_name)
            layer.addAgentFunction(fn)
        
        print("  RTC модуль compute_transitions зарегистрирован (7 функций для 7 состояний)")
        return layer
        
    except Exception as e:
        print(f"  ❌ Ошибка при регистрации compute_transitions: {e}")
        raise
