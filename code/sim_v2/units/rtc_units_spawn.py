#!/usr/bin/env python3
"""
RTC модуль динамического spawn агрегатов

Логика:
1. Когда агрегат выходит из operations → FIFO ищет замену
2. Если serviceable pool пуст (head >= tail) → сигнал на spawn
3. Агент из reserve с state=5 и active=0 → становится serviceable

Счётчики:
- mp_request_count[group] — количество неудовлетворённых запросов
- mp_spawn_trigger[group] — флаг на spawn новых агрегатов

Дата: 05.01.2026
"""

import pyflamegpu as fg

MAX_GROUPS = 50


def get_rtc_code_spawn_check() -> str:
    """
    RTC для проверки дефицита и активации резерва
    Работает с агентами в reserve (state=5, active=0)
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_spawn_check, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Функция для агентов в reserve
    const unsigned int active = FLAMEGPU->getVariable<unsigned int>("active");
    
    // Пропускаем уже активных
    if (active != 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Проверяем есть ли неудовлетворённые запросы (READ фаза)
    auto request_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_request_count");
    auto queue_head = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_queue_head");
    auto queue_tail = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_queue_tail");
    
    unsigned int requests = request_count[group_by];
    unsigned int head = queue_head[group_by];
    unsigned int tail = queue_tail[group_by];
    
    // Если есть запросы и очередь пуста (head >= tail)
    unsigned int queue_size = (tail > head) ? (tail - head) : 0u;
    bool need_spawn = (requests > queue_size);
    
    if (!need_spawn) {{
        return flamegpu::ALIVE;
    }}
    
    // Используем CAS для атомарного уменьшения счётчика
    unsigned int expected = requests;
    if (expected > 0u) {{
        unsigned int old_val = request_count[group_by].CAS(expected, expected - 1u);
        
        if (old_val == expected) {{
            // Успешно захватили слот — активируем агрегат
            FLAMEGPU->setVariable<unsigned int>("active", 1u);
            FLAMEGPU->setVariable<unsigned int>("state", 3u);  // serviceable
            FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
            
            // Устанавливаем начальные значения
            FLAMEGPU->setVariable<unsigned int>("sne", 0u);
            FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
            FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
            
            // Получаем позицию в FIFO через CAS на tail
            unsigned int expected_tail = tail;
            unsigned int new_tail = queue_tail[group_by].CAS(expected_tail, expected_tail + 1u);
            FLAMEGPU->setVariable<unsigned int>("queue_position", new_tail);
        }}
    }}
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_code_spawn_defrag() -> str:
    """
    RTC для очистки (defragmentation) счётчиков после каждого шага
    Сбрасывает mp_request_count если все запросы обработаны
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_spawn_defrag, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Только один агент (idx=0) делает дефрагментацию
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    if (idx != 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Это можно вынести в HostFunction для лучшей производительности
    // Но пока работаем GPU-only
    
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции spawn"""
    
    # === Spawn check (reserve → serviceable при дефиците) ===
    rtc_spawn = get_rtc_code_spawn_check()
    fn_spawn = agent.newRTCFunction("rtc_units_spawn_check", rtc_spawn)
    fn_spawn.setInitialState("reserve")
    fn_spawn.setEndState("reserve")
    # Условие active == 0 проверяется внутри функции
    
    layer_spawn = model.newLayer("layer_units_spawn")
    layer_spawn.addAgentFunction(fn_spawn)
    
    print("  RTC модуль units_spawn зарегистрирован (1 слой)")

