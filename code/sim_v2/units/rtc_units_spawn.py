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
    RTC для проверки дефицита и активации резерва (трёхуровневая система)
    Работает с агентами в reserve (state=5, active=0)
    
    Spawn срабатывает только если:
    1. Есть requests (requests > 0)
    2. Serviceable очередь пуста (svc_head >= svc_tail)
    3. Reserve очередь (active=1) пуста (rsv_head >= rsv_tail)
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
    
    // Проверяем requests и обе очереди (READ фаза)
    auto request_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_request_count");
    auto svc_head = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_svc_head");
    auto svc_tail = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_svc_tail");
    auto rsv_head = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_rsv_head");
    auto rsv_tail = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_rsv_tail");
    
    unsigned int requests = request_count[group_by];
    
    // Проверяем обе очереди
    unsigned int svc_size = (svc_tail[group_by] > svc_head[group_by]) ? 
                            (svc_tail[group_by] - svc_head[group_by]) : 0u;
    unsigned int rsv_size = (rsv_tail[group_by] > rsv_head[group_by]) ? 
                            (rsv_tail[group_by] - rsv_head[group_by]) : 0u;
    
    // Spawn только если обе очереди пусты И есть requests
    bool need_spawn = (requests > 0u && svc_size == 0u && rsv_size == 0u);
    
    if (!need_spawn) {{
        return flamegpu::ALIVE;
    }}
    
    // Используем CAS для атомарного уменьшения счётчика
    unsigned int expected = requests;
    if (expected > 0u) {{
        unsigned int old_val = request_count[group_by].CAS(expected, expected - 1u);
        
        if (old_val == expected) {{
            // Успешно захватили слот — активируем агрегат в serviceable
            FLAMEGPU->setVariable<unsigned int>("active", 1u);
            FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);  // serviceable
            
            // Устанавливаем начальные значения (новый агрегат)
            FLAMEGPU->setVariable<unsigned int>("sne", 0u);
            FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
            FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
            
            // Получаем позицию в svc-очереди через CAS на tail
            unsigned int expected_tail = svc_tail[group_by];
            unsigned int new_tail = svc_tail[group_by].CAS(expected_tail, expected_tail + 1u);
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

