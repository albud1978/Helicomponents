#!/usr/bin/env python3
"""
RTC модуль отслеживания выхода планера из operations

Когда планер уходит из operations (2→repair/storage/reserve):
- Двигатели на нём переходят operations → serviceable ("склад на крыле")
- aircraft_number обнуляется (отцепка)
- Двигатель становится доступен для назначения на другой планер

Логика:
1. Агрегат в operations проверяет mp_planer_in_ops[planer_idx]
2. Если планер НЕ в operations (mp_planer_in_ops = 0) → агрегат уходит в serviceable

Дата: 14.01.2026
"""

import pyflamegpu as fg

MAX_GROUPS = 50
MAX_PLANERS = 400
MAX_AC_NUMBER = 2000000


def get_rtc_code_planer_exit_check(max_days: int = 3651) -> str:
    """
    CUDA код: проверка выхода планера из operations
    
    Если агрегат в operations привязан к планеру, который больше не в operations:
    - Устанавливаем intent_state = 3 (serviceable)
    - Отцепляем от планера (aircraft_number = 0)
    
    Читает из mp_planer_in_ops_history[step_day * MAX_PLANERS + planer_idx]
    """
    planer_ops_history_size = MAX_PLANERS * (max_days + 1)
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_planer_exit_check, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Только для агрегатов в operations с привязкой к планеру
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    if (aircraft_number == 0u) {{
        return flamegpu::ALIVE;  // Не привязан к планеру
    }}
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    
    // Получаем planer_idx по aircraft_number
    auto mp_ac_to_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_AC_NUMBER}u>("mp_ac_to_idx");
    
    unsigned int planer_idx = 0u;
    if (aircraft_number < {MAX_AC_NUMBER}u) {{
        planer_idx = mp_ac_to_idx[aircraft_number];
    }}
    
    if (planer_idx == 0u || planer_idx >= {MAX_PLANERS}u) {{
        return flamegpu::ALIVE;  // Нет маппинга
    }}
    
    // Проверяем, в operations ли планер НА ТЕКУЩИЙ ДЕНЬ (из истории)
    auto mp_history = FLAMEGPU->environment.getMacroProperty<unsigned char, {planer_ops_history_size}u>("mp_planer_in_ops_history");
    const unsigned int history_pos = step_day * {MAX_PLANERS}u + planer_idx;
    
    unsigned char planer_in_ops = 0u;
    if (history_pos < {planer_ops_history_size}u) {{
        planer_in_ops = mp_history[history_pos];
    }}
    
    if (planer_in_ops == 1u) {{
        return flamegpu::ALIVE;  // Планер в operations — всё OK
    }}
    
    // Планер ушёл из operations — отцепляем агрегат!
    // Агрегат уходит в serviceable ("склад на крыле")
    FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);  // → serviceable
    FLAMEGPU->setVariable<unsigned int>("aircraft_number", 0u);  // Отцепка
    FLAMEGPU->setVariable<unsigned int>("transition_planer_exit", 1u);  // Маркер для логирования
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_3", 1u);  // Маркер для transition
    // Сохраняем planer_idx для декремента mp_slots в следующем слое
    FLAMEGPU->setVariable<unsigned int>("target_planer_idx", planer_idx);
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_code_planer_exit_decrement() -> str:
    """
    CUDA код: Phase 2 — декремент mp_planer_slots для отцепленных агрегатов
    
    Выполняется в отдельном слое чтобы избежать race condition
    """
    mp_slots_size = MAX_GROUPS * MAX_PLANERS
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_planer_exit_decrement, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Только для агрегатов которые были отцеплены
    const unsigned int transition_planer_exit = FLAMEGPU->getVariable<unsigned int>("transition_planer_exit");
    
    if (transition_planer_exit != 1u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int planer_idx = FLAMEGPU->getVariable<unsigned int>("target_planer_idx");
    
    if (planer_idx == 0u || planer_idx >= {MAX_PLANERS}u || group_by >= {MAX_GROUPS}u) {{
        return flamegpu::ALIVE;
    }}
    
    // Декремент mp_planer_slots (атомарный)
    auto mp_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, {mp_slots_size}u>("mp_planer_slots");
    const unsigned int slots_pos = group_by * {MAX_PLANERS}u + planer_idx;
    
    if (slots_pos < {mp_slots_size}u) {{
        mp_slots[slots_pos]--;
    }}
    
    // Сбрасываем target_planer_idx
    FLAMEGPU->setVariable<unsigned int>("target_planer_idx", 0u);
    
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription, max_days: int = 3651):
    """Регистрирует RTC функции отслеживания выхода планера (2 слоя)"""
    
    # Phase 1: Проверка и установка флагов (чтение mp_history)
    rtc_code_check = get_rtc_code_planer_exit_check(max_days)
    fn_exit_check = agent.newRTCFunction("rtc_units_planer_exit_check", rtc_code_check)
    fn_exit_check.setInitialState("operations")
    fn_exit_check.setEndState("operations")
    
    # Phase 2: Декремент mp_slots (атомарная запись)
    rtc_code_decrement = get_rtc_code_planer_exit_decrement()
    fn_exit_decrement = agent.newRTCFunction("rtc_units_planer_exit_decrement", rtc_code_decrement)
    fn_exit_decrement.setInitialState("operations")
    fn_exit_decrement.setEndState("operations")
    
    # Слой 1 — проверка
    layer1 = model.newLayer("layer_units_planer_exit_check")
    layer1.addAgentFunction(fn_exit_check)
    
    # Слой 2 — декремент slots
    layer2 = model.newLayer("layer_units_planer_exit_decrement")
    layer2.addAgentFunction(fn_exit_decrement)
    
    print("  ✅ RTC модуль units_planer_exit зарегистрирован (2 слоя)")

