"""
RTC модуль переходов из operations (агрегаты)
Аналог rtc_state_manager_operations планеров

Переходы:
- 2→2 (operations → operations) при intent=2
- 2→3 (operations → serviceable) при intent=3 (агрегат без AC)
- 2→4 (operations → repair) при intent=4
- 2→6 (operations → storage) при intent=6

Дата: 05.01.2026, обновлено 08.01.2026
"""

import pyflamegpu as fg


MAX_GROUPS = 50
MAX_PLANERS = 400
MAX_AC_NUMBER = 2000000


def get_rtc_code_2_to_2() -> str:
    """RTC код для 2→2 (остаёмся в operations)"""
    return """
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_2_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}
"""


def get_rtc_code_2_to_3(max_frames: int) -> str:
    """RTC код для 2→3 (operations → serviceable)
    
    Агрегат уходит с планера (планер не летает dt=0):
    1. Декрементируем mp_planer_slots (освобождаем слот)
    2. НЕ записываем запрос замены — планер сам не в operations!
    3. Обнуляем aircraft_number
    4. Получаем позицию в очереди serviceable
    """
    mp_slots_size = MAX_GROUPS * MAX_PLANERS
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_2_to_3, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_3", 1u);
    
    // === Декремент mp_planer_slots (освобождаем слот) ===
    if (aircraft_number > 0u && group_by < {MAX_GROUPS}u) {{
        auto mp_ac_to_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_AC_NUMBER}u>("mp_ac_to_idx");
        auto mp_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, {mp_slots_size}u>("mp_planer_slots");
        
        unsigned int planer_idx = 0u;
        if (aircraft_number < {MAX_AC_NUMBER}u) {{
            planer_idx = mp_ac_to_idx[aircraft_number];
        }}
        
        if (planer_idx > 0u && planer_idx < {MAX_PLANERS}u) {{
            const unsigned int slots_pos = group_by * {MAX_PLANERS}u + planer_idx;
            mp_slots[slots_pos]--;  // Освобождаем слот
        }}
    }}
    
    // Обнуляем aircraft_number
    FLAMEGPU->setVariable<unsigned int>("aircraft_number", 0u);
    
    // Получаем позицию в очереди serviceable: queue_position = svc_tail++
    if (group_by < {MAX_GROUPS}u) {{
        auto mp_svc_tail = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_svc_tail");
        unsigned int my_pos = mp_svc_tail[group_by]++;  // atomicAdd, возвращает старое значение
        FLAMEGPU->setVariable<unsigned int>("queue_position", my_pos);
    }} else {{
        FLAMEGPU->setVariable<unsigned int>("queue_position", 0u);
    }}
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_code_2_to_4(max_frames: int) -> str:
    """RTC код для 2→4 (operations → repair)
    
    При уходе агрегата с планера:
    1. Декрементируем mp_planer_slots (освобождаем слот)
    2. Записываем запрос замены в mp_replacement_request[idx]
    3. Инкрементируем mp_request_count
    4. FIFO найдёт запрос и назначит замену на тот же планер
    """
    mp_slots_size = MAX_GROUPS * MAX_PLANERS
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_2_to_4, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_4", 1u);
    
    if (aircraft_number > 0u && group_by < {MAX_GROUPS}u) {{
        // === Декремент mp_planer_slots (освобождаем слот) ===
        auto mp_ac_to_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_AC_NUMBER}u>("mp_ac_to_idx");
        auto mp_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, {mp_slots_size}u>("mp_planer_slots");
        
        unsigned int planer_idx = 0u;
        if (aircraft_number < {MAX_AC_NUMBER}u) {{
            planer_idx = mp_ac_to_idx[aircraft_number];
        }}
        
        if (planer_idx > 0u && planer_idx < {MAX_PLANERS}u) {{
            unsigned int slots_pos = group_by * {MAX_PLANERS}u + planer_idx;
            // Атомарный декремент
            mp_slots[slots_pos]--;
        }}
        
        // === FIX: Записываем запрос замены в очередь (первые 2000 слотов) ===
        // Используем idx % 2000 для компактного хранения
        auto requests = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_request");
        auto req_groups = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_group");
        const unsigned int queue_pos = idx % 2000u;
        requests[queue_pos].exchange(aircraft_number);
        req_groups[queue_pos].exchange(group_by);
        
        // === Инкремент mp_request_count ===
        auto request_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_request_count");
        request_count[group_by] += 1u;
    }}
    
    FLAMEGPU->setVariable<unsigned int>("aircraft_number", 0u);
    return flamegpu::ALIVE;
}}
"""


def get_rtc_code_2_to_6(max_frames: int) -> str:
    """RTC код для 2→6 (operations → storage)
    
    При уходе агрегата с планера в storage:
    1. Декрементируем mp_planer_slots (освобождаем слот)
    2. Записываем запрос замены в mp_replacement_request[idx]
    3. Инкрементируем mp_request_count
    4. FIFO найдёт запрос и назначит замену на тот же планер
    """
    mp_slots_size = MAX_GROUPS * MAX_PLANERS
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_2_to_6, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_6", 1u);
    
    if (aircraft_number > 0u && group_by < {MAX_GROUPS}u) {{
        // === Декремент mp_planer_slots (освобождаем слот) ===
        auto mp_ac_to_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_AC_NUMBER}u>("mp_ac_to_idx");
        auto mp_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, {mp_slots_size}u>("mp_planer_slots");
        
        unsigned int planer_idx = 0u;
        if (aircraft_number < {MAX_AC_NUMBER}u) {{
            planer_idx = mp_ac_to_idx[aircraft_number];
        }}
        
        if (planer_idx > 0u && planer_idx < {MAX_PLANERS}u) {{
            unsigned int slots_pos = group_by * {MAX_PLANERS}u + planer_idx;
            // Атомарный декремент
            mp_slots[slots_pos]--;
        }}
        
        // === FIX: Записываем запрос замены в очередь (первые 2000 слотов) ===
        auto requests = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_request");
        auto req_groups = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_group");
        const unsigned int queue_pos = idx % 2000u;
        requests[queue_pos].exchange(aircraft_number);
        req_groups[queue_pos].exchange(group_by);
        
        // === Инкремент mp_request_count ===
        auto request_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_request_count");
        request_count[group_by] += 1u;
    }}
    
    FLAMEGPU->setVariable<unsigned int>("aircraft_number", 0u);
    return flamegpu::ALIVE;
}}
"""


RTC_COND_OPS_STAY = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_ops_stay) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
}
"""

RTC_COND_OPS_TO_SERVICEABLE = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_ops_to_serviceable) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 3u;
}
"""

RTC_COND_OPS_TO_REPAIR = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_ops_to_repair) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 4u;
}
"""

RTC_COND_OPS_TO_STORAGE = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_ops_to_storage) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 6u;
}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription, max_frames: int = 40000):
    """Регистрирует RTC функции переходов из operations
    
    ВАЖНО: Каждая функция имеет ОТДЕЛЬНЫЙ RTC код для правильной компиляции!
    
    Args:
        model: FLAME GPU модель
        agent: AgentDescription
        max_frames: Размер MacroProperty для requests (из env_data['units_frames_total'])
    """
    
    # === 2→3 (operations → serviceable) — для агрегатов без AC или планер не летает ===
    rtc_2_to_3 = get_rtc_code_2_to_3(max_frames)
    fn_2_to_3 = agent.newRTCFunction("rtc_units_apply_2_to_3", rtc_2_to_3)
    fn_2_to_3.setRTCFunctionCondition(RTC_COND_OPS_TO_SERVICEABLE)
    fn_2_to_3.setInitialState("operations")
    fn_2_to_3.setEndState("serviceable")
    
    # === 2→4 (operations → repair) ===
    rtc_2_to_4 = get_rtc_code_2_to_4(max_frames)
    fn_2_to_4 = agent.newRTCFunction("rtc_units_apply_2_to_4", rtc_2_to_4)
    fn_2_to_4.setRTCFunctionCondition(RTC_COND_OPS_TO_REPAIR)
    fn_2_to_4.setInitialState("operations")
    fn_2_to_4.setEndState("repair")
    
    # === 2→6 (operations → storage) ===
    rtc_2_to_6 = get_rtc_code_2_to_6(max_frames)
    fn_2_to_6 = agent.newRTCFunction("rtc_units_apply_2_to_6", rtc_2_to_6)
    fn_2_to_6.setRTCFunctionCondition(RTC_COND_OPS_TO_STORAGE)
    fn_2_to_6.setInitialState("operations")
    fn_2_to_6.setEndState("storage")
    
    # === 2→2 (остаёмся в operations) ===
    rtc_2_to_2 = get_rtc_code_2_to_2()
    fn_2_to_2 = agent.newRTCFunction("rtc_units_apply_2_to_2", rtc_2_to_2)
    fn_2_to_2.setRTCFunctionCondition(RTC_COND_OPS_STAY)
    fn_2_to_2.setInitialState("operations")
    fn_2_to_2.setEndState("operations")
    
    # Слои в правильном порядке: сначала выходы, потом "остаёмся"
    layer_2_to_3 = model.newLayer("layer_units_ops_2_to_3")
    layer_2_to_3.addAgentFunction(fn_2_to_3)
    
    layer_2_to_4 = model.newLayer("layer_units_ops_2_to_4")
    layer_2_to_4.addAgentFunction(fn_2_to_4)
    
    layer_2_to_6 = model.newLayer("layer_units_ops_2_to_6")
    layer_2_to_6.addAgentFunction(fn_2_to_6)
    
    layer_2_to_2 = model.newLayer("layer_units_ops_2_to_2")
    layer_2_to_2.addAgentFunction(fn_2_to_2)
    
    print("  RTC модуль units_transition_ops зарегистрирован (4 слоя: 3, 4, 6, 2)")

