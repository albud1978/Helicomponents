#!/usr/bin/env python3
"""
RTC модуль FIFO Phase 2: Назначение замены (WRITE phase)

Двухфазная архитектура для избежания race conditions:
- Phase 1: неисправные агрегаты записали запросы
- Phase 2 (этот модуль): исправные из пула назначаются на планеры

Логика FIFO:
1. Агрегат в serviceable читает mp_request_count[group_by]
2. Если есть запросы — атомарно инкрементирует mp_queue_head[group_by]
3. Если queue_position == old_head — агрегат "выигрывает" и назначается
4. Сканирует mp_replacement_request для поиска aircraft_number

Принцип: "Старые первыми" (FIFO) — агрегаты с меньшим queue_position 
назначаются раньше.

Дата: 05.01.2026
"""

import pyflamegpu as fg

MAX_GROUPS = 50  # Максимум групп агрегатов


def get_rtc_code_phase2(max_frames: int) -> str:
    """
    CUDA код Phase 2: назначение агрегатов из пула
    
    Атомарные операции:
    - atomicAdd для сдвига head: mp_queue_head[group_by]++
    - Сравнение queue_position с old_head для определения "победителя"
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_fifo_phase2_assign, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Функция привязана к state=serviceable, проверка state не нужна
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int queue_position = FLAMEGPU->getVariable<unsigned int>("queue_position");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    if (group_by >= {MAX_GROUPS}u) {{
        return flamegpu::ALIVE;
    }}
    
    // Читаем количество запросов для нашей группы
    auto request_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_request_count");
    const unsigned int requests_for_group = request_count[group_by];
    
    if (requests_for_group == 0u) {{
        return flamegpu::ALIVE;  // Нет запросов — остаёмся в пуле
    }}
    
    // === Атомарная попытка занять позицию в очереди ===
    auto queue_head = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_queue_head");
    auto queue_tail = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_queue_tail");
    
    const unsigned int current_head = queue_head[group_by];
    const unsigned int current_tail = queue_tail[group_by];
    
    // Проверяем, что очередь не пуста и мы в начале
    // queue_position должен быть >= head и < tail
    if (queue_position < current_head || queue_position >= current_tail) {{
        return flamegpu::ALIVE;  // Не в очереди
    }}
    
    // Проверяем, мы ли следующие в очереди (FIFO)
    if (queue_position != current_head) {{
        return flamegpu::ALIVE;  // Не наша очередь — ждём
    }}
    
    // === Мы первые в очереди! ===
    
    // Атомарно сдвигаем head (берём агрегат из очереди)
    queue_head[group_by] += 1u;  // atomicAdd
    
    // Ищем первый непустой запрос для нашей группы
    auto requests = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_request");
    auto req_groups = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_group");
    
    unsigned int target_aircraft = 0u;
    
    for (unsigned int i = 0u; i < {max_frames}u; ++i) {{
        const unsigned int req_ac = requests[i];
        const unsigned int req_grp = req_groups[i];
        
        if (req_ac > 0u && req_grp == group_by) {{
            // Нашли запрос для нашей группы
            target_aircraft = req_ac;
            
            // Очищаем запрос (чтобы другие не взяли)
            requests[i].exchange(0u);
            req_groups[i].exchange(0u);
            
            // Декрементируем счётчик запросов
            request_count[group_by] -= 1u;
            
            break;
        }}
    }}
    
    if (target_aircraft > 0u) {{
        // Назначаем агрегат на планер
        FLAMEGPU->setVariable<unsigned int>("aircraft_number", target_aircraft);
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);  // → operations
        
        // Обнуляем queue_position (больше не в очереди)
        FLAMEGPU->setVariable<unsigned int>("queue_position", 0u);
    }}
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_code_return_to_pool(max_frames: int) -> str:
    """
    CUDA код: агрегат после ремонта возвращается в пул (serviceable)
    Получает новую queue_position = tail++
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_return_to_pool, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Функция привязана к state=reserve, проверка state не нужна
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int queue_position = FLAMEGPU->getVariable<unsigned int>("queue_position");
    
    // Если уже в очереди — ничего не делаем
    if (queue_position > 0u) {{
        return flamegpu::ALIVE;
    }}
    
    if (group_by >= {MAX_GROUPS}u) {{
        return flamegpu::ALIVE;
    }}
    
    // Атомарно получаем следующую позицию в очереди
    auto queue_tail = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_queue_tail");
    
    // atomicAdd возвращает старое значение, которое и будет нашей позицией
    const unsigned int new_position = queue_tail[group_by];
    queue_tail[group_by] += 1u;
    
    // Записываем позицию
    FLAMEGPU->setVariable<unsigned int>("queue_position", new_position);
    
    // Остаёмся в reserve (intent_state = 5)
    // НЕ переходим в serviceable — это две разные очереди!
    // Reserve (после ремонта) имеет отдельную FIFO-очередь от serviceable (склад)
    FLAMEGPU->setVariable<unsigned int>("intent_state", 5u);
    
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription, 
                 max_frames: int, max_days: int = 3650):
    """Регистрирует RTC функции Phase 2 (назначение из FIFO)"""
    
    # === Функция назначения (serviceable → operations) ===
    rtc_assign = get_rtc_code_phase2(max_frames)
    fn_assign = agent.newRTCFunction("rtc_units_fifo_phase2_assign", rtc_assign)
    fn_assign.setInitialState("serviceable")
    fn_assign.setEndState("serviceable")
    
    # === Функция возврата в пул (reserve → serviceable) ===
    rtc_return = get_rtc_code_return_to_pool(max_frames)
    fn_return = agent.newRTCFunction("rtc_units_return_to_pool", rtc_return)
    fn_return.setInitialState("reserve")
    fn_return.setEndState("reserve")
    
    # === Слои ===
    # Возврат в пул — после ремонта
    layer_return = model.newLayer("layer_units_return_to_pool")
    layer_return.addAgentFunction(fn_return)
    
    # Назначение — после записи запросов (Phase 1)
    layer_assign = model.newLayer("layer_units_fifo_phase2")
    layer_assign.addAgentFunction(fn_assign)
    
    print("  RTC модуль units_fifo_phase2 зарегистрирован (2 слоя)")

