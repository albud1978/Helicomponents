#!/usr/bin/env python3
"""
RTC модуль трёхуровневой приоритетной FIFO для замены агрегатов

Приоритеты:
1. Serviceable (state=3) — готовые агрегаты на складе
2. Reserve (state=5, active=1) — после ремонта, ждут востребованности  
3. Reserve (state=5, active=0) — spawn новых (покупка)

Архитектура очередей:
- mp_svc_head/tail[group] — FIFO для serviceable
- mp_rsv_head/tail[group] — FIFO для reserve (active=1)
- Spawn не в очереди — активируется при пустых очередях

Дата: 05.01.2026
"""

import pyflamegpu as fg

MAX_GROUPS = 50


def get_rtc_code_assign_serviceable(max_frames: int) -> str:
    """
    CUDA код: назначение агрегата из serviceable (приоритет 1)
    Агрегат проверяет: есть ли запросы И он первый в svc-очереди
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_fifo_assign_serviceable, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Функция привязана к state=serviceable
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int queue_position = FLAMEGPU->getVariable<unsigned int>("queue_position");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    if (group_by >= {MAX_GROUPS}u) {{
        return flamegpu::ALIVE;
    }}
    
    // Читаем количество запросов
    auto request_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_request_count");
    const unsigned int requests = request_count[group_by];
    
    if (requests == 0u) {{
        return flamegpu::ALIVE;  // Нет запросов
    }}
    
    // Читаем очередь serviceable
    auto svc_head = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_svc_head");
    auto svc_tail = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_svc_tail");
    
    const unsigned int head = svc_head[group_by];
    const unsigned int tail = svc_tail[group_by];
    
    // Проверяем, что мы в очереди и первые
    if (queue_position < head || queue_position >= tail) {{
        return flamegpu::ALIVE;  // Не в очереди
    }}
    
    if (queue_position != head) {{
        return flamegpu::ALIVE;  // Не наша очередь
    }}
    
    // === Мы первые! Атомарно сдвигаем head ===
    svc_head[group_by] += 1u;
    
    // Ищем запрос для нашей группы
    auto requests_arr = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_request");
    auto req_groups = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_group");
    
    unsigned int target_ac = 0u;
    
    for (unsigned int i = 0u; i < {max_frames}u; ++i) {{
        if (requests_arr[i] > 0u && req_groups[i] == group_by) {{
            target_ac = requests_arr[i];
            requests_arr[i].exchange(0u);
            req_groups[i].exchange(0u);
            request_count[group_by] -= 1u;
            break;
        }}
    }}
    
    if (target_ac > 0u) {{
        FLAMEGPU->setVariable<unsigned int>("aircraft_number", target_ac);
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);  // → operations
        FLAMEGPU->setVariable<unsigned int>("queue_position", 0u);
        
        const unsigned int step_day = FLAMEGPU->getStepCounter();
        const unsigned int psn = FLAMEGPU->getVariable<unsigned int>("psn");
        printf("  [FIFO-SVC Day %u] PSN %u (grp %u): serviceable -> ops, AC=%u\\n",
               step_day, psn, group_by, target_ac);
    }}
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_code_assign_reserve(max_frames: int) -> str:
    """
    CUDA код: назначение агрегата из reserve (приоритет 2)
    Срабатывает ТОЛЬКО если svc-очередь пуста
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_fifo_assign_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Функция привязана к state=reserve
    const unsigned int active = FLAMEGPU->getVariable<unsigned int>("active");
    
    // Только активные (после ремонта), не spawn-резерв
    if (active != 1u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int queue_position = FLAMEGPU->getVariable<unsigned int>("queue_position");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    if (group_by >= {MAX_GROUPS}u) {{
        return flamegpu::ALIVE;
    }}
    
    // Читаем количество запросов
    auto request_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_request_count");
    const unsigned int requests = request_count[group_by];
    
    if (requests == 0u) {{
        return flamegpu::ALIVE;  // Нет запросов
    }}
    
    // Проверяем, что svc-очередь пуста
    auto svc_head = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_svc_head");
    auto svc_tail = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_svc_tail");
    
    const unsigned int svc_h = svc_head[group_by];
    const unsigned int svc_t = svc_tail[group_by];
    
    if (svc_t > svc_h) {{
        return flamegpu::ALIVE;  // Есть агрегаты в serviceable — ждём
    }}
    
    // === svc-очередь пуста, используем reserve ===
    
    // Читаем rsv-очередь
    auto rsv_head = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_rsv_head");
    auto rsv_tail = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_rsv_tail");
    
    const unsigned int head = rsv_head[group_by];
    const unsigned int tail = rsv_tail[group_by];
    
    // Проверяем, что мы в очереди и первые
    if (queue_position < head || queue_position >= tail) {{
        return flamegpu::ALIVE;
    }}
    
    if (queue_position != head) {{
        return flamegpu::ALIVE;
    }}
    
    // === Мы первые! Атомарно сдвигаем head ===
    rsv_head[group_by] += 1u;
    
    // Ищем запрос
    auto requests_arr = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_request");
    auto req_groups = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_group");
    
    unsigned int target_ac = 0u;
    
    for (unsigned int i = 0u; i < {max_frames}u; ++i) {{
        if (requests_arr[i] > 0u && req_groups[i] == group_by) {{
            target_ac = requests_arr[i];
            requests_arr[i].exchange(0u);
            req_groups[i].exchange(0u);
            request_count[group_by] -= 1u;
            break;
        }}
    }}
    
    if (target_ac > 0u) {{
        FLAMEGPU->setVariable<unsigned int>("aircraft_number", target_ac);
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);  // → operations
        FLAMEGPU->setVariable<unsigned int>("queue_position", 0u);
        
        const unsigned int step_day = FLAMEGPU->getStepCounter();
        const unsigned int psn = FLAMEGPU->getVariable<unsigned int>("psn");
        printf("  [FIFO-RSV Day %u] PSN %u (grp %u): reserve -> ops, AC=%u\\n",
               step_day, psn, group_by, target_ac);
    }}
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_code_spawn(max_frames: int) -> str:
    """
    CUDA код: spawn новых агрегатов (приоритет 3)
    Срабатывает ТОЛЬКО если ОБЕ очереди пусты
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_fifo_spawn, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Функция привязана к state=reserve
    const unsigned int active = FLAMEGPU->getVariable<unsigned int>("active");
    
    // Только spawn-резерв (active=0)
    if (active != 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    if (group_by >= {MAX_GROUPS}u) {{
        return flamegpu::ALIVE;
    }}
    
    // Читаем количество запросов
    auto request_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_request_count");
    const unsigned int requests = request_count[group_by];
    
    if (requests == 0u) {{
        return flamegpu::ALIVE;  // Нет запросов
    }}
    
    // Проверяем, что svc-очередь пуста
    auto svc_head = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_svc_head");
    auto svc_tail = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_svc_tail");
    
    if (svc_tail[group_by] > svc_head[group_by]) {{
        return flamegpu::ALIVE;  // Есть в serviceable
    }}
    
    // Проверяем, что rsv-очередь пуста
    auto rsv_head = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_rsv_head");
    auto rsv_tail = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_rsv_tail");
    
    if (rsv_tail[group_by] > rsv_head[group_by]) {{
        return flamegpu::ALIVE;  // Есть в reserve
    }}
    
    // === ОБЕ очереди пусты — spawn! ===
    
    // Атомарно уменьшаем request_count
    unsigned int expected = requests;
    if (expected > 0u) {{
        unsigned int old_val = request_count[group_by].CAS(expected, expected - 1u);
        
        if (old_val == expected) {{
            // Успешно захватили слот — активируемся
            FLAMEGPU->setVariable<unsigned int>("active", 1u);
            FLAMEGPU->setVariable<unsigned int>("state", 3u);  // → serviceable
            FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
            
            // Новый агрегат — нулевые наработки
            FLAMEGPU->setVariable<unsigned int>("sne", 0u);
            FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
            FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
            
            // Получаем позицию в svc-очереди
            unsigned int new_pos = svc_tail[group_by];
            svc_tail[group_by] += 1u;
            FLAMEGPU->setVariable<unsigned int>("queue_position", new_pos);
            
            const unsigned int step_day = FLAMEGPU->getStepCounter();
            const unsigned int psn = FLAMEGPU->getVariable<unsigned int>("psn");
            printf("  [SPAWN Day %u] PSN %u (grp %u): spawn -> serviceable, pos=%u\\n",
                   step_day, psn, group_by, new_pos);
        }}
    }}
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_code_return_to_rsv_pool_write() -> str:
    """
    CUDA код: агрегат после ремонта встаёт в rsv-очередь (WRITE ONLY)
    Использует постфиксный инкремент (++) который возвращает старое значение
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_fifo_return_to_rsv, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Функция привязана к state=reserve
    const unsigned int active = FLAMEGPU->getVariable<unsigned int>("active");
    const unsigned int queue_position = FLAMEGPU->getVariable<unsigned int>("queue_position");
    
    // Только активные (после ремонта) без позиции в очереди
    if (active != 1u || queue_position > 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    if (group_by >= {MAX_GROUPS}u) {{
        return flamegpu::ALIVE;
    }}
    
    // Атомарный постфиксный инкремент возвращает СТАРОЕ значение (нашу позицию)
    auto rsv_tail = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_rsv_tail");
    unsigned int my_position = rsv_tail[group_by]++;  // atomicAdd, возвращает старое значение
    
    FLAMEGPU->setVariable<unsigned int>("queue_position", my_position + 1u);  // +1 т.к. позиция с 1
    
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription, 
                 max_frames: int, max_days: int = 3650):
    """Регистрирует RTC функции трёхуровневой FIFO"""
    
    # === 1. Назначение из serviceable (приоритет 1) ===
    rtc_svc = get_rtc_code_assign_serviceable(max_frames)
    fn_svc = agent.newRTCFunction("rtc_fifo_assign_serviceable", rtc_svc)
    fn_svc.setInitialState("serviceable")
    fn_svc.setEndState("serviceable")
    
    # === 2. Назначение из reserve (приоритет 2) ===
    rtc_rsv = get_rtc_code_assign_reserve(max_frames)
    fn_rsv = agent.newRTCFunction("rtc_fifo_assign_reserve", rtc_rsv)
    fn_rsv.setInitialState("reserve")
    fn_rsv.setEndState("reserve")
    
    # === 3. Spawn (приоритет 3) ===
    rtc_spawn = get_rtc_code_spawn(max_frames)
    fn_spawn = agent.newRTCFunction("rtc_fifo_spawn", rtc_spawn)
    fn_spawn.setInitialState("reserve")
    fn_spawn.setEndState("reserve")  # Остаётся в reserve, но intent→serviceable
    
    # === 4. Возврат в rsv-очередь после ремонта ===
    rtc_return = get_rtc_code_return_to_rsv_pool_write()
    fn_return = agent.newRTCFunction("rtc_fifo_return_to_rsv", rtc_return)
    fn_return.setInitialState("reserve")
    fn_return.setEndState("reserve")
    
    # === Слои (порядок важен!) ===
    
    # Возврат после ремонта — в начале дня
    layer_return = model.newLayer("layer_fifo_return_to_rsv")
    layer_return.addAgentFunction(fn_return)
    
    # Назначение serviceable — приоритет 1
    layer_svc = model.newLayer("layer_fifo_assign_svc")
    layer_svc.addAgentFunction(fn_svc)
    
    # Назначение reserve — приоритет 2
    layer_rsv = model.newLayer("layer_fifo_assign_rsv")
    layer_rsv.addAgentFunction(fn_rsv)
    
    # Spawn — приоритет 3
    layer_spawn = model.newLayer("layer_fifo_spawn")
    layer_spawn.addAgentFunction(fn_spawn)
    
    print("  RTC модуль units_fifo_priority зарегистрирован (4 слоя)")



