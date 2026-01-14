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
MAX_PLANERS = 400
MAX_AC_NUMBER = 2000000


def get_rtc_code_assign_serviceable_check(max_frames: int, max_planers: int = 400) -> str:
    """
    CUDA код: Phase 1 — проверка условий для назначения из serviceable (только чтение MP)
    Устанавливает want_assign=1 если есть запросы и агент первый в очереди
    
    Также проверяем что есть планер с свободными слотами для этой группы
    """
    mp_slots_size = MAX_GROUPS * max_planers
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_fifo_assign_svc_check, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Функция привязана к state=serviceable
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int queue_position = FLAMEGPU->getVariable<unsigned int>("queue_position");
    
    if (group_by >= {MAX_GROUPS}u) {{
        return flamegpu::ALIVE;
    }}
    
    // Читаем количество запросов (только чтение)
    auto request_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_request_count");
    const unsigned int requests = request_count[group_by];
    
    if (requests == 0u) {{
        return flamegpu::ALIVE;  // Нет запросов
    }}
    
    // Читаем очередь serviceable (только чтение)
    auto svc_head = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_svc_head");
    auto svc_tail = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_svc_tail");
    
    const unsigned int head = svc_head[group_by];
    const unsigned int tail = svc_tail[group_by];
    
    // FIX: Позволяем нескольким агентам одновременно назначаться
    // Проверяем, что мы в очереди и в пределах количества запросов
    if (queue_position < head || queue_position >= tail) {{
        return flamegpu::ALIVE;  // Не в очереди
    }}
    
    // Можем назначить не более requests агентов за раз
    // queue_position должен быть в диапазоне [head, head + requests)
    if (queue_position >= head + requests) {{
        return flamegpu::ALIVE;  // Ждём своей очереди
    }}
    
    // === Мы в первых N и есть запросы! ===
    FLAMEGPU->setVariable<unsigned int>("want_assign", 1u);
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_code_assign_serviceable_activate(max_frames: int, max_planers: int = 400) -> str:
    """
    CUDA код: Phase 2 — назначение агрегата из serviceable (только атомарные операции MP)
    Срабатывает для агентов с want_assign=1
    
    ВАЖНО: Используем CAS для атомарного захвата слота (не смешиваем read и write)
    FIX: Инкрементируем mp_planer_slots после успешного назначения!
    """
    mp_slots_size = MAX_GROUPS * max_planers
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_fifo_assign_svc_activate, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int want_assign = FLAMEGPU->getVariable<unsigned int>("want_assign");
    
    if (want_assign != 1u) {{
        return flamegpu::ALIVE;
    }}
    
    // Сбрасываем флаг
    FLAMEGPU->setVariable<unsigned int>("want_assign", 0u);
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    if (group_by >= {MAX_GROUPS}u) {{
        return flamegpu::ALIVE;
    }}
    
    // Получаем comp_per_planer
    // FIX 15.01.2026: Используем правильный API для PropertyArray
    const unsigned int comp_per_planer = FLAMEGPU->environment.getProperty<unsigned int, {MAX_GROUPS}>("comp_numbers", group_by);
    
    // Атомарно ищем и забираем запрос для нашей группы
    auto requests_arr = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_request");
    auto req_groups = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_group");
    auto request_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_request_count");
    auto mp_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, {mp_slots_size}u>("mp_planer_slots");
    auto svc_head = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_svc_head");
    auto mp_ac_to_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_AC_NUMBER}u>("mp_ac_to_idx");
    
    unsigned int target_ac = 0u;
    unsigned int found_slot_idx = 0u;
    
    // Атомарный поиск: пробуем exchange(0) и проверяем результат
    for (unsigned int i = 0u; i < {max_frames}u; ++i) {{
        // Атомарно пробуем забрать запрос
        unsigned int old_ac = requests_arr[i].exchange(0u);
        
        if (old_ac > 0u) {{
            // Проверяем группу атомарно
            unsigned int old_grp = req_groups[i].exchange(0u);
            
            if (old_grp == group_by) {{
                // Нашли запрос для нашей группы — НЕ декрементируем пока!
                target_ac = old_ac;
                found_slot_idx = i;
                break;
            }} else {{
                // Не наша группа — возвращаем обратно
                requests_arr[i].exchange(old_ac);
                req_groups[i].exchange(old_grp);
            }}
        }}
    }}
    
    if (target_ac > 0u) {{
        // FIX: Проверяем и инкрементируем mp_planer_slots
        unsigned int planer_idx = 0u;
        if (target_ac < {MAX_AC_NUMBER}u) {{
            planer_idx = mp_ac_to_idx[target_ac];
        }}
        
        bool slot_ok = false;
        if (planer_idx > 0u && planer_idx < {max_planers}u) {{
            unsigned int slots_pos = group_by * {max_planers}u + planer_idx;
            unsigned int old_count = mp_slots[slots_pos]++;
            
            if (old_count < comp_per_planer) {{
                slot_ok = true;  // Успешно захватили слот
            }} else {{
                // Слот уже заполнен — откатываем
                mp_slots[slots_pos] -= 1u;
            }}
        }}
        
        if (slot_ok) {{
            // Успешно — декрементируем счётчики и назначаем
            request_count[group_by] -= 1u;
            svc_head[group_by] += 1u;
            FLAMEGPU->setVariable<unsigned int>("aircraft_number", target_ac);
            FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);  // → operations
            FLAMEGPU->setVariable<unsigned int>("queue_position", 0u);
        }} else {{
            // FIX: Слот не захвачен — возвращаем запрос обратно!
            requests_arr[found_slot_idx].exchange(target_ac);
            req_groups[found_slot_idx].exchange(group_by);
        }}
    }}
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_code_assign_reserve_check(max_frames: int, max_planers: int = 400) -> str:
    """
    CUDA код: Phase 1 — проверка условий для назначения из reserve (только чтение MP)
    Устанавливает want_assign=1 если svc пуста, есть запросы и агент первый в rsv-очереди
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_fifo_assign_rsv_check, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Функция привязана к state=reserve
    const unsigned int active = FLAMEGPU->getVariable<unsigned int>("active");
    
    // Только активные (после ремонта), не spawn-резерв
    if (active != 1u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int queue_position = FLAMEGPU->getVariable<unsigned int>("queue_position");
    
    if (group_by >= {MAX_GROUPS}u) {{
        return flamegpu::ALIVE;
    }}
    
    // Читаем количество запросов (только чтение)
    auto request_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_request_count");
    const unsigned int requests = request_count[group_by];
    
    if (requests == 0u) {{
        return flamegpu::ALIVE;  // Нет запросов
    }}
    
    // FIX: Убрана блокировка на ожидание svc!
    // Резерв работает параллельно с serviceable для скорости комплектации.
    // Оба конкурируют за запросы — кто первый success exchange, тот забирает.
    
    // === Проверяем rsv-очередь ===
    
    auto rsv_head = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_rsv_head");
    auto rsv_tail = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_rsv_tail");
    
    const unsigned int head = rsv_head[group_by];
    const unsigned int tail = rsv_tail[group_by];
    
    // FIX: Убираем ограничение по позиции в очереди
    // Все активные агенты в rsv конкурируют за запросы атомарно
    // Кто первый успешно сделает exchange — тот получит запрос
    // queue_position используется только для статистики, не для блокировки
    
    if (queue_position < head || queue_position >= tail) {{
        return flamegpu::ALIVE;  // Не в очереди
    }}
    
    // === Есть запросы — пробуем назначиться! ===
    FLAMEGPU->setVariable<unsigned int>("want_assign", 1u);
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_code_assign_reserve_activate(max_frames: int, max_planers: int = 400) -> str:
    """
    CUDA код: Phase 2 — назначение агрегата из reserve (только атомарные операции MP)
    Срабатывает для агентов с want_assign=1
    
    ОПТИМИЗАЦИЯ: ищем запрос только среди первых 2000 слотов (достаточно для 1 дня)
    FIX: Инкрементируем mp_planer_slots после успешного назначения!
    """
    # Ограничение поиска — обычно запросов немного за день
    search_limit = min(2000, max_frames)
    mp_slots_size = MAX_GROUPS * max_planers
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_fifo_assign_rsv_activate, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int want_assign = FLAMEGPU->getVariable<unsigned int>("want_assign");
    
    if (want_assign != 1u) {{
        return flamegpu::ALIVE;
    }}
    
    // Сбрасываем флаг
    FLAMEGPU->setVariable<unsigned int>("want_assign", 0u);
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    if (group_by >= {MAX_GROUPS}u) {{
        return flamegpu::ALIVE;
    }}
    
    // Атомарно ищем и забираем запрос для нашей группы
    // ОПТИМИЗАЦИЯ: ограничиваем поиск первыми {search_limit} слотами
    auto requests_arr = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_request");
    auto req_groups = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_group");
    auto request_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_request_count");
    auto rsv_head = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_rsv_head");
    auto mp_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, {mp_slots_size}u>("mp_planer_slots");
    auto mp_ac_to_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_AC_NUMBER}u>("mp_ac_to_idx");
    
    unsigned int target_ac = 0u;
    
    // Рандомизируем начало поиска чтобы не все искали с начала
    const unsigned int my_idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int start = my_idx % {search_limit}u;
    
    unsigned int found_slot_idx = 0u;
    
    for (unsigned int j = 0u; j < {search_limit}u; ++j) {{
        unsigned int i = (start + j) % {search_limit}u;
        unsigned int old_ac = requests_arr[i].exchange(0u);
        
        if (old_ac > 0u) {{
            unsigned int old_grp = req_groups[i].exchange(0u);
            
            if (old_grp == group_by) {{
                target_ac = old_ac;
                found_slot_idx = i;
                // НЕ декрементируем request_count пока не проверим слот!
                break;
            }} else {{
                // Возвращаем обратно — не наша группа
                requests_arr[i].exchange(old_ac);
                req_groups[i].exchange(old_grp);
            }}
        }}
    }}
    
    if (target_ac > 0u) {{
        // FIX: Проверяем и инкрементируем mp_planer_slots
        // FIX 15.01.2026: Используем правильный API для PropertyArray
        const unsigned int comp_per_planer = FLAMEGPU->environment.getProperty<unsigned int, {MAX_GROUPS}>("comp_numbers", group_by);
        
        unsigned int planer_idx = 0u;
        if (target_ac < {MAX_AC_NUMBER}u) {{
            planer_idx = mp_ac_to_idx[target_ac];
        }}
        
        bool slot_ok = false;
        if (planer_idx > 0u && planer_idx < {max_planers}u) {{
            unsigned int slots_pos = group_by * {max_planers}u + planer_idx;
            unsigned int old_count = mp_slots[slots_pos]++;
            
            if (old_count < comp_per_planer) {{
                slot_ok = true;  // Успешно захватили слот
            }} else {{
                // Слот уже заполнен — откатываем
                mp_slots[slots_pos] -= 1u;
            }}
        }}
        
        if (slot_ok) {{
            // Успешно — декрементируем счётчики и назначаем
            request_count[group_by] -= 1u;
            rsv_head[group_by] += 1u;
            
            // FIX 14.01.2026: Декрементируем mp_rsv_count при назначении из reserve!
            // Используем только атомарную запись (без чтения) чтобы избежать race condition
            auto mp_rsv_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_rsv_count");
            mp_rsv_count[group_by] -= 1u;
            
            FLAMEGPU->setVariable<unsigned int>("aircraft_number", target_ac);
            FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);  // → operations
            FLAMEGPU->setVariable<unsigned int>("queue_position", 0u);
        }} else {{
            // FIX: Слот не захвачен — возвращаем запрос обратно!
            requests_arr[found_slot_idx].exchange(target_ac);
            req_groups[found_slot_idx].exchange(group_by);
        }}
    }}
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_code_spawn_check(max_planers: int = 400, max_days: int = 3651) -> str:
    """
    CUDA код: Phase 1 — проверка условий для spawn (только чтение MP)
    
    Spawn происходит ТОЛЬКО когда:
    1. Есть запросы (request_count > 0)
    2. Очередь serviceable пуста (svc_tail <= svc_head)
    3. Очередь reserve пуста (mp_rsv_count == 0)
    4. Есть планер с дефицитом
    
    FIX 14.01.2026: Ищет target_planer_idx и сохраняет в agent variable
    """
    mp_slots_size = MAX_GROUPS * max_planers
    history_size = max_planers * (max_days + 1)
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_fifo_spawn_check, flamegpu::MessageNone, flamegpu::MessageNone) {{
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
    
    // Читаем количество запросов (только чтение)
    auto request_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_request_count");
    const unsigned int requests = request_count[group_by];
    
    if (requests == 0u) {{
        return flamegpu::ALIVE;  // Нет запросов — spawn не нужен
    }}
    
    // Проверяем есть ли свободные агрегаты в serviceable очереди
    auto svc_head = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_svc_head");
    auto svc_tail = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_svc_tail");
    if (svc_tail[group_by] > svc_head[group_by]) {{
        return flamegpu::ALIVE;  // Есть свободные в serviceable — spawn не нужен
    }}
    
    // Проверяем reserve (mp_rsv_count)
    auto mp_rsv_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_rsv_count");
    if (mp_rsv_count[group_by] > 0u) {{
        return flamegpu::ALIVE;  // Есть свободные в reserve — spawn не нужен
    }}
    
    // === Ищем планер с дефицитом (только чтение!) ===
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    // FIX 15.01.2026: Используем правильный API для PropertyArray
    const unsigned int comp_per_planer = FLAMEGPU->environment.getProperty<unsigned int, {MAX_GROUPS}>("comp_numbers", group_by);
    
    if (comp_per_planer == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    auto mp_history = FLAMEGPU->environment.getMacroProperty<unsigned char, {history_size}u>("mp_planer_in_ops_history");
    auto mp_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, {mp_slots_size}u>("mp_planer_slots");
    auto mp_planer_type = FLAMEGPU->environment.getMacroProperty<unsigned char, {max_planers}u>("mp_planer_type");
    
    // Определяем требуемый тип планера
    unsigned char required_type = 0u;
    if (group_by == 3u) {{
        required_type = 1u;  // Mi-8
    }} else if (group_by == 4u) {{
        required_type = 2u;  // Mi-17
    }}
    
    // Рандомизируем начало поиска
    const unsigned int my_idx = FLAMEGPU->getVariable<unsigned int>("idx");
    unsigned int best_planer_idx = 0u;
    unsigned int min_slots = comp_per_planer;
    
    for (unsigned int j = 0u; j < {max_planers}u; ++j) {{
        unsigned int planer_idx = 1u + ((my_idx + j) % ({max_planers}u - 1u));
        
        // Проверяем тип планера
        if (required_type > 0u) {{
            if (mp_planer_type[planer_idx] != required_type) {{
                continue;
            }}
        }}
        
        // Проверяем что планер в operations
        unsigned int history_pos = step_day * {max_planers}u + planer_idx;
        if (mp_history[history_pos] != 1u) {{
            continue;
        }}
        
        // Проверяем слоты
        unsigned int slots_pos = group_by * {max_planers}u + planer_idx;
        unsigned int current_slots = mp_slots[slots_pos];
        
        if (current_slots < min_slots) {{
            min_slots = current_slots;
            best_planer_idx = planer_idx;
        }}
    }}
    
    if (best_planer_idx == 0u) {{
        return flamegpu::ALIVE;  // Нет планеров с дефицитом
    }}
    
    // Нашли планер — spawn!
    FLAMEGPU->setVariable<unsigned int>("want_spawn", 1u);
    FLAMEGPU->setVariable<unsigned int>("target_planer_idx", best_planer_idx);
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_code_spawn_activate(max_frames: int, max_planers: int = 400, max_days: int = 3651) -> str:
    """
    CUDA код: Phase 2 — активация spawn (ТОЛЬКО АТОМАРНЫЕ ЗАПИСИ!)
    Срабатывает для агентов с want_spawn=1
    
    FIX 14.01.2026: Упрощённая версия — target_planer_idx передаётся из spawn_check.
    Здесь только атомарные записи mp_slots и request_count.
    """
    mp_slots_size = MAX_GROUPS * max_planers
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_fifo_spawn_activate, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int want_spawn = FLAMEGPU->getVariable<unsigned int>("want_spawn");
    
    if (want_spawn != 1u) {{
        return flamegpu::ALIVE;
    }}
    
    // Сбрасываем флаг
    FLAMEGPU->setVariable<unsigned int>("want_spawn", 0u);
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int target_planer_idx = FLAMEGPU->getVariable<unsigned int>("target_planer_idx");
    
    if (group_by >= {MAX_GROUPS}u || target_planer_idx == 0u || target_planer_idx >= {max_planers}u) {{
        return flamegpu::ALIVE;
    }}
    
    // FIX 15.01.2026: Используем правильный API для PropertyArray
    const unsigned int comp_per_planer = FLAMEGPU->environment.getProperty<unsigned int, {MAX_GROUPS}>("comp_numbers", group_by);
    
    // === Атомарно захватываем слот ===
    auto mp_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, {mp_slots_size}u>("mp_planer_slots");
    unsigned int slots_pos = group_by * {max_planers}u + target_planer_idx;
    unsigned int old_count = mp_slots[slots_pos]++;
    
    if (old_count >= comp_per_planer) {{
        // Слот уже заполнен — откатываем
        mp_slots[slots_pos]--;
        return flamegpu::ALIVE;
    }}
    
    // === Успешно — активируем и назначаем на планер ===
    auto mp_idx_to_ac = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_planers}u>("mp_idx_to_ac");
    auto request_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_request_count");
    
    unsigned int target_ac = mp_idx_to_ac[target_planer_idx];
    request_count[group_by]--;
    
    FLAMEGPU->setVariable<unsigned int>("active", 1u);
    FLAMEGPU->setVariable<unsigned int>("aircraft_number", target_ac);
    FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);  // → operations
    FLAMEGPU->setVariable<unsigned int>("queue_position", 0u);
    
    // Новый агрегат — нулевые наработки
    FLAMEGPU->setVariable<unsigned int>("sne", 0u);
    FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
    FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
    
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
    """Регистрирует RTC функции трёхуровневой FIFO
    
    Архитектура: двухфазные операции для избежания смешения read/write в одном слое
    - Phase 1 (check): только чтение MacroProperty — устанавливает agent-флаг
    - Phase 2 (activate): только атомарные записи MacroProperty
    """
    
    # === 1. Назначение из serviceable (приоритет 1) — двухфазный ===
    rtc_svc_check = get_rtc_code_assign_serviceable_check(max_frames)  # FIX: передаём max_frames
    fn_svc_check = agent.newRTCFunction("rtc_fifo_assign_svc_check", rtc_svc_check)
    fn_svc_check.setInitialState("serviceable")
    fn_svc_check.setEndState("serviceable")
    
    rtc_svc_activate = get_rtc_code_assign_serviceable_activate(max_frames)
    fn_svc_activate = agent.newRTCFunction("rtc_fifo_assign_svc_activate", rtc_svc_activate)
    fn_svc_activate.setInitialState("serviceable")
    fn_svc_activate.setEndState("serviceable")
    
    # === 2. Назначение из reserve (приоритет 2) — двухфазный ===
    rtc_rsv_check = get_rtc_code_assign_reserve_check(max_frames)  # FIX: передаём max_frames
    fn_rsv_check = agent.newRTCFunction("rtc_fifo_assign_rsv_check", rtc_rsv_check)
    fn_rsv_check.setInitialState("reserve")
    fn_rsv_check.setEndState("reserve")
    
    rtc_rsv_activate = get_rtc_code_assign_reserve_activate(max_frames)
    fn_rsv_activate = agent.newRTCFunction("rtc_fifo_assign_rsv_activate", rtc_rsv_activate)
    fn_rsv_activate.setInitialState("reserve")
    fn_rsv_activate.setEndState("reserve")
    
    # === 3. Spawn — двухфазный (Phase 1: check, Phase 2: activate) ===
    rtc_spawn_check = get_rtc_code_spawn_check(max_planers=400, max_days=max_days)
    fn_spawn_check = agent.newRTCFunction("rtc_fifo_spawn_check", rtc_spawn_check)
    fn_spawn_check.setInitialState("reserve")
    fn_spawn_check.setEndState("reserve")
    
    rtc_spawn_activate = get_rtc_code_spawn_activate(max_frames, max_planers=400, max_days=max_days)
    fn_spawn_activate = agent.newRTCFunction("rtc_fifo_spawn_activate", rtc_spawn_activate)
    fn_spawn_activate.setInitialState("reserve")
    fn_spawn_activate.setEndState("reserve")  # Остаётся в reserve, переход через transition_reserve
    
    # === 4. Возврат в rsv-очередь после ремонта ===
    rtc_return = get_rtc_code_return_to_rsv_pool_write()
    fn_return = agent.newRTCFunction("rtc_fifo_return_to_rsv", rtc_return)
    fn_return.setInitialState("reserve")
    fn_return.setEndState("reserve")
    
    # === Слои (порядок важен!) ===
    # Двухфазная архитектура: ВСЕ CHECK → ВСЕ ACTIVATE
    # FIX 14.01.2026: CHECK должны быть ДО ACTIVATE чтобы spawn_check видел request_count > 0
    
    # 0. Возврат после ремонта — в начале дня (только запись)
    layer_return = model.newLayer("layer_fifo_return_to_rsv")
    layer_return.addAgentFunction(fn_return)
    
    # === PHASE 1: ВСЕ CHECK (только чтение MP) ===
    # 1. Serviceable CHECK
    layer_svc_check = model.newLayer("layer_fifo_svc_check")
    layer_svc_check.addAgentFunction(fn_svc_check)
    
    # 2. Reserve CHECK  
    layer_rsv_check = model.newLayer("layer_fifo_rsv_check")
    layer_rsv_check.addAgentFunction(fn_rsv_check)
    
    # 3. Spawn CHECK — теперь ПЕРЕД activate, видит request_count > 0!
    layer_spawn_check = model.newLayer("layer_fifo_spawn_check")
    layer_spawn_check.addAgentFunction(fn_spawn_check)
    
    # === PHASE 2: ВСЕ ACTIVATE (только атомарные записи MP) ===
    # 4. Serviceable ACTIVATE
    layer_svc_activate = model.newLayer("layer_fifo_svc_activate")
    layer_svc_activate.addAgentFunction(fn_svc_activate)
    
    # 5. Reserve ACTIVATE
    layer_rsv_activate = model.newLayer("layer_fifo_rsv_activate")
    layer_rsv_activate.addAgentFunction(fn_rsv_activate)
    
    # 6. Spawn ACTIVATE
    layer_spawn_activate = model.newLayer("layer_fifo_spawn_activate")
    layer_spawn_activate.addAgentFunction(fn_spawn_activate)
    
    print("  RTC модуль units_fifo_priority зарегистрирован (7 слоёв: return, 3×CHECK, 3×ACTIVATE)")



