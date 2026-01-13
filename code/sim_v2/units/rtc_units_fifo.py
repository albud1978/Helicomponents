#!/usr/bin/env python3
"""
RTC модуль FIFO: Трёхфазная архитектура замены агрегатов

Проблема: FLAMEGPU не позволяет смешивать read и atomic write в одном слое.

Решение — три слоя:
- Phase 1 (Write): Неисправные записывают запрос в mp_replacement_request[idx]
- Phase 2 (Read):  Serviceable читают head и сохраняют в агентную переменную
- Phase 3 (CAS):   Агрегат с queue_position == saved_head атомарно "побеждает"

Ключевой принцип FIFO: 
- queue_position растёт при входе в пул (tail++)
- Назначение идёт с головы (head++)
- Старые агрегаты (меньший queue_position) назначаются первыми

Дата: 05.01.2026
"""

import pyflamegpu as fg

MAX_GROUPS = 50


def get_rtc_phase1_write_request(max_frames: int) -> str:
    """
    Phase 1: Неисправные агрегаты записывают запрос на замену
    Только WRITE операции — никакого чтения MacroProperty!
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_fifo_phase1_write, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Функция привязана к operations
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Только агрегаты на планере
    if (aircraft_number == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Условия замены
    bool needs_repair = (oh > 0u && ppr >= oh);
    bool needs_storage = (ll > 0u && sne >= ll) || (br > 0u && sne >= br);
    
    if (!needs_repair && !needs_storage) {{
        return flamegpu::ALIVE;
    }}
    
    // === WRITE ONLY: записываем запрос ===
    if (idx < {max_frames}u && group_by < {MAX_GROUPS}u) {{
        auto requests = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_request");
        auto req_groups = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_group");
        
        requests[idx].exchange(aircraft_number);
        req_groups[idx].exchange(group_by);
    }}
    
    // Устанавливаем intent
    if (needs_storage) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);
    }} else {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);
    }}
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_phase2_read_head(max_frames: int) -> str:
    """
    Phase 2: Serviceable читают head своей группы и сохраняют локально
    Только READ операции — никакой записи в MacroProperty!
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_fifo_phase2_read, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Функция привязана к serviceable
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int queue_position = FLAMEGPU->getVariable<unsigned int>("queue_position");
    
    if (group_by >= {MAX_GROUPS}u) {{
        return flamegpu::ALIVE;
    }}
    
    // === READ ONLY: читаем head и сохраняем в агентную переменную ===
    auto queue_head = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_queue_head");
    const unsigned int current_head = queue_head[group_by];
    
    // Сохраняем в агентную переменную для Phase 3
    // Используем bi_counter как временное хранилище (потом обнулим)
    // Кодируем: если queue_position == current_head, агент — кандидат
    if (queue_position == current_head) {{
        FLAMEGPU->setVariable<unsigned int>("bi_counter", 1u);  // Кандидат!
    }} else {{
        FLAMEGPU->setVariable<unsigned int>("bi_counter", 0u);
    }}
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_phase3a_increment_head() -> str:
    """
    Phase 3a: Кандидаты (bi_counter==1) атомарно сдвигают head
    ТОЛЬКО WRITE в mp_queue_head
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_fifo_phase3a_head, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int is_candidate = FLAMEGPU->getVariable<unsigned int>("bi_counter");
    
    if (is_candidate != 1u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    if (group_by >= {MAX_GROUPS}u) {{
        return flamegpu::ALIVE;
    }}
    
    // === WRITE ONLY: инкремент head ===
    auto queue_head = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_queue_head");
    queue_head[group_by] += 1u;
    
    // Помечаем что мы готовы к назначению (bi_counter = 2)
    FLAMEGPU->setVariable<unsigned int>("bi_counter", 2u);
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_phase3b_find_request(max_frames: int) -> str:
    """
    Phase 3b: Агенты с bi_counter==2 ищут запрос своей группы (READ requests)
    ТОЛЬКО READ из mp_replacement_request/group
    Сохраняем found_idx в repair_days, target_aircraft в intent_state
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_fifo_phase3b_find, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int ready_to_assign = FLAMEGPU->getVariable<unsigned int>("bi_counter");
    
    if (ready_to_assign != 2u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    // === READ ONLY: ищем запрос ===
    auto requests = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_request");
    auto req_groups = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_group");
    
    unsigned int found_idx = 0xFFFFFFFFu;
    unsigned int target_aircraft = 0u;
    
    for (unsigned int i = 0u; i < {max_frames}u; ++i) {{
        const unsigned int req_grp = req_groups[i];
        const unsigned int req_ac = requests[i];
        if (req_grp == group_by && req_ac > 0u) {{
            found_idx = i;
            target_aircraft = req_ac;
            break;
        }}
    }}
    
    // Сохраняем в агентные переменные (временно используем repair_days для idx)
    FLAMEGPU->setVariable<unsigned int>("repair_days", found_idx);
    FLAMEGPU->setVariable<unsigned int>("intent_state", target_aircraft);  // 0 если не нашли
    
    // Переходим к Phase 3c
    FLAMEGPU->setVariable<unsigned int>("bi_counter", 3u);
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_phase3c_clear_and_assign(max_frames: int) -> str:
    """
    Phase 3c: Очистка запроса и назначение
    ТОЛЬКО WRITE в mp_replacement_request/group (без чтения!)
    Все данные приходят из агентных переменных (Phase 3b)
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_fifo_phase3c_assign, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int phase = FLAMEGPU->getVariable<unsigned int>("bi_counter");
    
    if (phase != 3u) {{
        return flamegpu::ALIVE;
    }}
    
    // Данные из Phase 3b
    const unsigned int found_idx = FLAMEGPU->getVariable<unsigned int>("repair_days");
    const unsigned int target_aircraft = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    if (target_aircraft == 0u || found_idx >= {max_frames}u) {{
        // Не нашли запрос — возвращаем в нормальное состояние
        FLAMEGPU->setVariable<unsigned int>("bi_counter", 1u);
        FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);  // serviceable
        FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
        return flamegpu::ALIVE;
    }}
    
    // === WRITE ONLY: очищаем запрос ===
    auto requests = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_request");
    auto req_groups = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_group");
    
    requests[found_idx].exchange(0u);
    req_groups[found_idx].exchange(0u);
    
    // Назначаем на планер!
    FLAMEGPU->setVariable<unsigned int>("aircraft_number", target_aircraft);
    FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);  // → operations
    FLAMEGPU->setVariable<unsigned int>("queue_position", 0u);
    FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);  // Восстанавливаем
    FLAMEGPU->setVariable<unsigned int>("bi_counter", 1u);
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_return_read_tail() -> str:
    """
    Return Phase A: Читаем текущий tail и сохраняем в агентную переменную
    ТОЛЬКО READ
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_fifo_return_read, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int queue_position = FLAMEGPU->getVariable<unsigned int>("queue_position");
    
    // Если уже в очереди — пропускаем
    if (queue_position > 0u) {{
        return flamegpu::ALIVE;
    }}
    
    if (group_by >= {MAX_GROUPS}u) {{
        return flamegpu::ALIVE;
    }}
    
    // === READ ONLY: читаем текущий tail ===
    auto queue_tail = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_queue_tail");
    const unsigned int current_tail = queue_tail[group_by];
    
    // Сохраняем в агентную переменную (используем repair_days временно)
    FLAMEGPU->setVariable<unsigned int>("repair_days", current_tail);
    
    // Помечаем что готовы к записи
    FLAMEGPU->setVariable<unsigned int>("bi_counter", 10u);  // Специальный маркер
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_return_write_tail() -> str:
    """
    Return Phase B: Инкрементируем tail и записываем queue_position
    ТОЛЬКО WRITE
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_fifo_return_write, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int marker = FLAMEGPU->getVariable<unsigned int>("bi_counter");
    
    if (marker != 10u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int saved_tail = FLAMEGPU->getVariable<unsigned int>("repair_days");
    
    if (group_by >= {MAX_GROUPS}u) {{
        FLAMEGPU->setVariable<unsigned int>("bi_counter", 1u);
        return flamegpu::ALIVE;
    }}
    
    // === WRITE ONLY: инкрементируем tail ===
    auto queue_tail = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_queue_tail");
    queue_tail[group_by] += 1u;
    
    // Записываем позицию (saved_tail — то что прочитали в Phase A)
    FLAMEGPU->setVariable<unsigned int>("queue_position", saved_tail);
    FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);  // Восстанавливаем
    FLAMEGPU->setVariable<unsigned int>("bi_counter", 1u);
    
    // Остаёмся в reserve (intent_state = 5)
    // FIX: НЕ переходим в serviceable — reserve имеет отдельную FIFO-очередь
    FLAMEGPU->setVariable<unsigned int>("intent_state", 5u);
    
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription, 
                 max_frames: int, max_days: int = 3650):
    """Регистрирует пятифазную FIFO архитектуру (избегаем race conditions)"""
    
    # === Phase 1: Write requests (operations) ===
    rtc_p1 = get_rtc_phase1_write_request(max_frames)
    fn_p1 = agent.newRTCFunction("rtc_fifo_phase1_write", rtc_p1)
    fn_p1.setInitialState("operations")
    fn_p1.setEndState("operations")
    
    layer_p1 = model.newLayer("layer_fifo_phase1")
    layer_p1.addAgentFunction(fn_p1)
    
    # === Phase 2: Read head (serviceable) ===
    rtc_p2 = get_rtc_phase2_read_head(max_frames)
    fn_p2 = agent.newRTCFunction("rtc_fifo_phase2_read", rtc_p2)
    fn_p2.setInitialState("serviceable")
    fn_p2.setEndState("serviceable")
    
    layer_p2 = model.newLayer("layer_fifo_phase2")
    layer_p2.addAgentFunction(fn_p2)
    
    # === Phase 3a: Increment head (serviceable, WRITE ONLY) ===
    rtc_p3a = get_rtc_phase3a_increment_head()
    fn_p3a = agent.newRTCFunction("rtc_fifo_phase3a_head", rtc_p3a)
    fn_p3a.setInitialState("serviceable")
    fn_p3a.setEndState("serviceable")
    
    layer_p3a = model.newLayer("layer_fifo_phase3a")
    layer_p3a.addAgentFunction(fn_p3a)
    
    # === Phase 3b: Find request (serviceable, READ ONLY) ===
    rtc_p3b = get_rtc_phase3b_find_request(max_frames)
    fn_p3b = agent.newRTCFunction("rtc_fifo_phase3b_find", rtc_p3b)
    fn_p3b.setInitialState("serviceable")
    fn_p3b.setEndState("serviceable")
    
    layer_p3b = model.newLayer("layer_fifo_phase3b")
    layer_p3b.addAgentFunction(fn_p3b)
    
    # === Phase 3c: Clear and assign (serviceable, WRITE) ===
    rtc_p3c = get_rtc_phase3c_clear_and_assign(max_frames)
    fn_p3c = agent.newRTCFunction("rtc_fifo_phase3c_assign", rtc_p3c)
    fn_p3c.setInitialState("serviceable")
    fn_p3c.setEndState("serviceable")
    
    layer_p3c = model.newLayer("layer_fifo_phase3c")
    layer_p3c.addAgentFunction(fn_p3c)
    
    # === Return Phase A: Read tail (reserve) ===
    rtc_return_read = get_rtc_return_read_tail()
    fn_return_read = agent.newRTCFunction("rtc_fifo_return_read", rtc_return_read)
    fn_return_read.setInitialState("reserve")
    fn_return_read.setEndState("reserve")
    
    layer_return_read = model.newLayer("layer_fifo_return_read")
    layer_return_read.addAgentFunction(fn_return_read)
    
    # === Return Phase B: Write tail (reserve) ===
    rtc_return_write = get_rtc_return_write_tail()
    fn_return_write = agent.newRTCFunction("rtc_fifo_return_write", rtc_return_write)
    fn_return_write.setInitialState("reserve")
    fn_return_write.setEndState("reserve")
    
    layer_return_write = model.newLayer("layer_fifo_return_write")
    layer_return_write.addAgentFunction(fn_return_write)
    
    print("  RTC модуль units_fifo зарегистрирован (7 слоёв: P1, P2, P3a/b/c, returnA/B)")

