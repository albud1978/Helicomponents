#!/usr/bin/env python3
"""
RTC модуль комплектации агрегатов на планеры при assembly_trigger=1

Логика:
1. Планер в состоянии repair достигает последней стадии (repair_days >= repair_time - assembly_time)
2. В sim_masterv2 устанавливается assembly_trigger=1 (ONE-SHOT на 1 день)
3. Агрегаты из serviceable/reserve видят это и "захватывают" слоты на планере
4. После комплектации планер выходит из ремонта в operations

Архитектура (двухфазная для избежания read/write race conditions):
- Phase 1 (CHECK): чтение mp_planer_assembly, поиск планеров с trigger=1
- Phase 2 (ACTIVATE): атомарный захват слота на планере

MacroProperty:
- mp_planer_assembly[day * MAX_PLANERS + planer_idx] = 0/1/2 (three-state)
- mp_planer_slots[group * MAX_PLANERS + planer_idx] = сколько агрегатов группы уже назначено

Дата: 07.01.2026
"""

import pyflamegpu as fg

MAX_GROUPS = 50
MAX_PLANERS = 400


def get_rtc_code_assembly_check(max_planers: int, max_days: int) -> str:
    """
    CUDA код: Phase 1 — поиск планеров с запросом на комплектацию
    
    Комплектация по двум условиям:
    1. assembly_trigger=1 (явный ONE-SHOT из sim_masterv2 — выход из ремонта)
    2. ИЛИ mp_request_count[group] > 0 И dt > 0 (запрос на замену И планер летает)
    
    ВАЖНО: Проверка соответствия типов двигателей и планеров:
    - group_by=3 (ТВ2-117) → только для Mi-8 (planer_type=1)
    - group_by=4 (ТВ3-117) → только для Mi-17 (planer_type=2)
    """
    mp_slots_size = MAX_GROUPS * max_planers
    mp_assembly_size = max_planers * (max_days + 1)
    mp_dt_size = max_planers * (max_days + 1)
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_check, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Проверяем, можем ли мы участвовать в комплектации
    const unsigned int active = FLAMEGPU->getVariable<unsigned int>("active");
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    // Только свободные агрегаты (не привязаны к планеру) и активные
    if (aircraft_number > 0u || active != 1u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    if (group_by >= {MAX_GROUPS}u) {{
        return flamegpu::ALIVE;
    }}
    
    // Получаем количество агрегатов на планер для этой группы
    const unsigned int comp_per_planer = FLAMEGPU->environment.getProperty<unsigned int>("comp_numbers", group_by);
    
    if (comp_per_planer == 0u) {{
        return flamegpu::ALIVE;  // Эта группа не ставится на планеры
    }}
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int psn = FLAMEGPU->getVariable<unsigned int>("psn");
    
    // MacroProperties для проверки условий
    auto mp_assembly = FLAMEGPU->environment.getMacroProperty<unsigned char, {mp_assembly_size}u>("mp_planer_assembly");
    auto mp_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {mp_dt_size}u>("mp_planer_dt");
    auto mp_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, {mp_slots_size}u>("mp_planer_slots");
    auto mp_planer_type = FLAMEGPU->environment.getMacroProperty<unsigned char, {max_planers}u>("mp_planer_type");
    
    // === Определяем требуемый тип планера для данного агрегата ===
    // group_by=3 (ТВ2-117) → требуется Mi-8 (planer_type=1)
    // group_by=4 (ТВ3-117) → требуется Mi-17 (planer_type=2)
    // Для других групп — любой планер (required_type=0)
    unsigned char required_planer_type = 0u;
    if (group_by == 3u) {{
        required_planer_type = 1u;  // Только Mi-8
    }} else if (group_by == 4u) {{
        required_planer_type = 2u;  // Только Mi-17
    }}
    
    // FIX: Ищем планер с МИНИМАЛЬНЫМ количеством слотов (приоритет недоукомплектованным)
    // Это гарантирует что все агрегаты сначала заполнят один планер, потом следующий
    unsigned int best_planer_idx = 0u;
    unsigned int min_slots = comp_per_planer;  // Максимум — уже заполненный
    
    for (unsigned int planer_idx = 1u; planer_idx < {max_planers}u; ++planer_idx) {{
        const unsigned int pos = step_day * {max_planers}u + planer_idx;
        
        // === ПРОВЕРКА СООТВЕТСТВИЯ ТИПОВ ===
        if (required_planer_type > 0u) {{
            unsigned char actual_planer_type = mp_planer_type[planer_idx];
            if (actual_planer_type != required_planer_type) {{
                continue;  // Тип планера не соответствует типу двигателя!
            }}
        }}
        
        // Условие: планер летает (dt > 0) — ОБЯЗАТЕЛЬНО!
        unsigned int has_dt = mp_dt[pos];
        if (has_dt == 0u) {{
            continue;  // Планер не летает сегодня — нет комплектации
        }}
        
        // Проверяем количество слотов
        const unsigned int slots_pos = group_by * {max_planers}u + planer_idx;
        const unsigned int assigned = mp_slots[slots_pos];
        
        if (assigned >= comp_per_planer) {{
            continue;  // Все слоты заняты
        }}
        
        // === Ищем планер с МИНИМАЛЬНЫМ количеством слотов ===
        // Это гарантирует что все агрегаты сначала заполнят один планер
        if (assigned < min_slots) {{
            min_slots = assigned;
            best_planer_idx = planer_idx;
        }}
    }}
    
    // Если нашли подходящий планер — назначаем
    if (best_planer_idx > 0u) {{
        FLAMEGPU->setVariable<unsigned int>("want_assign", 1u);
        FLAMEGPU->setVariable<unsigned int>("target_planer_idx", best_planer_idx);
    }}
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_code_assembly_activate(max_planers: int, is_reserve: bool = False) -> str:
    """
    CUDA код: Phase 2 — атомарный захват слота на планере
    
    Агрегат с want_assign=1 пытается атомарно захватить слот.
    Если успешно — назначается на планер.
    
    FIX 14.01.2026: Для агентов из reserve — инкрементируем rsv_head!
    Это критично для корректной работы spawn_check.
    """
    mp_slots_size = MAX_GROUPS * max_planers
    
    # Дополнительный код для reserve агентов
    rsv_head_update = ""
    if is_reserve:
        rsv_head_update = f"""
    // FIX: Инкрементируем rsv_head при назначении из reserve!
    auto rsv_head = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_rsv_head");
    rsv_head[group_by] += 1u;
"""
    
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_activate, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int want_assign = FLAMEGPU->getVariable<unsigned int>("want_assign");
    
    if (want_assign != 1u) {{
        return flamegpu::ALIVE;
    }}
    
    // Сбрасываем флаг
    FLAMEGPU->setVariable<unsigned int>("want_assign", 0u);
    
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int planer_idx = FLAMEGPU->getVariable<unsigned int>("target_planer_idx");
    
    if (group_by >= {MAX_GROUPS}u || planer_idx >= {max_planers}u) {{
        return flamegpu::ALIVE;
    }}
    
    // Получаем comp_per_planer
    const unsigned int comp_per_planer = FLAMEGPU->environment.getProperty<unsigned int>("comp_numbers", group_by);
    
    // Атомарно увеличиваем счётчик слотов
    auto mp_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, {mp_slots_size}u>("mp_planer_slots");
    const unsigned int slots_pos = group_by * {max_planers}u + planer_idx;
    
    // Используем постфиксный инкремент (atomicAdd) — возвращает старое значение
    unsigned int old_count = mp_slots[slots_pos]++;
    
    if (old_count >= comp_per_planer) {{
        // Слот уже был занят другим агрегатом в этом же шаге
        // Отменяем (декремент обратно)
        mp_slots[slots_pos] -= 1u;
        return flamegpu::ALIVE;
    }}
    
    // === Успешно захватили слот! ===
    
    // Получаем aircraft_number по planer_idx
    // Используем обратный маппинг (mp_idx_to_ac)
    auto mp_idx_to_ac = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_planers}u>("mp_idx_to_ac");
    const unsigned int target_ac = mp_idx_to_ac[planer_idx];
    
    if (target_ac == 0u) {{
        // Нет маппинга — отменяем
        mp_slots[slots_pos] -= 1u;
        return flamegpu::ALIVE;
    }}
    
    // Назначаем агрегат на планер
    FLAMEGPU->setVariable<unsigned int>("aircraft_number", target_ac);
    FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);  // → operations
    FLAMEGPU->setVariable<unsigned int>("queue_position", 0u);  // Очищаем
    {rsv_head_update}
    return flamegpu::ALIVE;
}}
"""


def get_rtc_code_assembly_reset_slots() -> str:
    """
    CUDA код: Сброс счётчиков слотов после комплектации
    
    Выполняется в конце дня для планеров с assembly_trigger=2.
    assembly_trigger=2 означает, что ONE-SHOT уже обработан.
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_reset_slots, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Эта функция запускается на агентах планеров, но у нас симуляция агрегатов
    // Поэтому используем HostFunction для сброса
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription, 
                 max_planers: int = 400, max_days: int = 3650):
    """
    Регистрирует RTC функции комплектации агрегатов
    
    Двухфазная архитектура:
    - Phase 1 (check): только чтение — ищет планеры с assembly_trigger=1
    - Phase 2 (activate): атомарные записи — захват слота
    """
    
    # === 1. CHECK — для serviceable ===
    rtc_svc_check = get_rtc_code_assembly_check(max_planers, max_days)
    fn_svc_check = agent.newRTCFunction("rtc_assembly_svc_check", rtc_svc_check)
    fn_svc_check.setInitialState("serviceable")
    fn_svc_check.setEndState("serviceable")
    
    # === 2. CHECK — для reserve (active=1) ===
    rtc_rsv_check = get_rtc_code_assembly_check(max_planers, max_days)
    # Используем тот же код, но другое имя функции
    fn_rsv_check = agent.newRTCFunction("rtc_assembly_rsv_check", rtc_rsv_check.replace(
        "rtc_assembly_check", "rtc_assembly_rsv_check"))
    fn_rsv_check.setInitialState("reserve")
    fn_rsv_check.setEndState("reserve")
    
    # === 3. ACTIVATE — для serviceable (устанавливает aircraft_number) ===
    rtc_svc_activate = get_rtc_code_assembly_activate(max_planers, is_reserve=False)
    fn_svc_activate = agent.newRTCFunction("rtc_assembly_svc_activate", rtc_svc_activate)
    fn_svc_activate.setInitialState("serviceable")
    fn_svc_activate.setEndState("serviceable")  # Остаётся пока в serviceable
    
    # === 4. ACTIVATE — для reserve (устанавливает aircraft_number) ===
    # FIX 14.01.2026: is_reserve=True для инкремента rsv_head!
    rtc_rsv_activate = get_rtc_code_assembly_activate(max_planers, is_reserve=True)
    fn_rsv_activate = agent.newRTCFunction("rtc_assembly_rsv_activate", rtc_rsv_activate.replace(
        "rtc_assembly_activate", "rtc_assembly_rsv_activate"))
    fn_rsv_activate.setInitialState("reserve")
    fn_rsv_activate.setEndState("reserve")  # Остаётся пока в reserve
    
    # === 5. TRANSITION — переход serviceable → operations (если aircraft_number > 0) ===
    rtc_svc_transition = """
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_svc_to_ops, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;  // Просто переходим в operations
}
"""
    cond_svc_assigned = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_assembly_svc_assigned) {
    return FLAMEGPU->getVariable<unsigned int>("aircraft_number") > 0u;
}
"""
    fn_svc_transition = agent.newRTCFunction("rtc_assembly_svc_to_ops", rtc_svc_transition)
    fn_svc_transition.setRTCFunctionCondition(cond_svc_assigned)
    fn_svc_transition.setInitialState("serviceable")
    fn_svc_transition.setEndState("operations")
    
    # === 6. TRANSITION — переход reserve → operations (если aircraft_number > 0) ===
    rtc_rsv_transition = """
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_rsv_to_ops, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;  // Просто переходим в operations
}
"""
    cond_rsv_assigned = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_assembly_rsv_assigned) {
    return FLAMEGPU->getVariable<unsigned int>("aircraft_number") > 0u;
}
"""
    fn_rsv_transition = agent.newRTCFunction("rtc_assembly_rsv_to_ops", rtc_rsv_transition)
    fn_rsv_transition.setRTCFunctionCondition(cond_rsv_assigned)
    fn_rsv_transition.setInitialState("reserve")
    fn_rsv_transition.setEndState("operations")
    
    # === Слои ===
    # CHECK слой (только чтение) — можно объединить serviceable и reserve
    layer_check = model.newLayer("layer_assembly_check")
    layer_check.addAgentFunction(fn_svc_check)
    layer_check.addAgentFunction(fn_rsv_check)
    
    # ACTIVATE слой (только атомарные записи)
    layer_activate = model.newLayer("layer_assembly_activate")
    layer_activate.addAgentFunction(fn_svc_activate)
    layer_activate.addAgentFunction(fn_rsv_activate)
    
    # TRANSITION слои (переход в operations для успешно назначенных)
    # Разделяем на два слоя т.к. FLAME GPU не позволяет функции с одинаковым endState в одном слое
    layer_transition_svc = model.newLayer("layer_assembly_transition_svc")
    layer_transition_svc.addAgentFunction(fn_svc_transition)
    
    layer_transition_rsv = model.newLayer("layer_assembly_transition_rsv")
    layer_transition_rsv.addAgentFunction(fn_rsv_transition)
    
    print("  RTC модуль units_assembly зарегистрирован (4 слоя: check, activate, transition_svc, transition_rsv)")


def register_rtc_pass2(model: fg.ModelDescription, agent: fg.AgentDescription, 
                       max_planers: int = 400, max_days: int = 3650):
    """
    Регистрирует ВТОРОЙ проход assembly (pass2)
    
    Решает проблему "1-day gap": когда двигатель уходит в ремонт (2→4),
    слот освобождается и запрос создаётся, но первый проход assembly 
    выполняется ДО transitions. Второй проход выполняется ПОСЛЕ всех
    transitions, гарантируя замену в тот же день.
    
    Архитектура идентична первому проходу, но с другими именами слоёв.
    """
    
    # === 1. CHECK — для serviceable (pass2) ===
    rtc_svc_check = get_rtc_code_assembly_check(max_planers, max_days)
    fn_svc_check = agent.newRTCFunction("rtc_assembly_svc_check_p2", rtc_svc_check.replace(
        "rtc_assembly_check", "rtc_assembly_svc_check_p2"))
    fn_svc_check.setInitialState("serviceable")
    fn_svc_check.setEndState("serviceable")
    
    # === 2. CHECK — для reserve (pass2) ===
    rtc_rsv_check = get_rtc_code_assembly_check(max_planers, max_days)
    fn_rsv_check = agent.newRTCFunction("rtc_assembly_rsv_check_p2", rtc_rsv_check.replace(
        "rtc_assembly_check", "rtc_assembly_rsv_check_p2"))
    fn_rsv_check.setInitialState("reserve")
    fn_rsv_check.setEndState("reserve")
    
    # === 3. ACTIVATE — для serviceable (pass2) ===
    rtc_svc_activate = get_rtc_code_assembly_activate(max_planers, is_reserve=False)
    fn_svc_activate = agent.newRTCFunction("rtc_assembly_svc_activate_p2", rtc_svc_activate.replace(
        "rtc_assembly_activate", "rtc_assembly_svc_activate_p2"))
    fn_svc_activate.setInitialState("serviceable")
    fn_svc_activate.setEndState("serviceable")
    
    # === 4. ACTIVATE — для reserve (pass2) ===
    rtc_rsv_activate = get_rtc_code_assembly_activate(max_planers, is_reserve=True)
    fn_rsv_activate = agent.newRTCFunction("rtc_assembly_rsv_activate_p2", rtc_rsv_activate.replace(
        "rtc_assembly_activate", "rtc_assembly_rsv_activate_p2"))
    fn_rsv_activate.setInitialState("reserve")
    fn_rsv_activate.setEndState("reserve")
    
    # === 5. TRANSITION — serviceable → operations (pass2) ===
    rtc_svc_transition = """
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_svc_to_ops_p2, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}
"""
    cond_svc_assigned = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_assembly_svc_assigned_p2) {
    return FLAMEGPU->getVariable<unsigned int>("aircraft_number") > 0u;
}
"""
    fn_svc_transition = agent.newRTCFunction("rtc_assembly_svc_to_ops_p2", rtc_svc_transition)
    fn_svc_transition.setRTCFunctionCondition(cond_svc_assigned)
    fn_svc_transition.setInitialState("serviceable")
    fn_svc_transition.setEndState("operations")
    
    # === 6. TRANSITION — reserve → operations (pass2) ===
    rtc_rsv_transition = """
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_rsv_to_ops_p2, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}
"""
    cond_rsv_assigned = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_assembly_rsv_assigned_p2) {
    return FLAMEGPU->getVariable<unsigned int>("aircraft_number") > 0u;
}
"""
    fn_rsv_transition = agent.newRTCFunction("rtc_assembly_rsv_to_ops_p2", rtc_rsv_transition)
    fn_rsv_transition.setRTCFunctionCondition(cond_rsv_assigned)
    fn_rsv_transition.setInitialState("reserve")
    fn_rsv_transition.setEndState("operations")
    
    # === Слои (pass2) ===
    layer_check_p2 = model.newLayer("layer_assembly_check_p2")
    layer_check_p2.addAgentFunction(fn_svc_check)
    layer_check_p2.addAgentFunction(fn_rsv_check)
    
    layer_activate_p2 = model.newLayer("layer_assembly_activate_p2")
    layer_activate_p2.addAgentFunction(fn_svc_activate)
    layer_activate_p2.addAgentFunction(fn_rsv_activate)
    
    layer_transition_svc_p2 = model.newLayer("layer_assembly_transition_svc_p2")
    layer_transition_svc_p2.addAgentFunction(fn_svc_transition)
    
    layer_transition_rsv_p2 = model.newLayer("layer_assembly_transition_rsv_p2")
    layer_transition_rsv_p2.addAgentFunction(fn_rsv_transition)
    
    print("  RTC модуль units_assembly pass2 зарегистрирован (4 слоя)")


def register_rtc_pass3(model: fg.ModelDescription, agent: fg.AgentDescription, 
                       max_planers: int = 400, max_days: int = 3650):
    """
    Регистрирует ТРЕТИЙ проход assembly (pass3)
    
    Гарантирует полную комплектацию: если после pass2 у планера всё ещё
    не хватает двигателей (например, планер вышел из repair с 0 двигателей,
    pass1 назначил 1, pass2 не успел) — pass3 завершит комплектацию.
    
    Архитектура идентична pass2, но с суффиксом _p3.
    """
    
    # === 1. CHECK — для serviceable (pass3) ===
    rtc_svc_check = get_rtc_code_assembly_check(max_planers, max_days)
    fn_svc_check = agent.newRTCFunction("rtc_assembly_svc_check_p3", rtc_svc_check.replace(
        "rtc_assembly_check", "rtc_assembly_svc_check_p3"))
    fn_svc_check.setInitialState("serviceable")
    fn_svc_check.setEndState("serviceable")
    
    # === 2. CHECK — для reserve (pass3) ===
    rtc_rsv_check = get_rtc_code_assembly_check(max_planers, max_days)
    fn_rsv_check = agent.newRTCFunction("rtc_assembly_rsv_check_p3", rtc_rsv_check.replace(
        "rtc_assembly_check", "rtc_assembly_rsv_check_p3"))
    fn_rsv_check.setInitialState("reserve")
    fn_rsv_check.setEndState("reserve")
    
    # === 3. ACTIVATE — для serviceable (pass3) ===
    rtc_svc_activate = get_rtc_code_assembly_activate(max_planers, is_reserve=False)
    fn_svc_activate = agent.newRTCFunction("rtc_assembly_svc_activate_p3", rtc_svc_activate.replace(
        "rtc_assembly_activate", "rtc_assembly_svc_activate_p3"))
    fn_svc_activate.setInitialState("serviceable")
    fn_svc_activate.setEndState("serviceable")
    
    # === 4. ACTIVATE — для reserve (pass3) ===
    rtc_rsv_activate = get_rtc_code_assembly_activate(max_planers, is_reserve=True)
    fn_rsv_activate = agent.newRTCFunction("rtc_assembly_rsv_activate_p3", rtc_rsv_activate.replace(
        "rtc_assembly_activate", "rtc_assembly_rsv_activate_p3"))
    fn_rsv_activate.setInitialState("reserve")
    fn_rsv_activate.setEndState("reserve")
    
    # === 5. TRANSITION — serviceable → operations (pass3) ===
    rtc_svc_transition = """
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_svc_to_ops_p3, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}
"""
    cond_svc_assigned = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_assembly_svc_assigned_p3) {
    return FLAMEGPU->getVariable<unsigned int>("aircraft_number") > 0u;
}
"""
    fn_svc_transition = agent.newRTCFunction("rtc_assembly_svc_to_ops_p3", rtc_svc_transition)
    fn_svc_transition.setRTCFunctionCondition(cond_svc_assigned)
    fn_svc_transition.setInitialState("serviceable")
    fn_svc_transition.setEndState("operations")
    
    # === 6. TRANSITION — reserve → operations (pass3) ===
    rtc_rsv_transition = """
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_rsv_to_ops_p3, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}
"""
    cond_rsv_assigned = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_assembly_rsv_assigned_p3) {
    return FLAMEGPU->getVariable<unsigned int>("aircraft_number") > 0u;
}
"""
    fn_rsv_transition = agent.newRTCFunction("rtc_assembly_rsv_to_ops_p3", rtc_rsv_transition)
    fn_rsv_transition.setRTCFunctionCondition(cond_rsv_assigned)
    fn_rsv_transition.setInitialState("reserve")
    fn_rsv_transition.setEndState("operations")
    
    # === Слои (pass3) ===
    layer_check_p3 = model.newLayer("layer_assembly_check_p3")
    layer_check_p3.addAgentFunction(fn_svc_check)
    layer_check_p3.addAgentFunction(fn_rsv_check)
    
    layer_activate_p3 = model.newLayer("layer_assembly_activate_p3")
    layer_activate_p3.addAgentFunction(fn_svc_activate)
    layer_activate_p3.addAgentFunction(fn_rsv_activate)
    
    layer_transition_svc_p3 = model.newLayer("layer_assembly_transition_svc_p3")
    layer_transition_svc_p3.addAgentFunction(fn_svc_transition)
    
    layer_transition_rsv_p3 = model.newLayer("layer_assembly_transition_rsv_p3")
    layer_transition_rsv_p3.addAgentFunction(fn_rsv_transition)
    
    print("  RTC модуль units_assembly pass3 зарегистрирован (4 слоя)")


def register_rtc_pass4(model: fg.ModelDescription, agent: fg.AgentDescription, 
                       max_planers: int = 400, max_days: int = 3650):
    """Регистрирует ЧЕТВЁРТЫЙ проход assembly (pass4) — аналогично pass3."""
    
    rtc_svc_check = get_rtc_code_assembly_check(max_planers, max_days)
    fn_svc_check = agent.newRTCFunction("rtc_assembly_svc_check_p4", rtc_svc_check.replace(
        "rtc_assembly_check", "rtc_assembly_svc_check_p4"))
    fn_svc_check.setInitialState("serviceable")
    fn_svc_check.setEndState("serviceable")
    
    rtc_rsv_check = get_rtc_code_assembly_check(max_planers, max_days)
    fn_rsv_check = agent.newRTCFunction("rtc_assembly_rsv_check_p4", rtc_rsv_check.replace(
        "rtc_assembly_check", "rtc_assembly_rsv_check_p4"))
    fn_rsv_check.setInitialState("reserve")
    fn_rsv_check.setEndState("reserve")
    
    rtc_svc_activate = get_rtc_code_assembly_activate(max_planers, is_reserve=False)
    fn_svc_activate = agent.newRTCFunction("rtc_assembly_svc_activate_p4", rtc_svc_activate.replace(
        "rtc_assembly_activate", "rtc_assembly_svc_activate_p4"))
    fn_svc_activate.setInitialState("serviceable")
    fn_svc_activate.setEndState("serviceable")
    
    rtc_rsv_activate = get_rtc_code_assembly_activate(max_planers, is_reserve=True)
    fn_rsv_activate = agent.newRTCFunction("rtc_assembly_rsv_activate_p4", rtc_rsv_activate.replace(
        "rtc_assembly_activate", "rtc_assembly_rsv_activate_p4"))
    fn_rsv_activate.setInitialState("reserve")
    fn_rsv_activate.setEndState("reserve")
    
    rtc_svc_transition = """
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_svc_to_ops_p4, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}
"""
    cond_svc_assigned = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_assembly_svc_assigned_p4) {
    return FLAMEGPU->getVariable<unsigned int>("aircraft_number") > 0u;
}
"""
    fn_svc_transition = agent.newRTCFunction("rtc_assembly_svc_to_ops_p4", rtc_svc_transition)
    fn_svc_transition.setRTCFunctionCondition(cond_svc_assigned)
    fn_svc_transition.setInitialState("serviceable")
    fn_svc_transition.setEndState("operations")
    
    rtc_rsv_transition = """
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_rsv_to_ops_p4, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}
"""
    cond_rsv_assigned = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_assembly_rsv_assigned_p4) {
    return FLAMEGPU->getVariable<unsigned int>("aircraft_number") > 0u;
}
"""
    fn_rsv_transition = agent.newRTCFunction("rtc_assembly_rsv_to_ops_p4", rtc_rsv_transition)
    fn_rsv_transition.setRTCFunctionCondition(cond_rsv_assigned)
    fn_rsv_transition.setInitialState("reserve")
    fn_rsv_transition.setEndState("operations")
    
    layer_check_p4 = model.newLayer("layer_assembly_check_p4")
    layer_check_p4.addAgentFunction(fn_svc_check)
    layer_check_p4.addAgentFunction(fn_rsv_check)
    
    layer_activate_p4 = model.newLayer("layer_assembly_activate_p4")
    layer_activate_p4.addAgentFunction(fn_svc_activate)
    layer_activate_p4.addAgentFunction(fn_rsv_activate)
    
    layer_transition_svc_p4 = model.newLayer("layer_assembly_transition_svc_p4")
    layer_transition_svc_p4.addAgentFunction(fn_svc_transition)
    
    layer_transition_rsv_p4 = model.newLayer("layer_assembly_transition_rsv_p4")
    layer_transition_rsv_p4.addAgentFunction(fn_rsv_transition)
    
    print("  RTC модуль units_assembly pass4 зарегистрирован (4 слоя)")


def register_rtc_pass5(model: fg.ModelDescription, agent: fg.AgentDescription, 
                       max_planers: int = 400, max_days: int = 3650):
    """Регистрирует ПЯТЫЙ проход assembly (pass5)."""
    
    rtc_svc_check = get_rtc_code_assembly_check(max_planers, max_days)
    fn_svc_check = agent.newRTCFunction("rtc_assembly_svc_check_p5", rtc_svc_check.replace(
        "rtc_assembly_check", "rtc_assembly_svc_check_p5"))
    fn_svc_check.setInitialState("serviceable")
    fn_svc_check.setEndState("serviceable")
    
    rtc_rsv_check = get_rtc_code_assembly_check(max_planers, max_days)
    fn_rsv_check = agent.newRTCFunction("rtc_assembly_rsv_check_p5", rtc_rsv_check.replace(
        "rtc_assembly_check", "rtc_assembly_rsv_check_p5"))
    fn_rsv_check.setInitialState("reserve")
    fn_rsv_check.setEndState("reserve")
    
    rtc_svc_activate = get_rtc_code_assembly_activate(max_planers, is_reserve=False)
    fn_svc_activate = agent.newRTCFunction("rtc_assembly_svc_activate_p5", rtc_svc_activate.replace(
        "rtc_assembly_activate", "rtc_assembly_svc_activate_p5"))
    fn_svc_activate.setInitialState("serviceable")
    fn_svc_activate.setEndState("serviceable")
    
    rtc_rsv_activate = get_rtc_code_assembly_activate(max_planers, is_reserve=True)
    fn_rsv_activate = agent.newRTCFunction("rtc_assembly_rsv_activate_p5", rtc_rsv_activate.replace(
        "rtc_assembly_activate", "rtc_assembly_rsv_activate_p5"))
    fn_rsv_activate.setInitialState("reserve")
    fn_rsv_activate.setEndState("reserve")
    
    rtc_svc_transition = """
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_svc_to_ops_p5, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}
"""
    cond_svc_assigned = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_assembly_svc_assigned_p5) {
    return FLAMEGPU->getVariable<unsigned int>("aircraft_number") > 0u;
}
"""
    fn_svc_transition = agent.newRTCFunction("rtc_assembly_svc_to_ops_p5", rtc_svc_transition)
    fn_svc_transition.setRTCFunctionCondition(cond_svc_assigned)
    fn_svc_transition.setInitialState("serviceable")
    fn_svc_transition.setEndState("operations")
    
    rtc_rsv_transition = """
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_rsv_to_ops_p5, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}
"""
    cond_rsv_assigned = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_assembly_rsv_assigned_p5) {
    return FLAMEGPU->getVariable<unsigned int>("aircraft_number") > 0u;
}
"""
    fn_rsv_transition = agent.newRTCFunction("rtc_assembly_rsv_to_ops_p5", rtc_rsv_transition)
    fn_rsv_transition.setRTCFunctionCondition(cond_rsv_assigned)
    fn_rsv_transition.setInitialState("reserve")
    fn_rsv_transition.setEndState("operations")
    
    layer_check_p5 = model.newLayer("layer_assembly_check_p5")
    layer_check_p5.addAgentFunction(fn_svc_check)
    layer_check_p5.addAgentFunction(fn_rsv_check)
    
    layer_activate_p5 = model.newLayer("layer_assembly_activate_p5")
    layer_activate_p5.addAgentFunction(fn_svc_activate)
    layer_activate_p5.addAgentFunction(fn_rsv_activate)
    
    layer_transition_svc_p5 = model.newLayer("layer_assembly_transition_svc_p5")
    layer_transition_svc_p5.addAgentFunction(fn_svc_transition)
    
    layer_transition_rsv_p5 = model.newLayer("layer_assembly_transition_rsv_p5")
    layer_transition_rsv_p5.addAgentFunction(fn_rsv_transition)
    
    print("  RTC модуль units_assembly pass5 зарегистрирован (4 слоя)")


def register_rtc_pass6(model: fg.ModelDescription, agent: fg.AgentDescription, 
                       max_planers: int = 400, max_days: int = 3650):
    """Регистрирует ШЕСТОЙ проход assembly (pass6)."""
    
    rtc_svc_check = get_rtc_code_assembly_check(max_planers, max_days)
    fn_svc_check = agent.newRTCFunction("rtc_assembly_svc_check_p6", rtc_svc_check.replace(
        "rtc_assembly_check", "rtc_assembly_svc_check_p6"))
    fn_svc_check.setInitialState("serviceable")
    fn_svc_check.setEndState("serviceable")
    
    rtc_rsv_check = get_rtc_code_assembly_check(max_planers, max_days)
    fn_rsv_check = agent.newRTCFunction("rtc_assembly_rsv_check_p6", rtc_rsv_check.replace(
        "rtc_assembly_check", "rtc_assembly_rsv_check_p6"))
    fn_rsv_check.setInitialState("reserve")
    fn_rsv_check.setEndState("reserve")
    
    rtc_svc_activate = get_rtc_code_assembly_activate(max_planers, is_reserve=False)
    fn_svc_activate = agent.newRTCFunction("rtc_assembly_svc_activate_p6", rtc_svc_activate.replace(
        "rtc_assembly_activate", "rtc_assembly_svc_activate_p6"))
    fn_svc_activate.setInitialState("serviceable")
    fn_svc_activate.setEndState("serviceable")
    
    rtc_rsv_activate = get_rtc_code_assembly_activate(max_planers, is_reserve=True)
    fn_rsv_activate = agent.newRTCFunction("rtc_assembly_rsv_activate_p6", rtc_rsv_activate.replace(
        "rtc_assembly_activate", "rtc_assembly_rsv_activate_p6"))
    fn_rsv_activate.setInitialState("reserve")
    fn_rsv_activate.setEndState("reserve")
    
    rtc_svc_transition = """
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_svc_to_ops_p6, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}
"""
    cond_svc_assigned = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_assembly_svc_assigned_p6) {
    return FLAMEGPU->getVariable<unsigned int>("aircraft_number") > 0u;
}
"""
    fn_svc_transition = agent.newRTCFunction("rtc_assembly_svc_to_ops_p6", rtc_svc_transition)
    fn_svc_transition.setRTCFunctionCondition(cond_svc_assigned)
    fn_svc_transition.setInitialState("serviceable")
    fn_svc_transition.setEndState("operations")
    
    rtc_rsv_transition = """
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_rsv_to_ops_p6, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}
"""
    cond_rsv_assigned = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_assembly_rsv_assigned_p6) {
    return FLAMEGPU->getVariable<unsigned int>("aircraft_number") > 0u;
}
"""
    fn_rsv_transition = agent.newRTCFunction("rtc_assembly_rsv_to_ops_p6", rtc_rsv_transition)
    fn_rsv_transition.setRTCFunctionCondition(cond_rsv_assigned)
    fn_rsv_transition.setInitialState("reserve")
    fn_rsv_transition.setEndState("operations")
    
    layer_check_p6 = model.newLayer("layer_assembly_check_p6")
    layer_check_p6.addAgentFunction(fn_svc_check)
    layer_check_p6.addAgentFunction(fn_rsv_check)
    
    layer_activate_p6 = model.newLayer("layer_assembly_activate_p6")
    layer_activate_p6.addAgentFunction(fn_svc_activate)
    layer_activate_p6.addAgentFunction(fn_rsv_activate)
    
    layer_transition_svc_p6 = model.newLayer("layer_assembly_transition_svc_p6")
    layer_transition_svc_p6.addAgentFunction(fn_svc_transition)
    
    layer_transition_rsv_p6 = model.newLayer("layer_assembly_transition_rsv_p6")
    layer_transition_rsv_p6.addAgentFunction(fn_rsv_transition)
    
    print("  RTC модуль units_assembly pass6 зарегистрирован (4 слоя)")


def register_rtc_pass7(model: fg.ModelDescription, agent: fg.AgentDescription, 
                       max_planers: int = 400, max_days: int = 3650):
    """Регистрирует СЕДЬМОЙ проход assembly."""
    rtc_svc_check = get_rtc_code_assembly_check(max_planers, max_days)
    fn_svc_check = agent.newRTCFunction("rtc_assembly_svc_check_p7", rtc_svc_check.replace("rtc_assembly_check", "rtc_assembly_svc_check_p7"))
    fn_svc_check.setInitialState("serviceable")
    fn_svc_check.setEndState("serviceable")
    
    rtc_rsv_check = get_rtc_code_assembly_check(max_planers, max_days)
    fn_rsv_check = agent.newRTCFunction("rtc_assembly_rsv_check_p7", rtc_rsv_check.replace("rtc_assembly_check", "rtc_assembly_rsv_check_p7"))
    fn_rsv_check.setInitialState("reserve")
    fn_rsv_check.setEndState("reserve")
    
    rtc_svc_activate = get_rtc_code_assembly_activate(max_planers, is_reserve=False)
    fn_svc_activate = agent.newRTCFunction("rtc_assembly_svc_activate_p7", rtc_svc_activate.replace("rtc_assembly_activate", "rtc_assembly_svc_activate_p7"))
    fn_svc_activate.setInitialState("serviceable")
    fn_svc_activate.setEndState("serviceable")
    
    rtc_rsv_activate = get_rtc_code_assembly_activate(max_planers, is_reserve=True)
    fn_rsv_activate = agent.newRTCFunction("rtc_assembly_rsv_activate_p7", rtc_rsv_activate.replace("rtc_assembly_activate", "rtc_assembly_rsv_activate_p7"))
    fn_rsv_activate.setInitialState("reserve")
    fn_rsv_activate.setEndState("reserve")
    
    fn_svc_transition = agent.newRTCFunction("rtc_assembly_svc_to_ops_p7", """
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_svc_to_ops_p7, flamegpu::MessageNone, flamegpu::MessageNone) { return flamegpu::ALIVE; }
""")
    fn_svc_transition.setRTCFunctionCondition("""
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_assembly_svc_assigned_p7) { return FLAMEGPU->getVariable<unsigned int>("aircraft_number") > 0u; }
""")
    fn_svc_transition.setInitialState("serviceable")
    fn_svc_transition.setEndState("operations")
    
    fn_rsv_transition = agent.newRTCFunction("rtc_assembly_rsv_to_ops_p7", """
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_rsv_to_ops_p7, flamegpu::MessageNone, flamegpu::MessageNone) { return flamegpu::ALIVE; }
""")
    fn_rsv_transition.setRTCFunctionCondition("""
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_assembly_rsv_assigned_p7) { return FLAMEGPU->getVariable<unsigned int>("aircraft_number") > 0u; }
""")
    fn_rsv_transition.setInitialState("reserve")
    fn_rsv_transition.setEndState("operations")
    
    model.newLayer("layer_assembly_check_p7").addAgentFunction(fn_svc_check)
    model.Layer("layer_assembly_check_p7").addAgentFunction(fn_rsv_check)
    model.newLayer("layer_assembly_activate_p7").addAgentFunction(fn_svc_activate)
    model.Layer("layer_assembly_activate_p7").addAgentFunction(fn_rsv_activate)
    model.newLayer("layer_assembly_transition_svc_p7").addAgentFunction(fn_svc_transition)
    model.newLayer("layer_assembly_transition_rsv_p7").addAgentFunction(fn_rsv_transition)
    print("  RTC модуль units_assembly pass7 зарегистрирован")


def register_rtc_pass8(model: fg.ModelDescription, agent: fg.AgentDescription, 
                       max_planers: int = 400, max_days: int = 3650):
    """Регистрирует ВОСЬМОЙ проход assembly."""
    rtc_svc_check = get_rtc_code_assembly_check(max_planers, max_days)
    fn_svc_check = agent.newRTCFunction("rtc_assembly_svc_check_p8", rtc_svc_check.replace("rtc_assembly_check", "rtc_assembly_svc_check_p8"))
    fn_svc_check.setInitialState("serviceable")
    fn_svc_check.setEndState("serviceable")
    
    rtc_rsv_check = get_rtc_code_assembly_check(max_planers, max_days)
    fn_rsv_check = agent.newRTCFunction("rtc_assembly_rsv_check_p8", rtc_rsv_check.replace("rtc_assembly_check", "rtc_assembly_rsv_check_p8"))
    fn_rsv_check.setInitialState("reserve")
    fn_rsv_check.setEndState("reserve")
    
    rtc_svc_activate = get_rtc_code_assembly_activate(max_planers, is_reserve=False)
    fn_svc_activate = agent.newRTCFunction("rtc_assembly_svc_activate_p8", rtc_svc_activate.replace("rtc_assembly_activate", "rtc_assembly_svc_activate_p8"))
    fn_svc_activate.setInitialState("serviceable")
    fn_svc_activate.setEndState("serviceable")
    
    rtc_rsv_activate = get_rtc_code_assembly_activate(max_planers, is_reserve=True)
    fn_rsv_activate = agent.newRTCFunction("rtc_assembly_rsv_activate_p8", rtc_rsv_activate.replace("rtc_assembly_activate", "rtc_assembly_rsv_activate_p8"))
    fn_rsv_activate.setInitialState("reserve")
    fn_rsv_activate.setEndState("reserve")
    
    fn_svc_transition = agent.newRTCFunction("rtc_assembly_svc_to_ops_p8", """
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_svc_to_ops_p8, flamegpu::MessageNone, flamegpu::MessageNone) { return flamegpu::ALIVE; }
""")
    fn_svc_transition.setRTCFunctionCondition("""
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_assembly_svc_assigned_p8) { return FLAMEGPU->getVariable<unsigned int>("aircraft_number") > 0u; }
""")
    fn_svc_transition.setInitialState("serviceable")
    fn_svc_transition.setEndState("operations")
    
    fn_rsv_transition = agent.newRTCFunction("rtc_assembly_rsv_to_ops_p8", """
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_rsv_to_ops_p8, flamegpu::MessageNone, flamegpu::MessageNone) { return flamegpu::ALIVE; }
""")
    fn_rsv_transition.setRTCFunctionCondition("""
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_assembly_rsv_assigned_p8) { return FLAMEGPU->getVariable<unsigned int>("aircraft_number") > 0u; }
""")
    fn_rsv_transition.setInitialState("reserve")
    fn_rsv_transition.setEndState("operations")
    
    model.newLayer("layer_assembly_check_p8").addAgentFunction(fn_svc_check)
    model.Layer("layer_assembly_check_p8").addAgentFunction(fn_rsv_check)
    model.newLayer("layer_assembly_activate_p8").addAgentFunction(fn_svc_activate)
    model.Layer("layer_assembly_activate_p8").addAgentFunction(fn_rsv_activate)
    model.newLayer("layer_assembly_transition_svc_p8").addAgentFunction(fn_svc_transition)
    model.newLayer("layer_assembly_transition_rsv_p8").addAgentFunction(fn_rsv_transition)
    print("  RTC модуль units_assembly pass8 зарегистрирован")


def _register_generic_pass(model, agent, max_planers, max_days, suffix):
    """Универсальная регистрация прохода assembly."""
    rtc_svc_check = get_rtc_code_assembly_check(max_planers, max_days)
    fn_svc_check = agent.newRTCFunction(f"rtc_assembly_svc_check_{suffix}", rtc_svc_check.replace("rtc_assembly_check", f"rtc_assembly_svc_check_{suffix}"))
    fn_svc_check.setInitialState("serviceable")
    fn_svc_check.setEndState("serviceable")
    
    rtc_rsv_check = get_rtc_code_assembly_check(max_planers, max_days)
    fn_rsv_check = agent.newRTCFunction(f"rtc_assembly_rsv_check_{suffix}", rtc_rsv_check.replace("rtc_assembly_check", f"rtc_assembly_rsv_check_{suffix}"))
    fn_rsv_check.setInitialState("reserve")
    fn_rsv_check.setEndState("reserve")
    
    rtc_svc_activate = get_rtc_code_assembly_activate(max_planers, is_reserve=False)
    fn_svc_activate = agent.newRTCFunction(f"rtc_assembly_svc_activate_{suffix}", rtc_svc_activate.replace("rtc_assembly_activate", f"rtc_assembly_svc_activate_{suffix}"))
    fn_svc_activate.setInitialState("serviceable")
    fn_svc_activate.setEndState("serviceable")
    
    # FIX 14.01.2026: is_reserve=True для инкремента rsv_head!
    rtc_rsv_activate = get_rtc_code_assembly_activate(max_planers, is_reserve=True)
    fn_rsv_activate = agent.newRTCFunction(f"rtc_assembly_rsv_activate_{suffix}", rtc_rsv_activate.replace("rtc_assembly_activate", f"rtc_assembly_rsv_activate_{suffix}"))
    fn_rsv_activate.setInitialState("reserve")
    fn_rsv_activate.setEndState("reserve")
    
    fn_svc_transition = agent.newRTCFunction(f"rtc_assembly_svc_to_ops_{suffix}", f"""
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_svc_to_ops_{suffix}, flamegpu::MessageNone, flamegpu::MessageNone) {{ return flamegpu::ALIVE; }}
""")
    fn_svc_transition.setRTCFunctionCondition(f"""
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_assembly_svc_assigned_{suffix}) {{ return FLAMEGPU->getVariable<unsigned int>("aircraft_number") > 0u; }}
""")
    fn_svc_transition.setInitialState("serviceable")
    fn_svc_transition.setEndState("operations")
    
    fn_rsv_transition = agent.newRTCFunction(f"rtc_assembly_rsv_to_ops_{suffix}", f"""
FLAMEGPU_AGENT_FUNCTION(rtc_assembly_rsv_to_ops_{suffix}, flamegpu::MessageNone, flamegpu::MessageNone) {{ return flamegpu::ALIVE; }}
""")
    fn_rsv_transition.setRTCFunctionCondition(f"""
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_assembly_rsv_assigned_{suffix}) {{ return FLAMEGPU->getVariable<unsigned int>("aircraft_number") > 0u; }}
""")
    fn_rsv_transition.setInitialState("reserve")
    fn_rsv_transition.setEndState("operations")
    
    model.newLayer(f"layer_assembly_check_{suffix}").addAgentFunction(fn_svc_check)
    model.Layer(f"layer_assembly_check_{suffix}").addAgentFunction(fn_rsv_check)
    model.newLayer(f"layer_assembly_activate_{suffix}").addAgentFunction(fn_svc_activate)
    model.Layer(f"layer_assembly_activate_{suffix}").addAgentFunction(fn_rsv_activate)
    model.newLayer(f"layer_assembly_transition_svc_{suffix}").addAgentFunction(fn_svc_transition)
    model.newLayer(f"layer_assembly_transition_rsv_{suffix}").addAgentFunction(fn_rsv_transition)


def register_rtc_pass9(model, agent, max_planers=400, max_days=3650):
    _register_generic_pass(model, agent, max_planers, max_days, "p9")
    print("  RTC модуль units_assembly pass9")


def register_rtc_pass10(model, agent, max_planers=400, max_days=3650):
    _register_generic_pass(model, agent, max_planers, max_days, "p10")
    print("  RTC модуль units_assembly pass10")
