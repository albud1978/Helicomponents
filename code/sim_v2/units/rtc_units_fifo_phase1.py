#!/usr/bin/env python3
"""
RTC модуль FIFO Phase 1: Запрос замены (READ phase)

Двухфазная архитектура для избежания race conditions:
- Phase 1 (этот модуль): агрегаты с исчерпанным ресурсом записывают запросы
- Phase 2: исправные агрегаты из пула обрабатывают запросы

Логика:
1. Агрегат в operations проверяет ppr >= oh или sne >= ll
2. Если условие выполнено — атомарно инкрементирует mp_request_count[group_by]
3. Записывает свой aircraft_number в mp_replacement_request[idx]

Дата: 05.01.2026
"""

import pyflamegpu as fg

MAX_GROUPS = 50  # Максимум групп агрегатов


def get_rtc_code_phase1(max_frames: int) -> str:
    """
    CUDA код Phase 1: запись запросов на замену
    
    Атомарные операции:
    - atomicAdd для счётчика запросов mp_request_count[group_by]
    - exchange для записи aircraft_number в mp_replacement_request[idx]
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_fifo_phase1_request, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Функция привязана к state=operations, проверка state не нужна
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Только агрегаты на планере (aircraft_number > 0)
    if (aircraft_number == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Условия замены:
    // 1. ppr >= oh (межремонтный ресурс исчерпан) — ремонт
    // 2. sne >= ll (назначенный ресурс исчерпан) — списание
    // 3. sne >= br (breakeven) — дешевле списать
    bool needs_repair = (oh > 0u && ppr >= oh);
    bool needs_storage = (ll > 0u && sne >= ll) || (br > 0u && sne >= br);
    
    if (!needs_repair && !needs_storage) {{
        return flamegpu::ALIVE;  // Ресурс ещё есть
    }}
    
    // === Phase 1: Запись запроса ===
    
    // 1. Атомарно инкрементируем счётчик запросов для группы
    auto request_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_request_count");
    if (group_by < {MAX_GROUPS}u) {{
        request_count[group_by] += 1u;  // atomicAdd
    }}
    
    // 2. Записываем детали запроса
    auto requests = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_request");
    auto req_groups = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_group");
    
    if (idx < {max_frames}u) {{
        requests[idx].exchange(aircraft_number);  // Для какого планера
        req_groups[idx].exchange(group_by);       // Какой тип нужен
    }}
    
    // 3. Устанавливаем intent (переход произойдёт в transition модуле)
    if (needs_storage) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);  // → storage
    }} else {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);  // → repair
    }}
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_code_reset_counts() -> str:
    """
    CUDA код для сброса счётчиков в начале дня
    Выполняется ОДНИМ агентом (idx == 0)
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_fifo_reset_counts, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Только первый агент сбрасывает счётчики
    if (idx != 0u) {{
        return flamegpu::ALIVE;
    }}
    
    auto request_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_request_count");
    
    // Сброс всех счётчиков
    for (unsigned int g = 0u; g < {MAX_GROUPS}u; ++g) {{
        request_count[g].exchange(0u);
    }}
    
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription, 
                 max_frames: int, max_days: int = 3650):
    """Регистрирует RTC функции Phase 1 (запросы на замену)"""
    
    # === Функция сброса счётчиков (в начале дня) ===
    rtc_reset = get_rtc_code_reset_counts()
    fn_reset = agent.newRTCFunction("rtc_units_fifo_reset_counts", rtc_reset)
    fn_reset.setInitialState("operations")
    fn_reset.setEndState("operations")
    
    # === Функция записи запросов ===
    rtc_phase1 = get_rtc_code_phase1(max_frames)
    fn_request = agent.newRTCFunction("rtc_units_fifo_phase1_request", rtc_phase1)
    fn_request.setInitialState("operations")
    fn_request.setEndState("operations")
    
    # === Слои ===
    # Сброс должен быть ПЕРВЫМ в дне
    layer_reset = model.newLayer("layer_units_fifo_reset")
    layer_reset.addAgentFunction(fn_reset)
    
    # Запросы после наработки (dt/sne уже обновлены)
    layer_request = model.newLayer("layer_units_fifo_phase1")
    layer_request.addAgentFunction(fn_request)
    
    print("  RTC модуль units_fifo_phase1 зарегистрирован (2 слоя)")

