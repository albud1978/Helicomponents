"""
RTC модуль комплектации планеров агрегатами

Логика:
1. В начале каждого дня проверяем состояние планеров (mp_planer_state)
2. Для планеров в operations (state=2) проверяем, есть ли агрегаты
3. Если агрегатов не хватает — создаём запросы на комплектацию

Механизм:
- Каждый агрегат в serviceable/reserve проверяет, есть ли "свободные" планеры
  (планеры в operations без полного комплекта агрегатов своей группы)
- Если такой планер найден — агрегат назначается на него

Дата: 07.01.2026
"""

import pyflamegpu as fg

MAX_GROUPS = 50
MAX_PLANERS = 300  # Максимум планеров


def get_rtc_code_check_planer_demand(max_days: int = 3650) -> str:
    """
    CUDA код: проверка потребности планеров в агрегатах.
    Агрегат в serviceable проверяет, есть ли планеры без полного комплекта.
    
    Примечание: Этот модуль создаёт ЗАПРОСЫ для FIFO, которые потом обрабатываются.
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_planer_demand_check, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Функция привязана к state=serviceable
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int queue_position = FLAMEGPU->getVariable<unsigned int>("queue_position");
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    
    if (group_by >= {MAX_GROUPS}u) {{
        return flamegpu::ALIVE;
    }}
    
    // Читаем состояние планеров
    auto planer_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_days}u, {MAX_PLANERS}u>("mp_planer_state");
    
    // Читаем текущий request_count
    auto request_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_request_count");
    
    // Проверяем svc-очередь — только первый агент в группе создаёт запросы
    auto svc_head = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_svc_head");
    
    if (queue_position != svc_head[group_by]) {{
        return flamegpu::ALIVE;  // Не первый — ждём
    }}
    
    // Считаем, сколько планеров в operations
    unsigned int planers_in_ops = 0u;
    for (unsigned int ac = 1u; ac < {MAX_PLANERS}u; ++ac) {{
        if (planer_state[step_day][ac] == 2u) {{  // operations
            ++planers_in_ops;
        }}
    }}
    
    // Если планеров в operations > 0 и запросов нет — создаём запросы
    // Количество агрегатов на планер зависит от group_by:
    // - Лопасти (group_by=6): 5 на борту
    // - Другие агрегаты: 1 на борту (упрощение)
    unsigned int units_per_planer = (group_by == 6u) ? 5u : 1u;
    unsigned int needed = planers_in_ops * units_per_planer;
    
    // Читаем текущие запросы
    unsigned int current_requests = request_count[group_by];
    
    // Если нужно больше чем есть запросов — добавляем
    if (needed > current_requests) {{
        // Атомарно увеличиваем request_count
        request_count[group_by] += (needed - current_requests);
    }}
    
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription, 
                 max_days: int = 3650):
    """Регистрирует RTC функции комплектации планеров"""
    
    rtc_code = get_rtc_code_check_planer_demand(max_days)
    fn_demand = agent.newRTCFunction("rtc_planer_demand_check", rtc_code)
    fn_demand.setInitialState("serviceable")
    fn_demand.setEndState("serviceable")
    
    # Слой — в самом начале дня, до FIFO
    layer_demand = model.newLayer("layer_planer_demand")
    layer_demand.addAgentFunction(fn_demand)
    
    print("  RTC модуль planer_demand зарегистрирован (1 слой)")












