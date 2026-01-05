#!/usr/bin/env python3
"""
RTC модуль запроса замены агрегата

Неисправный агрегат (ppr >= oh или sne >= ll) пишет запрос на замену
в MacroProperty mp_replacement_request

Дата: 05.01.2026
"""

import pyflamegpu as fg

UNITS_MAX_FRAMES = 12000  # Константа для агрегатов


def get_rtc_code(max_frames: int) -> str:
    """Возвращает CUDA код для запроса замены"""
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_request_replacement, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Условия замены:
    // 1. ppr >= oh (межремонтный ресурс исчерпан) — отправляем в ремонт
    // 2. sne >= ll (назначенный ресурс исчерпан) — списание
    // 3. sne >= br (breakeven) — дешевле списать
    
    bool needs_repair = (oh > 0u && ppr >= oh);
    bool needs_storage = (ll > 0u && sne >= ll) || (br > 0u && sne >= br);
    
    if (needs_repair || needs_storage) {{
        // Только если агрегат на планере (aircraft_number > 0)
        if (aircraft_number > 0u && idx < {max_frames}u) {{
            // Записываем запрос на замену
            auto requests = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_request");
            auto groups = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mp_replacement_group");
            
            requests[idx].exchange(aircraft_number);  // Для какого планера нужна замена
            groups[idx].exchange(group_by);           // Какой тип агрегата нужен
            
            // Устанавливаем intent
            if (needs_storage) {{
                FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);  // storage
            }} else {{
                FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);  // repair
            }}
        }}
    }}
    
    return flamegpu::ALIVE;
}}
"""


class ResetRequestsHostFunction(fg.HostFunction):
    """Host function для сброса запросов в начале дня"""
    
    def __init__(self, max_frames: int):
        super().__init__()
        self.max_frames = max_frames
    
    def run(self, FLAMEGPU):
        requests = FLAMEGPU.environment.getMacroPropertyUInt("mp_replacement_request")
        groups = FLAMEGPU.environment.getMacroPropertyUInt("mp_replacement_group")
        
        # Используем min чтобы не выйти за границы
        actual_size = min(self.max_frames, len(requests))
        for i in range(actual_size):
            requests[i] = 0
            groups[i] = 0


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription, max_frames: int = UNITS_MAX_FRAMES):
    """Регистрирует RTC функции запроса замены"""
    rtc_code = get_rtc_code(max_frames)
    
    # Функция проверки ресурса для агентов в operations
    fn_request = agent.newRTCFunction("rtc_units_request_replacement", rtc_code)
    fn_request.setInitialState("operations")
    fn_request.setEndState("operations")
    
    # Host function для сброса
    hf_reset = ResetRequestsHostFunction(max_frames)
    
    # Слои
    layer_reset = model.newLayer("layer_units_reset_requests")
    layer_reset.addHostFunction(hf_reset)
    
    layer_request = model.newLayer("layer_units_request_replacement")
    layer_request.addAgentFunction(fn_request)
    
    print("  RTC модуль units_request_replacement зарегистрирован (2 слоя)")
