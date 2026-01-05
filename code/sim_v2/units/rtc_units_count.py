#!/usr/bin/env python3
"""
RTC модуль подсчёта агрегатов по группам и состояниям

Подсчитывает:
- units_pool_count[group_by] — агенты в reserve + serviceable (готовые к установке)
- units_ops_count[group_by] — агенты в operations (на планерах)
- units_repair_count[group_by] — агенты в repair

ВАЖНО: Используется atomicAdd, а не read+exchange, чтобы избежать race conditions.

Дата: 05.01.2026
"""

import pyflamegpu as fg

MAX_GROUPS = 50  # Увеличено, т.к. реально до 42 групп


def get_rtc_code() -> str:
    """Возвращает CUDA код для подсчёта агрегатов"""
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_count_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    if (group_by < {MAX_GROUPS}u) {{
        auto pool_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("units_pool_count");
        pool_count[group_by] += 1u;  // atomicAdd
    }}
    return flamegpu::ALIVE;
}}

FLAMEGPU_AGENT_FUNCTION(rtc_units_count_serviceable, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    if (group_by < {MAX_GROUPS}u) {{
        auto pool_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("units_pool_count");
        pool_count[group_by] += 1u;  // atomicAdd
    }}
    return flamegpu::ALIVE;
}}

FLAMEGPU_AGENT_FUNCTION(rtc_units_count_operations, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    if (group_by < {MAX_GROUPS}u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("units_ops_count");
        ops_count[group_by] += 1u;  // atomicAdd
    }}
    return flamegpu::ALIVE;
}}

FLAMEGPU_AGENT_FUNCTION(rtc_units_count_repair, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    if (group_by < {MAX_GROUPS}u) {{
        auto repair_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("units_repair_count");
        repair_count[group_by] += 1u;  // atomicAdd
    }}
    return flamegpu::ALIVE;
}}
"""


class ResetCountersHostFunction(fg.HostFunction):
    """Host function для сброса счётчиков в начале дня"""
    
    def run(self, FLAMEGPU):
        pool_count = FLAMEGPU.environment.getMacroPropertyUInt("units_pool_count")
        ops_count = FLAMEGPU.environment.getMacroPropertyUInt("units_ops_count")
        repair_count = FLAMEGPU.environment.getMacroPropertyUInt("units_repair_count")
        
        for i in range(MAX_GROUPS):
            pool_count[i] = 0
            ops_count[i] = 0
            repair_count[i] = 0


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции подсчёта агрегатов"""
    rtc_code = get_rtc_code()
    
    # Функции подсчёта
    fn_reserve = agent.newRTCFunction("rtc_units_count_reserve", rtc_code)
    fn_reserve.setInitialState("reserve")
    fn_reserve.setEndState("reserve")
    
    fn_serviceable = agent.newRTCFunction("rtc_units_count_serviceable", rtc_code)
    fn_serviceable.setInitialState("serviceable")
    fn_serviceable.setEndState("serviceable")
    
    fn_operations = agent.newRTCFunction("rtc_units_count_operations", rtc_code)
    fn_operations.setInitialState("operations")
    fn_operations.setEndState("operations")
    
    fn_repair = agent.newRTCFunction("rtc_units_count_repair", rtc_code)
    fn_repair.setInitialState("repair")
    fn_repair.setEndState("repair")
    
    # Host function для сброса
    hf_reset = ResetCountersHostFunction()
    
    # Слои
    layer_reset = model.newLayer("layer_units_reset")
    layer_reset.addHostFunction(hf_reset)
    
    layer_count = model.newLayer("layer_units_count")
    layer_count.addAgentFunction(fn_reserve)
    layer_count.addAgentFunction(fn_serviceable)
    layer_count.addAgentFunction(fn_operations)
    layer_count.addAgentFunction(fn_repair)
    
    print("  RTC модуль units_count зарегистрирован (2 слоя)")
