#!/usr/bin/env python3
"""
RTC модуль V6: Состояния агентов (упрощённый)

V6 АРХИТЕКТУРА (12.01.2026):
- repair (4) и reserve (5) обрабатываются через rtc_deterministic_exit
- repair_days/repair_time больше НЕ инкрементируются здесь
- Этот модуль только устанавливает базовый intent_state

Состояния:
- serviceable (3): intent=3, ждёт промоут P1
- repair (4): intent=4, ждёт exit_date (rtc_deterministic_exit)
- reserve (5): intent=5, ждёт exit_date (spawn)
- storage (6): intent=6, неизменяемое
- unserviceable (7): intent=7, ждёт промоут P2
"""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from model_build import RTC_MAX_FRAMES, MAX_DAYS, MAX_SIZE

MAX_FRAMES = RTC_MAX_FRAMES

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


# RTC функция для state_3 (serviceable)
RTC_STATE_3_SERVICEABLE = """
FLAMEGPU_AGENT_FUNCTION(rtc_state_3_serviceable_v2, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Serviceable: исправные в холдинге, ждут промоут P1
    FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    return flamegpu::ALIVE;
}
"""

# RTC функция для state_4 (repair) — V6: упрощённая
# Логика exit_date обрабатывается в rtc_deterministic_exit
RTC_STATE_4_REPAIR = """
FLAMEGPU_AGENT_FUNCTION(rtc_state_4_repair_v6, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V6: Repair — ждём детерминированного выхода через exit_date
    // ❌ НЕТ инкремента repair_days — это делает rtc_deterministic_exit
    // Просто остаёмся в repair с intent=4
    FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    return flamegpu::ALIVE;
}
"""

# RTC функция для state_5 (reserve) — V6: только spawn
RTC_STATE_5_RESERVE = """
FLAMEGPU_AGENT_FUNCTION(rtc_state_5_reserve_v6, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V6: Reserve ТОЛЬКО для spawn — ждём exit_date
    // Логика spawn обрабатывается в rtc_deterministic_exit
    FLAMEGPU->setVariable<unsigned int>("intent_state", 5u);
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    return flamegpu::ALIVE;
}
"""

# RTC функция для state_6 (storage)
RTC_STATE_6_STORAGE = """
FLAMEGPU_AGENT_FUNCTION(rtc_state_6_storage_v6, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Storage: списанные агенты, неизменяемое состояние
    FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    return flamegpu::ALIVE;
}
"""

# RTC функция для state_7 (unserviceable) — V6: НОВЫЙ
RTC_STATE_7_UNSERVICEABLE = """
FLAMEGPU_AGENT_FUNCTION(rtc_state_7_unserviceable_v6, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V6: Unserviceable — после достижения OH, ждёт промоут P2
    // Квотирование может изменить intent на 2 (operations)
    FLAMEGPU->setVariable<unsigned int>("intent_state", 7u);
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    return flamegpu::ALIVE;
}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции V6 для состояний"""
    
    funcs = [
        ("rtc_state_3_serviceable_v2", RTC_STATE_3_SERVICEABLE, "serviceable", "serviceable"),
        ("rtc_state_4_repair_v6", RTC_STATE_4_REPAIR, "repair", "repair"),
        ("rtc_state_5_reserve_v6", RTC_STATE_5_RESERVE, "reserve", "reserve"),
        ("rtc_state_6_storage_v6", RTC_STATE_6_STORAGE, "storage", "storage"),
        ("rtc_state_7_unserviceable_v6", RTC_STATE_7_UNSERVICEABLE, "unserviceable", "unserviceable")
    ]
    
    for func_name, func_code, state_name, end_state in funcs:
        try:
            rtc_func = agent.newRTCFunction(func_name, func_code)
            rtc_func.setInitialState(state_name)
            rtc_func.setEndState(end_state)
            
            layer = model.newLayer()
            layer.addAgentFunction(rtc_func)
            
            print(f"  V6: RTC функция {func_name} → {state_name}")
            
        except Exception as e:
            print(f"  Ошибка регистрации {func_name}: {e}")
    
    print("  ✅ RTC модуль states_stub_v2 (V6) зарегистрирован")
