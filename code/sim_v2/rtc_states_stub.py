#!/usr/bin/env python3
"""
RTC модуль-заглушка для остальных состояний (временный для тестирования)
Обрабатывает state_1, state_3, state_4, state_5, state_6
"""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from model_build import MAX_FRAMES, MAX_DAYS, MAX_SIZE

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


# RTC функция для state_1 (inactive)
RTC_STATE_1_INACTIVE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_1_inactive, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Неактивные агенты ничего не делают
    // Но обновляем MP5 данные для консистентности
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = (step_day < days_total ? step_day : (days_total > 0u ? days_total - 1u : 0u));
    
    const unsigned int base = safe_day * {MAX_FRAMES}u + idx;
    const unsigned int base_next = base + {MAX_FRAMES}u;
    
    auto mp5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_SIZE}u>("mp5_lin");
    const unsigned int dt = mp5[base];
    const unsigned int dn = (safe_day < days_total - 1u ? mp5[base_next] : 0u);
    
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
    
    // Нет intent (остаемся неактивными)
    FLAMEGPU->setVariable<unsigned int>("intent_state", 0u);
    return flamegpu::ALIVE;
}}
"""

# RTC функция для state_3 (serviceable)
RTC_STATE_3_SERVICEABLE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_3_serviceable, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Исправные агенты хотят в эксплуатацию
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = (step_day < days_total ? step_day : (days_total > 0u ? days_total - 1u : 0u));
    
    const unsigned int base = safe_day * {MAX_FRAMES}u + idx;
    const unsigned int base_next = base + {MAX_FRAMES}u;
    
    auto mp5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_SIZE}u>("mp5_lin");
    const unsigned int dt = mp5[base];
    const unsigned int dn = (safe_day < days_total - 1u ? mp5[base_next] : 0u);
    
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
    
    // Намерение перейти в эксплуатацию
    FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
    return flamegpu::ALIVE;
}}
"""

# RTC функция для state_4 (repair)
RTC_STATE_4_REPAIR = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_4_repair, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Агенты в ремонте
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = (step_day < days_total ? step_day : (days_total > 0u ? days_total - 1u : 0u));
    
    const unsigned int base = safe_day * {MAX_FRAMES}u + idx;
    const unsigned int base_next = base + {MAX_FRAMES}u;
    
    auto mp5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_SIZE}u>("mp5_lin");
    const unsigned int dt = mp5[base];
    const unsigned int dn = (safe_day < days_total - 1u ? mp5[base_next] : 0u);
    
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
    
    // Увеличиваем счетчик дней ремонта
    unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    repair_days++;
    FLAMEGPU->setVariable<unsigned int>("repair_days", repair_days);
    
    // Проверяем завершение ремонта
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    if (repair_days >= repair_time) {{
        // Детерминированный переход 4->5
        FLAMEGPU->setVariable<unsigned int>("ppr", 0u); // Сброс PPR
        FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
        FLAMEGPU->setVariable<unsigned int>("intent_state", 5u); // Намерение перейти в резерв
    }} else {{
        // Остаемся в ремонте
        FLAMEGPU->setVariable<unsigned int>("intent_state", 0u);
    }}
    
    return flamegpu::ALIVE;
}}
"""

# RTC функция для state_5 (reserve)
RTC_STATE_5_RESERVE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_5_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Агенты в резерве хотят в эксплуатацию
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = (step_day < days_total ? step_day : (days_total > 0u ? days_total - 1u : 0u));
    
    const unsigned int base = safe_day * {MAX_FRAMES}u + idx;
    const unsigned int base_next = base + {MAX_FRAMES}u;
    
    auto mp5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_SIZE}u>("mp5_lin");
    const unsigned int dt = mp5[base];
    const unsigned int dn = (safe_day < days_total - 1u ? mp5[base_next] : 0u);
    
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
    
    // Намерение перейти в эксплуатацию
    FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
    return flamegpu::ALIVE;
}}
"""

# RTC функция для state_6 (storage)
RTC_STATE_6_STORAGE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_6_storage, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Агенты в хранении
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int s6_started = FLAMEGPU->getVariable<unsigned int>("s6_started");
    
    // Обновляем MP5 данные
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = (step_day < days_total ? step_day : (days_total > 0u ? days_total - 1u : 0u));
    
    const unsigned int base = safe_day * {MAX_FRAMES}u + idx;
    const unsigned int base_next = base + {MAX_FRAMES}u;
    
    auto mp5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_SIZE}u>("mp5_lin");
    const unsigned int dt = mp5[base];
    const unsigned int dn = (safe_day < days_total - 1u ? mp5[base_next] : 0u);
    
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
    
    // Если агент изначально был в статусе 6, ничего не делаем
    if (s6_started == 0u) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 0u);
        return flamegpu::ALIVE;
    }}
    
    // Для перешедших в статус 6 - увеличиваем счетчик дней
    unsigned int s6_days = FLAMEGPU->getVariable<unsigned int>("s6_days");
    const unsigned int partout_time = FLAMEGPU->getVariable<unsigned int>("partout_time");
    
    if (s6_days < partout_time) {{
        s6_days++;
        FLAMEGPU->setVariable<unsigned int>("s6_days", s6_days);
        
        if (s6_days >= partout_time) {{
            FLAMEGPU->setVariable<unsigned int>("partout_trigger", 1u);
        }}
    }}
    
    // Остаемся в хранении
    FLAMEGPU->setVariable<unsigned int>("intent_state", 0u);
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции для всех состояний"""
    
    # Создаем RTC функции
    funcs = [
        ("rtc_state_1_inactive", RTC_STATE_1_INACTIVE, "inactive"),
        ("rtc_state_3_serviceable", RTC_STATE_3_SERVICEABLE, "serviceable"),
        ("rtc_state_4_repair", RTC_STATE_4_REPAIR, "repair"),
        ("rtc_state_5_reserve", RTC_STATE_5_RESERVE, "reserve"),
        ("rtc_state_6_storage", RTC_STATE_6_STORAGE, "storage")
    ]
    
    for func_name, func_code, state_name in funcs:
        try:
            # Создаем RTC функцию
            rtc_func = agent.newRTCFunction(func_name, func_code)
            
            # Устанавливаем состояние для функции
            rtc_func.setInitialState(state_name)
            rtc_func.setEndState(state_name)  # Не меняем состояние
            
            # Создаем слой для функции
            layer = model.newLayer()
            
            # Добавляем функцию в слой
            layer.addAgentFunction(rtc_func)
            
            print(f"  RTC функция {func_name} зарегистрирована для состояния {state_name}")
            
        except Exception as e:
            print(f"  Ошибка регистрации {func_name}: {e}")
    
    print("  RTC модуль states_stub зарегистрирован")
