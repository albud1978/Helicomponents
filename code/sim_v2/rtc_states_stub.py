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
    
    // Читаем MP5 всегда, даже на шаге 0
    const unsigned int base = safe_day * {MAX_FRAMES}u + idx;
    const unsigned int base_next = base + {MAX_FRAMES}u;
    
    auto mp5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_SIZE}u>("mp5_lin");
    const unsigned int dt = mp5[base];
    const unsigned int dn = (safe_day < days_total - 1u ? mp5[base_next] : 0u);
    
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
    
    // Логика state_1: if (current_date - repair_time) > version_date then intent = 2 else intent = 1
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    const unsigned int version_date = FLAMEGPU->environment.getProperty<unsigned int>("version_date");
    
    if ((step_day - repair_time) > version_date) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
        
        // Не логируем, так как для state_1 intent=2 это нормально
    }} else {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 1u);
    }}
    // Логирование отключено для inactive
    
    return flamegpu::ALIVE;
}}
"""

# RTC функция для state_3 (serviceable)
RTC_STATE_3_SERVICEABLE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_3_serviceable, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Исправные агенты хотят в эксплуатацию
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    
    // На шаге 0 не выполняем переходы, но intent должен быть задан явно
    if (step_day == 0u) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
        return flamegpu::ALIVE;
    }}
    
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = (step_day < days_total ? step_day : (days_total > 0u ? days_total - 1u : 0u));
    
    const unsigned int base = safe_day * {MAX_FRAMES}u + idx;
    const unsigned int base_next = base + {MAX_FRAMES}u;
    
    auto mp5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_SIZE}u>("mp5_lin");
    const unsigned int dt = mp5[base];
    const unsigned int dn = (safe_day < days_total - 1u ? mp5[base_next] : 0u);
    
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
    
    // State_3: всегда хочет в эксплуатацию
    FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
    // Логирование отключено для serviceable
    
    return flamegpu::ALIVE;
}}
"""

# RTC функция для state_4 (repair)
RTC_STATE_4_REPAIR = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_4_repair, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Агенты в ремонте
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    
    // На шаге 0 не выполняем переходы, но intent должен быть задан явно
    if (step_day == 0u) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);
        return flamegpu::ALIVE;
    }}
    
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
    
    // Логирование прогресса ремонта отключено - логируем только переходы
    
    // Проверяем assembly_trigger
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    const unsigned int assembly_time = FLAMEGPU->getVariable<unsigned int>("assembly_time");
    if (repair_days >= repair_time - assembly_time) {{
        FLAMEGPU->setVariable<unsigned int>("assembly_trigger", 1u);
    }}
    
    // Проверяем завершение ремонта
    if (repair_days == repair_time) {{
        // Переход в резерв
        FLAMEGPU->setVariable<unsigned int>("intent_state", 5u);
        
        // Логирование перехода
        const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
        const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
        const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
        printf("  [Step %u] AC %u: intent=5 (reserve), repair complete rd=%u/%u, ll=%u, oh=%u, br=%u\\n", 
               step_day, aircraft_number, repair_days, repair_time, ll, oh, br);

        // Сбрасываем ppr и repair_days согласно требованию
        FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
        FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
    }} else {{
        // Остаемся в ремонте
        FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);
        // Логирование отключено для repair (продолжается)
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
    
    // На шаге 0 не выполняем переходы, но intent должен быть задан явно
    if (step_day == 0u) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
        return flamegpu::ALIVE;
    }}
    
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = (step_day < days_total ? step_day : (days_total > 0u ? days_total - 1u : 0u));
    
    const unsigned int base = safe_day * {MAX_FRAMES}u + idx;
    const unsigned int base_next = base + {MAX_FRAMES}u;
    
    auto mp5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_SIZE}u>("mp5_lin");
    const unsigned int dt = mp5[base];
    const unsigned int dn = (safe_day < days_total - 1u ? mp5[base_next] : 0u);
    
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
    
    // State_5: всегда хочет в эксплуатацию
    FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
    // Логирование отключено для reserve
    
    return flamegpu::ALIVE;
}}
"""

# RTC функция для state_6 (storage)
RTC_STATE_6_STORAGE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_6_storage, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Агенты в хранении
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int s6_started = FLAMEGPU->getVariable<unsigned int>("s6_started");
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    
    // На шаге 0 не выполняем переходы, но intent должен быть задан явно
    if (step_day == 0u) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);
        return flamegpu::ALIVE;
    }}
    
    // Обновляем MP5 данные
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = (step_day < days_total ? step_day : (days_total > 0u ? days_total - 1u : 0u));
    
    const unsigned int base = safe_day * {MAX_FRAMES}u + idx;
    const unsigned int base_next = base + {MAX_FRAMES}u;
    
    auto mp5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_SIZE}u>("mp5_lin");
    const unsigned int dt = mp5[base];
    const unsigned int dn = (safe_day < days_total - 1u ? mp5[base_next] : 0u);
    
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
    
    // State_6: остаемся в хранении
    FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);
    
    // Логирование отключено для storage
    
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции для всех состояний с setEndState"""
    
    # Все функции теперь с setEndState для корректной работы
    funcs = [
        ("rtc_state_1_inactive", RTC_STATE_1_INACTIVE, "inactive", "inactive"),  # Остаётся
        ("rtc_state_3_serviceable", RTC_STATE_3_SERVICEABLE, "serviceable", "serviceable"),  # Остаётся  
        ("rtc_state_4_repair", RTC_STATE_4_REPAIR, "repair", None),  # Условные переходы внутри
        ("rtc_state_5_reserve", RTC_STATE_5_RESERVE, "reserve", "reserve"),  # Остаётся
        ("rtc_state_6_storage", RTC_STATE_6_STORAGE, "storage", "storage")  # Остаётся
    ]
    
    for func_name, func_code, state_name, end_state in funcs:
        try:
            # Создаем RTC функцию
            rtc_func = agent.newRTCFunction(func_name, func_code)
            
            # Устанавливаем состояние для функции
            rtc_func.setInitialState(state_name)
            # ВАЖНО: всегда устанавливаем setEndState, иначе агенты уйдут в inactive
            if not end_state:
                end_state = state_name  # Остаются в том же состоянии
            rtc_func.setEndState(end_state)
            
            # Создаем слой для функции
            layer = model.newLayer()
            
            # Добавляем функцию в слой
            layer.addAgentFunction(rtc_func)
            
            print(f"  RTC функция {func_name} зарегистрирована для состояния {state_name}")
            
        except Exception as e:
            print(f"  Ошибка регистрации {func_name}: {e}")
    
    print("  RTC модуль states_stub зарегистрирован")
