#!/usr/bin/env python3
"""
RTC модуль: RepairAgent V8 — Агент ремонтной мощности

АРХИТЕКТУРА V8:
RepairAgent управляет квотой ремонта через счётчик агрегато-дней (capacity).
Это заменяет exit_date для unserviceable агентов.

Переменные RepairAgent:
- capacity (UInt32): Накопленные агрегато-дни для ремонта
- repair_quota (UInt16): Дневная квота (слотов)
- repair_time (UInt16): Время ремонта одного агента (дней)

Протокол обмена сообщениями (внутри одного шага):
1. RepairAgent → QuotaManager: { capacity, slots }
2. QuotaManager принимает решение (P2/P3)
3. QuotaManager → RepairAgent: { to_deduct }
4. RepairAgent списывает capacity

См. docs/adaptive_steps_logic.md для полной архитектуры.

Дата: 16.01.2026
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from model_build import RTC_MAX_FRAMES

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# Определение RepairAgent
# ═══════════════════════════════════════════════════════════════════════════════

def create_repair_agent(model, env, repair_quota: int = 8, repair_time: int = 180):
    """
    Создаёт агента RepairAgent для управления ремонтной мощностью.
    
    Args:
        model: ModelDescription
        env: EnvironmentDescription
        repair_quota: Дневная квота ремонтных слотов (default=8)
        repair_time: Время ремонта одного агента (default=180 дней)
    
    Returns:
        AgentDescription
    """
    print(f"\n📦 V8: Создание RepairAgent (quota={repair_quota}, time={repair_time})...")
    
    repair_agent = model.newAgent("RepairAgent")
    repair_agent.newState("default")
    repair_agent.setInitialState("default")
    
    # Переменные
    repair_agent.newVariableUInt32("capacity")       # Накопленные агрегато-дни
    repair_agent.newVariableUInt16("repair_quota")   # Дневная квота
    repair_agent.newVariableUInt16("repair_time")    # Время ремонта
    repair_agent.newVariableUInt32("to_deduct")      # Сколько списать (от QM)
    repair_agent.newVariableUInt32("slots")          # Доступные слоты
    
    # Environment properties для доступа из RTC
    try:
        env.newPropertyUInt("repair_quota", repair_quota)
    except:
        env.setPropertyUInt("repair_quota", repair_quota)
    
    try:
        env.newPropertyUInt("repair_time_const", repair_time)
    except:
        env.setPropertyUInt("repair_time_const", repair_time)
    
    print(f"  ✅ RepairAgent создан")
    
    return repair_agent


# ═══════════════════════════════════════════════════════════════════════════════
# RTC: Подсчёт HELI в repair (записывает 1 в буфер)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_COUNT_REPAIR = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_count_repair_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    auto mp_repair = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("count_repair_buffer");
    mp_repair[idx].exchange(1u);
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# RTC: Инкремент capacity (подсчитывает repair из буфера)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_REPAIR_INCREMENT_CAPACITY = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_repair_increment_capacity_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // V8: Инкремент capacity на (repair_quota - count_repair)
    // count_repair подсчитывается из count_repair_buffer
    
    const unsigned int capacity = FLAMEGPU->getVariable<unsigned int>("capacity");
    const unsigned int repair_quota = FLAMEGPU->environment.getProperty<unsigned int>("repair_quota");
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    // Подсчёт агентов в repair из буфера (БЕЗ сброса — сброс в отдельном слое)
    auto mp_repair = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("count_repair_buffer");
    unsigned int count_repair = 0u;
    for (unsigned int i = 0u; i < frames && i < {RTC_MAX_FRAMES}u; ++i) {{
        count_repair += mp_repair[i];
    }}
    
    // capacity += (repair_quota - count_repair)
    // Если count_repair > repair_quota, инкремент = 0
    unsigned int increment = 0u;
    if (repair_quota > count_repair) {{
        increment = repair_quota - count_repair;
    }}
    
    const unsigned int new_capacity = capacity + increment;
    FLAMEGPU->setVariable<unsigned int>("capacity", new_capacity);
    
    // Вычисляем slots = floor(capacity / repair_time)
    const unsigned int repair_time = FLAMEGPU->environment.getProperty<unsigned int>("repair_time_const");
    const unsigned int slots = (repair_time > 0u) ? (new_capacity / repair_time) : 0u;
    FLAMEGPU->setVariable<unsigned int>("slots", slots);
    
    // DEBUG
    const unsigned int step = FLAMEGPU->getStepCounter();
    if (step % 50u == 0u || step < 5u) {{
        printf("[RepairAgent] step=%u, capacity=%u (+%u), slots=%u\\n",
               step, new_capacity, increment, slots);
    }}
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# RTC: Сброс буфера count_repair_buffer (отдельный слой после increment)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_RESET_COUNT_REPAIR_BUFFER = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_reset_count_repair_buffer_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // V8: Сброс буфера count_repair_buffer для следующего шага
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    auto mp_repair = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("count_repair_buffer");
    for (unsigned int i = 0u; i < frames && i < {RTC_MAX_FRAMES}u; ++i) {{
        mp_repair[i].exchange(0u);
    }}
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# RTC: Списание capacity (после решения QuotaManager)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_REPAIR_DEDUCT_CAPACITY = """
FLAMEGPU_AGENT_FUNCTION(rtc_repair_deduct_capacity_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V8: Списание capacity после одобрения QuotaManager
    // to_deduct устанавливается QuotaManager через MacroProperty
    
    const unsigned int capacity = FLAMEGPU->getVariable<unsigned int>("capacity");
    const unsigned int to_deduct = FLAMEGPU->getVariable<unsigned int>("to_deduct");
    
    if (to_deduct > 0u && to_deduct <= capacity) {
        const unsigned int new_capacity = capacity - to_deduct;
        FLAMEGPU->setVariable<unsigned int>("capacity", new_capacity);
        
        // Пересчёт slots
        const unsigned int repair_time = FLAMEGPU->environment.getProperty<unsigned int>("repair_time_const");
        const unsigned int slots = (repair_time > 0u) ? (new_capacity / repair_time) : 0u;
        FLAMEGPU->setVariable<unsigned int>("slots", slots);
        
        // DEBUG
        const unsigned int step = FLAMEGPU->getStepCounter();
        printf("[RepairAgent] step=%u, DEDUCT %u, capacity=%u -> %u\\n",
               step, to_deduct, capacity, new_capacity);
    }
    
    // Сброс to_deduct
    FLAMEGPU->setVariable<unsigned int>("to_deduct", 0u);
    
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# MacroProperty для передачи данных RepairAgent ↔ QuotaManager
# ═══════════════════════════════════════════════════════════════════════════════

def setup_repair_agent_macroproperties(env):
    """
    Настраивает MacroProperty для обмена данными RepairAgent ↔ QuotaManager.
    
    Используется MacroProperty вместо сообщений для простоты:
    - repair_capacity_mp[0] = текущая capacity
    - repair_slots_mp[0] = текущие slots
    - repair_to_deduct_mp[0] = сколько списать
    """
    
    # RepairAgent → QuotaManager
    env.newMacroPropertyUInt("repair_capacity_mp", 4)
    env.newMacroPropertyUInt("repair_slots_mp", 4)
    
    # QuotaManager → RepairAgent
    env.newMacroPropertyUInt("repair_to_deduct_mp", 4)
    
    # Буфер для подсчёта агентов в repair (HELI записывает 1, RepairAgent суммирует)
    env.newMacroPropertyUInt("count_repair_buffer", RTC_MAX_FRAMES)
    
    print("  ✅ V8 MacroProperty для RepairAgent: capacity_mp, slots_mp, to_deduct_mp, count_repair_buffer")


# RTC: RepairAgent отправляет capacity в MacroProperty
RTC_REPAIR_SEND_CAPACITY = """
FLAMEGPU_AGENT_FUNCTION(rtc_repair_send_capacity_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V8: RepairAgent записывает capacity/slots в MacroProperty для QuotaManager
    
    const unsigned int capacity = FLAMEGPU->getVariable<unsigned int>("capacity");
    const unsigned int slots = FLAMEGPU->getVariable<unsigned int>("slots");
    
    auto mp_cap = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("repair_capacity_mp");
    auto mp_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("repair_slots_mp");
    
    mp_cap[0].exchange(capacity);
    mp_slots[0].exchange(slots);
    
    return flamegpu::ALIVE;
}
"""


# RTC: RepairAgent подсчитывает одобренных из буферов P2/P3
RTC_REPAIR_RECEIVE_DEDUCT = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_repair_receive_deduct_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // V8: RepairAgent подсчитывает одобренных P2/P3 из буферов
    
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    const unsigned int repair_time = FLAMEGPU->environment.getProperty<unsigned int>("repair_time_const");
    
    auto mp_p2 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("repair_p2_approved");
    auto mp_p3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("repair_p3_approved");
    
    // Подсчёт одобренных
    unsigned int p2_count = 0u;
    unsigned int p3_count = 0u;
    
    for (unsigned int i = 0u; i < frames && i < {RTC_MAX_FRAMES}u; ++i) {{
        p2_count += mp_p2[i];
        p3_count += mp_p3[i];
        // БЕЗ сброса — сброс в отдельном слое rtc_quota_v8.py
    }}
    
    const unsigned int approved_total = p2_count + p3_count;
    const unsigned int to_deduct = approved_total * repair_time;
    
    FLAMEGPU->setVariable<unsigned int>("to_deduct", to_deduct);
    
    // DEBUG
    const unsigned int step = FLAMEGPU->getStepCounter();
    if (to_deduct > 0u || step < 5u) {{
        printf("[RepairAgent] step=%u, P2=%u, P3=%u, to_deduct=%u\\n",
               step, p2_count, p3_count, to_deduct);
    }}
    
    return flamegpu::ALIVE;
}}
"""


# RTC: Сброс буферов P2/P3 (отдельный слой после receive)
RTC_RESET_P2P3_BUFFERS = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_reset_p2p3_buffers_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    auto mp_p2 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("repair_p2_approved");
    auto mp_p3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("repair_p3_approved");
    
    for (unsigned int i = 0u; i < frames && i < {RTC_MAX_FRAMES}u; ++i) {{
        mp_p2[i].exchange(0u);
        mp_p3[i].exchange(0u);
    }}
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# Регистрация слоёв RepairAgent
# ═══════════════════════════════════════════════════════════════════════════════

def register_repair_agent_layers(model, repair_agent, heli_agent=None):
    """
    Регистрирует слои RepairAgent.
    
    Слои:
    0. v8_count_repair — HELI в repair записывает 1 в буфер (требует heli_agent)
    1. v8_repair_increment — инкремент capacity (подсчитывает из буфера)
    2. v8_repair_send — отправка capacity/slots в MacroProperty
    (между ними — QuotaManager принимает решение)
    3. v8_repair_receive — получение to_deduct
    4. v8_repair_deduct — списание capacity
    """
    print("\n📦 V8: Регистрация RepairAgent слоёв...")
    
    # 0. HELI в repair записывает 1 в буфер для подсчёта
    if heli_agent:
        layer_count = model.newLayer("v8_count_repair")
        fn = heli_agent.newRTCFunction("rtc_count_repair_v8", RTC_COUNT_REPAIR)
        fn.setInitialState("repair")
        fn.setEndState("repair")
        layer_count.addAgentFunction(fn)
    
    # 1. Инкремент capacity (подсчитывает repair из буфера)
    layer_inc = model.newLayer("v8_repair_increment")
    fn = repair_agent.newRTCFunction("rtc_repair_increment_capacity_v8", RTC_REPAIR_INCREMENT_CAPACITY)
    fn.setInitialState("default")
    fn.setEndState("default")
    layer_inc.addAgentFunction(fn)
    
    # 1.5. Сброс буфера count_repair_buffer (отдельный слой для избежания race condition)
    layer_reset = model.newLayer("v8_reset_count_repair")
    fn = repair_agent.newRTCFunction("rtc_reset_count_repair_buffer_v8", RTC_RESET_COUNT_REPAIR_BUFFER)
    fn.setInitialState("default")
    fn.setEndState("default")
    layer_reset.addAgentFunction(fn)
    
    # 2. Отправка в MacroProperty
    layer_send = model.newLayer("v8_repair_send")
    fn = repair_agent.newRTCFunction("rtc_repair_send_capacity_v8", RTC_REPAIR_SEND_CAPACITY)
    fn.setInitialState("default")
    fn.setEndState("default")
    layer_send.addAgentFunction(fn)
    
    print("  ✅ RepairAgent слои: increment + send")
    
    return layer_inc, layer_send


def register_repair_agent_post_quota_layers(model, repair_agent):
    """
    Регистрирует слои RepairAgent ПОСЛЕ QuotaManager.
    
    Слои:
    3. v8_repair_receive — получение to_deduct
    4. v8_repair_deduct — списание capacity
    """
    print("  📦 V8: RepairAgent post-quota слои...")
    
    # 3. Получение to_deduct
    layer_recv = model.newLayer("v8_repair_receive")
    fn = repair_agent.newRTCFunction("rtc_repair_receive_deduct_v8", RTC_REPAIR_RECEIVE_DEDUCT)
    fn.setInitialState("default")
    fn.setEndState("default")
    layer_recv.addAgentFunction(fn)
    
    # 3.5. Сброс буферов P2/P3 (отдельный слой для избежания race condition)
    layer_reset = model.newLayer("v8_reset_p2p3")
    fn = repair_agent.newRTCFunction("rtc_reset_p2p3_buffers_v8", RTC_RESET_P2P3_BUFFERS)
    fn.setInitialState("default")
    fn.setEndState("default")
    layer_reset.addAgentFunction(fn)
    
    # 4. Списание
    layer_deduct = model.newLayer("v8_repair_deduct")
    fn = repair_agent.newRTCFunction("rtc_repair_deduct_capacity_v8", RTC_REPAIR_DEDUCT_CAPACITY)
    fn.setInitialState("default")
    fn.setEndState("default")
    layer_deduct.addAgentFunction(fn)
    
    print("  ✅ RepairAgent post-quota: receive + deduct")
    
    return layer_recv, layer_deduct


# ═══════════════════════════════════════════════════════════════════════════════
# Инициализация RepairAgent популяции
# ═══════════════════════════════════════════════════════════════════════════════

def init_repair_agent_population(simulation, repair_agent, repair_quota: int, repair_time: int, count_repair: int):
    """
    Инициализирует популяцию RepairAgent (1 агент).
    
    Args:
        simulation: CUDASimulation
        repair_agent: AgentDescription
        repair_quota: Дневная квота
        repair_time: Время ремонта
        count_repair: Количество агентов в repair на старте
    """
    import pyflamegpu as fg
    
    # Начальная capacity = repair_quota - count_repair
    initial_capacity = max(0, repair_quota - count_repair)
    initial_slots = initial_capacity // repair_time if repair_time > 0 else 0
    
    pop = fg.AgentVector(repair_agent, 1)
    pop[0].setVariableUInt32("capacity", initial_capacity)
    pop[0].setVariableUInt16("repair_quota", repair_quota)
    pop[0].setVariableUInt16("repair_time", repair_time)
    pop[0].setVariableUInt32("to_deduct", 0)
    pop[0].setVariableUInt32("slots", initial_slots)
    
    simulation.setPopulationData(pop, "default")
    
    print(f"  ✅ RepairAgent инициализирован: capacity={initial_capacity}, slots={initial_slots}")

