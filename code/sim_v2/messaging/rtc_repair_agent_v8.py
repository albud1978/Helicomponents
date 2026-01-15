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
# RTC: Инкремент capacity
# ═══════════════════════════════════════════════════════════════════════════════

RTC_REPAIR_INCREMENT_CAPACITY = """
FLAMEGPU_AGENT_FUNCTION(rtc_repair_increment_capacity_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V8: Инкремент capacity на (repair_quota - count_repair)
    // count_repair передаётся через Environment (подсчитан ранее)
    
    const unsigned int capacity = FLAMEGPU->getVariable<unsigned int>("capacity");
    const unsigned int repair_quota = FLAMEGPU->environment.getProperty<unsigned int>("repair_quota");
    const unsigned int count_repair = FLAMEGPU->environment.getProperty<unsigned int>("count_repair");
    
    // capacity += (repair_quota - count_repair)
    // Если count_repair > repair_quota, инкремент = 0
    unsigned int increment = 0u;
    if (repair_quota > count_repair) {
        increment = repair_quota - count_repair;
    }
    
    const unsigned int new_capacity = capacity + increment;
    FLAMEGPU->setVariable<unsigned int>("capacity", new_capacity);
    
    // Вычисляем slots = floor(capacity / repair_time)
    const unsigned int repair_time = FLAMEGPU->environment.getProperty<unsigned int>("repair_time_const");
    const unsigned int slots = (repair_time > 0u) ? (new_capacity / repair_time) : 0u;
    FLAMEGPU->setVariable<unsigned int>("slots", slots);
    
    // DEBUG
    const unsigned int step = FLAMEGPU->getStepCounter();
    if (step % 50u == 0u || step < 5u) {
        printf("[RepairAgent] step=%u, capacity=%u (+%u), slots=%u\\n",
               step, new_capacity, increment, slots);
    }
    
    return flamegpu::ALIVE;
}
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
    
    print("  ✅ V8 MacroProperty для RepairAgent: capacity_mp, slots_mp, to_deduct_mp")


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


# RTC: RepairAgent читает to_deduct из MacroProperty
RTC_REPAIR_RECEIVE_DEDUCT = """
FLAMEGPU_AGENT_FUNCTION(rtc_repair_receive_deduct_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V8: RepairAgent читает to_deduct от QuotaManager
    
    auto mp_deduct = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("repair_to_deduct_mp");
    const unsigned int to_deduct = mp_deduct[0];
    
    FLAMEGPU->setVariable<unsigned int>("to_deduct", to_deduct);
    
    // Сброс MacroProperty
    mp_deduct[0].exchange(0u);
    
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# Регистрация слоёв RepairAgent
# ═══════════════════════════════════════════════════════════════════════════════

def register_repair_agent_layers(model, repair_agent):
    """
    Регистрирует слои RepairAgent.
    
    Слои:
    1. v8_repair_increment — инкремент capacity
    2. v8_repair_send — отправка capacity/slots в MacroProperty
    (между ними — QuotaManager принимает решение)
    3. v8_repair_receive — получение to_deduct
    4. v8_repair_deduct — списание capacity
    """
    print("\n📦 V8: Регистрация RepairAgent слоёв...")
    
    # 1. Инкремент capacity
    layer_inc = model.newLayer("v8_repair_increment")
    fn = repair_agent.newRTCFunction("rtc_repair_increment_capacity_v8", RTC_REPAIR_INCREMENT_CAPACITY)
    fn.setInitialState("default")
    fn.setEndState("default")
    layer_inc.addAgentFunction(fn)
    
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

