#!/usr/bin/env python3
"""
Отладка rtc_status_6 - пошаговое упрощение до рабочего состояния
Дата: 2025-09-12
"""

import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent / 'utils'))
from config_loader import get_clickhouse_client

try:
    import pyflamegpu
except ImportError:
    pyflamegpu = None


def test_minimal_status_6():
    """Тест минимальной версии rtc_status_6"""
    
    if pyflamegpu is None:
        print("❌ pyflamegpu не установлен")
        return False
    
    print("🧪 Тест минимальной rtc_status_6")
    
    # Версия 1: Абсолютный минимум
    rtc_minimal = """
FLAMEGPU_AGENT_FUNCTION(rtc_status_6_minimal, flamegpu::MessageNone, flamegpu::MessageNone) {
    if (FLAMEGPU->getVariable<unsigned int>("status_id") != 6u) return flamegpu::ALIVE;
    return flamegpu::ALIVE;
}
    """
    
    try:
        model = pyflamegpu.ModelDescription("Status6Debug")
        agent = model.newAgent("component")
        
        # Минимальные переменные
        agent.newVariableUInt("status_id", 0)
        
        # Добавляем RTC
        agent.newRTCFunction("rtc_status_6_minimal", rtc_minimal)
        
        # Слой
        layer = model.newLayer()
        layer.addAgentFunction(agent.getFunction("rtc_status_6_minimal"))
        
        # Симуляция
        sim = pyflamegpu.CUDASimulation(model)
        
        print("✅ Минимальная версия компилируется")
        return True
        
    except Exception as e:
        print(f"❌ Даже минимальная версия не компилируется: {e}")
        return False


def test_with_export_phase():
    """Тест с добавлением export_phase"""
    
    rtc_with_phase = """
FLAMEGPU_AGENT_FUNCTION(rtc_status_6_phase, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int phase = FLAMEGPU->environment.getProperty<unsigned int>("export_phase");
    if (phase != 0u) return flamegpu::ALIVE;
    if (FLAMEGPU->getVariable<unsigned int>("status_id") != 6u) return flamegpu::ALIVE;
    return flamegpu::ALIVE;
}
    """
    
    try:
        model = pyflamegpu.ModelDescription("Status6PhaseDebug")
        env = model.Environment()
        env.newPropertyUInt("export_phase", 0)
        
        agent = model.newAgent("component")
        agent.newVariableUInt("status_id", 0)
        
        agent.newRTCFunction("rtc_status_6_phase", rtc_with_phase)
        layer = model.newLayer()
        layer.addAgentFunction(agent.getFunction("rtc_status_6_phase"))
        
        sim = pyflamegpu.CUDASimulation(model)
        sim.setEnvironmentPropertyUInt("export_phase", 0)
        
        print("✅ Версия с export_phase компилируется")
        return True
        
    except Exception as e:
        print(f"❌ Версия с export_phase не компилируется: {e}")
        return False


def test_with_s6_variables():
    """Тест с добавлением s6_started и s6_days"""
    
    rtc_with_s6 = """
FLAMEGPU_AGENT_FUNCTION(rtc_status_6_s6vars, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int phase = FLAMEGPU->environment.getProperty<unsigned int>("export_phase");
    if (phase != 0u) return flamegpu::ALIVE;
    if (FLAMEGPU->getVariable<unsigned int>("status_id") != 6u) return flamegpu::ALIVE;
    
    if (FLAMEGPU->getVariable<unsigned int>("s6_started") == 0u) return flamegpu::ALIVE;
    
    unsigned int d6 = FLAMEGPU->getVariable<unsigned int>("s6_days");
    FLAMEGPU->setVariable<unsigned int>("s6_days", d6 + 1u);
    
    return flamegpu::ALIVE;
}
    """
    
    try:
        model = pyflamegpu.ModelDescription("Status6S6Debug")
        env = model.Environment()
        env.newPropertyUInt("export_phase", 0)
        
        agent = model.newAgent("component")
        agent.newVariableUInt("status_id", 0)
        agent.newVariableUInt("s6_started", 0)
        agent.newVariableUInt("s6_days", 0)
        
        agent.newRTCFunction("rtc_status_6_s6vars", rtc_with_s6)
        layer = model.newLayer()
        layer.addAgentFunction(agent.getFunction("rtc_status_6_s6vars"))
        
        sim = pyflamegpu.CUDASimulation(model)
        sim.setEnvironmentPropertyUInt("export_phase", 0)
        
        print("✅ Версия с s6_variables компилируется")
        return True
        
    except Exception as e:
        print(f"❌ Версия с s6_variables не компилируется: {e}")
        return False


def main():
    """Пошаговая отладка rtc_status_6"""
    
    print("🚀 Пошаговая отладка rtc_status_6")
    print("=" * 40)
    
    tests = [
        ("Минимальная версия", test_minimal_status_6),
        ("С export_phase", test_with_export_phase),
        ("С s6 переменными", test_with_s6_variables),
    ]
    
    for test_name, test_func in tests:
        print(f"\n🧪 {test_name}:")
        try:
            success = test_func()
            if not success:
                print(f"❌ Тест '{test_name}' провалился - останавливаемся")
                break
        except Exception as e:
            print(f"❌ Критическая ошибка в '{test_name}': {e}")
            break
    
    print(f"\n📋 Рекомендация:")
    print(f"Используйте последнюю рабочую версию для интеграции")


if __name__ == '__main__':
    main()


