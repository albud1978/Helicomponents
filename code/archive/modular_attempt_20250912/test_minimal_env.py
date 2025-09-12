#!/usr/bin/env python3
"""
Минимальный тест Environment без лишних Property
Только базовые скаляры и простые массивы
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


def test_minimal_environment():
    """Тест минимального Environment"""
    
    if pyflamegpu is None:
        print("❌ pyflamegpu не установлен")
        return False
    
    print("🧪 Тест минимального Environment...")
    
    try:
        # Получаем базовые данные
        client = get_clickhouse_client()
        rows = client.execute(
            "SELECT version_date, version_id FROM heli_pandas "
            "ORDER BY version_date DESC, version_id DESC LIMIT 1"
        )
        vdate, vid = rows[0]
        
        # Подсчет агентов
        mp3_count = client.execute("SELECT count() FROM heli_pandas")[0][0]
        
        print(f"📊 Базовые данные: version_date={vdate}, агентов={mp3_count}")
        
        # Создание модели
        model = pyflamegpu.ModelDescription("MinimalTest")
        env = model.Environment()
        
        # Только самые необходимые скаляры
        env.newPropertyUInt("version_date", 0)
        env.newPropertyUInt("frames_total", 0)
        env.newPropertyUInt("days_total", 0)
        
        # Простые массивы (небольшие размеры)
        test_days = 30
        test_frames = 100
        
        env.newPropertyArrayUInt32("mp4_ops_counter_mi8", [0] * test_days)
        env.newPropertyArrayUInt32("mp4_ops_counter_mi17", [0] * test_days)
        
        print(f"✅ Environment создан: {test_frames} кадров, {test_days} дней")
        
        # Создание симуляции
        sim = pyflamegpu.CUDASimulation(model)
        
        # Установка скаляров
        from datetime import date
        epoch = date(1970, 1, 1)
        vdate_ordinal = (vdate - epoch).days
        
        sim.setEnvironmentPropertyUInt("version_date", vdate_ordinal)
        sim.setEnvironmentPropertyUInt("frames_total", test_frames)
        sim.setEnvironmentPropertyUInt("days_total", test_days)
        
        # Установка простых массивов
        sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8", [10, 15, 20] + [0] * (test_days - 3))
        sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi17", [5, 8, 12] + [0] * (test_days - 3))
        
        # Проверка что данные установились
        vd_check = sim.getEnvironmentPropertyUInt("version_date")
        ft_check = sim.getEnvironmentPropertyUInt("frames_total")
        dt_check = sim.getEnvironmentPropertyUInt("days_total")
        
        mp4_check = sim.getEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8")
        
        print(f"✅ Environment проверен:")
        print(f"  version_date: {vd_check} (ожидалось {vdate_ordinal})")
        print(f"  frames_total: {ft_check} (ожидалось {test_frames})")
        print(f"  days_total: {dt_check} (ожидалось {test_days})")
        print(f"  mp4_mi8[0]: {mp4_check[0]} (ожидалось 10)")
        print(f"  mp4_mi8[1]: {mp4_check[1]} (ожидалось 15)")
        
        # Валидация
        success = (
            vd_check == vdate_ordinal and
            ft_check == test_frames and
            dt_check == test_days and
            mp4_check[0] == 10 and
            mp4_check[1] == 15
        )
        
        if success:
            print("✅ Все проверки пройдены!")
        else:
            print("❌ Некоторые проверки провалились")
        
        return success
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_minimal_agent():
    """Тест минимального агента без RTC"""
    
    print("\n🧪 Тест минимального агента...")
    
    try:
        # Создание модели
        model = pyflamegpu.ModelDescription("MinimalAgentTest")
        env = model.Environment()
        
        # Базовые скаляры
        env.newPropertyUInt("frames_total", 0)
        
        # Агент с минимальными переменными
        agent = model.newAgent("component")
        agent.newVariableUInt("idx", 0)
        agent.newVariableUInt("status_id", 0)
        agent.newVariableUInt("aircraft_number", 0)
        
        print("✅ Агент создан с базовыми переменными")
        
        # Создание симуляции
        sim = pyflamegpu.CUDASimulation(model)
        sim.setEnvironmentPropertyUInt("frames_total", 10)
        
        # Создание популяции
        population = pyflamegpu.AgentVector(agent, 5)
        for i in range(5):
            ag = population[i]
            ag.setVariableUInt("idx", i)
            ag.setVariableUInt("status_id", 2)  # Все в эксплуатации
            ag.setVariableUInt("aircraft_number", 20000 + i)
        
        sim.setPopulationData(population)
        
        # Проверка популяции
        test_pop = pyflamegpu.AgentVector(agent)
        sim.getPopulationData(test_pop)
        
        print(f"✅ Популяция проверена:")
        for i in range(min(3, len(test_pop))):
            ag = test_pop[i]
            idx = int(ag.getVariableUInt("idx"))
            status = int(ag.getVariableUInt("status_id"))
            ac_num = int(ag.getVariableUInt("aircraft_number"))
            print(f"  Агент {i}: idx={idx}, status={status}, ac_num={ac_num}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Главная функция"""
    
    print("🚀 Минимальный тест FLAME GPU Environment")
    print("=" * 45)
    
    # Тест Environment
    env_success = test_minimal_environment()
    
    # Тест агентов
    agent_success = test_minimal_agent()
    
    print("\n" + "=" * 45)
    print("📊 Результаты:")
    print(f"  Environment: {'✅ PASS' if env_success else '❌ FAIL'}")
    print(f"  Agent: {'✅ PASS' if agent_success else '❌ FAIL'}")
    
    if env_success and agent_success:
        print("🎉 Базовая архитектура готова для добавления RTC!")
        return 0
    else:
        print("⚠️ Есть проблемы, требующие исправления")
        return 1


if __name__ == '__main__':
    sys.exit(main())


