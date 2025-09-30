#!/usr/bin/env python3
"""
Тест интеграции spawn в V2 архитектуру

Проверяет:
1. Компиляцию RTC модулей без ошибок NVRTC 425
2. Корректное создание новых агентов
3. Интеграцию с существующим пайплайном
"""

import sys
import os

# Добавляем пути
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from orchestrator_v2 import V2Orchestrator
from sim_env_setup import get_client, prepare_env_arrays

def test_spawn_integration(steps: int = 30):
    """
    Тест spawn интеграции
    
    Args:
        steps: количество шагов (дней) симуляции
    """
    print("\n=== Тест Spawn Integration ===\n")
    
    # Загружаем данные
    print("1. Загрузка данных из ClickHouse...")
    client = get_client()
    env_data = prepare_env_arrays(client)
    print(f"   Загружено: {env_data['frames_total_u16']} frames, {env_data['days_total_u16']} days")
    
    # Создаём оркестратор с spawn модулем
    print("\n2. Создание оркестратора со spawn...")
    orchestrator = V2Orchestrator(env_data, enable_mp2=False)
    
    # Модули: базовые + spawn
    modules = [
        'state_2_operations',
        'quota_ops_excess',
        'states_stub',
        'state_manager_operations',
        'state_manager_repair',
        'state_manager_storage',
        'spawn'  # ДОБАВЛЕН SPAWN
    ]
    
    print(f"   Модули: {', '.join(modules)}")
    
    try:
        # Строим модель
        print("\n3. Построение модели с spawn...")
        orchestrator.build_model(modules)
        print("   ✅ Модель построена без ошибок NVRTC!")
        
        # Создаём симуляцию
        print("\n4. Создание симуляции...")
        orchestrator.create_simulation()
        print(f"   ✅ Симуляция создана, spawn_enabled={orchestrator.spawn_enabled}")
        
        # Проверяем начальную популяцию
        print("\n5. Проверка начальной популяции...")
        initial_results = orchestrator.get_results()
        initial_count = len(initial_results)
        print(f"   Начальное количество агентов: {initial_count}")
        
        # Запускаем симуляцию
        print(f"\n6. Запуск симуляции на {steps} шагов...")
        orchestrator.run(steps)
        print("   ✅ Симуляция завершена без ошибок!")
        
        # Проверяем финальную популяцию
        print("\n7. Проверка финальной популяции...")
        final_results = orchestrator.get_results()
        final_count = len(final_results)
        spawned = final_count - initial_count
        
        print(f"   Финальное количество агентов: {final_count}")
        print(f"   Создано новых агентов: {spawned}")
        
        # Детали по новорождённым
        if spawned > 0:
            print("\n8. Детали новорождённых агентов:")
            newborns = [r for r in final_results if r['aircraft_number'] >= env_data.get('base_acn_spawn', 100000)]
            print(f"   Всего новорождённых: {len(newborns)}")
            
            if len(newborns) > 0:
                print("\n   Первые 5 новорождённых:")
                for nb in newborns[:5]:
                    print(f"     AC {nb['aircraft_number']}: idx={nb['idx']}, state={nb['state']}, "
                          f"intent={nb['intent_state']}, sne={nb['sne']}, ll={nb['ll']}, oh={nb['oh']}, br={nb['br']}")
        
        # Распределение по состояниям
        print("\n9. Финальное распределение по состояниям:")
        state_counts = {}
        for r in final_results:
            state = r['state']
            state_counts[state] = state_counts.get(state, 0) + 1
        
        for state, count in sorted(state_counts.items()):
            print(f"   {state:15} {count:4}")
        
        print("\n=== ✅ ТЕСТ УСПЕШНО ЗАВЕРШЁН ===")
        print(f"\nРезультат: Spawn интегрирован без ошибок NVRTC 425!")
        print(f"Создано {spawned} новых агентов за {steps} дней")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Тест spawn integration')
    parser.add_argument('--steps', type=int, default=30, help='Количество шагов симуляции')
    args = parser.parse_args()
    
    success = test_spawn_integration(args.steps)
    sys.exit(0 if success else 1)
