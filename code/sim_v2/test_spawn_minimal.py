#!/usr/bin/env python3
"""
Минимальный тест spawn без state managers
Проверяет только компиляцию и базовое создание агентов
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from orchestrator_v2 import V2Orchestrator
from sim_env_setup import get_client, prepare_env_arrays

def test_spawn_minimal(steps: int = 5):
    """Минимальный тест только spawn модуля"""
    print("\n=== Минимальный тест Spawn (без state managers) ===\n")
    
    # Загружаем данные
    print("1. Загрузка данных...")
    client = get_client()
    env_data = prepare_env_arrays(client)
    
    # Создаём оркестратор
    print("\n2. Создание оркестратора...")
    orchestrator = V2Orchestrator(env_data, enable_mp2=False)
    
    # ТОЛЬКО базовые модули + spawn (БЕЗ state managers)
    modules = [
        'spawn_simple'  # УПРОЩЁННЫЙ SPAWN для отладки
    ]
    
    try:
        # Строим модель
        print("\n3. Построение модели...")
        orchestrator.build_model(modules)
        print("   ✅ Модель построена!")
        
        # Создаём симуляцию
        print("\n4. Создание симуляции...")
        orchestrator.create_simulation()
        print("   ✅ Симуляция создана!")
        
        # Проверяем начальную популяцию
        initial_results = orchestrator.get_results()
        print(f"\n5. Начальная популяция: {len(initial_results)} агентов")
        
        # Запускаем
        print(f"\n6. Запуск на {steps} шагов...")
        orchestrator.run(steps)
        print("   ✅ Завершено!")
        
        # Проверяем финал
        final_results = orchestrator.get_results()
        spawned = len(final_results) - len(initial_results)
        print(f"\n7. Финал: {len(final_results)} агентов (+{spawned} новых)")
        
        if spawned > 0:
            print("\n   Новорождённые:")
            for r in final_results[-spawned:]:
                print(f"     AC {r['aircraft_number']}: idx={r['idx']}, state={r['state']}")
        
        print("\n✅ УСПЕХ: Spawn работает без ошибок!")
        return True
        
    except Exception as e:
        print(f"\n❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = test_spawn_minimal()
    sys.exit(0 if success else 1)
