#!/usr/bin/env python3
"""
Тест компиляции RTC функций для Mode A со спавном
"""
import os
import sys

# Включаем spawn и MP2
os.environ['HL_ENABLE_SPAWN'] = '1'
os.environ['HL_ENABLE_MP2'] = '1'
os.environ['HL_STATUS246_SMOKE'] = '1'
os.environ['PYTHONUNBUFFERED'] = '1'

try:
    import pyflamegpu
except ImportError:
    print("pyflamegpu не установлен")
    sys.exit(1)

from model_build import build_model_for_quota_smoke

def main():
    FRAMES = 286  # frames_total с учетом MP5
    DAYS = 7
    
    print(f"Создание модели: FRAMES={FRAMES}, DAYS={DAYS}")
    print(f"HL_ENABLE_SPAWN={os.environ.get('HL_ENABLE_SPAWN')}")
    print(f"HL_ENABLE_MP2={os.environ.get('HL_ENABLE_MP2')}")
    
    try:
        model, agent_desc = build_model_for_quota_smoke(FRAMES, DAYS)
        print(f"✅ Модель создана успешно")
        
        # Пытаемся создать симуляцию чтобы вызвать компиляцию
        print("Создание симуляции...")
        sim = pyflamegpu.CUDASimulation(model)
        
        # Устанавливаем минимальные environment свойства
        sim.setEnvironmentPropertyUInt("version_date", 0)
        sim.setEnvironmentPropertyUInt("frames_total", FRAMES)
        sim.setEnvironmentPropertyUInt("days_total", DAYS)
        
        print("✅ Симуляция создана, RTC функции скомпилированы")
        
        # Проверяем наличие агентов и слоев
        print(f"\nАнализ модели:")
        try:
            spawn_mgr = model.getAgent("spawn_mgr")
            print("✅ spawn_mgr агент найден")
        except:
            print("❌ spawn_mgr агент НЕ найден")
            
        try:
            spawn_ticket = model.getAgent("spawn_ticket")
            print("✅ spawn_ticket агент найден")
        except:
            print("❌ spawn_ticket агент НЕ найден")
            
        # Подсчет слоев
        layer_count = model.getLayersCount()
        print(f"Количество слоев: {layer_count}")
        
    except Exception as e:
        print(f"\n❌ Ошибка: {type(e).__name__}: {e}")
        
        # Пытаемся найти больше информации об ошибке
        import traceback
        traceback.print_exc()
        
        # Если это ошибка NVRTC, она может содержать детали компиляции
        error_str = str(e)
        if "compile" in error_str.lower() or "nvrtc" in error_str.lower():
            print("\n⚠️ Обнаружена ошибка компиляции NVRTC")
            print("Попробуйте запустить с FLAMEGPU_VERBOSE=1 для деталей")

if __name__ == "__main__":
    main()
