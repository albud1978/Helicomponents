#!/usr/bin/env python3
"""
Тест компиляции модели с spawn слоями
"""

import os
import sys

# Устанавливаем флаг для включения spawn
os.environ['HL_ENABLE_SPAWN'] = '1'
os.environ['HL_ENABLE_MP2'] = '1'

from model_build import build_model_for_quota_smoke

def main():
    print("🔨 Тестирование компиляции модели с spawn...")
    
    try:
        # Пробуем собрать модель
        model, agent_desc = build_model_for_quota_smoke(frames_total=286, days_total=30)
        
        print("✅ Модель успешно скомпилирована!")
        
        # Проверяем наличие spawn агентов
        try:
            spawn_mgr = model.getAgent("spawn_mgr")
            spawn_ticket = model.getAgent("spawn_ticket")
            print("✅ Spawn агенты найдены в модели")
            
            # Проверяем функции
            spawn_mgr_fn = spawn_mgr.getFunction("rtc_spawn_mgr")
            spawn_ticket_fn = spawn_ticket.getFunction("rtc_spawn_mi17_atomic")
            print("✅ Spawn RTC функции найдены")
            
        except Exception as e:
            print(f"❌ Spawn агенты/функции не найдены: {e}")
            
        # Проверяем количество слоев
        layer_count = model.getLayersCount()
        print(f"📊 Количество слоев в модели: {layer_count}")
        
    except Exception as e:
        print(f"❌ Ошибка компиляции модели: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
