#!/usr/bin/env python3
"""
Тест унифицированного RTC пайплайна

Проверяет работу новой системы профилей и флагов
"""

import os
import sys

# Устанавливаем тестовые флаги
os.environ['HL_USE_UNIFIED_PIPELINE'] = '1'
os.environ['HL_PROFILE'] = 'minimal'  # Начинаем с минимального профиля

try:
    from model_build import build_model_for_quota_smoke, apply_profile, migrate_legacy_flags, RTC_PROFILES
    print("✅ Модули импортированы успешно")
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    sys.exit(1)

def test_profiles():
    """Тестирует работу профилей"""
    print("\n🧪 Тестирование профилей...")
    
    for profile_name in RTC_PROFILES.keys():
        print(f"\n📋 Тестируем профиль '{profile_name}':")
        try:
            apply_profile(profile_name)
            
            # Проверяем какие флаги установлены
            enabled_flags = []
            for _, flag in [
                ("rtc_status_6", "HL_ENABLE_STATUS_6"),
                ("rtc_status_4", "HL_ENABLE_STATUS_4"),
                ("rtc_status_2", "HL_ENABLE_STATUS_2"),
                ("rtc_quota_s2", "HL_ENABLE_QUOTA_S2"),
                ("rtc_spawn", "HL_ENABLE_SPAWN"),
                ("rtc_log", "HL_ENABLE_MP2_LOG"),
            ]:
                if os.environ.get(flag, '0') == '1':
                    enabled_flags.append(flag)
            
            print(f"  ✅ Включено {len(enabled_flags)} функций: {', '.join(enabled_flags)}")
            
        except Exception as e:
            print(f"  ❌ Ошибка: {e}")

def test_legacy_migration():
    """Тестирует миграцию старых флагов"""
    print("\n🔄 Тестирование миграции старых флагов...")
    
    # Сохраняем текущие переменные окружения
    original_env = dict(os.environ)
    
    test_cases = [
        ("HL_RTC_MODE=spawn_only", {"HL_RTC_MODE": "spawn_only"}),
        ("HL_STATUS246_SMOKE=1", {"HL_STATUS246_SMOKE": "1"}),
        ("HL_STATUS4_SMOKE=1", {"HL_STATUS4_SMOKE": "1"}),
    ]
    
    for test_name, test_env in test_cases:
        print(f"\n📤 Тест: {test_name}")
        
        # Очищаем окружение
        for key in list(os.environ.keys()):
            if key.startswith('HL_'):
                del os.environ[key]
        
        # Устанавливаем тестовые флаги
        for key, value in test_env.items():
            os.environ[key] = value
        
        try:
            migrate_legacy_flags()
            print("  ✅ Миграция выполнена")
        except Exception as e:
            print(f"  ❌ Ошибка миграции: {e}")
    
    # Восстанавливаем окружение
    os.environ.clear()
    os.environ.update(original_env)

def test_unified_build():
    """Тестирует создание унифицированной модели"""
    print("\n🏗️ Тестирование создания унифицированной модели...")
    
    try:
        # Устанавливаем минимальную конфигурацию для теста
        os.environ['HL_USE_UNIFIED_PIPELINE'] = '1'
        os.environ['HL_PROFILE'] = 'minimal'
        
        print("  📦 Создание модели...")
        model, agent = build_model_for_quota_smoke(frames_total=10, days_total=7)
        
        if model and agent:
            print("  ✅ Модель создана успешно")
            print(f"  📊 Тип модели: {type(model).__name__}")
            print(f"  👤 Тип агента: {type(agent).__name__}")
        else:
            print("  ⚠️ Модель создана, но результат неполный")
            
    except Exception as e:
        print(f"  ❌ Ошибка создания модели: {e}")
        import traceback
        traceback.print_exc()

def main():
    print("🚀 Тестирование унифицированного RTC пайплайна")
    print("=" * 50)
    
    # Тесты
    test_profiles()
    test_legacy_migration()
    test_unified_build()
    
    print("\n" + "=" * 50)
    print("✅ Тестирование завершено")

if __name__ == "__main__":
    main()

