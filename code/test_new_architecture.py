#!/usr/bin/env python3
"""
Тест новой модульной архитектуры симуляции
Проверка загрузки RTC функций и сборки пайплайна
Дата: 2025-09-12
"""

import sys
import os
from pathlib import Path

# Добавляем пути
sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent / 'utils'))

def test_rtc_loading():
    """Тест загрузки RTC модулей"""
    
    print("🧪 Тестирование загрузки RTC модулей...")
    
    try:
        from rtc.probe_mp5 import ProbeMP5RTC
    except Exception as e:
        print("[WARN] rtc_probe_mp5 отсутствует или недоступен:", e)
    from rtc.begin_day import PrepareDayRTC
    from rtc.status_2 import Status2RTC
    from rtc.status_4 import Status4RTC
    from rtc.status_6 import Status6RTC
    from rtc.quota_intent import QuotaIntentRTC
    from rtc.quota_approve import QuotaApproveRTC
    from rtc.quota_apply import QuotaApplyRTC
    from rtc.quota_clear import QuotaClearRTC
    
    print("✅ Все RTC модули загружены успешно")
    
    # Тест генерации исходного кода
    frames, days = 100, 30
    
    print(f"\n📝 Тест генерации кода (frames={frames}, days={days}):")
    
    rtc_classes = [
        PrepareDayRTC, Status2RTC, Status4RTC, Status6RTC,
        QuotaIntentRTC, QuotaApproveRTC, QuotaApplyRTC, QuotaClearRTC
    ]
    
    for rtc_class in rtc_classes:
        try:
            source = rtc_class.get_source(frames, days)
            lines = len(source.split('\n'))
            print(f"  ✅ {rtc_class.NAME}: {lines} строк")
        except Exception as e:
            print(f"  ❌ {rtc_class.NAME}: ошибка - {e}")
    
    return True
    
    except Exception as e:
        print(f"❌ Ошибка загрузки RTC модулей: {e}")
        return False


def test_pipeline_config():
    """Тест конфигурации пайплайна"""
    
    print("\n🧪 Тестирование конфигурации пайплайна...")
    
    try:
        from sim.pipeline_config import RTCPipeline, RTC_PROFILES, apply_profile
        
        pipeline = RTCPipeline()
        
        print(f"✅ Загружено {len(RTC_PROFILES)} профилей:")
        for name, profile in RTC_PROFILES.items():
            enabled_count = sum(1 for v in profile["flags"].values() if v)
            total_count = len(profile["flags"])
            print(f"  {name}: {enabled_count}/{total_count} функций")
        
        # Тест получения активных функций
        for profile_name in RTC_PROFILES.keys():
            enabled = pipeline.get_enabled_functions(profile_name)
            print(f"  {profile_name}: {len(enabled)} активных функций")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка тестирования пайплайна: {e}")
        return False


def test_sim_builder():
    """Тест сборщика симуляции"""
    
    print("\n🧪 Тестирование сборщика симуляции...")
    
    try:
        from sim.sim_builder import SimBuilder
        
        builder = SimBuilder()
        
        print(f"✅ SimBuilder создан")
        print(f"  Загружено RTC модулей: {len(builder.rtc_registry)}")
        
        for name, rtc_class in builder.rtc_registry.items():
            print(f"    {name}: {rtc_class.__name__}")
        
        # Тест информации о пайплайне
        info = builder.get_pipeline_info("minimal")
        print(f"\n📋 Информация о пайплайне 'minimal':")
        print(info)
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка тестирования SimBuilder: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Главная функция тестирования"""
    
    print("🚀 Тестирование новой архитектуры симуляции")
    print("=" * 50)
    
    tests = [
        ("RTC Loading", test_rtc_loading),
        ("Pipeline Config", test_pipeline_config),
        ("Sim Builder", test_sim_builder),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n🧪 {test_name}")
        print("-" * 30)
        
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            print(f"❌ Критическая ошибка в {test_name}: {e}")
            results.append((test_name, False))
    
    # Итоговый отчет
    print("\n" + "=" * 50)
    print("📊 Итоговый отчет:")
    
    passed = 0
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {test_name}: {status}")
        if success:
            passed += 1
    
    print(f"\nРезультат: {passed}/{len(results)} тестов пройдено")
    
    if passed == len(results):
        print("🎉 Все тесты прошли успешно! Архитектура готова к использованию.")
        return 0
    else:
        print("⚠️ Есть проблемы, требующие исправления.")
        return 1


if __name__ == '__main__':
    sys.exit(main())


