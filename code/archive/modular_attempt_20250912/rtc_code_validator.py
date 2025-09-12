#!/usr/bin/env python3
"""
Валидатор RTC кода без выполнения FLAME GPU
Проверка готовности кода к переносу из бэкапов
Дата: 2025-09-12
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))


def validate_rtc_ready_for_transfer():
    """Проверяет готовность RTC функций к переносу из бэкапов"""
    
    print("🔍 Валидация готовности RTC функций")
    print("=" * 40)
    
    # Проверяем что все RTC модули загружаются
    rtc_modules = [
        ("begin_day", "PrepareDayRTC"),
        ("status_2", "Status2RTC"),
        ("status_4", "Status4RTC"),
        ("status_6", "Status6RTC"),
        ("quota_intent", "QuotaIntentRTC"),
        ("quota_approve", "QuotaApproveRTC"),
        ("quota_apply", "QuotaApplyRTC"),
        ("quota_clear", "QuotaClearRTC"),
        ("status_post", "StatusPostRTC"),
        ("log_mp2", "LogMP2RTC"),
        ("spawn_mgr", "SpawnMgrRTC"),
        ("spawn_ticket", "SpawnTicketRTC"),
    ]
    
    loaded = 0
    generated = 0
    
    for module_name, class_name in rtc_modules:
        try:
            # Импорт модуля
            module = __import__(f'rtc.{module_name}', fromlist=[class_name])
            rtc_class = getattr(module, class_name)
            
            print(f"✅ {module_name}: {class_name} загружен")
            loaded += 1
            
            # Тест генерации кода
            try:
                source = rtc_class.get_source(100, 30)
                lines = len(source.split('\n'))
                if lines > 5:  # Минимальная проверка что код сгенерировался
                    print(f"  📝 Код сгенерирован: {lines} строк")
                    generated += 1
                else:
                    print(f"  ⚠️ Код слишком короткий: {lines} строк")
            except Exception as e:
                print(f"  ❌ Ошибка генерации кода: {e}")
                
        except Exception as e:
            print(f"❌ {module_name}: ошибка загрузки - {e}")
    
    print("\n" + "=" * 40)
    print(f"📊 Результат валидации:")
    print(f"  Загружено модулей: {loaded}/{len(rtc_modules)}")
    print(f"  Генерирует код: {generated}/{len(rtc_modules)}")
    
    if loaded == len(rtc_modules) and generated >= len(rtc_modules) * 0.8:
        print("✅ RTC функции готовы к переносу из бэкапов")
        return True
    else:
        print("❌ Есть проблемы, требующие исправления")
        return False


def show_rtc_function_source(func_name: str):
    """Показывает исходный код RTC функции"""
    
    rtc_map = {
        "prepare_day": ("rtc.begin_day", "PrepareDayRTC"),
        "begin_day": ("rtc.begin_day", "PrepareDayRTC"),  # Алиас
        "status_2": ("rtc.status_2", "Status2RTC"),
        "status_4": ("rtc.status_4", "Status4RTC"),
        "status_6": ("rtc.status_6", "Status6RTC"),
        "quota_intent": ("rtc.quota_intent", "QuotaIntentRTC"),
        "quota_approve": ("rtc.quota_approve", "QuotaApproveRTC"),
        "quota_apply": ("rtc.quota_apply", "QuotaApplyRTC"),
        "quota_clear": ("rtc.quota_clear", "QuotaClearRTC"),
        "status_post": ("rtc.status_post", "StatusPostRTC"),
        "log_mp2": ("rtc.log_mp2", "LogMP2RTC"),
        "spawn_mgr": ("rtc.spawn_mgr", "SpawnMgrRTC"),
        "spawn_ticket": ("rtc.spawn_ticket", "SpawnTicketRTC"),
    }
    
    if func_name not in rtc_map:
        print(f"❌ Неизвестная функция: {func_name}")
        print(f"Доступные: {', '.join(rtc_map.keys())}")
        return
    
    module_name, class_name = rtc_map[func_name]
    
    try:
        module = __import__(module_name, fromlist=[class_name])
        rtc_class = getattr(module, class_name)
        source = rtc_class.get_source(100, 30)
        
        print(f"📝 Исходный код {func_name}:")
        print("=" * 50)
        print(source)
        print("=" * 50)
        
    except Exception as e:
        print(f"❌ Ошибка получения кода {func_name}: {e}")


def main():
    """Главная функция"""
    
    if len(sys.argv) > 1:
        show_rtc_function_source(sys.argv[1])
    else:
        validate_rtc_ready_for_transfer()


if __name__ == '__main__':
    main()
