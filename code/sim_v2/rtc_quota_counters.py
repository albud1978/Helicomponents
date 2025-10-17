"""
Модуль для отладочных счетчиков квотирования
Используется MacroProperty вместо printf для надежности
"""

def register_quota_counters(model, max_frames, max_days):
    """Регистрирует MacroProperty для отладки квотирования"""
    
    # Создаем массивы-счетчики по дням
    model.Environment().newMacroPropertyUInt("debug_ops_count_by_day", max_days + 1)
    model.Environment().newMacroPropertyUInt("debug_svc_count_by_day", max_days + 1)
    model.Environment().newMacroPropertyUInt("debug_reserve_count_by_day", max_days + 1)
    model.Environment().newMacroPropertyUInt("debug_inactive_count_by_day", max_days + 1)
    
    # Булевы флаги для каждого агента на каждый день (для AC 24113)
    # На день 100 дней * 286 агентов = 28600 элементов
    model.Environment().newMacroPropertyUInt("debug_ac24113_state_by_day", max_days + 1)  # 0=ops, 1=svc, 2=reserve, 3=inactive, 4=repair, 5=storage
    model.Environment().newMacroPropertyUInt("debug_ac24113_intent_by_day", max_days + 1)
    
    print("  Отладочные счетчики квотирования зарегистрированы")

