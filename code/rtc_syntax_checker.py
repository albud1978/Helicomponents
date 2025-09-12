#!/usr/bin/env python3
"""
Проверка синтаксиса RTC функций без компиляции NVRTC
Быстрая валидация исходного кода перед добавлением в модель
Дата: 2025-09-12
"""

import re
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))


class RTCSyntaxChecker:
    """Проверка синтаксиса RTC функций без NVRTC"""
    
    def __init__(self):
        self.errors = []
        self.warnings = []
    
    def check_rtc_function(self, name: str, source: str, frames: int, days: int) -> bool:
        """Проверяет синтаксис RTC функции"""
        
        self.errors.clear()
        self.warnings.clear()
        
        print(f"🔍 Проверка синтаксиса: {name}")
        
        # Базовые проверки
        self._check_function_signature(source, name)
        self._check_frame_days_usage(source, frames, days)
        self._check_variable_access(source)
        self._check_environment_access(source)
        self._check_common_patterns(source)
        
        # Результат
        if self.errors:
            print(f"❌ Найдено ошибок: {len(self.errors)}")
            for error in self.errors:
                print(f"  🔴 {error}")
        
        if self.warnings:
            print(f"⚠️ Предупреждений: {len(self.warnings)}")
            for warning in self.warnings:
                print(f"  🟡 {warning}")
        
        if not self.errors and not self.warnings:
            print(f"✅ Синтаксис корректен")
        
        return len(self.errors) == 0
    
    def _check_function_signature(self, source: str, expected_name: str):
        """Проверяет сигнатуру функции"""
        
        # Поиск FLAMEGPU_AGENT_FUNCTION
        pattern = r'FLAMEGPU_AGENT_FUNCTION\s*\(\s*(\w+)\s*,'
        match = re.search(pattern, source)
        
        if not match:
            self.errors.append("Не найдена сигнатура FLAMEGPU_AGENT_FUNCTION")
            return
        
        actual_name = match.group(1)
        if actual_name != expected_name:
            self.warnings.append(f"Имя функции '{actual_name}' не соответствует ожидаемому '{expected_name}'")
    
    def _check_frame_days_usage(self, source: str, frames: int, days: int):
        """Проверяет использование FRAMES и DAYS"""
        
        # Проверка объявления констант
        if f"FRAMES = {frames}u" not in source:
            self.warnings.append(f"Константа FRAMES не соответствует параметру {frames}")
        
        if f"DAYS = {days}u" in source:
            # DAYS используется - проверим корректность
            pass
        
        # Проверка потенциально опасных выражений
        dangerous_patterns = [
            r'FRAMES\s*\*\s*DAYS',
            r'DAYS\s*\*\s*FRAMES',
            r'getMacroProperty<[^>]*,\s*\(\s*FRAMES\s*\*\s*DAYS\s*\)\s*>'
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, source):
                self.warnings.append(f"Потенциально проблемное выражение FRAMES*DAYS (NVRTC лимиты)")
    
    def _check_variable_access(self, source: str):
        """Проверяет обращения к агентным переменным"""
        
        # Поиск getVariable/setVariable
        get_vars = re.findall(r'getVariable<[^>]*>\s*\(\s*"(\w+)"\s*\)', source)
        set_vars = re.findall(r'setVariable<[^>]*>\s*\(\s*"(\w+)"\s*,', source)
        
        # Проверка на типичные переменные
        common_vars = {
            "idx", "status_id", "group_by", "ops_ticket", "intent_flag",
            "sne", "ppr", "ll", "oh", "br", "repair_days", "repair_time",
            "assembly_time", "partout_time", "mfg_date",
            "daily_today_u32", "daily_next_u32",
            "active_trigger", "assembly_trigger", "partout_trigger",
            "active_trigger_mark", "assembly_trigger_mark", "partout_trigger_mark",
            "psn", "partseqno_i", "aircraft_number", "ac_type_mask",
            "s6_started", "s6_days"
        }
        
        for var in get_vars + set_vars:
            if var not in common_vars:
                self.warnings.append(f"Неизвестная переменная: {var}")
    
    def _check_environment_access(self, source: str):
        """Проверяет обращения к Environment"""
        
        # Поиск getProperty
        get_props = re.findall(r'getProperty<[^>]*>\s*\(\s*"(\w+)"\s*', source)
        
        # Поиск getMacroProperty
        macro_props = re.findall(r'getMacroProperty<[^>]*>\s*\(\s*"(\w+)"\s*\)', source)
        
        # Проверка типичных свойств
        common_props = {
            "version_date", "frames_total", "days_total", "export_phase",
            "mp4_ops_counter_mi8", "mp4_ops_counter_mi17", "mp5_daily_hours"
        }
        
        for prop in get_props:
            if prop not in common_props:
                self.warnings.append(f"Неизвестное Environment Property: {prop}")
        
        common_macro = {
            "mi8_intent", "mi17_intent", "mi8_approve", "mi17_approve",
            "mp2_status", "mp2_repair_days", "mp2_sne", "mp2_ppr"
        }
        
        for prop in macro_props:
            if prop not in common_macro:
                self.warnings.append(f"Неизвестное MacroProperty: {prop}")
    
    def _check_common_patterns(self, source: str):
        """Проверяет общие паттерны и потенциальные проблемы"""
        
        # Проверка return flamegpu::ALIVE
        if "return flamegpu::ALIVE" not in source:
            self.errors.append("Отсутствует 'return flamegpu::ALIVE'")
        
        # Проверка экспорт фазы
        if "export_phase" in source:
            if 'getProperty<unsigned int>("export_phase")' not in source:
                self.warnings.append("Возможно неправильное обращение к export_phase")
        
        # Проверка границ массивов
        if "idx >= FRAMES" not in source and "getVariable" in source:
            self.warnings.append("Отсутствует проверка границ idx >= FRAMES")


def test_all_rtc_functions():
    """Тестирует синтаксис всех RTC функций"""
    
    checker = RTCSyntaxChecker()
    
    print("🔍 Проверка синтаксиса всех RTC функций")
    print("=" * 40)
    
    # Список RTC модулей для проверки
    rtc_tests = [
        ("rtc.begin_day", "PrepareDayRTC", "rtc_prepare_day"),
        ("rtc.status_2", "Status2RTC", "rtc_status_2"),
        ("rtc.status_4", "Status4RTC", "rtc_status_4"),
        ("rtc.status_6", "Status6RTC", "rtc_status_6"),
    ]
    
    frames, days = 100, 30
    passed = 0
    
    for module_name, class_name, func_name in rtc_tests:
        try:
            # Импортируем модуль
            module = __import__(module_name, fromlist=[class_name])
            rtc_class = getattr(module, class_name)
            
            # Получаем исходный код
            source = rtc_class.get_source(frames, days)
            
            # Проверяем синтаксис
            if checker.check_rtc_function(func_name, source, frames, days):
                passed += 1
            
            print()  # Пустая строка между проверками
            
        except Exception as e:
            print(f"❌ Ошибка загрузки {module_name}: {e}")
    
    print("=" * 40)
    print(f"📊 Результат: {passed}/{len(rtc_tests)} функций прошли проверку")
    
    return passed == len(rtc_tests)


def check_single_rtc():
    """Проверяет одну RTC функцию по выбору"""
    
    if len(sys.argv) < 2:
        print("Использование: python3 rtc_syntax_checker.py <rtc_function_name>")
        print("Доступные функции: probe_mp5, begin_day, status_2, status_4, status_6")
        return
    
    func_name = sys.argv[1]
    
    rtc_map = {
        "prepare_day": ("rtc.begin_day", "PrepareDayRTC", "rtc_prepare_day"),
        "begin_day": ("rtc.begin_day", "PrepareDayRTC", "rtc_prepare_day"),  # Алиас
        "status_2": ("rtc.status_2", "Status2RTC", "rtc_status_2"),
        "status_4": ("rtc.status_4", "Status4RTC", "rtc_status_4"),
        "status_6": ("rtc.status_6", "Status6RTC", "rtc_status_6"),
    }
    
    if func_name not in rtc_map:
        print(f"❌ Неизвестная функция: {func_name}")
        return
    
    module_name, class_name, rtc_name = rtc_map[func_name]
    
    try:
        module = __import__(module_name, fromlist=[class_name])
        rtc_class = getattr(module, class_name)
        source = rtc_class.get_source(100, 30)
        
        checker = RTCSyntaxChecker()
        checker.check_rtc_function(rtc_name, source, 100, 30)
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")


def main():
    """Главная функция"""
    
    if len(sys.argv) > 1:
        check_single_rtc()
    else:
        test_all_rtc_functions()


if __name__ == '__main__':
    main()
