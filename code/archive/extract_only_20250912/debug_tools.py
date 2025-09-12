#!/usr/bin/env python3
"""
Инструменты отладки NVRTC/JIT для FLAME GPU
Основано на исследовании логирования и лучших практиках
Дата: 2025-09-12
"""

import os
import sys
import time
from pathlib import Path
from typing import Dict, Any

sys.path.append(str(Path(__file__).parent / 'utils'))


class NVRTCDebugger:
    """Инструменты отладки NVRTC компиляции"""
    
    @staticmethod
    def setup_debug_environment(jit_log: bool = True, seatbelts: bool = True, 
                               cuda_blocking: bool = True):
        """Настройка environment для максимальной отладочной информации"""
        
        debug_vars = {}
        
        if jit_log:
            # Включаем подробный JIT лог
            debug_vars['HL_JIT_LOG'] = '1'
            debug_vars['FLAMEGPU_JIT_LOG_LEVEL'] = '1'
            debug_vars['PYTHONUNBUFFERED'] = '1'
        
        if seatbelts:
            # Включаем дополнительные проверки FLAME GPU
            debug_vars['FLAMEGPU_SEATBELTS'] = '1'
        
        if cuda_blocking:
            # Блокирующий режим CUDA для синхронных ошибок
            debug_vars['CUDA_LAUNCH_BLOCKING'] = '1'
        
        # Применяем к окружению
        for key, value in debug_vars.items():
            os.environ[key] = value
            print(f"🔧 {key}={value}")
        
        return debug_vars
    
    @staticmethod
    def log_rtc_source(rtc_name: str, source: str, frames: int, days: int):
        """Логирует исходный код RTC функции для отладки"""
        
        print(f"\n===== RTC SOURCE: {rtc_name} =====")
        print(f"FRAMES={frames}, DAYS={days}")
        print(f"----- SOURCE BEGIN -----")
        print(source)
        print(f"----- SOURCE END -----\n")
    
    @staticmethod
    def create_minimal_rtc_test(frames: int, days: int) -> str:
        """Создает минимальную RTC функцию для проверки компиляции"""
        
        return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_minimal_test, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Минимальный тест компиляции NVRTC
    static const unsigned int FRAMES = {frames}u;
    static const unsigned int DAYS = {days}u;
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int day = FLAMEGPU->getStepCounter();
    
    // Простая проверка границ
    if (idx >= FRAMES || day >= DAYS) return flamegpu::ALIVE;
    
    // Тестовая запись в переменную
    FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u);
    
    return flamegpu::ALIVE;
}}
        """
    
    @staticmethod
    def create_probe_layer_rtc(property_name: str, frames: int) -> str:
        """Создает RTC функцию-пробник для проверки Environment Property"""
        
        return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_probe_{property_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    static const unsigned int FRAMES = {frames}u;
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    if (idx >= FRAMES) return flamegpu::ALIVE;
    
    // Проверяем доступность Environment Property
    try {{
        const unsigned int value = FLAMEGPU->environment.getProperty<unsigned int>("{property_name}", idx);
        // Записываем в агентную переменную для проверки
        FLAMEGPU->setVariable<unsigned int>("ops_ticket", value > 0u ? 1u : 0u);
    }} catch (...) {{
        // При ошибке записываем 0
        FLAMEGPU->setVariable<unsigned int>("ops_ticket", 0u);
    }}
    
    return flamegpu::ALIVE;
}}
        """


class FlameGPUTester:
    """Тестер для пошаговой проверки RTC функций"""
    
    def __init__(self, debug_mode: bool = True):
        self.debug_mode = debug_mode
        self.debugger = NVRTCDebugger()
        
        if debug_mode:
            self.debugger.setup_debug_environment()
    
    def test_single_rtc(self, rtc_name: str, rtc_source: str, 
                       frames: int = 10, days: int = 3, agents: int = 5) -> bool:
        """Тестирует одну RTC функцию изолированно"""
        
        try:
            import pyflamegpu
        except ImportError:
            print("❌ pyflamegpu не установлен")
            return False
        
        try:
            print(f"🧪 Тестирование RTC функции: {rtc_name}")
            
            if self.debug_mode:
                self.debugger.log_rtc_source(rtc_name, rtc_source, frames, days)
            
            # Минимальная модель
            model = pyflamegpu.ModelDescription("RTCTest")
            env = model.Environment()
            
            # Базовые свойства
            env.newPropertyUInt("frames_total", 0)
            env.newPropertyUInt("days_total", 0)
            env.newPropertyArrayUInt32("mp4_ops_counter_mi8", [0] * days)
            env.newPropertyArrayUInt16("mp5_daily_hours", [0] * ((days + 1) * frames))
            
            # Агент с базовыми переменными
            agent = model.newAgent("component")
            basic_vars = ["idx", "status_id", "ops_ticket", "daily_today_u32", "daily_next_u32"]
            for var in basic_vars:
                agent.newVariableUInt(var, 0)
            
            # Добавляем RTC функцию
            agent.newRTCFunction(rtc_name, rtc_source)
            
            # Создаем слой
            layer = model.newLayer()
            layer.addAgentFunction(agent.getFunction(rtc_name))
            
            # Симуляция
            sim = pyflamegpu.CUDASimulation(model)
            sim.setEnvironmentPropertyUInt("frames_total", frames)
            sim.setEnvironmentPropertyUInt("days_total", days)
            sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8", [5] * days)
            
            # Тестовая популяция
            population = pyflamegpu.AgentVector(agent, agents)
            for i in range(agents):
                ag = population[i]
                ag.setVariableUInt("idx", i)
                ag.setVariableUInt("status_id", 2)
                ag.setVariableUInt("ops_ticket", 0)
            
            sim.setPopulationData(population)
            
            # Выполнение
            start_time = time.perf_counter()
            sim.step()
            end_time = time.perf_counter()
            
            # Проверка результата
            result_pop = pyflamegpu.AgentVector(agent)
            sim.getPopulationData(result_pop)
            
            # Анализ изменений
            changes = 0
            for ag in result_pop:
                if int(ag.getVariableUInt("ops_ticket")) != 0:
                    changes += 1
            
            duration = (end_time - start_time) * 1000
            
            print(f"✅ RTC тест прошел: {rtc_name}")
            print(f"  Время: {duration:.2f} мс")
            print(f"  Изменений: {changes}/{agents}")
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка тестирования RTC {rtc_name}: {e}")
            
            if self.debug_mode:
                print(f"\n===== NVRTC ERROR DETAILS =====")
                print(f"Функция: {rtc_name}")
                print(f"Параметры: frames={frames}, days={days}, agents={agents}")
                print(f"Ошибка: {e}")
                import traceback
                traceback.print_exc()
            
            return False
    
    def test_environment_property(self, property_name: str, property_type: str,
                                 test_data: Any, frames: int = 10) -> bool:
        """Тестирует доступность Environment Property из RTC"""
        
        try:
            import pyflamegpu
        except ImportError:
            return False
        
        try:
            print(f"🧪 Тестирование Environment Property: {property_name}")
            
            # Создаем пробник
            probe_source = self.debugger.create_probe_layer_rtc(property_name, frames)
            
            return self.test_single_rtc(f"rtc_probe_{property_name}", probe_source, frames)
            
        except Exception as e:
            print(f"❌ Ошибка тестирования Property {property_name}: {e}")
            return False


def test_nvrtc_compilation():
    """Тестирует базовую компиляцию NVRTC"""
    
    print("🧪 Тест базовой компиляции NVRTC...")
    
    debugger = NVRTCDebugger()
    debugger.setup_debug_environment()
    
    tester = FlameGPUTester(debug_mode=True)
    
    # Тест минимальной RTC функции
    minimal_source = debugger.create_minimal_rtc_test(frames=10, days=5)
    success = tester.test_single_rtc("rtc_minimal_test", minimal_source)
    
    if success:
        print("✅ Базовая компиляция NVRTC работает")
    else:
        print("❌ Проблемы с базовой компиляцией NVRTC")
    
    return success


def main():
    """Главная функция для тестирования отладочных инструментов"""
    
    print("🚀 Тестирование инструментов отладки NVRTC")
    print("=" * 45)
    
    # Тест базовой компиляции
    if not test_nvrtc_compilation():
        print("❌ Базовая компиляция не работает")
        return 1
    
    print("✅ Инструменты отладки готовы к использованию")
    
    # Рекомендации
    print("\n📋 Рекомендации по отладке:")
    print("1. Используйте HL_JIT_LOG=1 для полного лога компиляции")
    print("2. Включайте seatbelts=on для дополнительных проверок")
    print("3. Тестируйте RTC функции изолированно перед интеграцией")
    print("4. Минимизируйте размеры FRAMES*DAYS в шаблонах")
    print("5. Используйте printf в RTC только для коротких smoke тестов")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())


