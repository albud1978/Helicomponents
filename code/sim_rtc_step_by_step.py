#!/usr/bin/env python3
"""
Пошаговое добавление RTC функций
Начинаем с Environment, затем добавляем RTC по одной для тестирования
Дата: 2025-09-12
"""

import os
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Tuple

sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent / 'utils'))

from config_loader import get_clickhouse_client

try:
    import pyflamegpu
except ImportError:
    pyflamegpu = None


class StepByStepBuilder:
    """Сборщик для пошагового добавления RTC функций"""
    
    def __init__(self):
        self.rtc_sources = {}
        self.model = None
        self.agent = None
        self.sim = None
    
    def create_base_model(self, frames: int = 100, days: int = 30) -> bool:
        """Создает базовую модель с Environment и агентом"""
        
        if pyflamegpu is None:
            print("❌ pyflamegpu не установлен")
            return False
        
        try:
            # Модель
            self.model = pyflamegpu.ModelDescription("StepByStepModel")
            env = self.model.Environment()
            
            # Базовые скаляры
            env.newPropertyUInt("version_date", 0)
            env.newPropertyUInt("frames_total", 0)
            env.newPropertyUInt("days_total", 0)
            
            # Простые массивы
            env.newPropertyArrayUInt32("mp4_ops_counter_mi8", [0] * days)
            env.newPropertyArrayUInt32("mp4_ops_counter_mi17", [0] * days)
            env.newPropertyArrayUInt16("mp5_daily_hours", [0] * ((days + 1) * frames))
            
            # Агент
            self.agent = self.model.newAgent("component")
            
            # Основные переменные
            basic_vars = [
                "idx", "group_by", "status_id", "repair_days", "repair_time",
                "sne", "ppr", "ll", "oh", "br",
                "daily_today_u32", "daily_next_u32", "ops_ticket", "intent_flag"
            ]
            
            for var_name in basic_vars:
                self.agent.newVariableUInt(var_name, 0)
            
            print(f"✅ Базовая модель создана: {frames} кадров, {days} дней")
            print(f"✅ Агент создан с {len(basic_vars)} переменными")
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка создания базовой модели: {e}")
            return False
    
    def add_rtc_function(self, name: str, source: str, debug_log: bool = True) -> bool:
        """Добавляет одну RTC функцию с отладочным логированием"""
        
        if self.agent is None:
            print("❌ Сначала создайте базовую модель")
            return False
        
        if debug_log:
            # Логируем исходный код согласно лучшим практикам
            print(f"\n===== RTC SOURCE: {name} =====")
            lines = len(source.split('\n'))
            print(f"Строк кода: {lines}")
            print(f"----- SOURCE BEGIN -----")
            print(source)
            print(f"----- SOURCE END -----\n")
        
        try:
            self.agent.newRTCFunction(name, source)
            self.rtc_sources[name] = source
            print(f"✅ RTC функция добавлена: {name}")
            return True
            
        except Exception as e:
            print(f"❌ NVRTC компиляция провалилась для {name}")
            print(f"\n===== NVRTC ERROR DETAILS =====")
            print(f"Функция: {name}")
            print(f"Ошибка: {e}")
            print(f"Рекомендации:")
            print(f"  1. Проверьте размерности FRAMES/DAYS в шаблонах")
            print(f"  2. Убедитесь что все Environment Properties объявлены")
            print(f"  3. Проверьте синтаксис C++/CUDA")
            print(f"  4. Минимизируйте сложные выражения в шаблонах")
            print(f"----- ERROR SOURCE BEGIN -----")
            print(source)
            print(f"----- ERROR SOURCE END -----\n")
            return False
    
    def add_layer(self, rtc_name: str) -> bool:
        """Добавляет слой с RTC функцией"""
        
        if self.model is None or self.agent is None:
            print("❌ Сначала создайте модель и добавьте RTC функцию")
            return False
        
        if rtc_name not in self.rtc_sources:
            print(f"❌ RTC функция {rtc_name} не добавлена")
            return False
        
        try:
            layer = self.model.newLayer()
            rtc_function = self.agent.getFunction(rtc_name)
            layer.addAgentFunction(rtc_function)
            print(f"✅ Слой добавлен: {rtc_name}")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка создания слоя {rtc_name}: {e}")
            return False
    
    def create_simulation(self) -> bool:
        """Создает симуляцию из модели"""
        
        if self.model is None:
            print("❌ Сначала создайте модель")
            return False
        
        try:
            self.sim = pyflamegpu.CUDASimulation(self.model)
            print("✅ Симуляция создана")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка создания симуляции: {e}")
            return False
    
    def setup_basic_environment(self, frames: int = 100, days: int = 30):
        """Настраивает базовое окружение симуляции"""
        
        if self.sim is None:
            print("❌ Сначала создайте симуляцию")
            return False
        
        try:
            # Базовые скаляры
            self.sim.setEnvironmentPropertyUInt("version_date", 20273)  # 2025-07-04
            self.sim.setEnvironmentPropertyUInt("frames_total", frames)
            self.sim.setEnvironmentPropertyUInt("days_total", days)
            
            # Тестовые квоты
            mp4_mi8 = [10, 15, 20] + [5] * (days - 3)
            mp4_mi17 = [5, 8, 12] + [3] * (days - 3)
            
            self.sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8", mp4_mi8)
            self.sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi17", mp4_mi17)
            
            # MP5 тестовые данные (все по 60 минут)
            mp5_size = (days + 1) * frames
            mp5_data = [60] * mp5_size
            self.sim.setEnvironmentPropertyArrayUInt16("mp5_daily_hours", mp5_data)
            
            print(f"✅ Environment настроен: квоты mi8={mp4_mi8[:3]}..., mi17={mp4_mi17[:3]}...")
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка настройки Environment: {e}")
            return False
    
    def create_test_population(self, count: int = 20) -> bool:
        """Создает тестовую популяцию агентов"""
        
        if self.sim is None or self.agent is None:
            print("❌ Сначала создайте симуляцию")
            return False
        
        try:
            population = pyflamegpu.AgentVector(self.agent, count)
            
            for i in range(count):
                agent = population[i]
                agent.setVariableUInt("idx", i)
                agent.setVariableUInt("group_by", 1 if i % 2 == 0 else 2)  # Чередуем MI-8/MI-17
                agent.setVariableUInt("status_id", 2)  # Все в эксплуатации
                agent.setVariableUInt("sne", 1000 + i * 100)
                agent.setVariableUInt("ppr", 500 + i * 50)
                agent.setVariableUInt("ll", 50000)
                agent.setVariableUInt("oh", 10000)
                agent.setVariableUInt("repair_time", 180)
            
            self.sim.setPopulationData(population)
            
            print(f"✅ Тестовая популяция создана: {count} агентов")
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка создания популяции: {e}")
            return False
    
    def test_step(self, steps: int = 1) -> bool:
        """Тестирует выполнение шагов симуляции"""
        
        if self.sim is None:
            print("❌ Сначала создайте симуляцию")
            return False
        
        try:
            print(f"🧪 Тест {steps} шагов симуляции...")
            
            # Состояние до
            pop_before = pyflamegpu.AgentVector(self.agent)
            self.sim.getPopulationData(pop_before)
            
            status_before = {}
            for ag in pop_before:
                status = int(ag.getVariableUInt('status_id'))
                status_before[status] = status_before.get(status, 0) + 1
            
            print(f"  До: статусы {status_before}")
            
            # Выполнение шагов
            for step in range(steps):
                self.sim.step()
                print(f"  Шаг {step + 1}: OK")
            
            # Состояние после
            pop_after = pyflamegpu.AgentVector(self.agent)
            self.sim.getPopulationData(pop_after)
            
            status_after = {}
            for ag in pop_after:
                status = int(ag.getVariableUInt('status_id'))
                status_after[status] = status_after.get(status, 0) + 1
            
            print(f"  После: статусы {status_after}")
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка выполнения шагов: {e}")
            return False
    
    def get_info(self) -> str:
        """Возвращает информацию о текущей модели"""
        
        info = []
        info.append(f"RTC функций: {len(self.rtc_sources)}")
        
        for name in self.rtc_sources.keys():
            info.append(f"  - {name}")
        
        return "\n".join(info)


def create_parser() -> argparse.ArgumentParser:
    """Создает парсер аргументов"""
    
    parser = argparse.ArgumentParser(description='Step-by-step RTC builder')
    
    parser.add_argument('--frames', type=int, default=100, help='Количество кадров')
    parser.add_argument('--days', type=int, default=30, help='Количество дней')
    parser.add_argument('--agents', type=int, default=20, help='Количество тестовых агентов')
    parser.add_argument('--steps', type=int, default=1, help='Количество шагов симуляции')
    
    # Какие RTC добавлять
    parser.add_argument('--add-probe-mp5', action='store_true', help='Добавить rtc_probe_mp5')
    parser.add_argument('--add-begin-day', action='store_true', help='Добавить rtc_quota_begin_day')
    parser.add_argument('--add-status-2', action='store_true', help='Добавить rtc_status_2')
    parser.add_argument('--add-minimal-test', action='store_true', help='Добавить минимальную тестовую RTC')
    
    # Отладка NVRTC
    parser.add_argument('--jit-log', action='store_true', help='Включить подробный JIT лог')
    parser.add_argument('--seatbelts', choices=['on', 'off'], default='on', help='FLAME GPU seatbelts')
    parser.add_argument('--debug-source', action='store_true', help='Выводить исходный код RTC')
    
    return parser


def main():
    """Главная функция для пошагового тестирования"""
    
    if pyflamegpu is None:
        print("❌ pyflamegpu не установлен")
        return 1
    
    parser = create_parser()
    args = parser.parse_args()
    
    # Настройка отладки NVRTC
    if args.jit_log:
        os.environ['HL_JIT_LOG'] = '1'
        os.environ['PYTHONUNBUFFERED'] = '1'
        os.environ['CUDA_LAUNCH_BLOCKING'] = '1'
    
    if args.seatbelts == 'on':
        os.environ['FLAMEGPU_SEATBELTS'] = '1'
    else:
        os.environ['FLAMEGPU_SEATBELTS'] = '0'
    
    builder = StepByStepBuilder()
    
    print("🚀 Пошаговое тестирование RTC функций")
    print("=" * 40)
    
    if args.jit_log:
        print("🔧 Отладка NVRTC включена (HL_JIT_LOG=1)")
    if args.seatbelts == 'on':
        print("🔧 FLAME GPU seatbelts включены")
    
    # 1. Создание базовой модели
    if not builder.create_base_model(args.frames, args.days):
        return 1
    
    # 2. Добавление RTC функций по запросу
    debug_source = args.debug_source or args.jit_log
    
    if args.add_minimal_test:
        # Минимальная тестовая RTC для проверки компиляции
        minimal_source = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_minimal_test, flamegpu::MessageNone, flamegpu::MessageNone) {{
    static const unsigned int FRAMES = {args.frames}u;
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    if (idx >= FRAMES) return flamegpu::ALIVE;
    FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u);
    return flamegpu::ALIVE;
}}
        """
        if not builder.add_rtc_function("rtc_minimal_test", minimal_source, debug_source):
            return 1
        builder.add_layer("rtc_minimal_test")
    
    if args.add_probe_mp5:
        print("[WARN] rtc_probe_mp5 удалён: функциональность объединена в rtc_prepare_day")
        
    # Добавляем begin_day (сброс и чтение MP5)
    if args.add_begin_day:
        from rtc.begin_day import PrepareDayRTC
        source = PrepareDayRTC.get_source(args.frames, args.days)
        if not builder.add_rtc_function("rtc_prepare_day", source, debug_source):
            return 1
        builder.add_layer("rtc_prepare_day")
    
    if args.add_status_2:
        from rtc.status_2 import Status2RTC
        source = Status2RTC.get_source(args.frames, args.days)
        if not builder.add_rtc_function("rtc_status_2", source, debug_source):
            return 1
        builder.add_layer("rtc_status_2")
    
    # 3. Создание симуляции
    if not builder.create_simulation():
        return 1
    
    # 4. Настройка Environment
    if not builder.setup_basic_environment(args.frames, args.days):
        return 1
    
    # 5. Создание популяции
    if not builder.create_test_population(args.agents):
        return 1
    
    # 6. Тестирование шагов
    if not builder.test_step(args.steps):
        return 1
    
    print("\n📋 Информация о модели:")
    print(builder.get_info())
    
    print("\n✅ Пошаговое тестирование завершено успешно!")
    return 0


if __name__ == '__main__':
    sys.exit(main())
