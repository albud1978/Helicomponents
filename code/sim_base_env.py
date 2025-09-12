#!/usr/bin/env python3
"""
Базовая версия симуляции - только Environment без RTC
Для пошагового переноса ядер из старого кода
Дата: 2025-09-12
"""

import os
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Tuple

# Добавляем пути
sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent / 'utils'))

from sim.env_setup import EnvironmentSetup
from config_loader import get_clickhouse_client

try:
    import pyflamegpu
except ImportError:
    pyflamegpu = None


class BaseEnvironmentBuilder:
    """Базовый сборщик только Environment без RTC функций"""
    
    def __init__(self):
        self.env_setup = EnvironmentSetup()
    
    def build_env_only_model(self, frames_total: int, days_total: int) -> "pyflamegpu.ModelDescription":
        """Создает модель только с Environment (без агентов и RTC)"""
        
        if pyflamegpu is None:
            raise RuntimeError("pyflamegpu не установлен")
        
        model = pyflamegpu.ModelDescription("BaseEnvModel")
        env = model.Environment()
        
        # Только базовые скаляры
        env.newPropertyUInt("version_date", 0)
        env.newPropertyUInt("frames_total", 0)
        env.newPropertyUInt("days_total", 0)
        env.newPropertyUInt("frames_initial", 0)
        
        # MP4 квоты
        env.newPropertyArrayUInt32("mp4_ops_counter_mi8", [0] * days_total)
        env.newPropertyArrayUInt32("mp4_ops_counter_mi17", [0] * days_total)
        
        # MP5 налеты (с паддингом D+1)
        env.newPropertyArrayUInt16("mp5_daily_hours", [0] * ((days_total + 1) * frames_total))
        
        print(f"✅ Environment создан: {frames_total} кадров, {days_total} дней")
        
        return model
    
    def build_env_with_minimal_agent(self, frames_total: int, days_total: int) -> Tuple["pyflamegpu.ModelDescription", "pyflamegpu.AgentDescription"]:
        """Создает модель с Environment и минимальным агентом (без RTC)"""
        
        model = self.build_env_only_model(frames_total, days_total)
        
        # Минимальный агент
        agent = model.newAgent("component")
        
        # Только основные переменные
        basic_vars = [
            "idx", "psn", "partseqno_i", "group_by", "aircraft_number", "ac_type_mask",
            "status_id", "sne", "ppr", "ll", "oh", "repair_days",
            "daily_today_u32", "daily_next_u32", "ops_ticket"
        ]
        
        for var_name in basic_vars:
            agent.newVariableUInt(var_name, 0)
        
        print(f"✅ Агент создан с {len(basic_vars)} переменными")
        
        return model, agent
    
    def test_environment_loading(self) -> bool:
        """Тестирует загрузку данных в Environment"""
        
        try:
            print("🧪 Тестирование загрузки Environment...")
            
            # Подготовка данных
            env_data = self.env_setup.prepare_full_environment()
            
            frames_total = env_data['frames_total']
            days_total_full = env_data['days_total']
            days_total_test = min(days_total_full, 30)  # Ограничиваем для теста
            
            print(f"📊 Данные: {frames_total} кадров, {days_total_full} дней (тест: {days_total_test}), {env_data['mp3_count']} агентов")
            
            # Создание модели с полным размером данных
            model, agent = self.build_env_with_minimal_agent(frames_total, days_total_full)
            sim = pyflamegpu.CUDASimulation(model)
            
            # Применение данных (полных)
            self.env_setup.apply_to_simulation(sim, env_data)
            
            # Проверка что данные загрузились
            vd = sim.getEnvironmentPropertyUInt("version_date")
            ft = sim.getEnvironmentPropertyUInt("frames_total")
            dt = sim.getEnvironmentPropertyUInt("days_total")
            
            mp4_mi8 = sim.getEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8")
            mp5_arr = sim.getEnvironmentPropertyArrayUInt16("mp5_daily_hours")
            
            print(f"✅ Environment загружен:")
            print(f"  version_date: {vd}")
            print(f"  frames_total: {ft}")
            print(f"  days_total: {dt}")
            print(f"  mp4_mi8[0]: {mp4_mi8[0] if mp4_mi8 else 0}")
            print(f"  mp5 размер: {len(mp5_arr) if mp5_arr else 0}")
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка тестирования Environment: {e}")
            import traceback
            traceback.print_exc()
            return False


def create_argument_parser() -> argparse.ArgumentParser:
    """Создает парсер аргументов"""
    
    parser = argparse.ArgumentParser(description='Base Environment Builder')
    
    parser.add_argument('--test-env', action='store_true',
                       help='Тест загрузки Environment')
    parser.add_argument('--test-agent', action='store_true', 
                       help='Тест создания агентов')
    parser.add_argument('--frames', type=int, default=None,
                       help='Количество кадров (по умолчанию из данных)')
    parser.add_argument('--days', type=int, default=7,
                       help='Количество дней')
    
    return parser


def test_agent_creation(builder: BaseEnvironmentBuilder, frames: int = None, days: int = 7):
    """Тестирует создание агентов без RTC"""
    
    print("🧪 Тестирование создания агентов...")
    
    try:
        # Подготовка данных
        env_data = builder.env_setup.prepare_full_environment()
        frames_total = frames or env_data['frames_total']
        days_total = min(days, 30)
        
        # Создание модели
        model, agent = builder.build_env_with_minimal_agent(frames_total, days_total)
        sim = pyflamegpu.CUDASimulation(model)
        
        # Применение Environment
        builder.env_setup.apply_to_simulation(sim, env_data)
        
        # Создание популяции из MP3
        mp3_rows = env_data['mp3_rows']
        mp3_fields = env_data['mp3_fields']
        frames_index = env_data['frames_index']
        
        idx_map = {name: i for i, name in enumerate(mp3_fields)}
        population = pyflamegpu.AgentVector(agent, len(mp3_rows))
        
        status_counts = {}
        
        for i, row in enumerate(mp3_rows):
            ag = population[i]
            
            ac_num = int(row[idx_map['aircraft_number']] or 0)
            frame_idx = frames_index.get(ac_num, i % frames_total)
            status = int(row[idx_map['status_id']] or 0)
            
            ag.setVariableUInt("idx", frame_idx)
            ag.setVariableUInt("psn", int(row[idx_map['psn']] or 0))
            ag.setVariableUInt("partseqno_i", int(row[idx_map['partseqno_i']] or 0))
            ag.setVariableUInt("aircraft_number", ac_num)
            ag.setVariableUInt("group_by", int(row[idx_map.get('group_by', -1)] or 0))
            ag.setVariableUInt("ac_type_mask", int(row[idx_map['ac_type_mask']] or 0))
            ag.setVariableUInt("status_id", status)
            ag.setVariableUInt("sne", int(row[idx_map['sne']] or 0))
            ag.setVariableUInt("ppr", int(row[idx_map['ppr']] or 0))
            ag.setVariableUInt("ll", int(row[idx_map['ll']] or 0))
            ag.setVariableUInt("oh", int(row[idx_map['oh']] or 0))
            ag.setVariableUInt("repair_days", int(row[idx_map['repair_days']] or 0))
            
            status_counts[status] = status_counts.get(status, 0) + 1
        
        sim.setPopulationData(population)
        
        # Проверка популяции
        test_population = pyflamegpu.AgentVector(agent)
        sim.getPopulationData(test_population)
        
        print(f"✅ Популяция создана:")
        print(f"  Агентов: {len(test_population)}")
        print(f"  Статусы: {status_counts}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка создания агентов: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Главная функция"""
    
    if pyflamegpu is None:
        print("❌ pyflamegpu не установлен")
        return 1
    
    parser = create_argument_parser()
    args = parser.parse_args()
    
    builder = BaseEnvironmentBuilder()
    
    print("🚀 Базовый тест Environment (без RTC)")
    print("=" * 40)
    
    if args.test_env:
        success = builder.test_environment_loading()
        if not success:
            return 1
    
    if args.test_agent:
        success = test_agent_creation(builder, args.frames, args.days)
        if not success:
            return 1
    
    if not args.test_env and not args.test_agent:
        # По умолчанию тестируем Environment
        success = builder.test_environment_loading()
        if not success:
            return 1
    
    print("✅ Базовая архитектура готова для пошагового добавления RTC")
    return 0


if __name__ == '__main__':
    sys.exit(main())
