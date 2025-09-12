#!/usr/bin/env python3
"""
Упрощенный сборщик для пошагового добавления RTC функций
Начинаем с Environment, затем добавляем RTC по одной
Дата: 2025-09-12
"""

from typing import Dict, List, Optional, Any, Tuple
import os

try:
    import pyflamegpu
except ImportError:
    pyflamegpu = None

from .env_setup import EnvironmentSetup


class SimpleBuilder:
    """Упрощенный сборщик для пошагового добавления RTC"""
    
    def __init__(self):
        self.env_setup = EnvironmentSetup()
        self.registered_rtc = {}
    
    def build_env_only(self, frames_total: int, days_total: int) -> "pyflamegpu.ModelDescription":
        """Создает модель только с Environment (без агентов и RTC)"""
        
        if pyflamegpu is None:
            raise RuntimeError("pyflamegpu не установлен")
        
        model = pyflamegpu.ModelDescription("SimpleEnvModel")
        env = model.Environment()
        
        # Базовые скаляры
        env.newPropertyUInt("version_date", 0)
        env.newPropertyUInt("frames_total", 0)
        env.newPropertyUInt("days_total", 0)
        env.newPropertyUInt("frames_initial", 0)
        
        # MP4 квоты по дням
        env.newPropertyArrayUInt32("mp4_ops_counter_mi8", [0] * days_total)
        env.newPropertyArrayUInt32("mp4_ops_counter_mi17", [0] * days_total)
        
        # MP5 налеты (с паддингом D+1)
        env.newPropertyArrayUInt16("mp5_daily_hours", [0] * ((days_total + 1) * frames_total))
        
        # MP1 справочники (минимальные)
        env.newPropertyArrayUInt32("mp1_br_mi8", [0])
        env.newPropertyArrayUInt32("mp1_br_mi17", [0])
        env.newPropertyArrayUInt32("mp1_repair_time", [0])
        env.newPropertyArrayUInt32("mp1_partout_time", [0])
        env.newPropertyArrayUInt32("mp1_assembly_time", [0])
        
        print(f"✅ Environment создан: {frames_total} кадров, {days_total} дней")
        
        return model
    
    def build_with_agent(self, frames_total: int, days_total: int) -> Tuple["pyflamegpu.ModelDescription", "pyflamegpu.AgentDescription"]:
        """Создает модель с Environment и минимальным агентом"""
        
        model = self.build_env_only(frames_total, days_total)
        
        # Минимальный агент
        agent = model.newAgent("component")
        
        # Основные переменные агента
        basic_vars = [
            "idx", "psn", "partseqno_i", "group_by", "aircraft_number", "ac_type_mask",
            "status_id", "sne", "ppr", "ll", "oh", "repair_days", "repair_time",
            "assembly_time", "partout_time", "br",
            "daily_today_u32", "daily_next_u32", "ops_ticket", "intent_flag",
            "active_trigger", "assembly_trigger", "partout_trigger"
        ]
        
        for var_name in basic_vars:
            agent.newVariableUInt(var_name, 0)
        
        print(f"✅ Агент создан с {len(basic_vars)} переменными")
        
        return model, agent
    
    def add_single_rtc(self, agent: "pyflamegpu.AgentDescription", rtc_name: str, 
                      rtc_source: str) -> bool:
        """Добавляет одну RTC функцию в агента"""
        
        try:
            agent.newRTCFunction(rtc_name, rtc_source)
            self.registered_rtc[rtc_name] = rtc_source
            print(f"✅ RTC добавлена: {rtc_name}")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка добавления RTC {rtc_name}: {e}")
            print(f"\n===== NVRTC ERROR =====")
            print(f"{e}")
            print(f"----- SOURCE BEGIN -----")
            print(rtc_source)
            print(f"----- SOURCE END -----\n")
            return False
    
    def add_simple_layer(self, model: "pyflamegpu.ModelDescription", 
                        agent: "pyflamegpu.AgentDescription", rtc_name: str):
        """Добавляет простой слой с одной RTC функцией"""
        
        if rtc_name not in self.registered_rtc:
            print(f"❌ RTC функция {rtc_name} не зарегистрирована")
            return False
        
        try:
            layer = model.newLayer()
            rtc_function = agent.getFunction(rtc_name)
            layer.addAgentFunction(rtc_function)
            print(f"✅ Слой добавлен: {rtc_name}")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка создания слоя {rtc_name}: {e}")
            return False
    
    def create_simulation(self, model: "pyflamegpu.ModelDescription") -> "pyflamegpu.CUDASimulation":
        """Создает симуляцию"""
        return pyflamegpu.CUDASimulation(model)
    
    def create_test_population(self, agent_desc: "pyflamegpu.AgentDescription", 
                              env_data: Dict, max_agents: int = 100) -> "pyflamegpu.AgentVector":
        """Создает тестовую популяцию (ограниченную для отладки)"""
        
        mp3_rows = env_data['mp3_rows']
        mp3_fields = env_data['mp3_fields']
        frames_index = env_data['frames_index']
        
        # Ограничиваем количество агентов для быстрого тестирования
        test_rows = mp3_rows[:max_agents]
        
        idx_map = {name: i for i, name in enumerate(mp3_fields)}
        population = pyflamegpu.AgentVector(agent_desc, len(test_rows))
        
        for i, row in enumerate(test_rows):
            agent = population[i]
            
            ac_num = int(row[idx_map['aircraft_number']] or 0)
            frame_idx = frames_index.get(ac_num, i)
            
            agent.setVariableUInt("idx", frame_idx)
            agent.setVariableUInt("psn", int(row[idx_map['psn']] or 0))
            agent.setVariableUInt("partseqno_i", int(row[idx_map['partseqno_i']] or 0))
            agent.setVariableUInt("aircraft_number", ac_num)
            agent.setVariableUInt("group_by", int(row[idx_map.get('group_by', -1)] or 0))
            agent.setVariableUInt("ac_type_mask", int(row[idx_map['ac_type_mask']] or 0))
            agent.setVariableUInt("status_id", int(row[idx_map['status_id']] or 0))
            agent.setVariableUInt("sne", int(row[idx_map['sne']] or 0))
            agent.setVariableUInt("ppr", int(row[idx_map['ppr']] or 0))
            agent.setVariableUInt("ll", int(row[idx_map['ll']] or 0))
            agent.setVariableUInt("oh", int(row[idx_map['oh']] or 0))
            agent.setVariableUInt("repair_days", int(row[idx_map['repair_days']] or 0))
        
        print(f"✅ Тестовая популяция: {len(test_rows)} агентов")
        
        return population


def main():
    """Главная функция для тестирования базового Environment"""
    
    if pyflamegpu is None:
        print("❌ pyflamegpu не установлен")
        return 1
    
    parser = create_argument_parser()
    args = parser.parse_args()
    
    builder = BaseEnvironmentBuilder()
    
    print("🚀 Тестирование базового Environment")
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
    
    print("✅ Базовая архитектура готова для добавления RTC")
    return 0


if __name__ == '__main__':
    sys.exit(main())


