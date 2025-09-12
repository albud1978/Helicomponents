#!/usr/bin/env python3
"""
Полноценный тестер RTC функций с таймингами и экспортом в базу
Тестирует каждую RTC функцию вплоть до 3650 дней с полной выгрузкой
Дата: 2025-09-12
"""

import os
import sys
import argparse
import time
from pathlib import Path
from typing import Dict, List, Any

sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent / 'utils'))

from sim.env_setup import EnvironmentSetup
from sim.timing_system import TimingCollector, create_timing_context
from config_loader import get_clickhouse_client

try:
    import pyflamegpu
except ImportError:
    pyflamegpu = None


class FullRTCTester:
    """Полноценный тестер RTC функций с реальными данными"""
    
    def __init__(self):
        self.env_setup = EnvironmentSetup()
        self.client = get_clickhouse_client()
        self.timing = TimingCollector()
    
    def test_rtc_function(self, rtc_name: str, rtc_source: str, 
                         test_days: int = 30, export_to_db: bool = False) -> bool:
        """Тестирует одну RTC функцию с полным циклом"""
        
        if pyflamegpu is None:
            print("❌ pyflamegpu не установлен")
            return False
        
        print(f"🧪 Полный тест RTC функции: {rtc_name}")
        print(f"📅 Период: {test_days} дней, Экспорт в БД: {export_to_db}")
        print("=" * 50)
        
        try:
            # === ЭТАП 1: Подготовка данных ===
            with create_timing_context(self.timing, "load_gpu"):
                env_data = self.env_setup.prepare_environment_for_period("custom", test_days)
                frames_total = env_data['frames_total']
                days_total = env_data['days_total']
            
            print(f"📊 Данные: {frames_total} кадров, {days_total} дней, {env_data['mp3_count']} агентов")
            
            # === ЭТАП 2: Создание модели ===
            with create_timing_context(self.timing, "compile_rtc"):
                model = self._create_test_model(frames_total, days_total, rtc_name, rtc_source)
                sim = pyflamegpu.CUDASimulation(model)
                
                # Применение Environment
                self.env_setup.apply_to_simulation(sim, env_data)
            
            # === ЭТАП 3: Создание популяции ===
            with create_timing_context(self.timing, "population"):
                agent_desc = model.getAgent("component")
                population = self._create_real_population(agent_desc, env_data)
                sim.setPopulationData(population)
            
            # === ЭТАП 4: Выполнение симуляции ===
            print(f"🚀 Запуск симуляции на {days_total} дней...")
            
            with create_timing_context(self.timing, "sim_gpu"):
                for day in range(days_total):
                    step_start = time.perf_counter()
                    sim.step()
                    step_time = (time.perf_counter() - step_start) * 1000
                    self.timing.add_step_time(step_time)
                    
                    if day % 365 == 0 or day < 10 or day == days_total - 1:
                        print(f"  День {day}: {step_time:.2f} мс")
            
            # === ЭТАП 5: Анализ результатов ===
            with create_timing_context(self.timing, "cpu_log"):
                final_population = pyflamegpu.AgentVector(agent_desc)
                sim.getPopulationData(final_population)
                
                results = self._analyze_results(final_population, rtc_name)
            
            # === ЭТАП 6: Экспорт в базу ===
            if export_to_db:
                with create_timing_context(self.timing, "db_insert"):
                    self._export_to_database(results, env_data, rtc_name)
            
            # === ЭТАП 7: Финальные метрики ===
            self.timing.set_metadata(days_total, len(final_population), 1)
            self.timing.get_metrics().print_summary()
            
            print(f"\n✅ Тест {rtc_name} завершен успешно")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка тестирования {rtc_name}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _create_test_model(self, frames: int, days: int, rtc_name: str, rtc_source: str):
        """Создает тестовую модель с одной RTC функцией"""
        
        model = pyflamegpu.ModelDescription("RTCFullTest")
        env = model.Environment()
        
        # Базовые скаляры
        env.newPropertyUInt("version_date", 0)
        env.newPropertyUInt("frames_total", 0)
        env.newPropertyUInt("days_total", 0)
        env.newPropertyUInt("frames_initial", 0)
        env.newPropertyUInt("export_phase", 0)
        
        # MP4 квоты и спавн
        env.newPropertyArrayUInt32("mp4_ops_counter_mi8", [0] * days)
        env.newPropertyArrayUInt32("mp4_ops_counter_mi17", [0] * days)
        env.newPropertyArrayUInt32("mp4_new_counter_mi17_seed", [0] * days)
        env.newPropertyArrayUInt32("month_first_u32", [0] * days)
        
        # MP5 налеты
        env.newPropertyArrayUInt16("mp5_daily_hours", [0] * ((days + 1) * frames))
        
        # MP1 справочники (базовые размеры)
        env.newPropertyArrayUInt32("mp1_br_mi8", [0] * 1000)
        env.newPropertyArrayUInt32("mp1_br_mi17", [0] * 1000)
        env.newPropertyArrayUInt32("mp1_repair_time", [0] * 1000)
        env.newPropertyArrayUInt32("mp1_partout_time", [0] * 1000)
        env.newPropertyArrayUInt32("mp1_assembly_time", [0] * 1000)
        env.newPropertyArrayUInt32("mp1_oh_mi8", [0] * 1000)
        env.newPropertyArrayUInt32("mp1_oh_mi17", [0] * 1000)
        
        # MP3 агентные данные (базовые размеры)
        env.newPropertyArrayUInt32("mp3_psn", [0] * 10000)
        env.newPropertyArrayUInt32("mp3_aircraft_number", [0] * 10000)
        env.newPropertyArrayUInt32("mp3_ac_type_mask", [0] * 10000)
        env.newPropertyArrayUInt32("mp3_status_id", [0] * 10000)
        env.newPropertyArrayUInt32("mp3_sne", [0] * 10000)
        env.newPropertyArrayUInt32("mp3_ppr", [0] * 10000)
        env.newPropertyArrayUInt32("mp3_repair_days", [0] * 10000)
        env.newPropertyArrayUInt32("mp3_ll", [0] * 10000)
        env.newPropertyArrayUInt32("mp3_oh", [0] * 10000)
        env.newPropertyArrayUInt32("mp3_mfg_date_days", [0] * 10000)
        
        # Агент с полным набором переменных
        agent = model.newAgent("component")
        
        # Все переменные из реальной модели
        agent_vars = [
            "idx", "psn", "partseqno_i", "group_by", "aircraft_number", "ac_type_mask",
            "status_id", "sne", "ppr", "ll", "oh", "br", "repair_days", "repair_time",
            "assembly_time", "partout_time", "mfg_date",
            "daily_today_u32", "daily_next_u32", "ops_ticket", "intent_flag",
            "active_trigger", "assembly_trigger", "partout_trigger",
            "s6_started", "s6_days"  # Для status_6
        ]
        
        for var_name in agent_vars:
            agent.newVariableUInt(var_name, 0)
        
        # Добавляем RTC функцию
        agent.newRTCFunction(rtc_name, rtc_source)
        
        # Создаем слой
        layer = model.newLayer()
        layer.addAgentFunction(agent.getFunction(rtc_name))
        
        return model
    
    def _create_real_population(self, agent_desc, env_data: Dict):
        """Создает популяцию из реальных данных MP3"""
        
        mp3_rows = env_data['mp3_rows']
        mp3_fields = env_data['mp3_fields']
        frames_index = env_data['frames_index']
        
        idx_map = {name: i for i, name in enumerate(mp3_fields)}
        population = pyflamegpu.AgentVector(agent_desc, len(mp3_rows))
        
        for i, row in enumerate(mp3_rows):
            agent = population[i]
            
            # Базовые поля из MP3
            ac_num = int(row[idx_map['aircraft_number']] or 0)
            frame_idx = frames_index.get(ac_num, i % env_data['frames_total'])
            
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
            
            # Для status_6: если агент уже в статусе 6, устанавливаем s6_started=1
            if int(row[idx_map['status_id']] or 0) == 6:
                agent.setVariableUInt("s6_started", 1)
                agent.setVariableUInt("s6_days", 0)  # Начинаем счетчик
            else:
                agent.setVariableUInt("s6_started", 0)
                agent.setVariableUInt("s6_days", 0)
            
            # Остальные поля
            agent.setVariableUInt("daily_today_u32", 0)
            agent.setVariableUInt("daily_next_u32", 0)
            agent.setVariableUInt("ops_ticket", 0)
            agent.setVariableUInt("intent_flag", 0)
            agent.setVariableUInt("active_trigger", 0)
            agent.setVariableUInt("assembly_trigger", 0)
            agent.setVariableUInt("partout_trigger", 0)
        
        return population
    
    def _analyze_results(self, population, rtc_name: str) -> Dict[str, Any]:
        """Анализирует результаты симуляции"""
        
        # Подсчет статусов
        status_counts = {}
        for agent in population:
            status = int(agent.getVariableUInt('status_id'))
            status_counts[status] = status_counts.get(status, 0) + 1
        
        # Специальный анализ для status_6
        s6_analysis = {}
        if rtc_name == "rtc_status_6":
            s6_agents = [ag for ag in population if int(ag.getVariableUInt('status_id')) == 6]
            s6_analysis = {
                "s6_count": len(s6_agents),
                "s6_started_count": sum(1 for ag in s6_agents if int(ag.getVariableUInt('s6_started')) == 1),
                "s6_days_max": max([int(ag.getVariableUInt('s6_days')) for ag in s6_agents]) if s6_agents else 0,
                "partout_triggered": sum(1 for ag in s6_agents if int(ag.getVariableUInt('partout_trigger')) == 1)
            }
        
        results = {
            "rtc_name": rtc_name,
            "total_agents": len(population),
            "status_counts": status_counts,
            "s6_analysis": s6_analysis
        }
        
        # Печать результатов
        print(f"\n📊 Результаты симуляции {rtc_name}:")
        print(f"  Агентов: {len(population)}")
        
        status_names = {1: "Неактивно", 2: "Эксплуатация", 3: "Исправен", 
                       4: "Ремонт", 5: "Резерв", 6: "Хранение"}
        
        for status_id in sorted(status_counts.keys()):
            count = status_counts[status_id]
            name = status_names.get(status_id, f"Статус_{status_id}")
            print(f"    {status_id} ({name}): {count}")
        
        if s6_analysis:
            print(f"  📋 Анализ статуса 6:")
            print(f"    Агентов в хранении: {s6_analysis['s6_count']}")
            print(f"    С активным счетчиком: {s6_analysis['s6_started_count']}")
            print(f"    Макс дней в хранении: {s6_analysis['s6_days_max']}")
            print(f"    Partout триггеров: {s6_analysis['partout_triggered']}")
        
        return results
    
    def _export_to_database(self, results: Dict, env_data: Dict, rtc_name: str):
        """Экспортирует результаты в базу данных"""
        
        table_name = f"rtc_test_results_{rtc_name.replace('rtc_', '')}"
        
        # Создание таблицы если не существует
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            test_date       Date,
            rtc_name        String,
            days_simulated  UInt16,
            agents_total    UInt32,
            status_1        UInt32,
            status_2        UInt32,
            status_3        UInt32,
            status_4        UInt32,
            status_5        UInt32,
            status_6        UInt32,
            s6_started      UInt32,
            s6_days_max     UInt16,
            partout_triggered UInt32,
            load_gpu_ms     Float32,
            compile_rtc_ms  Float32,
            sim_gpu_ms      Float32,
            cpu_log_ms      Float32,
            db_insert_ms    Float32,
            total_ms        Float32
        )
        ENGINE = MergeTree()
        ORDER BY (test_date, rtc_name, days_simulated)
        """
        
        try:
            self.client.execute(ddl)
            print(f"✅ Таблица {table_name} готова")
        except Exception as e:
            print(f"⚠️ Предупреждение создания таблицы: {e}")
        
        # Подготовка данных для вставки
        from datetime import date
        
        metrics = self.timing.get_metrics()
        status_counts = results["status_counts"]
        s6_analysis = results.get("s6_analysis", {})
        
        row_data = (
            date.today(),
            rtc_name,
            env_data['days_total'],
            results["total_agents"],
            status_counts.get(1, 0),
            status_counts.get(2, 0),
            status_counts.get(3, 0),
            status_counts.get(4, 0),
            status_counts.get(5, 0),
            status_counts.get(6, 0),
            s6_analysis.get("s6_started_count", 0),
            s6_analysis.get("s6_days_max", 0),
            s6_analysis.get("partout_triggered", 0),
            metrics.load_gpu_ms,
            metrics.compile_rtc_ms,
            metrics.sim_gpu_ms,
            metrics.cpu_log_ms,
            metrics.db_insert_ms,
            metrics.total_ms()
        )
        
        # Вставка данных
        insert_sql = f"""
        INSERT INTO {table_name} VALUES
        """
        
        try:
            self.client.execute(insert_sql, [row_data])
            print(f"✅ Результаты экспортированы в {table_name}")
        except Exception as e:
            print(f"❌ Ошибка экспорта в БД: {e}")


def create_parser():
    """Создает парсер аргументов"""
    
    parser = argparse.ArgumentParser(description='Полный тестер RTC функций')
    
    parser.add_argument('--rtc', required=True, 
                       choices=['prepare_day', 'status_6', 'status_4', 'status_2'],
                       help='RTC функция для тестирования')
    parser.add_argument('--days', type=int, default=30,
                       help='Количество дней симуляции')
    parser.add_argument('--export-db', action='store_true',
                       help='Экспортировать результаты в ClickHouse')
    parser.add_argument('--seatbelts', choices=['on', 'off'], default='on',
                       help='FLAME GPU seatbelts')
    parser.add_argument('--jit-log', action='store_true',
                       help='Подробный JIT лог')
    
    return parser


def main():
    """Главная функция"""
    
    if pyflamegpu is None:
        print("❌ pyflamegpu не установлен")
        return 1
    
    parser = create_parser()
    args = parser.parse_args()
    
    # Настройка отладки
    if args.jit_log:
        os.environ['HL_JIT_LOG'] = '1'
        os.environ['PYTHONUNBUFFERED'] = '1'
        os.environ['CUDA_LAUNCH_BLOCKING'] = '1'
    
    os.environ['FLAMEGPU_SEATBELTS'] = '1' if args.seatbelts == 'on' else '0'
    
    # Получение RTC функции
    rtc_map = {
        "prepare_day": ("rtc.begin_day", "PrepareDayRTC"),
        "status_6": ("rtc.status_6", "Status6RTC"),
        "status_4": ("rtc.status_4", "Status4RTC"),
        "status_2": ("rtc.status_2", "Status2RTC"),
    }
    
    if args.rtc not in rtc_map:
        print(f"❌ Неизвестная RTC функция: {args.rtc}")
        return 1
    
    module_name, class_name = rtc_map[args.rtc]
    
    try:
        # Загрузка RTC модуля
        module = __import__(module_name, fromlist=[class_name])
        rtc_class = getattr(module, class_name)
        
        # Генерация исходного кода
        # Используем большие размеры для реального теста
        rtc_source = rtc_class.get_source(frames=300, days=args.days)
        
        # Запуск теста
        tester = FullRTCTester()
        success = tester.test_rtc_function(
            rtc_class.NAME, rtc_source, args.days, args.export_db
        )
        
        return 0 if success else 1
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
