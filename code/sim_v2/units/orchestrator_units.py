#!/usr/bin/env python3
"""
Оркестратор симуляции агрегатов (Units)

Запуск:
    python orchestrator_units.py --version-date 2025-07-04 --steps 100

Дата: 05.01.2026
"""

import os
import sys
import time
import argparse
from datetime import date, datetime
from typing import Dict, Optional

# Пути
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
sys.path.insert(0, os.path.join(SCRIPT_DIR, '..'))
sys.path.insert(0, os.path.join(SCRIPT_DIR, '..', '..'))

# Настройка окружения
def setup_environment():
    """Настройка переменных окружения для CUDA/RTC"""
    project_root = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..', '..'))
    
    # RTC кэш
    rtc_cache = os.environ.get('FLAMEGPU2_RTC_CACHE', os.path.join(project_root, '.rtc_cache'))
    os.environ['FLAMEGPU2_RTC_CACHE'] = rtc_cache
    if not os.path.exists(rtc_cache):
        os.makedirs(rtc_cache)
    
    # CUDA PATH
    if 'CUDA_PATH' not in os.environ:
        os.environ['CUDA_PATH'] = '/home/albud/miniconda3/targets/x86_64-linux'
    
    print(f"⚡ RTC кэш: {rtc_cache}")
    print(f"🚀 CUDA_PATH: {os.environ.get('CUDA_PATH')}")


setup_environment()

try:
    import pyflamegpu as fg
except ImportError as e:
    print(f"❌ Ошибка импорта pyflamegpu: {e}")
    sys.exit(1)

from base_model_units import V2BaseModelUnits
from agent_population_units import AgentPopulationUnitsBuilder


class UnitsOrchestrator:
    """Оркестратор симуляции агрегатов"""
    
    def __init__(self, version_date: date, version_id: int = 1):
        self.version_date = version_date
        self.version_id = version_id
        
        self.base_model: Optional[V2BaseModelUnits] = None
        self.simulation: Optional[fg.CUDASimulation] = None
        self.env_data: Dict = {}
        
        # Тайминги
        self.timing = {
            'load': 0.0,
            'build': 0.0,
            'populate': 0.0,
            'simulate': 0.0,
            'total': 0.0
        }
    
    def load_data(self):
        """Загружает данные из ClickHouse"""
        t0 = time.time()
        
        print("=" * 60)
        print(f"📊 ЗАГРУЗКА ДАННЫХ АГРЕГАТОВ")
        print(f"   Дата версии: {self.version_date}")
        print("=" * 60)
        
        population_builder = AgentPopulationUnitsBuilder(self.version_date, self.version_id)
        self.env_data = population_builder.load_data()
        self.population_builder = population_builder
        
        # Загрузка dt планеров для интеграции
        try:
            from planer_dt_loader import load_planer_dt
            dt_array, ac_to_idx = load_planer_dt(str(self.version_date), self.version_id)
            if dt_array is not None:
                self.env_data['planer_dt_array'] = dt_array
                self.env_data['ac_to_idx'] = ac_to_idx
                print(f"   ✅ Загружено dt {len(ac_to_idx)} планеров")
        except Exception as e:
            print(f"   ⚠️ Не удалось загрузить dt планеров: {e}")
            self.env_data['planer_dt_array'] = None
            self.env_data['ac_to_idx'] = {}
        
        self.timing['load'] = time.time() - t0
        print(f"✅ Данные загружены за {self.timing['load']:.2f}с")
    
    def build_model(self):
        """Создаёт модель FLAME GPU
        
        Порядок модулей (как у планеров):
        1. states_stub — инициализация intent для reserve/serviceable/storage
        2. state_operations — вычисление intent + инкремент sne/ppr для operations
        3. state_repair — вычисление intent + инкремент repair_days для repair
        4. count — подсчёт агентов по состояниям
        5. request_replacement — запрос замены от неисправного агрегата
        6. fifo_assignment — FIFO-выбор замены из пула
        7. transition_ops — переходы 2→2, 2→4, 2→6
        8. transition_repair — переходы 4→4, 4→5
        9. transition_reserve — переходы 5→5, 5→2
        10. transition_serviceable — переходы 3→3, 3→2
        11. return_to_pool — возврат в FIFO-очередь после ремонта
        12. mp2_writer — запись в MP2
        """
        t0 = time.time()
        
        print("\n" + "=" * 60)
        print("🔧 ПОСТРОЕНИЕ МОДЕЛИ (12 модулей как у планеров)")
        print("=" * 60)
        
        self.base_model = V2BaseModelUnits()
        model = self.base_model.create_model(self.env_data)
        agent = self.base_model.agent
        max_frames = int(self.env_data.get('units_frames_total', 12000))
        max_days = int(self.env_data.get('days_total_u16', 3650))
        
        modules_ok = 0
        modules_failed = 0
        
        # 1. states_stub — инициализация intent для не-operations состояний
        try:
            import rtc_units_states_stub
            rtc_units_states_stub.register_rtc(model, agent)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_states_stub: {e}")
            modules_failed += 1
        
        # 2. state_operations — intent для operations
        try:
            import rtc_units_state_operations
            rtc_units_state_operations.register_rtc(model, agent)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_state_operations: {e}")
            modules_failed += 1
        
        # 2b. increment — чтение dt от планера и инкремент sne/ppr
        try:
            import rtc_units_increment
            rtc_units_increment.register_rtc(model, agent, max_days)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_increment: {e}")
            modules_failed += 1
        
        # 3. state_repair — intent + инкремент для repair
        try:
            import rtc_units_state_repair
            rtc_units_state_repair.register_rtc(model, agent)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_state_repair: {e}")
            modules_failed += 1
        
        # 4. count — подсчёт агентов
        try:
            import rtc_units_count
            rtc_units_count.register_rtc(model, agent)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_count: {e}")
            modules_failed += 1
        
        # 5-6. FIFO модули — семифазная архитектура
        try:
            import rtc_units_fifo
            rtc_units_fifo.register_rtc(model, agent, max_frames, max_days)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_fifo: {e}")
            modules_failed += 1
        
        # 6b. spawn — динамический spawn (TODO: требует резервных слотов)
        # Пока отключен — дефицит будет отслеживаться через накопление запросов
        # try:
        #     import rtc_units_spawn
        #     rtc_units_spawn.register_rtc(model, agent)
        #     modules_ok += 1
        # except Exception as e:
        #     print(f"  ❌ units_spawn: {e}")
        #     modules_failed += 1
        
        # 7. transition_ops — переходы из operations
        try:
            import rtc_units_transition_ops
            rtc_units_transition_ops.register_rtc(model, agent)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_transition_ops: {e}")
            modules_failed += 1
        
        # 8. transition_repair (уже в state_repair, но отдельно для 4→5)
        # Переходы 4→4, 4→5 уже включены в rtc_units_state_repair
        
        # 9. transition_reserve — переходы из reserve
        try:
            import rtc_units_transition_reserve
            rtc_units_transition_reserve.register_rtc(model, agent, max_frames)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_transition_reserve: {e}")
            modules_failed += 1
        
        # 10. transition_serviceable — переходы из serviceable
        try:
            import rtc_units_transition_serviceable
            rtc_units_transition_serviceable.register_rtc(model, agent, max_frames)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_transition_serviceable: {e}")
            modules_failed += 1
        
        # 11. return_to_pool — включён в fifo_phase2 (reserve → serviceable)
        
        # 12. mp2_writer — запись результатов
        try:
            import rtc_units_mp2_writer
            rtc_units_mp2_writer.register_rtc(model, agent, max_frames, max_days)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_mp2_writer: {e}")
            modules_failed += 1
        
        self.timing['build'] = time.time() - t0
        print(f"✅ Модель построена за {self.timing['build']:.2f}с")
        print(f"   Модулей OK: {modules_ok}, Failed: {modules_failed}")
    
    def populate_agents(self):
        """Инициализирует агентов"""
        t0 = time.time()
        
        print("\n" + "=" * 60)
        print("👥 ИНИЦИАЛИЗАЦИЯ АГЕНТОВ")
        print("=" * 60)
        
        self.simulation = fg.CUDASimulation(self.base_model.model)
        
        self.population_builder.populate_agents(
            self.simulation, 
            self.base_model.agent,
            self.env_data
        )
        
        # Инициализация FIFO MacroProperty
        self._init_fifo_macroproperty()
        
        self.timing['populate'] = time.time() - t0
        print(f"✅ Агенты инициализированы за {self.timing['populate']:.2f}с")
    
    def _init_fifo_macroproperty(self):
        """Инициализирует MacroProperty для FIFO-очереди и dt планеров"""
        queue_heads = self.env_data.get('queue_heads', {})
        queue_tails = self.env_data.get('queue_tails', {})
        
        # Получаем доступ к MacroProperty через HostFunction (нельзя напрямую)
        # Будет инициализировано в первом step через host function
        print(f"   FIFO очереди будут инициализированы в первом step")
        
        # Инициализация dt планеров
        planer_dt = self.env_data.get('planer_dt_array')
        ac_to_idx = self.env_data.get('ac_to_idx', {})
        
        if planer_dt is not None and len(ac_to_idx) > 0:
            # Записываем через setEnvironmentMacroProperty (если поддерживается)
            # Альтернатива — InitFunction
            print(f"   📊 dt планеров: {len(ac_to_idx)} маппингов загружено")
            # Сохраняем для InitFunction
            self._planer_dt_array = planer_dt
            self._ac_to_idx = ac_to_idx
        else:
            self._planer_dt_array = None
            self._ac_to_idx = {}
    
    def run(self, steps: int = 100):
        """Запускает симуляцию"""
        t0 = time.time()
        
        print("\n" + "=" * 60)
        print(f"🚀 ЗАПУСК СИМУЛЯЦИИ НА {steps} ШАГОВ")
        print("=" * 60)
        
        step_times = []
        
        for step in range(steps):
            step_t0 = time.time()
            self.simulation.step()
            step_time = time.time() - step_t0
            step_times.append(step_time)
            
            # Прогресс каждые 100 шагов
            if (step + 1) % 100 == 0:
                avg_time = sum(step_times[-100:]) / min(100, len(step_times))
                print(f"  [День {step + 1:4d}] avg={avg_time*1000:.1f}мс")
        
        self.timing['simulate'] = time.time() - t0
        
        # Статистика
        if step_times:
            avg_step = sum(step_times) / len(step_times)
            max_step = max(step_times)
            min_step = min(step_times)
            
            print(f"\n✅ Симуляция завершена за {self.timing['simulate']:.2f}с")
            print(f"   Среднее время шага: {avg_step*1000:.2f}мс")
            print(f"   Мин/Макс: {min_step*1000:.2f}мс / {max_step*1000:.2f}мс")
    
    def print_summary(self):
        """Выводит итоговую статистику"""
        self.timing['total'] = sum([
            self.timing['load'],
            self.timing['build'],
            self.timing['populate'],
            self.timing['simulate']
        ])
        
        print("\n" + "=" * 60)
        print("📊 ИТОГОВАЯ СТАТИСТИКА")
        print("=" * 60)
        print(f"  Загрузка данных:     {self.timing['load']:.2f}с")
        print(f"  Построение модели:   {self.timing['build']:.2f}с")
        print(f"  Инициализация:       {self.timing['populate']:.2f}с")
        print(f"  Симуляция:           {self.timing['simulate']:.2f}с")
        print(f"  ─────────────────────────")
        print(f"  ВСЕГО:               {self.timing['total']:.2f}с")


def parse_args():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(description='Симуляция агрегатов (Units)')
    parser.add_argument('--version-date', type=str, required=True,
                       help='Дата версии данных (YYYY-MM-DD)')
    parser.add_argument('--version-id', type=int, default=1,
                       help='ID версии данных (по умолчанию 1)')
    parser.add_argument('--steps', type=int, default=100,
                       help='Количество шагов симуляции (по умолчанию 100)')
    parser.add_argument('--export', action='store_true',
                       help='Экспортировать результаты в ClickHouse')
    parser.add_argument('--drop-table', action='store_true',
                       help='Удалить таблицу перед экспортом')
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Парсим дату
    version_date = datetime.strptime(args.version_date, '%Y-%m-%d').date()
    
    print("=" * 60)
    print("🚁 СИМУЛЯЦИЯ АГРЕГАТОВ (UNITS) V2")
    print(f"   Дата версии: {version_date}")
    print(f"   Шагов: {args.steps}")
    print("=" * 60)
    
    orchestrator = UnitsOrchestrator(version_date, args.version_id)
    
    try:
        orchestrator.load_data()
        orchestrator.build_model()
        orchestrator.populate_agents()
        orchestrator.run(args.steps)
        orchestrator.print_summary()
        
        # Экспорт в ClickHouse
        if args.export:
            print("\n" + "=" * 60)
            print("📤 ЭКСПОРТ В CLICKHOUSE")
            print("=" * 60)
            try:
                from mp2_exporter_units import export_mp2_to_clickhouse
                export_mp2_to_clickhouse(
                    orchestrator.simulation,
                    orchestrator.env_data,
                    version_date,
                    args.version_id,
                    drop_table=args.drop_table,
                    agent_desc=orchestrator.base_model.agent
                )
            except Exception as e:
                print(f"   ⚠️ Ошибка экспорта: {e}")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())

