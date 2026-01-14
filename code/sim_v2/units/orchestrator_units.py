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

# Константы
MAX_PLANERS = 400  # Максимум планеров для assembly


class UnitsOrchestrator:
    """Оркестратор симуляции агрегатов"""
    
    def __init__(self, version_date: date, version_id: int = 1):
        self.version_date = version_date
        self.version_id = version_id
        
        self.base_model: Optional[V2BaseModelUnits] = None
        self.simulation: Optional[fg.CUDASimulation] = None
        self.env_data: Dict = {}
        self.mp2_drain_fn = None  # HostFunction для дренажа MP2
        
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
        
        # Загрузка dt, assembly_trigger, planer_in_ops, planer_type и planer_in_ops_history планеров
        try:
            from planer_dt_loader import load_planer_data
            dt_array, assembly_array, ac_to_idx, planer_in_ops, planer_type, planer_in_ops_history = load_planer_data(str(self.version_date), self.version_id)
            if dt_array is not None:
                self.env_data['planer_dt_array'] = dt_array
                self.env_data['ac_to_idx'] = ac_to_idx
                self.env_data['planer_assembly_array'] = assembly_array
                self.env_data['planer_in_ops'] = planer_in_ops
                self.env_data['planer_type'] = planer_type
                self.env_data['planer_in_ops_history'] = planer_in_ops_history
                print(f"   ✅ Загружено dt {len(ac_to_idx)} планеров, {len(planer_in_ops)} в ops")
                if assembly_array is not None:
                    print(f"   ✅ Загружено assembly_trigger: {assembly_array.sum()} записей")
                if planer_type:
                    mi8 = sum(1 for t in planer_type.values() if t == 1)
                    mi17 = sum(1 for t in planer_type.values() if t == 2)
                    print(f"   ✅ Загружено planer_type: Mi-8={mi8}, Mi-17={mi17}")
                if planer_in_ops_history is not None:
                    ops_history_sum = planer_in_ops_history.sum()
                    print(f"   ✅ Загружено planer_in_ops_history: {ops_history_sum:,} записей")
        except Exception as e:
            print(f"   ⚠️ Не удалось загрузить данные планеров: {e}")
            import traceback
            traceback.print_exc()
            self.env_data['planer_dt_array'] = None
            self.env_data['ac_to_idx'] = {}
            self.env_data['planer_assembly_array'] = None
            self.env_data['planer_type'] = {}
            self.env_data['planer_in_ops_history'] = None
        
        # Количество компонентов на планер по group_by (для assembly)
        # Данные из анализа: group_by=6 (лопасти) = 5, остальные в основном 1
        comp_numbers = [0] * 50  # По умолчанию 0
        # Основные группы с количеством > 1:
        comp_numbers[3] = 2   # group_by=3: ~1.5 → 2
        comp_numbers[4] = 2   # group_by=4: ~1.7 → 2
        comp_numbers[5] = 1   # group_by=5: 1
        comp_numbers[6] = 5   # group_by=6 (лопасти НВ): 5
        comp_numbers[22] = 3  # group_by=22: ~2.9 → 3
        comp_numbers[24] = 4  # group_by=24: ~3.5 → 4
        comp_numbers[35] = 2  # group_by=35: ~1.9 → 2
        comp_numbers[40] = 2  # group_by=40: ~1.6 → 2
        comp_numbers[41] = 2  # group_by=41: ~1.7 → 2
        comp_numbers[42] = 2  # group_by=42: ~1.7 → 2
        # Остальные группы по умолчанию 1 (если есть агрегаты)
        for i in range(7, 50):
            if i not in [22, 24, 35, 40, 41, 42] and comp_numbers[i] == 0:
                comp_numbers[i] = 1
        self.env_data['comp_numbers'] = comp_numbers
        print(f"   ✅ comp_numbers: group_by=6 (лопасти) = {comp_numbers[6]}")
        
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
        
        # 0. InitPlanerDt — загрузка dt, assembly_trigger, planer_in_ops, planer_type, history и initial_slots в MacroProperty (ПЕРВЫМ!)
        self.init_planer_dt_fn = None
        try:
            from init_planer_dt import register_init_planer_dt
            dt_array = self.env_data.get('planer_dt_array')
            ac_to_idx = self.env_data.get('ac_to_idx', {})
            assembly_array = self.env_data.get('planer_assembly_array')
            planer_in_ops = self.env_data.get('planer_in_ops', {})
            planer_type = self.env_data.get('planer_type', {})
            planer_in_ops_history = self.env_data.get('planer_in_ops_history')
            if dt_array is not None and len(ac_to_idx) > 0:
                self.init_planer_dt_fn = register_init_planer_dt(
                    model, dt_array, ac_to_idx, max_days, 
                    assembly_array, planer_in_ops, planer_type, planer_in_ops_history
                )
                modules_ok += 1
                assembly_count = assembly_array.sum() if assembly_array is not None else 0
                mi8 = sum(1 for t in planer_type.values() if t == 1)
                mi17 = sum(1 for t in planer_type.values() if t == 2)
                history_count = planer_in_ops_history.sum() if planer_in_ops_history is not None else 0
                print(f"  ✅ init_planer_dt: {len(ac_to_idx)} планеров, dt_size={len(dt_array):,}, assembly={assembly_count:,}, in_ops={len(planer_in_ops)}, types=Mi8:{mi8}/Mi17:{mi17}, history={history_count:,}")
            else:
                print(f"  ⚠️ init_planer_dt: нет данных, будет fallback 90 мин/день")
        except Exception as e:
            print(f"  ❌ init_planer_dt: {e}")
            import traceback
            traceback.print_exc()
            modules_failed += 1
        
        # 0.5 planer_exit — детекция выхода планера из operations
        # ПЕРЕД states_stub! Если планер ушёл из ops → агрегат отцепляется → serviceable
        try:
            import rtc_units_planer_exit
            rtc_units_planer_exit.register_rtc(model, agent, max_days)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_planer_exit: {e}")
            import traceback
            traceback.print_exc()
            modules_failed += 1
        
        # 0.6 demand_host — расчёт дефицита и установка mp_request_count
        # ВАЖНО: Независимо от наличия агрегатов в serviceable!
        try:
            from rtc_units_demand_host import register_demand_host
            comp_numbers = self.env_data.get('comp_numbers', {3: 2, 4: 2})
            comp_dict = {i: v for i, v in enumerate(comp_numbers)} if isinstance(comp_numbers, list) else comp_numbers
            register_demand_host(model, comp_dict, target_groups=[3, 4])
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ demand_host: {e}")
            import traceback
            traceback.print_exc()
            modules_failed += 1
        
        # 1. states_stub — инициализация intent для не-operations состояний
        try:
            import rtc_units_states_stub
            rtc_units_states_stub.register_rtc(model, agent)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_states_stub: {e}")
            modules_failed += 1
        
        # 2. check_limits — ПЕРЕД transition_ops!
        # FIX 14.01.2026: check_limits устанавливает intent_state, transition_ops его проверяет
        try:
            import rtc_units_increment
            rtc_units_increment.register_check_limits(model, agent, max_days)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_check_limits: {e}")
            modules_failed += 1
        
        # 2b. increment — ПЕРЕМЕЩЁН ПОСЛЕ assembly!
        # Пилот не полетит без двигателя — сначала комплектация, потом полёт.
        print("  ℹ️ units_increment: будет зарегистрирован ПОСЛЕ assembly")
        
        # 3. state_repair — intent + инкремент для repair
        try:
            import rtc_units_state_repair
            rtc_units_state_repair.register_rtc(model, agent)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_state_repair: {e}")
            modules_failed += 1
        
        # 4. count — подсчёт агентов (ОТКЛЮЧЕН для отладки)
        # try:
        #     import rtc_units_count
        #     rtc_units_count.register_rtc(model, agent)
        #     modules_ok += 1
        # except Exception as e:
        #     print(f"  ❌ units_count: {e}")
        #     modules_failed += 1
        print("  ⚠️ units_count: ОТКЛЮЧЕН для отладки")
        
        # 5. transition_ops — переходы из operations (СОЗДАЁТ REQUESTS!)
        # Должен быть ПЕРЕД FIFO!
        try:
            import rtc_units_transition_ops
            rtc_units_transition_ops.register_rtc(model, agent, max_frames)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_transition_ops: {e}")
            modules_failed += 1
        
        # 6-7. Трёхуровневая приоритетная FIFO + spawn
        # Приоритет: serviceable → reserve(active=1) → spawn(active=0)
        # Работает ПОСЛЕ transition_ops, когда requests уже созданы
        try:
            import rtc_units_fifo_priority
            rtc_units_fifo_priority.register_rtc(model, agent, max_frames, max_days)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_fifo_priority: {e}")
            modules_failed += 1
        
        # 7b. Assembly — комплектация агрегатов на планеры при assembly_trigger=1
        # Работает ПОСЛЕ FIFO, обрабатывает ONE-SHOT сигнал от планеров в ремонте
        try:
            import rtc_units_assembly
            rtc_units_assembly.register_rtc(model, agent, MAX_PLANERS, max_days)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_assembly: {e}")
            import traceback
            traceback.print_exc()
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
        
        # 10b. transition_storage — storage терминальное, блокирует выход
        try:
            import rtc_units_transition_storage
            rtc_units_transition_storage.register_rtc(model, agent)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_transition_storage: {e}")
            modules_failed += 1
        
        # 11. return_to_pool — включён в fifo_phase2 (reserve → serviceable)
        
        # 11b. ВТОРОЙ ПРОХОД assembly — заполняем слоты освободившиеся после transitions
        # Решает проблему "1-day gap": когда двигатель уходит в ремонт (2→4),
        # слот освобождается, но замена должна прийти В ТОТ ЖЕ ДЕНЬ, не на следующий.
        try:
            import rtc_units_assembly as rtc_units_assembly_pass2
            rtc_units_assembly_pass2.register_rtc_pass2(model, agent, MAX_PLANERS, max_days)
            modules_ok += 1
            print(f"  ✅ units_assembly_pass2: второй проход для instant replacement")
        except Exception as e:
            print(f"  ⚠️ units_assembly_pass2: {e} (не критично)")
        
        # 11c. ТРЕТИЙ ПРОХОД assembly — гарантия полной комплектации
        # Решает проблему: планер вышел из repair, assembly_pass2 назначил только 1 двигатель
        try:
            import rtc_units_assembly as rtc_units_assembly_pass3
            rtc_units_assembly_pass3.register_rtc_pass3(model, agent, MAX_PLANERS, max_days)
            modules_ok += 1
            print(f"  ✅ units_assembly_pass3: третий проход для полной комплектации")
        except Exception as e:
            print(f"  ⚠️ units_assembly_pass3: {e}")
        
        # 11d. ЧЕТВЁРТЫЙ ПРОХОД assembly — последняя попытка
        try:
            import rtc_units_assembly as rtc_units_assembly_pass4
            rtc_units_assembly_pass4.register_rtc_pass4(model, agent, MAX_PLANERS, max_days)
            modules_ok += 1
            print(f"  ✅ units_assembly_pass4: четвёртый проход")
        except Exception as e:
            print(f"  ⚠️ units_assembly_pass4: {e}")
        
        # 11e. ПЯТЫЙ ПРОХОД assembly
        try:
            import rtc_units_assembly as rtc_units_assembly_pass5
            rtc_units_assembly_pass5.register_rtc_pass5(model, agent, MAX_PLANERS, max_days)
            print(f"  ✅ units_assembly_pass5: пятый проход")
        except Exception as e:
            print(f"  ⚠️ units_assembly_pass5: {e}")
        
        # 11f. ШЕСТОЙ ПРОХОД assembly
        try:
            import rtc_units_assembly as rtc_units_assembly_pass6
            rtc_units_assembly_pass6.register_rtc_pass6(model, agent, MAX_PLANERS, max_days)
            print(f"  ✅ units_assembly_pass6: шестой проход")
        except Exception as e:
            print(f"  ⚠️ units_assembly_pass6: {e}")
        
        # 11g. СЕДЬМОЙ ПРОХОД assembly
        try:
            import rtc_units_assembly as rtc_units_assembly_pass7
            rtc_units_assembly_pass7.register_rtc_pass7(model, agent, MAX_PLANERS, max_days)
            print(f"  ✅ units_assembly_pass7: седьмой проход")
        except Exception as e:
            print(f"  ⚠️ units_assembly_pass7: {e}")
        
        # 11h. ВОСЬМОЙ ПРОХОД assembly  
        try:
            import rtc_units_assembly as rtc_units_assembly_pass8
            rtc_units_assembly_pass8.register_rtc_pass8(model, agent, MAX_PLANERS, max_days)
            print(f"  ✅ units_assembly_pass8: восьмой проход")
        except Exception as e:
            print(f"  ⚠️ units_assembly_pass8: {e}")
        
        # 11i. ДЕВЯТЫЙ ПРОХОД
        try:
            import rtc_units_assembly as rtc_units_assembly_pass9
            rtc_units_assembly_pass9.register_rtc_pass9(model, agent, MAX_PLANERS, max_days)
            print(f"  ✅ units_assembly_pass9")
        except Exception as e:
            print(f"  ⚠️ pass9: {e}")
        
        # 11j. ДЕСЯТЫЙ ПРОХОД
        try:
            import rtc_units_assembly as rtc_units_assembly_pass10
            rtc_units_assembly_pass10.register_rtc_pass10(model, agent, MAX_PLANERS, max_days)
            print(f"  ✅ units_assembly_pass10")
        except Exception as e:
            print(f"  ⚠️ pass10: {e}")
        
        # 11k. increment — ТЕПЕРЬ ПОСЛЕ всех 10 проходов assembly!
        # Сначала комплектация, потом полёт. Пилот не полетит без двигателя.
        # FIX 14.01.2026: Используем register_increment (не register_rtc!)
        try:
            import rtc_units_increment
            rtc_units_increment.register_increment(model, agent, max_days)
            modules_ok += 1
            print(f"  ✅ units_increment: SNE/PPR рост ПОСЛЕ всех комплектаций")
        except Exception as e:
            print(f"  ❌ units_increment: {e}")
            modules_failed += 1
        
        # 11.5a. DEBUG: отладка очередей
        try:
            from debug_step import DebugQueueStepFunction
            self.debug_queue_fn = DebugQueueStepFunction(interval=500, target_groups=[4])
            model.addStepFunction(self.debug_queue_fn)
            print(f"  🔍 DEBUG: DebugQueueStepFunction зарегистрирован")
        except Exception as e:
            print(f"  ⚠️ DEBUG: {e}")
        
        # 11.5. deficit_check — StepFunction для проверки дефицита планеров
        # Выполняется ПОСЛЕ assembly, записывает дефициты если планеры не укомплектованы
        try:
            from deficit_check_step import DeficitCheckStepFunction
            # Получаем comp_numbers из self.env_data (сохранён при загрузке)
            comp_numbers_dict = {}
            comp_numbers_list = list(self.env_data.get('comp_numbers', []))
            for i, cn in enumerate(comp_numbers_list):
                if cn > 0:
                    comp_numbers_dict[i] = cn
            
            # Для отладки: только группа 4 (ТВ3-117)
            target_groups = [4]  # Группы для проверки дефицита
            
            self.deficit_check_fn = DeficitCheckStepFunction(comp_numbers_dict, target_groups)
            model.addStepFunction(self.deficit_check_fn)
            print(f"  RTC модуль deficit_check зарегистрирован (StepFunction, groups={target_groups})")
            modules_ok += 1
        except Exception as e:
            print(f"  ⚠️ deficit_check: {e}")
        
        # 12. mp2_writer — запись результатов в MacroProperty (циклический буфер)
        drain_interval = 10  # Буфер на 10 дней
        try:
            import rtc_units_mp2_writer
            rtc_units_mp2_writer.register_rtc(model, agent, max_frames, max_days, drain_interval)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_mp2_writer: {e}")
            modules_failed += 1
        
        # 13. mp2_drain — HostFunction для дренажа MP2 в СУБД
        try:
            from mp2_drain_units import MP2DrainUnitsHostFunction
            from utils.config_loader import get_clickhouse_client
            
            client = get_clickhouse_client()
            self.mp2_drain_fn = MP2DrainUnitsHostFunction(
                client=client,
                table_name='sim_units_v2',
                batch_size=500000,
                simulation_steps=max_days,
                version_date=self.version_date,
                version_id=self.version_id
            )
            # Регистрируем как StepFunction (вызывается после каждого step)
            model.addStepFunction(self.mp2_drain_fn)
            print(f"  RTC модуль mp2_drain зарегистрирован (drain каждые 100 дней)")
            modules_ok += 1
        except Exception as e:
            print(f"  ⚠️ mp2_drain: {e} (история будет только финальная)")
        
        # 14. init_fifo_queues — InitFunction для инициализации FIFO ДО step=0
        # ВАЖНО: используем addInitFunction, а не addStepFunction!
        # mp_planer_slots должен быть инициализирован ДО первого вызова assembly
        try:
            from init_fifo_queues import InitFifoQueuesFunction
            self.init_fifo_fn = InitFifoQueuesFunction()
            model.addInitFunction(self.init_fifo_fn)
            print(f"  RTC модуль init_fifo_queues зарегистрирован (InitFunction ДО step=0)")
            modules_ok += 1
        except Exception as e:
            print(f"  ⚠️ init_fifo_queues: {e}")
        
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
        """Инициализирует MacroProperty для FIFO-очередей и dt планеров"""
        # Получаем данные из population_builder (трёхуровневая система)
        svc_tails = getattr(self.population_builder, 'svc_tails', {})
        rsv_tails = getattr(self.population_builder, 'rsv_tails', {})
        initial_slots = getattr(self.population_builder, 'initial_slots', {})
        
        total_svc = sum(svc_tails.values())
        total_rsv = sum(rsv_tails.values())
        total_slots = sum(initial_slots.values())
        print(f"   FIFO очереди: svc={total_svc}, rsv={total_rsv}, initial_slots={total_slots}")
        
        # Передаём данные в InitFunction для FIFO
        if hasattr(self, 'init_fifo_fn') and self.init_fifo_fn is not None:
            self.init_fifo_fn.set_tails(svc_tails, rsv_tails, initial_slots)
            print(f"   ✅ FIFO данные переданы в InitFunction")
        else:
            print(f"   ⚠️ init_fifo_fn не зарегистрирован, FIFO не будет инициализирован!")
        
        # КРИТИЧНО: Передаём initial_slots в InitPlanerDt (выполняется ПЕРВЫМ в step=0)
        if hasattr(self, 'init_planer_dt_fn') and self.init_planer_dt_fn is not None:
            self.init_planer_dt_fn.set_initial_slots(initial_slots)
            print(f"   ✅ initial_slots переданы в InitPlanerDt ({total_slots} агрегатов)")
        else:
            print(f"   ⚠️ init_planer_dt_fn не зарегистрирован, mp_planer_slots НЕ будет инициализирован!")
        
        # Инициализация dt планеров
        planer_dt = self.env_data.get('planer_dt_array')
        ac_to_idx = self.env_data.get('ac_to_idx', {})
        
        if planer_dt is not None and len(ac_to_idx) > 0:
            print(f"   📊 dt планеров: {len(ac_to_idx)} маппингов загружено")
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
        
        # FIFO очереди инициализируются через StepFunction на step=0
        
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
        
        # Экспорт в ClickHouse (полная история через HostFunction)
        if args.export:
            print("\n" + "=" * 60)
            print("📤 ФИНАЛЬНЫЙ DRAIN MP2 В CLICKHOUSE")
            print("=" * 60)
            
            if orchestrator.mp2_drain_fn is not None:
                # Финальный дренаж уже должен был сработать на последнем step
                # Но для надёжности запускаем ещё один step
                print("   🔄 Запуск финального drain step...")
                orchestrator.simulation.step()  # Это вызовет HostFunction.run()
                print(f"   ✅ Итого записей: {orchestrator.mp2_drain_fn.total_rows_written:,}")
                print(f"   ⏱️ Время drain: {orchestrator.mp2_drain_fn.total_drain_time:.2f}с")
            else:
                # Fallback: старый метод экспорта
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

