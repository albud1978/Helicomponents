#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (sim_v2 dead-code cleanup): не используется активным V8-ядром (orchestrator_limiter_v8). Внутренние импорты заморожены.
"""
V2 Orchestrator с LIMITER_DATE архитектурой

Особенности:
- limiter_date вычисляется при входе агента в operations
- step_days = min(all_limiter_dates, next_program_change) - current_day
- GPU-side reduction по ~300 агентам (быстро!)
- Предрасчёт program_change_days из flight_program_ac

Запуск:
    python3 orchestrator_limiter.py --version-date 2025-07-04 --end-day 3650 --enable-mp2
"""
import os
import sys
import argparse
import time
from typing import Dict, List
from datetime import date as dt_date

# Добавляем пути для импорта
_MESSAGING_DIR = os.path.dirname(__file__)
_SIM_V2_DIR = os.path.join(_MESSAGING_DIR, '..')
_CODE_DIR = os.path.join(_SIM_V2_DIR, '..')

sys.path.insert(0, _MESSAGING_DIR)
sys.path.insert(0, _SIM_V2_DIR)
sys.path.insert(0, _CODE_DIR)

from sim_env_setup import get_client, prepare_env_arrays
from base_model_messaging import V2BaseModelMessaging
from precompute_events import compute_mp5_cumsum
import rtc_limiter_date
import rtc_batch_operations
import rtc_publish_event
import rtc_quota_manager_event
import rtc_apply_decisions
import model_build

from components.agent_population import AgentPopulationBuilder

try:
    import pyflamegpu as fg
    import numpy as np
except ImportError as e:
    raise RuntimeError(f"Зависимости не установлены: {e}")


class HF_InitMP5Cumsum(fg.HostFunction):
    """HostFunction для инициализации mp5_cumsum MacroProperty"""
    
    def __init__(self, cumsum_data: np.ndarray, frames: int, days: int):
        super().__init__()
        self.data = cumsum_data
        self.frames = frames
        self.days = days
        self.initialized = False
    
    def run(self, FLAMEGPU):
        if self.initialized:
            return  # Выполняется только на первом шаге
        
        step = FLAMEGPU.getStepCounter()
        if step > 0:
            return
        
        mp = FLAMEGPU.environment.getMacroPropertyUInt32("mp5_cumsum")
        
        # RTC использует MAX_FRAMES=400, MAX_DAYS+1 для stride
        MAX_FRAMES = 400
        MAX_DAYS_PLUS_1 = 4001  # MAX_DAYS + 1
        
        print(f"HF_InitMP5Cumsum: Инициализация для FRAMES={self.frames}, DAYS={self.days}")
        
        # RTC индексация: cumsum[idx * (MAX_DAYS+1) + day] — frame-major
        # self.data из compute_mp5_cumsum: flat [frames * (days+1)], frame-major
        count = 0
        for f in range(self.frames):
            for d in range(self.days + 1):
                src_idx = f * (self.days + 1) + d  # Python cumsum: frame-major
                dst_idx = f * MAX_DAYS_PLUS_1 + d  # RTC: idx * (MAX_DAYS+1) + day
                if src_idx < len(self.data):
                    mp[dst_idx] = int(self.data[src_idx])
                    count += 1
        
        print(f"HF_InitMP5Cumsum: Инициализировано {count} элементов")
        self.initialized = True


class LimiterOrchestrator:
    """Оркестратор с limiter_date архитектурой"""
    
    def __init__(self, env_data: Dict, end_day: int = 3650,
                 enable_mp2: bool = False, clickhouse_client=None,
                 version_date_str: str = "2025-07-04"):
        self.env_data = env_data
        self.end_day = end_day
        self.enable_mp2 = enable_mp2
        self.clickhouse_client = clickhouse_client
        self.version_date_str = version_date_str
        
        self.base_model = V2BaseModelMessaging()
        self.model = None
        self.simulation = None
        
        # Параметры
        self.frames = int(env_data['frames_total_u16'])
        self.days = int(env_data.get('days_total_u16', 3650))
        
        # Предрасчёт изменений программы
        print("\n📊 Предрасчёт program_change_days...")
        self.program_change_days = rtc_limiter_date.precompute_program_changes(
            clickhouse_client, version_date_str
        )
        
        # Загрузка MP5 данных (лётные часы)
        print("📊 Загрузка MP5 данных...")
        mp5_raw = np.array(env_data.get('mp5_daily_hours_linear', []), dtype=np.uint32)
        
        # mp5_daily_hours_linear содержит dt (ежедневную наработку), размер = frames * (days + 1)
        # ВСЕГДА вычисляем cumsum!
        print(f"  ✅ mp5_lin (dt): {len(mp5_raw)} элементов, non-zero: {np.count_nonzero(mp5_raw)}")
        print(f"     Пример dt: {mp5_raw[:5]} (должно быть ~90-180 мин/день)")
        
        # Reshape для compute_mp5_cumsum: нужен [frames, days]
        # mp5_raw имеет размер frames * (days + 1), берём только первые frames * days
        expected_lin_size = self.frames * self.days
        if len(mp5_raw) >= expected_lin_size:
            mp5_for_cumsum = mp5_raw[:expected_lin_size]
            self.mp5_cumsum = compute_mp5_cumsum(mp5_for_cumsum, self.frames, self.days)
            print(f"  ✅ mp5_cumsum: {len(self.mp5_cumsum)} элементов")
        else:
            print(f"  ⚠️ mp5 размер {len(mp5_raw)} < ожидаемого {expected_lin_size}")
            self.mp5_cumsum = np.zeros(self.frames * (self.days + 1), dtype=np.uint32)
        
        # Компоненты
        self.population_builder = AgentPopulationBuilder(env_data)
        
        # MP5 Strategy для инициализации mp5_lin
        from components.mp5_strategy import HostOnlyMP5Strategy
        self.mp5_strategy = HostOnlyMP5Strategy(env_data, self.frames, self.days)
    
    def build_model(self):
        """Строит модель с limiter_date архитектурой"""
        
        print("\n" + "=" * 60)
        print("🔧 Построение модели с LIMITER_DATE архитектурой")
        print("=" * 60)
        
        # Создаём базовую модель (current_day, step_days, quota_enabled уже определены в base_model)
        self.model = self.base_model.create_model(self.env_data)
        
        # Дополнительные Environment properties
        self.base_model.env.newPropertyUInt("end_day", self.end_day)
        
        # MacroProperty для limiter_date системы
        rtc_limiter_date.setup_limiter_macroproperties(
            self.base_model.env, 
            self.program_change_days
        )
        
        # MacroProperty для mp5_cumsum (batch операции)
        cumsum_size = model_build.RTC_MAX_FRAMES * (model_build.MAX_DAYS + 1)
        self.base_model.env.newMacroPropertyUInt32("mp5_cumsum", cumsum_size)
        print(f"  ✅ MacroProperty mp5_cumsum: {cumsum_size} элементов")
        
        # HostFunction для инициализации mp5_cumsum
        hf_init_cumsum = HF_InitMP5Cumsum(self.mp5_cumsum, self.frames, self.days)
        layer_init = self.model.newLayer("layer_init_mp5_cumsum")
        layer_init.addHostFunction(hf_init_cumsum)
        print("  ✅ HostFunction для mp5_cumsum зарегистрирован")
        
        # Инициализация mp5_lin через MP5Strategy (для rtc_state_2_operations)
        self.mp5_strategy.register(self.model)
        print("  ✅ mp5_lin инициализирован через MP5Strategy")
        
        # Получаем агентов
        heli_agent = self.base_model.agent
        quota_agent = self.base_model.quota_agent
        
        # ═══════════════════════════════════════════════════════════════════════
        # Регистрация RTC модулей
        # ═══════════════════════════════════════════════════════════════════════
        
        # 1. Ежедневные инкременты SNE/PPR (state_2_operations + states_stub)
        print("\n📦 Подключение state_2_operations + states_stub...")
        import rtc_state_2_operations
        import rtc_states_stub
        rtc_state_2_operations.register_rtc(self.model, heli_agent)
        rtc_states_stub.register_rtc(self.model, heli_agent)
        
        # 3. Baseline квотирование (вместо event-driven)
        print("\n📊 Подключение baseline квотирования...")
        self._register_quota_modules()
        
        # 4. State managers
        print("\n📦 Подключение state managers...")
        self._register_state_managers()
        
        # 5. Spawn_v2 (плановый спавн)
        print("\n📦 Подключение spawn_v2...")
        self._register_spawn()
        
        # 6. Spawn_dynamic (динамический спавн при выбытии)
        print("\n📦 Подключение spawn_dynamic...")
        self._register_spawn_dynamic()
        
        print("\n✅ Модель с LIMITER_DATE построена")
        print("=" * 60)
        
        return self.model
    
    def _register_quota_modules(self):
        """Регистрирует baseline модули квотирования"""
        import rtc_quota_count_ops
        import rtc_quota_ops_excess
        import rtc_quota_promote_serviceable
        import rtc_quota_promote_reserve
        import rtc_quota_promote_inactive
        from rtc_modules import rtc_quota_repair
        
        rtc_quota_count_ops.register_rtc(self.model, self.base_model.agent)
        rtc_quota_ops_excess.register_rtc(self.model, self.base_model.agent)
        rtc_quota_promote_serviceable.register_rtc(self.model, self.base_model.agent)
        rtc_quota_promote_reserve.register_rtc(self.model, self.base_model.agent)
        rtc_quota_promote_inactive.register_rtc(self.model, self.base_model.agent)
        
        # ✅ Инициализируем repair_number_by_idx ДО quota_repair
        self._init_repair_number_buffer_in_model()
        
        # ✅ Добавляем quota_repair для FIFO очереди на ремонт
        rtc_quota_repair.register_rtc(self.model, self.base_model.agent)
        
        print("  ✅ Baseline квотирование + quota_repair подключено")
    
    def _register_state_managers(self):
        """Регистрирует state managers"""
        import rtc_state_manager_serviceable
        import rtc_state_manager_operations
        import rtc_state_manager_repair
        import rtc_state_manager_storage
        import rtc_state_manager_reserve
        import rtc_state_manager_inactive
        
        rtc_state_manager_serviceable.register_rtc(self.model, self.base_model.agent)
        rtc_state_manager_operations.register_state_manager_operations(self.model, self.base_model.agent)
        rtc_state_manager_repair.register_state_manager_repair(self.model, self.base_model.agent)
        rtc_state_manager_storage.register_state_manager_storage(self.model, self.base_model.agent)
        rtc_state_manager_reserve.register_state_manager_reserve(self.model, self.base_model.agent)
        rtc_state_manager_inactive.register_state_manager_inactive(self.model, self.base_model.agent)
        
        print("  ✅ State managers подключены")
    
    def _register_spawn(self):
        """Регистрирует spawn_v2 для планового создания агентов"""
        from rtc_modules import rtc_spawn_v2
        
        rtc_spawn_v2.register_rtc(self.model, self.base_model.agent, self.env_data)
        
        print("  ✅ Spawn_v2 зарегистрирован (плановый spawn)")
    
    def _register_spawn_dynamic(self):
        """Регистрирует spawn_dynamic для динамического создания агентов при выбытии"""
        from rtc_modules import rtc_spawn_dynamic
        
        rtc_spawn_dynamic.register_rtc(self.model, self.base_model.agent, self.env_data)
        
        print("  ✅ Spawn_dynamic зарегистрирован (динамический spawn)")
    
    def create_simulation(self):
        """Создаёт симуляцию"""
        print("\n🚀 Создание симуляции...")
        
        self.simulation = fg.CUDASimulation(self.model)
        
        # Загружаем агентов
        self.population_builder.populate_agents(self.simulation, self.base_model.agent)
        
        # Создаём QuotaManager агентов
        self._populate_quota_managers()
        
        # Инициализируем MacroProperty
        self._init_macroproperties()
        
        # Инициализируем spawn_v2
        self._init_spawn()
        
        # Инициализируем spawn_dynamic
        self._init_spawn_dynamic()
        
        # Инициализируем repair_number_by_idx для quota_repair
        self._init_repair_number_buffer()
        
        print("  ✅ Симуляция создана")
        return self.simulation
    
    def _init_spawn(self):
        """Инициализирует spawn_v2 данные"""
        from rtc_modules import rtc_spawn_v2
        
        rtc_spawn_v2.initialize_spawn_population(
            self.simulation,
            self.model,
            self.env_data
        )
        
        print("  ✅ Spawn_v2 инициализирован")
    
    def _init_spawn_dynamic(self):
        """Инициализирует spawn_dynamic данные"""
        from rtc_modules import rtc_spawn_dynamic
        
        rtc_spawn_dynamic.init_population(
            self.simulation,
            self.model,
            self.env_data
        )
        
        print("  ✅ Spawn_dynamic инициализирован")
    
    def _init_repair_number_buffer_in_model(self):
        """Инициализирует repair_number_by_idx HostFunction в build_model (ДО quota_repair)"""
        print("  📊 Инициализация repair_number_by_idx для quota_repair...")
        
        mp3 = self.env_data.get('mp3_arrays', {})
        mp3_aircraft_number = mp3.get('mp3_aircraft_number', [])
        mp3_group_by = mp3.get('mp3_group_by', [])
        frames_index = self.env_data.get('frames_index', {})
        
        frames_total = model_build.RTC_MAX_FRAMES
        PLANER_REPAIR_NUMBER = 18
        
        # Строим маппинг frame_idx → group_by через aircraft_number
        frame_to_group_by = {}
        for j in range(len(mp3_aircraft_number)):
            if j < len(mp3_group_by):
                gb = mp3_group_by[j]
                if gb in [1, 2]:
                    ac = mp3_aircraft_number[j]
                    if ac in frames_index:
                        frame_idx = frames_index[ac]
                        frame_to_group_by[frame_idx] = gb
        
        # Создаём массив repair_number по idx
        repair_number_by_idx = []
        for frame_idx in range(frames_total):
            gb = frame_to_group_by.get(frame_idx, 0)
            if gb in [1, 2]:
                repair_number_by_idx.append(PLANER_REPAIR_NUMBER)
            else:
                repair_number_by_idx.append(0)
        
        non_zero = sum(1 for x in repair_number_by_idx if x > 0)
        print(f"  📊 frame_to_group_by: {len(frame_to_group_by)} планеров")
        print(f"  📊 repair_number_by_idx: {non_zero} > 0 (quota={PLANER_REPAIR_NUMBER})")
        
        # HostFunction для инициализации MacroProperty
        class HF_InitRepairNumber(fg.HostFunction):
            def __init__(self, data):
                super().__init__()
                self.data = data
                self.initialized = False
            
            def run(self, FLAMEGPU):
                if self.initialized:
                    return
                
                mp = FLAMEGPU.environment.getMacroPropertyUInt32("repair_number_by_idx")
                for i, val in enumerate(self.data):
                    mp[i] = int(val)
                
                print(f"  ✅ HF_InitRepairNumber: инициализировано {len(self.data)} элементов")
                self.initialized = True
        
        hf = HF_InitRepairNumber(repair_number_by_idx)
        init_layer = self.model.newLayer("init_repair_number")
        init_layer.addHostFunction(hf)
        print("  ✅ HostFunction init_repair_number зарегистрирован")
    
    def _init_repair_number_buffer(self):
        """DEPRECATED: Перенесено в _init_repair_number_buffer_in_model"""
        print("  ⚠️ _init_repair_number_buffer вызван (уже инициализировано в build_model)")
        
        mp3 = self.env_data.get('mp3_arrays', {})
        mp3_aircraft_number = mp3.get('mp3_aircraft_number', [])
        mp3_group_by = mp3.get('mp3_group_by', [])
        frames_index = self.env_data.get('frames_index', {})
        
        frames_total = model_build.RTC_MAX_FRAMES
        
        # ✅ ИСПРАВЛЕНО: Для планеров (group_by=1,2) repair_number=18 из md_components
        # Это глобальный лимит: max 18 планеров одновременно в ремонте
        PLANER_REPAIR_NUMBER = 18
        
        # Строим маппинг frame_idx → group_by через aircraft_number
        frame_to_group_by = {}
        for j in range(len(mp3_aircraft_number)):
            if j < len(mp3_group_by):
                gb = mp3_group_by[j]
                if gb in [1, 2]:  # Только планеры
                    ac = mp3_aircraft_number[j]
                    if ac in frames_index:
                        frame_idx = frames_index[ac]
                        frame_to_group_by[frame_idx] = gb
        
        print(f"  📊 frame_to_group_by: {len(frame_to_group_by)} планеров")
        
        # Создаём массив repair_number по idx
        repair_number_by_idx = []
        for frame_idx in range(frames_total):
            gb = frame_to_group_by.get(frame_idx, 0)
            
            # Для планеров (group_by=1,2) → repair_number=18
            if gb in [1, 2]:
                repair_number_by_idx.append(PLANER_REPAIR_NUMBER)
            else:
                # Для агрегатов и пустых слотов → 0 (без квоты)
                repair_number_by_idx.append(0)
        
        # Инициализируем MacroProperty через HostFunction
        class HF_InitRepairNumber(fg.HostFunction):
            def __init__(self, data):
                super().__init__()
                self.data = data
                self.initialized = False
            
            def run(self, FLAMEGPU):
                if self.initialized:
                    return
                
                mp = FLAMEGPU.environment.getMacroPropertyUInt32("repair_number_by_idx")
                for i, val in enumerate(self.data):
                    mp[i] = int(val)
                
                self.initialized = True
        
        hf = HF_InitRepairNumber(repair_number_by_idx)
        init_layer = self.model.newLayer()
        init_layer.addHostFunction(hf)
        
        non_zero = sum(1 for x in repair_number_by_idx if x > 0)
        print(f"  ✅ repair_number_by_idx инициализирован ({len(repair_number_by_idx)} элементов, {non_zero} > 0)")
    
    def _populate_quota_managers(self):
        """Создаёт QuotaManager агентов"""
        initial_ops = self.population_builder.get_initial_ops_count()
        mi8_ops = initial_ops.get(1, 0)
        mi17_ops = initial_ops.get(2, 0)
        
        quota_pop = fg.AgentVector(self.base_model.quota_agent)
        
        # Mi-8 QuotaManager
        quota_pop.push_back()
        mi8_mgr = quota_pop[len(quota_pop) - 1]
        mi8_mgr.setVariableUInt8("group_by", 1)
        mi8_mgr.setVariableUInt("target", 0)
        mi8_mgr.setVariableUInt("current", mi8_ops)
        mi8_mgr.setVariableInt("balance", 0)
        mi8_mgr.setVariableUInt("remaining_deficit", 0)
        
        # Mi-17 QuotaManager
        quota_pop.push_back()
        mi17_mgr = quota_pop[len(quota_pop) - 1]
        mi17_mgr.setVariableUInt8("group_by", 2)
        mi17_mgr.setVariableUInt("target", 0)
        mi17_mgr.setVariableUInt("current", mi17_ops)
        mi17_mgr.setVariableInt("balance", 0)
        mi17_mgr.setVariableUInt("remaining_deficit", 0)
        
        self.simulation.setPopulationData(quota_pop)
        print(f"  ✅ QuotaManager: Mi-8 (curr={mi8_ops}), Mi-17 (curr={mi17_ops})")
    
    def _init_macroproperties(self):
        """Инициализирует MacroProperty"""
        # program_change_days будут инициализированы через HostFunction
        # или при первом вызове RTC (читают из PropertyArray)
        print(f"  ✅ MacroProperty инициализированы")
    
    def run(self, max_steps: int = 10000):
        """Запускает симуляцию с ежедневными шагами GPU + квотированием на каждом шаге"""
        
        print(f"\n▶️  Запуск LIMITER_DATE симуляции (end_day={self.end_day})")
        print(f"  MP2 экспорт: {'✅' if self.enable_mp2 else '❌'}")
        print(f"  Режим: ежедневные GPU шаги + квотирование на КАЖДОМ шаге")
        print(f"  (Шаг = лимитер = событие → квотирование обязательно)")
        
        t_start = time.perf_counter()
        
        total_gpu_time = 0.0
        total_collect_time = 0.0
        all_mp2_rows = []
        
        # ═══════════════════════════════════════════════════════════════════════════
        # ИСПРАВЛЕНО: День 0 записывается ПОСЛЕ step(0), как в baseline
        # step(0) применяет квотирование → результат = "день 0" (готовность к работе)
        # ═══════════════════════════════════════════════════════════════════════════
        
        # Ежедневные шаги на GPU (начиная с дня 0, как в baseline)
        for day in range(0, self.end_day):
            # Квотирование на КАЖДОМ шаге (т.к. шаг = лимитер = событие)
            self.simulation.setEnvironmentPropertyUInt("current_day", day)
            self.simulation.setEnvironmentPropertyUInt("step_days", 1)
            self.simulation.setEnvironmentPropertyUInt("quota_enabled", 1)  # ВСЕГДА включено
            
            # GPU шаг
            t_gpu = time.perf_counter()
            self.simulation.step()
            total_gpu_time += time.perf_counter() - t_gpu
            
            # Собираем MP2 данные
            if self.enable_mp2:
                t_collect = time.perf_counter()
                self._collect_mp2_day(all_mp2_rows, day)
                total_collect_time += time.perf_counter() - t_collect
            
            # Прогресс
            if day % 500 == 0 or day == self.end_day - 1:
                print(f"  День {day}/{self.end_day}, GPU: {total_gpu_time:.2f}с, строк: {len(all_mp2_rows)}")
        
        # ═══════════════════════════════════════════════════════════════════════════
        # ПОСТПРОЦЕССИНГ: Заполнение истории ремонта для inactive→operations
        # ═══════════════════════════════════════════════════════════════════════════
        total_drain_time = total_collect_time
        if self.enable_mp2 and all_mp2_rows:
            t_postprocess = time.perf_counter()
            modified_count = self._postprocess_active_trigger(all_mp2_rows)
            total_drain_time += time.perf_counter() - t_postprocess
            if modified_count > 0:
                print(f"  ✅ Постпроцессинг: {modified_count} записей изменено (inactive→repair задним числом)")
        
        # Финальный INSERT
        if self.enable_mp2 and all_mp2_rows:
            t_insert = time.perf_counter()
            columns = list(all_mp2_rows[0].keys())
            values = [[row[col] for col in columns] for row in all_mp2_rows]
            col_str = ', '.join(columns)
            self.clickhouse_client.execute(
                f"INSERT INTO sim_masterv2_limiter ({col_str}) VALUES",
                values
            )
            total_drain_time += time.perf_counter() - t_insert
            print(f"  ✅ Финальный INSERT: {len(all_mp2_rows)} строк")
        
        t_end = time.perf_counter()
        elapsed = t_end - t_start
        step_count = self.end_day
        current_day = self.end_day
        total_rows_exported = len(all_mp2_rows)
        
        print(f"\n✅ LIMITER_DATE симуляция завершена:")
        print(f"  • Шагов: {step_count}")
        print(f"  • Дней: {current_day}")
        print(f"  • Время общее: {elapsed:.2f}с")
        print(f"  • Время GPU: {total_gpu_time:.2f}с ({100*total_gpu_time/elapsed:.1f}%)")
        print(f"  • Время drain: {total_drain_time:.2f}с ({100*total_drain_time/elapsed:.1f}%)")
        print(f"  • Дней/сек: {current_day / elapsed:.1f}")
        if self.enable_mp2:
            print(f"  • Строк выгружено: {total_rows_exported}")
    
    def _get_step_days_from_gpu(self, current_day: int) -> int:
        """
        Получает step_days вычисленный на GPU из MacroProperty.
        Fallback на Python расчёт если GPU вернул 0.
        """
        # Читаем из MacroProperty mp_step_days_result
        try:
            mp_result = self.simulation.environment.getMacroPropertyUInt32("mp_step_days_result")
            step_days = int(mp_result[0])
            if step_days > 0 and step_days <= self.end_day:
                return step_days
        except Exception as e:
            print(f"  ⚠️ Не удалось прочитать step_days с GPU: {e}")
        
        # Fallback: Python расчёт по program_changes
        next_program = self.end_day
        for pday in self.program_change_days:
            if pday > current_day:
                next_program = pday
                break
        
        step_days = next_program - current_day
        step_days = max(step_days, 1)
        step_days = min(step_days, self.end_day - current_day)
        
        return step_days
    
    def _collect_mp2_day(self, all_rows: list, day: int) -> int:
        """Собирает MP2 данные за один день (быстро, без интерполяции)"""
        states = ['inactive', 'operations', 'serviceable', 'repair', 'reserve', 'storage']
        
        vd = dt_date.fromisoformat(self.version_date_str)
        version_date = vd.year * 10000 + vd.month * 100 + vd.day
        version_id = int(self.env_data.get('version_id_u32', 1))
        
        count = 0
        for state_name in states:
            heli_pop = fg.AgentVector(self.base_model.agent)
            self.simulation.getPopulationData(heli_pop, state_name)
            
            for i in range(heli_pop.size()):
                agent = heli_pop.at(i)
                all_rows.append({
                    'version_date': version_date,
                    'version_id': version_id,
                    'day_u16': day,
                    'idx': agent.getVariableUInt('idx'),
                    'aircraft_number': agent.getVariableUInt('aircraft_number'),
                    'group_by': agent.getVariableUInt('group_by'),
                    'state': state_name,
                    'dt': agent.getVariableUInt('daily_today_u32') if state_name == 'operations' else 0,
                    'sne': agent.getVariableUInt('sne'),
                    'ppr': agent.getVariableUInt('ppr'),
                    'll': agent.getVariableUInt('ll'),
                    'oh': agent.getVariableUInt('oh'),
                    'br': agent.getVariableUInt('br'),
                    'repair_days': agent.getVariableUInt('repair_days'),
                    'repair_time': agent.getVariableUInt('repair_time'),
                    'mfg_date': agent.getVariableUInt('mfg_date'),
                    'intent_state': agent.getVariableUInt('intent_state'),
                })
                count += 1
        return count

    def _collect_mp2_rows_range(self, all_rows: list, start_day: int, end_day: int) -> int:
        """Собирает MP2 данные для ВСЕХ дней в диапазоне [start_day, end_day]"""
        if not self.enable_mp2:
            return 0
        
        states = ['inactive', 'operations', 'serviceable', 'repair', 'reserve', 'storage']
        
        vd = dt_date.fromisoformat(self.version_date_str)
        version_date = vd.year * 10000 + vd.month * 100 + vd.day
        version_id = int(self.env_data.get('version_id_u32', 1))
        
        count = 0
        
        # Читаем состояние агентов ОДИН раз
        agent_data = []
        for state_name in states:
            heli_pop = fg.AgentVector(self.base_model.agent)
            self.simulation.getPopulationData(heli_pop, state_name)
            
            for i in range(heli_pop.size()):
                agent = heli_pop.at(i)
                agent_data.append({
                    'idx': agent.getVariableUInt('idx'),
                    'aircraft_number': agent.getVariableUInt('aircraft_number'),
                    'group_by': agent.getVariableUInt('group_by'),
                    'state': state_name,
                    'sne': agent.getVariableUInt('sne'),
                    'ppr': agent.getVariableUInt('ppr'),
                    'll': agent.getVariableUInt('ll'),
                    'oh': agent.getVariableUInt('oh'),
                    'br': agent.getVariableUInt('br'),
                    'repair_days': agent.getVariableUInt('repair_days'),
                    'repair_time': agent.getVariableUInt('repair_time'),
                    'mfg_date': agent.getVariableUInt('mfg_date'),
                    'intent_state': agent.getVariableUInt('intent_state'),
                })
        
        # Интерполируем на все дни в диапазоне
        for day in range(start_day, end_day + 1):
            for ad in agent_data:
                all_rows.append({
                    'version_date': version_date,
                    'version_id': version_id,
                    'day_u16': day,
                    'idx': ad['idx'],
                    'aircraft_number': ad['aircraft_number'],
                    'group_by': ad['group_by'],
                    'state': ad['state'],
                    'dt': 0,
                    'sne': ad['sne'],
                    'ppr': ad['ppr'],
                    'll': ad['ll'],
                    'oh': ad['oh'],
                    'br': ad['br'],
                    'repair_days': ad['repair_days'],
                    'repair_time': ad['repair_time'],
                    'mfg_date': ad['mfg_date'],
                    'intent_state': ad['intent_state'],
                })
                count += 1
        
        return count

    def _postprocess_active_trigger(self, all_rows: list) -> int:
        """
        Постпроцессинг: заполнение истории ремонта для переходов inactive→operations
        
        Логика (как в baseline mp2_postprocess_active):
        1. Находим день d_event где агент перешёл inactive → operations
        2. Заполняем историю ремонта задним числом: [d_event - repair_time .. d_event - 1]
        3. Устанавливаем state='repair', repair_days=1..R для этого окна
        """
        if not all_rows:
            return 0
        
        # Группируем по aircraft_number
        from collections import defaultdict
        by_ac = defaultdict(list)
        for i, row in enumerate(all_rows):
            by_ac[row['aircraft_number']].append((i, row))
        
        modified_count = 0
        
        for ac, rows_list in by_ac.items():
            # Сортируем по дню
            rows_list.sort(key=lambda x: x[1]['day_u16'])
            
            # Ищем переход inactive → operations
            prev_state = None
            for idx, (row_idx, row) in enumerate(rows_list):
                curr_state = row['state']
                curr_day = row['day_u16']
                
                if prev_state == 'inactive' and curr_state == 'operations':
                    # Нашли переход inactive → operations!
                    d_event = curr_day
                    repair_time = row.get('repair_time', 0)
                    
                    if repair_time > 0 and d_event > 0:
                        # Окно ремонта: [d_event - repair_time .. d_event - 1]
                        s = max(0, d_event - repair_time)
                        e = d_event - 1
                        
                        if s <= e:
                            # Заполняем окно ремонта
                            repair_day_counter = 1
                            for j, (j_row_idx, j_row) in enumerate(rows_list):
                                j_day = j_row['day_u16']
                                if s <= j_day <= e:
                                    # Меняем state на 'repair'
                                    all_rows[j_row_idx]['state'] = 'repair'
                                    all_rows[j_row_idx]['repair_days'] = repair_day_counter
                                    repair_day_counter += 1
                                    modified_count += 1
                
                prev_state = curr_state
        
        return modified_count


def create_limiter_table(client):
    """Создаёт таблицу sim_masterv2_limiter"""
    ddl = """
    CREATE TABLE IF NOT EXISTS sim_masterv2_limiter (
        version_date UInt32,
        version_id UInt32,
        day_u16 UInt16,
        idx UInt16,
        aircraft_number UInt32,
        group_by UInt8,
        state String,
        dt UInt16,
        sne UInt32,
        ppr UInt32,
        ll UInt32,
        oh UInt32,
        br UInt32,
        repair_days UInt16,
        repair_time UInt16,
        mfg_date UInt32,
        intent_state UInt8
    ) ENGINE = MergeTree()
    ORDER BY (version_date, day_u16, idx)
    """
    client.execute(ddl)
    print("✅ Таблица sim_masterv2_limiter создана/существует")


def main():
    """Точка входа"""
    
    parser = argparse.ArgumentParser(description="V2 Orchestrator с LIMITER_DATE")
    parser.add_argument("--version-date", required=True, help="Дата версии (YYYY-MM-DD)")
    parser.add_argument("--end-day", type=int, default=3650, help="Конечный день")
    parser.add_argument("--max-steps", type=int, default=10000, help="Максимум шагов")
    parser.add_argument("--enable-mp2", action="store_true", help="MP2 экспорт")
    parser.add_argument("--drop-table", action="store_true", help="Очистить таблицу")
    args = parser.parse_args()
    
    print("=" * 70)
    print("🚀 V2 ORCHESTRATOR — LIMITER_DATE ARCHITECTURE")
    print("=" * 70)
    print(f"  Version date: {args.version_date}")
    print(f"  End day: {args.end_day}")
    print("=" * 70)
    
    # Подключение к СУБД
    client = get_client()
    
    # Создаём таблицу
    create_limiter_table(client)
    
    if args.drop_table:
        client.execute("TRUNCATE TABLE IF EXISTS sim_masterv2_limiter")
        print("✅ Таблица очищена")
    
    # Загрузка данных
    print("\n📥 Загрузка данных...")
    version_date = dt_date.fromisoformat(args.version_date)
    env_data = prepare_env_arrays(client, version_date)
    
    # Создаём оркестратор
    orchestrator = LimiterOrchestrator(
        env_data=env_data,
        end_day=args.end_day,
        enable_mp2=args.enable_mp2,
        clickhouse_client=client,
        version_date_str=args.version_date
    )
    
    # Строим модель
    orchestrator.build_model()
    
    # Создаём симуляцию
    orchestrator.create_simulation()
    
    # Запускаем
    orchestrator.run(args.max_steps)
    
    print("\n✅ Готово!")


if __name__ == "__main__":
    main()

