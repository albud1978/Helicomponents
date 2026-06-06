#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (sim_v2 dead-code cleanup): не используется активным V8-ядром (orchestrator_limiter_v8). Внутренние импорты заморожены.
"""
Orchestrator для Adaptive Step v3

Архитектура:
1. Вычисление горизонтов (бинарный поиск по cumsum)
2. Global min → adaptive_days
3. Батчевые инкременты
4. Переходы (repair→reserve ПЕРВЫМ!)
5. Квотирование
6. MP2 запись только на adaptive_day

Запуск:
    python3 orchestrator_adaptive_v3.py --version-date 2025-07-04 --end-day 3650 --enable-mp2

Дата: 10.01.2026
"""
import os
import sys
import argparse
import time
from typing import Dict, List, Tuple
from datetime import date as dt_date

# Добавляем пути
_MESSAGING_DIR = os.path.dirname(__file__)
_SIM_V2_DIR = os.path.join(_MESSAGING_DIR, '..')
_CODE_DIR = os.path.join(_SIM_V2_DIR, '..')

sys.path.insert(0, _MESSAGING_DIR)
sys.path.insert(0, _SIM_V2_DIR)
sys.path.insert(0, _CODE_DIR)

from sim_env_setup import get_client, prepare_env_arrays
from base_model_messaging import V2BaseModelMessaging
from precompute_events import compute_mp5_cumsum
import rtc_adaptive_v3
import model_build

from components.agent_population import AgentPopulationBuilder

# Импорт baseline модулей для квотирования
import rtc_quota_count_ops
import rtc_quota_ops_excess
import rtc_quota_promote_serviceable
import rtc_quota_promote_reserve
import rtc_quota_promote_inactive
import rtc_state_manager_operations
import rtc_state_manager_serviceable
import rtc_state_manager_repair
import rtc_state_manager_reserve
import rtc_state_manager_inactive
import rtc_state_manager_storage
from rtc_modules import rtc_quota_repair

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
            return
        
        step = FLAMEGPU.getStepCounter()
        if step > 0:
            return
        
        mp = FLAMEGPU.environment.getMacroPropertyUInt32("mp5_cumsum")
        
        MAX_FRAMES = 400
        MAX_DAYS_PLUS_1 = 4001
        
        print(f"HF_InitMP5Cumsum: Инициализация для FRAMES={self.frames}, DAYS={self.days}")
        
        count = 0
        for f in range(self.frames):
            for d in range(self.days + 1):
                src_idx = f * (self.days + 1) + d
                dst_idx = f * MAX_DAYS_PLUS_1 + d
                
                if src_idx < len(self.data) and dst_idx < MAX_FRAMES * MAX_DAYS_PLUS_1:
                    mp[dst_idx] = int(self.data[src_idx])
                    count += 1
        
        print(f"HF_InitMP5Cumsum: Инициализировано {count} элементов")
        self.initialized = True


class HF_InitMP5(fg.HostFunction):
    """HostFunction для инициализации mp5_lin MacroProperty"""
    
    def __init__(self, mp5_data: np.ndarray, frames: int, days: int):
        super().__init__()
        self.data = mp5_data
        self.frames = frames
        self.days = days
        self.initialized = False
    
    def run(self, FLAMEGPU):
        if self.initialized:
            return
        
        step = FLAMEGPU.getStepCounter()
        if step > 0:
            return
        
        mp = FLAMEGPU.environment.getMacroPropertyUInt32("mp5_lin")
        
        MAX_FRAMES = 400
        MAX_DAYS_PLUS_1 = 4001
        
        print(f"HF_InitMP5: Инициализация mp5_lin для FRAMES={self.frames}, DAYS={self.days}")
        
        count = 0
        for f in range(self.frames):
            for d in range(self.days + 1):
                src_idx = d * self.frames + f
                dst_idx = d * MAX_FRAMES + f
                
                if src_idx < len(self.data) and dst_idx < MAX_FRAMES * MAX_DAYS_PLUS_1:
                    mp[dst_idx] = int(self.data[src_idx])
                    count += 1
        
        print(f"HF_InitMP5: Инициализировано {count} элементов")
        self.initialized = True


class HF_ReadAdaptiveDays(fg.HostFunction):
    """HostFunction для чтения adaptive_days из MacroProperty после каждого шага"""
    
    def __init__(self):
        super().__init__()
        self.adaptive_days = 1  # Значение по умолчанию
    
    def run(self, FLAMEGPU):
        """Читает adaptive_days из MacroProperty mp_adaptive_result"""
        try:
            mp = FLAMEGPU.environment.getMacroPropertyUInt32("mp_adaptive_result")
            value = int(mp[0])
            if value > 0 and value < 10000:
                self.adaptive_days = value
            else:
                self.adaptive_days = 1
        except Exception as e:
            self.adaptive_days = 1
    
    def get_adaptive_days(self) -> int:
        """Возвращает последнее прочитанное значение"""
        return self.adaptive_days


class AdaptiveV3Orchestrator:
    """
    Оркестратор Adaptive Step v3.
    """
    
    def __init__(self, version_date: str, end_day: int = 3650, 
                 enable_mp2: bool = True, drop_table: bool = False):
        self.version_date = version_date
        self.end_day = end_day
        self.enable_mp2 = enable_mp2
        self.drop_table = drop_table
        
        self.client = get_client()
        self.env_data: Dict = {}
        self.model: fg.ModelDescription = None
        self.simulation: fg.CUDASimulation = None
        self.base_model: V2BaseModelMessaging = None
        
        # Таблица для результатов
        self.output_table = "sim_masterv2_adaptive_v3"
    
    def prepare_environment(self):
        """Подготовка данных окружения."""
        print("\n📊 Подготовка данных окружения")
        print("=" * 60)
        
        self.env_data = prepare_env_arrays(self.client, self.version_date)
        
        # Кумулятивная сумма MP5
        # Данные называются 'mp5_daily_hours_linear' в sim_env_setup
        mp5_lin = self.env_data.get('mp5_daily_hours_linear', np.array([]))
        if len(mp5_lin) == 0:
            mp5_lin = self.env_data.get('mp5_lin', np.array([]))
        
        frames = self.env_data.get('frames_total_u16', 279)
        days = self.end_day
        
        # Сохраняем для использования в init функциях
        self.mp5_lin_data = np.array(mp5_lin, dtype=np.uint32)
        
        if len(mp5_lin) > 0:
            print(f"  Вычисление mp5_cumsum для {frames} агентов × {days} дней...")
            print(f"  📊 mp5_lin: {len(mp5_lin)} элементов, первые 10: {list(mp5_lin[:10])}")
            self.mp5_cumsum = compute_mp5_cumsum(mp5_lin, frames, days)
            print(f"  ✅ mp5_cumsum: {len(self.mp5_cumsum)} элементов")
        else:
            self.mp5_cumsum = np.zeros(frames * (days + 1), dtype=np.uint32)
            print("  ⚠️ mp5_lin пустой, cumsum = zeros")
    
    def build_model(self):
        """Построение модели FLAME GPU."""
        print("\n🔧 Построение модели Adaptive v3")
        print("=" * 60)
        
        # Базовая модель
        self.base_model = V2BaseModelMessaging()
        self.model = self.base_model.create_model(self.env_data)
        
        # Добавляем end_day в environment
        self.base_model.env.newPropertyUInt("end_day", self.end_day)
        
        # MacroProperty для Adaptive v3
        rtc_adaptive_v3.setup_adaptive_v3_macroproperties(self.base_model.env)
        
        # Переменная агента: horizon
        self.base_model.agent.newVariableUInt("horizon", 0xFFFFFFFF)
        
        # ═══════════════════════════════════════════════════════════════════════
        # КРИТИЧНО: Init функции ПЕРВЫМИ (до adaptive модулей)
        # ═══════════════════════════════════════════════════════════════════════
        self._register_init_functions()
        
        # ═══════════════════════════════════════════════════════════════════════
        # Регистрация RTC модулей
        # ═══════════════════════════════════════════════════════════════════════
        
        # 1. Adaptive v3: горизонты и adaptive_days
        rtc_adaptive_v3.register_adaptive_v3(
            self.model, 
            self.base_model.agent,
            self.base_model.quota_agent
        )
        
        # 2. Baseline квотирование
        self._register_quota_modules()
        
        # 3. State managers
        self._register_state_managers()
        
        # 4. Spawn для динамического создания агентов
        self._register_spawn()
        
        # 5. MP2 writer — в Adaptive v3 используем собственный метод collect_mp2_data
        # (RTC MP2 writer не нужен, данные собираем через getPopulationData)
    
    def _register_quota_modules(self):
        """Регистрирует модули квотирования из baseline."""
        print("  Регистрация baseline квотирования...")
        
        rtc_quota_count_ops.register_rtc(self.model, self.base_model.agent)
        rtc_quota_ops_excess.register_rtc(self.model, self.base_model.agent)
        rtc_quota_promote_serviceable.register_rtc(self.model, self.base_model.agent)
        rtc_quota_promote_reserve.register_rtc(self.model, self.base_model.agent)
        rtc_quota_promote_inactive.register_rtc(self.model, self.base_model.agent)
        rtc_quota_repair.register_rtc(self.model, self.base_model.agent)
        
        print("  ✅ Baseline квотирование подключено")
    
    def _register_state_managers(self):
        """Регистрирует state managers."""
        print("  Регистрация state managers...")
        
        rtc_state_manager_operations.register_state_manager_operations(
            self.model, self.base_model.agent)
        rtc_state_manager_serviceable.register_rtc(
            self.model, self.base_model.agent)
        rtc_state_manager_repair.register_state_manager_repair(
            self.model, self.base_model.agent)
        rtc_state_manager_reserve.register_state_manager_reserve(
            self.model, self.base_model.agent)
        rtc_state_manager_inactive.register_state_manager_inactive(
            self.model, self.base_model.agent)
        rtc_state_manager_storage.register_state_manager_storage(
            self.model, self.base_model.agent)
        
        print("  ✅ State managers подключены")
    
    def _register_spawn(self):
        """Регистрирует модуль spawn для динамического создания агентов."""
        print("  Регистрация spawn...")
        
        try:
            import rtc_spawn_v2
            rtc_spawn_v2.register_spawn_v2(self.model, self.base_model.agent)
            print("  ✅ Spawn подключен")
        except ImportError as e:
            print(f"  ⚠️ Spawn недоступен: {e}")
        
        # Финальный слой для чтения adaptive_days
        self.hf_read_adaptive = HF_ReadAdaptiveDays()
        read_layer = self.model.newLayer("z_read_adaptive_days")
        read_layer.addHostFunction(self.hf_read_adaptive)
        
        print("  ✅ Модель построена")
    
    def _register_init_functions(self):
        """Регистрирует HostFunctions для инициализации данных.
        
        КРИТИЧНО: Этот метод должен вызываться ПЕРВЫМ в build_model(),
        до регистрации Adaptive v3 модулей, чтобы mp5_cumsum был 
        инициализирован к моменту вычисления горизонтов.
        """
        print("  Регистрация init функций (ПЕРВЫМИ)...")
        
        frames = self.env_data.get('frames_total_u16', 279)
        days = self.end_day
        
        # Инициализация repair_number_by_idx (нужен для quota_repair)
        self._init_repair_number_buffer()
        
        # Слой инициализации MP5 cumsum
        hf_cumsum = HF_InitMP5Cumsum(self.mp5_cumsum, frames, days)
        init_layer = self.model.newLayer("init_01_mp5_cumsum")
        init_layer.addHostFunction(hf_cumsum)
        
        # Слой инициализации MP5 lin (используем данные из prepare_environment)
        if hasattr(self, 'mp5_lin_data') and len(self.mp5_lin_data) > 0:
            hf_mp5 = HF_InitMP5(self.mp5_lin_data, frames, days)
            init_layer2 = self.model.newLayer("init_02_mp5_lin")
            init_layer2.addHostFunction(hf_mp5)
        
        print(f"  ✅ Init функции зарегистрированы (cumsum: {len(self.mp5_cumsum)} элементов)")
    
    def create_simulation(self) -> fg.CUDASimulation:
        """Создаёт CUDA симуляцию."""
        print("\n🖥️  Создание CUDA симуляции")
        print("=" * 60)
        
        self.simulation = fg.CUDASimulation(self.model)
        
        # Популяция агентов
        pop_builder = AgentPopulationBuilder(self.env_data)
        pop_builder.populate_agents(self.simulation, self.base_model.agent)
        
        # QuotaManager агенты
        quota_pop = fg.AgentVector(self.base_model.quota_agent, 2)
        quota_pop[0].setVariableUChar("group_by", 1)  # Mi-8
        quota_pop[1].setVariableUChar("group_by", 2)  # Mi-17
        self.simulation.setPopulationData(quota_pop)
        
        print("  ✅ Симуляция создана")
        return self.simulation
    
    def _init_repair_number_buffer(self):
        """Инициализирует repair_number_by_idx для quota_repair через слой модели."""
        print("    Инициализация repair_number_by_idx...")
        
        mp3 = self.env_data.get('mp3_arrays', {})
        mp3_group_by = mp3.get('mp3_group_by', [])
        mp3_aircraft_number = mp3.get('mp3_aircraft_number', [])
        frames_index = self.env_data.get('frames_index', {})
        frames_total = model_build.RTC_MAX_FRAMES
        
        # Маппинг frame_idx → group_by
        frame_to_group = {}
        for j in range(len(mp3_aircraft_number)):
            if j < len(mp3_group_by):
                gb = mp3_group_by[j]
                if gb in [1, 2]:
                    ac_num = mp3_aircraft_number[j]
                    if ac_num in frames_index:
                        frame_idx = frames_index[ac_num]
                        frame_to_group[frame_idx] = gb
        
        # repair_number = 18 для планеров
        repair_number_by_idx = []
        for frame_idx in range(frames_total):
            value = 18 if frame_idx in frame_to_group else 0
            repair_number_by_idx.append(value)
        
        # HostFunction для инициализации
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
                print(f"    ✅ HF_InitRepairNumber: {sum(1 for v in self.data if v > 0)} > 0")
        
        hf = HF_InitRepairNumber(repair_number_by_idx)
        init_layer = self.model.newLayer("init_00_repair_number")
        init_layer.addHostFunction(hf)
    
    def run(self):
        """Запуск симуляции с adaptive steps."""
        print(f"\n▶️  Запуск Adaptive v3 симуляции (end_day={self.end_day})")
        print("=" * 60)
        print(f"  MP2 экспорт: {'✅' if self.enable_mp2 else '❌'}")
        
        # Создаём/очищаем таблицу
        if self.enable_mp2:
            self._prepare_output_table()
        
        t_start = time.perf_counter()
        total_gpu_time = 0.0
        total_drain_time = 0.0
        all_mp2_rows = []
        
        current_day = 0
        step_count = 0
        
        while current_day < self.end_day:
            # Устанавливаем current_day
            self.simulation.setEnvironmentPropertyUInt("current_day", current_day)
            self.simulation.setEnvironmentPropertyUInt("quota_enabled", 1)
            
            # GPU шаг
            t_gpu_start = time.perf_counter()
            self.simulation.step()
            t_gpu_end = time.perf_counter()
            total_gpu_time += (t_gpu_end - t_gpu_start)
            
            # Читаем adaptive_days из MacroProperty
            adaptive_days = self._get_adaptive_days()
            
            # MP2 drain
            if self.enable_mp2:
                t_drain_start = time.perf_counter()
                rows = self._collect_mp2_data(current_day, adaptive_days)
                all_mp2_rows.extend(rows)
                t_drain_end = time.perf_counter()
                total_drain_time += (t_drain_end - t_drain_start)
            
            # Логирование
            if step_count % 10 == 0 or adaptive_days > 10:
                print(f"  День {current_day}/{self.end_day}, adaptive={adaptive_days}, "
                      f"GPU: {total_gpu_time:.2f}с, строк: {len(all_mp2_rows)}")
            
            # Переход к следующему дню
            current_day += adaptive_days
            step_count += 1
        
        # Финальный drain в СУБД
        if self.enable_mp2 and all_mp2_rows:
            t_drain_start = time.perf_counter()
            self._drain_to_db(all_mp2_rows)
            t_drain_end = time.perf_counter()
            total_drain_time += (t_drain_end - t_drain_start)
        
        t_end = time.perf_counter()
        elapsed = t_end - t_start
        
        print(f"\n✅ Adaptive v3 симуляция завершена:")
        print(f"  • Шагов: {step_count}")
        print(f"  • Дней: {current_day}")
        print(f"  • Время общее: {elapsed:.2f}с")
        print(f"  • Время GPU: {total_gpu_time:.2f}с ({100*total_gpu_time/elapsed:.1f}%)")
        print(f"  • Время drain: {total_drain_time:.2f}с ({100*total_drain_time/elapsed:.1f}%)")
        print(f"  • Шагов/год: {step_count / 10:.1f}")
        if self.enable_mp2:
            print(f"  • Строк выгружено: {len(all_mp2_rows)}")
        
        return {
            'steps': step_count,
            'days': current_day,
            'elapsed': elapsed,
            'gpu_time': total_gpu_time,
            'drain_time': total_drain_time,
            'rows': len(all_mp2_rows)
        }
    
    def _get_adaptive_days(self) -> int:
        """Читает adaptive_days из HostFunction."""
        if hasattr(self, 'hf_read_adaptive'):
            return self.hf_read_adaptive.get_adaptive_days()
        return 1
    
    def _prepare_output_table(self):
        """Создаёт/очищает выходную таблицу."""
        from clickhouse_driver import Client
        client = Client(
            host=os.environ.get('CLICKHOUSE_HOST', 'localhost'),
            port=int(os.environ.get('CLICKHOUSE_PORT', 9000)),
            user=os.environ.get('CLICKHOUSE_USER', 'default'),
            password=os.environ.get('CLICKHOUSE_PASSWORD', '')
        )
        
        # DDL
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.output_table} (
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
            adaptive_days UInt16
        ) ENGINE = MergeTree()
        ORDER BY (version_date, day_u16, idx)
        """
        client.execute(ddl)
        
        if self.drop_table:
            client.execute(f"TRUNCATE TABLE {self.output_table}")
            print(f"  ✅ Таблица {self.output_table} очищена")
    
    def _collect_mp2_data(self, current_day: int, adaptive_days: int) -> List[Tuple]:
        """Собирает данные для MP2 экспорта через AgentVector."""
        rows = []
        
        version_date_int = int(dt_date.fromisoformat(self.version_date).toordinal() - dt_date(1970, 1, 1).toordinal())
        
        states = ['inactive', 'operations', 'serviceable', 'repair', 'reserve', 'storage']
        
        for state_name in states:
            try:
                # Создаём пустой AgentVector с указанием начального состояния
                pop = fg.AgentVector(self.base_model.agent)
                # Читаем данные для конкретного состояния
                self.simulation.getPopulationData(pop, state_name)
                
                for i in range(pop.size()):
                    agent = pop[i]
                    row = (
                        version_date_int,
                        1,  # version_id
                        current_day,
                        agent.getVariableUInt("idx"),
                        agent.getVariableUInt("aircraft_number"),
                        agent.getVariableUInt("group_by"),
                        state_name,
                        0,  # dt (не используем в adaptive)
                        agent.getVariableUInt("sne"),
                        agent.getVariableUInt("ppr"),
                        agent.getVariableUInt("ll"),
                        agent.getVariableUInt("oh"),
                        agent.getVariableUInt("br"),
                        agent.getVariableUInt("repair_days"),
                        agent.getVariableUInt("repair_time"),
                        adaptive_days
                    )
                    rows.append(row)
            except Exception as e:
                # Логируем для отладки
                if current_day == 0:
                    print(f"    ⚠️ Ошибка чтения состояния '{state_name}': {e}")
        
        return rows
    
    def _drain_to_db(self, rows: List[Tuple]):
        """Выгружает данные в СУБД."""
        from clickhouse_driver import Client
        client = Client(
            host=os.environ.get('CLICKHOUSE_HOST', 'localhost'),
            port=int(os.environ.get('CLICKHOUSE_PORT', 9000)),
            user=os.environ.get('CLICKHOUSE_USER', 'default'),
            password=os.environ.get('CLICKHOUSE_PASSWORD', '')
        )
        
        # Batch insert
        batch_size = 100000
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            client.execute(
                f"INSERT INTO {self.output_table} VALUES",
                batch
            )
        
        print(f"  ✅ Выгружено {len(rows)} строк в {self.output_table}")


def main():
    parser = argparse.ArgumentParser(description='Adaptive Step v3 Orchestrator')
    parser.add_argument('--version-date', required=True, help='Дата версии (YYYY-MM-DD)')
    parser.add_argument('--end-day', type=int, default=3650, help='Конечный день симуляции')
    parser.add_argument('--enable-mp2', action='store_true', help='Включить MP2 экспорт')
    parser.add_argument('--drop-table', action='store_true', help='Очистить таблицу перед записью')
    
    args = parser.parse_args()
    
    orchestrator = AdaptiveV3Orchestrator(
        version_date=args.version_date,
        end_day=args.end_day,
        enable_mp2=args.enable_mp2,
        drop_table=args.drop_table
    )
    
    orchestrator.prepare_environment()
    orchestrator.build_model()
    orchestrator.create_simulation()
    result = orchestrator.run()
    
    print("\n✅ Готово!")
    return result


if __name__ == '__main__':
    main()

