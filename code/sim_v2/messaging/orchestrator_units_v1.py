#!/usr/bin/env python3
# EXPERIMENTAL / REFERENCE (2026-06-06): пробный L2-контур (group_by=3/4) в messaging. Не production. Боевой L2 — code/sim_v2/units/orchestrator_units.py. Оставлен как справочный черновик.
"""
Оркестратор L2 engines (group_by=3/4) в messaging-контуре.

Запуск:
    python orchestrator_units_v1.py --version-date 2025-07-04 --steps 3650 --group-scope 3,4

Дата: 26.02.2026
"""

import os
import sys
import time
import argparse
from datetime import date, datetime
from typing import Dict, Optional, List

import numpy as np

# Пути
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SIM_V2_DIR = os.path.join(SCRIPT_DIR, '..')
CODE_DIR = os.path.join(SIM_V2_DIR, '..')
UNITS_DIR = os.path.join(SIM_V2_DIR, 'units')

sys.path.insert(0, SCRIPT_DIR)
sys.path.insert(0, SIM_V2_DIR)
sys.path.insert(0, CODE_DIR)
sys.path.insert(0, UNITS_DIR)


def setup_environment():
    """Настройка переменных окружения для CUDA/RTC"""
    project_root = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..', '..'))

    rtc_cache = os.environ.get('FLAMEGPU2_RTC_CACHE', os.path.join(project_root, '.rtc_cache'))
    os.environ['FLAMEGPU2_RTC_CACHE'] = rtc_cache
    if not os.path.exists(rtc_cache):
        os.makedirs(rtc_cache)

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

from base_model_units_v1 import V1BaseModelUnits
from agent_population_units_v1 import AgentPopulationUnitsBuilderV1
from planer_l2_loader import load_planer_signals

from utils.config_loader import get_clickhouse_client
from sim_v2.units import rtc_units_increment
from sim_v2.units import rtc_units_fifo_priority
from sim_v2.units import rtc_units_transition_reserve
from sim_v2.units import rtc_units_state_repair

from mp2_drain_units_v1 import MP2DrainUnitsHostFunction

import rtc_units_save_pre_state_v1
import rtc_units_planner_sync_v1
import rtc_units_transition_ops_v1
import rtc_units_transition_serviceable_v1
import rtc_units_mp2_writer_v1
from l2_fullkit_postprocess import apply_l2_fullkit_postprocess


class InitPlanerSignalsHostFunction(fg.HostFunction):
    """HostFunction для инициализации mp_planer_* и mp_ac_to_idx"""

    def __init__(self, dt_array, status_array, assembly_array, ac_to_idx):
        super().__init__()
        self.dt_array = dt_array
        self.status_array = status_array
        self.assembly_array = assembly_array
        self.ac_to_idx = ac_to_idx
        self.initialized = False

    def _load_array(self, mp, arr, name: str, nonzero_only: bool = True):
        if arr is None or len(arr) == 0:
            print(f"     ⚠️ {name}: пустой массив")
            return
        count = 0
        for i, val in enumerate(arr):
            if nonzero_only and int(val) == 0:
                continue
            mp[i] = int(val)
            count += 1
        print(f"     {name}: записано {count:,} значений")

    def run(self, FLAMEGPU):
        if self.initialized:
            return
        if FLAMEGPU.getStepCounter() != 0:
            return

        print("  📥 InitPlanerSignals: загрузка mp_planer_* и mp_ac_to_idx...")

        try:
            mp_dt = FLAMEGPU.environment.getMacroPropertyUInt("mp_planer_dt")
            self._load_array(mp_dt, self.dt_array, "mp_planer_dt")
        except Exception as e:
            print(f"     ⚠️ mp_planer_dt: {e}")

        try:
            mp_status = FLAMEGPU.environment.getMacroPropertyUInt("mp_planer_status")
            self._load_array(mp_status, self.status_array, "mp_planer_status")
        except Exception as e:
            print(f"     ⚠️ mp_planer_status: {e}")

        try:
            mp_assembly = FLAMEGPU.environment.getMacroPropertyUInt("mp_planer_assembly_trigger")
            self._load_array(mp_assembly, self.assembly_array, "mp_planer_assembly_trigger")
        except Exception as e:
            print(f"     ⚠️ mp_planer_assembly_trigger: {e}")

        try:
            if self.ac_to_idx:
                mp_ac = FLAMEGPU.environment.getMacroPropertyUInt("mp_ac_to_idx")
                for ac_num, idx in self.ac_to_idx.items():
                    if ac_num < 2000000:
                        mp_ac[ac_num] = idx
                print(f"     mp_ac_to_idx: {len(self.ac_to_idx)} маппингов")
        except Exception as e:
            print(f"     ⚠️ mp_ac_to_idx: {e}")

        self.initialized = True
        print("  ✅ InitPlanerSignals: завершено")


def register_init_planer_signals(model, dt_array, status_array, assembly_array, ac_to_idx):
    hf = InitPlanerSignalsHostFunction(dt_array, status_array, assembly_array, ac_to_idx)
    layer = model.newLayer("layer_init_planer_signals")
    layer.addHostFunction(hf)
    print("  ✅ InitPlanerSignals зарегистрирован (layer_init_planer_signals)")
    return hf


class UnitsOrchestratorV1:
    """Оркестратор симуляции агрегатов (L2 engines)"""

    def __init__(self, version_date: date, version_id: int = 1, group_scope: List[int] = None):
        self.version_date = version_date
        self.version_id = version_id
        self.group_scope = group_scope or [3, 4]

        self.base_model: Optional[V1BaseModelUnits] = None
        self.simulation: Optional[fg.CUDASimulation] = None
        self.env_data: Dict = {}
        self.mp2_drain_fn = None

        self.population_builder = None

        self.timing = {
            'load': 0.0,
            'build': 0.0,
            'populate': 0.0,
            'simulate': 0.0,
            'total': 0.0
        }

    def load_data(self):
        t0 = time.time()

        print("=" * 60)
        print(f"📊 ЗАГРУЗКА ДАННЫХ L2 ENGINES (group_by={self.group_scope})")
        print(f"   Дата версии: {self.version_date}")
        print("=" * 60)

        population_builder = AgentPopulationUnitsBuilderV1(
            self.version_date,
            self.version_id,
            group_scope=self.group_scope
        )
        self.env_data = population_builder.load_data()
        self.population_builder = population_builder

        # Планерные сигналы
        try:
            dt_array, status_array, assembly_array, ac_to_idx = load_planer_signals(
                str(self.version_date), self.version_id
            )
            self.env_data['planer_dt_array'] = dt_array
            self.env_data['planer_status_array'] = status_array
            self.env_data['planer_assembly_trigger_array'] = assembly_array
            self.env_data['ac_to_idx'] = ac_to_idx or {}
            if ac_to_idx:
                print(f"   ✅ Планерные сигналы: {len(ac_to_idx)} планеров")
        except Exception as e:
            print(f"   ⚠️ Не удалось загрузить планерные сигналы: {e}")
            self.env_data['planer_dt_array'] = None
            self.env_data['planer_status_array'] = None
            self.env_data['planer_assembly_trigger_array'] = None
            self.env_data['ac_to_idx'] = {}

        self.timing['load'] = time.time() - t0
        print(f"✅ Данные загружены за {self.timing['load']:.2f}с")

    def build_model(self):
        t0 = time.time()

        print("\n" + "=" * 60)
        print("🔧 ПОСТРОЕНИЕ МОДЕЛИ L2 ENGINES")
        print("=" * 60)

        self.base_model = V1BaseModelUnits()
        model = self.base_model.create_model(self.env_data)
        agent = self.base_model.agent

        max_frames = int(self.env_data.get('units_frames_total', 12000))
        max_days = int(self.env_data.get('days_total_u16', 3650))
        max_groups = int(self.env_data.get('max_groups', 50))
        self._max_groups = max_groups

        modules_ok = 0
        modules_failed = 0

        # 0. InitPlanerSignals — загрузка dt/status/assembly в MacroProperty
        try:
            dt_array = self.env_data.get('planer_dt_array')
            status_array = self.env_data.get('planer_status_array')
            assembly_array = self.env_data.get('planer_assembly_trigger_array')
            ac_to_idx = self.env_data.get('ac_to_idx', {})
            if dt_array is not None or status_array is not None:
                register_init_planer_signals(model, dt_array, status_array, assembly_array, ac_to_idx)
                modules_ok += 1
        except Exception as e:
            print(f"  ❌ init_planer_signals: {e}")
            modules_failed += 1

        # 1. pre_state snapshot (до любых переходов)
        try:
            rtc_units_save_pre_state_v1.register_rtc(model, agent)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_save_pre_state_v1: {e}")
            modules_failed += 1

        # 2. planner sync
        try:
            rtc_units_planner_sync_v1.register_rtc(model, agent, max_days)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_planner_sync_v1: {e}")
            modules_failed += 1

        # 3. increment + check_limits
        try:
            rtc_units_increment.register_rtc(model, agent, max_days)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_increment: {e}")
            modules_failed += 1

        # 4. fifo_priority (EP1/EP2/EP4 + spawn)
        try:
            rtc_units_fifo_priority.register_rtc(
                model, agent, max_frames, max_days, max_groups=max_groups
            )
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_fifo_priority: {e}")
            modules_failed += 1

        # 5. transition ops
        try:
            rtc_units_transition_ops_v1.register_rtc(model, agent)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_transition_ops_v1: {e}")
            modules_failed += 1

        # 6. transition serviceable
        try:
            rtc_units_transition_serviceable_v1.register_rtc(model, agent)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_transition_serviceable_v1: {e}")
            modules_failed += 1

        # 7. transition reserve (stable)
        try:
            rtc_units_transition_reserve.register_rtc(model, agent, max_frames)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_transition_reserve: {e}")
            modules_failed += 1

        # 8. state_repair
        try:
            rtc_units_state_repair.register_rtc(model, agent)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_state_repair: {e}")
            modules_failed += 1

        # 9. mp2 writer
        drain_interval = 10
        try:
            rtc_units_mp2_writer_v1.register_rtc(model, agent, max_frames, max_days, drain_interval)
            modules_ok += 1
        except Exception as e:
            print(f"  ❌ units_mp2_writer: {e}")
            modules_failed += 1

        # 10. mp2 drain
        try:
            client = get_clickhouse_client()
            self.mp2_drain_fn = MP2DrainUnitsHostFunction(
                client=client,
                table_name='sim_units_v2',
                batch_size=500000,
                simulation_steps=max_days,
                version_date=self.version_date,
                version_id=self.version_id
            )
            model.addStepFunction(self.mp2_drain_fn)
            print("  RTC модуль mp2_drain зарегистрирован (drain каждые 10 дней)")
            modules_ok += 1
        except Exception as e:
            print(f"  ⚠️ mp2_drain: {e} (история будет только финальная)")

        self.timing['build'] = time.time() - t0
        print(f"✅ Модель построена за {self.timing['build']:.2f}с")
        print(f"   Модулей OK: {modules_ok}, Failed: {modules_failed}")

    def populate_agents(self):
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

        self._init_fifo_macroproperty()

        self.timing['populate'] = time.time() - t0
        print(f"✅ Агенты инициализированы за {self.timing['populate']:.2f}с")

    def _init_fifo_macroproperty(self):
        svc_tails = getattr(self.population_builder, 'svc_tails', {})
        rsv_tails = getattr(self.population_builder, 'rsv_tails', {})

        self._svc_tails = svc_tails
        self._rsv_tails = rsv_tails

        total_svc = sum(svc_tails.values())
        total_rsv = sum(rsv_tails.values())
        print(f"   FIFO очереди: svc={total_svc}, rsv={total_rsv}")

    def _init_fifo_on_first_step(self):
        try:
            max_groups = getattr(self, "_max_groups", 50)
            for gb, tail in self._svc_tails.items():
                if gb < max_groups and tail > 0:
                    self.simulation.environment.setMacroPropertyUInt32("mp_svc_tail", gb, tail)

            for gb, tail in self._rsv_tails.items():
                if gb < max_groups and tail > 0:
                    self.simulation.environment.setMacroPropertyUInt32("mp_rsv_tail", gb, tail)

            total_svc = sum(self._svc_tails.values())
            total_rsv = sum(self._rsv_tails.values())
            print(f"   ✅ FIFO очереди инициализированы: svc={total_svc}, rsv={total_rsv}")
        except Exception as e:
            print(f"   ⚠️ Ошибка инициализации FIFO: {e}")
            print("      (очереди будут инициализированы через HostFunction)")

    def run(self, steps: int = 100):
        t0 = time.time()

        print("\n" + "=" * 60)
        print(f"🚀 ЗАПУСК СИМУЛЯЦИИ НА {steps} ШАГОВ")
        print("=" * 60)

        self._init_fifo_on_first_step()

        step_times = []

        for step in range(steps):
            step_t0 = time.time()
            self.simulation.step()
            step_time = time.time() - step_t0
            step_times.append(step_time)

            if (step + 1) % 100 == 0:
                avg_time = sum(step_times[-100:]) / min(100, len(step_times))
                print(f"  [День {step + 1:4d}] avg={avg_time*1000:.1f}мс")

        self.timing['simulate'] = time.time() - t0

        if step_times:
            avg_step = sum(step_times) / len(step_times)
            max_step = max(step_times)
            min_step = min(step_times)
            print(f"\n✅ Симуляция завершена за {self.timing['simulate']:.2f}с")
            print(f"   Среднее время шага: {avg_step*1000:.2f}мс")
            print(f"   Мин/Макс: {min_step*1000:.2f}мс / {max_step*1000:.2f}мс")

    def print_summary(self):
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


def parse_group_scope(scope_str: str) -> List[int]:
    parts = [p.strip() for p in scope_str.split(",") if p.strip()]
    scope = [int(p) for p in parts if p.isdigit()]
    scope = sorted({x for x in scope if x > 0})
    if not scope:
        raise ValueError("group-scope пуст — укажите хотя бы один group_by")
    return scope


def parse_args():
    parser = argparse.ArgumentParser(description='Симуляция L2 engines (units)')
    parser.add_argument('--version-date', type=str, required=True,
                       help='Дата версии данных (YYYY-MM-DD)')
    parser.add_argument('--version-id', type=int, default=1,
                       help='ID версии данных (по умолчанию 1)')
    parser.add_argument('--steps', type=int, default=3650,
                       help='Количество шагов симуляции (по умолчанию 3650)')
    parser.add_argument('--group-scope', type=str, default="3,4",
                       help='group_by для агрегатов (по умолчанию "3,4")')
    parser.add_argument('--export', action='store_true',
                       help='Экспортировать результаты в ClickHouse')
    return parser.parse_args()


def main():
    args = parse_args()
    version_date = datetime.strptime(args.version_date, '%Y-%m-%d').date()
    group_scope = parse_group_scope(args.group_scope)

    print("=" * 60)
    print("🚁 СИМУЛЯЦИЯ L2 ENGINES (UNITS) V1")
    print(f"   Дата версии: {version_date}")
    print(f"   Шагов: {args.steps}")
    print(f"   group_scope: {group_scope}")
    print("=" * 60)

    orchestrator = UnitsOrchestratorV1(version_date, args.version_id, group_scope)

    try:
        orchestrator.load_data()
        orchestrator.build_model()
        orchestrator.populate_agents()
        orchestrator.run(args.steps)
        orchestrator.print_summary()

        if args.export:
            print("\n" + "=" * 60)
            print("📤 ФИНАЛЬНЫЙ DRAIN MP2 В CLICKHOUSE")
            print("=" * 60)
            if orchestrator.mp2_drain_fn is not None:
                print("   🔄 Запуск финального drain step...")
                orchestrator.simulation.step()
                print(f"   ✅ Итого записей: {orchestrator.mp2_drain_fn.total_rows_written:,}")
                print(f"   ⏱️ Время drain: {orchestrator.mp2_drain_fn.total_drain_time:.2f}с")
            else:
                print("   ⚠️ mp2_drain не инициализирован")

            print("\n" + "=" * 60)
            print("🧩 L2 FULLKIT POSTPROCESS (planner ops -> engines)")
            print("=" * 60)
            planner_version_date = int(version_date.strftime("%Y%m%d"))
            units_version_date_int = (version_date - date(1970, 1, 1)).days
            inserted = apply_l2_fullkit_postprocess(
                planner_version_date=planner_version_date,
                units_version_date_int=units_version_date_int,
                version_id=args.version_id,
            )
            print(f"   ✅ Synthetic rows inserted: {inserted:,}")
        return 0

    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
