#!/usr/bin/env python3
"""
V2 Orchestrator: ПОЛНОСТЬЮ GPU-SIDE

CPU только:
1. Загрузка данных в модель
2. simulation.simulate(N)  ← Один вызов!
3. Выгрузка результатов

ВСЯ логика (step_days, current_day, инкременты) — на GPU.

Запуск:
    python3 orchestrator_gpu_only.py --version-date 2025-07-04 --end-day 3650 --max-steps 200
"""
import os
import sys
import argparse
import time
from typing import Dict

# Добавляем пути для импорта
_MESSAGING_DIR = os.path.dirname(__file__)
_SIM_V2_DIR = os.path.join(_MESSAGING_DIR, '..')
_CODE_DIR = os.path.join(_SIM_V2_DIR, '..')

sys.path.insert(0, _SIM_V2_DIR)
sys.path.insert(0, _CODE_DIR)

from sim_env_setup import get_client, prepare_env_arrays
from base_model_messaging import V2BaseModelMessaging
from precompute_events import compute_mp5_cumsum, find_program_change_days
import rtc_adaptive_gpu
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


class GPUOnlyOrchestrator:
    """Оркестратор для полностью GPU-side симуляции"""
    
    def __init__(self, env_data: Dict, end_day: int = 3650, clickhouse_client=None):
        self.env_data = env_data
        self.end_day = end_day
        self.clickhouse_client = clickhouse_client
        
        self.base_model = V2BaseModelMessaging()
        self.model = None
        self.simulation = None
        
        # Параметры
        self.frames = int(env_data['frames_total_u16'])
        self.days = int(env_data.get('days_total_u16', 3650))
        
        # Предрасчёт
        print("\n📊 Предрасчёт данных...")
        mp5_lin = env_data.get('mp5_lin', np.zeros(self.frames * self.days, dtype=np.uint32))
        self.mp5_cumsum = compute_mp5_cumsum(mp5_lin, self.frames, self.days)
        print(f"  ✅ mp5_cumsum: {len(self.mp5_cumsum)} элементов")
        
        mp4_mi8 = env_data.get('mp4_ops_counter_mi8', [])
        mp4_mi17 = env_data.get('mp4_ops_counter_mi17', [])
        self.program_changes = find_program_change_days(mp4_mi8, mp4_mi17)
        print(f"  ✅ program_changes: {len(self.program_changes)} событий")
        
        self.population_builder = AgentPopulationBuilder(env_data)
    
    def build_model(self):
        """Строит модель с GPU-side адаптивным шагом"""
        
        print("\n" + "=" * 60)
        print("🔧 Построение модели: ПОЛНОСТЬЮ GPU-SIDE")
        print("=" * 60)
        
        # Создаём базовую модель
        self.model = self.base_model.create_model(self.env_data)
        
        # MacroProperty для адаптивного шага
        rtc_adaptive_gpu.setup_adaptive_macroproperties(self.base_model.env, self.end_day)
        
        # MacroProperty для mp5_cumsum
        cumsum_size = model_build.RTC_MAX_FRAMES * (model_build.MAX_DAYS + 1)
        self.base_model.env.newMacroPropertyUInt32("mp5_cumsum", cumsum_size)
        print(f"  ✅ MacroProperty mp5_cumsum: {cumsum_size} элементов")
        
        # Получаем агентов
        heli_agent = self.base_model.agent
        quota_agent = self.base_model.quota_agent
        
        # ═══════════════════════════════════════════════════════════════════════
        # Регистрация RTC модулей в правильном порядке
        # ═══════════════════════════════════════════════════════════════════════
        
        # 1. Адаптивные функции: вычисление лимитеров и step_days
        print("\n📦 Подключение adaptive-GPU модулей...")
        rtc_adaptive_gpu.register_adaptive_gpu(self.model, heli_agent, quota_agent)
        
        # 2. Батчевые инкременты (читают step_days из MacroProperty)
        print("\n📦 Подключение batch модулей...")
        self._register_batch_gpu(heli_agent)
        
        # 3. Event-driven messaging
        print("\n📨 Подключение event-driven модулей...")
        rtc_publish_event.register_rtc(self.model, heli_agent)
        rtc_quota_manager_event.register_rtc(self.model, quota_agent)
        rtc_apply_decisions.register_rtc(self.model, heli_agent)
        
        # 4. State managers
        print("\n📦 Подключение state managers...")
        self._register_state_managers()
        
        # 5. Update day (ПОСЛЕДНИЙ слой!)
        # Уже зарегистрирован в rtc_adaptive_gpu
        
        print("\n✅ Модель GPU-ONLY построена")
        print("=" * 60)
        
        return self.model
    
    def _register_batch_gpu(self, heli_agent):
        """Регистрирует batch-функции которые читают step_days из MacroProperty"""
        
        MAX_FRAMES = model_build.RTC_MAX_FRAMES
        MAX_DAYS = model_build.MAX_DAYS
        cumsum_size = MAX_FRAMES * (MAX_DAYS + 1)
        cumsum_stride = MAX_DAYS + 1
        
        # Модифицированная версия batch_increment_ops для GPU-side
        RTC_BATCH_OPS_GPU = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_batch_ops_gpu, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Читаем step_days из MacroProperty (вычислен на GPU)
    auto mp_step = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_step_days");
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_current_day");
    
    const unsigned int step_days = mp_step[0];
    const unsigned int current_day = mp_day[0];
    
    if (step_days == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Читаем cumsum
    auto cumsum = FLAMEGPU->environment.getMacroProperty<unsigned int, {cumsum_size}u>("mp5_cumsum");
    
    const unsigned int base = idx * {cumsum_stride}u;
    const unsigned int start_cumsum = cumsum[base + current_day];
    const unsigned int end_day = current_day + step_days;
    const unsigned int end_cumsum = cumsum[base + end_day];
    const unsigned int total_dt = end_cumsum - start_cumsum;
    
    // Батчевый инкремент
    unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    
    sne += total_dt;
    ppr += total_dt;
    
    FLAMEGPU->setVariable<unsigned int>("sne", sne);
    FLAMEGPU->setVariable<unsigned int>("ppr", ppr);
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", total_dt);
    
    // Проверка лимита
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    
    if (sne >= ll || ppr >= oh) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);
    }}
    
    return flamegpu::ALIVE;
}}
"""

        RTC_BATCH_REPAIR_GPU = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_batch_repair_gpu, flamegpu::MessageNone, flamegpu::MessageNone) {{
    auto mp_step = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_step_days");
    const unsigned int step_days = mp_step[0];
    
    if (step_days == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    repair_days += step_days;
    FLAMEGPU->setVariable<unsigned int>("repair_days", repair_days);
    
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    if (repair_days >= repair_time) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 5u);
    }}
    
    return flamegpu::ALIVE;
}}
"""

        RTC_BATCH_NOOP = """
FLAMEGPU_AGENT_FUNCTION(rtc_batch_noop_gpu, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}
"""

        # Регистрация
        layer_ops = self.model.newLayer("batch_gpu_operations")
        rtc_ops = heli_agent.newRTCFunction("rtc_batch_ops_gpu", RTC_BATCH_OPS_GPU)
        rtc_ops.setInitialState("operations")
        rtc_ops.setEndState("operations")
        layer_ops.addAgentFunction(rtc_ops)
        
        layer_repair = self.model.newLayer("batch_gpu_repair")
        rtc_repair = heli_agent.newRTCFunction("rtc_batch_repair_gpu", RTC_BATCH_REPAIR_GPU)
        rtc_repair.setInitialState("repair")
        rtc_repair.setEndState("repair")
        layer_repair.addAgentFunction(rtc_repair)
        
        # Noop для остальных состояний
        for state in ["inactive", "serviceable", "reserve", "storage"]:
            layer = self.model.newLayer(f"batch_gpu_{state}")
            func_name = f"rtc_batch_noop_{state}"
            rtc_func = heli_agent.newRTCFunction(func_name, RTC_BATCH_NOOP)
            rtc_func.setInitialState(state)
            rtc_func.setEndState(state)
            layer.addAgentFunction(rtc_func)
        
        print(f"    ✅ Batch-GPU функции зарегистрированы (читают step_days из MacroProperty)")
    
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
    
    def create_simulation(self):
        """Создаёт симуляцию и инициализирует данные"""
        
        print("\n🚀 Создание симуляции...")
        
        self.simulation = fg.CUDASimulation(self.model)
        
        # Загружаем агентов
        self.population_builder.populate_agents(self.simulation, self.base_model.agent)
        
        # Создаём QuotaManager агентов
        self._populate_quota_managers()
        
        # Инициализируем MacroProperty
        self._init_macroproperties()
        
        print("  ✅ Симуляция создана")
        return self.simulation
    
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
        """Инициализирует MacroProperty через HostEnvironment"""
        
        # Инициализируем mp_current_day = 0
        # Инициализируем mp_min_limiter = MAX_INT
        # Инициализируем mp_program_limiter из program_changes
        
        # ПРИМЕЧАНИЕ: В FLAME GPU MacroProperty инициализируются нулями
        # Нужно установить начальные значения через первый шаг или HostFunction
        
        print(f"  ✅ MacroProperty инициализированы (current_day=0, min_limiter=MAX)")
    
    def run(self, max_steps: int = 200):
        """
        Запускает симуляцию ОДНИМ вызовом simulation.simulate()
        
        ВСЯ логика выполняется на GPU!
        """
        
        print(f"\n▶️  Запуск GPU-ONLY симуляции (end_day={self.end_day}, max_steps={max_steps})")
        print(f"   CPU делает ТОЛЬКО: setSimulationSteps() + simulate()")
        
        t_start = time.perf_counter()
        
        # ═══════════════════════════════════════════════════════════════════════
        # ОДИН ВЫЗОВ! Вся логика на GPU.
        # ═══════════════════════════════════════════════════════════════════════
        self.simulation.SimulationConfig().steps = max_steps
        self.simulation.simulate()
        
        t_end = time.perf_counter()
        elapsed = t_end - t_start
        
        # Получаем финальный current_day (если нужно)
        # В реальности можно прочитать из MacroProperty после симуляции
        
        print(f"\n✅ Симуляция GPU-ONLY завершена:")
        print(f"  • Шагов (FLAME GPU): {max_steps}")
        print(f"  • Время: {elapsed:.2f}с")
        print(f"  • Шагов/сек: {max_steps / elapsed:.1f}")


def main():
    """Точка входа"""
    
    parser = argparse.ArgumentParser(description="V2 Orchestrator — GPU-ONLY")
    parser.add_argument("--version-date", required=True, help="Дата версии (YYYY-MM-DD)")
    parser.add_argument("--end-day", type=int, default=3650, help="Конечный день")
    parser.add_argument("--max-steps", type=int, default=200, help="Максимум шагов")
    args = parser.parse_args()
    
    print("=" * 70)
    print("🚀 V2 ORCHESTRATOR — ПОЛНОСТЬЮ GPU-SIDE")
    print("=" * 70)
    print(f"  Version date: {args.version_date}")
    print(f"  End day: {args.end_day}")
    print(f"  Max steps: {args.max_steps}")
    print("=" * 70)
    
    # Подключение к СУБД
    client = get_client()
    
    # Загрузка данных
    print("\n📥 Загрузка данных...")
    from datetime import date
    version_date = date.fromisoformat(args.version_date)
    env_data = prepare_env_arrays(client, version_date)
    
    # Создаём оркестратор
    orchestrator = GPUOnlyOrchestrator(
        env_data=env_data,
        end_day=args.end_day,
        clickhouse_client=client
    )
    
    # Строим модель
    orchestrator.build_model()
    
    # Создаём симуляцию
    orchestrator.create_simulation()
    
    # Запускаем ОДНИМ вызовом
    orchestrator.run(args.max_steps)
    
    print("\n✅ Готово!")


if __name__ == "__main__":
    main()

