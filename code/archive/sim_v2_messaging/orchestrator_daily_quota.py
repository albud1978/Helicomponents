#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (sim_v2 dead-code cleanup): не используется активным V8-ядром (orchestrator_limiter_v8). Внутренние импорты заморожены.
"""
Оркестратор: ежедневные инкременты + event-driven квотирование

Архитектура:
- Шаг модели = 1 день
- Ежедневные инкременты SNE/PPR (как в baseline)
- Ежедневный MP2 экспорт
- Квотирование только на событиях (~100 раз/год):
  - Изменение программы (program_change_days)
  - Выбытие агента из ops (достиг лимита)

Ожидаемый результат:
- 3650 шагов (ежедневно)
- ~100 квотирований (вместо 3650)
- ~36x меньше тяжёлых операций
"""
import os
import sys
import argparse
import time
from typing import Dict, List, Set
from datetime import date as dt_date

_MESSAGING_DIR = os.path.dirname(__file__)
_SIM_V2_DIR = os.path.join(_MESSAGING_DIR, '..')
_CODE_DIR = os.path.join(_SIM_V2_DIR, '..')

sys.path.insert(0, _MESSAGING_DIR)
sys.path.insert(0, _SIM_V2_DIR)
sys.path.insert(0, _CODE_DIR)

from sim_env_setup import get_client, prepare_env_arrays
from base_model_messaging import V2BaseModelMessaging
import rtc_limiter_date
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


class HF_InitMP4(fg.HostFunction):
    """HostFunction для инициализации mp4_ops_counter MacroProperty"""
    
    def __init__(self, mp4_mi8: list, mp4_mi17: list):
        super().__init__()
        self.mp4_mi8 = mp4_mi8
        self.mp4_mi17 = mp4_mi17
        self.initialized = False
    
    def run(self, FLAMEGPU):
        if self.initialized:
            return
        
        if FLAMEGPU.getStepCounter() > 0:
            return
        
        mp_mi8 = FLAMEGPU.environment.getMacroPropertyUInt32("mp4_ops_counter_mi8")
        mp_mi17 = FLAMEGPU.environment.getMacroPropertyUInt32("mp4_ops_counter_mi17")
        
        for i, val in enumerate(self.mp4_mi8):
            if i < 4001:
                mp_mi8[i] = int(val)
        
        for i, val in enumerate(self.mp4_mi17):
            if i < 4001:
                mp_mi17[i] = int(val)
        
        print(f"HF_InitMP4: Инициализировано {len(self.mp4_mi8)} Mi-8, {len(self.mp4_mi17)} Mi-17 targets")
        self.initialized = True


class DailyQuotaOrchestrator:
    """
    Оркестратор с ежедневными инкрементами и event-driven квотированием
    """
    
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
        
        # Предрасчёт дней квотирования (изменения программы)
        print("\n📊 Предрасчёт quota_event_days...")
        self.quota_event_days: Set[int] = set(
            rtc_limiter_date.precompute_program_changes(clickhouse_client, version_date_str)
        )
        # Добавляем день 0 для начального квотирования
        self.quota_event_days.add(0)
        print(f"  ✅ {len(self.quota_event_days)} событий квотирования")
        
        # Компоненты
        self.population_builder = AgentPopulationBuilder(env_data)
    
    def build_model(self):
        """Строит модель"""
        
        print("\n" + "=" * 60)
        print("🔧 Построение модели: DAILY + EVENT-DRIVEN QUOTA")
        print("=" * 60)
        
        # Создаём базовую модель
        self.model = self.base_model.create_model(self.env_data)
        
        # Environment properties
        self.base_model.env.newPropertyUInt("current_day", 0)
        self.base_model.env.newPropertyUInt("quota_enabled", 1)  # Флаг квотирования
        
        # MacroProperty для mp4_ops_counter (target values)
        self.base_model.env.newMacroPropertyUInt32("mp4_ops_counter_mi8", 4001)
        self.base_model.env.newMacroPropertyUInt32("mp4_ops_counter_mi17", 4001)
        print("  ✅ MacroProperty: mp4_ops_counter_mi8/mi17[4001]")
        
        # HostFunction для инициализации mp4
        mp4_mi8 = list(self.env_data.get('mp4_ops_counter_mi8', [0] * 4001))
        mp4_mi17 = list(self.env_data.get('mp4_ops_counter_mi17', [0] * 4001))
        hf_init_mp4 = HF_InitMP4(mp4_mi8, mp4_mi17)
        layer_init = self.model.newLayer("layer_init_mp4")
        layer_init.addHostFunction(hf_init_mp4)
        print("  ✅ HostFunction для mp4 зарегистрирован")
        
        # Получаем агентов
        heli_agent = self.base_model.agent
        quota_agent = self.base_model.quota_agent
        
        # ═══════════════════════════════════════════════════════════════════════
        # Регистрация RTC модулей
        # ═══════════════════════════════════════════════════════════════════════
        
        # 1. Ежедневные инкременты (используем state_2_operations из baseline)
        print("\n📦 Подключение ежедневных инкрементов...")
        self._register_daily_increments()
        
        # 2. Baseline quota модули
        print("\n📊 Подключение baseline quota модулей...")
        import rtc_quota_count_ops
        import rtc_quota_ops_excess
        import rtc_quota_promote_serviceable
        import rtc_quota_promote_reserve
        import rtc_quota_promote_inactive
        
        rtc_quota_count_ops.register_rtc(self.model, heli_agent)
        rtc_quota_ops_excess.register_rtc(self.model, heli_agent)
        rtc_quota_promote_serviceable.register_rtc(self.model, heli_agent)
        rtc_quota_promote_reserve.register_rtc(self.model, heli_agent)
        rtc_quota_promote_inactive.register_rtc(self.model, heli_agent)
        print("  ✅ Baseline quota модули подключены")
        
        # 3. State managers
        print("\n📦 Подключение state managers...")
        self._register_state_managers()
        
        # 4. MP2 writer (запись в MacroProperty каждый день) + drain в конце
        if self.enable_mp2:
            print("\n📦 Подключение MP2 writer + drain...")
            import rtc_mp2_writer
            rtc_mp2_writer.register_mp2_writer(self.model, heli_agent, self.clickhouse_client)
            
            # HostFunction для финального слива MP2
            from mp2_drain_host import MP2DrainHostFunction
            self.mp2_drain_func = MP2DrainHostFunction(
                self.clickhouse_client,
                table_name='sim_masterv2_daily',
                batch_size=500000,
                simulation_steps=self.end_day
            )
            # Добавляем в финальный слой
            final_layer = self.model.newLayer("layer_mp2_drain_final")
            final_layer.addHostFunction(self.mp2_drain_func)
            
            print("  ✅ rtc_mp2_writer + MP2DrainHostFunction")
        
        print("\n✅ Модель DAILY+EVENT построена")
        print("=" * 60)
        
        return self.model
    
    def _register_daily_increments(self):
        """Регистрирует ежедневные инкременты SNE/PPR"""
        import rtc_state_2_operations
        rtc_state_2_operations.register_rtc(self.model, self.base_model.agent)
        print("  ✅ rtc_state_2_operations (ежедневные инкременты)")
    
    
    def _register_quota_manager_conditional(self, quota_agent):
        """Регистрирует QuotaManager с проверкой quota_enabled"""
        # Модифицированный RTC код с условием
        MAX_EVENTS = 256
        
        RTC_QUOTA_CONDITIONAL = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_quota_manager_conditional, flamegpu::MessageBruteForce, flamegpu::MessageBruteForce) {{
    // Проверяем флаг квотирования
    const unsigned int quota_enabled = FLAMEGPU->environment.getProperty<unsigned int>("quota_enabled");
    if (quota_enabled == 0u) {{
        return flamegpu::ALIVE;  // Пропуск квотирования
    }}
    
    const unsigned char my_group = FLAMEGPU->getVariable<unsigned char>("group_by");
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    
    // Читаем target из mp4_ops_counter
    unsigned int target = 0u;
    if (my_group == 1u) {{
        auto mp4_mi8 = FLAMEGPU->environment.getMacroProperty<unsigned int, 4001u>("mp4_ops_counter_mi8");
        target = mp4_mi8[current_day];
    }} else {{
        auto mp4_mi17 = FLAMEGPU->environment.getMacroProperty<unsigned int, 4001u>("mp4_ops_counter_mi17");
        target = mp4_mi17[current_day];
    }}
    
    // Используем current из агентной переменной QuotaManager
    unsigned int ops_count = FLAMEGPU->getVariable<unsigned int>("current");
    
    // Собираем READY события от агентов (для промоута)
    unsigned int ready_svc = 0u;
    unsigned int svc_idx[{MAX_EVENTS}];
    unsigned int svc_mfg[{MAX_EVENTS}];
    
    for (const auto& msg : FLAMEGPU->message_in) {{
        if (msg.getVariable<unsigned char>("group_by") != my_group) continue;
        
        const unsigned char event_type = msg.getVariable<unsigned char>("event_type");
        const unsigned short idx = msg.getVariable<unsigned short>("idx");
        const unsigned short mfg = msg.getVariable<unsigned short>("mfg_date");
        
        if (event_type == 2u) {{  // READY
            if (ready_svc < {MAX_EVENTS}u) {{
                svc_idx[ready_svc] = idx;
                svc_mfg[ready_svc] = mfg;
                ready_svc++;
            }}
        }}
    }}
    
    // Баланс
    int balance = (int)ops_count - (int)target;
    
    // Демоут в этой архитектуре происходит через intent в rtc_quota_ops_excess
    unsigned int demoted = 0u;
    
    // Промоут (если balance < 0)
    unsigned int promoted = 0u;
    if (balance < 0) {{
        unsigned int deficit = (unsigned int)(-balance);
        unsigned int K = (deficit < ready_svc) ? deficit : ready_svc;
        // Сортируем по mfg_date (youngest first)
        for (unsigned int i = 0u; i < ready_svc && i < K; i++) {{
            unsigned int min_idx = i;
            for (unsigned int j = i + 1u; j < ready_svc; j++) {{
                if (svc_mfg[j] > svc_mfg[min_idx]) min_idx = j;
            }}
            if (min_idx != i) {{
                unsigned int tmp_idx = svc_idx[i]; svc_idx[i] = svc_idx[min_idx]; svc_idx[min_idx] = tmp_idx;
                unsigned int tmp_mfg = svc_mfg[i]; svc_mfg[i] = svc_mfg[min_idx]; svc_mfg[min_idx] = tmp_mfg;
            }}
        }}
        // Отправляем PROMOTE
        for (unsigned int i = 0u; i < K; i++) {{
            FLAMEGPU->message_out.setVariable<unsigned short>("idx", (unsigned short)svc_idx[i]);
            FLAMEGPU->message_out.setVariable<unsigned char>("action", 2u);  // PROMOTE
            FLAMEGPU->message_out.setVariable<unsigned char>("group_by", my_group);
            promoted++;
        }}
    }}
    
    // Логирование
    if (demoted > 0u || promoted > 0u || current_day % 365u == 0u) {{
        printf("[QM Day %u] group=%u: target=%u, ops=%u, balance=%d, demoted=%u, promoted=%u\\n",
               current_day, my_group, target, ops_count, balance, demoted, promoted);
    }}
    
    return flamegpu::ALIVE;
}}
"""
        
        fn = quota_agent.newRTCFunction("rtc_quota_manager_conditional", RTC_QUOTA_CONDITIONAL)
        fn.setMessageInput("PlanerEvent")
        fn.setMessageOutput("QuotaDecision")
        fn.setMessageOutputOptional(True)
        
        layer = self.model.newLayer("layer_quota_manager")
        layer.addAgentFunction(fn)
        
        print("  ✅ rtc_quota_manager_conditional (с проверкой quota_enabled)")
    
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
        """Создаёт симуляцию"""
        print("\n🚀 Создание симуляции...")
        
        self.simulation = fg.CUDASimulation(self.model)
        
        # Загружаем агентов
        self.population_builder.populate_agents(self.simulation, self.base_model.agent)
        
        # Создаём QuotaManager агентов
        self._populate_quota_managers()
        
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
    
    def run(self, max_days: int = 3650):
        """Запускает симуляцию"""
        
        print(f"\n▶️  Запуск DAILY+EVENT симуляции (end_day={self.end_day})")
        print(f"  MP2 экспорт: {'✅ (в MacroProperty, слив в конце)' if self.enable_mp2 else '❌'}")
        print(f"  Событий квотирования: {len(self.quota_event_days)}")
        
        t_start = time.perf_counter()
        t_gpu_start = time.perf_counter()
        
        actual_days = min(max_days, self.end_day)
        
        for day in range(actual_days):
            # Устанавливаем текущий день
            self.simulation.setEnvironmentPropertyUInt("current_day", day)
            
            # Выполняем шаг (все модули: инкременты, квота, state managers, MP2)
            self.simulation.step()
            
            # Логирование
            if day % 365 == 0 or day == actual_days - 1:
                print(f"  День {day}/{actual_days}")
        
        t_gpu_end = time.perf_counter()
        gpu_time = t_gpu_end - t_gpu_start
        
        t_end = time.perf_counter()
        elapsed = t_end - t_start
        
        # Статистика MP2 (если был включен drain)
        total_rows_exported = 0
        drain_time = 0.0
        if self.enable_mp2 and hasattr(self, 'mp2_drain_func'):
            total_rows_exported = self.mp2_drain_func.total_rows_written
            drain_time = self.mp2_drain_func.total_drain_time
        
        print(f"\n✅ DAILY симуляция завершена:")
        print(f"  • Дней: {actual_days}")
        print(f"  • Время GPU: {gpu_time:.2f}с ({actual_days/gpu_time:.1f} дней/сек)")
        if self.enable_mp2:
            print(f"  • Время слива: {drain_time:.2f}с")
            print(f"  • Строк выгружено: {total_rows_exported:,}")
        print(f"  • Общее время: {elapsed:.2f}с")
    
    def _drain_mp2_final(self, end_day: int) -> int:
        """Финальный снимок агентов (только последний день)"""
        if not self.enable_mp2 or not self.clickhouse_client:
            return 0
        
        # Снимаем состояние агентов на последний день
        return self._drain_mp2_single(end_day - 1)
    
    def _drain_mp2_single(self, day: int) -> int:
        """Выгружает MP2 для одного дня"""
        if not self.enable_mp2 or not self.clickhouse_client:
            return 0
        
        states = ['inactive', 'operations', 'serviceable', 'repair', 'reserve', 'storage']
        all_rows = []
        
        vd = dt_date.fromisoformat(self.version_date_str)
        version_date = vd.year * 10000 + vd.month * 100 + vd.day
        version_id = int(self.env_data.get('version_id_u32', 1))
        
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
        
        if all_rows:
            columns = list(all_rows[0].keys())
            values = [[row[col] for col in columns] for row in all_rows]
            col_str = ', '.join(columns)
            self.clickhouse_client.execute(
                f"INSERT INTO sim_masterv2_daily ({col_str}) VALUES",
                values
            )
        
        return len(all_rows)


def main():
    """Точка входа"""
    
    parser = argparse.ArgumentParser(description="V2 Orchestrator: Daily + Event-driven Quota")
    parser.add_argument("--version-date", required=True, help="Дата версии (YYYY-MM-DD)")
    parser.add_argument("--end-day", type=int, default=3650, help="Конечный день")
    parser.add_argument("--enable-mp2", action="store_true", help="MP2 экспорт")
    parser.add_argument("--drop-table", action="store_true", help="Очистить таблицу")
    args = parser.parse_args()
    
    print("=" * 70)
    print("🚀 V2 ORCHESTRATOR — DAILY + EVENT-DRIVEN QUOTA")
    print("=" * 70)
    print(f"  Version date: {args.version_date}")
    print(f"  End day: {args.end_day}")
    print("=" * 70)
    
    # Подключение к СУБД
    client = get_client()
    
    # Создаём таблицу
    if args.enable_mp2:
        client.execute("""
            CREATE TABLE IF NOT EXISTS sim_masterv2_daily (
                version_date UInt32,
                version_id UInt8,
                day_u16 UInt16,
                idx UInt32,
                aircraft_number UInt32,
                group_by UInt8,
                state String,
                dt UInt32,
                sne UInt32,
                ppr UInt32,
                ll UInt32,
                oh UInt32,
                br UInt32,
                repair_days UInt32,
                repair_time UInt32,
                mfg_date UInt32,
                intent_state UInt8
            ) ENGINE = MergeTree()
            ORDER BY (version_date, day_u16, idx)
        """)
        
        if args.drop_table:
            client.execute("TRUNCATE TABLE IF EXISTS sim_masterv2_daily")
            print("✅ Таблица sim_masterv2_daily очищена")
    
    # Загрузка данных
    print("\n📥 Загрузка данных...")
    version_date = dt_date.fromisoformat(args.version_date)
    env_data = prepare_env_arrays(client, version_date)
    
    # Создаём оркестратор
    orchestrator = DailyQuotaOrchestrator(
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
    orchestrator.run(args.end_day)
    
    print("\n✅ Готово!")


if __name__ == "__main__":
    main()

