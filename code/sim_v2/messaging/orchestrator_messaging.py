#!/usr/bin/env python3
"""
V2 Orchestrator с Messaging архитектурой

Альтернативная реализация квотирования через нативные FLAME GPU сообщения:
- QuotaManager агенты (2 шт: Mi-8, Mi-17) управляют квотами
- Планеры публикуют PlanerReport и читают QuotaDecision
- Централизованная логика в QuotaManager
"""
import os
import sys
import argparse
import datetime
import time
from typing import Dict, List, Optional

# Добавляем пути для импорта
_MESSAGING_DIR = os.path.dirname(__file__)
_SIM_V2_DIR = os.path.join(_MESSAGING_DIR, '..')
_CODE_DIR = os.path.join(_SIM_V2_DIR, '..')

# Важно: добавляем sim_v2 ПЕРЕД code, чтобы components был пакетом
sys.path.insert(0, _SIM_V2_DIR)
sys.path.insert(0, _CODE_DIR)

from sim_env_setup import get_client, prepare_env_arrays
from base_model_messaging import V2BaseModelMessaging
import rtc_publish_report
import rtc_quota_manager
import rtc_apply_decisions
# Event-driven модули
import rtc_publish_event
import rtc_quota_manager_event

# Импорт компонентов как пакета
from components.agent_population import AgentPopulationBuilder
from components.telemetry_collector import TelemetryCollector
from components.mp5_strategy import MP5StrategyFactory

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


class MessagingOrchestrator:
    """Оркестратор для messaging архитектуры"""
    
    def __init__(self, env_data: Dict[str, object], enable_mp2: bool = False,
                 clickhouse_client = None, table_suffix: str = "_msg",
                 event_driven: bool = False):
        self.env_data = env_data
        self.base_model = V2BaseModelMessaging()
        self.model = None
        self.simulation = None
        self.event_driven = event_driven  # EVENT-DRIVEN режим
        
        # Параметры
        self.frames = int(env_data['frames_total_u16'])
        self.days = int(env_data.get('days_total_u16', 90))
        self.version_date_ord = int(env_data.get('version_date_u16', 0))
        
        # MP2 параметры
        self.enable_mp2 = enable_mp2
        self.clickhouse_client = clickhouse_client
        self.table_suffix = table_suffix  # Суффикс для messaging таблиц
        self.mp2_drain_func = None
        
        # Компоненты
        self.population_builder = AgentPopulationBuilder(env_data)
        self.telemetry: Optional[TelemetryCollector] = None
        
        # MP5 стратегия — загрузка данных лётных часов
        self.mp5_strategy = MP5StrategyFactory.create('host_only', env_data, self.frames, self.days)
    
    def build_model(self):
        """Строит модель с messaging архитектурой"""
        print("=" * 60)
        print("🔧 Построение модели с MESSAGING архитектурой")
        print("=" * 60)
        
        # Создаём базовую модель с сообщениями
        self.model = self.base_model.create_model(self.env_data)
        
        # Получаем агенты
        heli_agent = self.base_model.agent
        quota_agent = self.base_model.quota_agent
        
        # ═══════════════════════════════════════════════════════════════════════
        # MP5 ИНИЦИАЛИЗАЦИЯ (лётные часы)
        # ═══════════════════════════════════════════════════════════════════════
        print("\n📦 Регистрация MP5 стратегии (лётные часы)...")
        self.mp5_strategy.register(self.model)
        print("  ✅ MP5 стратегия зарегистрирована")
        
        # ═══════════════════════════════════════════════════════════════════════
        # ПАЙПЛАЙН MESSAGING
        # ═══════════════════════════════════════════════════════════════════════
        # Слой 1: Все состояния (state functions) — из основной ветки
        # Слой 2: publish_report — планеры публикуют PlanerReport
        # Слой 3: quota_manager — QuotaManager читает и публикует QuotaDecision
        # Слой 4: apply_decisions — планеры применяют решения
        # Слой 5: state_managers — применение переходов (из основной ветки)
        
        # ═══════════════════════════════════════════════════════════════════════
        # СЛОЙ 1: States (базовая логика состояний)
        # ═══════════════════════════════════════════════════════════════════════
        print("\n📦 Подключение модулей состояний...")
        self._register_state_modules()
        
        # ═══════════════════════════════════════════════════════════════════════
        # СЛОЙ 2-4: Messaging (выбор между polling и event-driven)
        # ═══════════════════════════════════════════════════════════════════════
        if self.event_driven:
            print("\n📨 Подключение EVENT-DRIVEN модулей...")
            rtc_publish_event.register_rtc(self.model, heli_agent)
            rtc_quota_manager_event.register_rtc(self.model, quota_agent)
            rtc_apply_decisions.register_rtc(self.model, heli_agent)
        else:
            print("\n📨 Подключение POLLING модулей...")
            rtc_publish_report.register_rtc(self.model, heli_agent)
            rtc_quota_manager.register_rtc(self.model, quota_agent)
            rtc_apply_decisions.register_rtc(self.model, heli_agent)
        
        # ═══════════════════════════════════════════════════════════════════════
        # СЛОЙ 5: State Managers (переходы)
        # ═══════════════════════════════════════════════════════════════════════
        print("\n📦 Подключение state managers...")
        self._register_state_managers()
        
        # ═══════════════════════════════════════════════════════════════════════
        # СЛОЙ 6: Spawn V2 (динамические агенты)
        # ═══════════════════════════════════════════════════════════════════════
        print("\n📦 Подключение spawn_v2...")
        self._register_spawn_v2()
        
        # ═══════════════════════════════════════════════════════════════════════
        # MP2 Export (если включен)
        # ═══════════════════════════════════════════════════════════════════════
        if self.enable_mp2:
            self._setup_mp2_export()
        
        print("\n✅ Модель с messaging построена")
        print("=" * 60)
        
        return self.model
    
    def _register_state_modules(self):
        """Регистрирует модули обработки состояний (из основной ветки)"""
        # Модули состояний находятся в sim_v2 (не в rtc_modules)
        import rtc_state_2_operations
        import rtc_states_stub
        
        # Регистрация
        rtc_states_stub.register_rtc(self.model, self.base_model.agent)
        rtc_state_2_operations.register_rtc(self.model, self.base_model.agent)
        
        print("  ✅ Модули состояний подключены")
    
    def _register_state_managers(self):
        """Регистрирует state managers (переходы состояний)"""
        # State managers находятся в sim_v2 (не в rtc_modules)
        import rtc_state_manager_serviceable
        import rtc_state_manager_operations
        import rtc_state_manager_repair
        import rtc_state_manager_storage
        import rtc_state_manager_reserve
        import rtc_state_manager_inactive
        
        # Регистрация в правильном порядке
        # Разные модули имеют разные имена функций
        rtc_state_manager_serviceable.register_rtc(self.model, self.base_model.agent)
        rtc_state_manager_operations.register_state_manager_operations(self.model, self.base_model.agent)
        rtc_state_manager_repair.register_state_manager_repair(self.model, self.base_model.agent)
        rtc_state_manager_storage.register_state_manager_storage(self.model, self.base_model.agent)
        rtc_state_manager_reserve.register_state_manager_reserve(self.model, self.base_model.agent)
        rtc_state_manager_inactive.register_state_manager_inactive(self.model, self.base_model.agent)
        
        print("  ✅ State managers подключены")
    
    def _register_spawn_v2(self):
        """Регистрирует spawn_v2 для динамических агентов"""
        from rtc_modules import rtc_spawn_v2
        
        rtc_spawn_v2.register_rtc(self.model, self.base_model.agent, self.env_data)
        self.spawn_enabled = True
        
        print("  ✅ spawn_v2 зарегистрирован")
    
    def _initialize_spawn_population(self):
        """Инициализирует spawn агентов после создания симуляции"""
        if not getattr(self, 'spawn_enabled', False):
            return
            
        from rtc_modules import rtc_spawn_v2
        rtc_spawn_v2.initialize_spawn_population(self.simulation, self.model, self.env_data)
        
        print("  ✅ Spawn популяция инициализирована")
    
    def _setup_mp2_export(self):
        """Настройка MP2 экспорта в messaging таблицы"""
        print("\n📤 Настройка MP2 экспорта...")
        
        # Используем MP2 writer из основной ветки, но с другой таблицей
        sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
        import rtc_mp2_writer
        from mp2_drain_host import MP2DrainHostFunction
        
        # Регистрируем writer
        rtc_mp2_writer.register_mp2_writer(self.model, self.base_model.agent, self.clickhouse_client)
        
        # Создаём drain с messaging таблицей
        table_name = f"sim_masterv2{self.table_suffix}"
        self.mp2_drain_func = MP2DrainHostFunction(
            self.clickhouse_client,
            table_name=table_name,
            batch_size=500000,
            simulation_steps=self.days
        )
        
        layer_drain = self.model.newLayer("mp2_final_drain")
        layer_drain.addHostFunction(self.mp2_drain_func)
        
        print(f"  ✅ MP2 экспорт: таблица {table_name}")
    
    def create_simulation(self):
        """Создаёт и настраивает симуляцию"""
        if not self.model:
            raise RuntimeError("Модель не построена")
        
        print("\n🚀 Создание симуляции...")
        self.simulation = fg.CUDASimulation(self.model)
        
        # Создаём популяцию планеров
        self._populate_planers()
        
        # Создаём QuotaManager агентов
        self._populate_quota_managers()
        
        # Инициализируем spawn популяцию (если включен)
        self._initialize_spawn_population()
        
        # Инициализируем телеметрию
        self.telemetry = TelemetryCollector(
            simulation=self.simulation,
            agent_def=self.base_model.agent,
            version_date_ord=self.version_date_ord,
            enable_state_counts=True,
            enable_intent_tracking=True
        )
        
        print("  ✅ Симуляция создана")
        return self.simulation
    
    def _populate_planers(self):
        """Загружает планеры через AgentPopulationBuilder"""
        self.population_builder.populate_agents(self.simulation, self.base_model.agent)
    
    def _populate_quota_managers(self):
        """Создаёт 2 QuotaManager агента (Mi-8, Mi-17) с начальным curr_ops"""
        print("  📊 Создание QuotaManager агентов...")
        
        # Подсчитываем начальное количество в operations по типам
        initial_ops = self.population_builder.get_initial_ops_count()
        mi8_ops = initial_ops.get(1, 0)  # group_by=1
        mi17_ops = initial_ops.get(2, 0)  # group_by=2
        
        quota_pop = fg.AgentVector(self.base_model.quota_agent)
        
        # Mi-8 QuotaManager
        quota_pop.push_back()
        mi8_mgr = quota_pop[len(quota_pop) - 1]
        mi8_mgr.setVariableUInt8("group_by", 1)
        mi8_mgr.setVariableUInt("target", 0)
        mi8_mgr.setVariableUInt("current", mi8_ops)  # Начальное количество
        mi8_mgr.setVariableInt("balance", 0)
        mi8_mgr.setVariableUInt("remaining_deficit", 0)
        
        # Mi-17 QuotaManager
        quota_pop.push_back()
        mi17_mgr = quota_pop[len(quota_pop) - 1]
        mi17_mgr.setVariableUInt8("group_by", 2)
        mi17_mgr.setVariableUInt("target", 0)
        mi17_mgr.setVariableUInt("current", mi17_ops)  # Начальное количество
        mi17_mgr.setVariableInt("balance", 0)
        mi17_mgr.setVariableUInt("remaining_deficit", 0)
        
        self.simulation.setPopulationData(quota_pop)
        print(f"  ✅ QuotaManager: Mi-8 (curr={mi8_ops}), Mi-17 (curr={mi17_ops})")
    
    def run(self, steps: int):
        """Запускает симуляцию"""
        print(f"\n▶️  Запуск симуляции на {steps} шагов")
        t_start = time.perf_counter()
        
        # Обновляем шаги в drain
        if self.mp2_drain_func:
            self.mp2_drain_func.simulation_steps = steps
        
        # Инициализация телеметрии
        if self.telemetry:
            self.telemetry.before_simulation()
        
        # Основной цикл
        for step in range(steps):
            if self.telemetry:
                self.telemetry.track_step(step)
            else:
                self.simulation.step()
            
            # Логирование каждые 100 шагов
            if step > 0 and step % 100 == 0:
                self._log_progress(step)
        
        t_total = time.perf_counter() - t_start
        print(f"\n✅ Симуляция завершена за {t_total:.2f}с ({steps} шагов)")
        
        # Статистика производительности
        print(f"  • Среднее время шага: {(t_total/steps)*1000:.1f}мс")
        print(f"  • Шагов/сек: {steps/t_total:.1f}")
    
    def _log_progress(self, step: int):
        """Логирует прогресс симуляции"""
        # Получаем данные QuotaManager
        try:
            quota_pop = fg.AgentVector(self.base_model.quota_agent)
            self.simulation.getPopulationData(quota_pop)
            
            if len(quota_pop) >= 2:
                mi8 = quota_pop[0]
                mi17 = quota_pop[1]
                
                mi8_curr = mi8.getVariableUInt16("current")
                mi8_target = mi8.getVariableUInt16("target")
                mi17_curr = mi17.getVariableUInt16("current")
                mi17_target = mi17.getVariableUInt16("target")
                
                print(f"  [Day {step}] Mi-8: {mi8_curr}/{mi8_target}, Mi-17: {mi17_curr}/{mi17_target}")
        except Exception as e:
            print(f"  [Day {step}] (не удалось получить данные QuotaManager: {e})")
    
    def get_results(self):
        """Извлекает результаты симуляции"""
        results = []
        
        state_to_status = {
            'inactive': 1,
            'operations': 2,
            'serviceable': 3,
            'repair': 4,
            'reserve': 5,
            'storage': 6
        }
        
        for state_name, status_id in state_to_status.items():
            pop = fg.AgentVector(self.base_model.agent)
            self.simulation.getPopulationData(pop, state_name)
            
            for i in range(len(pop)):
                agent = pop[i]
                results.append({
                    'idx': agent.getVariableUInt("idx"),
                    'aircraft_number': agent.getVariableUInt("aircraft_number"),
                    'state': state_name,
                    'status_id': status_id,
                    'intent_state': agent.getVariableUInt("intent_state"),
                    'sne': agent.getVariableUInt("sne"),
                    'ppr': agent.getVariableUInt("ppr"),
                })
        
        return results


def main():
    """Точка входа для messaging оркестратора"""
    parser = argparse.ArgumentParser(description="V2 Orchestrator с Messaging архитектурой")
    parser.add_argument("--version-date", required=True, help="Дата версии (YYYY-MM-DD)")
    parser.add_argument("--steps", type=int, default=90, help="Количество шагов симуляции")
    parser.add_argument("--enable-mp2", action="store_true", help="Включить MP2 экспорт")
    parser.add_argument("--drop-table", action="store_true", help="Очистить таблицу перед записью")
    parser.add_argument("--event-driven", action="store_true", help="Использовать event-driven архитектуру")
    args = parser.parse_args()
    
    mode = "EVENT-DRIVEN" if args.event_driven else "POLLING"
    print("=" * 70)
    print(f"🚀 V2 ORCHESTRATOR — MESSAGING ARCHITECTURE ({mode})")
    print("=" * 70)
    print(f"  Version date: {args.version_date}")
    print(f"  Steps: {args.steps}")
    print(f"  MP2 export: {args.enable_mp2}")
    print(f"  Mode: {mode}")
    print("=" * 70)
    
    # Получаем клиент СУБД
    client = get_client()
    
    # Подготовка данных окружения
    print("\n📥 Загрузка данных...")
    # Парсим дату
    from datetime import date
    version_date = date.fromisoformat(args.version_date)
    env_data = prepare_env_arrays(client, version_date)
    # args.steps используется для ограничения симуляции, но days_total берём из данных
    
    # Создаём дублирующие таблицы если нужно
    if args.enable_mp2 and args.drop_table:
        print("\n🗑️  Очистка messaging таблиц...")
        client.execute("TRUNCATE TABLE IF EXISTS sim_masterv2_msg")
    
    # Создаём оркестратор
    orchestrator = MessagingOrchestrator(
        env_data=env_data,
        enable_mp2=args.enable_mp2,
        clickhouse_client=client,
        table_suffix="_msg",
        event_driven=args.event_driven
    )
    
    # Строим модель и создаём симуляцию
    orchestrator.build_model()
    orchestrator.create_simulation()
    
    # Запускаем симуляцию
    orchestrator.run(args.steps)
    
    # Выводим результаты
    results = orchestrator.get_results()
    
    # Статистика по состояниям
    state_counts = {}
    for r in results:
        state = r['state']
        state_counts[state] = state_counts.get(state, 0) + 1
    
    print("\n📊 Финальное распределение по состояниям:")
    for state, count in sorted(state_counts.items()):
        print(f"  {state}: {count}")
    
    print("\n✅ Готово!")


if __name__ == "__main__":
    main()

