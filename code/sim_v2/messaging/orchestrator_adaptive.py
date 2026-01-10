#!/usr/bin/env python3
"""
V2 Orchestrator с АДАПТИВНЫМ ШАГОМ (DES + ABM гибрид)

Особенности:
- step_days вычисляется динамически (1-365 дней)
- Батчевые инкременты sne/ppr/repair_days
- Предрасчёт событий (program_changes, mp5_cumsum)
- Интеграция с event-driven messaging
- MP2 экспорт с интерполяцией промежуточных дней

Запуск:
    python3 orchestrator_adaptive.py --version-date 2025-07-04 --end-day 3650 --enable-mp2
"""
import os
import sys
import argparse
import time
from typing import Dict, Optional, List

# Добавляем пути для импорта
_MESSAGING_DIR = os.path.dirname(__file__)
_SIM_V2_DIR = os.path.join(_MESSAGING_DIR, '..')
_CODE_DIR = os.path.join(_SIM_V2_DIR, '..')

sys.path.insert(0, _SIM_V2_DIR)
sys.path.insert(0, _CODE_DIR)

from sim_env_setup import get_client, prepare_env_arrays
from base_model_messaging import V2BaseModelMessaging
from precompute_events import EventPrecomputer, compute_mp5_cumsum, find_program_change_days
from host_adaptive_step import AdaptiveStepHostFunction, UpdateDayHostFunction
import rtc_batch_operations
import rtc_publish_event
import rtc_quota_manager_event
import rtc_apply_decisions

from components.agent_population import AgentPopulationBuilder
from components.telemetry_collector import TelemetryCollector

try:
    import pyflamegpu as fg
    import numpy as np
except ImportError as e:
    raise RuntimeError(f"Зависимости не установлены: {e}")


class AdaptiveOrchestrator:
    """Оркестратор для адаптивного шага симуляции"""
    
    def __init__(self, env_data: Dict, end_day: int = 3650,
                 enable_mp2: bool = False, clickhouse_client=None, 
                 version_date_str: str = "2025-07-04"):
        self.env_data = env_data
        self.end_day = end_day
        self.enable_mp2 = enable_mp2
        self.clickhouse_client = clickhouse_client
        self.version_date_str = version_date_str  # "2025-07-04"
        
        self.base_model = V2BaseModelMessaging()
        self.model = None
        self.simulation = None
        
        # Параметры
        self.frames = int(env_data['frames_total_u16'])
        self.days = int(env_data.get('days_total_u16', 3650))
        
        # Предрасчёт событий
        print("\n📊 Предрасчёт событий для адаптивного шага...")
        self.event_precomputer = EventPrecomputer(env_data)
        
        # Кумулятивные суммы dt
        mp5_lin = env_data.get('mp5_lin', np.zeros(self.frames * self.days, dtype=np.uint32))
        self.mp5_cumsum = compute_mp5_cumsum(mp5_lin, self.frames, self.days)
        print(f"  ✅ mp5_cumsum: {len(self.mp5_cumsum)} элементов")
        
        # Изменения программы
        mp4_mi8 = env_data.get('mp4_ops_counter_mi8', [])
        mp4_mi17 = env_data.get('mp4_ops_counter_mi17', [])
        self.program_changes = find_program_change_days(mp4_mi8, mp4_mi17)
        print(f"  ✅ program_changes: {len(self.program_changes)} событий")
        
        # Компоненты
        self.population_builder = AgentPopulationBuilder(env_data)
    
    def build_model(self):
        """Строит модель с адаптивным шагом"""
        
        print("\n" + "=" * 60)
        print("🔧 Построение модели с АДАПТИВНЫМ ШАГОМ")
        print("=" * 60)
        
        # Создаём базовую модель (с messaging)
        self.model = self.base_model.create_model(self.env_data)
        
        # Добавляем Environment properties для адаптивного шага
        self.base_model.env.newPropertyUInt("current_day", 0)
        self.base_model.env.newPropertyUInt("step_days", 1)
        self.base_model.env.newPropertyUInt("end_day", self.end_day)
        
        # MacroProperty для mp5_cumsum — используем MAX размеры для совместимости с RTC
        import model_build
        cumsum_size = model_build.RTC_MAX_FRAMES * (model_build.MAX_DAYS + 1)
        self.base_model.env.newMacroPropertyUInt32("mp5_cumsum", cumsum_size)
        print(f"  ✅ MacroProperty mp5_cumsum: {cumsum_size} элементов (MAX размер)")
        
        # Получаем агентов
        heli_agent = self.base_model.agent
        quota_agent = self.base_model.quota_agent
        
        # ═══════════════════════════════════════════════════════════════════════
        # Регистрация RTC модулей
        # ═══════════════════════════════════════════════════════════════════════
        
        # 1. Батчевые инкременты (вместо ежедневных)
        print("\n📦 Подключение batch модулей...")
        rtc_batch_operations.register_rtc(self.model, heli_agent)
        
        # 2. Event-driven messaging (после батчевых инкрементов)
        print("\n📨 Подключение event-driven модулей...")
        rtc_publish_event.register_rtc(self.model, heli_agent)
        rtc_quota_manager_event.register_rtc(self.model, quota_agent)
        rtc_apply_decisions.register_rtc(self.model, heli_agent)
        
        # 3. State managers (применение переходов)
        print("\n📦 Подключение state managers...")
        self._register_state_managers()
        
        print("\n✅ Модель с адаптивным шагом построена")
        print("=" * 60)
        
        return self.model
    
    def _register_state_managers(self):
        """Регистрирует state managers из основной ветки"""
        
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
        """Создаёт симуляцию и инициализирует популяции"""
        
        print("\n🚀 Создание симуляции...")
        
        self.simulation = fg.CUDASimulation(self.model)
        
        # Загружаем агентов
        self.population_builder.populate_agents(self.simulation, self.base_model.agent)
        
        # Создаём QuotaManager агентов
        self._populate_quota_managers()
        
        # Инициализируем mp5_cumsum в MacroProperty
        self._init_mp5_cumsum()
        
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
    
    def _init_mp5_cumsum(self):
        """Инициализирует mp5_cumsum в MacroProperty"""
        
        # ПРИМЕЧАНИЕ: FLAME GPU не позволяет напрямую записать MacroProperty из host
        # Нужно использовать HostFunction для инициализации
        # Для MVP используем альтернативный подход через Environment array
        
        # Альтернатива: храним mp5_cumsum как PropertyArray (если размер позволяет)
        # Или используем отдельный HostFunction для инициализации
        
        print(f"  ⚠️ mp5_cumsum требует инициализации через HostFunction")
    
    def run(self, max_steps: int = 10000):
        """Запускает симуляцию с адаптивным шагом"""
        
        print(f"\n▶️  Запуск адаптивной симуляции (end_day={self.end_day}, max_steps={max_steps})")
        print(f"  MP2 экспорт: {'✅ включён' if self.enable_mp2 else '❌ выключен'}")
        
        t_start = time.perf_counter()
        
        current_day = 0
        step_count = 0
        total_days_simulated = 0
        total_rows_exported = 0
        
        # Батч для MP2 экспорта
        mp2_batch_start = 0
        mp2_batch_interval = 365  # Выгружаем раз в год
        
        while current_day < self.end_day and step_count < max_steps:
            # Вычисляем step_days
            step_days = self._compute_step_days(current_day)
            step_days = min(step_days, self.end_day - current_day)
            
            if step_days <= 0:
                break
            
            # ═══════════════════════════════════════════════════════════════
            # КРИТИЧНО: Устанавливаем step_days и current_day в Environment
            # Это определяет длительность шага для RTC функций!
            # ═══════════════════════════════════════════════════════════════
            self.simulation.setEnvironmentPropertyUInt("current_day", current_day)
            self.simulation.setEnvironmentPropertyUInt("step_days", step_days)
            
            # Выполняем шаг симуляции (RTC читает step_days из Environment)
            self.simulation.step()
            
            # Обновляем счётчики
            prev_day = current_day
            current_day += step_days
            step_count += 1
            total_days_simulated += step_days
            
            # MP2 экспорт после каждого шага (с интерполяцией)
            if self.enable_mp2:
                rows = self._drain_mp2_adaptive(prev_day, current_day - 1)
                total_rows_exported += rows if rows else 0
            
            # Логирование
            if step_count % 10 == 0 or step_days > 10:
                print(f"  Step {step_count}: day={current_day}, step_days={step_days}")
        
        # Финальный экспорт последнего дня
        if self.enable_mp2 and current_day > 0:
            rows = self._drain_mp2_adaptive(current_day, current_day)
            total_rows_exported += rows if rows else 0
        
        t_end = time.perf_counter()
        elapsed = t_end - t_start
        
        print(f"\n✅ Симуляция завершена:")
        print(f"  • Шагов: {step_count}")
        print(f"  • Дней: {total_days_simulated}")
        print(f"  • Время: {elapsed:.2f}с")
        print(f"  • Дней/сек: {total_days_simulated / elapsed:.1f}")
        print(f"  • Шагов/сек: {step_count / elapsed:.1f}")
        if self.enable_mp2:
            print(f"  • Строк выгружено: {total_rows_exported}")
    
    def _compute_step_days(self, current_day: int) -> int:
        """Вычисляет step_days (упрощённая версия для MVP)"""
        
        # 1. Программный лимитер
        program_days = 999999
        for day, mi8, mi17 in self.program_changes:
            if day > current_day:
                program_days = day - current_day
                break
        
        # 2. Для MVP: фиксированный шаг 30 дней (упрощение)
        # В полной версии здесь будет расчёт ресурсных и ремонтных лимитеров
        
        step_days = min(program_days, 30)  # Максимум 30 дней за шаг
        step_days = max(step_days, 1)
        
        return step_days
    
    def _extract_agent_snapshot(self, heli_pop, state_name: str, day: int) -> List[Dict]:
        """Извлекает данные агентов в указанном состоянии"""
        rows = []
        count = heli_pop.size()
        
        for i in range(count):
            agent = heli_pop.at(i)
            rows.append({
                'day_u16': day,
                'idx': agent.getVariableUInt('idx'),
                'aircraft_number': agent.getVariableUInt('aircraft_number'),
                'group_by': agent.getVariableUInt('group_by'),
                'state': state_name,
                'dt': agent.getVariableUInt('daily_today_u32'),  # dt из daily_today
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
        return rows
    
    def _drain_mp2_adaptive(self, start_day: int, end_day: int):
        """Выгружает MP2 данные с интерполяцией"""
        
        if not self.enable_mp2 or not self.clickhouse_client:
            return
        
        print(f"  📤 Выгрузка MP2: дни {start_day}-{end_day}...")
        
        # Получаем состояния агентов
        states = ['inactive', 'operations', 'serviceable', 'repair', 'reserve', 'storage']
        
        all_rows = []
        # Конвертируем строку даты в YYYYMMDD формат
        from datetime import date as dt_date
        vd = dt_date.fromisoformat(self.version_date_str)
        version_date = vd.year * 10000 + vd.month * 100 + vd.day
        version_id = int(self.env_data.get('version_id_u32', 1))
        
        for state_name in states:
            heli_pop = fg.AgentVector(self.base_model.agent)
            self.simulation.getPopulationData(heli_pop, state_name)
            for day in range(start_day, end_day + 1):
                # Для каждого дня в интервале записываем одно состояние
                # (интерполяция — состояние не меняется внутри шага)
                rows = self._extract_agent_snapshot(heli_pop, state_name, day)
                for row in rows:
                    row['version_date'] = version_date
                    row['version_id'] = version_id
                    all_rows.append(row)
        
        if all_rows:
            # Батчевая вставка в ClickHouse
            columns = list(all_rows[0].keys())
            values = [[row[col] for col in columns] for row in all_rows]
            
            col_str = ', '.join(columns)
            self.clickhouse_client.execute(
                f"INSERT INTO sim_masterv2_adaptive ({col_str}) VALUES",
                values
            )
            print(f"    ✅ Выгружено {len(all_rows)} строк")
        
        return len(all_rows)


def main():
    """Точка входа"""
    
    parser = argparse.ArgumentParser(description="V2 Orchestrator с адаптивным шагом")
    parser.add_argument("--version-date", required=True, help="Дата версии (YYYY-MM-DD)")
    parser.add_argument("--end-day", type=int, default=3650, help="Конечный день симуляции")
    parser.add_argument("--max-steps", type=int, default=10000, help="Максимум шагов")
    parser.add_argument("--enable-mp2", action="store_true", help="Включить MP2 экспорт")
    args = parser.parse_args()
    
    print("=" * 70)
    print("🚀 V2 ORCHESTRATOR — ADAPTIVE STEP (DES + ABM)")
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
    orchestrator = AdaptiveOrchestrator(
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

