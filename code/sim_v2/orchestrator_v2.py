#!/usr/bin/env python3
"""
V2 Orchestrator: модульная архитектура с динамической загрузкой RTC модулей
"""
import os
import sys
import json
import argparse
import datetime
import time
from typing import Dict, List, Optional

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from sim_env_setup import get_client, prepare_env_arrays
from base_model import V2BaseModel
from components.agent_population import AgentPopulationBuilder
from components.telemetry_collector import TelemetryCollector
from components.mp5_strategy import MP5StrategyFactory

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


class V2Orchestrator:
    """Оркестратор для управления модульной симуляцией"""
    
    def __init__(self, env_data: Dict[str, object], enable_mp2: bool = False, clickhouse_client = None):
        self.env_data = env_data
        self.base_model = V2BaseModel()
        self.model = None
        self.simulation = None
        
        # Параметры из окружения
        self.frames = int(env_data['frames_total_u16'])
        # Берем реальное количество дней из данных MP5
        self.days = int(env_data.get('days_total_u16', 90))
        # Базовая дата (ord) для вычисления календарного дня
        self.version_date_ord = int(env_data.get('version_date_u16', 0))
        
        # MP2 параметры
        self.enable_mp2 = enable_mp2
        self.clickhouse_client = clickhouse_client
        self.mp2_drain_func = None
        
        # Компоненты
        self.population_builder = AgentPopulationBuilder(env_data)
        self.telemetry: Optional[TelemetryCollector] = None
        self.mp5_strategy = MP5StrategyFactory.create('host_only', env_data, self.frames, self.days)
        
        # Флаг для спавна
        self.spawn_enabled = False
        
    def build_model(self, rtc_modules: List[str]):
        """Строит модель с указанными RTC модулями"""
        print(f"Построение модели с модулями: {', '.join(rtc_modules)}")
        
        # Сохраняем список модулей для проверки в create_simulation
        self.modules = rtc_modules
        
        # Создаем базовую модель
        self.model = self.base_model.create_model(self.env_data)
        
        # MP5 всегда инициализируется, так как используется в функциях состояний
        self.mp5_strategy.register(self.model)
        
        # Создаем слой для обработки состояний
        state_layer = self.model.newLayer('state_processing')
        
        # Подключаем RTC модули
        for module_name in rtc_modules:
            print(f"  Подключение модуля: {module_name}")
            self.base_model.add_rtc_module(module_name)
            
            # Отмечаем если подключён спавн
            if module_name in ("spawn", "spawn_simple", "spawn_v2"):
                self.spawn_enabled = True
            
        # Добавляем MP2 writer если включен
        if self.enable_mp2:
            print("  Подключение MP2 device-side export")
            import rtc_mp2_writer
            self.mp2_drain_func = rtc_mp2_writer.register_mp2_writer(self.model, self.base_model.agent, self.clickhouse_client)
        
        return self.model
    
    def create_simulation(self):
        """Создает и настраивает симуляцию"""
        if not self.model:
            raise RuntimeError("Модель не построена")
            
        self.simulation = fg.CUDASimulation(self.model)
        
        # FIX 1: Environment properties УЖЕ установлены в base_model.create_model()
        # Повторная установка триггерит NVRTC компиляцию и вызывает Error 425
        
        # Создаем популяцию агентов из MP3
        self._populate_agents()
        
        # Инициализируем spawn популяцию если включен
        if self.spawn_enabled:
            # Проверяем какой модуль spawn используется
            if 'spawn_v2' in self.modules:
                from rtc_modules import rtc_spawn_v2
                rtc_spawn_v2.initialize_spawn_population(self.simulation, self.model, self.env_data)
            elif 'spawn_simple' in self.modules:
                from rtc_modules import rtc_spawn_simple
                rtc_spawn_simple.initialize_simple_spawn_population(self.simulation, self.env_data)
            else:
                from rtc_modules import rtc_spawn_integration
                rtc_spawn_integration.initialize_spawn_population(self.simulation, self.env_data)
        
        # Инициализируем телеметрию (по умолчанию включена)
        self.telemetry = TelemetryCollector(
            simulation=self.simulation,
            agent_def=self.base_model.agent,
            version_date_ord=self.version_date_ord,
            enable_state_counts=False,  # отключено по умолчанию
            enable_intent_tracking=True  # включено по умолчанию
        )
        
        return self.simulation
    
    def _populate_agents(self):
        """Загружает агентов через AgentPopulationBuilder (делегирование)"""
        self.population_builder.populate_agents(self.simulation, self.base_model.agent)
    
    def run(self, steps: int):
        """Запускает симуляцию на указанное количество шагов"""
        print(f"Запуск симуляции на {steps} шагов")
        
        # Обновляем количество шагов в MP2 drain функции
        if self.mp2_drain_func:
            self.mp2_drain_func.simulation_steps = steps
        
        # Инициализация телеметрии
        if self.telemetry:
            self.telemetry.before_simulation()
        
        # Основной цикл симуляции
        for step in range(steps):
            if self.telemetry:
                self.telemetry.track_step(step)
            else:
                # Fallback если телеметрия отключена
                self.simulation.step()
    
    def get_results(self):
        """Извлекает результаты симуляции из всех состояний"""
        results = []
        
        # Маппинг state -> status_id для обратной совместимости
        state_to_status = {
            'inactive': 1,
            'operations': 2,
            'serviceable': 3,
            'repair': 4,
            'reserve': 5,
            'storage': 6
        }
        
        # Извлекаем агентов из каждого состояния
        for state_name in state_to_status.keys():
            pop = fg.AgentVector(self.base_model.agent)
            self.simulation.getPopulationData(pop, state_name)
            
            # Подробный вывод по извлечению отключен
            
            for i in range(len(pop)):
                agent = pop[i]
                results.append({
                    'idx': agent.getVariableUInt("idx"),
                    'aircraft_number': agent.getVariableUInt("aircraft_number"),
                    'state': state_name,
                    'sne': agent.getVariableUInt("sne"),
                    'ppr': agent.getVariableUInt("ppr"),
                    'daily_today': agent.getVariableUInt("daily_today_u32"),
                    'daily_next': agent.getVariableUInt("daily_next_u32"),
                    'intent_state': agent.getVariableUInt("intent_state"),
                    'repair_days': agent.getVariableUInt("repair_days"),
                    'll': agent.getVariableUInt("ll"),
                    'oh': agent.getVariableUInt("oh"),
                    'br': agent.getVariableUInt("br")
                })
        
        # Сортируем по idx для удобства
        results.sort(key=lambda x: x['idx'])
        
        # Отладочная информация о пропущенных слотах
        actual_count = len(results)
        expected_count = self.env_data.get('first_reserved_idx', self.frames)
        if actual_count != expected_count:
            print(f"  Внимание: создано {actual_count} агентов из {expected_count} ожидаемых (без учета {self.frames - expected_count} зарезервированных слотов)")
        
        return results


def main():
    """Главная функция оркестратора"""
    parser = argparse.ArgumentParser(description='V2 Orchestrator с модульной архитектурой')
    parser.add_argument('--modules', nargs='+', default=['mp5_probe', 'status_246'],
                      help='Список RTC модулей для подключения')
    parser.add_argument('--steps', type=int, default=None,
                      help='Количество шагов симуляции (по умолчанию из HL_V2_STEPS)')
    parser.add_argument('--enable-mp2', action='store_true',
                      help='Включить MP2 device-side export')
    parser.add_argument('--mp2-drain-interval', type=int, default=0,
                      help='Интервал дренажа MP2 (шаги). 0 = только финальный дренаж')
    parser.add_argument('--drop-table', action='store_true',
                      help='Перед запуском дропнуть таблицу sim_masterv2 (DROP TABLE IF EXISTS)')
    args = parser.parse_args()
    
    # Начало общего времени
    t_total_start = time.perf_counter()
    
    # Загружаем данные
    print("Загрузка данных из ClickHouse...")
    t_data_start = time.perf_counter()
    client = get_client()
    # Опционально дропаем таблицу проекта перед запуском
    if args.drop_table:
        try:
            print("Удаление таблицы sim_masterv2 (DROP TABLE IF EXISTS)...")
            client.execute("DROP TABLE IF EXISTS sim_masterv2")
            print("  Таблица sim_masterv2 удалена (если существовала)")
        except Exception as e:
            print(f"  Ошибка удаления таблицы sim_masterv2: {e}")
            raise

    env_data = prepare_env_arrays(client)
    t_data_load = time.perf_counter() - t_data_start
    print(f"  Данные загружены за {t_data_load:.2f}с")
    
    # Создаем оркестратор с поддержкой MP2
    orchestrator = V2Orchestrator(env_data, enable_mp2=args.enable_mp2,
                                  clickhouse_client=client if args.enable_mp2 else None)
    
    # Строим модель с указанными модулями
    orchestrator.build_model(args.modules)
    
    # Создаем симуляцию
    orchestrator.create_simulation()
    # Настраиваем интервал инкрементального дренажа MP2 (если подключен)
    if args.enable_mp2 and orchestrator.mp2_drain_func:
        try:
            orchestrator.mp2_drain_func.interval_days = max(0, int(args.mp2_drain_interval))
        except Exception:
            pass
    
    # Замеряем время GPU обработки
    t_gpu_start = time.perf_counter()
    
    # Запускаем симуляцию
    steps = args.steps or orchestrator.days
    orchestrator.run(steps)
    
    t_gpu_total = time.perf_counter() - t_gpu_start
    
    # Получаем время дренажа если MP2 включен
    t_db_total = 0.0
    if args.enable_mp2 and orchestrator.mp2_drain_func:
        t_db_total = orchestrator.mp2_drain_func.total_drain_time
    
    # Общее время
    t_total = time.perf_counter() - t_total_start
    
    # Выводим статистику по таймингам
    print(f"\n=== Тайминги выполнения ===")
    print(f"Загрузка модели и данных: {t_data_load:.2f}с")
    print(f"Обработка на GPU: {t_gpu_total:.2f}с")
    if args.enable_mp2:
        print(f"  - в т.ч. выгрузка в СУБД: {t_db_total:.2f}с (параллельно)")
    print(f"Общее время выполнения: {t_total:.2f}с")
    
    # Статистика шагов из телеметрии
    if orchestrator.telemetry:
        timings = orchestrator.telemetry.get_timing_summary()
        print(f"Среднее время на шаг: {timings['mean']*1000:.1f}мс")
        print(f"Шаги: p50={timings['p50']*1000:.1f}мс, p95={timings['p95']*1000:.1f}мс, max={timings['max']*1000:.1f}мс")
    else:
        print(f"Среднее время на шаг: {t_gpu_total/steps*1000:.1f}мс")
    
    # Статистика дренажа MP2
    if args.enable_mp2 and orchestrator.mp2_drain_func:
        d = orchestrator.mp2_drain_func
        rows = getattr(d, 'total_rows_written', 0)
        flushes = getattr(d, 'flush_count', 0)
        t_flush = getattr(d, 'total_flush_time', 0.0)
        max_batch = getattr(d, 'max_batch_rows', 0)
        rps = (rows / t_db_total) if t_db_total > 0 else 0.0
        print(f"Дренаж MP2: rows={rows}, flushes={flushes}, max_batch={max_batch}, flush_time={t_flush:.2f}с, rows/s≈{rps:,.0f}")
    
    # Получаем результаты (без подробного печатного вывода)
    _ = orchestrator.get_results()
    # Итоговую сводку по состояниям не печатаем — по требованию выводим только
    # поминутные/помесячные сводки по шагам и переходы intent в operations
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
