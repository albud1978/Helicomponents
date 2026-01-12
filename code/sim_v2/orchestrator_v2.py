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
    
    def __init__(self, env_data: Dict[str, object], enable_mp2: bool = False, 
                 enable_mp2_postprocess: bool = False, clickhouse_client = None):
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
        self.enable_mp2_postprocess = enable_mp2_postprocess
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
        
        # Инициализация repair_number_by_idx для quota_repair
        if 'quota_repair' in self.modules:
            self._init_repair_number_buffer()
        
        # Создаем слой для обработки состояний
        state_layer = self.model.newLayer('state_processing')
        
        # Если MP2 включен, создаём MacroProperties ДО регистрации модулей
        # (spawn_v2 и другие модули используют mp2_transition_*)
        if self.enable_mp2:
            import rtc_mp2_writer
            rtc_mp2_writer.setup_mp2_macroproperties(self.model)
        
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
            # Проверяем, не был ли mp2_writer уже подключен как модуль
            if "mp2_writer" not in rtc_modules:
                self.mp2_drain_func = rtc_mp2_writer.register_mp2_writer(self.model, self.base_model.agent, self.clickhouse_client)
            else:
                print("  ⚠️  mp2_writer уже подключен в списке модулей, пропускаем повторное подключение")
            
            # Transition флаги вычисляются на GPU
            print("  ✅ Transition флаги вычисляются на GPU (слой compute_transitions → MacroProperty)")
            
            # Добавляем постпроцессинг active_trigger (если включен)
            if self.enable_mp2_postprocess:
                print("  Подключение GPU постпроцессинга MP2 (active_trigger → repair history)")
                import rtc_mp2_postprocess_active
                rtc_mp2_postprocess_active.register_mp2_postprocess_active(self.model, self.base_model.agent)
                print("  ⚠️  Слой mp2_postprocess_active будет активен ТОЛЬКО при export_phase=2")
            
            # Создаём финальный MP2 drain который работает только в конце
            if self.enable_mp2:
                print("  Подключение финального дренажа MP2 (батчи в конце симуляции)")
                from mp2_drain_host import MP2DrainHostFunction
                # interval_days=0 (по умолчанию) означает дренаж ТОЛЬКО в конце
                self.mp2_drain_func = MP2DrainHostFunction(
                    self.clickhouse_client,
                    table_name='sim_masterv2',
                    batch_size=500000,
                    simulation_steps=self.days
                )
                # Добавляем в слой модели - будет вызывать run() на каждом шаге
                # но дренаж происходит только когда step == simulation_steps - 1 (финальный шаг)
                # На шагах 0..simulation_steps-2 проверка if is_final очень дешева
                layer_drain = self.model.newLayer("mp2_final_drain")
                layer_drain.addHostFunction(self.mp2_drain_func)
        else:
            pass
        
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
        
        # Инициализируем динамический spawn если включен
        if 'spawn_dynamic' in self.modules:
            from rtc_modules import rtc_spawn_dynamic
            rtc_spawn_dynamic.init_population(self.simulation, self.model, self.env_data)
            print("  ✅ Динамический spawn инициализирован")
        
        # Инициализируем телеметрию (по умолчанию включена)
        self.telemetry = TelemetryCollector(
            simulation=self.simulation,
            agent_def=self.base_model.agent,
            version_date_ord=self.version_date_ord,
            enable_state_counts=False,  # отключено по умолчанию
            enable_intent_tracking=True  # включено по умолчанию
        )
        
        return self.simulation
    
    def _init_repair_number_buffer(self):
        """Инициализирует MacroProperty repair_number_by_idx для quota_repair"""
        print("  Инициализация repair_number_by_idx для quota_repair...")

        # Подготавливаем данные: для каждого frame_idx получаем repair_number
        mp1_index = self.env_data.get('mp1_index', {})
        mp1_repair_number = self.env_data.get('mp1_repair_number', [])
        mp3 = self.env_data.get('mp3_arrays', {})
        mp3_partseqno = mp3.get('mp3_partseqno_i', [])
        mp3_aircraft_number = mp3.get('mp3_aircraft_number', [])
        mp3_group_by = mp3.get('mp3_group_by', [])
        frames_index = self.env_data.get('frames_index', {})
        
        frames_total = int(self.env_data['frames_total_u16'])
        
        # Диагностика данных
        glider_count_mp3 = sum(1 for gb in mp3_group_by if gb == 1 or gb == 2)
        unique_gb = set(mp3_group_by) if mp3_group_by else set()
        
        print(f"  📋 Диагностика входных данных:")
        print(f"     - mp1_index size: {len(mp1_index)}")
        print(f"     - mp1_repair_number size: {len(mp1_repair_number)}")
        print(f"     - mp3_partseqno size: {len(mp3_partseqno)}")
        print(f"     - mp3_group_by size: {len(mp3_group_by)}")
        print(f"     - frames_index size: {len(frames_index)}")
        print(f"     - Планеров в MP3 (group_by=1,2): {glider_count_mp3}")
        print(f"     - Уникальные group_by в MP3: {sorted(unique_gb)}")
        print(f"     - frames_total: {frames_total}")
        
        # Строим маппинг frame_idx → partseqno_i для планеров
        frame_to_partseqno = {}
        for j in range(len(mp3_aircraft_number)):
            if j < len(mp3_group_by):
                gb = mp3_group_by[j]
                if gb in [1, 2]:  # Только планеры
                    ac = mp3_aircraft_number[j]
                    if ac in frames_index:
                        frame_idx = frames_index[ac]
                        partseqno = mp3_partseqno[j] if j < len(mp3_partseqno) else 0
                        frame_to_partseqno[frame_idx] = partseqno
        
        print(f"     - Построен маппинг frame_idx → partseqno для {len(frame_to_partseqno)} планеров")
        
        # Проверка ненулевых значений в mp1_repair_number
        non_zero_in_mp1 = sum(1 for x in mp1_repair_number if x > 0 and x != 255)
        print(f"     - mp1_repair_number с значениями > 0 (не 255): {non_zero_in_mp1}")
        if non_zero_in_mp1 > 0:
            unique_rn = set(x for x in mp1_repair_number if x > 0 and x != 255)
            print(f"     - Уникальные repair_number: {sorted(unique_rn)}")
            # Покажем первые 10 partseqno с repair_number > 0
            partseqno_list = list(mp1_index.keys())
            sample = []
            for psn in partseqno_list[:50]:
                pidx = mp1_index.get(psn, -1)
                if 0 <= pidx < len(mp1_repair_number):
                    rn = mp1_repair_number[pidx]
                    if rn > 0 and rn != 255:
                        sample.append((psn, pidx, rn))
                        if len(sample) >= 10:
                            break
            if sample:
                print(f"     - Образцы (partseqno, pidx, repair_number):")
                for psn, pidx, rn in sample:
                    print(f"         partseqno={psn}, pidx={pidx}, repair_number={rn}")
        
        # Создаём массив repair_number по idx (frame_idx)
        repair_number_by_idx = []
        non_zero_count = 0
        repair_numbers_found = set()
        missing_count = 0
        sample_mismatches = []
        glider_samples = []  # Для планеров
        
        for frame_idx in range(frames_total):
            # Используем маппинг frame_idx → partseqno
            partseqno = frame_to_partseqno.get(frame_idx, 0)
            
            if partseqno > 0:
                pidx = mp1_index.get(partseqno, -1)
                
                if pidx >= 0 and pidx < len(mp1_repair_number):
                    rn = mp1_repair_number[pidx]
                    # 0 означает отсутствие квоты (или SENTINEL 255)
                    value = 0 if rn == 255 else int(rn)
                    repair_number_by_idx.append(value)
                    if value > 0:
                        non_zero_count += 1
                        repair_numbers_found.add(value)
                    
                    # Собираем образцы планеров для диагностики
                    if len(glider_samples) < 10:
                        glider_samples.append((frame_idx, partseqno, pidx, rn, value))
                else:
                    repair_number_by_idx.append(0)
                    missing_count += 1
                    if len(sample_mismatches) < 10:
                        sample_mismatches.append((frame_idx, partseqno, pidx))
            else:
                # Будущий слот для spawn или не найдено
                repair_number_by_idx.append(0)
        
        print(f"  📊 Статистика repair_number:")
        print(f"     - Агентов с repair_number > 0: {non_zero_count}/{frames_total}")
        print(f"     - Уникальные значения: {sorted(repair_numbers_found)}")
        print(f"     - Агентов БЕЗ соответствия в mp1_index: {missing_count}/{frames_total}")
        if sample_mismatches:
            print(f"     - Образцы несоответствий (frame_idx, partseqno, pidx):")
            for fi, psn, pi in sample_mismatches[:5]:
                print(f"         frame_idx={fi}, partseqno={psn}, pidx={pi} (НЕ найден в mp1_index)")
        
        # Показываем планеры
        print(f"     - Количество планеров в выборке: {len(glider_samples)}")
        if glider_samples:
            print(f"     - Образцы ПЛАНЕРОВ (frame_idx, partseqno, pidx, rn_raw, rn_final):")
            for fi, psn, pi, rn_raw, rn_final in glider_samples:
                print(f"         frame={fi}, psn={psn}, pidx={pi}, rn={rn_raw}, final={rn_final}")
        
        # Создаём HostFunction для инициализации
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
        
        # Добавляем HostFunction в первый слой модели
        hf = HF_InitRepairNumber(repair_number_by_idx)
        init_layer = self.model.newLayer()
        init_layer.addHostFunction(hf)
        
        print(f"  ✅ repair_number_by_idx инициализирован ({len(repair_number_by_idx)} элементов)")
    
    def _populate_agents(self):
        """Загружает агентов через AgentPopulationBuilder (делегирование)"""
        self.population_builder.populate_agents(self.simulation, self.base_model.agent)
    
    def run(self, steps: int):
        """Запускает симуляцию на указанное количество шагов"""
        print(f"Запуск симуляции на {steps} шагов")
        
        # Отладка (по умолчанию выключена):
        # - ORCH_DEBUG_SPAWN=1 включает логирование "после динамического spawn"
        # - Параметры можно тонко настроить переменными окружения ниже
        debug_spawn = os.getenv("ORCH_DEBUG_SPAWN") == "1"
        debug_spawn_day = int(os.getenv("ORCH_DEBUG_SPAWN_DAY", "302"))
        debug_acn_min = int(os.getenv("ORCH_DEBUG_ACN_MIN", "100006"))
        debug_limit = int(os.getenv("ORCH_DEBUG_LIMIT", "5"))
        
        # Обновляем количество шагов в MP2 drain функции
        # Если включён постпроцессинг, дренаж будет на шаге steps+1
        actual_drain_step = steps + 1 if self.enable_mp2_postprocess else steps
        if self.mp2_drain_func:
            self.mp2_drain_func.simulation_steps = actual_drain_step
        
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
            
            # Логирование spawn каждые 50 шагов
            if self.spawn_enabled and step > 0 and step % 50 == 0:
                serv_pop = fg.AgentVector(self.base_model.agent)
                self.simulation.getPopulationData(serv_pop, 'serviceable')
                print(f"  [Day {step}] serviceable={len(serv_pop)}")
            
            # ОТЛАДКА: Логирование после динамического spawn (выключено по умолчанию)
            if debug_spawn and step == debug_spawn_day:
                print("\n" + "="*60)
                print(f"ОТЛАДКА: Проверка агентов после динамического spawn (день {debug_spawn_day})")
                print("="*60)
                for state_name in ['operations', 'serviceable', 'inactive', 'repair', 'reserve', 'storage']:
                    pop = fg.AgentVector(self.base_model.agent)
                    self.simulation.getPopulationData(pop, state_name)
                    
                    # Считаем агентов с ACN >= debug_acn_min (динамический spawn)
                    dynamic_count = 0
                    for i in range(len(pop)):
                        acn = pop[i].getVariableUInt("aircraft_number")
                        if acn >= debug_acn_min:
                            dynamic_count += 1
                            if dynamic_count <= debug_limit:  # Первые N
                                idx = pop[i].getVariableUInt("idx")
                                intent = pop[i].getVariableUInt("intent_state")
                                print(f"  {state_name}: ACN={acn}, idx={idx}, intent={intent}")
                    
                    print(f"  {state_name}: всего={len(pop)}, динамических (ACN>={debug_acn_min})={dynamic_count}")
                print("="*60 + "\n")
    
        # ═══════════════════════════════════════════════════════════════════════════
        # GPU ПОСТПРОЦЕССИНГ MP2 (если включен)
        # ═══════════════════════════════════════════════════════════════════════════
        if self.enable_mp2_postprocess:
            print("  🔄 Запуск GPU постпроцессинга MP2 (active_trigger → repair history)...")
            import time
            t_post_start = time.perf_counter()
            
            # Устанавливаем export_phase=2 для активации постпроцессинга
            self.simulation.setEnvironmentPropertyUInt("export_phase", 2)
            
            # Один фиктивный шаг для выполнения RTC постпроцессинга
            # MP2 drain НЕ сработает т.к. export_phase != 0
            self.simulation.step()
            
            # Сбрасываем export_phase обратно в 0
            self.simulation.setEnvironmentPropertyUInt("export_phase", 0)
            
            t_post = time.perf_counter() - t_post_start
            print(f"  ✅ Постпроцессинг завершён за {t_post:.2f}с")
            
            # Теперь выполняем финальный дренаж вручную (ещё один шаг с export_phase=0)
            # Этот шаг сработает на mp2_final_drain т.к. step == simulation_steps - 1
            print("  📤 Финальный дренаж MP2 после постпроцессинга...")
            self.simulation.step()
        
        # ═══════════════════════════════════════════════════════════════════════════
        # ПРОВЕРКА ИСЧЕРПАНИЯ РЕЗЕРВА ДИНАМИЧЕСКОГО SPAWN
        # ═══════════════════════════════════════════════════════════════════════════
        if 'spawn_dynamic' in self.modules:
            self._check_spawn_dynamic_exhaustion()
        
    def _check_spawn_dynamic_exhaustion(self):
        """Проверяет исчерпание резерва динамического spawn и выводит WARNING"""
        try:
            # Получаем данные spawn_dynamic_mgr
            mgr_pop = fg.AgentVector(self.model.Agent("spawn_dynamic_mgr"))
            self.simulation.getPopulationData(mgr_pop, 'default')
            
            if len(mgr_pop) == 0:
                return  # Нет менеджера — ничего не проверяем
            
            mgr_agent = mgr_pop[0]
            exhausted_day = mgr_agent.getVariableUInt("exhausted_day")
            total_spawned = mgr_agent.getVariableUInt("total_spawned")
            dynamic_reserve = self.env_data.get('dynamic_reserve_mi17', 50)
            
            if exhausted_day > 0:
                # Резерв был исчерпан!
                print("\n" + "="*80)
                print("⚠️  WARNING: РЕЗЕРВ ДИНАМИЧЕСКОГО SPAWN ИСЧЕРПАН!")
                print("="*80)
                print(f"  • День исчерпания: {exhausted_day}")
                print(f"  • Всего создано агентов: {total_spawned}")
                print(f"  • Максимальный резерв: {dynamic_reserve}")
                print(f"  • Дефицит НЕ покрывается полностью после дня {exhausted_day}")
                print("\n  Рекомендации:")
                print("  1. Увеличить резерв (изменить параметры формулы расчёта)")
                print("  2. Проверить корректность LL и среднего налёта")
                print("  3. Проверить целевые квоты (mp4_ops_counter_mi17)")
                print("="*80 + "\n")
            else:
                # Резерв НЕ исчерпан
                print(f"\n✅ Динамический spawn: резерв достаточен ({total_spawned}/{dynamic_reserve} использовано)\n")
                
        except Exception as e:
            print(f"⚠️  Ошибка при проверке spawn_dynamic: {e}")
    
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
                    'second_ll': agent.getVariableUInt("second_ll"),
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
    # Полный набор модулей для симуляции планеров
    DEFAULT_MODULES = [
        'state_2_operations',      # Инкремент sne/ppr для operations
        'states_stub',             # Заглушки для неактивных состояний
        'count_ops',               # Подсчёт агентов в operations
        'quota_repair',            # Квота на repair
        'quota_ops_excess',        # Избыток operations
        'quota_promote_serviceable',  # Промоут serviceable → operations
        'quota_promote_reserve',      # Промоут reserve → operations
        'quota_promote_inactive',     # Промоут inactive → operations
        'state_manager_serviceable',  # Переходы serviceable
        'state_manager_operations',   # Переходы operations → repair/storage
        'state_manager_repair',       # Переходы repair → operations/reserve
        'state_manager_reserve',      # Переходы reserve → operations
        'state_manager_storage',      # Storage (терминальное)
        'state_manager_inactive',     # inactive → operations/repair
        'spawn_v2',                   # Динамический спавн
    ]
    parser.add_argument('--modules', nargs='+', default=DEFAULT_MODULES,
                      help='Список RTC модулей для подключения')
    parser.add_argument('--steps', type=int, default=None,
                      help='Количество шагов симуляции (по умолчанию из HL_V2_STEPS)')
    parser.add_argument('--version-date', type=str, default=None,
                      help='Дата версии данных (YYYY-MM-DD). Если не указана - интерактивный выбор')
    parser.add_argument('--enable-mp2', action='store_true',
                      help='Включить MP2 device-side export')
    parser.add_argument('--enable-mp2-postprocess', action='store_true',
                      help='Включить GPU постпроцессинг MP2 (active_trigger → repair history)')
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
    
    # Определяем версию данных
    from datetime import datetime as dt
    from sim_env_setup import list_available_versions, select_version_interactive
    
    version_date = None
    if args.version_date:
        # Версия указана в командной строке
        try:
            version_date = dt.strptime(args.version_date, '%Y-%m-%d').date()
            print(f"📅 Версия данных (из CLI): {version_date}")
        except ValueError:
            print(f"❌ Неверный формат даты: {args.version_date}. Используйте YYYY-MM-DD")
            return 1
    else:
        # Интерактивный выбор версии
        versions = list_available_versions(client)
        if len(versions) > 1:
            try:
                version_date, _ = select_version_interactive(client)
            except KeyboardInterrupt:
                print("\n❌ Отменено пользователем")
                return 1
        elif len(versions) == 1:
            version_date = versions[0][0]
            print(f"📅 Единственная версия данных: {version_date}")
        else:
            print("❌ Нет доступных версий данных!")
            return 1
    
    # Опционально дропаем таблицу проекта перед запуском
    if args.drop_table:
        try:
            print("Удаление таблицы sim_masterv2 (DROP TABLE IF EXISTS)...")
            client.execute("DROP TABLE IF EXISTS sim_masterv2")
            print("  Таблица sim_masterv2 удалена (если существовала)")
        except Exception as e:
            print(f"  Ошибка удаления таблицы sim_masterv2: {e}")
            raise

    env_data = prepare_env_arrays(client, version_date)
    t_data_load = time.perf_counter() - t_data_start
    print(f"  Данные загружены за {t_data_load:.2f}с")
    
    # Создаем оркестратор с поддержкой MP2
    orchestrator = V2Orchestrator(env_data, 
                                  enable_mp2=args.enable_mp2,
                                  enable_mp2_postprocess=args.enable_mp2_postprocess,
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
    if args.enable_mp2 and t_db_total > 0:
        print(f"  - в т.ч. дренаж MP2 в СУБД: {t_db_total:.2f}с")
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
