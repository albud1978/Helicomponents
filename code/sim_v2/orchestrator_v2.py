#!/usr/bin/env python3
"""
V2 Orchestrator: модульная архитектура с динамической загрузкой RTC модулей
"""
import os
import sys
import json
import argparse
from typing import Dict, List

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from sim_env_setup import get_client, prepare_env_arrays
from base_model import V2BaseModel
from rtc_mp5_probe import create_host_function_mp5_init

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


class V2Orchestrator:
    """Оркестратор для управления модульной симуляцией"""
    
    def __init__(self, env_data: Dict[str, object]):
        self.env_data = env_data
        self.base_model = V2BaseModel()
        self.model = None
        self.simulation = None
        
        # Параметры из окружения
        self.frames = int(env_data['frames_total_u16'])
        self.days = int(os.environ.get('HL_V2_STEPS', '90'))
        
    def build_model(self, rtc_modules: List[str]):
        """Строит модель с указанными RTC модулями"""
        print(f"Построение модели с модулями: {', '.join(rtc_modules)}")
        
        # Создаем базовую модель
        self.model = self.base_model.create_model(self.env_data)
        
        # Если есть MP5 модуль, добавляем слой инициализации первым
        if 'mp5_probe' in rtc_modules:
            self._add_mp5_init_layer()
        
        # Создаем слой для обработки состояний
        state_layer = self.model.newLayer('state_processing')
        
        # Подключаем RTC модули
        for module_name in rtc_modules:
            print(f"  Подключение модуля: {module_name}")
            self.base_model.add_rtc_module(module_name)
            
        return self.model
    
    def create_simulation(self):
        """Создает и настраивает симуляцию"""
        if not self.model:
            raise RuntimeError("Модель не построена")
            
        self.simulation = fg.CUDASimulation(self.model)
        
        # Заполняем окружение
        sim = self.simulation
        sim.setEnvironmentPropertyUInt("version_date", int(self.env_data['version_date_u16']))
        sim.setEnvironmentPropertyUInt("frames_total", self.frames)
        sim.setEnvironmentPropertyUInt("days_total", self.days)
        
        # MP4 квоты уже установлены в базовой модели с правильным размером
        
        # Создаем популяцию агентов из MP3
        self._populate_agents()
        
        return self.simulation
    
    def _populate_agents(self):
        """Загружает агентов из MP3 данных с поддержкой States"""
        # Извлекаем массивы MP3
        mp3 = self.env_data.get('mp3_arrays', {})
        ac_list = mp3.get('mp3_aircraft_number', [])
        status_list = mp3.get('mp3_status_id', [])
        sne_list = mp3.get('mp3_sne', [])
        ppr_list = mp3.get('mp3_ppr', [])
        repair_days_list = mp3.get('mp3_repair_days', [])
        gb_list = mp3.get('mp3_group_by', [])
        pseq_list = mp3.get('mp3_partseqno_i', [])
        
        # Получаем MP1 данные для OH
        mp1_arrays = self.env_data.get('mp1_arrays', {})
        mp1_partseqno = mp1_arrays.get('partseqno_i', [])
        mp1_oh_mi8 = mp1_arrays.get('oh_mi8', [])
        mp1_oh_mi17 = mp1_arrays.get('oh_mi17', [])
        
        # Индекс кадров
        frames_index = self.env_data.get('frames_index', {})
        
        # Предварительно вычисляем LL/OH/BR по кадрам
        ll_by_frame, oh_by_frame, br_by_frame = self._build_norms_by_frame()
        
        # Создаем популяции для каждого состояния
        populations = {
            'inactive': fg.AgentVector(self.base_model.agent),      # state_1
            'operations': fg.AgentVector(self.base_model.agent),    # state_2
            'serviceable': fg.AgentVector(self.base_model.agent),   # state_3
            'repair': fg.AgentVector(self.base_model.agent),        # state_4
            'reserve': fg.AgentVector(self.base_model.agent),       # state_5
            'storage': fg.AgentVector(self.base_model.agent)        # state_6
        }
        
        # Маппинг status_id -> state name
        status_to_state = {
            0: 'inactive',      # Статус 0 тоже считаем неактивным
            1: 'inactive',
            2: 'operations',
            3: 'serviceable',
            4: 'repair',
            5: 'reserve',
            6: 'storage'
        }
        
        # Сначала фильтруем записи с group_by in [1,2]
        plane_records = []
        for j in range(len(ac_list)):
            gb = int(gb_list[j] or 0) if j < len(gb_list) else 0
            if gb in [1, 2]:
                ac = int(ac_list[j] or 0)
                if ac > 0 and ac in frames_index:
                    plane_records.append({
                        'idx': j,
                        'aircraft_number': ac,
                        'frame_idx': frames_index[ac],
                        'status_id': int(status_list[j] or 1) if j < len(status_list) else 1,
                        'sne': int(sne_list[j] or 0) if j < len(sne_list) else 0,
                        'ppr': int(ppr_list[j] or 0) if j < len(ppr_list) else 0,
                        'repair_days': int(repair_days_list[j] or 0) if j < len(repair_days_list) else 0,
                        'group_by': gb,
                        'partseqno_i': int(pseq_list[j] or 0) if j < len(pseq_list) else 0
                    })
        
        # Группируем по frame_idx и берем первую запись для каждого
        records_by_frame = {}
        for rec in plane_records:
            frame_idx = rec['frame_idx']
            if frame_idx not in records_by_frame:
                records_by_frame[frame_idx] = rec
        
        # Получаем информацию о зарезервированных слотах
        first_reserved_idx = self.env_data.get('first_reserved_idx', self.frames)
        
        # Заполняем агентов и распределяем по состояниям
        # Создаем только реальных агентов, пропускаем зарезервированные слоты
        for i in range(self.frames):
            # Пропускаем зарезервированные слоты для будущего спавна
            if i >= first_reserved_idx:
                continue
                
            # Берем данные для этого frame_idx
            agent_data = records_by_frame.get(i, {
                'aircraft_number': 0,
                'status_id': 1,  # По умолчанию inactive
                'sne': 0,
                'ppr': 0,
                'repair_days': 0,
                'group_by': 0,
                'partseqno_i': 0
            })
            
            # Определяем состояние и добавляем агента
            status_id = agent_data['status_id']
            state_name = status_to_state.get(status_id, 'inactive')
            pop = populations[state_name]
            pop.push_back()
            agent = pop[len(pop) - 1]
            
            # Базовые переменные
            agent.setVariableUInt("idx", i)
            agent.setVariableUInt("aircraft_number", agent_data['aircraft_number'])
            agent.setVariableUInt("status_id", status_id)
            agent.setVariableUInt("sne", agent_data['sne'])
            agent.setVariableUInt("ppr", agent_data['ppr'])
            agent.setVariableUInt("repair_days", agent_data['repair_days'])
            agent.setVariableUInt("group_by", agent_data['group_by'])
            agent.setVariableUInt("partseqno_i", agent_data['partseqno_i'])
            
            # Определяем OH из MP1 (приоритет oh_mi8/oh_mi17 > 0)
            partseqno = agent_data.get('partseqno_i', 0)
            oh_value = oh_by_frame[i]  # Значение по умолчанию
            
            if partseqno > 0 and mp1_partseqno:
                # Ищем в MP1
                try:
                    idx_in_mp1 = mp1_partseqno.index(partseqno)
                    oh_mi8 = int(mp1_oh_mi8[idx_in_mp1]) if idx_in_mp1 < len(mp1_oh_mi8) else 0
                    oh_mi17 = int(mp1_oh_mi17[idx_in_mp1]) if idx_in_mp1 < len(mp1_oh_mi17) else 0
                    
                    # Берем ненулевое значение (минуты -> часы)
                    if oh_mi8 > 0:
                        oh_value = oh_mi8 * 60
                    elif oh_mi17 > 0:
                        oh_value = oh_mi17 * 60
                except ValueError:
                    pass  # partseqno не найден в MP1
            
            # Нормативы
            agent.setVariableUInt("ll", ll_by_frame[i])
            agent.setVariableUInt("oh", oh_value)
            agent.setVariableUInt("br", br_by_frame[i])
            
            # Времена ремонта из констант
            gb = agent_data.get('group_by', 0)
            if gb == 1:
                agent.setVariableUInt("repair_time", self.simulation.getEnvironmentPropertyUInt("mi8_repair_time_const"))
                agent.setVariableUInt("assembly_time", self.simulation.getEnvironmentPropertyUInt("mi8_assembly_time_const"))
            elif gb == 2:
                agent.setVariableUInt("repair_time", self.simulation.getEnvironmentPropertyUInt("mi17_repair_time_const"))
                agent.setVariableUInt("assembly_time", self.simulation.getEnvironmentPropertyUInt("mi17_assembly_time_const"))
            
            # Для агентов в статусе 6 устанавливаем s6_started
            if status_id == 6:
                agent.setVariableUInt("s6_started", 0)  # Изначально в статусе 6
            
            # Для агентов в статусе 4 проверяем assembly_trigger
            if status_id == 4:
                repair_time = agent.getVariableUInt("repair_time")
                repair_days = agent.getVariableUInt("repair_days")
                assembly_time = agent.getVariableUInt("assembly_time")
                
                if repair_time - repair_days > assembly_time:
                    agent.setVariableUInt("assembly_trigger", 1)
            
            # intent_state = 0 для всех (нет начальных намерений)
            agent.setVariableUInt("intent_state", 0)
        
        # Загружаем популяции в симуляцию по состояниям
        for state_name, pop in populations.items():
            if len(pop) > 0:
                self.simulation.setPopulationData(pop, state_name)
                print(f"  Загружено {len(pop)} агентов в состояние '{state_name}'")
    
    def _build_norms_by_frame(self):
        """Вычисляет нормативы LL/OH/BR по кадрам"""
        # TODO: Перенести эту логику в отдельный модуль
        # Временно используем упрощенную версию
        ll_by_frame = [1080000] * self.frames  # Значения по умолчанию
        oh_by_frame = [270000] * self.frames
        br_by_frame = [973750] * self.frames
        
        # Можно улучшить, взяв из MP1/MP3 реальные значения
        return ll_by_frame, oh_by_frame, br_by_frame
    
    def _add_mp5_init_layer(self):
        """Добавляет слой инициализации MP5 данных через HostFunction"""
            
        # Подготовка данных MP5
        mp5_data = list(self.env_data['mp5_daily_hours_linear'])
        need = (self.days + 1) * self.frames
        mp5_data = mp5_data[:need]
        
        # Создаем HostFunction
        hf_init = create_host_function_mp5_init(mp5_data, self.frames, self.days)
        
        # Создаем отдельный слой для инициализации MP5 в самом начале
        # Важно: это должно быть до всех RTC функций
        init_layer = self.model.newLayer()
        init_layer.addHostFunction(hf_init)
        
        print("MP5 будет инициализирован при первом шаге симуляции")
    
    def run(self, steps: int):
        """Запускает симуляцию на указанное количество шагов"""
        print(f"Запуск симуляции на {steps} шагов")
        
        for step in range(steps):
            self.simulation.step()
            
            # Логирование прогресса каждые 10 шагов
            if step % 10 == 0 or step == steps - 1:
                print(f"  Шаг {step+1}/{steps}")
    
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
        for state_name, status_id in state_to_status.items():
            pop = fg.AgentVector(self.base_model.agent)
            self.simulation.getPopulationData(pop, state_name)
            
            for i in range(len(pop)):
                agent = pop[i]
                results.append({
                    'idx': agent.getVariableUInt("idx"),
                    'aircraft_number': agent.getVariableUInt("aircraft_number"),
                    'status_id': status_id,  # Восстанавливаем из state
                    'state': state_name,
                    'sne': agent.getVariableUInt("sne"),
                    'ppr': agent.getVariableUInt("ppr"),
                    'daily_today': agent.getVariableUInt("daily_today_u32"),
                    'daily_next': agent.getVariableUInt("daily_next_u32"),
                    'intent_state': agent.getVariableUInt("intent_state")
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
    args = parser.parse_args()
    
    # Загружаем данные
    print("Загрузка данных из ClickHouse...")
    client = get_client()
    env_data = prepare_env_arrays(client)
    
    # Создаем оркестратор
    orchestrator = V2Orchestrator(env_data)
    
    # Строим модель с указанными модулями
    orchestrator.build_model(args.modules)
    
    # Создаем симуляцию
    orchestrator.create_simulation()
    
    # Запускаем симуляцию
    steps = args.steps or orchestrator.days
    orchestrator.run(steps)
    
    # Получаем и выводим результаты
    results = orchestrator.get_results()
    
    # Выводим сводку
    status_counts = {}
    for r in results:
        st = r['status_id']
        status_counts[st] = status_counts.get(st, 0) + 1
    
    print(f"\nРезультаты симуляции:")
    print(f"  Всего агентов: {len(results)}")
    print(f"  Статусы: {dict(sorted(status_counts.items()))}")
    
    # Примеры агентов по состояниям
    print(f"\n  Примеры по состояниям (idx, state, st, intent, sne, ppr, dt, dn):")
    
    # Группируем по состояниям
    by_state = {}
    for r in results:
        state = r.get('state', 'unknown')
        if state not in by_state:
            by_state[state] = []
        by_state[state].append(r)
    
    # Выводим по 2 примера из каждого состояния
    for state, agents in sorted(by_state.items()):
        print(f"\n  {state} ({len(agents)} агентов):")
        for r in agents[:2]:
            print(f"    {r['idx']}: st={r['status_id']}, intent={r.get('intent_state', '?')}, sne={r['sne']}, ppr={r['ppr']}, dt={r['daily_today']}, dn={r['daily_next']}")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
