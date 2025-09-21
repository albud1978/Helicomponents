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
        """Загружает агентов из MP3 данных"""
        # Создаем пустую популяцию для заполнения
        pop = fg.AgentVector(self.base_model.agent)
        
        # Извлекаем массивы MP3
        mp3 = self.env_data.get('mp3_arrays', {})
        ac_list = mp3.get('mp3_aircraft_number', [])
        status_list = mp3.get('mp3_status_id', [])
        sne_list = mp3.get('mp3_sne', [])
        ppr_list = mp3.get('mp3_ppr', [])
        repair_days_list = mp3.get('mp3_repair_days', [])
        gb_list = mp3.get('mp3_group_by', [])
        pseq_list = mp3.get('mp3_partseqno_i', [])
        
        # Индекс кадров
        frames_index = self.env_data.get('frames_index', {})
        
        # Предварительно вычисляем LL/OH/BR по кадрам
        ll_by_frame, oh_by_frame, br_by_frame = self._build_norms_by_frame()
        
        # Заполняем агентов
        for i in range(self.frames):
            pop.push_back()
            agent = pop[i]
            
            # Базовые переменные
            agent.setVariableUInt("idx", i)
            
            # Ищем данные по индексу кадра
            ac = 0
            for j, ac_num in enumerate(ac_list):
                if frames_index.get(int(ac_num or 0), -1) == i:
                    ac = int(ac_num or 0)
                    agent.setVariableUInt("aircraft_number", ac)
                    agent.setVariableUInt("status_id", int(status_list[j] or 0) if j < len(status_list) else 0)
                    agent.setVariableUInt("sne", int(sne_list[j] or 0) if j < len(sne_list) else 0)
                    agent.setVariableUInt("ppr", int(ppr_list[j] or 0) if j < len(ppr_list) else 0)
                    agent.setVariableUInt("repair_days", int(repair_days_list[j] or 0) if j < len(repair_days_list) else 0)
                    agent.setVariableUInt("group_by", int(gb_list[j] or 0) if j < len(gb_list) else 0)
                    agent.setVariableUInt("partseqno_i", int(pseq_list[j] or 0) if j < len(pseq_list) else 0)
                    break
            
            # Нормативы
            agent.setVariableUInt("ll", ll_by_frame[i])
            agent.setVariableUInt("oh", oh_by_frame[i])
            agent.setVariableUInt("br", br_by_frame[i])
            
            # Времена ремонта из констант
            gb = agent.getVariableUInt("group_by")
            if gb == 1:
                agent.setVariableUInt("repair_time", self.simulation.getEnvironmentPropertyUInt("mi8_repair_time_const"))
                agent.setVariableUInt("assembly_time", self.simulation.getEnvironmentPropertyUInt("mi8_assembly_time_const"))
            elif gb == 2:
                agent.setVariableUInt("repair_time", self.simulation.getEnvironmentPropertyUInt("mi17_repair_time_const"))
                agent.setVariableUInt("assembly_time", self.simulation.getEnvironmentPropertyUInt("mi17_assembly_time_const"))
        
        # Загружаем популяцию в симуляцию (состояние по умолчанию)
        self.simulation.setPopulationData(pop)
    
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
        """Извлекает результаты симуляции"""
        pop = fg.AgentVector(self.base_model.agent)
        self.simulation.getPopulationData(pop)
        results = []
        
        for i in range(self.frames):
            agent = pop[i]
            results.append({
                'idx': i,
                'aircraft_number': agent.getVariableUInt("aircraft_number"),
                'status_id': agent.getVariableUInt("status_id"),
                'sne': agent.getVariableUInt("sne"),
                'ppr': agent.getVariableUInt("ppr"),
                'daily_today': agent.getVariableUInt("daily_today_u32"),
                'daily_next': agent.getVariableUInt("daily_next_u32")
            })
        
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
    
    # Примеры агентов
    samples = results[:5]
    print(f"  Примеры (idx, st, sne, ppr, dt, dn):")
    for r in samples:
        print(f"    {r['idx']}: st={r['status_id']}, sne={r['sne']}, ppr={r['ppr']}, dt={r['daily_today']}, dn={r['daily_next']}")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
