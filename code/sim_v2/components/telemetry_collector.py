"""
TelemetryCollector - модуль для сбора телеметрии и логирования

Ответственность:
- Сбор метрик по шагам симуляции (state counts, intent changes)
- Логирование переходов между состояниями
- Отслеживание производительности (step times, percentiles)

Архитектурный принцип:
- Изолированный модуль, работает с результатами симуляции
- Не изменяет состояние агентов (read-only)
- Конфигурируемый уровень детализации логов
"""

import time
import datetime
from typing import Dict, List, Tuple, Optional
import pyflamegpu as fg

# Импорт адаптера доступен, но не используется напрямую в телеметрии
# (она работает с simulation, не с env_data)


class TelemetryCollector:
    """Коллектор телеметрии для симуляции"""
    
    def __init__(self, simulation: fg.CUDASimulation, agent_def: fg.AgentDescription, 
                 version_date_ord: int, enable_state_counts: bool = False,
                 enable_intent_tracking: bool = True):
        """
        Инициализация коллектора
        
        Args:
            simulation: объект симуляции FLAME GPU
            agent_def: определение агента
            version_date_ord: базовая дата (ordinal days от 1970-01-01)
            enable_state_counts: включить логи счётчиков состояний на каждом шаге
            enable_intent_tracking: отслеживать изменения intent в operations
        """
        self.simulation = simulation
        self.agent_def = agent_def
        self.version_date_ord = version_date_ord
        self.enable_state_counts = enable_state_counts
        self.enable_intent_tracking = enable_intent_tracking
        
        # Состояния для отслеживания
        self.state_names = [
            'inactive', 'operations', 'serviceable', 'repair', 'reserve', 'storage'
        ]
        
        # Метрики
        self.step_times: List[float] = []
        self.prev_ops_intent: Dict[int, Tuple] = {}
        
    def before_simulation(self):
        """Вызывается перед началом симуляции для инициализации"""
        if self.enable_intent_tracking:
            self.prev_ops_intent = self._get_operations_intents()
    
    def track_step(self, step: int) -> float:
        """
        Отслеживает выполнение одного шага симуляции
        
        Args:
            step: номер шага (0-based)
        
        Returns:
            время выполнения шага в секундах
        """
        t0 = time.perf_counter()
        self.simulation.step()
        step_time = time.perf_counter() - t0
        self.step_times.append(step_time)
        
        # Логи счётчиков состояний (опционально)
        if self.enable_state_counts:
            self._print_state_counts(step)
        
        # Отслеживание изменений intent в operations
        if self.enable_intent_tracking:
            self._track_intent_changes(step)
        
        return step_time
    
    def get_timing_summary(self) -> Dict[str, float]:
        """
        Возвращает статистику по таймингам
        
        Returns:
            словарь с метриками: total, mean, p50, p95, max
        """
        if not self.step_times:
            return {'total': 0.0, 'mean': 0.0, 'p50': 0.0, 'p95': 0.0, 'max': 0.0}
        
        total = sum(self.step_times)
        mean = total / len(self.step_times)
        
        sorted_times = sorted(self.step_times)
        p50 = sorted_times[int(0.50 * len(sorted_times))] if sorted_times else 0.0
        p95 = sorted_times[int(0.95 * len(sorted_times)) - 1] if len(sorted_times) > 1 else sorted_times[0]
        max_time = max(sorted_times) if sorted_times else 0.0
        
        return {
            'total': total,
            'mean': mean,
            'p50': p50,
            'p95': p95,
            'max': max_time
        }
    
    def get_state_counts(self) -> Dict[str, int]:
        """
        Возвращает текущие счётчики агентов по состояниям
        
        Returns:
            словарь {state_name: count}
        """
        counts = {}
        for state_name in self.state_names:
            pop = fg.AgentVector(self.agent_def)
            self.simulation.getPopulationData(pop, state_name)
            counts[state_name] = len(pop)
        return counts
    
    def _get_operations_intents(self) -> Dict[int, Tuple]:
        """
        Извлекает текущие intent и метрики из состояния operations
        
        Returns:
            словарь {idx: (intent, ac, sne, ppr, dt, dn, ll, oh, br)}
        """
        pop = fg.AgentVector(self.agent_def)
        self.simulation.getPopulationData(pop, 'operations')
        result = {}
        for i in range(len(pop)):
            ag = pop[i]
            idx = ag.getVariableUInt('idx')
            intent = ag.getVariableUInt('intent_state')
            ac = ag.getVariableUInt('aircraft_number')
            sne = ag.getVariableUInt('sne')
            ppr = ag.getVariableUInt('ppr')
            dt = ag.getVariableUInt('daily_today_u32')
            dn = ag.getVariableUInt('daily_next_u32')
            ll = ag.getVariableUInt('ll')
            oh = ag.getVariableUInt('oh')
            br = ag.getVariableUInt('br')
            result[idx] = (intent, ac, sne, ppr, dt, dn, ll, oh, br)
        return result
    
    def _print_state_counts(self, step: int):
        """Печатает счётчики состояний для текущего шага"""
        counts = self.get_state_counts()
        print(f"  Step {step}: counts "
              f"inactive={counts['inactive']}, operations={counts['operations']}, "
              f"serviceable={counts['serviceable']}, repair={counts['repair']}, "
              f"reserve={counts['reserve']}, storage={counts['storage']}")
    
    def _track_intent_changes(self, step: int):
        """
        Отслеживает изменения intent в operations и логирует переходы
        
        Args:
            step: номер шага (0-based)
        """
        curr_ops_intent = self._get_operations_intents()
        
        for idx, vals in curr_ops_intent.items():
            new_intent, ac, sne, ppr, dt, dn, ll, oh, br = vals
            old_vals = self.prev_ops_intent.get(idx, (None, ac, None, None, None, None, None, None, None))
            old_intent, _ac_old, sne_old, ppr_old, dt_old, dn_old, _ll_old, _oh_old, _br_old = old_vals
            
            # Логируем только изменения intent на значения != 2 (т.е. переходы из operations)
            if new_intent != 2 and new_intent != old_intent:
                # Прогноз на завтра основывается на значениях ДО шага
                s_next = (sne_old + dn_old) if sne_old is not None and dn_old is not None else None
                p_next = (ppr_old + dn_old) if ppr_old is not None and dn_old is not None else None
                
                # Вычисляем календарную дату перехода
                base_date = datetime.date(1970, 1, 1)
                day_abs = int(self.version_date_ord) + (step + 1)
                date_str = (base_date + datetime.timedelta(days=day_abs)).isoformat()
                
                print(
                    f"  [Day {step+1} | date={date_str}] AC {ac} idx={idx}: "
                    f"intent {old_intent}->{new_intent} (operations) "
                    f"sne={sne}, ppr={ppr}, dt={dt}, dn={dn}, s_next={s_next}, p_next={p_next}, "
                    f"ll={ll}, oh={oh}, br={br}"
                )
        
        self.prev_ops_intent = curr_ops_intent
    
    def print_summary(self, steps: int):
        """
        Печатает итоговую сводку по телеметрии
        
        Args:
            steps: общее количество шагов
        """
        timings = self.get_timing_summary()
        counts = self.get_state_counts()
        
        print(f"\n=== Телеметрия симуляции ===")
        print(f"Шагов выполнено: {steps}")
        print(f"Общее время GPU: {timings['total']:.2f}с")
        print(f"Среднее время на шаг: {timings['mean']*1000:.1f}мс")
        print(f"Шаги: p50={timings['p50']*1000:.1f}мс, p95={timings['p95']*1000:.1f}мс, max={timings['max']*1000:.1f}мс")
        print(f"\nФинальные счётчики состояний:")
        for state_name in self.state_names:
            print(f"  {state_name}: {counts[state_name]}")
