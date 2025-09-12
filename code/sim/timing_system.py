#!/usr/bin/env python3
"""
Система измерения таймингов симуляции
Измеряет: загрузку в GPU, компиляцию ядер, обработку на GPU, логирование CPU, выгрузку в СУБД
Дата: 2025-09-12
"""

import time
from typing import Dict, List, Any
from dataclasses import dataclass, field


@dataclass
class TimingMetrics:
    """Метрики производительности симуляции"""
    
    # Основные этапы
    load_gpu_ms: float = 0.0        # Загрузка данных в GPU
    compile_rtc_ms: float = 0.0     # Компиляция RTC ядер (NVRTC)
    sim_gpu_ms: float = 0.0         # Обработка на GPU (step)
    cpu_log_ms: float = 0.0         # Логирование на CPU
    db_insert_ms: float = 0.0       # Выгрузка в СУБД
    
    # Детальные метрики
    env_setup_ms: float = 0.0       # Настройка Environment
    population_ms: float = 0.0      # Создание популяции агентов
    step_times: List[float] = field(default_factory=list)  # Время каждого шага
    
    # Метаданные
    days_simulated: int = 0
    agents_count: int = 0
    rtc_functions_count: int = 0
    
    def total_ms(self) -> float:
        """Общее время выполнения"""
        return (self.load_gpu_ms + self.compile_rtc_ms + self.sim_gpu_ms + 
                self.cpu_log_ms + self.db_insert_ms)
    
    def print_summary(self):
        """Выводит сводку таймингов"""
        
        total = self.total_ms()
        
        print(f"\n⏱️ Сводка таймингов:")
        print(f"  📥 Загрузка GPU:     {self.load_gpu_ms:>8.2f} мс ({self.load_gpu_ms/total*100:>5.1f}%)")
        print(f"  🔧 Компиляция RTC:   {self.compile_rtc_ms:>8.2f} мс ({self.compile_rtc_ms/total*100:>5.1f}%)")
        print(f"  🚀 Симуляция GPU:    {self.sim_gpu_ms:>8.2f} мс ({self.sim_gpu_ms/total*100:>5.1f}%)")
        print(f"  📊 Логирование CPU:  {self.cpu_log_ms:>8.2f} мс ({self.cpu_log_ms/total*100:>5.1f}%)")
        print(f"  💾 Выгрузка СУБД:    {self.db_insert_ms:>8.2f} мс ({self.db_insert_ms/total*100:>5.1f}%)")
        print(f"  ⏱️ Общее время:      {total:>8.2f} мс")
        
        if self.step_times:
            avg_step = sum(self.step_times) / len(self.step_times)
            min_step = min(self.step_times)
            max_step = max(self.step_times)
            print(f"\n📈 Статистика шагов:")
            print(f"  Шагов: {len(self.step_times)}")
            print(f"  Средний: {avg_step:.2f} мс")
            print(f"  Мин: {min_step:.2f} мс")
            print(f"  Макс: {max_step:.2f} мс")
        
        # Производительность
        if self.days_simulated > 0 and self.agents_count > 0:
            days_per_sec = 1000 / (total / self.days_simulated) if total > 0 else 0
            agents_per_ms = self.agents_count / total if total > 0 else 0
            
            print(f"\n🚀 Производительность:")
            print(f"  Дней/сек: {days_per_sec:.1f}")
            print(f"  Агентов/мс: {agents_per_ms:.1f}")
            print(f"  RTC функций: {self.rtc_functions_count}")


class TimingCollector:
    """Сборщик таймингов для симуляции"""
    
    def __init__(self):
        self.metrics = TimingMetrics()
        self._start_times = {}
    
    def start_timer(self, stage: str):
        """Начинает измерение времени для этапа"""
        self._start_times[stage] = time.perf_counter()
    
    def end_timer(self, stage: str) -> float:
        """Заканчивает измерение времени для этапа"""
        if stage not in self._start_times:
            return 0.0
        
        duration = (time.perf_counter() - self._start_times[stage]) * 1000
        
        # Записываем в соответствующее поле
        if stage == "load_gpu":
            self.metrics.load_gpu_ms += duration
        elif stage == "compile_rtc":
            self.metrics.compile_rtc_ms += duration
        elif stage == "sim_gpu":
            self.metrics.sim_gpu_ms += duration
        elif stage == "cpu_log":
            self.metrics.cpu_log_ms += duration
        elif stage == "db_insert":
            self.metrics.db_insert_ms += duration
        elif stage == "env_setup":
            self.metrics.env_setup_ms += duration
        elif stage == "population":
            self.metrics.population_ms += duration
        
        del self._start_times[stage]
        return duration
    
    def add_step_time(self, step_ms: float):
        """Добавляет время одного шага"""
        self.metrics.step_times.append(step_ms)
    
    def set_metadata(self, days: int, agents: int, rtc_count: int):
        """Устанавливает метаданные симуляции"""
        self.metrics.days_simulated = days
        self.metrics.agents_count = agents
        self.metrics.rtc_functions_count = rtc_count
    
    def get_metrics(self) -> TimingMetrics:
        """Возвращает собранные метрики"""
        return self.metrics


def create_timing_context(collector: TimingCollector, stage: str):
    """Создает контекстный менеджер для измерения времени"""
    
    class TimingContext:
        def __init__(self, collector, stage):
            self.collector = collector
            self.stage = stage
        
        def __enter__(self):
            self.collector.start_timer(self.stage)
            return self
        
        def __exit__(self, exc_type, exc_val, exc_tb):
            duration = self.collector.end_timer(self.stage)
            print(f"  ⏱️ {self.stage}: {duration:.2f} мс")
    
    return TimingContext(collector, stage)


def main():
    """Тест системы таймингов"""
    
    print("🚀 Тест системы таймингов")
    print("=" * 30)
    
    collector = TimingCollector()
    
    # Имитация этапов симуляции
    with create_timing_context(collector, "load_gpu"):
        time.sleep(0.1)  # Имитация загрузки
    
    with create_timing_context(collector, "compile_rtc"):
        time.sleep(0.05)  # Имитация компиляции
    
    with create_timing_context(collector, "sim_gpu"):
        time.sleep(0.2)  # Имитация симуляции
        collector.add_step_time(20.0)
        collector.add_step_time(25.0)
        collector.add_step_time(22.0)
    
    with create_timing_context(collector, "cpu_log"):
        time.sleep(0.03)  # Имитация логирования
    
    with create_timing_context(collector, "db_insert"):
        time.sleep(0.08)  # Имитация вставки в БД
    
    # Метаданные
    collector.set_metadata(days=30, agents=1000, rtc_count=4)
    
    # Результаты
    collector.get_metrics().print_summary()
    
    print("\n✅ Система таймингов готова")


if __name__ == '__main__':
    main()


