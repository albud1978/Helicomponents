#!/usr/bin/env python3
"""
Flame GPU 2 Helicopter Model (Skeleton)

Каркас модели с 6 RTC функциями и 2 host-функциями, зафиксирующий порядок
выполнения и интерфейсы под group_by. Реализация логики будет добавлена на
следующем шаге. Скрипт безопасен для окружений без pyflamegpu (мягкий выход).

RTC функции:
- rtc_repair
- rtc_ops_check
- rtc_balance
- rtc_main
- rtc_change
- rtc_pass_through

Host-функции:
- host_compute_trigger_mi8
- host_compute_trigger_mi17

Дата: 2025-08-10
"""

import sys
import os
from typing import Optional

try:
    import pyflamegpu
except Exception as e:
    pyflamegpu = None


class HelicopterFlameModel:
    """Каркас FLAME GPU модели под group_by=1/2 (МИ‑8Т/МИ‑17)"""

    def __init__(self):
        self.model = None
        self.sim = None

    def build_model(self) -> Optional["pyflamegpu.ModelDescription"]:
        if pyflamegpu is None:
            print("⚠️ pyflamegpu не установлен. Каркас модели создан логически (без сборки).")
            return None

        model = pyflamegpu.ModelDescription("Helicopter_ABM")

        # Среда: минимальные свойства для хоста/RTC (скалярные триггеры)
        env = model.Environment()
        env.newPropertyInt("trigger_pr_final_mi8", 0)
        env.newPropertyInt("trigger_pr_final_mi17", 0)
        env.newPropertyInt("current_day", 0)  # индекс дня D

        # Агент (placeholder). Реальная логика — позже
        agent = model.newAgent("plane")
        agent.newVariableUInt("psn", 0)
        agent.newVariableUInt("group_by", 0)       # 1|2
        agent.newVariableUInt("status_id", 0)
        agent.newVariableUInt("status_change", 0)
        agent.newVariableUInt("sne", 0)
        agent.newVariableUInt("ppr", 0)

        # === RTC функции (как StepFunction, operate-on-environment style) ===
        # В каркасе только объявления и регистрация
        def rtc_repair_func(step: "pyflamegpu.StepInterface"):
            pass

        def rtc_ops_check_func(step: "pyflamegpu.StepInterface"):
            pass

        def rtc_balance_func(step: "pyflamegpu.StepInterface"):
            pass

        def rtc_main_func(step: "pyflamegpu.StepInterface"):
            pass

        def rtc_change_func(step: "pyflamegpu.StepInterface"):
            pass

        def rtc_pass_through_func(step: "pyflamegpu.StepInterface"):
            pass

        model.addStepFunction(pyflamegpu.StepFunction(rtc_repair_func))
        model.addStepFunction(pyflamegpu.StepFunction(rtc_ops_check_func))

        # Host: вычисление триггеров по группам
        def host_compute_trigger_mi8(sim: "pyflamegpu.CUDASimulation"):
            # Реальная логика позже; сейчас — заглушка интерфейса
            env = sim.getEnvironment()
            _ = env.getPropertyInt("trigger_pr_final_mi8")

        def host_compute_trigger_mi17(sim: "pyflamegpu.CUDASimulation"):
            env = sim.getEnvironment()
            _ = env.getPropertyInt("trigger_pr_final_mi17")

        model.addInitFunction(pyflamegpu.HostFunction(host_compute_trigger_mi8))
        model.addInitFunction(pyflamegpu.HostFunction(host_compute_trigger_mi17))

        # Продолжаем порядок RTC
        model.addStepFunction(pyflamegpu.StepFunction(rtc_balance_func))
        model.addStepFunction(pyflamegpu.StepFunction(rtc_main_func))
        model.addStepFunction(pyflamegpu.StepFunction(rtc_change_func))
        model.addStepFunction(pyflamegpu.StepFunction(rtc_pass_through_func))

        self.model = model
        return model

    def build_simulation(self) -> Optional["pyflamegpu.CUDASimulation"]:
        if pyflamegpu is None:
            return None
        if self.model is None:
            self.build_model()
        sim = pyflamegpu.CUDASimulation(self.model)
        self.sim = sim
        return sim

    def run(self, days: int = 1):
        if pyflamegpu is None:
            print("⚠️ pyflamegpu отсутствует. Запуск симуляции пропущен.")
            return
        if self.sim is None:
            self.build_simulation()
        # В каркасе не запускаем цикл симуляции. Оставлено для будущей реализации.
        print(f"🧪 Каркас модели собран. Симуляция не выполняется (days={days}).")


def main():
    print("🔥 FLAME GPU 2 Helicopter Model — каркас")
    if pyflamegpu is None:
        print("⚠️ Библиотека pyflamegpu не найдена. Установите её, чтобы запустить модель.")
        sys.exit(0)

    model = HelicopterFlameModel()
    model.build_model()
    model.build_simulation()
    model.run(days=1)


if __name__ == "__main__":
    main()