#!/usr/bin/env python3
"""
Flame GPU 2 Helicopter Model (Agent-based)

Модель с 6 RTC-слоями (как агент-функции) и host-балансировкой.
- Агент: component (каждый ряд MP3)
- Переменные агента покрывают поля MP3 + обогащение из MP1 (br, repair_time) и служебные idx
- Окружение: скаляры (триггеры), массивы `daily_today`, `daily_next`, `partout_time_arr`, `assembly_time_arr` (size=N_agents)

Сутки D: repair → ops_check → host_balance → main → change → pass

Дата: 2025-08-10
"""

from typing import Optional

try:
    import pyflamegpu
except Exception:
    pyflamegpu = None


class HelicopterFlameModel:
    """FLAME GPU модель с агентами и окружением"""

    def __init__(self):
        self.model = None
        self.sim = None
        self.agent = None
        self.num_agents = 0

    def build_model(self, num_agents: int) -> Optional["pyflamegpu.ModelDescription"]:
        if pyflamegpu is None:
            print("⚠️ pyflamegpu не установлен. Пропуск сборки модели.")
            return None

        self.num_agents = int(num_agents)
        model = pyflamegpu.ModelDescription("Helicopter_ABM")

        # Окружение
        env = model.Environment()
        env.newPropertyInt("trigger_pr_final_mi8", 0)
        env.newPropertyInt("trigger_pr_final_mi17", 0)
        env.newPropertyInt("current_day_index", 0)
        env.newPropertyUInt("current_day_ordinal", 0)
        # Инварианты
        env.newPropertyInt("ops_check_violation", 0)
        env.newPropertyInt("pass_through_violation", 0)
        # Массивы окружения
        env.newPropertyArrayUInt32("daily_today", [0] * self.num_agents)
        env.newPropertyArrayUInt32("daily_next", [0] * self.num_agents)
        env.newPropertyArrayUInt32("partout_time_arr", [0] * self.num_agents)
        env.newPropertyArrayUInt32("assembly_time_arr", [0] * self.num_agents)

        # Агент и переменные
        agent = model.newAgent("component")
        self.agent = agent
        # Индекс в массивах окружения
        agent.newVariableUInt("idx", 0)
        # Идентификация/группы
        agent.newVariableUInt("psn", 0)
        agent.newVariableUInt("partseqno_i", 0)
        agent.newVariableUInt("aircraft_number", 0)
        agent.newVariableUInt("group_by", 0)       # 1|2|...
        agent.newVariableUInt("ac_type_mask", 0)
        # Статусы/переходы
        agent.newVariableUInt("status_id", 0)
        agent.newVariableUInt("status_change", 0)
        # Ресурсы
        agent.newVariableUInt("ll", 0)
        agent.newVariableUInt("oh", 0)
        agent.newVariableUInt("oh_threshold", 0)
        agent.newVariableUInt("sne", 0)
        agent.newVariableUInt("ppr", 0)
        # Ремонт/даты
        agent.newVariableUInt("repair_days", 0)      # UInt16 по смыслу, но UInt32 для простоты API
        agent.newVariableUInt("mfg_date", 0)         # Date как UInt16 days since epoch → UInt32 для API
        agent.newVariableUInt("version_date", 0)     # Date как UInt16 → UInt32 для API
        # Обогащение из MP1
        agent.newVariableUInt("br", 0)
        agent.newVariableUInt("repair_time", 0)      # UInt16 → UInt32
        # Служебные даты-триггеры (как ordinal)
        agent.newVariableUInt("partout_trigger_ord", 0)
        agent.newVariableUInt("assembly_trigger_ord", 0)
        agent.newVariableUInt("active_trigger_ord", 0)

        # === Агент‑функции ===
        def rtc_repair(agent, messages=None, messageOut=None, environment=None):
            if agent.getVariableUInt("status_id") == 4:
                # Инкремент дня ремонта
                rd = agent.getVariableUInt("repair_days") + 1
                agent.setVariableUInt("repair_days", rd)
                # Завершение ремонта: планируем переход 4→5 (проверка status_change не требуется)
                rt = agent.getVariableUInt("repair_time")
                if rd >= rt:
                    agent.setVariableUInt("status_change", 5)

        def rtc_ops_check(agent, messages=None, messageOut=None, environment=None):
            # Инвариант: на входе status_change должен быть 0
            if agent.getVariableUInt("status_change") != 0:
                environment.setPropertyInt("ops_check_violation", environment.getPropertyInt("ops_check_violation") + 1)
                return
            if agent.getVariableUInt("status_id") != 2:
                return
            idx = agent.getVariableUInt("idx")
            # Чтение суточных налётов из окружения
            dt = environment.getPropertyArrayUInt32("daily_today")[idx]
            dn = environment.getPropertyArrayUInt32("daily_next")[idx]
            sne = agent.getVariableUInt("sne")
            ppr = agent.getVariableUInt("ppr")
            ll = agent.getVariableUInt("ll")
            oh = agent.getVariableUInt("oh")
            br = agent.getVariableUInt("br")
            # LL: хватит на сегодня, не хватит на завтра → 6
            if (ll - sne) >= dt and (ll - sne) < (dt + dn):
                agent.setVariableUInt("status_change", 6)
                return
            # OH: хватит на сегодня, не хватит на завтра
            if (oh - ppr) >= dt and (oh - ppr) < (dt + dn):
                # Ремонтопригодность: sne + dt < br → 4, иначе 6
                if (sne + dt) < br:
                    agent.setVariableUInt("status_change", 4)
                else:
                    agent.setVariableUInt("status_change", 6)

        def rtc_main(agent, messages=None, messageOut=None, environment=None):
            # Начисление налёта для status_id=2
            if agent.getVariableUInt("status_id") == 2:
                idx = agent.getVariableUInt("idx")
                dt = environment.getPropertyArrayUInt32("daily_today")[idx]
                agent.setVariableUInt("sne", agent.getVariableUInt("sne") + dt)
                agent.setVariableUInt("ppr", agent.getVariableUInt("ppr") + dt)
            # Применение перехода по словарю
            chg = agent.getVariableUInt("status_change")
            if chg:
                agent.setVariableUInt("status_id", chg)

        def rtc_change(agent, messages=None, messageOut=None, environment=None):
            # Сайд‑эффекты переходов
            chg = agent.getVariableUInt("status_change")
            current_day_ord = int(environment.getPropertyUInt("current_day_ordinal"))
            idx = agent.getVariableUInt("idx")
            rt = int(agent.getVariableUInt("repair_time"))
            # значения времени из окружения
            pt = int(environment.getPropertyArrayUInt32("partout_time_arr")[idx])
            at = int(environment.getPropertyArrayUInt32("assembly_time_arr")[idx])
            prev_status = agent.getVariableUInt("status_id")
            if chg == 4:
                # Вход в ремонт
                agent.setVariableUInt("repair_days", 1)
                # Триггеры как даты (ordinal)
                agent.setVariableUInt("partout_trigger_ord", current_day_ord + pt)
                agent.setVariableUInt("assembly_trigger_ord", current_day_ord + max(rt - at, 0))
            elif chg == 5:
                # Окончание ремонта
                agent.setVariableUInt("ppr", 0)
                agent.setVariableUInt("repair_days", 0)
            elif chg == 2 and prev_status == 1:
                # Активация из неактивного
                # active = текущая дата симуляции - repair_time
                active_ord = current_day_ord - rt if current_day_ord >= rt else 0
                agent.setVariableUInt("active_trigger_ord", active_ord)
                # assembly = текущая дата симуляции - assembly_time
                asm_ord = current_day_ord - at if current_day_ord >= at else 0
                agent.setVariableUInt("assembly_trigger_ord", asm_ord)
            # Сброс метки перехода (для всех ненулевых chg) в конце суток
            if chg != 0:
                agent.setVariableUInt("status_change", 0)

        def rtc_pass_through(agent, messages=None, messageOut=None, environment=None):
            # Инвариант: после rtc_change не должно остаться status_change>0
            if agent.getVariableUInt("status_change") != 0:
                environment.setPropertyInt("pass_through_violation", environment.getPropertyInt("pass_through_violation") + 1)

        agent.newFunction("rtc_repair", rtc_repair)
        agent.newFunction("rtc_ops_check", rtc_ops_check)
        agent.newFunction("rtc_main", rtc_main)
        agent.newFunction("rtc_change", rtc_change)
        agent.newFunction("rtc_pass_through", rtc_pass_through)

        # Host‑функции
        def host_compute_trigger_mi8(sim: "pyflamegpu.CUDASimulation"):
            env = sim.getEnvironment()
            _ = env.getPropertyInt("trigger_pr_final_mi8")

        def host_compute_trigger_mi17(sim: "pyflamegpu.CUDASimulation"):
            env = sim.getEnvironment()
            _ = env.getPropertyInt("trigger_pr_final_mi17")

        def host_balance(sim: "pyflamegpu.CUDASimulation"):
            # Балансировка по дефициту/избытку с приоритетами
            env = sim.getEnvironment()
            agents = sim.getAgents("component")
            pop = agents.getPopulationData()
            # Готовим списки по группам
            by_group = {1: [], 2: []}
            for ag in pop:
                gb = ag.getVariableUInt("group_by")
                if gb in by_group:
                    by_group[gb].append(ag)
            # helper: сортировка для сокращения (ppr DESC, sne DESC, mfg_date ASC)
            def sort_key_cut(ag):
                return (-int(ag.getVariableUInt("ppr")), -int(ag.getVariableUInt("sne")), int(ag.getVariableUInt("mfg_date")))
            # вычисляем current_ops и триггеры на базе target из окружения
            for grp, env_field in [(1, "trigger_pr_final_mi8"), (2, "trigger_pr_final_mi17")]:
                group_agents = by_group.get(grp, [])
                current_ops = sum(1 for ag in group_agents if ag.getVariableUInt("status_id") == 2 and ag.getVariableUInt("status_change") == 0)
                target_ops = int(env.getPropertyInt(env_field))
                trigger = target_ops - current_ops
                if trigger < 0:
                    # Сокращаем из OPS → 3
                    candidates = [ag for ag in group_agents if ag.getVariableUInt("status_id") == 2 and ag.getVariableUInt("status_change") == 0]
                    candidates.sort(key=sort_key_cut)
                    for ag in candidates[:abs(trigger)]:
                        ag.setVariableUInt("status_change", 3)
                elif trigger > 0:
                    remaining = trigger
                    # Phase1: 5→2
                    for ag in group_agents:
                        if remaining <= 0:
                            break
                        if ag.getVariableUInt("status_id") == 5 and ag.getVariableUInt("status_change") == 0:
                            ag.setVariableUInt("status_change", 2)
                            remaining -= 1
                    # Phase2: 3→2
                    if remaining > 0:
                        for ag in group_agents:
                            if remaining <= 0:
                                break
                            if ag.getVariableUInt("status_id") == 3 and ag.getVariableUInt("status_change") == 0:
                                ag.setVariableUInt("status_change", 2)
                                remaining -= 1
                    # Phase3: 1→2 при (D - version_date) >= repair_time
                    if remaining > 0:
                        current_day_ord = int(env.getPropertyUInt("current_day_ordinal"))
                        for ag in group_agents:
                            if remaining <= 0:
                                break
                            if ag.getVariableUInt("status_id") == 1 and ag.getVariableUInt("status_change") == 0:
                                version_date = int(ag.getVariableUInt("version_date"))
                                repair_time = int(ag.getVariableUInt("repair_time"))
                                if current_day_ord - version_date >= repair_time:
                                    ag.setVariableUInt("status_change", 2)
                                    remaining -= 1

        model.addInitFunction(pyflamegpu.HostFunction(host_compute_trigger_mi8))
        model.addInitFunction(pyflamegpu.HostFunction(host_compute_trigger_mi17))

        # Порядок выполнения в сутках
        layer = model.newLayer()
        layer.addAgentFunction(agent.getFunction("rtc_repair"))
        layer = model.newLayer()
        layer.addAgentFunction(agent.getFunction("rtc_ops_check"))
        # Host балансировка
        model.addStepFunction(pyflamegpu.HostFunction(host_balance))
        # Основные переходы/эффекты
        layer = model.newLayer()
        layer.addAgentFunction(agent.getFunction("rtc_main"))
        layer = model.newLayer()
        layer.addAgentFunction(agent.getFunction("rtc_change"))
        layer = model.newLayer()
        layer.addAgentFunction(agent.getFunction("rtc_pass_through"))

        self.model = model
        return model

    def build_simulation(self) -> Optional["pyflamegpu.CUDASimulation"]:
        if pyflamegpu is None:
            return None
        if self.model is None:
            raise RuntimeError("Model is not built. Call build_model(num_agents) first.")
        sim = pyflamegpu.CUDASimulation(self.model)
        self.sim = sim
        return sim
