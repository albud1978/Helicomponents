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
        env.newPropertyInt("trigger_program_mi8", 0)
        env.newPropertyInt("trigger_program_mi17", 0)
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
        agent.newVariableUInt("partout_time", 0)
        agent.newVariableUInt("assembly_time", 0)
        # Служебные даты-триггеры (как ordinal)
        agent.newVariableUInt("partout_trigger_ord", 0)
        agent.newVariableUInt("assembly_trigger_ord", 0)
        agent.newVariableUInt("active_trigger_ord", 0)

        # === Host RTC‑функции в виде классов PyFLAMEGPU ===
        class HostRTCRepair(pyflamegpu.HostFunction):
            def run(self, sim):
                agents = sim.getAgents("component")
                pop = agents.getPopulationData()
                for ag in pop:
                    if ag.getVariableUInt("status_id") == 4:
                        rd = ag.getVariableUInt("repair_days") + 1
                        ag.setVariableUInt("repair_days", rd)
                        if rd >= ag.getVariableUInt("repair_time"):
                            ag.setVariableUInt("status_change", 5)

        class HostRTCOpsCheck(pyflamegpu.HostFunction):
            def run(self, sim):
                env = sim.getEnvironment()
                dt_arr = env.getPropertyArrayUInt32("daily_today")
                dn_arr = env.getPropertyArrayUInt32("daily_next")
                agents = sim.getAgents("component")
                pop = agents.getPopulationData()
                for ag in pop:
                    if ag.getVariableUInt("status_id") != 2:
                        continue
                    if ag.getVariableUInt("status_change") != 0:
                        env.setPropertyInt("ops_check_violation", env.getPropertyInt("ops_check_violation") + 1)
                        continue
                    idx = ag.getVariableUInt("idx")
                    dt = int(dt_arr[idx]) if idx < len(dt_arr) else 0
                    dn = int(dn_arr[idx]) if idx < len(dn_arr) else 0
                    sne = ag.getVariableUInt("sne")
                    ppr = ag.getVariableUInt("ppr")
                    ll = ag.getVariableUInt("ll")
                    oh = ag.getVariableUInt("oh")
                    br = ag.getVariableUInt("br")
                    if (ll - sne) >= dt and (ll - sne) < (dt + dn):
                        ag.setVariableUInt("status_change", 6)
                        continue
                    if (oh - ppr) >= dt and (oh - ppr) < (dt + dn):
                        if (sne + dt) < br:
                            ag.setVariableUInt("status_change", 4)
                        else:
                            ag.setVariableUInt("status_change", 6)

        class HostRTCMain(pyflamegpu.HostFunction):
            def run(self, sim):
                env = sim.getEnvironment()
                dt_arr = env.getPropertyArrayUInt32("daily_today")
                agents = sim.getAgents("component")
                pop = agents.getPopulationData()
                for ag in pop:
                    if ag.getVariableUInt("status_id") == 2:
                        idx = ag.getVariableUInt("idx")
                        dt = int(dt_arr[idx]) if idx < len(dt_arr) else 0
                        ag.setVariableUInt("sne", ag.getVariableUInt("sne") + dt)
                        ag.setVariableUInt("ppr", ag.getVariableUInt("ppr") + dt)
                    chg = ag.getVariableUInt("status_change")
                    if chg:
                        ag.setVariableUInt("status_id", chg)

        class HostRTCChange(pyflamegpu.HostFunction):
            def run(self, sim):
                env = sim.getEnvironment()
                current_day_ord = int(env.getPropertyUInt("current_day_ordinal"))
                pt_arr = env.getPropertyArrayUInt32("partout_time_arr")
                at_arr = env.getPropertyArrayUInt32("assembly_time_arr")
                agents = sim.getAgents("component")
                pop = agents.getPopulationData()
                for ag in pop:
                    chg = ag.getVariableUInt("status_change")
                    prev_status = ag.getVariableUInt("status_id")
                    idx = ag.getVariableUInt("idx")
                    rt = int(ag.getVariableUInt("repair_time"))
                    pt = int(pt_arr[idx]) if idx < len(pt_arr) else 0
                    at = int(at_arr[idx]) if idx < len(at_arr) else 0
                    if chg == 4:
                        ag.setVariableUInt("repair_days", 1)
                        ag.setVariableUInt("partout_trigger_ord", current_day_ord + pt)
                        ag.setVariableUInt("assembly_trigger_ord", current_day_ord + max(rt - at, 0))
                    elif chg == 5:
                        ag.setVariableUInt("ppr", 0)
                        ag.setVariableUInt("repair_days", 0)
                    elif chg == 2 and prev_status == 1:
                        active_ord = current_day_ord - rt if current_day_ord >= rt else 0
                        ag.setVariableUInt("active_trigger_ord", active_ord)
                        asm_ord = current_day_ord - at if current_day_ord >= at else 0
                        ag.setVariableUInt("assembly_trigger_ord", asm_ord)
                    if chg != 0:
                        ag.setVariableUInt("status_change", 0)

        class HostRTCPass(pyflamegpu.HostFunction):
            def run(self, sim):
                env = sim.getEnvironment()
                agents = sim.getAgents("component")
                pop = agents.getPopulationData()
                for ag in pop:
                    if ag.getVariableUInt("status_change") != 0:
                        env.setPropertyInt("pass_through_violation", env.getPropertyInt("pass_through_violation") + 1)

        # === RTC (CUDA) функции агентов ===
        rtc_repair_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_repair, flamegpu::MessageNone, flamegpu::MessageNone) {
            unsigned int status_id = FLAMEGPU->getVariable<unsigned int>("status_id");
            if (status_id == 4u) {
                unsigned int rd = FLAMEGPU->getVariable<unsigned int>("repair_days") + 1u;
                FLAMEGPU->setVariable<unsigned int>("repair_days", rd);
                unsigned int rt = FLAMEGPU->getVariable<unsigned int>("repair_time");
                if (rd >= rt) {
                    FLAMEGPU->setVariable<unsigned int>("status_change", 5u);
                }
            }
            return flamegpu::ALIVE;
        }
        """
        rtc_ops_check_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_ops_check, flamegpu::MessageNone, flamegpu::MessageNone) {
            if (FLAMEGPU->getVariable<unsigned int>("status_id") != 2u) return flamegpu::ALIVE;
            if (FLAMEGPU->getVariable<unsigned int>("status_change") != 0u) return flamegpu::ALIVE;
            unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
            unsigned int dt = FLAMEGPU->environment.getPropertyArray<unsigned int>("daily_today", idx);
            unsigned int dn = FLAMEGPU->environment.getPropertyArray<unsigned int>("daily_next", idx);
            unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
            unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
            unsigned int ll  = FLAMEGPU->getVariable<unsigned int>("ll");
            unsigned int oh  = FLAMEGPU->getVariable<unsigned int>("oh");
            unsigned int br  = FLAMEGPU->getVariable<unsigned int>("br");
            if ((ll >= sne ? (ll - sne) : 0u) >= dt && (ll >= sne ? (ll - sne) : 0u) < (dt + dn)) {
                FLAMEGPU->setVariable<unsigned int>("status_change", 6u);
                return flamegpu::ALIVE;
            }
            unsigned int rem = (oh >= ppr ? (oh - ppr) : 0u);
            if (rem >= dt && rem < (dt + dn)) {
                if ((sne + dt) < br) FLAMEGPU->setVariable<unsigned int>("status_change", 4u);
                else FLAMEGPU->setVariable<unsigned int>("status_change", 6u);
            }
            return flamegpu::ALIVE;
        }
        """
        rtc_main_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_main, flamegpu::MessageNone, flamegpu::MessageNone) {
            if (FLAMEGPU->getVariable<unsigned int>("status_id") == 2u) {
                unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
                unsigned int dt = FLAMEGPU->environment.getPropertyArray<unsigned int>("daily_today", idx);
                FLAMEGPU->setVariable<unsigned int>("sne", FLAMEGPU->getVariable<unsigned int>("sne") + dt);
                FLAMEGPU->setVariable<unsigned int>("ppr", FLAMEGPU->getVariable<unsigned int>("ppr") + dt);
            }
            unsigned int chg = FLAMEGPU->getVariable<unsigned int>("status_change");
            if (chg != 0u) FLAMEGPU->setVariable<unsigned int>("status_id", chg);
            return flamegpu::ALIVE;
        }
        """
        rtc_change_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_change, flamegpu::MessageNone, flamegpu::MessageNone) {
            unsigned int chg = FLAMEGPU->getVariable<unsigned int>("status_change");
            unsigned int prev = FLAMEGPU->getVariable<unsigned int>("status_id");
            unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
            unsigned int current_day_ord = FLAMEGPU->environment.getProperty<unsigned int>("current_day_ordinal");
            unsigned int rt = FLAMEGPU->getVariable<unsigned int>("repair_time");
            unsigned int pt = FLAMEGPU->environment.getPropertyArray<unsigned int>("partout_time_arr", idx);
            unsigned int at = FLAMEGPU->environment.getPropertyArray<unsigned int>("assembly_time_arr", idx);
            if (chg == 4u) {
                FLAMEGPU->setVariable<unsigned int>("repair_days", 1u);
                FLAMEGPU->setVariable<unsigned int>("partout_trigger_ord", current_day_ord + pt);
                unsigned int asm_days = (rt > at ? (rt - at) : 0u);
                FLAMEGPU->setVariable<unsigned int>("assembly_trigger_ord", current_day_ord + asm_days);
            } else if (chg == 5u) {
                FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
                FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
            } else if (chg == 2u && prev == 1u) {
                unsigned int active_ord = (current_day_ord >= rt ? (current_day_ord - rt) : 0u);
                FLAMEGPU->setVariable<unsigned int>("active_trigger_ord", active_ord);
                unsigned int asm_ord = (current_day_ord >= at ? (current_day_ord - at) : 0u);
                FLAMEGPU->setVariable<unsigned int>("assembly_trigger_ord", asm_ord);
            }
            if (chg != 0u) FLAMEGPU->setVariable<unsigned int>("status_change", 0u);
            return flamegpu::ALIVE;
        }
        """
        rtc_pass_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_pass_through, flamegpu::MessageNone, flamegpu::MessageNone) {
            // инвариант-проход
            return flamegpu::ALIVE;
        }
        """

        agent.newRTCFunction("rtc_repair", rtc_repair_src)
        agent.newRTCFunction("rtc_ops_check", rtc_ops_check_src)
        agent.newRTCFunction("rtc_main", rtc_main_src)
        agent.newRTCFunction("rtc_change", rtc_change_src)
        agent.newRTCFunction("rtc_pass_through", rtc_pass_src)

        # Host‑функции
        class HostInitMi8(pyflamegpu.HostFunction):
            def run(self, sim):
                env = sim.getEnvironment()
                _ = env.getPropertyInt("trigger_pr_final_mi8")

        class HostInitMi17(pyflamegpu.HostFunction):
            def run(self, sim):
                env = sim.getEnvironment()
                _ = env.getPropertyInt("trigger_pr_final_mi17")

        class HostBalance(pyflamegpu.HostFunction):
            def run(self, sim):
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
                    # Используем программный дневной триггер как прямую квоту перемещения
                    trigger_prog = int(env.getPropertyInt("trigger_program_mi8" if grp == 1 else "trigger_program_mi17"))
                    trigger = trigger_prog
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

        model.addInitFunction(HostInitMi8())
        model.addInitFunction(HostInitMi17())

        # Порядок выполнения в сутках
        # Порядок выполнения в сутках (RTC на GPU)
        layer = model.newLayer()
        layer.addAgentFunction(agent.getFunction("rtc_repair"))
        layer = model.newLayer()
        layer.addAgentFunction(agent.getFunction("rtc_ops_check"))
        # (баланс будет перенесён позже через сообщения)
        layer = model.newLayer()
        layer.addAgentFunction(agent.getFunction("rtc_main"))
        layer = model.newLayer()
        layer.addAgentFunction(agent.getFunction("rtc_change"))
        layer = model.newLayer()
        layer.addAgentFunction(agent.getFunction("rtc_pass_through"))

        # === Каркас сообщений и контроллера для полной GPU логики ===
        # Контроллер (singleton) для балансировки на сообщениях
        controller = model.newAgent("controller")
        # Индекс текущего дня
        controller.newVariableUInt("day_index", 0)
        # Контроллер без массивов: дневные цели/налёты подставляются в env на хосте перед сутками

        # Сообщения (BruteForce) — минимальные поля без лишних имён
        # 1) Действующие OPS, готовые остаться
        msg_ops_persist = model.newMessageBruteForce("ops_persist")
        msg_ops_persist.newVariableUInt("psn")
        msg_ops_persist.newVariableUInt8("group_by")

        # 2) Кандидаты на ввод в OPS: три фазы
        for name in ("add_candidate_p1", "add_candidate_p2", "add_candidate_p3"):
            m = model.newMessageBruteForce(name)
            m.newVariableUInt("psn")
            m.newVariableUInt8("group_by")
            if name == "add_candidate_p3":
                m.newVariableUInt8("can_activate")

        # 3) Кандидаты на сокращение из OPS (с приоритетной метрикой)
        msg_cut = model.newMessageBruteForce("cut_candidate")
        msg_cut.newVariableUInt("psn")
        msg_cut.newVariableUInt8("group_by")
        msg_cut.newVariableUInt64("score")

        # 4) Решения контроллера на сутки (назначение статуса 2/3)
        msg_assign = model.newMessageBruteForce("assignment")
        msg_assign.newVariableUInt("psn")
        msg_assign.newVariableUInt8("new_status_id")

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
