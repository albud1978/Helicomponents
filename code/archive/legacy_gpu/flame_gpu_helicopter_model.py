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
import os
import os

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

        # Минимальный RTC-пробник: если задан FLAMEGPU_PROBE, строим изолированную модель
        probe_name = os.getenv("FLAMEGPU_PROBE", "").strip().lower()
        rtc_minimal = os.getenv("RTC_MINIMAL", "").strip().lower() in ("1", "true", "yes")
        if rtc_minimal:
            agent = model.newAgent("component")
            agent.newVariableUInt("status_id", 0)
            agent.newVariableUInt("repair_days", 0)
            agent.newVariableUInt("repair_time", 0)
            agent.newVariableUInt("ppr", 0)
            rtc_repair_src = r"""
            FLAMEGPU_AGENT_FUNCTION(rtc_repair, flamegpu::MessageNone, flamegpu::MessageNone) {
                unsigned int status_id = FLAMEGPU->getVariable<unsigned int>("status_id");
                if (status_id == 4u) {
                    unsigned int rd = FLAMEGPU->getVariable<unsigned int>("repair_days") + 1u;
                    FLAMEGPU->setVariable<unsigned int>("repair_days", rd);
                    unsigned int rt = FLAMEGPU->getVariable<unsigned int>("repair_time");
                    if (rd >= rt) { FLAMEGPU->setVariable<unsigned int>("status_id", 5u); FLAMEGPU->setVariable<unsigned int>("repair_days", 0u); FLAMEGPU->setVariable<unsigned int>("ppr", 0u); }
                }
                return flamegpu::ALIVE;
            }
            """
            agent.newRTCFunction("rtc_repair", rtc_repair_src)
            lyr = model.newLayer()
            lyr.addAgentFunction(agent.getFunction("rtc_repair"))
            self.model = model
            return model

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
        # Статусы
        agent.newVariableUInt("status_id", 0)
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

        # Режим быстрого подтверждения пайплайна без RTC (Host-only)
        host_only = os.getenv("HOST_ONLY_SIM", "").strip().lower() in ("1", "true", "yes")

        if host_only and pyflamegpu is not None:
            class HostLayerRepairStore(pyflamegpu.HostFunction):
                def run(self, FLAMEGPU):
                    agent = FLAMEGPU.agent("component")
                    pop = agent.getPopulationData()
                    for ag in pop:
                        status = ag.getVariableUInt("status_id")
                        if status == 4:
                            rd = ag.getVariableUInt("repair_days") + 1
                            ag.setVariableUInt("repair_days", rd)
                            if rd >= ag.getVariableUInt("repair_time"):
                                ag.setVariableUInt("status_id", 5)
                                ag.setVariableUInt("ppr", 0)
                                ag.setVariableUInt("repair_days", 0)
                        elif status == 6:
                            # Ничего не делаем
                            pass
            # Регистрируем только Host-слой без RTC
            model.addStepFunction(HostLayerRepairStore())
            self.model = model
            return model

        # === RTC (CUDA) функции агентов ===
        rtc_repair_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_repair, flamegpu::MessageNone, flamegpu::MessageNone) {
            unsigned int status_id = FLAMEGPU->getVariable<unsigned int>("status_id");
            if (status_id == 4u) {
                unsigned int rd = FLAMEGPU->getVariable<unsigned int>("repair_days") + 1u;
                FLAMEGPU->setVariable<unsigned int>("repair_days", rd);
                unsigned int rt = FLAMEGPU->getVariable<unsigned int>("repair_time");
                if (rd >= rt) {
                    // Завершение ремонта: немедленно 4->5 без status_change
                    FLAMEGPU->setVariable<unsigned int>("status_id", 5u);
                    FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
                    FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
                }
            }
            return flamegpu::ALIVE;
        }
        """
        rtc_ops_check_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_ops_check, flamegpu::MessageNone, flamegpu::MessageNone) {
            if (FLAMEGPU->getVariable<unsigned int>("status_id") != 2u) return flamegpu::ALIVE;
            unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
            unsigned int dt = FLAMEGPU->environment.getPropertyArray<unsigned int>("daily_today", idx);
            unsigned int dn = FLAMEGPU->environment.getPropertyArray<unsigned int>("daily_next", idx);
            unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
            unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
            unsigned int ll  = FLAMEGPU->getVariable<unsigned int>("ll");
            unsigned int oh  = FLAMEGPU->getVariable<unsigned int>("oh");
            unsigned int br  = FLAMEGPU->getVariable<unsigned int>("br");
            unsigned int rt  = FLAMEGPU->getVariable<unsigned int>("repair_time");
            unsigned int rem_ll = (ll >= sne ? (ll - sne) : 0u);
            if (rem_ll >= dt && rem_ll < (dt + dn)) {
                // Завтра попадём в окно LL → на D+1 хранение
                FLAMEGPU->setVariable<unsigned int>("status_id", 6u);
                return flamegpu::ALIVE;
            }
            unsigned int rem_oh = (oh >= ppr ? (oh - ppr) : 0u);
            if (rem_oh >= dt && rem_oh < (dt + dn)) {
                if ((sne + dt) < br) {
                    // Завтра ремонт → выставляем статус и сайд‑эффекты сразу
                    FLAMEGPU->setVariable<unsigned int>("status_id", 4u);
                    FLAMEGPU->setVariable<unsigned int>("repair_days", 1u);
                    unsigned int current_day_ord = FLAMEGPU->environment.getProperty<unsigned int>("current_day_ordinal");
                    unsigned int pt = FLAMEGPU->environment.getPropertyArray<unsigned int>("partout_time_arr", idx);
                    unsigned int at = FLAMEGPU->environment.getPropertyArray<unsigned int>("assembly_time_arr", idx);
                    unsigned int asm_days = (rt > at ? (rt - at) : 0u);
                    FLAMEGPU->setVariable<unsigned int>("partout_trigger_ord", current_day_ord + pt);
                    FLAMEGPU->setVariable<unsigned int>("assembly_trigger_ord", current_day_ord + asm_days);
                } else {
                    FLAMEGPU->setVariable<unsigned int>("status_id", 6u);
                }
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
            return flamegpu::ALIVE;
        }
        """
        rtc_change_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_change, flamegpu::MessageNone, flamegpu::MessageNone) {
            // Без отложенных переходов: слой не используется
            return flamegpu::ALIVE;
        }
        """
        rtc_pass_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_pass_through, flamegpu::MessageNone, flamegpu::MessageNone) {
            // pass-through invariant
            return flamegpu::ALIVE;
        }
        """
        probe = os.getenv("FLAMEGPU_PROBE", "").strip().lower()
        if probe:
            if probe == "repair":
                # Минимальная агентная спецификация для rtc_repair
                agent = self.agent
                # Гарантируем, что базовые переменные есть
                # (в основной модели уже объявлены)
                agent.newRTCFunction("rtc_repair", rtc_repair_src)
            elif probe == "ops_check":
                agent = self.agent
                agent.newRTCFunction("rtc_ops_check", rtc_ops_check_src)
            elif probe == "main":
                agent = self.agent
                agent.newRTCFunction("rtc_main", rtc_main_src)
            elif probe == "change":
                agent = self.agent
                agent.newRTCFunction("rtc_change", rtc_change_src)
            elif probe == "pass":
                agent = self.agent
                agent.newRTCFunction("rtc_pass_through", rtc_pass_src)
        else:
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

        # === Каркас сообщений и контроллера для полной GPU логики ===
        # Контроллер (singleton) для балансировки на сообщениях
        controller = model.newAgent("controller")
        # Индекс текущего дня
        controller.newVariableUInt("day_index", 0)
        # Переменные контроллера для балансировки
        controller.newVariableInt("cur_ops_mi8", 0)
        controller.newVariableInt("cur_ops_mi17", 0)
        controller.newVariableInt("need_add_mi8", 0)
        controller.newVariableInt("need_add_mi17", 0)
        controller.newVariableInt("need_cut_mi8", 0)
        controller.newVariableInt("need_cut_mi17", 0)
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
        # Разделённый score: ppr как старшее слово, sne<<16|inv_mfg16 как младшее
        msg_cut.newVariableUInt("score_hi")
        msg_cut.newVariableUInt("score_lo")

        # 4) Решения контроллера на сутки (назначение статуса 2/3)
        msg_assign = model.newMessageBruteForce("assignment")
        msg_assign.newVariableUInt("psn")
        msg_assign.newVariableUInt8("new_status_id")

        # === RTC функции публикации сообщений компонентом ===
        rtc_pub_ops_persist_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_publish_ops_persist, flamegpu::MessageNone, flamegpu::MessageBruteForce) {
            if (FLAMEGPU->getVariable<unsigned int>("status_id") == 2u && FLAMEGPU->getVariable<unsigned int>("status_change") == 0u) {
                flamegpu::MessageBruteForce::Out msg = FLAMEGPU->message_out;
                msg.setVariable<unsigned int>("psn", FLAMEGPU->getVariable<unsigned int>("psn"));
                msg.setVariable<unsigned char>("group_by", (unsigned char)FLAMEGPU->getVariable<unsigned int>("group_by"));
            }
            return flamegpu::ALIVE;
        }
        """

        rtc_pub_add_p1_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_publish_add_candidates_p1, flamegpu::MessageNone, flamegpu::MessageBruteForce) {
            if (FLAMEGPU->getVariable<unsigned int>("status_id") == 5u && FLAMEGPU->getVariable<unsigned int>("status_change") == 0u) {
                auto msg = FLAMEGPU->message_out;
                msg.setVariable<unsigned int>("psn", FLAMEGPU->getVariable<unsigned int>("psn"));
                msg.setVariable<unsigned char>("group_by", (unsigned char)FLAMEGPU->getVariable<unsigned int>("group_by"));
            }
            return flamegpu::ALIVE;
        }
        """

        rtc_pub_add_p2_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_publish_add_candidates_p2, flamegpu::MessageNone, flamegpu::MessageBruteForce) {
            if (FLAMEGPU->getVariable<unsigned int>("status_id") == 3u && FLAMEGPU->getVariable<unsigned int>("status_change") == 0u) {
                auto msg = FLAMEGPU->message_out;
                msg.setVariable<unsigned int>("psn", FLAMEGPU->getVariable<unsigned int>("psn"));
                msg.setVariable<unsigned char>("group_by", (unsigned char)FLAMEGPU->getVariable<unsigned int>("group_by"));
            }
            return flamegpu::ALIVE;
        }
        """

        rtc_pub_add_p3_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_publish_add_candidates_p3, flamegpu::MessageNone, flamegpu::MessageBruteForce) {
            if (FLAMEGPU->getVariable<unsigned int>("status_id") == 1u && FLAMEGPU->getVariable<unsigned int>("status_change") == 0u) {
                unsigned int current_day_ord = FLAMEGPU->environment.getProperty<unsigned int>("current_day_ordinal");
                unsigned int version_date = FLAMEGPU->getVariable<unsigned int>("version_date");
                unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
                if (current_day_ord >= version_date && (current_day_ord - version_date) >= repair_time) {
                    auto msg = FLAMEGPU->message_out;
                    msg.setVariable<unsigned int>("psn", FLAMEGPU->getVariable<unsigned int>("psn"));
                    msg.setVariable<unsigned char>("group_by", (unsigned char)FLAMEGPU->getVariable<unsigned int>("group_by"));
                    msg.setVariable<unsigned char>("can_activate", (unsigned char)1);
                }
            }
            return flamegpu::ALIVE;
        }
        """

        rtc_pub_cut_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_publish_cut_candidates, flamegpu::MessageNone, flamegpu::MessageBruteForce) {
            if (FLAMEGPU->getVariable<unsigned int>("status_id") == 2u && FLAMEGPU->getVariable<unsigned int>("status_change") == 0u) {
                unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
                unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
                unsigned int mfg = FLAMEGPU->getVariable<unsigned int>("mfg_date");
                unsigned int mfg16 = mfg & 0xFFFFu;
                unsigned int inv_mfg16 = 0xFFFFu - mfg16;
                unsigned int lo = (sne << 16) | (inv_mfg16 & 0xFFFFu);
                auto msg = FLAMEGPU->message_out;
                msg.setVariable<unsigned int>("psn", FLAMEGPU->getVariable<unsigned int>("psn"));
                msg.setVariable<unsigned char>("group_by", (unsigned char)FLAMEGPU->getVariable<unsigned int>("group_by"));
                msg.setVariable<unsigned int>("score_hi", ppr);
                msg.setVariable<unsigned int>("score_lo", lo);
            }
            return flamegpu::ALIVE;
        }
        """

        rtc_apply_assign_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_apply_assignments, flamegpu::MessageBruteForce, flamegpu::MessageNone) {
            if (FLAMEGPU->getVariable<unsigned int>("status_change") != 0u) return flamegpu::ALIVE;
            unsigned int self_psn = FLAMEGPU->getVariable<unsigned int>("psn");
            for (const auto &msg : FLAMEGPU->message_in) {
                unsigned int psn = msg.getVariable<unsigned int>("psn");
                if (psn == self_psn) {
                    unsigned int ns = (unsigned int)msg.getVariable<unsigned char>("new_status_id");
                    if (ns == 2u || ns == 3u) {
                        FLAMEGPU->setVariable<unsigned int>("status_change", ns);
                    }
                    break;
                }
            }
            return flamegpu::ALIVE;
        }
        """

        layer_mode = os.getenv("LAYER_MODE", "full").strip().lower()
        if probe:
            if probe == "pub_ops":
                f = agent.newRTCFunction("rtc_publish_ops_persist", rtc_pub_ops_persist_src); f.setMessageOutput("ops_persist")
            elif probe == "pub_p1":
                f = agent.newRTCFunction("rtc_publish_add_candidates_p1", rtc_pub_add_p1_src); f.setMessageOutput("add_candidate_p1")
            elif probe == "pub_p2":
                f = agent.newRTCFunction("rtc_publish_add_candidates_p2", rtc_pub_add_p2_src); f.setMessageOutput("add_candidate_p2")
            elif probe == "pub_p3":
                f = agent.newRTCFunction("rtc_publish_add_candidates_p3", rtc_pub_add_p3_src); f.setMessageOutput("add_candidate_p3")
            elif probe == "pub_cut":
                f = agent.newRTCFunction("rtc_publish_cut_candidates", rtc_pub_cut_src); f.setMessageOutput("cut_candidate")
            elif probe == "apply":
                f = agent.newRTCFunction("rtc_apply_assignments", rtc_apply_assign_src); f.setMessageInput("assignment")
        else:
            if layer_mode != "repair_only":
                f = agent.newRTCFunction("rtc_publish_ops_persist", rtc_pub_ops_persist_src); f.setMessageOutput("ops_persist")
                f = agent.newRTCFunction("rtc_publish_add_candidates_p1", rtc_pub_add_p1_src); f.setMessageOutput("add_candidate_p1")
                f = agent.newRTCFunction("rtc_publish_add_candidates_p2", rtc_pub_add_p2_src); f.setMessageOutput("add_candidate_p2")
                f = agent.newRTCFunction("rtc_publish_add_candidates_p3", rtc_pub_add_p3_src); f.setMessageOutput("add_candidate_p3")
                f = agent.newRTCFunction("rtc_publish_cut_candidates", rtc_pub_cut_src); f.setMessageOutput("cut_candidate")
                f = agent.newRTCFunction("rtc_apply_assignments", rtc_apply_assign_src); f.setMessageInput("assignment")

        # === RTC функции контроллера (один агент) ===
        ctrl_count_ops_src = r"""
        FLAMEGPU_AGENT_FUNCTION(ctrl_count_ops, flamegpu::MessageBruteForce, flamegpu::MessageNone) {
            // Count OPS per group
            int cur8 = 0, cur17 = 0;
            for (const auto &msg : FLAMEGPU->message_in) {
                unsigned char gb = msg.getVariable<unsigned char>("group_by");
                if (gb == 1u) cur8++;
                else if (gb == 2u) cur17++;
            }
            FLAMEGPU->setVariable<int>("cur_ops_mi8", cur8);
            FLAMEGPU->setVariable<int>("cur_ops_mi17", cur17);
            int tgt8 = FLAMEGPU->environment.getProperty<int>("trigger_pr_final_mi8");
            int tgt17 = FLAMEGPU->environment.getProperty<int>("trigger_pr_final_mi17");
            int need_add8 = tgt8 - cur8; if (need_add8 < 0) need_add8 = 0;
            int need_add17 = tgt17 - cur17; if (need_add17 < 0) need_add17 = 0;
            int need_cut8 = cur8 - tgt8; if (need_cut8 < 0) need_cut8 = 0;
            int need_cut17 = cur17 - tgt17; if (need_cut17 < 0) need_cut17 = 0;
            FLAMEGPU->setVariable<int>("need_add_mi8", need_add8);
            FLAMEGPU->setVariable<int>("need_add_mi17", need_add17);
            FLAMEGPU->setVariable<int>("need_cut_mi8", need_cut8);
            FLAMEGPU->setVariable<int>("need_cut_mi17", need_cut17);
            return flamegpu::ALIVE;
        }
        """

        ctrl_pick_add_p1_src = r"""
        FLAMEGPU_AGENT_FUNCTION(ctrl_pick_add_p1, flamegpu::MessageBruteForce, flamegpu::MessageBruteForce) {
            int need8 = FLAMEGPU->getVariable<int>("need_add_mi8");
            int need17 = FLAMEGPU->getVariable<int>("need_add_mi17");
            if (need8 == 0 && need17 == 0) return flamegpu::ALIVE;
            for (const auto &msg : FLAMEGPU->message_in) {
                unsigned char gb = msg.getVariable<unsigned char>("group_by");
                if (gb == 1u && need8 > 0) {
                    auto out = FLAMEGPU->message_out;
                    out.setVariable<unsigned int>("psn", msg.getVariable<unsigned int>("psn"));
                    out.setVariable<unsigned char>("new_status_id", (unsigned char)2);
                    need8--; FLAMEGPU->setVariable<int>("need_add_mi8", need8);
                    if (need8 == 0 && need17 == 0) break;
                } else if (gb == 2u && need17 > 0) {
                    auto out = FLAMEGPU->message_out;
                    out.setVariable<unsigned int>("psn", msg.getVariable<unsigned int>("psn"));
                    out.setVariable<unsigned char>("new_status_id", (unsigned char)2);
                    need17--; FLAMEGPU->setVariable<int>("need_add_mi17", need17);
                    if (need8 == 0 && need17 == 0) break;
                }
            }
            return flamegpu::ALIVE;
        }
        """

        ctrl_pick_add_p2_src = r"""
        FLAMEGPU_AGENT_FUNCTION(ctrl_pick_add_p2, flamegpu::MessageBruteForce, flamegpu::MessageBruteForce) {
            int need8 = FLAMEGPU->getVariable<int>("need_add_mi8");
            int need17 = FLAMEGPU->getVariable<int>("need_add_mi17");
            if (need8 == 0 && need17 == 0) return flamegpu::ALIVE;
            for (const auto &msg : FLAMEGPU->message_in) {
                unsigned char gb = msg.getVariable<unsigned char>("group_by");
                if (gb == 1u && need8 > 0) {
                    auto out = FLAMEGPU->message_out;
                    out.setVariable<unsigned int>("psn", msg.getVariable<unsigned int>("psn"));
                    out.setVariable<unsigned char>("new_status_id", (unsigned char)2);
                    need8--; FLAMEGPU->setVariable<int>("need_add_mi8", need8);
                    if (need8 == 0 && need17 == 0) break;
                } else if (gb == 2u && need17 > 0) {
                    auto out = FLAMEGPU->message_out;
                    out.setVariable<unsigned int>("psn", msg.getVariable<unsigned int>("psn"));
                    out.setVariable<unsigned char>("new_status_id", (unsigned char)2);
                    need17--; FLAMEGPU->setVariable<int>("need_add_mi17", need17);
                    if (need8 == 0 && need17 == 0) break;
                }
            }
            return flamegpu::ALIVE;
        }
        """

        ctrl_pick_add_p3_src = r"""
        FLAMEGPU_AGENT_FUNCTION(ctrl_pick_add_p3, flamegpu::MessageBruteForce, flamegpu::MessageBruteForce) {
            int need8 = FLAMEGPU->getVariable<int>("need_add_mi8");
            int need17 = FLAMEGPU->getVariable<int>("need_add_mi17");
            if (need8 == 0 && need17 == 0) return flamegpu::ALIVE;
            for (const auto &msg : FLAMEGPU->message_in) {
                unsigned char can = msg.getVariable<unsigned char>("can_activate");
                if (!can) continue;
                unsigned char gb = msg.getVariable<unsigned char>("group_by");
                if (gb == 1u && need8 > 0) {
                    auto out = FLAMEGPU->message_out;
                    out.setVariable<unsigned int>("psn", msg.getVariable<unsigned int>("psn"));
                    out.setVariable<unsigned char>("new_status_id", (unsigned char)2);
                    need8--; FLAMEGPU->setVariable<int>("need_add_mi8", need8);
                    if (need8 == 0 && need17 == 0) break;
                } else if (gb == 2u && need17 > 0) {
                    auto out = FLAMEGPU->message_out;
                    out.setVariable<unsigned int>("psn", msg.getVariable<unsigned int>("psn"));
                    out.setVariable<unsigned char>("new_status_id", (unsigned char)2);
                    need17--; FLAMEGPU->setVariable<int>("need_add_mi17", need17);
                    if (need8 == 0 && need17 == 0) break;
                }
            }
            return flamegpu::ALIVE;
        }
        """

        ctrl_pick_cut_src = r"""
        FLAMEGPU_AGENT_FUNCTION(ctrl_pick_cut, flamegpu::MessageBruteForce, flamegpu::MessageBruteForce) {
            int need8 = FLAMEGPU->getVariable<int>("need_cut_mi8");
            int need17 = FLAMEGPU->getVariable<int>("need_cut_mi17");
            if (need8 == 0 && need17 == 0) return flamegpu::ALIVE;
            const int K8 = need8; const int K17 = need17;
            const int MAXK = 4096; // ограничение сверху
            unsigned int hi8[4096]; unsigned int lo8[4096]; unsigned int psn8[4096]; int len8 = 0;
            unsigned int hi17[4096]; unsigned int lo17[4096]; unsigned int psn17[4096]; int len17 = 0;
            for (const auto &msg : FLAMEGPU->message_in) {
                unsigned char gb = msg.getVariable<unsigned char>("group_by");
                unsigned int shi = msg.getVariable<unsigned int>("score_hi");
                unsigned int slo = msg.getVariable<unsigned int>("score_lo");
                unsigned int p = msg.getVariable<unsigned int>("psn");
                if (gb == 1u && K8 > 0) {
                    if (len8 < K8 && len8 < MAXK) { hi8[len8] = shi; lo8[len8] = slo; psn8[len8] = p; len8++; }
                    else {
                        int imin = 0; for (int i=1;i<len8;i++) {
                            if (hi8[i] < hi8[imin] || (hi8[i] == hi8[imin] && lo8[i] <= lo8[imin])) imin = i;
                        }
                        if (len8>0 && (shi > hi8[imin] || (shi == hi8[imin] && slo > lo8[imin]))) { hi8[imin] = shi; lo8[imin] = slo; psn8[imin] = p; }
                    }
                } else if (gb == 2u && K17 > 0) {
                    if (len17 < K17 && len17 < MAXK) { hi17[len17] = shi; lo17[len17] = slo; psn17[len17] = p; len17++; }
                    else {
                        int imin = 0; for (int i=1;i<len17;i++) {
                            if (hi17[i] < hi17[imin] || (hi17[i] == hi17[imin] && lo17[i] <= lo17[imin])) imin = i;
                        }
                        if (len17>0 && (shi > hi17[imin] || (shi == hi17[imin] && slo > lo17[imin]))) { hi17[imin] = shi; lo17[imin] = slo; psn17[imin] = p; }
                    }
                }
            }
            // Публикация назначений 3
            for (int i=0;i<len8;i++) { auto out = FLAMEGPU->message_out; out.setVariable<unsigned int>("psn", psn8[i]); out.setVariable<unsigned char>("new_status_id", (unsigned char)3); }
            for (int i=0;i<len17;i++) { auto out = FLAMEGPU->message_out; out.setVariable<unsigned int>("psn", psn17[i]); out.setVariable<unsigned char>("new_status_id", (unsigned char)3); }
            return flamegpu::ALIVE;
        }
        """

        if probe:
            if probe == "ctrl_count":
                f = controller.newRTCFunction("ctrl_count_ops", ctrl_count_ops_src); f.setMessageInput("ops_persist")
            elif probe == "ctrl_add_p1":
                f = controller.newRTCFunction("ctrl_pick_add_p1", ctrl_pick_add_p1_src); f.setMessageInput("add_candidate_p1"); f.setMessageOutput("assignment")
            elif probe == "ctrl_add_p2":
                f = controller.newRTCFunction("ctrl_pick_add_p2", ctrl_pick_add_p2_src); f.setMessageInput("add_candidate_p2"); f.setMessageOutput("assignment")
            elif probe == "ctrl_add_p3":
                f = controller.newRTCFunction("ctrl_pick_add_p3", ctrl_pick_add_p3_src); f.setMessageInput("add_candidate_p3"); f.setMessageOutput("assignment")
            elif probe == "ctrl_cut":
                f = controller.newRTCFunction("ctrl_pick_cut", ctrl_pick_cut_src); f.setMessageInput("cut_candidate"); f.setMessageOutput("assignment")
        else:
            if layer_mode != "repair_only":
                f = controller.newRTCFunction("ctrl_count_ops", ctrl_count_ops_src); f.setMessageInput("ops_persist")
                f = controller.newRTCFunction("ctrl_pick_add_p1", ctrl_pick_add_p1_src); f.setMessageInput("add_candidate_p1"); f.setMessageOutput("assignment")
                f = controller.newRTCFunction("ctrl_pick_add_p2", ctrl_pick_add_p2_src); f.setMessageInput("add_candidate_p2"); f.setMessageOutput("assignment")
                f = controller.newRTCFunction("ctrl_pick_add_p3", ctrl_pick_add_p3_src); f.setMessageInput("add_candidate_p3"); f.setMessageOutput("assignment")
                f = controller.newRTCFunction("ctrl_pick_cut", ctrl_pick_cut_src); f.setMessageInput("cut_candidate"); f.setMessageOutput("assignment")

        # Порядок выполнения в сутках (RTC на GPU) — после регистрации всех функций
        probe = os.getenv("FLAMEGPU_PROBE", "").strip().lower()
        def add(fn_name: str, from_agent: bool = True):
            lyr = model.newLayer()
            if from_agent:
                lyr.addAgentFunction(agent.getFunction(fn_name))
            else:
                lyr.addAgentFunction(controller.getFunction(fn_name))
        if probe:
            # Режим отладки: выполняем только указанную функцию
            mapping = {
                "repair": ("rtc_repair", True),
                "ops_check": ("rtc_ops_check", True),
                "pub_ops": ("rtc_publish_ops_persist", True),
                "pub_p1": ("rtc_publish_add_candidates_p1", True),
                "pub_p2": ("rtc_publish_add_candidates_p2", True),
                "pub_p3": ("rtc_publish_add_candidates_p3", True),
                "pub_cut": ("rtc_publish_cut_candidates", True),
                "ctrl_count": ("ctrl_count_ops", False),
                "ctrl_add_p1": ("ctrl_pick_add_p1", False),
                "ctrl_add_p2": ("ctrl_pick_add_p2", False),
                "ctrl_add_p3": ("ctrl_pick_add_p3", False),
                "ctrl_cut": ("ctrl_pick_cut", False),
                "apply": ("rtc_apply_assignments", True),
                "main": ("rtc_main", True),
                "change": ("rtc_change", True),
                "pass": ("rtc_pass_through", True),
            }
            if probe in mapping:
                fn, is_agent = mapping[probe]
                add(fn, is_agent)
            else:
                add("rtc_pass_through", True)
        else:
            layer_mode = os.getenv("LAYER_MODE", "full").strip().lower()
            if layer_mode == "repair_only":
                add("rtc_repair", True)
            elif layer_mode in ("repair_ops", "repair_ops_only"):
                # Минимальный набор: ремонт + проверка OPS без мейн/чейндж
                add("rtc_repair", True)
                add("rtc_ops_check", True)
            else:
                add("rtc_repair", True)
                add("rtc_ops_check", True)
                add("rtc_publish_ops_persist", True)
                add("rtc_publish_add_candidates_p1", True)
                add("rtc_publish_add_candidates_p2", True)
                add("rtc_publish_add_candidates_p3", True)
                add("rtc_publish_cut_candidates", True)
                add("ctrl_count_ops", False)
                add("ctrl_pick_add_p1", False)
                add("ctrl_pick_add_p2", False)
                add("ctrl_pick_add_p3", False)
                add("ctrl_pick_cut", False)
                add("rtc_apply_assignments", True)
                add("rtc_main", True)
                add("rtc_change", True)
                add("rtc_pass_through", True)

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
