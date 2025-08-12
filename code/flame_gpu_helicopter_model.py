#!/usr/bin/env python3
"""
Flame GPU 2 Helicopter Model (Agent-based)

Модель с 6 RTC-слоями (как агент-функции) и 2 host-функциями для триггеров.
- Агент: component (каждый ряд MP3)
- Переменные агента покрывают поля MP3 + обогащение из MP1 (br, repair_time) и служебные idx
- Окружение: скаляры (триггеры), массивы `daily_today`, `daily_next` (size=N_agents)

Слои (порядок выполнения за сутки D):
1) rtc_repair: status_id=4 → repair_days += 1
2) rtc_ops_check: по LL/OH/BR с daily_today/daily_next выставляет status_change∈{4,6}
3) host_compute_trigger_*: вычисление/установка триггеров (значения задаются раннером)
4) rtc_main: начисляет sne/ppr для status_id=2 и применяет status_change→status_id
5) rtc_change: сайд‑эффекты; при status_change=4 ставит repair_days=1; затем status_change=0
6) rtc_pass_through: без изменений (контроль порядка)

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
        # Массивы с суточным налётом для каждого агента (индексированные по agent.idx)
        env.newPropertyArrayUInt32("daily_today", [0] * self.num_agents)
        env.newPropertyArrayUInt32("daily_next", [0] * self.num_agents)

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

        # === Агент‑функции ===
        def rtc_repair(agent, messages=None, messageOut=None, environment=None):
            if agent.getVariableUInt("status_id") == 4:
                agent.setVariableUInt("repair_days", agent.getVariableUInt("repair_days") + 1)

        def rtc_ops_check(agent, messages=None, messageOut=None, environment=None):
            if agent.getVariableUInt("status_id") != 2:
                return
            if agent.getVariableUInt("status_change") != 0:
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
            # Применение перехода
            chg = agent.getVariableUInt("status_change")
            if chg == 3:
                agent.setVariableUInt("status_id", 3)
            elif chg == 2:
                agent.setVariableUInt("status_id", 2)
            elif chg == 4:
                agent.setVariableUInt("status_id", 4)

        def rtc_change(agent, messages=None, messageOut=None, environment=None):
            # Сайд‑эффекты и сброс метки
            if agent.getVariableUInt("status_change") == 4:
                agent.setVariableUInt("repair_days", 1)
            agent.setVariableUInt("status_change", 0)

        def rtc_pass_through(agent, messages=None, messageOut=None, environment=None):
            return

        agent.newFunction("rtc_repair", rtc_repair)
        agent.newFunction("rtc_ops_check", rtc_ops_check)
        agent.newFunction("rtc_main", rtc_main)
        agent.newFunction("rtc_change", rtc_change)
        agent.newFunction("rtc_pass_through", rtc_pass_through)

        # Host‑функции триггеров (реальные значения заполняет раннер)
        def host_compute_trigger_mi8(sim: "pyflamegpu.CUDASimulation"):
            env = sim.getEnvironment()
            _ = env.getPropertyInt("trigger_pr_final_mi8")

        def host_compute_trigger_mi17(sim: "pyflamegpu.CUDASimulation"):
            env = sim.getEnvironment()
            _ = env.getPropertyInt("trigger_pr_final_mi17")

        model.addInitFunction(pyflamegpu.HostFunction(host_compute_trigger_mi8))
        model.addInitFunction(pyflamegpu.HostFunction(host_compute_trigger_mi17))

        # Порядок выполнения в сутках: repair → ops_check → (host) → main → change → pass
        layer = model.newLayer()
        layer.addAgentFunction(agent.getFunction("rtc_repair"))
        layer = model.newLayer()
        layer.addAgentFunction(agent.getFunction("rtc_ops_check"))
        # host вычисление триггеров происходит в раннере перед step()
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
