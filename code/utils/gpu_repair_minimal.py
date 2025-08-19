#!/usr/bin/env python3
"""
Минимальная GPU-модель с одной RTC-функцией rtc_repair для проверки компиляции NVRTC.
"""

try:
    import pyflamegpu
except Exception as e:
    print(f"pyflamegpu import failed: {e}")
    raise


def main():
    model = pyflamegpu.ModelDescription("Repair_Minimal")
    agent = model.newAgent("component")
    agent.newVariableUInt("status_id", 0)
    agent.newVariableUInt("repair_days", 0)
    agent.newVariableUInt("repair_time", 0)
    agent.newVariableUInt("status_change", 0)

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
    agent.newRTCFunction("rtc_repair", rtc_repair_src)

    layer = model.newLayer()
    layer.addAgentFunction(agent.getFunction("rtc_repair"))

    sim = pyflamegpu.CUDASimulation(model)
    av = pyflamegpu.AgentVector(agent, 1)
    av[0].setVariableUInt("status_id", 4)
    av[0].setVariableUInt("repair_days", 0)
    av[0].setVariableUInt("repair_time", 1)
    av[0].setVariableUInt("status_change", 0)
    sim.setPopulationData(av)
    sim.step()
    pop = pyflamegpu.AgentVector(agent)
    sim.getPopulationData(pop)
    chg = pop[0].getVariableUInt("status_change")
    print(f"rtc_repair minimal OK, status_change={chg}")


if __name__ == "__main__":
    main()


