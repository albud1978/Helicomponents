#!/usr/bin/env python3
"""
RTC Smoketest: компиляция минимальной RTC-функции через NVRTC/Jitify.
Печатает OK/ошибку; для подробного лога запускайте с JITIFY_PRINT_LOG=1.
"""

import os

try:
    import pyflamegpu
except Exception as e:
    print(f"pyflamegpu import failed: {e}")
    raise


def main():
    model = pyflamegpu.ModelDescription("RTC_Smoke")
    agent = model.newAgent("a")
    agent.newVariableUInt("x", 0)

    rtc_src = r"""
    FLAMEGPU_AGENT_FUNCTION(noop, flamegpu::MessageNone, flamegpu::MessageNone) {
        // trivial
        return flamegpu::ALIVE;
    }
    """
    agent.newRTCFunction("noop", rtc_src)

    # Простой слой
    layer = model.newLayer()
    layer.addAgentFunction(agent.getFunction("noop"))

    sim = pyflamegpu.CUDASimulation(model)
    av = pyflamegpu.AgentVector(agent, 1)
    av[0].setVariableUInt("x", 1)
    sim.setPopulationData(av)
    sim.step()
    print("RTC smoketest: OK")


if __name__ == "__main__":
    main()


