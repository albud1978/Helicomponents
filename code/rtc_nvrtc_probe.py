#!/usr/bin/env python3
"""
NVRTC probe: минимальная проверка компиляции RTC-функции в текущем окружении pyflamegpu.
"""

import os
try:
    import pyflamegpu as fg
except Exception as e:
    print("ERR_NO_PYFLAMEGPU", e)
    raise SystemExit(1)

def main():
    model = fg.ModelDescription("NVRTCProbe")
    agent = model.newAgent("a")
    agent.newVariableUInt("idx", 0)
    rtc_src = r"""
    FLAMEGPU_AGENT_FUNCTION(rtc_noop, flamegpu::MessageNone, flamegpu::MessageNone) {
        return flamegpu::ALIVE;
    }
    """
    try:
        agent.newRTCFunction("rtc_noop", rtc_src)
        lyr = model.newLayer(); lyr.addAgentFunction(agent.getFunction("rtc_noop"))
    except Exception as e:
        print("NVRTC REGISTER ERROR:\n", e)
        print("--- SOURCE ---\n", rtc_src)
        raise

    # Ensure CUDA_PATH
    if not os.environ.get('CUDA_PATH'):
        for p in [
            '/usr/local/cuda',
            '/usr/local/cuda-12.4',
            '/usr/local/cuda-12.3',
            '/usr/local/cuda-12.2',
            '/usr/local/cuda-12.1',
            '/usr/local/cuda-12.0',
        ]:
            if os.path.isdir(p) and os.path.isdir(os.path.join(p, 'include')):
                os.environ['CUDA_PATH'] = p
                break
    sim = fg.CUDASimulation(model)
    try:
        sim.step()
        print("NVRTC OK: rtc_noop compiled and executed")
    except Exception as e:
        print("NVRTC STEP ERROR:\n", e)
        raise

if __name__ == "__main__":
    main()


