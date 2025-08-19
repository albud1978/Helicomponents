#!/usr/bin/env python3
import os
os.environ.setdefault('FLAMEGPU_PROBE', 'repair')
os.environ.setdefault('CUDA_PATH', '/usr/local/cuda')

from flame_gpu_helicopter_model import HelicopterFlameModel

try:
    import pyflamegpu
except Exception as e:
    print('pyflamegpu import failed:', e)
    raise


def main():
    m = HelicopterFlameModel()
    m.build_model(num_agents=1)
    sim = m.build_simulation()
    comp = m.model.getAgent('component')
    av = pyflamegpu.AgentVector(comp, 1)
    av[0].setVariableUInt('status_id', 4)
    av[0].setVariableUInt('repair_days', 0)
    av[0].setVariableUInt('repair_time', 1)
    av[0].setVariableUInt('status_change', 0)
    sim.setPopulationData(av)
    sim.step()
    pop = pyflamegpu.AgentVector(comp)
    sim.getPopulationData(pop)
    print('OK, status_change=', pop[0].getVariableUInt('status_change'))


if __name__ == '__main__':
    main()


