#!/usr/bin/env python3
import os
import sys

# Включаем probe-режим для изолированной сборки RTC
os.environ.setdefault('FLAMEGPU_PROBE', 'repair')

# Авто‑fallback CUDA_PATH, чтобы не указывать руками при каждом запуске
if not os.environ.get('CUDA_PATH'):
    candidates = [
        "/usr/local/cuda",
        "/usr/local/cuda-12.4",
        "/usr/local/cuda-12.3",
        "/usr/local/cuda-12.2",
        "/usr/local/cuda-12.1",
        "/usr/local/cuda-12.0",
    ]
    for p in candidates:
        if os.path.isdir(p) and os.path.isdir(os.path.join(p, 'include')):
            os.environ['CUDA_PATH'] = p
            break

# Обеспечиваем импорт модели без PYTHONPATH=code
this_dir = os.path.dirname(__file__)
repo_root = os.path.abspath(os.path.join(this_dir, '..', '..'))
code_dir = os.path.join(repo_root, 'code')
if code_dir not in sys.path:
    sys.path.insert(0, code_dir)

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
    # Стартуем в ремонте: ожидаем переход 4 -> 5 внутри rtc_repair
    av[0].setVariableUInt('status_id', 4)
    av[0].setVariableUInt('repair_days', 0)
    av[0].setVariableUInt('repair_time', 1)
    sim.setPopulationData(av)
    sim.step()
    pop = pyflamegpu.AgentVector(comp)
    sim.getPopulationData(pop)
    print('OK, status_id=', pop[0].getVariableUInt('status_id'))


if __name__ == '__main__':
    main()

