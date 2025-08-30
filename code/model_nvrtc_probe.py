#!/usr/bin/env python3
"""
NVRTC probe for HeliSimModel (intent → approve → apply):
- Устанавливает CUDA_PATH (fallback)
- Строит HeliSimModel с реальными DAYS/FRAMES (аргументы или дефолт)
- Минимально применяет Env (version_date, frames_total, days_total, mp4_ops*)
- Создаёт FRAMES агентов (idx=0..FRAMES-1, group_by=1)
- Выполняет один шаг и печатает OK/ошибку

Запуск:
  python3 code/model_nvrtc_probe.py --days 4000 --frames 279
"""
import os
import sys
import argparse

def ensure_cuda_path() -> None:
    if os.environ.get('CUDA_PATH') and os.path.isdir(os.environ['CUDA_PATH']):
        return
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
            return

def main() -> None:
    ensure_cuda_path()
    sys.path.append(os.path.join(os.path.dirname(__file__)))
    try:
        import pyflamegpu as fg
    except Exception as e:
        print('ERR_NO_PYFLAMEGPU:', e)
        sys.exit(1)
    from model_build import HeliSimModel

    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=int, default=2)
    ap.add_argument('--frames', type=int, default=3)
    ap.add_argument('--emit-code', action='store_true', help='Сохранить RTC-источники HeliSimModel в файл')
    ap.add_argument('--emit-path', type=str, default='rtc_dump.cu', help='Путь для сохранения RTC (если включен emit-code)')
    args = ap.parse_args()

    DAYS = max(1, int(args.days))
    FRAMES = max(1, int(args.frames))
    print(f'NVRTC probe: DAYS={DAYS}, FRAMES={FRAMES}, CUDA_PATH={os.environ.get("CUDA_PATH")}')

    m = HeliSimModel()
    m.build_model(num_agents=FRAMES, env_sizes={
        'days_total': DAYS,
        'frames_total': FRAMES,
        'mp1_len': 1,
        'mp3_count': FRAMES,
    })
    sim = m.build_simulation()

    # опциональная выгрузка RTC-источников (если доступны)
    if getattr(m, 'rtc_sources', None) and args.emit_code:
        try:
            with open(args.emit_path, 'w', encoding='utf-8') as f:
                for k, v in m.rtc_sources.items():
                    f.write(f"\n// ===== {k} =====\n")
                    f.write(v)
            print(f'RTC источники сохранены в {args.emit_path}')
        except Exception as e:
            print('Не удалось сохранить RTC источники:', e)

    # Минимальный набор Env свойств
    sim.setEnvironmentPropertyUInt('version_date', 0)
    sim.setEnvironmentPropertyUInt('frames_total', FRAMES)
    sim.setEnvironmentPropertyUInt('days_total', DAYS)
    sim.setEnvironmentPropertyArrayUInt32('mp4_ops_counter_mi8', [0] * DAYS)
    sim.setEnvironmentPropertyArrayUInt32('mp4_ops_counter_mi17', [0] * DAYS)

    # Популяция: FRAMES агентов, все gb=1
    agt = m.agent
    av = fg.AgentVector(agt, FRAMES)
    for i in range(FRAMES):
        av[i].setVariableUInt('idx', i)
        av[i].setVariableUInt('group_by', 1)
        av[i].setVariableUInt('ops_ticket', 0)
    sim.setPopulationData(av)

    # Один шаг; в случае падения NVRTC/Jitify печатает std::cout
    sim.step()
    print('NVRTC probe: OK step')

if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
NVRTC probe for HeliSimModel: пытается собрать модель и симуляцию без Env,
чтобы напечатать ошибки компиляции RTC (если есть).
"""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__)))

try:
    import pyflamegpu as fg
except Exception as e:
    print("ERR_NO_PYFLAMEGPU:", e)
    raise SystemExit(1)

from model_build import HeliSimModel


def ensure_cuda_path():
    if os.environ.get('CUDA_PATH'):
        return
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


def main():
    ensure_cuda_path()
    try:
        m = HeliSimModel()
        # Минимальные размеры Env (не используются, но требуются для шаблонов размеров)
        m.build_model(num_agents=1, env_sizes={'days_total': 3, 'frames_total': 2, 'mp1_len': 1, 'mp3_count': 1})
        sim = m.build_simulation()
        print("NVRTC OK: HeliSimModel built")
    except Exception as e:
        print("NVRTC ERROR while building HeliSimModel:\n", e)
        raise


if __name__ == '__main__':
    main()


