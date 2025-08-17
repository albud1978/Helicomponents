#!/usr/bin/env python3
"""
Transform Master — оркестратор загрузки MacroProperty (MP1, MP3, MP4, MP5) и Property

Назначение: последовательно вызывать микросервисы по цепочке loader → exporter → validator
для каждого MP. Для Property — только loader. Без изменения алгоритмов.

Дата: 2025-08-15
"""

import sys
import argparse
import subprocess


def run(cmd: list[str]) -> int:
    print(f"▶️  Запуск: {' '.join(cmd)}")
    proc = subprocess.run(cmd)
    return proc.returncode


def main():
    print("🔥 Transform Master — оркестратор загрузки MP (loader → exporter → validator)")

    parser = argparse.ArgumentParser(description='Оркестратор загрузки MacroProperty')
    parser.add_argument('--version-date', type=str, default=None)
    parser.add_argument('--version-id', type=str, default=None)
    args = parser.parse_args()

    v_args = []
    if args.version_date:
        v_args += ['--version-date', args.version_date]
    if args.version_id:
        v_args += ['--version-id', args.version_id]

    # Цепочки по MP: loader → exporter → validator
    chains = [
        # MP1
        [
            [sys.executable, 'code/flame_macroproperty1_loader.py', *v_args],
            [sys.executable, 'code/flame_macroproperty1_exporter.py', *v_args],
            [sys.executable, 'code/flame_macroproperty1_validator.py', *v_args],
        ],
        # MP3
        [
            [sys.executable, 'code/flame_macroproperty3_loader.py', *v_args],
            [sys.executable, 'code/flame_macroproperty3_exporter.py', *v_args],
            [sys.executable, 'code/flame_macroproperty3_validator.py', *v_args],
        ],
        # MP4
        [
            [sys.executable, 'code/flame_macroproperty4_loader.py', *v_args],
            [sys.executable, 'code/flame_macroproperty4_exporter.py', *v_args],
            [sys.executable, 'code/flame_macroproperty4_validator.py', *v_args],
        ],
        # MP5
        [
            [sys.executable, 'code/flame_macroproperty5_loader.py', *v_args],
            [sys.executable, 'code/flame_macroproperty5_exporter.py', *v_args],
            [sys.executable, 'code/flame_macroproperty5_validator.py', *v_args],
        ],
    ]

    # Property — только loader (версионные скаляры)
    property_step = [sys.executable, 'code/flame_property_loader.py', *v_args]

    all_ok = True
    step_index = 0
    for chain in chains:
        step_index += 1
        print(f"\n===== MP Chain #{step_index} =====")
        for cmd in chain:
            rc = run(cmd)
            if rc != 0:
                print(f"❌ Ошибка этапа: {' '.join(cmd)} (rc={rc})")
                all_ok = False
                # Продолжаем, чтобы собрать все ошибки

    print("\n===== Property =====")
    rc = run(property_step)
    if rc != 0:
        print(f"❌ Ошибка этапа: {' '.join(property_step)} (rc={rc})")
        all_ok = False

    print("\n" + ("✅ Завершено" if all_ok else "⚠️ Завершено с ошибками"))


if __name__ == '__main__':
    main()