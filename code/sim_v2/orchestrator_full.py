#!/usr/bin/env python3
"""
Единый оркестратор полной симуляции: Планеры → Агрегаты

Последовательный запуск:
1. Симуляция планеров (orchestrator_v2.py)
2. Экспорт MP2 планеров в ClickHouse (sim_masterv2)
3. Симуляция агрегатов (orchestrator_units.py)
4. Экспорт MP2 агрегатов в ClickHouse (sim_units_v2)

Использование:
    python orchestrator_full.py --version-date 2025-07-04 --steps 3650

Дата: 05.01.2026
"""

import os
import sys
import time
import argparse
import subprocess
from datetime import datetime

# Пути
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))


def run_command(cmd: list, description: str) -> tuple:
    """Запускает команду и возвращает (success, duration)"""
    print(f"\n{'='*60}")
    print(f"🚀 {description}")
    print(f"{'='*60}")
    print(f"   Команда: {' '.join(cmd)}")
    
    t0 = time.time()
    
    try:
        result = subprocess.run(
            cmd,
            cwd=SCRIPT_DIR,
            capture_output=False,
            text=True
        )
        duration = time.time() - t0
        
        if result.returncode == 0:
            print(f"\n✅ {description} завершено за {duration:.2f}с")
            return True, duration
        else:
            print(f"\n❌ {description} завершено с ошибкой (код {result.returncode})")
            return False, duration
            
    except Exception as e:
        duration = time.time() - t0
        print(f"\n❌ Ошибка: {e}")
        return False, duration


def main():
    parser = argparse.ArgumentParser(description='Полная симуляция: Планеры + Агрегаты')
    parser.add_argument('--version-date', type=str, required=True,
                       help='Дата версии данных (YYYY-MM-DD)')
    parser.add_argument('--version-id', type=int, default=1,
                       help='ID версии данных')
    parser.add_argument('--steps', type=int, default=3650,
                       help='Количество шагов симуляции')
    parser.add_argument('--planers-only', action='store_true',
                       help='Только симуляция планеров')
    parser.add_argument('--units-only', action='store_true',
                       help='Только симуляция агрегатов')
    parser.add_argument('--export', action='store_true',
                       help='Экспортировать результаты в ClickHouse')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("🚁 ПОЛНАЯ СИМУЛЯЦИЯ: ПЛАНЕРЫ + АГРЕГАТЫ")
    print("=" * 70)
    print(f"   Дата версии: {args.version_date}")
    print(f"   Шагов: {args.steps}")
    print(f"   Экспорт: {'да' if args.export else 'нет'}")
    
    total_start = time.time()
    results = {}
    
    # === ШАГ 1: Симуляция планеров ===
    if not args.units_only:
        planers_cmd = [
            sys.executable, 'orchestrator_v2.py',
            '--version-date', args.version_date,
            '--steps', str(args.steps),
            '--enable-mp2',
            '--enable-mp2-postprocess'
        ]
        
        success, duration = run_command(planers_cmd, "СИМУЛЯЦИЯ ПЛАНЕРОВ")
        results['planers'] = {'success': success, 'duration': duration}
        
        if not success:
            print("\n❌ Симуляция планеров не удалась, прерываем")
            return 1
    
    # === ШАГ 2: Симуляция агрегатов ===
    if not args.planers_only:
        units_cmd = [
            sys.executable, 'units/orchestrator_units.py',
            '--version-date', args.version_date,
            '--version-id', str(args.version_id),
            '--steps', str(args.steps)
        ]
        
        if args.export:
            units_cmd.append('--export')
        
        success, duration = run_command(units_cmd, "СИМУЛЯЦИЯ АГРЕГАТОВ")
        results['units'] = {'success': success, 'duration': duration}
    
    # === ИТОГИ ===
    total_duration = time.time() - total_start
    
    print("\n" + "=" * 70)
    print("📊 ИТОГИ ПОЛНОЙ СИМУЛЯЦИИ")
    print("=" * 70)
    
    for stage, data in results.items():
        status = "✅" if data['success'] else "❌"
        print(f"   {status} {stage}: {data['duration']:.2f}с")
    
    print(f"   ─────────────────────────")
    print(f"   ВСЕГО: {total_duration:.2f}с")
    
    # Проверяем успешность
    all_success = all(r['success'] for r in results.values())
    
    if all_success:
        print("\n✅ Полная симуляция завершена успешно!")
    else:
        print("\n❌ Были ошибки в симуляции")
    
    return 0 if all_success else 1


if __name__ == "__main__":
    sys.exit(main())

