#!/usr/bin/env python3
"""
SimMaster v2 - Упрощенный оркестратор симуляции
Модульная архитектура с RTC функциями в отдельных файлах
Основано на унифицированном пайплайне из rtc_pipeline_architecture.md
Дата: 2025-09-12
"""

import os
import sys
import argparse
import time
from pathlib import Path

# Добавляем пути
sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent / 'utils'))

from sim import SimBuilder, EnvironmentSetup, RTC_PROFILES, apply_profile
from sim.runners import SmokeRunner, ProductionRunner
from config_loader import get_clickhouse_client

try:
    import pyflamegpu
except ImportError:
    pyflamegpu = None


def create_argument_parser() -> argparse.ArgumentParser:
    """Создает парсер аргументов командной строки"""
    
    parser = argparse.ArgumentParser(description='SimMaster v2 - Модульная симуляция')
    
    # Профили выполнения
    parser.add_argument('--profile', choices=list(RTC_PROFILES.keys()), 
                       default='minimal', help='Профиль RTC функций')
    
    # Smoke тесты
    parser.add_argument('--gpu-quota-smoke', action='store_true', 
                       help='GPU квоты: тест intent/approve/apply')
    parser.add_argument('--status4-smoke', action='store_true',
                       help='Тест статуса 4 (ремонт)')
    parser.add_argument('--status246-smoke', action='store_true',
                       help='Тест статусов 2,4,6')
    parser.add_argument('--status12456-smoke', action='store_true',
                       help='Полный тест всех статусов с квотами')
    
    # Параметры симуляции
    parser.add_argument('--days', type=int, default=7, 
                       help='Количество дней симуляции')
    parser.add_argument('--frames', type=int, default=None,
                       help='Количество кадров (по умолчанию из данных)')
    
    # Экспорт результатов
    parser.add_argument('--export-sim', choices=['on', 'off'], default='off',
                       help='Экспорт в ClickHouse')
    parser.add_argument('--export-table', default='sim_results',
                       help='Таблица для экспорта')
    parser.add_argument('--export-truncate', action='store_true',
                       help='Очистить таблицу перед экспортом')
    
    # Отладка
    parser.add_argument('--seatbelts', choices=['on', 'off'], default='on',
                       help='FLAME GPU seatbelts')
    parser.add_argument('--jit-log', action='store_true',
                       help='Подробный лог NVRTC')
    parser.add_argument('--pipeline-info', action='store_true',
                       help='Показать информацию о пайплайне')
    
    return parser


def setup_environment_flags(args):
    """Настройка environment переменных"""
    
    # CUDA путь
    if not os.environ.get('CUDA_PATH'):
        cuda_paths = [
            "/usr/local/cuda", "/usr/local/cuda-12.4", "/usr/local/cuda-12.3",
            "/usr/local/cuda-12.2", "/usr/local/cuda-12.1", "/usr/local/cuda-12.0"
        ]
        for path in cuda_paths:
            if os.path.isdir(path) and os.path.isdir(os.path.join(path, 'include')):
                os.environ['CUDA_PATH'] = path
                break
    
    # FLAME GPU настройки
    os.environ['FLAMEGPU_SEATBELTS'] = '1' if args.seatbelts == 'on' else '0'
    
    if args.jit_log:
        os.environ['PYTHONUNBUFFERED'] = '1'
        os.environ['HL_JIT_LOG'] = '1'
        os.environ['CUDA_LAUNCH_BLOCKING'] = '1'


def run_smoke_test(args, builder: SimBuilder, env_setup: EnvironmentSetup):
    """Выполняет smoke тесты"""
    
    print(f"🧪 Запуск smoke теста...")
    
    # Создаем runners
    smoke_runner = SmokeRunner(builder, env_setup)
    production_runner = ProductionRunner(builder, env_setup)
    
    # Выбор типа теста
    if args.gpu_quota_smoke:
        result = smoke_runner.run_quota_smoke(args.frames, args.days)
    elif args.status4_smoke:
        result = smoke_runner.run_status_smoke([4], args.frames, args.days)
    elif args.status246_smoke:
        result = smoke_runner.run_status_smoke([2, 4, 6], args.frames, args.days)
    elif args.status12456_smoke:
        result = smoke_runner.run_status_smoke([1, 2, 4, 5, 6], args.frames, args.days)
    else:
        # Минимальный тест
        result = smoke_runner.run_status_smoke([2, 4, 6], args.frames, args.days)
    
    return result




def main():
    """Главная функция"""
    
    if pyflamegpu is None:
        print("❌ pyflamegpu не установлен")
        return 1
    
    # Парсинг аргументов
    parser = create_argument_parser()
    args = parser.parse_args()
    
    # Настройка окружения
    setup_environment_flags(args)
    
    # Создание компонентов
    builder = SimBuilder()
    env_setup = EnvironmentSetup()
    
    # Информация о пайплайне
    if args.pipeline_info:
        print(builder.get_pipeline_info(args.profile))
        return 0
    
    # Выполнение симуляции
    try:
        result = run_smoke_test(args, builder, env_setup)
        print("✅ Симуляция завершена успешно")
        print(f"📊 Результат: {result}")
        return 0
        
    except Exception as e:
        print(f"❌ Ошибка симуляции: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
