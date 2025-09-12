#!/usr/bin/env python3
"""
Тест rtc_status_6 по историческому алгоритму из бэкапов
Точная копия рабочего кода из sim_master_mp2_export_20250903_095924.py
Дата: 2025-09-12
"""

import os
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent / 'utils'))

from sim.env_setup import EnvironmentSetup
from sim.timing_system import TimingCollector, create_timing_context
from config_loader import get_clickhouse_client

try:
    import pyflamegpu
except ImportError:
    pyflamegpu = None

# Импортируем рабочую фабрику из бэкапа
sys.path.append(str(Path(__file__).parent))
from model_build_mp2_export_20250903_095924 import build_model_for_quota_smoke


def test_status6_historical(days: int = 7, seatbelts: bool = True, export_db: bool = False):
    """Тест rtc_status_6 по историческому алгоритму"""
    
    if pyflamegpu is None:
        print("❌ pyflamegpu не установлен")
        return False
    
    print(f"🧪 Исторический тест rtc_status_6 на {days} дней")
    print("=" * 50)
    
    timing = TimingCollector()
    
    try:
        # Настройка отладки
        if seatbelts:
            os.environ['FLAMEGPU_SEATBELTS'] = '1'
            print("🔧 FLAMEGPU seatbelts включены")
        else:
            os.environ['FLAMEGPU_SEATBELTS'] = '0'
        
        os.environ['HL_STATUS6_SMOKE'] = '1'
        
        # === ЭТАП 1: Подготовка данных ===
        with create_timing_context(timing, "load_gpu"):
            env_setup = EnvironmentSetup()
            env_data = env_setup.prepare_environment_for_period("custom", days)
            
            FRAMES = int(env_data['frames_total'])
            DAYS = int(env_data['days_total'])
            
            mp3_rows = env_data['mp3_rows']
            mp3_fields = env_data['mp3_fields']
        
        print(f"📊 Данные: {FRAMES} кадров, {DAYS} дней, {len(mp3_rows)} агентов")
        
        # === ЭТАП 2: Создание модели (по историческому алгоритму) ===
        with create_timing_context(timing, "compile_rtc"):
            model2, a_desc = build_model_for_quota_smoke(FRAMES, DAYS)
            sim2 = pyflamegpu.CUDASimulation(model2)
            
            # Настройка Environment (как в бэкапе)
            sim2.setEnvironmentPropertyUInt("version_date", int(env_data['version_date_u16']))
            sim2.setEnvironmentPropertyUInt("frames_total", FRAMES)
            sim2.setEnvironmentPropertyUInt("days_total", DAYS)
        
        # === ЭТАП 3: Создание популяции (только агенты в статусе 6) ===
        with create_timing_context(timing, "population"):
            idx_map = {name: i for i, name in enumerate(mp3_fields)}
            s6_rows = [r for r in mp3_rows if int(r[idx_map['status_id']] or 0) == 6]
            K = len(s6_rows)
            
            print(f"🎯 Агентов в статусе 6: {K}")
            
            if K == 0:
                print("⚠️ Нет агентов в статусе 6 для тестирования")
                return True
            
            av = pyflamegpu.AgentVector(a_desc, K)
            for i, r in enumerate(s6_rows):
                av[i].setVariableUInt("idx", int(i % max(1, FRAMES)))
                av[i].setVariableUInt("group_by", 1)
                av[i].setVariableUInt("status_id", 6)
                av[i].setVariableUInt("repair_days", int(r[idx_map['repair_days']] or 0))
                av[i].setVariableUInt("repair_time", 0)
                av[i].setVariableUInt("partout_time", 0)
                av[i].setVariableUInt("assembly_time", 0)
                av[i].setVariableUInt("partout_trigger", 0)
                av[i].setVariableUInt("assembly_trigger", 0)
                av[i].setVariableUInt("ppr", int(r[idx_map.get('ppr', -1)] or 0))
            
            sim2.setPopulationData(av)
        
        # === ЭТАП 4: Тест инвариантов ДО симуляции ===
        with create_timing_context(timing, "cpu_log"):
            before = pyflamegpu.AgentVector(a_desc)
            sim2.getPopulationData(before)
            s6_b = sum(1 for ag in before if int(ag.getVariableUInt('status_id')) == 6)
            sne_b = [int(ag.getVariableUInt('sne')) if 'sne' in dir(ag) else 0 for ag in before]
            ppr_b = [int(ag.getVariableUInt('ppr')) for ag in before]
            rd_b = [int(ag.getVariableUInt('repair_days')) for ag in before]
        
        print(f"📊 ДО симуляции: s6={s6_b}, ppr_sum={sum(ppr_b)}, rd_sum={sum(rd_b)}")
        
        # === ЭТАП 5: Выполнение симуляции ===
        with create_timing_context(timing, "sim_gpu"):
            for step in range(days):
                step_start = time.perf_counter()
                sim2.step()
                step_time = (time.perf_counter() - step_start) * 1000
                timing.add_step_time(step_time)
                
                if step < 3 or step == days - 1:
                    print(f"  Шаг {step}: {step_time:.2f} мс")
        
        # === ЭТАП 6: Проверка инвариантов ПОСЛЕ ===
        with create_timing_context(timing, "cpu_log"):
            after = pyflamegpu.AgentVector(a_desc)
            sim2.getPopulationData(after)
            s6_a = sum(1 for ag in after if int(ag.getVariableUInt('status_id')) == 6)
            sne_a = [int(ag.getVariableUInt('sne')) if 'sne' in dir(ag) else 0 for ag in after]
            ppr_a = [int(ag.getVariableUInt('ppr')) for ag in after]
            rd_a = [int(ag.getVariableUInt('repair_days')) for ag in after]
            
            # Проверка инвариантов (как в бэкапе)
            invariants = (s6_b == s6_a) and (sne_b == sne_a) and (ppr_b == ppr_a) and (rd_b == rd_a)
        
        print(f"📊 ПОСЛЕ симуляции: s6={s6_a}, ppr_sum={sum(ppr_a)}, rd_sum={sum(rd_a)}")
        print(f"✅ Инварианты соблюдены: {invariants}")
        
        # === ЭТАП 7: Экспорт в базу (если запрошен) ===
        if export_db:
            with create_timing_context(timing, "db_insert"):
                # Простой экспорт результатов
                client = get_clickhouse_client()
                
                # Создание таблицы результатов
                table_name = "rtc_status6_test_results"
                ddl = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    test_date Date,
                    days_tested UInt16,
                    agents_s6 UInt16,
                    invariants_ok UInt8,
                    load_gpu_ms Float32,
                    compile_rtc_ms Float32,
                    sim_gpu_ms Float32,
                    cpu_log_ms Float32,
                    db_insert_ms Float32,
                    total_ms Float32
                )
                ENGINE = MergeTree()
                ORDER BY (test_date, days_tested)
                """
                
                client.execute(ddl)
                
                # Вставка результатов
                from datetime import date
                metrics = timing.get_metrics()
                
                row_data = (
                    date.today(),
                    days,
                    K,
                    1 if invariants else 0,
                    metrics.load_gpu_ms,
                    metrics.compile_rtc_ms,
                    metrics.sim_gpu_ms,
                    metrics.cpu_log_ms,
                    metrics.db_insert_ms,
                    metrics.total_ms()
                )
                
                client.execute(f"INSERT INTO {table_name} VALUES", [row_data])
                print(f"✅ Результаты экспортированы в {table_name}")
        
        # === ЭТАП 8: Финальные метрики ===
        timing.set_metadata(days, K, 1)
        timing.get_metrics().print_summary()
        
        return invariants
        
    except Exception as e:
        print(f"❌ Ошибка тестирования: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Главная функция"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description='Исторический тест rtc_status_6')
    parser.add_argument('--days', type=int, default=7, help='Количество дней')
    parser.add_argument('--seatbelts', choices=['on', 'off'], default='on', help='FLAME GPU seatbelts')
    parser.add_argument('--export-db', action='store_true', help='Экспорт в ClickHouse')
    parser.add_argument('--jit-log', action='store_true', help='JIT лог')
    
    args = parser.parse_args()
    
    if args.jit_log:
        os.environ['HL_JIT_LOG'] = '1'
        os.environ['PYTHONUNBUFFERED'] = '1'
        os.environ['CUDA_LAUNCH_BLOCKING'] = '1'
    
    success = test_status6_historical(
        days=args.days,
        seatbelts=(args.seatbelts == 'on'),
        export_db=args.export_db
    )
    
    if success:
        print(f"\n🎉 Исторический тест rtc_status_6 прошел успешно!")
        return 0
    else:
        print(f"\n❌ Тест провалился")
        return 1


if __name__ == '__main__':
    sys.exit(main())


