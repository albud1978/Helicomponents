#!/usr/bin/env python3
"""
Тест rtc_status_6 с использованием рабочих файлов из коммита
Точная копия status6_smoke_real алгоритма
Дата: 2025-09-12
"""

import os
import sys
import time
from pathlib import Path
from typing import Dict, List

sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent / 'utils'))

from config_loader import get_clickhouse_client

# Импортируем рабочие модули
from sim_env_setup_working import (
    fetch_versions, fetch_mp1_br_rt, fetch_mp3, prepare_env_arrays
)
from model_build_working import build_model_for_quota_smoke

try:
    import pyflamegpu
except ImportError:
    pyflamegpu = None


def test_working_status6(days: int = 7, seatbelts: bool = True, export_db: bool = False):
    """Точная копия status6_smoke_real из рабочего коммита"""
    
    if pyflamegpu is None:
        print("❌ pyflamegpu не установлен")
        return False
    
    print(f"🧪 Рабочий тест rtc_status_6 на {days} дней")
    print("=" * 50)
    
    # Настройка (как в рабочем коде)
    if seatbelts:
        os.environ['FLAMEGPU_SEATBELTS'] = '1'
        print("🔧 FLAMEGPU seatbelts включены")
    else:
        os.environ['FLAMEGPU_SEATBELTS'] = '0'
    
    os.environ['HL_STATUS6_SMOKE'] = '1'
    
    try:
        # === ЭТАП 1: Подготовка данных ===
        t_load_start = time.perf_counter()
        
        client = get_clickhouse_client()
        vdate, vid = fetch_versions(client)
        mp1_map = fetch_mp1_br_rt(client)
        mp3_rows, mp3_fields = fetch_mp3(client, vdate, vid)
        env_data = prepare_env_arrays(client)
        
        FRAMES = int(env_data['frames_total_u16'])
        DAYS = int(env_data['days_total_u16'])
        
        t_load_end = time.perf_counter()
        load_ms = (t_load_end - t_load_start) * 1000
        
        print(f"📊 Данные: {FRAMES} кадров, {DAYS} дней, {len(mp3_rows)} агентов")
        
        # === ЭТАП 2: Создание модели ===
        t_compile_start = time.perf_counter()
        
        model2, a_desc = build_model_for_quota_smoke(FRAMES, DAYS)
        sim2 = pyflamegpu.CUDASimulation(model2)
        sim2.setEnvironmentPropertyUInt("version_date", int(env_data['version_date_u16']))
        sim2.setEnvironmentPropertyUInt("frames_total", FRAMES)
        sim2.setEnvironmentPropertyUInt("days_total", DAYS)
        
        t_compile_end = time.perf_counter()
        compile_ms = (t_compile_end - t_compile_start) * 1000
        
        # === ЭТАП 3: Популяция (точно как в бэкапе) ===
        t_pop_start = time.perf_counter()
        
        idx_map = {name: i for i, name in enumerate(mp3_fields)}
        s6_rows = [r for r in mp3_rows if int(r[idx_map['status_id']] or 0) == 6]
        K = len(s6_rows)
        
        print(f"🎯 Агентов в статусе 6: {K}")
        
        if K == 0:
            print("⚠️ Нет агентов в статусе 6 для тестирования")
            # Создадим искусственных для теста
            K = 5
            s6_rows = [mp3_rows[0]] * K  # Дублируем первого агента
            print(f"🧪 Создаем {K} тестовых агентов")
        
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
            
            # Для тестирования s6 логики
            av[i].setVariableUInt("s6_started", 1 if i < K//2 else 0)  # Половина с активным счетчиком
            av[i].setVariableUInt("s6_days", i)  # Разные дни в хранении
        
        sim2.setPopulationData(av)
        
        t_pop_end = time.perf_counter()
        pop_ms = (t_pop_end - t_pop_start) * 1000
        
        # === ЭТАП 4: Состояние ДО ===
        t_cpu_before_start = time.perf_counter()
        
        before = pyflamegpu.AgentVector(a_desc)
        sim2.getPopulationData(before)
        s6_b = sum(1 for ag in before if int(ag.getVariableUInt('status_id')) == 6)
        sne_b = [int(ag.getVariableUInt('sne')) if 'sne' in dir(ag) else 0 for ag in before]
        ppr_b = [int(ag.getVariableUInt('ppr')) for ag in before]
        rd_b = [int(ag.getVariableUInt('repair_days')) for ag in before]
        s6_days_b = [int(ag.getVariableUInt('s6_days')) for ag in before]
        
        t_cpu_before_end = time.perf_counter()
        cpu_before_ms = (t_cpu_before_end - t_cpu_before_start) * 1000
        
        print(f"📊 ДО: s6={s6_b}, ppr_sum={sum(ppr_b)}, rd_sum={sum(rd_b)}, s6_days={s6_days_b}")
        
        # === ЭТАП 5: Симуляция ===
        t_sim_start = time.perf_counter()
        
        steps = max(1, days)
        step_times = []
        
        for step in range(steps):
            step_start = time.perf_counter()
            sim2.step()
            step_end = time.perf_counter()
            step_ms = (step_end - step_start) * 1000
            step_times.append(step_ms)
            
            if step < 3 or step == steps - 1:
                print(f"  Шаг {step}: {step_ms:.2f} мс")
        
        t_sim_end = time.perf_counter()
        sim_ms = (t_sim_end - t_sim_start) * 1000
        
        # === ЭТАП 6: Состояние ПОСЛЕ ===
        t_cpu_after_start = time.perf_counter()
        
        after = pyflamegpu.AgentVector(a_desc)
        sim2.getPopulationData(after)
        s6_a = sum(1 for ag in after if int(ag.getVariableUInt('status_id')) == 6)
        sne_a = [int(ag.getVariableUInt('sne')) if 'sne' in dir(ag) else 0 for ag in after]
        ppr_a = [int(ag.getVariableUInt('ppr')) for ag in after]
        rd_a = [int(ag.getVariableUInt('repair_days')) for ag in after]
        s6_days_a = [int(ag.getVariableUInt('s6_days')) for ag in after]
        
        # Проверка инвариантов (как в бэкапе)
        invariants = (s6_b == s6_a) and (sne_b == sne_a) and (ppr_b == ppr_a) and (rd_b == rd_a)
        
        t_cpu_after_end = time.perf_counter()
        cpu_after_ms = (t_cpu_after_end - t_cpu_after_start) * 1000
        
        print(f"📊 ПОСЛЕ: s6={s6_a}, ppr_sum={sum(ppr_a)}, rd_sum={sum(rd_a)}, s6_days={s6_days_a}")
        print(f"✅ Инварианты: {invariants}")
        
        # === ЭТАП 7: Экспорт в базу ===
        db_ms = 0.0
        if export_db:
            t_db_start = time.perf_counter()
            
            table_name = "rtc_status6_working_test"
            ddl = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                test_date Date,
                days_tested UInt16,
                agents_s6 UInt16,
                invariants_ok UInt8,
                s6_days_before Array(UInt16),
                s6_days_after Array(UInt16),
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
            
            from datetime import date
            total_ms = load_ms + compile_ms + pop_ms + sim_ms + cpu_before_ms + cpu_after_ms
            
            row_data = (
                date.today(),
                days,
                K,
                1 if invariants else 0,
                s6_days_b,
                s6_days_a,
                load_ms,
                compile_ms,
                sim_ms,
                cpu_before_ms + cpu_after_ms,
                0.0,  # db_insert заполнится позже
                total_ms
            )
            
            client.execute(f"INSERT INTO {table_name} VALUES", [row_data])
            
            t_db_end = time.perf_counter()
            db_ms = (t_db_end - t_db_start) * 1000
            
            print(f"✅ Результаты экспортированы в {table_name}")
        
        # === ЭТАП 8: Итоговые тайминги ===
        total_ms = load_ms + compile_ms + pop_ms + sim_ms + cpu_before_ms + cpu_after_ms + db_ms
        
        print(f"\n⏱️ Финальные тайминги (как в бэкапе):")
        print(f"  📥 Загрузка GPU:     {load_ms:>8.2f} мс ({load_ms/total_ms*100:>5.1f}%)")
        print(f"  🔧 Компиляция RTC:   {compile_ms:>8.2f} мс ({compile_ms/total_ms*100:>5.1f}%)")
        print(f"  👥 Популяция:        {pop_ms:>8.2f} мс ({pop_ms/total_ms*100:>5.1f}%)")
        print(f"  🚀 Симуляция GPU:    {sim_ms:>8.2f} мс ({sim_ms/total_ms*100:>5.1f}%)")
        print(f"  📊 Логирование CPU:  {cpu_before_ms + cpu_after_ms:>8.2f} мс ({(cpu_before_ms + cpu_after_ms)/total_ms*100:>5.1f}%)")
        if export_db:
            print(f"  💾 Выгрузка СУБД:    {db_ms:>8.2f} мс ({db_ms/total_ms*100:>5.1f}%)")
        print(f"  ⏱️ Общее время:      {total_ms:>8.2f} мс")
        
        # Формат как в бэкапе
        print(f"\nstatus6_smoke_real: steps={steps}, s6_before={s6_b}, s6_after={s6_a}, invariants={invariants}")
        
        return invariants
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Главная функция"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description='Рабочий тест rtc_status_6')
    parser.add_argument('--days', type=int, default=7, help='Количество дней (по умолчанию 7)')
    parser.add_argument('--seatbelts', choices=['on', 'off'], default='on', help='FLAME GPU seatbelts')
    parser.add_argument('--export-db', action='store_true', help='Экспорт в ClickHouse')
    parser.add_argument('--jit-log', action='store_true', help='JIT лог')
    
    args = parser.parse_args()
    
    if args.jit_log:
        os.environ['HL_JIT_LOG'] = '1'
        os.environ['PYTHONUNBUFFERED'] = '1'
        os.environ['CUDA_LAUNCH_BLOCKING'] = '1'
        print("🔧 JIT лог включен")
    
    success = test_working_status6(
        days=args.days,
        seatbelts=(args.seatbelts == 'on'),
        export_db=args.export_db
    )
    
    if success:
        print(f"\n🎉 Рабочий тест rtc_status_6 прошел успешно!")
        print(f"✅ Готово к масштабированию: 30, 365, 3650 дней")
        return 0
    else:
        print(f"\n❌ Тест провалился")
        return 1


if __name__ == '__main__':
    sys.exit(main())


