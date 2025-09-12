#!/usr/bin/env python3
"""
Точная копия status6_smoke_real из рабочего бэкапа
Тестирует rtc_status_6 на реальных данных с таймингами
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
from sim_env_setup_backup_20250911_172013 import (
    fetch_versions, fetch_mp3, prepare_env_arrays
)
from model_build_backup_20250911_172530 import build_model_for_quota_smoke

try:
    import pyflamegpu
except ImportError:
    pyflamegpu = None


def test_status6_exact(days: int = 7, seatbelts: bool = True):
    """Точная копия status6_smoke_real из бэкапа"""
    
    if pyflamegpu is None:
        print("❌ pyflamegpu не установлен")
        return False
    
    print(f"🧪 Точный тест rtc_status_6 (как в бэкапе) на {days} дней")
    print("=" * 55)
    
    # Настройка окружения (как в бэкапе)
    if seatbelts:
        os.environ['FLAMEGPU_SEATBELTS'] = '1'
        print("🔧 FLAMEGPU seatbelts включены")
    else:
        os.environ['FLAMEGPU_SEATBELTS'] = '0'
    
    os.environ['HL_STATUS6_SMOKE'] = '1'
    
    try:
        # Таймеры
        t_load_start = time.perf_counter()
        
        # === Подготовка данных (как в бэкапе) ===
        client = get_clickhouse_client()
        vdate, vid = fetch_versions(client)
        mp3_rows, mp3_fields = fetch_mp3(client, vdate, vid)
        env_data = prepare_env_arrays(client)
        
        FRAMES = int(env_data['frames_total_u16'])
        DAYS = int(env_data['days_total_u16'])
        
        # Ограничиваем дни для теста
        if days < DAYS:
            DAYS = days
            print(f"📐 Ограничиваем до {DAYS} дней для теста")
        
        t_load_end = time.perf_counter()
        load_ms = (t_load_end - t_load_start) * 1000
        
        # === Создание модели (как в бэкапе) ===
        t_compile_start = time.perf_counter()
        
        model2, a_desc = build_model_for_quota_smoke(FRAMES, DAYS)
        sim2 = pyflamegpu.CUDASimulation(model2)
        
        sim2.setEnvironmentPropertyUInt("version_date", int(env_data['version_date_u16']))
        sim2.setEnvironmentPropertyUInt("frames_total", FRAMES)
        sim2.setEnvironmentPropertyUInt("days_total", DAYS)
        
        t_compile_end = time.perf_counter()
        compile_ms = (t_compile_end - t_compile_start) * 1000
        
        # === Создание популяции (точно как в бэкапе) ===
        t_pop_start = time.perf_counter()
        
        idx_map = {name: i for i, name in enumerate(mp3_fields)}
        s6_rows = [r for r in mp3_rows if int(r[idx_map['status_id']] or 0) == 6]
        K = len(s6_rows)
        
        print(f"📊 Данные: {FRAMES} кадров, {DAYS} дней, {K} агентов в статусе 6")
        
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
        
        t_pop_end = time.perf_counter()
        pop_ms = (t_pop_end - t_pop_start) * 1000
        
        # === Состояние ДО симуляции ===
        t_cpu_start = time.perf_counter()
        
        before = pyflamegpu.AgentVector(a_desc)
        sim2.getPopulationData(before)
        s6_b = sum(1 for ag in before if int(ag.getVariableUInt('status_id')) == 6)
        sne_b = [int(ag.getVariableUInt('sne')) if 'sne' in dir(ag) else 0 for ag in before]
        ppr_b = [int(ag.getVariableUInt('ppr')) for ag in before]
        rd_b = [int(ag.getVariableUInt('repair_days')) for ag in before]
        
        t_cpu_end = time.perf_counter()
        cpu_before_ms = (t_cpu_end - t_cpu_start) * 1000
        
        print(f"📊 ДО: s6={s6_b}, ppr_sum={sum(ppr_b)}, rd_sum={sum(rd_b)}")
        
        # === Выполнение симуляции ===
        t_sim_start = time.perf_counter()
        
        step_times = []
        for step in range(DAYS):
            step_start = time.perf_counter()
            sim2.step()
            step_end = time.perf_counter()
            step_ms = (step_end - step_start) * 1000
            step_times.append(step_ms)
            
            if step < 3 or step == DAYS - 1:
                print(f"  Шаг {step}: {step_ms:.2f} мс")
        
        t_sim_end = time.perf_counter()
        sim_ms = (t_sim_end - t_sim_start) * 1000
        
        # === Состояние ПОСЛЕ симуляции ===
        t_cpu_after_start = time.perf_counter()
        
        after = pyflamegpu.AgentVector(a_desc)
        sim2.getPopulationData(after)
        s6_a = sum(1 for ag in after if int(ag.getVariableUInt('status_id')) == 6)
        sne_a = [int(ag.getVariableUInt('sne')) if 'sne' in dir(ag) else 0 for ag in after]
        ppr_a = [int(ag.getVariableUInt('ppr')) for ag in after]
        rd_a = [int(ag.getVariableUInt('repair_days')) for ag in after]
        
        # Проверка инвариантов (как в бэкапе)
        invariants = (s6_b == s6_a) and (sne_b == sne_a) and (ppr_b == ppr_a) and (rd_b == rd_a)
        
        t_cpu_after_end = time.perf_counter()
        cpu_after_ms = (t_cpu_after_start - t_cpu_after_end) * 1000
        
        print(f"📊 ПОСЛЕ: s6={s6_a}, ppr_sum={sum(ppr_a)}, rd_sum={sum(rd_a)}")
        print(f"✅ Инварианты соблюдены: {invariants}")
        
        # === Сводка таймингов ===
        total_ms = load_ms + compile_ms + pop_ms + sim_ms + cpu_before_ms + cpu_after_ms
        
        print(f"\n⏱️ Детальные тайминги:")
        print(f"  📥 Загрузка данных:  {load_ms:>8.2f} мс ({load_ms/total_ms*100:>5.1f}%)")
        print(f"  🔧 Компиляция RTC:   {compile_ms:>8.2f} мс ({compile_ms/total_ms*100:>5.1f}%)")
        print(f"  👥 Создание агентов: {pop_ms:>8.2f} мс ({pop_ms/total_ms*100:>5.1f}%)")
        print(f"  🚀 Симуляция GPU:    {sim_ms:>8.2f} мс ({sim_ms/total_ms*100:>5.1f}%)")
        print(f"  📊 Логирование CPU:  {cpu_before_ms + cpu_after_ms:>8.2f} мс ({(cpu_before_ms + cpu_after_ms)/total_ms*100:>5.1f}%)")
        print(f"  ⏱️ Общее время:      {total_ms:>8.2f} мс")
        
        if step_times:
            avg_step = sum(step_times) / len(step_times)
            min_step = min(step_times)
            max_step = max(step_times)
            print(f"\n📈 Статистика шагов:")
            print(f"  Шагов: {len(step_times)}")
            print(f"  Средний: {avg_step:.2f} мс")
            print(f"  Мин: {min_step:.2f} мс")
            print(f"  Макс: {max_step:.2f} мс")
        
        # Производительность
        if DAYS > 0 and K > 0:
            days_per_sec = 1000 / (sim_ms / DAYS) if sim_ms > 0 else 0
            agents_per_ms = K / sim_ms if sim_ms > 0 else 0
            
            print(f"\n🚀 Производительность:")
            print(f"  Дней/сек: {days_per_sec:.1f}")
            print(f"  Агентов/мс: {agents_per_ms:.1f}")
        
        return invariants
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Главная функция"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description='Точный тест rtc_status_6')
    parser.add_argument('--days', type=int, default=7, help='Количество дней')
    parser.add_argument('--seatbelts', choices=['on', 'off'], default='on', help='FLAME GPU seatbelts')
    parser.add_argument('--jit-log', action='store_true', help='JIT лог')
    
    args = parser.parse_args()
    
    if args.jit_log:
        os.environ['HL_JIT_LOG'] = '1'
        os.environ['PYTHONUNBUFFERED'] = '1'
        os.environ['CUDA_LAUNCH_BLOCKING'] = '1'
        print("🔧 JIT лог включен")
    
    success = test_status6_exact(
        days=args.days,
        seatbelts=(args.seatbelts == 'on')
    )
    
    if success:
        print(f"\n🎉 Точный тест rtc_status_6 прошел успешно!")
        print(f"✅ Готово к тестированию на больших периодах (30, 365, 3650 дней)")
        return 0
    else:
        print(f"\n❌ Тест провалился")
        return 1


if __name__ == '__main__':
    sys.exit(main())


