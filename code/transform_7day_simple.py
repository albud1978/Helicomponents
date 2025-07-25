#!/usr/bin/env python3
"""
Transform 7-Day Analysis
Простой анализ симуляции на 7 дней без внешних зависимостей
Использует встроенные возможности Flame GPU и Transform Master

Дата: 25-07-2025
"""

import sys
import os
import json
from datetime import datetime
from collections import defaultdict, Counter

# Настройка путей
sys.path.append('code/')
sys.path.append('code/utils')

from transform_master import TransformMaster
from config_loader import get_clickhouse_client

def run_7day_simulation():
    """Запуск симуляции на 7 дней с встроенной отчетностью"""
    print("🚀 Запуск Transform симуляции на 7 дней...")
    print("=" * 60)
    
    config = {
        'simulation_start_date': None,  # Автоматическое определение из heli_pandas
        'simulation_days': 7,
        'validation_level': 'full',
        'export_to_clickhouse': True,
        'generate_reports': True
    }
    
    try:
        master = TransformMaster(config)
        
        # Поэтапное выполнение с детальным логированием
        print("1️⃣ Проверка Extract данных...")
        if not master.verify_extract_completion():
            print("❌ Extract данные не готовы")
            return None, None
            
        print("2️⃣ Запуск Flame GPU симуляции...")
        if not master.execute_flame_gpu_simulation():
            print("❌ Ошибка симуляции")
            return None, None
            
        print("3️⃣ Постпроцессинг...")
        if not master.execute_postprocessing():
            print("❌ Ошибка постпроцессинга")
            return None, None
            
        print("4️⃣ Валидация результатов...")
        if not master.validate_transform_results():
            print("❌ Ошибка валидации")
            return None, None
            
        print("5️⃣ Экспорт в ClickHouse...")
        if not master.export_to_clickhouse():
            print("❌ Ошибка экспорта")
            return None, None
            
        print("6️⃣ Генерация отчетов...")
        transform_report = master.generate_transform_report()
        flame_report = master.flame_gpu_model.generate_report()
        
        print("✅ Transform завершен успешно!")
        print(f"📊 Transform отчет: {transform_report}")
        print(f"🚁 Flame GPU отчет: {flame_report}")
        
        return master, master.transform_results.get('simulation_version_id')
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return None, None

def analyze_simulation_data(master, version_id):
    """Анализ данных симуляции с использованием встроенных возможностей"""
    print("\n" + "=" * 60)
    print("📊 АНАЛИЗ РЕЗУЛЬТАТОВ СИМУЛЯЦИИ")
    print("=" * 60)
    
    try:
        # Получаем данные из модели
        model = master.flame_gpu_model
        logging_data = model.macro_property_2
        
        print(f"🏷️ Version ID: {version_id}")
        print(f"📋 Общее количество записей: {len(logging_data):,}")
        print(f"🚁 Количество агентов: {len(model.agents)}")
        print(f"📅 Дней симуляции: {model.simulation_days}")
        
        # Группировка по дням для анализа
        daily_stats = defaultdict(lambda: {
            'status_counts': Counter(),
            'type_counts': Counter(),
            'total_hours': 0,
            'repairs': 0,
            'assemblies': 0,
            'active_aircraft': set()
        })
        
        status_names = {
            1: 'Неактивный',
            2: 'Операции', 
            3: 'Склад',
            4: 'Ремонт',
            5: 'Резерв',
            6: 'Хранение'
        }
        
        type_names = {64: 'МИ-17', 32: 'МИ-8'}
        
        # Обработка записей по дням
        for record in logging_data:
            date = record['dates']
            status_id = record['status_id']
            ac_type_mask = record['ac_type_mask']
            aircraft_num = record['aircraft_number']
            
            daily_stats[date]['status_counts'][status_names.get(status_id, f'Статус_{status_id}')] += 1
            daily_stats[date]['type_counts'][type_names.get(ac_type_mask, f'Тип_{ac_type_mask}')] += 1
            daily_stats[date]['total_hours'] += record.get('daily_hours', 0)
            daily_stats[date]['repairs'] += record.get('partout_trigger', 0)
            daily_stats[date]['assemblies'] += record.get('assembly_trigger', 0)
            daily_stats[date]['active_aircraft'].add(aircraft_num)
        
        # Вывод ежедневной статистики
        print("\n📈 ЕЖЕДНЕВНАЯ СТАТИСТИКА:")
        print("-" * 60)
        
        for date in sorted(daily_stats.keys()):
            stats = daily_stats[date]
            print(f"\n📅 {date}:")
            
            # Статусы
            print("   🎯 Статусы:")
            for status, count in stats['status_counts'].most_common():
                print(f"      {status}: {count}")
            
            # Типы ВС
            print("   ✈️ Типы ВС:")
            for ac_type, count in stats['type_counts'].items():
                print(f"      {ac_type}: {count}")
            
            # Активность
            print(f"   ⏰ Общий налет: {stats['total_hours']:.1f} часов")
            print(f"   🔧 Ремонты: {stats['repairs']}")
            print(f"   🔩 Сборки: {stats['assemblies']}")
            print(f"   🛩️ Активных ВС: {len(stats['active_aircraft'])}")
        
        # Сводная статистика за весь период
        print("\n📊 СВОДНАЯ СТАТИСТИКА:")
        print("-" * 60)
        
        all_statuses = Counter()
        all_types = Counter()
        total_hours = 0
        total_repairs = 0
        total_assemblies = 0
        
        for stats in daily_stats.values():
            all_statuses.update(stats['status_counts'])
            all_types.update(stats['type_counts'])
            total_hours += stats['total_hours']
            total_repairs += stats['repairs']
            total_assemblies += stats['assemblies']
        
        print(f"🎯 Распределение по статусам:")
        for status, count in all_statuses.most_common():
            percentage = (count / len(logging_data)) * 100
            print(f"   {status}: {count:,} ({percentage:.1f}%)")
        
        print(f"\n✈️ Распределение по типам ВС:")
        for ac_type, count in all_types.items():
            percentage = (count / len(logging_data)) * 100
            print(f"   {ac_type}: {count:,} ({percentage:.1f}%)")
        
        print(f"\n⏰ Общие показатели:")
        print(f"   Суммарный налет: {total_hours:.1f} часов")
        print(f"   Средний дневной налет: {total_hours / model.simulation_days:.1f} часов")
        print(f"   Всего ремонтов: {total_repairs}")
        print(f"   Всего сборок: {total_assemblies}")
        
        # Анализ динамики
        print(f"\n📈 АНАЛИЗ ДИНАМИКИ:")
        print("-" * 60)
        
        dates_sorted = sorted(daily_stats.keys())
        if len(dates_sorted) >= 2:
            first_day = daily_stats[dates_sorted[0]]
            last_day = daily_stats[dates_sorted[-1]]
            
            print(f"🚀 Изменения с {dates_sorted[0]} по {dates_sorted[-1]}:")
            
            # Сравнение статусов
            for status in status_names.values():
                first_count = first_day['status_counts'].get(status, 0)
                last_count = last_day['status_counts'].get(status, 0)
                change = last_count - first_count
                if change != 0:
                    direction = "↗️" if change > 0 else "↘️"
                    print(f"   {status}: {first_count} → {last_count} ({change:+d}) {direction}")
            
            # Изменение налета
            hours_change = last_day['total_hours'] - first_day['total_hours']
            direction = "↗️" if hours_change > 0 else "↘️" if hours_change < 0 else "➡️"
            print(f"   Дневной налет: {first_day['total_hours']:.1f} → {last_day['total_hours']:.1f} ({hours_change:+.1f}) {direction}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка анализа: {e}")
        import traceback
        traceback.print_exc()
        return False

def load_reports_analysis(master):
    """Анализ сгенерированных отчетов"""
    print("\n" + "=" * 60)
    print("📋 АНАЛИЗ ОТЧЕТОВ")
    print("=" * 60)
    
    try:
        # Загружаем Transform отчет
        transform_report_file = f"logs/transform_report_{master.session_id}.json"
        if os.path.exists(transform_report_file):
            with open(transform_report_file, 'r', encoding='utf-8') as f:
                transform_report = json.load(f)
            
            print("📊 Transform Master Report:")
            exec_stats = transform_report.get('execution_statistics', {})
            print(f"   📅 Сессия: {transform_report.get('session_id', 'N/A')}")
            print(f"   ⏱️ Общее время: {exec_stats.get('total_duration', 'N/A')}")
            print(f"   🎯 Успешность: {exec_stats.get('success_rate', 'N/A')}")
            
            results = transform_report.get('transform_results', {})
            print(f"   🚁 Агенты: {results.get('total_agents', 'N/A')}")
            print(f"   📋 Записи: {results.get('total_records', 'N/A')}")
        else:
            print("⚠️ Transform отчет не найден")
        
        # Загружаем Flame GPU отчет
        flame_report_file = f"reports/flame_gpu_report_{master.flame_gpu_model.version_id}.json"
        if os.path.exists(flame_report_file):
            with open(flame_report_file, 'r', encoding='utf-8') as f:
                flame_report = json.load(f)
            
            print("\n🚁 Flame GPU Model Report:")
            sim_info = flame_report.get('simulation_info', {})
            print(f"   🏷️ Version: {sim_info.get('version_id', 'N/A')}")
            print(f"   📅 Start Date: {sim_info.get('start_date', 'N/A')}")
            print(f"   📊 Days: {sim_info.get('simulation_days', 'N/A')}")
            
            perf_summary = flame_report.get('performance_summary', {})
            print(f"   📋 Logging Records: {perf_summary.get('total_logging_records', 'N/A'):,}")
            print(f"   ✈️ Ops Aircraft: {perf_summary.get('average_ops_aircraft', 'N/A')}")
            print(f"   🔧 Repairs: {perf_summary.get('repair_utilization', 'N/A')}")
        else:
            print("⚠️ Flame GPU отчет не найден")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка загрузки отчетов: {e}")
        return False

def main():
    """Основная функция"""
    print("🎯 TRANSFORM 7-DAY SIMPLE ANALYSIS")
    print("=" * 60)
    print("Использует встроенные возможности Flame GPU")
    print("=" * 60)
    
    # 1. Запуск симуляции
    master, version_id = run_7day_simulation()
    if not master or not version_id:
        print("❌ Не удалось запустить симуляцию")
        return
    
    # 2. Анализ результатов
    analysis_success = analyze_simulation_data(master, version_id)
    if not analysis_success:
        print("❌ Не удалось проанализировать данные")
        return
    
    # 3. Анализ отчетов
    reports_success = load_reports_analysis(master)
    if not reports_success:
        print("⚠️ Частичный анализ отчетов")
    
    # 4. Итоговый отчет
    print("\n" + "=" * 60)
    print("🎉 АНАЛИЗ ЗАВЕРШЕН!")
    print("=" * 60)
    print(f"📊 Version ID: {version_id}")
    print(f"🚁 Агенты: {len(master.flame_gpu_model.agents)}")
    print(f"📋 Записи: {len(master.flame_gpu_model.macro_property_2):,}")
    print(f"📅 Дней: {master.flame_gpu_model.simulation_days}")
    print(f"📁 Отчеты сохранены в: logs/ и reports/")
    print("=" * 60)
    print("🔍 Для детального анализа смотрите JSON отчеты:")
    print(f"   📊 Transform: logs/transform_report_{master.session_id}.json")
    print(f"   🚁 Flame GPU: reports/flame_gpu_report_{version_id}.json")
    print("=" * 60)

if __name__ == "__main__":
    main() 