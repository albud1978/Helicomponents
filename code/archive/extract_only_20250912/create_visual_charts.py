#!/usr/bin/env python3
"""
Создание визуальных графиков из результатов тестирования визуализации Flame GPU
"""

import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np

# Настройка русского шрифта и стиля
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def load_visualization_results():
    """Загружает результаты всех трех вариантов"""
    results = {}
    
    # Вариант 1: FGV
    try:
        with open('vis1_results_VIS1_20250725_120331.json', 'r', encoding='utf-8') as f:
            results['fgv'] = json.load(f)
        print("✅ Загружены результаты FGV визуализации")
    except FileNotFoundError:
        print("❌ Файл FGV результатов не найден")
        
    # Вариант 2: Программная
    try:
        with open('vis2_results_VIS2_20250725_120409.json', 'r', encoding='utf-8') as f:
            results['programmatic'] = json.load(f)
        print("✅ Загружены результаты программной визуализации")
    except FileNotFoundError:
        print("❌ Файл программных результатов не найден")
        
    # Вариант 3: Расширенная
    try:
        with open('vis3_results_VIS3_20250725_120421.json', 'r', encoding='utf-8') as f:
            results['extended'] = json.load(f)
        print("✅ Загружены результаты расширенной аналитики")
    except FileNotFoundError:
        print("❌ Файл расширенных результатов не найден")
    
    return results

def create_fgv_simulation_chart(results):
    """Создает график симуляции FGV (имитация 3D размещения)"""
    if 'fgv' not in results:
        return None
        
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # График 1: Динамика налета по дням
    daily_stats = results['fgv']['vis_data']['daily_stats']
    dates = [stat['date'] for stat in daily_stats]
    avg_hours = [stat['avg_daily_hours'] for stat in daily_stats]
    
    ax1.plot(dates, avg_hours, marker='o', linewidth=3, markersize=8)
    ax1.set_title('🎨 FGV: Динамика налета по дням', fontsize=14, fontweight='bold')
    ax1.set_xlabel('Дата')
    ax1.set_ylabel('Средний налет (часы)')
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis='x', rotation=45)
    
    # График 2: Имитация 3D размещения агентов (вид сверху)
    last_day = daily_stats[-1]
    
    # Создаем "псевдо-3D" размещение агентов
    mi8_count = last_day['by_type']['MI-8']
    mi17_count = last_day['by_type']['MI-17']
    
    # МИ-8 (левая сторона)
    mi8_x = np.random.uniform(0, 40, mi8_count)
    mi8_y = np.random.uniform(0, 50, mi8_count)
    
    # МИ-17 (правая сторона)  
    mi17_x = np.random.uniform(60, 100, mi17_count)
    mi17_y = np.random.uniform(0, 50, mi17_count)
    
    ax2.scatter(mi8_x, mi8_y, c='green', s=100, alpha=0.7, label=f'МИ-8 ({mi8_count})', marker='s')
    ax2.scatter(mi17_x, mi17_y, c='red', s=120, alpha=0.7, label=f'МИ-17 ({mi17_count})', marker='^')
    
    ax2.set_title('🎨 FGV: Размещение агентов (вид сверху)', fontsize=14, fontweight='bold')
    ax2.set_xlabel('X координата')
    ax2.set_ylabel('Y координата')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.set_xlim(0, 100)
    ax2.set_ylim(0, 50)
    
    plt.tight_layout()
    return fig

def create_programmatic_analysis_charts(results):
    """Создает графики программной визуализации"""
    if 'programmatic' not in results:
        return None
        
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # График 1: Тренды утилизации и здоровья флота
    snapshots = results['programmatic']['visualization_data']['agent_snapshots']
    dates = [s['date'] for s in snapshots]
    utilization = [s['summary']['avg_utilization'] for s in snapshots]
    
    analytics = results['programmatic']['visualization_data']['fleet_analytics']
    health_scores = [a['fleet_health']['health_score'] for a in analytics]
    
    ax1_twin = ax1.twinx()
    
    line1 = ax1.plot(dates, utilization, 'b-o', linewidth=2, label='Утилизация')
    line2 = ax1_twin.plot(dates, health_scores, 'r-s', linewidth=2, label='Здоровье флота')
    
    ax1.set_title('💻 Тренды утилизации и здоровья флота', fontsize=14, fontweight='bold')
    ax1.set_xlabel('Дата')
    ax1.set_ylabel('Утилизация', color='b')
    ax1_twin.set_ylabel('Здоровье флота', color='r')
    ax1.tick_params(axis='x', rotation=45)
    ax1.grid(True, alpha=0.3)
    
    # Легенда для двух осей
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc='center right')
    
    # График 2: Переходы состояний
    transitions = results['programmatic']['visualization_data']['state_transitions']
    transition_counts = [t['status_changes'] for t in transitions]
    transition_dates = [t['date'] for t in transitions]
    
    ax2.bar(transition_dates, transition_counts, alpha=0.7, color='orange')
    ax2.set_title('💻 Переходы состояний по дням', fontsize=14, fontweight='bold')
    ax2.set_xlabel('Дата')
    ax2.set_ylabel('Количество изменений')
    ax2.tick_params(axis='x', rotation=45)
    ax2.grid(True, alpha=0.3)
    
    # График 3: Распределение по статусам (последний день)
    last_snapshot = snapshots[-1]
    status_names = {
        'status_1': 'Неактивно', 'status_2': 'Эксплуатация', 'status_3': 'Исправен',
        'status_4': 'Ремонт', 'status_5': 'Резерв', 'status_6': 'Хранение'
    }
    
    statuses = []
    counts = []
    for status_key, count in last_snapshot['summary']['by_status'].items():
        status_name = status_names.get(status_key, status_key)
        statuses.append(status_name)
        counts.append(count)
    
    colors = ['gray', 'green', 'lightblue', 'red', 'yellow', 'purple']
    ax3.pie(counts, labels=statuses, autopct='%1.1f%%', colors=colors, startangle=90)
    ax3.set_title('💻 Распределение по статусам (день 7)', fontsize=14, fontweight='bold')
    
    # График 4: Операционные vs обслуживание
    utilization_data = results['programmatic']['visualization_data']['utilization_metrics']
    operational = [u['operational_aircraft'] for u in utilization_data]
    maintenance = [u['maintenance_aircraft'] for u in utilization_data]
    dates_util = [u['date'] for u in utilization_data]
    
    ax4.plot(dates_util, operational, 'g-o', linewidth=2, label='Операционные')
    ax4.plot(dates_util, maintenance, 'r-s', linewidth=2, label='На обслуживании')
    ax4.set_title('💻 Операционные vs Обслуживание', fontsize=14, fontweight='bold')
    ax4.set_xlabel('Дата')
    ax4.set_ylabel('Количество ВС')
    ax4.legend()
    ax4.tick_params(axis='x', rotation=45)
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig

def create_extended_analytics_charts(results):
    """Создает графики расширенной аналитики"""
    if 'extended' not in results:
        return None
        
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # График 1: Финансовые показатели по дням
    reports = results['extended']['analytics_data']['detailed_reports']
    dates = [r['date'] for r in reports]
    maintenance_costs = [r['financial_metrics']['total_maintenance_cost'] / 1000000 for r in reports]  # в млн
    operational_costs = [r['financial_metrics']['total_operational_cost'] / 1000000 for r in reports]   # в млн
    
    ax1.plot(dates, maintenance_costs, 'b-o', linewidth=2, label='Затраты на ТО')
    ax1.plot(dates, operational_costs, 'r-s', linewidth=2, label='Операционные затраты')
    ax1.set_title('📊 Финансовые показатели (млн руб.)', fontsize=14, fontweight='bold')
    ax1.set_xlabel('Дата')
    ax1.set_ylabel('Затраты (млн руб.)')
    ax1.legend()
    ax1.tick_params(axis='x', rotation=45)
    ax1.grid(True, alpha=0.3)
    
    # График 2: KPI показатели
    benchmarks = results['extended']['analytics_data']['performance_benchmarks']
    kpi_dates = [b['date'] for b in benchmarks]
    operational_readiness = [b['kpi_metrics']['operational_readiness'] * 100 for b in benchmarks]
    maintenance_efficiency = [b['kpi_metrics']['maintenance_efficiency'] * 100 for b in benchmarks]
    
    ax2.plot(kpi_dates, operational_readiness, 'g-o', linewidth=2, label='Готовность (%)')
    ax2.plot(kpi_dates, maintenance_efficiency, 'orange', linewidth=2, marker='s', label='Эффективность ТО (%)')
    ax2.set_title('📊 KPI показатели', fontsize=14, fontweight='bold')
    ax2.set_xlabel('Дата')
    ax2.set_ylabel('Процент (%)')
    ax2.legend()
    ax2.tick_params(axis='x', rotation=45)
    ax2.grid(True, alpha=0.3)
    
    # График 3: Прогноз рисков
    forecasts = results['extended']['analytics_data']['predictive_models']
    risk_data = forecasts[-1]['failure_predictions']  # Последний день
    
    risk_categories = list(risk_data.keys())
    risk_counts = list(risk_data.values())
    colors_risk = ['green', 'orange', 'red']
    
    ax3.bar(risk_categories, risk_counts, color=colors_risk, alpha=0.7)
    ax3.set_title('📊 Прогноз рисков отказов', fontsize=14, fontweight='bold')
    ax3.set_xlabel('Уровень риска')
    ax3.set_ylabel('Количество ВС')
    ax3.grid(True, alpha=0.3)
    
    # График 4: Стоимость флота по дням
    fleet_values = [r['fleet_summary']['total_fleet_value'] / 1000000 for r in reports]  # в млн
    
    ax4.plot(dates, fleet_values, 'purple', linewidth=3, marker='o', markersize=8)
    ax4.set_title('📊 Стоимость флота (млн руб.)', fontsize=14, fontweight='bold')
    ax4.set_xlabel('Дата')
    ax4.set_ylabel('Стоимость (млн руб.)')
    ax4.tick_params(axis='x', rotation=45)
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig

def create_comparison_summary(results):
    """Создает сводный график сравнения всех вариантов"""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # График 1: Сравнение производительности
    variants = ['FGV\n(Вариант 1)', 'Программная\n(Вариант 2)', 'Расширенная\n(Вариант 3)']
    execution_times = [25, 1, 1]  # секунды
    colors = ['lightblue', 'lightgreen', 'lightcoral']
    
    bars1 = ax1.bar(variants, execution_times, color=colors, alpha=0.7)
    ax1.set_title('⚡ Сравнение времени выполнения', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Время (секунды)')
    ax1.grid(True, alpha=0.3)
    
    # Добавляем значения на столбцы
    for bar, time in zip(bars1, execution_times):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                f'{time}с', ha='center', va='bottom', fontweight='bold')
    
    # График 2: Функциональные возможности (оценка)
    capabilities = {
        'Визуализация': [9, 6, 7],
        'Аналитика': [4, 8, 10],
        'Прогнозы': [2, 6, 10],
        'Отчеты': [5, 7, 10]
    }
    
    x = np.arange(len(variants))
    width = 0.15
    
    for i, (capability, scores) in enumerate(capabilities.items()):
        offset = (i - 1.5) * width
        ax2.bar(x + offset, scores, width, label=capability, alpha=0.8)
    
    ax2.set_title('🎯 Функциональные возможности (1-10)', fontsize=14, fontweight='bold')
    ax2.set_xlabel('Варианты')
    ax2.set_ylabel('Оценка')
    ax2.set_xticks(x)
    ax2.set_xticklabels(variants)
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # График 3: Размер выходных данных
    if all(variant in results for variant in ['fgv', 'programmatic', 'extended']):
        data_sizes = [4.6, 345, 134]  # КБ
        ax3.bar(variants, data_sizes, color=colors, alpha=0.7)
        ax3.set_title('💾 Размер результатов (КБ)', fontsize=14, fontweight='bold')
        ax3.set_ylabel('Размер (КБ)')
        ax3.grid(True, alpha=0.3)
        
        for i, size in enumerate(data_sizes):
            ax3.text(i, size + 5, f'{size} КБ', ha='center', va='bottom', fontweight='bold')
    
    # График 4: Применимость (радарная диаграмма упрощенная)
    use_cases = ['Презентации', 'Мониторинг', 'Планирование', 'Анализ', 'Прогнозы']
    fgv_scores = [10, 4, 3, 5, 2]
    prog_scores = [6, 9, 6, 8, 6]
    ext_scores = [7, 7, 10, 10, 10]
    
    x_pos = np.arange(len(use_cases))
    ax4.plot(x_pos, fgv_scores, 'o-', linewidth=2, label='FGV', color='lightblue')
    ax4.plot(x_pos, prog_scores, 's-', linewidth=2, label='Программная', color='lightgreen')
    ax4.plot(x_pos, ext_scores, '^-', linewidth=2, label='Расширенная', color='lightcoral')
    
    ax4.set_title('🎯 Применимость (1-10)', fontsize=14, fontweight='bold')
    ax4.set_xticks(x_pos)
    ax4.set_xticklabels(use_cases, rotation=45)
    ax4.set_ylabel('Пригодность')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    ax4.set_ylim(0, 10)
    
    plt.tight_layout()
    return fig

def main():
    """Главная функция создания визуализации"""
    print("🎨 СОЗДАНИЕ ВИЗУАЛЬНЫХ ГРАФИКОВ РЕЗУЛЬТАТОВ")
    print("=" * 60)
    
    # Загружаем результаты
    results = load_visualization_results()
    
    if not results:
        print("❌ Не найдено файлов результатов для визуализации")
        return
    
    # Создаем графики
    figures = []
    
    # 1. FGV симуляция
    print("\n🎨 Создание графиков FGV визуализации...")
    fgv_fig = create_fgv_simulation_chart(results)
    if fgv_fig:
        fgv_fig.suptitle('🎨 FLAME GPU FGV ВИЗУАЛИЗАЦИЯ', fontsize=16, fontweight='bold')
        fgv_fig.savefig('fgv_visualization_charts.png', dpi=300, bbox_inches='tight')
        figures.append(('fgv_visualization_charts.png', 'FGV визуализация'))
        print("✅ Сохранено: fgv_visualization_charts.png")
    
    # 2. Программная визуализация
    print("💻 Создание графиков программной визуализации...")
    prog_fig = create_programmatic_analysis_charts(results)
    if prog_fig:
        prog_fig.suptitle('💻 ПРОГРАММНАЯ ВИЗУАЛИЗАЦИЯ', fontsize=16, fontweight='bold')
        prog_fig.savefig('programmatic_visualization_charts.png', dpi=300, bbox_inches='tight')
        figures.append(('programmatic_visualization_charts.png', 'Программная визуализация'))
        print("✅ Сохранено: programmatic_visualization_charts.png")
    
    # 3. Расширенная аналитика
    print("📊 Создание графиков расширенной аналитики...")
    ext_fig = create_extended_analytics_charts(results)
    if ext_fig:
        ext_fig.suptitle('📊 РАСШИРЕННАЯ АНАЛИТИКА', fontsize=16, fontweight='bold')
        ext_fig.savefig('extended_analytics_charts.png', dpi=300, bbox_inches='tight')
        figures.append(('extended_analytics_charts.png', 'Расширенная аналитика'))
        print("✅ Сохранено: extended_analytics_charts.png")
    
    # 4. Сравнительный анализ
    print("🏆 Создание сводного сравнения...")
    comp_fig = create_comparison_summary(results)
    if comp_fig:
        comp_fig.suptitle('🏆 СРАВНИТЕЛЬНЫЙ АНАЛИЗ ВАРИАНТОВ', fontsize=16, fontweight='bold')
        comp_fig.savefig('comparison_summary_charts.png', dpi=300, bbox_inches='tight')
        figures.append(('comparison_summary_charts.png', 'Сравнительный анализ'))
        print("✅ Сохранено: comparison_summary_charts.png")
    
    # Финальный отчет
    print(f"\n🎯 СОЗДАНО ВИЗУАЛИЗАЦИЙ: {len(figures)}")
    for filename, description in figures:
        print(f"  📊 {filename} - {description}")
    
    print("\n✅ Все графики сохранены в PNG формате высокого разрешения!")
    print("🔍 Используйте любой просмотрщик изображений для просмотра результатов")

if __name__ == "__main__":
    main() 