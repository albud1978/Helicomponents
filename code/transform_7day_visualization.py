#!/usr/bin/env python3
"""
Transform 7-Day Visualization
Запуск симуляции на 7 дней и создание визуализации результатов

Дата: 25-07-2025
"""

import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, date
import json

# Настройка путей
sys.path.append('code/')
sys.path.append('code/utils')

from transform_master import TransformMaster
from config_loader import get_clickhouse_client

# Настройка matplotlib для русского языка
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['axes.unicode_minus'] = False

def run_7day_simulation():
    """Запуск симуляции на 7 дней"""
    print("🚀 Запуск Transform симуляции на 7 дней...")
    
    config = {
        'simulation_start_date': '2025-01-01',
        'simulation_days': 7,
        'validation_level': 'basic',
        'export_to_clickhouse': True,
        'generate_reports': True
    }
    
    try:
        master = TransformMaster(config)
        
        # Поэтапное выполнение для контроля
        print("1. Проверка Extract данных...")
        if not master.verify_extract_completion():
            print("❌ Extract данные не готовы")
            return None
            
        print("2. Запуск Flame GPU симуляции...")
        if not master.execute_flame_gpu_simulation():
            print("❌ Ошибка симуляции")
            return None
            
        print("3. Постпроцессинг...")
        if not master.execute_postprocessing():
            print("❌ Ошибка постпроцессинга")
            return None
            
        print("4. Валидация результатов...")
        if not master.validate_transform_results():
            print("❌ Ошибка валидации")
            return None
            
        print("5. Экспорт в ClickHouse...")
        if not master.export_to_clickhouse():
            print("❌ Ошибка экспорта")
            return None
            
        print("✅ Transform завершен успешно!")
        return master.transform_results.get('simulation_version_id')
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return None

def load_simulation_data(version_id):
    """Загрузка данных симуляции из ClickHouse"""
    print(f"📊 Загрузка данных версии {version_id}...")
    
    try:
        client = get_clickhouse_client()
        
        # Основные данные симуляции
        query = f"""
        SELECT 
            dates,
            aircraft_number,
            daily_hours,
            status_id,
            ac_type_mask,
            sne,
            ppr,
            partout_trigger,
            assembly_trigger,
            aircraft_age_years
        FROM LoggingLayer_Planes 
        WHERE version_id = '{version_id}'
        ORDER BY dates, aircraft_number
        """
        
        results = client.execute(query)
        df = pd.DataFrame(results, columns=[
            'dates', 'aircraft_number', 'daily_hours', 'status_id', 
            'ac_type_mask', 'sne', 'ppr', 'partout_trigger', 
            'assembly_trigger', 'aircraft_age_years'
        ])
        
        # Преобразуем типы
        df['dates'] = pd.to_datetime(df['dates'])
        df['ac_type'] = df['ac_type_mask'].map({64: 'МИ-17', 32: 'МИ-8'})
        
        # Добавляем названия статусов
        status_names = {
            1: 'Неактивный',
            2: 'Операции', 
            3: 'Склад',
            4: 'Ремонт',
            5: 'Резерв',
            6: 'Хранение'
        }
        df['status_name'] = df['status_id'].map(status_names)
        
        print(f"✅ Загружено {len(df):,} записей")
        return df
        
    except Exception as e:
        print(f"❌ Ошибка загрузки: {e}")
        return None

def create_visualizations(df, version_id):
    """Создание визуализаций"""
    print("📈 Создание визуализаций...")
    
    # Создаем папку для сохранения
    viz_dir = f"visualizations/transform_7day_{version_id}"
    os.makedirs(viz_dir, exist_ok=True)
    
    # График 1: Распределение планеров по статусам по дням
    plt.figure(figsize=(14, 8))
    
    # Группируем по дням и статусам
    daily_status = df.groupby(['dates', 'status_name']).size().unstack(fill_value=0)
    
    # Создаем stacked bar chart
    ax = daily_status.plot(kind='bar', stacked=True, figsize=(14, 8), 
                          colormap='Set3', width=0.8)
    
    plt.title(f'Распределение планеров по статусам (версия {version_id})', 
              fontsize=16, fontweight='bold')
    plt.xlabel('Дата', fontsize=12)
    plt.ylabel('Количество планеров', fontsize=12)
    plt.legend(title='Статус', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    plt.savefig(f'{viz_dir}/status_distribution.png', dpi=300, bbox_inches='tight')
    print(f"✅ График статусов сохранен: {viz_dir}/status_distribution.png")
    
    # График 2: Движение по типам ВС
    plt.figure(figsize=(14, 6))
    
    type_daily = df.groupby(['dates', 'ac_type', 'status_name']).size().unstack(fill_value=0)
    
    # Subplot для каждого типа
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # МИ-8
    mi8_data = df[df['ac_type'] == 'МИ-8'].groupby(['dates', 'status_name']).size().unstack(fill_value=0)
    mi8_data.plot(kind='area', ax=ax1, alpha=0.7, colormap='Set1')
    ax1.set_title('МИ-8: Динамика статусов', fontweight='bold')
    ax1.set_xlabel('Дата')
    ax1.set_ylabel('Количество планеров')
    ax1.legend(title='Статус', loc='upper right')
    ax1.grid(True, alpha=0.3)
    
    # МИ-17
    mi17_data = df[df['ac_type'] == 'МИ-17'].groupby(['dates', 'status_name']).size().unstack(fill_value=0)
    mi17_data.plot(kind='area', ax=ax2, alpha=0.7, colormap='Set2')
    ax2.set_title('МИ-17: Динамика статусов', fontweight='bold')
    ax2.set_xlabel('Дата')
    ax2.set_ylabel('Количество планеров')
    ax2.legend(title='Статус', loc='upper right')
    ax2.grid(True, alpha=0.3)
    
    plt.suptitle(f'Динамика по типам ВС (версия {version_id})', fontsize=16, fontweight='bold')
    plt.tight_layout()
    
    plt.savefig(f'{viz_dir}/aircraft_type_dynamics.png', dpi=300, bbox_inches='tight')
    print(f"✅ График типов ВС сохранен: {viz_dir}/aircraft_type_dynamics.png")
    
    # График 3: Heatmap налетов
    plt.figure(figsize=(12, 8))
    
    # Пивот по aircraft_number и dates для daily_hours
    flight_hours_pivot = df.pivot_table(
        values='daily_hours', 
        index='aircraft_number', 
        columns='dates', 
        aggfunc='sum',
        fill_value=0
    )
    
    # Берем первые 50 планеров для читаемости
    sample_aircraft = flight_hours_pivot.head(50)
    
    sns.heatmap(sample_aircraft, 
                cmap='YlOrRd', 
                cbar_kws={'label': 'Часы налета'},
                fmt='.1f')
    
    plt.title(f'Heatmap налетов (первые 50 планеров, версия {version_id})', 
              fontsize=14, fontweight='bold')
    plt.xlabel('Дата')
    plt.ylabel('Номер планера')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    plt.savefig(f'{viz_dir}/flight_hours_heatmap.png', dpi=300, bbox_inches='tight')
    print(f"✅ Heatmap налетов сохранен: {viz_dir}/flight_hours_heatmap.png")
    
    # График 4: Ключевые метрики
    plt.figure(figsize=(15, 10))
    
    # 2x2 subplot
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
    
    # Общее количество часов по дням
    daily_hours = df.groupby('dates')['daily_hours'].sum()
    daily_hours.plot(kind='line', ax=ax1, marker='o', linewidth=2, markersize=6)
    ax1.set_title('Общий налет по дням', fontweight='bold')
    ax1.set_ylabel('Часы')
    ax1.grid(True, alpha=0.3)
    
    # Среднее SNE по типам ВС
    avg_sne = df.groupby(['dates', 'ac_type'])['sne'].mean().unstack()
    avg_sne.plot(kind='line', ax=ax2, marker='s', linewidth=2)
    ax2.set_title('Средняя наработка SNE', fontweight='bold')
    ax2.set_ylabel('Часы SNE')
    ax2.legend(title='Тип ВС')
    ax2.grid(True, alpha=0.3)
    
    # Триггеры по дням
    triggers = df.groupby('dates')[['partout_trigger', 'assembly_trigger']].sum()
    triggers.plot(kind='bar', ax=ax3, width=0.8)
    ax3.set_title('Триггеры по дням', fontweight='bold')
    ax3.set_ylabel('Количество')
    ax3.legend(['Partout', 'Assembly'])
    ax3.grid(True, alpha=0.3)
    
    # Возрастное распределение
    age_dist = df.groupby('ac_type')['aircraft_age_years'].mean()
    age_dist.plot(kind='bar', ax=ax4, color=['skyblue', 'lightcoral'])
    ax4.set_title('Средний возраст планеров', fontweight='bold')
    ax4.set_ylabel('Годы')
    ax4.set_xticklabels(age_dist.index, rotation=0)
    ax4.grid(True, alpha=0.3)
    
    plt.suptitle(f'Ключевые метрики симуляции (версия {version_id})', 
                fontsize=16, fontweight='bold')
    plt.tight_layout()
    
    plt.savefig(f'{viz_dir}/key_metrics.png', dpi=300, bbox_inches='tight')
    print(f"✅ Ключевые метрики сохранены: {viz_dir}/key_metrics.png")
    
    # Создаем сводную статистику
    stats = {
        'simulation_version': version_id,
        'period': f"{df['dates'].min().strftime('%Y-%m-%d')} - {df['dates'].max().strftime('%Y-%m-%d')}",
        'total_records': len(df),
        'unique_aircraft': df['aircraft_number'].nunique(),
        'simulation_days': df['dates'].nunique(),
        'total_flight_hours': df['daily_hours'].sum(),
        'avg_daily_hours': df['daily_hours'].mean(),
        'aircraft_types': df['ac_type'].value_counts().to_dict(),
        'status_distribution': df.groupby('status_name').size().to_dict(),
        'triggers': {
            'partout_events': df['partout_trigger'].sum(),
            'assembly_events': df['assembly_trigger'].sum()
        },
        'aircraft_age': {
            'min': df['aircraft_age_years'].min(),
            'max': df['aircraft_age_years'].max(),
            'mean': df['aircraft_age_years'].mean()
        }
    }
    
    # Сохраняем статистику
    with open(f'{viz_dir}/simulation_stats.json', 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2, default=str)
    
    print(f"✅ Статистика сохранена: {viz_dir}/simulation_stats.json")
    
    return viz_dir

def main():
    """Основная функция"""
    print("🎯 Transform 7-Day Visualization")
    print("=" * 50)
    
    # 1. Запуск симуляции
    version_id = run_7day_simulation()
    if not version_id:
        print("❌ Не удалось запустить симуляцию")
        return
    
    # 2. Загрузка данных
    df = load_simulation_data(version_id)
    if df is None:
        print("❌ Не удалось загрузить данные")
        return
    
    # 3. Создание визуализаций
    viz_dir = create_visualizations(df, version_id)
    
    # 4. Итоговый отчет
    print("\n" + "=" * 50)
    print("🎉 ВИЗУАЛИЗАЦИЯ ЗАВЕРШЕНА!")
    print(f"📁 Файлы сохранены в: {viz_dir}")
    print(f"📊 Симуляция: {len(df):,} записей, {df['aircraft_number'].nunique()} планеров, {df['dates'].nunique()} дней")
    print(f"✈️ Типы ВС: {dict(df['ac_type'].value_counts())}")
    print(f"🔧 Статусы: {dict(df['status_name'].value_counts())}")
    print(f"⏱️ Период: {df['dates'].min().strftime('%Y-%m-%d')} → {df['dates'].max().strftime('%Y-%m-%d')}")
    print("=" * 50)

if __name__ == "__main__":
    main() 