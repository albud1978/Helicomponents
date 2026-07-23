#!/usr/bin/env python3
"""
Процессор неактивности планеров (legacy, не в cascade с 2026-07-24).

Ранее: шаг [3] cascade — 0→1 для хвоста планеров; заменён merge в
inactive_serviceable_classifier (3b, вход status=0). Модуль сохранён для
совместимости; вызов из dwh_post_enrichment / dual_loader удалён.

Функционал:
- Устанавливает статус 1 (неактивно) для планеров со статусом 0
- Работает только с конкретными partno планеров
- Обрабатывает данные в памяти (pandas DataFrame)

Планеры: МИ-8Т, МИ-8П, МИ-8ПС, МИ-8ТП, МИ-8АМТ, МИ-8МТВ
"""

import pandas as pd
from datetime import datetime

def process_inactive_planery_status(pandas_df, client):
    """
    Обрабатывает статусы неактивности планеров в DataFrame
    
    Args:
        pandas_df: DataFrame с данными heli_pandas
        client: Клиент ClickHouse (не используется, но передается для совместимости)
    
    Returns:
        DataFrame с обновленными статусами
    """
    try:
        print("🔍 Обработка статусов неактивности планеров...")
        
        # Список планеров со скрина
        planery_partno = ['МИ-8Т', 'МИ-8П', 'МИ-8ПС', 'МИ-8ТП', 'МИ-8АМТ', 'МИ-8МТВ']
        
        # Проверяем наличие колонки status_id
        if 'status_id' not in pandas_df.columns:
            print("⚠️ Колонка 'status_id' отсутствует - добавляем со значением 0")
            pandas_df['status_id'] = 0
        
        # Фильтруем данные по планерам
        planery_mask = pandas_df['partno'].isin(planery_partno)
        planery_df = pandas_df[planery_mask]
        
        print(f"📋 Найдено {len(planery_df)} записей планеров")
        
        if len(planery_df) == 0:
            print("⚠️ Записи планеров не найдены")
            return pandas_df
        
        # Анализируем статусы планеров ДО обработки
        print("📊 Статусы планеров ДО обработки:")
        status_counts_before = planery_df['status_id'].value_counts().sort_index()
        for status, count in status_counts_before.items():
            print(f"  Статус {status}: {count} записей")
        
        # Находим планеры со статусом 0 (не обработанные предыдущими процессорами)
        inactive_mask = planery_mask & (pandas_df['status_id'] == 0)
        inactive_count = inactive_mask.sum()
        
        if inactive_count == 0:
            print("✅ Все планеры уже имеют статус (обработаны предыдущими процессорами)")
            return pandas_df
        
        print(f"🎯 Найдено {inactive_count} планеров со статусом 0 → устанавливаем статус 1 (неактивно)")
        
        # Устанавливаем статус 1 (неактивно) для планеров со статусом 0
        pandas_df.loc[inactive_mask, 'status_id'] = 1
        
        # Анализируем статусы планеров ПОСЛЕ обработки
        planery_df_after = pandas_df[planery_mask]
        print("📊 Статусы планеров ПОСЛЕ обработки:")
        status_counts_after = planery_df_after['status_id'].value_counts().sort_index()
        for status, count in status_counts_after.items():
            print(f"  Статус {status}: {count} записей")
        
        # Статистика по partno
        print("📋 Статистика по типам планеров:")
        for partno in planery_partno:
            partno_mask = pandas_df['partno'] == partno
            partno_count = partno_mask.sum()
            if partno_count > 0:
                partno_inactive = ((pandas_df['partno'] == partno) & (pandas_df['status_id'] == 1)).sum()
                print(f"  {partno}: {partno_count} записей, {partno_inactive} неактивных")
        
        print(f"✅ Процессор неактивности планеров: обработано {inactive_count} записей")
        
        return pandas_df
        
    except Exception as e:
        print(f"❌ Ошибка обработки неактивности планеров: {e}")
        return pandas_df

if __name__ == "__main__":
    print("🧪 Тестирование процессора неактивности планеров")
    print("💡 Этот модуль используется в dual_loader.py")
    print("💡 Для тестирования запустите: python3 code/extract/dual_loader.py")