#!/usr/bin/env python3
"""
Тест сопоставления данных между status_overhaul и heli_pandas
Проверяем сколько из 7 вертолетов в ремонте найдем в pandas
"""

import pandas as pd
import sys
from pathlib import Path

sys.path.append('code')
from utils.config_loader import get_clickhouse_client

def test_overhaul_matching():
    """Тестирует сопоставление данных статусов"""
    
    client = get_clickhouse_client()
    
    print('🔍 === ТЕСТ СОПОСТАВЛЕНИЯ OVERHAUL ↔ PANDAS ===')
    
    # 1. Загружаем данные status_overhaul (status != 'Закрыто')
    print('\n📋 1. Загружаем ВС в капремонте:')
    
    overhaul_query = """
    SELECT ac_registr, status, act_start_date, sched_end_date
    FROM status_overhaul 
    WHERE status != 'Закрыто'
    ORDER BY ac_registr
    """
    
    overhaul_result = client.execute(overhaul_query)
    if not overhaul_result:
        print("❌ Нет ВС в капремонте")
        return
    
    overhaul_df = pd.DataFrame(overhaul_result, columns=['ac_registr', 'status', 'act_start_date', 'sched_end_date'])
    print(f"✅ Найдено {len(overhaul_df)} ВС в капремонте:")
    
    overhaul_numbers = []
    for _, row in overhaul_df.iterrows():
        ac_registr = str(row['ac_registr'])
        overhaul_numbers.append(ac_registr)
        print(f"   {ac_registr}: {row['status']}")
    
    print(f"📋 Ищем эти номера в heli_pandas: {overhaul_numbers}")
    
    # 2. Проверяем наличие этих номеров в heli_pandas
    print('\n🔍 2. Проверяем наличие в heli_pandas:')
    
    # Создаем запрос для поиска этих serialno в heli_pandas
    numbers_str = "', '".join(overhaul_numbers)
    
    pandas_query = f"""
    SELECT serialno, partno, ac_typ, location, status
    FROM heli_pandas 
    WHERE serialno IN ('{numbers_str}')
    ORDER BY serialno
    """
    
    pandas_result = client.execute(pandas_query)
    
    if pandas_result:
        pandas_df = pd.DataFrame(pandas_result, columns=['serialno', 'partno', 'ac_typ', 'location', 'status'])
        print(f"✅ Найдено {len(pandas_df)} совпадений в heli_pandas:")
        
        # Группируем по serialno для анализа
        matches = set()
        for _, row in pandas_df.iterrows():
            serialno = str(row['serialno'])
            matches.add(serialno)
            print(f"   serialno={serialno}, partno={row['partno']}, ac_typ={row['ac_typ']}, статус={row['status']}")
        
        print(f"\n📊 Статистика совпадений:")
        print(f"   ВС в капремонте: {len(overhaul_numbers)}")
        print(f"   Найдено в heli_pandas: {len(matches)}")
        print(f"   Процент совпадений: {len(matches)/len(overhaul_numbers)*100:.1f}%")
        
        # Показываем какие НЕ найдены
        not_found = set(overhaul_numbers) - matches
        if not_found:
            print(f"\n❌ НЕ НАЙДЕНЫ в heli_pandas ({len(not_found)} ВС):")
            for missing in sorted(not_found):
                overhaul_status = overhaul_df[overhaul_df['ac_registr'].astype(str) == missing]['status'].iloc[0]
                print(f"   {missing}: {overhaul_status}")
            
            print(f"\n💡 Возможные причины:")
            print(f"   1. Планеры этих ВС не попали в фильтр MD_Components")
            print(f"   2. Номера serialno не совпадают с ac_registr")
            print(f"   3. Дефекты данных в исходных файлах")
        else:
            print(f"\n✅ ВСЕ ВС В КАПРЕМОНТЕ НАЙДЕНЫ В HELI_PANDAS!")
    
    else:
        print("❌ НИ ОДНОГО совпадения не найдено в heli_pandas")
        print("💡 Возможные причины:")
        print("   1. Планеры ВС не попали в фильтр MD_Components")
        print("   2. Номера serialno кардинально отличаются от ac_registr")
    
    # 3. Дополнительная проверка - ищем похожие номера
    print('\n🔍 3. Дополнительная проверка - поиск похожих номеров:')
    
    # Загружаем все уникальные serialno из heli_pandas для анализа
    all_serialno_query = """
    SELECT DISTINCT serialno
    FROM heli_pandas 
    WHERE serialno IS NOT NULL
    ORDER BY serialno
    """
    
    all_serialno_result = client.execute(all_serialno_query)
    if all_serialno_result:
        all_serialno = [str(row[0]) for row in all_serialno_result]
        print(f"📋 Всего уникальных serialno в heli_pandas: {len(all_serialno)}")
        
        # Ищем частичные совпадения
        for overhaul_num in overhaul_numbers:
            similar = []
            for serialno in all_serialno:
                if overhaul_num in serialno or serialno in overhaul_num:
                    similar.append(serialno)
            
            if similar:
                print(f"🔍 Для {overhaul_num} похожие serialno: {similar[:5]}")  # Показываем первые 5
            else:
                print(f"❌ Для {overhaul_num} похожих serialno не найдено")

if __name__ == "__main__":
    test_overhaul_matching() 