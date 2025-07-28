#!/usr/bin/env python3
"""
Анализ расхождений между heli_pandas и dict_serialno_flat
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.config_loader import get_clickhouse_client

def analyze_serialno_discrepancy():
    """Детальный анализ причин расхождения между heli_pandas и dict_serialno_flat"""
    
    print("🔍 АНАЛИЗ РАСХОЖДЕНИЙ dict_serialno_flat vs heli_pandas")
    print("=" * 60)
    
    try:
        client = get_clickhouse_client()
        
        # Основная статистика
        print("📊 ОСНОВНАЯ СТАТИСТИКА:")
        print("-" * 25)
        
        # Количество записей в heli_pandas
        heli_count = client.execute('SELECT COUNT(*) FROM heli_pandas')[0][0]
        print(f'📊 heli_pandas общее: {heli_count:,} записей')
        
        # Количество уникальных serialno в heli_pandas
        unique_serialno = client.execute('SELECT COUNT(DISTINCT serialno) FROM heli_pandas')[0][0]
        print(f'🎯 Уникальных serialno: {unique_serialno:,}')
        
        # Количество записей в dict_serialno_flat
        dict_count = client.execute('SELECT COUNT(*) FROM dict_serialno_flat')[0][0]
        print(f'📋 dict_serialno_flat: {dict_count:,} записей')
        
        # Разница
        diff = unique_serialno - dict_count
        print(f'❌ РАЗНИЦА: {diff:,} записей')
        
        print("\n🔍 АНАЛИЗ NULL И ПУСТЫХ ЗНАЧЕНИЙ:")
        print("-" * 35)
        
        # NULL serialno
        null_serialno = client.execute('SELECT COUNT(*) FROM heli_pandas WHERE serialno IS NULL')[0][0]
        print(f'🚫 NULL serialno: {null_serialno:,}')
        
        # Пустые serialno
        empty_serialno = client.execute("SELECT COUNT(*) FROM heli_pandas WHERE serialno = ''")[0][0]
        print(f'🚫 Пустые serialno: {empty_serialno:,}')
        
        # NULL psn
        null_psn = client.execute('SELECT COUNT(*) FROM heli_pandas WHERE psn IS NULL')[0][0]
        print(f'🚫 NULL psn: {null_psn:,}')
        
        print("\n🔍 ФИЛЬТРАЦИЯ КАК В DICTIONARY_CREATOR:")
        print("-" * 40)
        
        # Применяем тот же фильтр что в dictionary_creator.py
        filtered_query = """
            SELECT COUNT(DISTINCT serialno) 
            FROM heli_pandas 
            WHERE serialno IS NOT NULL AND serialno != '' AND psn IS NOT NULL
        """
        filtered_count = client.execute(filtered_query)[0][0]
        print(f'✅ После фильтрации dictionary_creator: {filtered_count:,}')
        
        # Сравниваем с dict_serialno_flat
        filter_diff = filtered_count - dict_count
        print(f'❌ Разница после фильтрации: {filter_diff:,}')
        
        print("\n🔍 ДЕТАЛИЗАЦИЯ ПОТЕРЯННЫХ ЗАПИСЕЙ:")
        print("-" * 35)
        
        # Записи с serialno но без psn
        no_psn_query = """
            SELECT COUNT(*) 
            FROM heli_pandas 
            WHERE serialno IS NOT NULL AND serialno != '' AND psn IS NULL
        """
        no_psn = client.execute(no_psn_query)[0][0]
        print(f'⚠️ Записи с serialno, но БЕЗ psn: {no_psn:,}')
        
        # Записи с psn но без serialno
        no_serialno_query = """
            SELECT COUNT(*) 
            FROM heli_pandas 
            WHERE psn IS NOT NULL AND (serialno IS NULL OR serialno = '')
        """
        no_serialno = client.execute(no_serialno_query)[0][0]
        print(f'⚠️ Записи с psn, но БЕЗ serialno: {no_serialno:,}')
        
        print("\n🔍 ПРИМЕРЫ ПОТЕРЯННЫХ ЗАПИСЕЙ:")
        print("-" * 30)
        
        if no_psn > 0:
            print(f"📋 Примеры serialno БЕЗ psn:")
            no_psn_examples = client.execute("""
                SELECT serialno, partno, ac_typ 
                FROM heli_pandas 
                WHERE serialno IS NOT NULL AND serialno != '' AND psn IS NULL
                LIMIT 5
            """)
            for i, (serialno, partno, ac_typ) in enumerate(no_psn_examples, 1):
                print(f"   {i}. {serialno} | {partno} | {ac_typ}")
        
        if no_serialno > 0:
            print(f"\n📋 Примеры psn БЕЗ serialno:")
            no_serialno_examples = client.execute("""
                SELECT psn, partno, ac_typ 
                FROM heli_pandas 
                WHERE psn IS NOT NULL AND (serialno IS NULL OR serialno = '')
                LIMIT 5
            """)
            for i, (psn, partno, ac_typ) in enumerate(no_serialno_examples, 1):
                print(f"   {i}. PSN:{psn} | {partno} | {ac_typ}")
        
        print("\n🎯 ВЫВОДЫ:")
        print("-" * 10)
        print(f"1. В heli_pandas {heli_count:,} записей, {unique_serialno:,} уникальных serialno")
        print(f"2. В dict_serialno_flat {dict_count:,} записей")
        print(f"3. Фильтр dictionary_creator исключает {no_psn:,} записей без psn")
        print(f"4. Итоговая разница: {filter_diff:,} записей")
        
        if filter_diff > 0:
            print(f"\n❌ ПРОБЛЕМА: После фильтрации все еще есть разница в {filter_diff:,} записей!")
            print("💡 Возможные причины:")
            print("   - Аддитивный словарь не обновляется при повторных загрузках")
            print("   - Версионность создает дубликаты в словаре")
            print("   - Проблемы с логикой создания словаря")
        else:
            print("✅ Фильтрация объясняет расхождение полностью")
            
    except Exception as e:
        print(f"❌ Ошибка анализа: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_serialno_discrepancy() 