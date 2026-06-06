#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (code rudiments cleanup): orphan, не импортируется живым кодом (sim_v2/extract/validation).
"""
Оптимизация структуры flight_program_ac
=====================================

ПРОБЛЕМЫ ТЕКУЩЕЙ СТРУКТУРЫ:
1. field_name избыточно - лучше отдельные колонки
2. daily_value избыточно - каждое поле имеет свое ежедневное значение  
3. flight_date -> dates (корректнее именование)
4. ac_type_mask можно удалить (избыточно)

ПРЕДЛАГАЕМАЯ ОПТИМИЗИРОВАННАЯ СТРУКТУРА:
- dates (вместо flight_date)
- ops_counter_mi8 
- ops_counter_mi17
- ops_counter_total
- new_counter_mi17
- trigger_program_mi8
- trigger_program_mi17  
- trigger_program
- version_date
- version_id

ПРЕИМУЩЕСТВА:
- Убираем pivot-структуру с field_name
- Прямые колонки для каждого показателя
- Проще запросы и анализ
- Меньше избыточности данных

Автор: AI Assistant  
Дата: 2025-07-19
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.append(str(Path(__file__).parent.parent))
from config_loader import get_clickhouse_client

def analyze_current_structure():
    """Анализ текущей структуры и данных"""
    
    print("🔍 === АНАЛИЗ ТЕКУЩЕЙ СТРУКТУРЫ flight_program_ac ===")
    
    client = get_clickhouse_client()
    
    # Структура таблицы
    desc = client.execute('DESCRIBE flight_program_ac')
    print("\n📋 ТЕКУЩАЯ СТРУКТУРА:")
    for field_name, field_type, default, comment, *_ in desc:
        print(f"   {field_name:<15} {field_type:<15} {default:<10}")
    
    # Статистика по field_name
    stats = client.execute('''
        SELECT 
            field_name,
            COUNT(*) as records,
            MIN(flight_date) as min_date,
            MAX(flight_date) as max_date,
            AVG(daily_value) as avg_value,
            SUM(CASE WHEN daily_value > 0 THEN 1 ELSE 0 END) as non_zero
        FROM flight_program_ac 
        GROUP BY field_name 
        ORDER BY field_name
    ''')
    
    print(f"\n📊 СТАТИСТИКА ПО ПОЛЯМ:")
    print(f"{'Поле':<25} {'Записей':<10} {'Период':<20} {'Средн.знач.':<12} {'Ненулевых':<10}")
    print("-" * 80)
    for field_name, records, min_date, max_date, avg_value, non_zero in stats:
        period = f"{min_date} - {max_date}"
        print(f"{field_name:<25} {records:<10,} {period:<20} {avg_value:<12.2f} {non_zero:<10,}")
    
    # Уникальные ac_type_mask
    ac_types = client.execute('SELECT DISTINCT ac_type_mask FROM flight_program_ac ORDER BY ac_type_mask')
    print(f"\n🏷️ ТИПЫ ВС (ac_type_mask): {[row[0] for row in ac_types]}")
    
    # Размер данных
    total_size = client.execute('SELECT COUNT(*) FROM flight_program_ac')[0][0]
    print(f"\n💾 ОБЩИЙ РАЗМЕР: {total_size:,} записей")
    
    return stats

def propose_optimized_structure():
    """Предложение оптимизированной структуры"""
    
    print("\n💡 === ПРЕДЛАГАЕМАЯ ОПТИМИЗИРОВАННАЯ СТРУКТУРА ===")
    
    optimized_structure = {
        'dates': 'Date',
        'ops_counter_mi8': 'Float32',
        'ops_counter_mi17': 'Float32', 
        'ops_counter_total': 'Float32',
        'new_counter_mi17': 'Float32',
        'trigger_program_mi8': 'Float32',
        'trigger_program_mi17': 'Float32',
        'trigger_program': 'Float32',
        'version_date': 'Date DEFAULT today()',
        'version_id': 'UInt8 DEFAULT 1'
    }
    
    print("\n📋 НОВАЯ СТРУКТУРА:")
    for field, type_def in optimized_structure.items():
        print(f"   {field:<20} {type_def}")
    
    return optimized_structure

def calculate_optimization_benefits():
    """Расчет преимуществ оптимизации"""
    
    print("\n📈 === ПРЕИМУЩЕСТВА ОПТИМИЗАЦИИ ===")
    
    client = get_clickhouse_client()
    
    # Текущий размер данных
    current_records = client.execute('SELECT COUNT(*) FROM flight_program_ac')[0][0]
    unique_dates = client.execute('SELECT COUNT(DISTINCT flight_date) FROM flight_program_ac')[0][0]
    unique_fields = client.execute('SELECT COUNT(DISTINCT field_name) FROM flight_program_ac')[0][0]
    
    # Предполагаемый размер после оптимизации
    optimized_records = unique_dates  # Одна запись на дату
    
    print(f"📊 СРАВНЕНИЕ РАЗМЕРОВ:")
    print(f"   Текущий размер:     {current_records:,} записей")
    print(f"   Оптимизированный:   {optimized_records:,} записей")
    print(f"   Сжатие в:           {current_records / optimized_records:.1f} раз")
    print(f"   Экономия места:     {((current_records - optimized_records) / current_records * 100):.1f}%")
    
    print(f"\n✅ УЛУЧШЕНИЯ:")
    print(f"   • Устранение pivot-структуры с field_name")
    print(f"   • Прямые колонки для каждого показателя")
    print(f"   • Упрощение запросов и JOIN'ов")
    print(f"   • Лучшая производительность индексов")
    print(f"   • Убрана избыточность ac_type_mask")
    print(f"   • Корректное именование: dates вместо flight_date")

def generate_migration_sql():
    """Генерация SQL для миграции"""
    
    print("\n🔧 === SQL ДЛЯ МИГРАЦИИ ===")
    
    # Создание новой таблицы
    create_sql = """
-- Создание оптимизированной таблицы
DROP TABLE IF EXISTS flight_program_ac_optimized;

CREATE TABLE flight_program_ac_optimized (
    dates Date,
    ops_counter_mi8 Float32,
    ops_counter_mi17 Float32,
    ops_counter_total Float32,
    new_counter_mi17 Float32,
    trigger_program_mi8 Float32,
    trigger_program_mi17 Float32,
    trigger_program Float32,
    version_date Date DEFAULT today(),
    version_id UInt8 DEFAULT 1
) ENGINE = MergeTree()
ORDER BY dates
SETTINGS index_granularity = 8192;
"""
    
    # Миграция данных
    migration_sql = """
-- Миграция данных из старой структуры в новую
INSERT INTO flight_program_ac_optimized (
    dates, ops_counter_mi8, ops_counter_mi17, ops_counter_total, 
    new_counter_mi17, trigger_program_mi8, trigger_program_mi17, 
    trigger_program, version_date, version_id
)
SELECT 
    flight_date as dates,
    
    -- ops_counter поля
    MAX(CASE WHEN field_name = 'ops_counter_mi8' THEN daily_value ELSE 0 END) as ops_counter_mi8,
    MAX(CASE WHEN field_name = 'ops_counter_mi17' THEN daily_value ELSE 0 END) as ops_counter_mi17,
    MAX(CASE WHEN field_name = 'ops_counter_total' THEN daily_value ELSE 0 END) as ops_counter_total,
    
    -- new_counter поля  
    MAX(CASE WHEN field_name = 'new_counter_mi17' THEN daily_value ELSE 0 END) as new_counter_mi17,
    
    -- trigger_program поля
    MAX(CASE WHEN field_name = 'trigger_program_mi8' THEN daily_value ELSE 0 END) as trigger_program_mi8,
    MAX(CASE WHEN field_name = 'trigger_program_mi17' THEN daily_value ELSE 0 END) as trigger_program_mi17,
    MAX(CASE WHEN field_name = 'trigger_program' THEN daily_value ELSE 0 END) as trigger_program,
    
    -- Метаданные
    MAX(version_date) as version_date,
    MAX(version_id) as version_id

FROM flight_program_ac
GROUP BY flight_date
ORDER BY flight_date;
"""
    
    # Переименование таблиц
    rename_sql = """
-- Переименование таблиц
DROP TABLE flight_program_ac;
RENAME TABLE flight_program_ac_optimized TO flight_program_ac;
"""
    
    print("1️⃣ СОЗДАНИЕ НОВОЙ ТАБЛИЦЫ:")
    print(create_sql)
    
    print("\n2️⃣ МИГРАЦИЯ ДАННЫХ:")
    print(migration_sql)
    
    print("\n3️⃣ ПЕРЕИМЕНОВАНИЕ:")
    print(rename_sql)
    
    return create_sql, migration_sql, rename_sql

def create_migration_script():
    """Создание скрипта миграции"""
    
    print("\n💾 === СОЗДАНИЕ СКРИПТА МИГРАЦИИ ===")
    
    create_sql, migration_sql, rename_sql = generate_migration_sql()
    
    migration_script = f'''#!/usr/bin/env python3
"""
Скрипт миграции flight_program_ac к оптимизированной структуре
===========================================================

Выполняет оптимизацию согласно предложениям:
- flight_date -> dates
- Убирает field_name pivot-структуру  
- Прямые колонки для каждого показателя
- Убирает избыточные поля

Автор: AI Assistant
Дата: 2025-07-19
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent / 'utils'))
from config_loader import get_clickhouse_client

def migrate_flight_program_ac():
    """Выполнение миграции"""
    
    print("🚀 === МИГРАЦИЯ flight_program_ac ===")
    
    client = get_clickhouse_client()
    
    try:
        # 1. Создание новой таблицы
        print("1️⃣ Создание оптимизированной таблицы...")
        
        create_sql = """{create_sql}"""
        
        client.execute(create_sql)
        print("✅ Новая таблица создана")
        
        # 2. Миграция данных
        print("2️⃣ Миграция данных...")
        
        migration_sql = """{migration_sql}"""
        
        client.execute(migration_sql)
        
        # Проверка результата
        old_count = client.execute("SELECT COUNT(*) FROM flight_program_ac")[0][0]
        new_count = client.execute("SELECT COUNT(*) FROM flight_program_ac_optimized")[0][0]
        
        print(f"📊 Старая таблица: {{old_count:,}} записей")
        print(f"📊 Новая таблица: {{new_count:,}} записей")
        print(f"📈 Сжатие в: {{old_count / new_count:.1f}} раз")
        
        # 3. Переименование
        print("3️⃣ Переименование таблиц...")
        
        rename_sql = """{rename_sql}"""
        
        client.execute(rename_sql)
        print("✅ Таблицы переименованы")
        
        print("🎉 МИГРАЦИЯ ЗАВЕРШЕНА УСПЕШНО!")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка миграции: {{e}}")
        return False

if __name__ == "__main__":
    migrate_flight_program_ac()
'''
    
    # Сохраняем скрипт
    script_path = "code/migrate_flight_program_ac.py"
    with open(script_path, 'w', encoding='utf-8') as f:
        f.write(migration_script)
    
    print(f"✅ Скрипт миграции сохранен: {script_path}")

def main():
    """Основная функция анализа"""
    
    print("🎯 === АНАЛИЗ ОПТИМИЗАЦИИ flight_program_ac ===")
    print("Цель: упрощение избыточной pivot-структуры")
    
    # Анализ текущего состояния
    current_stats = analyze_current_structure()
    
    # Предложение новой структуры
    optimized_structure = propose_optimized_structure()
    
    # Расчет преимуществ
    calculate_optimization_benefits()
    
    # Генерация SQL
    generate_migration_sql()
    
    # Создание скрипта
    create_migration_script()
    
    print("\n✅ АНАЛИЗ ЗАВЕРШЕН")
    print("📁 Следующие шаги:")
    print("   1. Проверить предложенную структуру")
    print("   2. Запустить: python3 code/migrate_flight_program_ac.py")
    print("   3. Обновить загрузчики для новой структуры")

if __name__ == "__main__":
    main() 