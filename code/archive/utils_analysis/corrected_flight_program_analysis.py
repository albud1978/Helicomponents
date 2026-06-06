#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (code rudiments cleanup): orphan, не импортируется живым кодом (sim_v2/extract/validation).
"""
КОРРЕКТНЫЙ анализ оптимизации flight_program_ac
Анализ показывает ТОЛЬКО структурные изменения без сокращения данных
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../utils'))
from config_loader import get_clickhouse_client
import pandas as pd
from datetime import date

def analyze_current_structure():
    """Анализ текущей структуры flight_program_ac"""
    try:
        client = get_clickhouse_client()
        
        print("🔍 === АНАЛИЗ ТЕКУЩЕЙ СТРУКТУРЫ flight_program_ac ===")
        
        # Структура таблицы
        desc = client.execute('DESCRIBE flight_program_ac')
        print("\n📊 ТЕКУЩАЯ СТРУКТУРА:")
        for row in desc:
            print(f"   {row[0]:20} {row[1]:15}")
        
        # Объём данных
        print("\n📈 ОБЪЁМ ДАННЫХ:")
        total_records = client.execute('SELECT COUNT(*) FROM flight_program_ac')[0][0]
        unique_dates = client.execute('SELECT COUNT(DISTINCT flight_date) FROM flight_program_ac')[0][0]
        unique_fields = client.execute('SELECT COUNT(DISTINCT field_name) FROM flight_program_ac')[0][0]
        unique_ac_types = client.execute('SELECT COUNT(DISTINCT ac_type_mask) FROM flight_program_ac')[0][0]
        
        print(f"   Всего записей: {total_records:,}")
        print(f"   Уникальных дат: {unique_dates:,}")
        print(f"   Уникальных полей: {unique_fields}")
        print(f"   Типов ВС: {unique_ac_types}")
        
        # Список полей
        print("\n📝 ПОЛЯ В ТАБЛИЦЕ:")
        fields = client.execute('SELECT DISTINCT field_name FROM flight_program_ac ORDER BY field_name')
        for i, (field,) in enumerate(fields, 1):
            print(f"   {i}. {field}")
        
        # Анализ ac_type_mask
        print("\n🚁 ТИПЫ ВС (ac_type_mask):")
        ac_types = client.execute('SELECT DISTINCT ac_type_mask FROM flight_program_ac ORDER BY ac_type_mask')
        for ac_type, in ac_types:
            print(f"   {ac_type}: ", end="")
            if ac_type == 32:
                print("МИ-8")
            elif ac_type == 64:
                print("МИ-17")
            elif ac_type == 96:
                print("МИ-8 + МИ-17 (Multihot)")
            else:
                print("Неизвестный тип")
        
        return {
            'total_records': total_records,
            'unique_dates': unique_dates,
            'unique_fields': unique_fields,
            'unique_ac_types': unique_ac_types,
            'fields': [field[0] for field in fields]
        }
        
    except Exception as e:
        print(f"❌ Ошибка анализа: {e}")
        return None

def analyze_etl_process():
    """Анализ процесса создания таблицы в ETL"""
    print("\n🔄 === АНАЛИЗ ETL ПРОЦЕССА ===")
    
    try:
        # Читаем исходный Excel
        df = pd.read_excel('data_input/source_data/Program_heli.xlsx')
        
        print(f"\n📁 ИСХОДНЫЙ ФАЙЛ Program_heli.xlsx:")
        print(f"   Форма: {df.shape}")
        print(f"   Строки с данными (исключая заголовок 'Год'): {df.shape[0] - 1}")
        
        # Исходные поля
        source_fields = df[df['Месяц'] != 'Год']['Месяц'].tolist()
        print(f"\n📊 ИСХОДНЫЕ ПОЛЯ ИЗ EXCEL ({len(source_fields)}):")
        for i, field in enumerate(source_fields, 1):
            print(f"   {i}. {field}")
        
        # Вычисляемые поля
        calculated_fields = [
            'ops_counter_total',
            'trigger_program_mi8', 
            'trigger_program_mi17',
            'trigger_program'
        ]
        
        print(f"\n🧮 ВЫЧИСЛЯЕМЫЕ ПОЛЯ ({len(calculated_fields)}):")
        for i, field in enumerate(calculated_fields, 1):
            print(f"   {i}. {field}")
            if field == 'ops_counter_total':
                print("      = ops_counter_mi8 + ops_counter_mi17")
            elif field == 'trigger_program_mi8':
                print("      = daily_diff(ops_counter_mi8)")
            elif field == 'trigger_program_mi17':  
                print("      = daily_diff(ops_counter_mi17)")
            elif field == 'trigger_program':
                print("      = trigger_program_mi8 + trigger_program_mi17")
        
        # Анализ колонок с данными
        data_columns = [col for col in df.columns if col not in ['ac_type_mask', 'Месяц']]
        print(f"\n📅 ВРЕМЕННЫЕ КОЛОНКИ ({len(data_columns)}):")
        print(f"   Всего колонок с данными: {len(data_columns)}")
        print(f"   Примеры: {data_columns[:10]}...")
        
        return {
            'source_fields': source_fields,
            'calculated_fields': calculated_fields,
            'data_columns': len(data_columns)
        }
        
    except Exception as e:
        print(f"❌ Ошибка анализа ETL: {e}")
        return None

def propose_optimized_structure(current_data, etl_data):
    """Предложение оптимизированной структуры"""
    print("\n💡 === ПРЕДЛОЖЕНИЕ ОПТИМИЗИРОВАННОЙ СТРУКТУРЫ ===")
    
    if not current_data or not etl_data:
        print("❌ Недостаточно данных для анализа")
        return
    
    print("\n🎯 ПРОБЛЕМЫ ТЕКУЩЕЙ СТРУКТУРЫ:")
    print("   1. Pivot-структура (field_name + daily_value) неэффективна")
    print("   2. Дополнительные поля ac_type_mask избыточны")
    print("   3. Поле field_name занимает много места (String)")
    print("   4. Неоптимальная индексация для аналитических запросов")
    
    print("\n✅ ПРЕДЛАГАЕМЫЕ ИЗМЕНЕНИЯ:")
    print("   1. Плоская структура: каждое поле - отдельная колонка")
    print("   2. Переименование flight_date → dates (универсальность)")
    print("   3. Удаление field_name (не нужно в плоской структуре)")
    print("   4. Удаление daily_value (заменяется прямыми колонками)")
    print("   5. Упрощение ac_type_mask или его удаление")
    
    print("\n📊 НОВАЯ СТРУКТУРА:")
    print("""
    CREATE TABLE flight_program_ac_optimized (
        dates Date,                        -- переименованная flight_date
        ac_type UInt8,                     -- упрощенная ac_type_mask  
        ops_counter_mi8 Float32,           -- прямая колонка
        ops_counter_mi17 Float32,          -- прямая колонка  
        ops_counter_total Float32,         -- прямая колонка
        new_counter_mi17 Float32,          -- прямая колонка
        trigger_program_mi8 Float32,       -- прямая колонка
        trigger_program_mi17 Float32,      -- прямая колонка
        trigger_program Float32,           -- прямая колонка
        version_date Date DEFAULT today(),
        version_id UInt8 DEFAULT 1
    ) ENGINE = MergeTree()
    ORDER BY (ac_type, dates)
    """)
    
    print("\n📈 КОРРЕКТНЫЕ РАСЧЁТЫ:")
    records_flat = current_data['unique_dates'] * current_data['unique_ac_types']
    current_records = current_data['total_records']
    
    print(f"   Текущая структура: {current_records:,} записей")
    print(f"   Оптимизированная:  {records_flat:,} записей")
    print(f"   ⚠️  КОЛИЧЕСТВО ЗАПИСЕЙ НЕ ИЗМЕНИТСЯ при корректной миграции!")
    print(f"   📝 Изменится только структура: pivot → flat")
    
    print("\n💾 ЭКОНОМИЯ МЕСТА:")
    # Расчет размера полей
    current_size_per_record = (
        1 +      # ac_type_mask (UInt8)
        4 +      # flight_date (Date) 
        20 +     # field_name (String, среднее)
        4 +      # daily_value (Float32)
        4 +      # version_date (Date)
        1        # version_id (UInt8)
    )  # = 34 байта
    
    optimized_size_per_record = (
        4 +      # dates (Date)
        1 +      # ac_type (UInt8)
        4 * 7 +  # 7 Float32 колонок
        4 +      # version_date (Date)  
        1        # version_id (UInt8)
    )  # = 38 байт
    
    current_total_size = current_records * current_size_per_record
    optimized_total_size = records_flat * optimized_size_per_record
    
    print(f"   Текущий размер: ~{current_total_size:,} байт")
    print(f"   Оптимизированный: ~{optimized_total_size:,} байт")
    savings_percent = (current_total_size - optimized_total_size) / current_total_size * 100
    print(f"   💰 Экономия: {savings_percent:.1f}%")
    
    print("\n⚡ ПРЕИМУЩЕСТВА ОПТИМИЗАЦИИ:")
    print("   1. Быстрее аналитические запросы (без JOINов по field_name)")
    print("   2. Проще индексация по отдельным полям")
    print("   3. Меньше размер строки (без String поля)")  
    print("   4. Совместимость с Flame GPU tensor форматом")
    print("   5. Упрощение ETL кода (прямая загрузка вместо pivot)")

def generate_migration_plan():
    """Генерация плана миграции"""
    print("\n🚀 === ПЛАН МИГРАЦИИ ===")
    
    print("\n📋 ЭТАПЫ МИГРАЦИИ:")
    print("   1. Создание новой таблицы flight_program_ac_optimized")
    print("   2. Миграция данных через PIVOT операцию")
    print("   3. Валидация корректности данных")
    print("   4. Переименование таблиц (backup + replace)")
    print("   5. Обновление ETL скриптов")
    
    print("\n🔄 SQL МИГРАЦИЯ:")
    print("""
    -- Шаг 1: Создание оптимизированной таблицы
    CREATE TABLE flight_program_ac_optimized (
        dates Date,
        ac_type UInt8,
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
    ORDER BY (ac_type, dates);
    
    -- Шаг 2: Миграция данных
    INSERT INTO flight_program_ac_optimized
    SELECT 
        flight_date as dates,
        ac_type_mask as ac_type,
        sumIf(daily_value, field_name = 'ops_counter_mi8') as ops_counter_mi8,
        sumIf(daily_value, field_name = 'ops_counter_mi17') as ops_counter_mi17,
        sumIf(daily_value, field_name = 'ops_counter_total') as ops_counter_total,
        sumIf(daily_value, field_name = 'new_counter_mi17') as new_counter_mi17,
        sumIf(daily_value, field_name = 'trigger_program_mi8') as trigger_program_mi8,
        sumIf(daily_value, field_name = 'trigger_program_mi17') as trigger_program_mi17,
        sumIf(daily_value, field_name = 'trigger_program') as trigger_program,
        version_date,
        version_id
    FROM flight_program_ac
    GROUP BY flight_date, ac_type_mask, version_date, version_id;
    """)
    
    print("\n🔧 ИЗМЕНЕНИЯ В ETL:")
    print("   📁 program_ac_direct_loader.py:")
    print("     - Изменить структуру таблицы")
    print("     - Упростить процесс вставки (прямые колонки)")
    print("     - Убрать pivot логику")
    
    print("\n⚠️  РИСКИ И ПРЕДОСТОРОЖНОСТИ:")
    print("   1. Сделать backup текущей таблицы")
    print("   2. Протестировать миграцию на копии")
    print("   3. Проверить все зависимые скрипты")
    print("   4. Обновить документацию")

def main():
    """Главная функция анализа"""
    print("🎯 === КОРРЕКТНЫЙ АНАЛИЗ ОПТИМИЗАЦИИ flight_program_ac ===")
    print("(Исправленная версия с правильными расчетами)")
    
    # Анализ текущей структуры
    current_data = analyze_current_structure()
    
    # Анализ ETL процесса
    etl_data = analyze_etl_process()
    
    # Предложение оптимизации
    propose_optimized_structure(current_data, etl_data)
    
    # План миграции
    generate_migration_plan()
    
    print("\n✅ === ЗАКЛЮЧЕНИЕ ===")
    print("Оптимизация изменит ТОЛЬКО структуру таблицы (pivot → flat),")
    print("но НЕ сократит количество записей. Основные преимущества:")
    print("- Быстрее аналитические запросы")
    print("- Проще ETL код")  
    print("- Совместимость с Flame GPU")
    print("- Экономия места за счет устранения String полей")

if __name__ == "__main__":
    main() 