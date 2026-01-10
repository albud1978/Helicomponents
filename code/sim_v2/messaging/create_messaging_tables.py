#!/usr/bin/env python3
"""
Создание дублирующих таблиц СУБД для messaging-ветки

Таблицы:
- sim_masterv2_msg: Результаты симуляции планеров (messaging архитектура)

Использование:
    python create_messaging_tables.py [--drop]
    
Флаг --drop удаляет существующие таблицы перед созданием.
"""
import argparse
import os
import sys

# Настройка путей для импорта
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'utils'))
from config_loader import get_clickhouse_client


def create_sim_masterv2_msg(client, drop_existing: bool = False):
    """Создаёт таблицу sim_masterv2_msg (копия структуры sim_masterv2)"""
    
    table_name = "sim_masterv2_msg"
    
    if drop_existing:
        print(f"  🗑️  DROP TABLE {table_name}...")
        client.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table_name}
    (
        -- Идентификаторы
        version_id      UInt32,
        version_date    Date,
        sim_day         UInt16,
        idx             UInt16,
        aircraft_number UInt32,
        partseqno_i     UInt32,
        group_by        UInt8,
        
        -- Состояние
        state           UInt8,
        intent_state    UInt8,
        
        -- Наработка
        sne             UInt32,
        ppr             UInt32,
        dt              UInt32,
        
        -- Ремонт
        repair_days     UInt16,
        repair_time     UInt16,
        assembly_time   UInt16,
        assembly_trigger UInt8,
        
        -- Нормативы
        ll              UInt32,
        oh              UInt32,
        br              UInt32,
        
        -- Transition флаги
        transition_2_to_3   UInt8 DEFAULT 0,
        transition_2_to_4   UInt8 DEFAULT 0,
        transition_2_to_6   UInt8 DEFAULT 0,
        transition_3_to_2   UInt8 DEFAULT 0,
        transition_4_to_5   UInt8 DEFAULT 0,
        transition_5_to_2   UInt8 DEFAULT 0,
        transition_1_to_2   UInt8 DEFAULT 0,
        
        -- Квотирование (debugging)
        quota_gap_mi8       Int16 DEFAULT 0,
        quota_gap_mi17      Int16 DEFAULT 0,
        quota_demount       UInt8 DEFAULT 0,
        quota_promote_p1    UInt8 DEFAULT 0,
        quota_promote_p2    UInt8 DEFAULT 0,
        quota_promote_p3    UInt8 DEFAULT 0,
        
        -- Дата производства
        mfg_date        UInt16,
        
        -- Метаданные
        insert_time     DateTime DEFAULT now()
    )
    ENGINE = MergeTree()
    ORDER BY (version_id, version_date, sim_day, idx)
    SETTINGS index_granularity = 8192
    """
    
    print(f"  📦 CREATE TABLE {table_name}...")
    client.execute(ddl)
    print(f"  ✅ Таблица {table_name} создана")


def main():
    parser = argparse.ArgumentParser(description="Создание дублирующих таблиц для messaging-ветки")
    parser.add_argument("--drop", action="store_true", help="Удалить существующие таблицы перед созданием")
    args = parser.parse_args()
    
    print("=" * 60)
    print("🗄️  Создание таблиц СУБД для messaging-ветки")
    print("=" * 60)
    
    client = get_clickhouse_client()
    
    create_sim_masterv2_msg(client, drop_existing=args.drop)
    
    # Проверка
    print("\n📊 Проверка таблицы:")
    result = client.execute("SELECT count() FROM sim_masterv2_msg")
    count = result[0][0] if result else 0
    print(f"  sim_masterv2_msg: {count} строк")
    
    print("\n✅ Готово!")
    print("=" * 60)


if __name__ == "__main__":
    main()

