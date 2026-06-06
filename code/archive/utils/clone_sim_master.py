#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (code utils cleanup): orphan/устаревшая утилита, 0 импортёров; перекрыта живыми модулями или старым workflow.
"""
Скрипт для клонирования таблицы sim_masterv2 в sim_masterv3
⚠️ ВАЖНО: Не удаляет исходную таблицу sim_masterv2!
"""

import os
import sys

# Добавляем путь к utils для импорта config_loader
sys.path.append(os.path.join(os.path.dirname(__file__)))
from config_loader import get_clickhouse_client


def clone_table(source_table: str = "sim_masterv2", target_table: str = "sim_masterv3"):
    """
    Клонирует таблицу source_table в target_table
    
    Args:
        source_table: Имя исходной таблицы (по умолчанию sim_masterv2)
        target_table: Имя целевой таблицы (по умолчанию sim_masterv3)
    """
    client = get_clickhouse_client()
    
    print(f"\n🔄 Клонирование таблицы {source_table} → {target_table}\n")
    
    # 1. Проверяем существование исходной таблицы
    check_query = f"EXISTS TABLE {source_table}"
    result = client.execute(check_query)
    
    if not result[0][0]:
        print(f"❌ ОШИБКА: Исходная таблица {source_table} не существует!")
        return False
    
    print(f"✅ Исходная таблица {source_table} найдена")
    
    # 2. Получаем количество строк в исходной таблице
    count_query = f"SELECT COUNT(*) FROM {source_table}"
    count_result = client.execute(count_query)
    source_rows = count_result[0][0]
    print(f"📊 Количество строк в {source_table}: {source_rows:,}")
    
    # 3. Удаляем целевую таблицу если существует
    drop_query = f"DROP TABLE IF EXISTS {target_table}"
    print(f"\n🗑️  Удаление существующей таблицы {target_table} (если есть)...")
    client.execute(drop_query)
    print(f"✅ Таблица {target_table} удалена (если существовала)")
    
    # 4. Создаём целевую таблицу как копию исходной
    create_query = f"""
    CREATE TABLE {target_table} AS {source_table}
    """
    print(f"\n🏗️  Создание структуры таблицы {target_table}...")
    client.execute(create_query)
    print(f"✅ Структура таблицы {target_table} создана")
    
    # 5. Копируем данные по дням (чтобы избежать ошибки max_partitions_per_insert_block)
    print(f"\n📥 Копирование данных из {source_table} в {target_table}...")
    print(f"   (копирование по дням для избежания ошибки партиций...)")
    
    # Получаем список уникальных дней
    days_query = f"SELECT DISTINCT day_u16 FROM {source_table} ORDER BY day_u16"
    days_result = client.execute(days_query)
    days = [row[0] for row in days_result]
    
    print(f"   Найдено {len(days)} уникальных дней для копирования")
    
    # Копируем данные по дням
    for i, day in enumerate(days, 1):
        insert_query = f"""
        INSERT INTO {target_table}
        SELECT * FROM {source_table}
        WHERE day_u16 = {day}
        """
        client.execute(insert_query)
        
        if i % 100 == 0 or i == len(days):
            print(f"   Прогресс: {i}/{len(days)} дней ({i*100//len(days)}%)")
    
    print(f"✅ Все данные скопированы")
    
    # 6. Проверяем количество строк в целевой таблице
    target_count_result = client.execute(f"SELECT COUNT(*) FROM {target_table}")
    target_rows = target_count_result[0][0]
    print(f"✅ Данные скопированы: {target_rows:,} строк")
    
    # 7. Финальная проверка
    if source_rows == target_rows:
        print(f"\n✅ УСПЕШНО: Таблица {target_table} создана и содержит {target_rows:,} строк")
        print(f"   Исходная таблица {source_table} не изменена и содержит {source_rows:,} строк")
        return True
    else:
        print(f"\n⚠️  ПРЕДУПРЕЖДЕНИЕ: Количество строк не совпадает!")
        print(f"   {source_table}: {source_rows:,} строк")
        print(f"   {target_table}: {target_rows:,} строк")
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Клонирование таблицы sim_masterv2 в sim_masterv3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  # Клонировать sim_masterv2 → sim_masterv3
  python3 clone_sim_master.py
  
  # Клонировать с другими именами
  python3 clone_sim_master.py --source sim_masterv2 --target sim_master_backup
        """
    )
    
    parser.add_argument(
        "--source",
        default="sim_masterv2",
        help="Имя исходной таблицы (по умолчанию: sim_masterv2)"
    )
    
    parser.add_argument(
        "--target",
        default="sim_masterv3",
        help="Имя целевой таблицы (по умолчанию: sim_masterv3)"
    )
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("  КЛОНИРОВАНИЕ ТАБЛИЦЫ CLICKHOUSE")
    print("=" * 70)
    
    success = clone_table(args.source, args.target)
    
    print("\n" + "=" * 70)
    
    sys.exit(0 if success else 1)

