#!/usr/bin/env python3
"""
Утилита для переименования таблицы Heli_Components → OlapCube_Analytics
=====================================================================

Назначение: Восстановление демо-стенда OlapCube_Analytics для совместимости
Автор: AI Assistant  
Дата: 2025-01-16
Проект: Архивный VNV-CPU демо-стенд
"""

import sys
from pathlib import Path

# Добавляем путь к основным утилитам проекта
sys.path.append(str(Path(__file__).parent.parent / 'code' / 'utils'))

from config_loader import get_clickhouse_client

def rename_table():
    """Переименовывает таблицу Heli_Components в OlapCube_Analytics"""
    
    print("🔄 Подключение к ClickHouse...")
    client = get_clickhouse_client()
    
    print("📋 Проверяем существование таблицы Heli_Components...")
    
    # Проверяем существование исходной таблицы
    try:
        result = client.execute("SHOW TABLES LIKE 'Heli_Components'")
        if not result:
            print("❌ Таблица Heli_Components не найдена!")
            print("💡 Возможно, таблица уже переименована или не создана")
            print("📋 Проверим существование OlapCube_Analytics...")
            
            # Проверяем целевую таблицу
            result_target = client.execute("SHOW TABLES LIKE 'OlapCube_Analytics'")
            if result_target:
                count_result = client.execute("SELECT COUNT(*) FROM OlapCube_Analytics")
                record_count = count_result[0][0]
                print(f"✅ Таблица OlapCube_Analytics уже существует: {record_count:,} записей")
                return True
            else:
                print("❌ Ни одна из таблиц не найдена!")
                return False
    except Exception as e:
        print(f"❌ Ошибка проверки таблицы: {e}")
        return False
    
    # Показываем количество записей в исходной таблице
    try:
        count_result = client.execute("SELECT COUNT(*) FROM Heli_Components")
        record_count = count_result[0][0]
        print(f"✅ Таблица Heli_Components найдена: {record_count:,} записей")
    except Exception as e:
        print(f"⚠️ Не удалось посчитать записи: {e}")
    
    # Проверяем, существует ли уже целевая таблица
    try:
        result = client.execute("SHOW TABLES LIKE 'OlapCube_Analytics'")
        if result:
            print("⚠️ Таблица OlapCube_Analytics уже существует!")
            print("🗑️ Удаляем существующую таблицу...")
            client.execute("DROP TABLE OlapCube_Analytics")
            print("✅ Старая таблица OlapCube_Analytics удалена")
    except Exception as e:
        print(f"⚠️ Ошибка при проверке/удалении целевой таблицы: {e}")
    
    # Выполняем переименование
    try:
        print("🔄 Переименовываем Heli_Components → OlapCube_Analytics...")
        client.execute("RENAME TABLE Heli_Components TO OlapCube_Analytics")
        print("✅ Таблица успешно переименована!")
        
        # Проверяем результат
        result = client.execute("SHOW TABLES LIKE 'OlapCube_Analytics'")
        if result:
            # Показываем количество записей в новой таблице
            count_result = client.execute("SELECT COUNT(*) FROM OlapCube_Analytics")
            final_count = count_result[0][0]
            print(f"✅ Проверка: OlapCube_Analytics содержит {final_count:,} записей")
            return True
        else:
            print("❌ Переименование не выполнено!")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка переименования: {e}")
        return False

def show_table_info():
    """Показывает информацию о таблицах демо-стенда"""
    
    print("📊 Информация о таблицах демо-стенда:")
    print("-" * 50)
    
    client = get_clickhouse_client()
    
    # Список таблиц для проверки
    demo_tables = ['OlapCube_Analytics', 'Heli_Components', 'OlapCube_VNV']
    
    for table in demo_tables:
        try:
            result = client.execute(f"SHOW TABLES LIKE '{table}'")
            if result:
                count_result = client.execute(f"SELECT COUNT(*) FROM {table}")
                record_count = count_result[0][0]
                print(f"✅ {table}: {record_count:,} записей")
            else:
                print(f"❌ {table}: не найдена")
        except Exception as e:
            print(f"⚠️ {table}: ошибка проверки - {e}")

if __name__ == "__main__":
    print("🚁 Утилита переименования таблиц - Архивный VNV-CPU проект")
    print("=" * 70)
    print("📋 Назначение: Восстановление демо-стенда OlapCube_Analytics")
    print()
    
    # Показываем текущее состояние
    show_table_info()
    print()
    
    # Выполняем переименование
    success = rename_table()
    
    if success:
        print()
        print("🎉 Операция выполнена успешно!")
        print("📊 Демо-стенд OlapCube_Analytics готов для использования")
        print("🔗 Совместимость с архивными скриптами восстановлена")
    else:
        print()
        print("❌ Операция завершилась с ошибками")
        sys.exit(1) 