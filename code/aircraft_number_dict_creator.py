#!/usr/bin/env python3
"""
Создатель словаря номеров вертолетов для Direct Join

Функционал:
- Извлекает уникальные aircraft_number из heli_pandas
- Создает словарь с форматированными номерами (ведущие нули)
- Поддерживает Direct Join в ClickHouse для аналитических запросов
- Автоматическое обновление при изменении данных

Использование:
    python3 aircraft_number_dict_creator.py
    
Аналитические запросы:
    SELECT h.partno, d.registration_code
    FROM heli_pandas h
    LEFT JOIN aircraft_number_dict d ON h.aircraft_number = d.aircraft_number
"""

import sys
from pathlib import Path
import pandas as pd
from typing import Dict, List, Set
import logging

# Добавляем путь к утилитам
sys.path.append(str(Path(__file__).parent))
from utils.config_loader import get_clickhouse_client

def setup_logging() -> logging.Logger:
    """Настройка логирования"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def get_aircraft_numbers_from_heli_pandas(client) -> Set[int]:
    """
    Извлекает уникальные aircraft_number из heli_pandas
    
    Args:
        client: ClickHouse client
        
    Returns:
        Set[int]: множество уникальных номеров вертолетов
    """
    try:
        print("📊 Извлечение уникальных номеров вертолетов из heli_pandas...")
        
        query = """
        SELECT DISTINCT aircraft_number
        FROM heli_pandas 
        WHERE aircraft_number > 0
        ORDER BY aircraft_number
        """
        
        result = client.execute(query)
        aircraft_numbers = set(row[0] for row in result)
        
        print(f"✅ Найдено {len(aircraft_numbers)} уникальных номеров вертолетов")
        
        if aircraft_numbers:
            min_num = min(aircraft_numbers)
            max_num = max(aircraft_numbers)
            print(f"📈 Диапазон: {min_num} - {max_num}")
            
            # Показываем примеры с потенциальными ведущими нулями
            samples = sorted(list(aircraft_numbers))[:5]
            print("📋 Примеры номеров:")
            for num in samples:
                formatted = f"{num:05d}"
                print(f"   {num} → RA-{formatted}")
        
        return aircraft_numbers
        
    except Exception as e:
        print(f"❌ Ошибка извлечения номеров: {e}")
        return set()

def create_aircraft_number_dict_table(client) -> bool:
    """
    Создает таблицу-словарь aircraft_number_dict
    
    Args:
        client: ClickHouse client
        
    Returns:
        bool: True если успешно создана
    """
    try:
        print("🔧 Создание таблицы aircraft_number_dict...")
        
        create_sql = """
        CREATE TABLE IF NOT EXISTS aircraft_number_dict (
            `aircraft_number` UInt16,                    -- Числовой номер для JOIN
            `formatted_number` String,                   -- 5-значный формат с ведущими нулями
            `registration_code` String,                  -- Полный код RA-XXXXX
            `is_leading_zero` UInt8 DEFAULT 0           -- Флаг: имеет ведущие нули (1) или нет (0)
        ) ENGINE = MergeTree()
        ORDER BY aircraft_number
        SETTINGS index_granularity = 8192
        """
        
        client.execute(create_sql)
        print("✅ Таблица aircraft_number_dict создана")
        
        # Создаем Dictionary для Direct Join (опционально)
        print("🔧 Создание ClickHouse Dictionary...")
        
        dictionary_sql = """
        CREATE DICTIONARY IF NOT EXISTS aircraft_number_dictionary (
            aircraft_number UInt16,
            formatted_number String,
            registration_code String,
            is_leading_zero UInt8
        )
        PRIMARY KEY aircraft_number
        SOURCE(CLICKHOUSE(
            TABLE 'aircraft_number_dict'
        ))
        LAYOUT(HASHED())
        LIFETIME(MIN 300 MAX 600)
        """
        
        try:
            client.execute(dictionary_sql)
            print("✅ Dictionary aircraft_number_dictionary создан")
        except Exception as dict_error:
            print(f"⚠️ Dictionary не создан (возможно не поддерживается): {dict_error}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка создания таблицы: {e}")
        return False

def populate_aircraft_number_dict(client, aircraft_numbers: Set[int]) -> bool:
    """
    Аддитивно заполняет словарь данными о номерах вертолетов (без TRUNCATE)
    
    Args:
        client: ClickHouse client
        aircraft_numbers: множество номеров для обработки
        
    Returns:
        bool: True если успешно заполнен
    """
    try:
        print(f"📦 Аддитивное заполнение словаря для {len(aircraft_numbers)} номеров...")
        
        # Получаем уже существующие номера из словаря
        existing_query = "SELECT DISTINCT aircraft_number FROM aircraft_number_dict"
        try:
            existing_result = client.execute(existing_query)
            existing_numbers = {row[0] for row in existing_result}
            print(f"📋 Найдено {len(existing_numbers)} существующих записей в словаре")
        except:
            # Таблица может не существовать или быть пустой
            existing_numbers = set()
            print("📋 Словарь пуст или таблица не существует")
        
        # Определяем новые номера для добавления
        new_numbers = aircraft_numbers - existing_numbers
        
        if not new_numbers:
            print("✅ Все номера уже существуют в словаре, новых записей не требуется")
            return True
        
        print(f"🆕 Добавляем {len(new_numbers)} новых номеров (аддитивно)")
        
        # Подготавливаем данные только для новых номеров
        dict_data = []
        leading_zero_count = 0
        
        for aircraft_number in sorted(new_numbers):
            formatted_number = f"{aircraft_number:05d}"
            registration_code = f"RA-{formatted_number}"
            is_leading_zero = 1 if aircraft_number < 10000 else 0
            
            if is_leading_zero:
                leading_zero_count += 1
            
            dict_data.append((
                aircraft_number,
                formatted_number,
                registration_code,
                is_leading_zero
            ))
        
        # Загружаем только новые данные батчами
        batch_size = 100
        inserted_count = 0
        
        for i in range(0, len(dict_data), batch_size):
            batch = dict_data[i:i + batch_size]
            client.execute(
                'INSERT INTO aircraft_number_dict (aircraft_number, formatted_number, registration_code, is_leading_zero) VALUES',
                batch
            )
            inserted_count += len(batch)
        
        print(f"✅ Добавлено {inserted_count} новых записей в aircraft_number_dict (аддитивно)")
        print(f"📊 Новых номеров с ведущими нулями: {leading_zero_count}")
        print(f"📊 Новых обычных номеров: {len(dict_data) - leading_zero_count}")
        
        # Показываем примеры новых записей
        if dict_data:
            print("📋 Примеры добавленных записей:")
            for i, (aircraft_number, formatted_number, registration_code, is_leading_zero) in enumerate(dict_data[:5]):
                zero_flag = " (с ведущими нулями)" if is_leading_zero else ""
                print(f"   {aircraft_number} → {formatted_number} → {registration_code}{zero_flag}")
        
        # Показываем общую статистику
        total_count = client.execute("SELECT COUNT(*) FROM aircraft_number_dict")[0][0]
        print(f"📊 Общее количество записей в словаре: {total_count}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка аддитивного заполнения словаря: {e}")
        return False

def validate_aircraft_number_dict(client, original_count: int) -> bool:
    """
    Проверяет качество заполнения словаря
    
    Args:
        client: ClickHouse client
        original_count: ожидаемое количество записей
        
    Returns:
        bool: True если валидация прошла
    """
    try:
        print("🔍 Валидация словаря aircraft_number_dict...")
        
        # Проверяем количество записей
        dict_count = client.execute("SELECT COUNT(*) FROM aircraft_number_dict")[0][0]
        
        print(f"📊 Статистика словаря:")
        print(f"   Всего записей: {dict_count}")
        print(f"   Ожидалось: {original_count}")
        
        if dict_count != original_count:
            print(f"⚠️ Количество записей не совпадает")
            return False
        
        # Проверяем корректность форматирования
        validation_query = """
        SELECT 
            aircraft_number,
            formatted_number,
            registration_code,
            is_leading_zero
        FROM aircraft_number_dict
        WHERE 
            LENGTH(formatted_number) != 5 
            OR registration_code != CONCAT('RA-', formatted_number)
            OR (aircraft_number < 10000 AND is_leading_zero != 1)
            OR (aircraft_number >= 10000 AND is_leading_zero != 0)
        """
        
        invalid_records = client.execute(validation_query)
        
        if invalid_records:
            print(f"❌ Найдено {len(invalid_records)} некорректных записей:")
            for record in invalid_records[:5]:  # Показываем первые 5
                print(f"   {record}")
            return False
        
        # Статистика по ведущим нулям
        stats_query = """
        SELECT 
            is_leading_zero,
            COUNT(*) as count,
            MIN(aircraft_number) as min_num,
            MAX(aircraft_number) as max_num
        FROM aircraft_number_dict 
        GROUP BY is_leading_zero
        ORDER BY is_leading_zero
        """
        
        stats = client.execute(stats_query)
        print("📈 Статистика по ведущим нулям:")
        for is_leading_zero, count, min_num, max_num in stats:
            category = "С ведущими нулями" if is_leading_zero else "Обычные номера"
            print(f"   {category}: {count} записей ({min_num}-{max_num})")
        
        print("✅ Валидация словаря прошла успешно!")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка валидации: {e}")
        return False

def show_usage_examples(client):
    """Показывает примеры использования словаря в аналитических запросах"""
    print("\n📚 === ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ СЛОВАРЯ ===")
    
    examples = [
        {
            "title": "Простой JOIN для отображения",
            "query": """
            SELECT 
                h.partno,
                h.aircraft_number,
                d.registration_code
            FROM heli_pandas h
            LEFT JOIN aircraft_number_dict d ON h.aircraft_number = d.aircraft_number
            WHERE h.aircraft_number > 0
            LIMIT 5
            """
        },
        {
            "title": "Анализ номеров с ведущими нулями",
            "query": """
            SELECT 
                d.is_leading_zero,
                COUNT(*) as components_count,
                COUNT(DISTINCT h.aircraft_number) as aircraft_count
            FROM heli_pandas h
            JOIN aircraft_number_dict d ON h.aircraft_number = d.aircraft_number
            GROUP BY d.is_leading_zero
            """
        },
        {
            "title": "Форматированный отчет по вертолетам",
            "query": """
            SELECT 
                d.registration_code,
                COUNT(*) as components,
                COUNT(DISTINCT h.partno) as unique_parts
            FROM heli_pandas h
            JOIN aircraft_number_dict d ON h.aircraft_number = d.aircraft_number
            GROUP BY d.registration_code
            ORDER BY components DESC
            LIMIT 10
            """
        }
    ]
    
    for example in examples:
        print(f"\n🔍 {example['title']}:")
        print("```sql")
        print(example['query'].strip())
        print("```")
        
        try:
            # Выполняем пример запроса
            result = client.execute(example['query'])
            if result:
                print("📊 Результат:")
                for i, row in enumerate(result[:3]):  # Показываем первые 3 строки
                    print(f"   {row}")
                if len(result) > 3:
                    print(f"   ... и еще {len(result)-3} записей")
        except Exception as e:
            print(f"⚠️ Ошибка выполнения примера: {e}")

def main():
    """Основная функция"""
    print("🚀 === СОЗДАТЕЛЬ СЛОВАРЯ НОМЕРОВ ВЕРТОЛЕТОВ ===")
    
    logger = setup_logging()
    
    try:
        # 1. Подключение к ClickHouse
        print("🔗 Подключение к ClickHouse...")
        client = get_clickhouse_client()
        
        # 2. Извлечение номеров из heli_pandas
        aircraft_numbers = get_aircraft_numbers_from_heli_pandas(client)
        if not aircraft_numbers:
            print("❌ Не найдено номеров вертолетов в heli_pandas")
            return 1
        
        # 3. Создание таблицы словаря
        if not create_aircraft_number_dict_table(client):
            return 1
        
        # 4. Заполнение словаря
        if not populate_aircraft_number_dict(client, aircraft_numbers):
            return 1
        
        # 5. Валидация результата
        if not validate_aircraft_number_dict(client, len(aircraft_numbers)):
            return 1
        
        # 6. Примеры использования
        show_usage_examples(client)
        
        print(f"\n🎯 === СЛОВАРЬ ГОТОВ К ИСПОЛЬЗОВАНИЮ ===")
        print(f"✅ Таблица: aircraft_number_dict ({len(aircraft_numbers)} записей)")
        print(f"✅ Dictionary: aircraft_number_dictionary (опционально)")
        print(f"🔥 Теперь можно использовать Direct Join для сохранения ведущих нулей!")
        print(f"📚 Примеры запросов показаны выше")
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        return 1

if __name__ == "__main__":
    exit(main()) 