#!/usr/bin/env python3
"""
Унифицированный процессор для извлечения номеров вертолетов из поля location

Функционал:
- Извлекает номера вертолетов из значений RA-XXXXX (5 цифр)
- Создает поле aircraft_number (UInt32)
- Очищает location от не-вертолетных значений
- Поддерживает работу с DataFrame (in-memory) и ClickHouse (SQL)
- Валидация и статистика обработки

Использование:
    # In-memory (для ETL пайплайна)
    df = process_aircraft_numbers_in_memory(df)
    
    # ClickHouse (для standalone обработки)
    process_aircraft_numbers_in_clickhouse(client)
"""

import pandas as pd
import logging
from typing import Dict, List, Tuple, Optional
import sys
from pathlib import Path

def setup_logging() -> logging.Logger:
    """Настройка логирования"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def extract_aircraft_numbers_from_dataframe(df: pd.DataFrame) -> Tuple[Dict[str, int], int]:
    """
    Извлекает номера вертолетов из DataFrame
    
    Args:
        df: DataFrame с полем location
        
    Returns:
        Tuple[Dict[str, int], int]: (mapping location->aircraft_number, invalid_count)
    """
    # Ищем все RA- значения в location
    ra_mask = df['location'].str.startswith('RA-', na=False)
    ra_locations = df[ra_mask]['location'].unique()
    
    aircraft_mapping = {}
    invalid_count = 0
    
    for location in ra_locations:
        # Убираем префикс 'RA-'
        digits_part = location[3:]
        
        # Проверяем что это 5 цифр
        if len(digits_part) == 5 and digits_part.isdigit():
            aircraft_number = int(digits_part)
            aircraft_mapping[location] = aircraft_number
        else:
            invalid_count += 1
            print(f"⚠️ Неправильный формат: {location}")
    
    return aircraft_mapping, invalid_count

def process_aircraft_numbers_in_memory(df: pd.DataFrame) -> Tuple[pd.DataFrame, int, int]:
    """
    Обрабатывает aircraft_number в DataFrame (in-memory подход)
    
    Args:
        df: DataFrame с полем location
        
    Returns:
        Tuple[pd.DataFrame, int, int]: (обработанный_df, aircraft_count, invalid_count)
    """
    print("🔍 Извлечение номеров вертолетов из RA- значений...")
    
    # Добавляем колонку aircraft_number со значением по умолчанию
    df['aircraft_number'] = 0
    
    # Извлекаем номера вертолетов
    aircraft_mapping, invalid_count = extract_aircraft_numbers_from_dataframe(df)
    
    print(f"✅ Извлечено {len(aircraft_mapping)} номеров вертолетов")
    if invalid_count > 0:
        print(f"⚠️ Найдено {invalid_count} значений неправильного формата")
    
    # Обновляем aircraft_number для соответствующих записей
    for location, aircraft_number in aircraft_mapping.items():
        mask = df['location'] == location
        df.loc[mask, 'aircraft_number'] = aircraft_number
    
    # Очищаем location для не-RA значений И для невалидных RA- форматов
    # 1. Очищаем все не-RA значения
    non_ra_mask = ~df['location'].str.startswith('RA-', na=False)
    df.loc[non_ra_mask, 'location'] = ''
    
    # 2. Очищаем невалидные RA- форматы (которые не попали в aircraft_mapping)
    valid_ra_locations = set(aircraft_mapping.keys())
    invalid_ra_mask = (df['location'].str.startswith('RA-', na=False)) & (~df['location'].isin(valid_ra_locations))
    df.loc[invalid_ra_mask, 'location'] = ''
    
    aircraft_count = (df['aircraft_number'] > 0).sum()
    print(f"✅ Обогащено {aircraft_count} записей номерами вертолетов")
    
    return df, len(aircraft_mapping), invalid_count

def validate_aircraft_numbers(df: pd.DataFrame) -> bool:
    """
    Валидация обработки aircraft_number
    
    Args:
        df: DataFrame с обработанными данными
        
    Returns:
        bool: True если валидация прошла успешно
    """
    print("🔍 Валидация обработки aircraft_number...")
    
    total_count = len(df)
    aircraft_count = (df['aircraft_number'] > 0).sum()
    location_count = (df['location'] != '').sum()
    
    print(f"📊 Статистика (всего записей: {total_count:,}):")
    print(f"   aircraft_number > 0: {aircraft_count:,} записей ({aircraft_count/total_count*100:.1f}%)")
    print(f"   location не пусто: {location_count:,} записей")
    
    # Проверяем соответствие: у всех записей с aircraft_number > 0 должен быть location
    inconsistent_mask = (df['aircraft_number'] > 0) & (df['location'] == '')
    inconsistent_count = inconsistent_mask.sum()
    
    if inconsistent_count > 0:
        print(f"⚠️ Найдено {inconsistent_count} несогласованных записей")
        return False
    
    # Проверяем диапазон номеров
    if aircraft_count > 0:
        min_num = df[df['aircraft_number'] > 0]['aircraft_number'].min()
        max_num = df[df['aircraft_number'] > 0]['aircraft_number'].max()
        unique_count = df[df['aircraft_number'] > 0]['aircraft_number'].nunique()
        
        print(f"📈 Диапазон номеров: {min_num} - {max_num} ({unique_count} уникальных)")
        
        # Показываем примеры
        print("📋 Примеры обработанных записей:")
        sample_df = df[df['aircraft_number'] > 0][['location', 'aircraft_number']].head(5)
        for _, row in sample_df.iterrows():
            print(f"   location: '{row['location']}' → aircraft_number: {row['aircraft_number']}")
    
    print("✅ Валидация успешно завершена")
    return True

def process_aircraft_numbers_in_clickhouse(client, table_name: str = 'heli_pandas') -> bool:
    """
    Обрабатывает aircraft_number в ClickHouse (SQL подход)
    
    Args:
        client: ClickHouse client
        table_name: имя таблицы для обработки
        
    Returns:
        bool: True если обработка прошла успешно
    """
    logger = setup_logging()
    logger.info(f"🚀 Обработка aircraft_number в таблице {table_name}")
    
    try:
        # 1. Добавляем колонку aircraft_number если её нет
        logger.info("🔧 Добавление колонки aircraft_number...")
        alter_query = f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS aircraft_number UInt32 DEFAULT 0"
        client.execute(alter_query)
        logger.info("✅ Колонка aircraft_number добавлена")
        
        # 2. Извлекаем номера вертолетов
        logger.info("🔍 Извлечение номеров вертолетов из RA- значений...")
        ra_result = client.execute(f"""
            SELECT DISTINCT location
            FROM {table_name}
            WHERE location LIKE 'RA-%'
            ORDER BY location
        """)
        
        aircraft_mapping = {}
        invalid_count = 0
        
        for row in ra_result:
            location = row[0]
            digits_part = location[3:]  # Убираем 'RA-'
            
            if len(digits_part) == 5 and digits_part.isdigit():
                aircraft_number = int(digits_part)
                aircraft_mapping[location] = aircraft_number
            else:
                invalid_count += 1
                logger.warning(f"⚠️ Неправильный формат: {location}")
        
        logger.info(f"✅ Извлечено {len(aircraft_mapping)} номеров вертолетов")
        if invalid_count > 0:
            logger.warning(f"⚠️ Найдено {invalid_count} значений неправильного формата")
        
        # 3. Обновляем aircraft_number батчами
        logger.info("🔢 Обновление поля aircraft_number...")
        
        # Сначала очищаем поле
        client.execute(f"ALTER TABLE {table_name} UPDATE aircraft_number = 0 WHERE 1=1")
        
        # Обновляем батчами для производительности
        batch_size = 50
        mapping_items = list(aircraft_mapping.items())
        
        for i in range(0, len(mapping_items), batch_size):
            batch = mapping_items[i:i + batch_size]
            
            # Строим CASE WHEN для батча
            case_when_parts = []
            locations_list = []
            
            for location, aircraft_number in batch:
                case_when_parts.append(f"WHEN location = '{location}' THEN {aircraft_number}")
                locations_list.append(f"'{location}'")
            
            case_when_sql = " ".join(case_when_parts)
            locations_sql = ",".join(locations_list)
            
            update_query = f"""
            ALTER TABLE {table_name}
            UPDATE aircraft_number = CASE {case_when_sql} ELSE 0 END
            WHERE location IN ({locations_sql})
            """
            
            client.execute(update_query)
            logger.info(f"  📊 Обработано {min(i + batch_size, len(mapping_items))}/{len(mapping_items)} значений")
        
        # 4. Очищаем location для не-вертолетов
        logger.info("🧹 Очистка location для не-вертолетов...")
        
        # Получаем статистику до очистки
        before_count = client.execute(f"SELECT COUNT(*) FROM {table_name} WHERE location IS NOT NULL AND location != ''")[0][0]
        
        # Очищаем location для записей которые НЕ являются валидными вертолетами
        # 1. Очищаем все не-RA значения
        clear_non_ra_query = f"""
        ALTER TABLE {table_name}
        UPDATE location = ''
        WHERE location IS NOT NULL 
          AND location != '' 
          AND NOT (location LIKE 'RA-%')
        """
        
        client.execute(clear_non_ra_query)
        
        # 2. Очищаем невалидные RA- форматы (которые не имеют aircraft_number)
        clear_invalid_ra_query = f"""
        ALTER TABLE {table_name}
        UPDATE location = ''
        WHERE location IS NOT NULL 
          AND location != '' 
          AND location LIKE 'RA-%'
          AND aircraft_number = 0
        """
        
        clear_query = clear_invalid_ra_query  # Для статистики ниже
        
        client.execute(clear_query)
        
        # Получаем статистику после очистки
        after_count = client.execute(f"SELECT COUNT(*) FROM {table_name} WHERE location IS NOT NULL AND location != ''")[0][0]
        
        cleared_count = before_count - after_count
        logger.info(f"✅ Очищено {cleared_count} записей не-вертолетов")
        logger.info(f"📊 Осталось {after_count} записей с location (только RA- значения)")
        
        # 5. Проверяем результат
        enriched_count = client.execute(f"SELECT COUNT(*) FROM {table_name} WHERE aircraft_number > 0")[0][0]
        total_count = client.execute(f"SELECT COUNT(*) FROM {table_name}")[0][0]
        
        logger.info(f"✅ Обогащено {enriched_count} записей номерами вертолетов")
        logger.info(f"📊 Покрытие: {enriched_count/total_count*100:.1f}% записей")
        
        # Показываем примеры
        sample_result = client.execute(f"""
            SELECT location, aircraft_number 
            FROM {table_name}
            WHERE aircraft_number > 0 
            ORDER BY aircraft_number 
            LIMIT 5
        """)
        
        logger.info("📋 Примеры обработанных записей:")
        for row in sample_result:
            logger.info(f"   location: '{row[0]}' → aircraft_number: {row[1]}")
        
        logger.info("🎯 Обработка aircraft_number завершена успешно!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка обработки aircraft_number: {e}")
        return False

def main():
    """Основная функция для standalone использования"""
    print("🚀 === ОБРАБОТЧИК НОМЕРОВ ВЕРТОЛЕТОВ ===")
    print("💡 Данный модуль предназначен для импорта в другие скрипты")
    print("📋 Доступные функции:")
    print("   - process_aircraft_numbers_in_memory(df)")
    print("   - process_aircraft_numbers_in_clickhouse(client)")
    print("   - validate_aircraft_numbers(df)")
    
    # Для standalone использования с ClickHouse
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--clickhouse':
        sys.path.append(str(Path(__file__).parent))
        try:
            from utils.config_loader import get_clickhouse_client
            client = get_clickhouse_client()
            success = process_aircraft_numbers_in_clickhouse(client)
            return 0 if success else 1
        except ImportError as e:
            print(f"❌ Не удалось импортировать config_loader: {e}")
            return 1
        except Exception as e:
            print(f"❌ Ошибка: {e}")
            return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 