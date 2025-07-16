#!/usr/bin/env python3
"""
Модуль обработки статусов компонентов на основе таблицы program_ac для dual_loader.py

Определяет статус "Эксплуатация" (2) для компонентов ВС, находящихся в эксплуатации
НЕ ПЕРЕЗАПИСЫВАЕТ уже установленные статусы - работает только с status_id = 0
"""

import pandas as pd
from datetime import datetime, date


def load_dict_status_flat():
    """Возвращает словарь статусов с правильной нумерацией"""
    return {
        1: "Неактивно",
        2: "Эксплуатация", 
        3: "Исправен",
        4: "Ремонт",
        5: "Резерв",
        6: "Хранение"
    }


def get_program_ac_data(client):
    """
    Получает данные из таблицы program_ac - реестр вертолетов в эксплуатации
    
    Возвращает DataFrame с регистрационными номерами ВС в активной эксплуатации
    """
    try:
        print("📋 Загружаем данные из program_ac...")
        
        # Проверяем наличие таблицы
        check_table_query = "SELECT COUNT(*) FROM system.tables WHERE name = 'program_ac'"
        table_exists = client.execute(check_table_query)[0][0] > 0
        
        if not table_exists:
            print("❌ Таблица program_ac не найдена в ClickHouse!")
            print("💡 Сначала запустите: python3 code/program_ac_loader.py")
            return None
        
        # Получаем все данные о ВС в эксплуатации
        query = """
        SELECT 
            ac_registr,
            ac_typ,
            owner,
            operator,
            homebase,
            homebase_name
        FROM program_ac 
        ORDER BY ac_registr
        """
        
        result = client.execute(query)
        
        if not result:
            print("ℹ️ Нет данных о ВС в эксплуатации в program_ac")
            return pd.DataFrame(columns=['ac_registr', 'ac_typ', 'owner', 'operator', 'homebase', 'homebase_name'])
        
        # Создаем DataFrame
        df = pd.DataFrame(result, columns=['ac_registr', 'ac_typ', 'owner', 'operator', 'homebase', 'homebase_name'])
        
        print(f"✅ Загружено {len(df)} записей ВС в эксплуатации")
        print(f"📊 Типы ВС: {df['ac_typ'].value_counts().head(3).to_dict()}")
        
        # Показываем примеры ВС в эксплуатации
        if len(df) > 0:
            print(f"🔍 Примеры ВС в эксплуатации:")
            for i, (_, row) in enumerate(df.head(3).iterrows()):
                print(f"   RA-{row['ac_registr']}: {row['ac_typ']} ({row['operator']})")
        
        return df
        
    except Exception as e:
        print(f"❌ Ошибка загрузки данных program_ac: {e}")
        return None


def process_aircraft_operation_status(pandas_df, client):
    """
    Обрабатывает статусы компонентов ВС на основе program_ac (реестр в эксплуатации)
    
    Логика:
    1. КЛЮЧЕВОЕ УСЛОВИЕ: ac_registr (program_ac) = serialno (heli_pandas)
    2. Если ВС в реестре эксплуатации - устанавливаем статус 2 (Эксплуатация)
    3. НЕ ПЕРЕЗАПИСЫВАЕМ уже установленные статусы (только если status_id = 0)
    
    Приоритеты:
    - status_id = 4 (Ремонт) имеет приоритет - НЕ перезаписываем
    - status_id = 0 (не определен) → можем установить 2 (Эксплуатация)
    """
    try:
        print("🔧 Обработка статусов компонентов через program_ac...")
        
        # Получаем данные по ВС в эксплуатации
        program_ac_df = get_program_ac_data(client)
        if program_ac_df is None:
            print("⚠️ Не удалось загрузить данные program_ac - пропускаем обработку статусов эксплуатации")
            return pandas_df
        
        if len(program_ac_df) == 0:
            print("ℹ️ Нет данных о ВС в эксплуатации - пропускаем обработку")
            return pandas_df
        
        # Проверяем наличие колонки status_id
        if 'status_id' not in pandas_df.columns:
            print("⚠️ Колонка 'status_id' отсутствует в DataFrame - добавляем со значениями по умолчанию")
            pandas_df['status_id'] = 0
        
        # Создаем словарь для быстрого поиска
        # Ключ: регистрационный номер ВС (str), значение: данные ВС
        program_dict = {}
        for _, row in program_ac_df.iterrows():
            ac_registr = str(row['ac_registr']).zfill(5)  # Приводим к 5-значному формату
            program_dict[ac_registr] = {
                'ac_typ': row['ac_typ'],
                'owner': row['owner'],
                'operator': row['operator'],
                'homebase': row['homebase'],
                'homebase_name': row['homebase_name']
            }
        
        print(f"📋 Создан словарь эксплуатации для {len(program_dict)} ВС")
        
        # КЛЮЧЕВОЕ УСЛОВИЕ: ac_registr (program_ac) = serialno (heli_pandas)
        # Ищем компоненты где serialno совпадает с номерами ВС из program_ac
        aircraft_serialnos = set(program_dict.keys())
        aircraft_mask = pandas_df['serialno'].isin(aircraft_serialnos)
        
        aircraft_rows = pandas_df[aircraft_mask].copy()
        print(f"🚁 Найдено {len(aircraft_rows)} компонентов с serialno = ac_registr для обработки")
        
        if len(aircraft_rows) == 0:
            print("ℹ️ Не найдено компонентов с serialno = ac_registr - обработка не требуется")
            return pandas_df
        
        # Показываем примеры найденных компонентов
        print(f"🔍 Примеры компонентов для обработки:")
        for i, (_, row) in enumerate(aircraft_rows.head(3).iterrows()):
            current_status = row.get('status_id', 0)
            print(f"   serialno={row['serialno']}, current_status={current_status}")
        
        # Счетчики для отчета
        processed_count = 0
        status_updated_count = 0
        skipped_count = 0
        
        # Обрабатываем каждый компонент с совпадающим serialno
        for idx, row in aircraft_rows.iterrows():
            serialno = row['serialno']  # Номер ВС из serialno
            current_status = row.get('status_id', 0)
            
            try:
                # Нормализуем serialno к 5-значному формату для сравнения
                serialno_normalized = str(serialno).zfill(5)
                
                # ПРЯМОЕ СРАВНЕНИЕ: ac_registr = serialno
                if serialno_normalized in program_dict:
                    program_data = program_dict[serialno_normalized]
                    
                    # КРИТИЧНО: НЕ ПЕРЕЗАПИСЫВАЕМ уже установленные статусы
                    if current_status == 0:
                        # Устанавливаем статус 2 (Эксплуатация) только если текущий статус = 0
                        pandas_df.at[idx, 'status_id'] = 2
                        status_updated_count += 1
                        
                        print(f"✅ Компонент serialno={serialno}: статус 0 → 2 (Эксплуатация), ВС {program_data['ac_typ']}")
                    else:
                        # Пропускаем - статус уже установлен (например, 4=Ремонт имеет приоритет)
                        skipped_count += 1
                        dict_status = load_dict_status_flat()
                        status_name = dict_status.get(current_status, f"Неизвестно({current_status})")
                        print(f"⏭️ Компонент serialno={serialno}: статус уже установлен ({current_status}={status_name}) - пропускаем")
                    
                processed_count += 1
                
            except (ValueError, TypeError) as e:
                print(f"⚠️ Ошибка обработки компонента {serialno}: {e}")
                continue
        
        print(f"\n📊 Результаты обработки компонентов через program_ac:")
        print(f"   Найдено компонентов ВС: {processed_count}")
        print(f"   Статусов обновлено до 'Эксплуатация': {status_updated_count}")
        print(f"   Пропущено (статус уже установлен): {skipped_count}")
        
        return pandas_df
        
    except Exception as e:
        print(f"❌ Ошибка обработки статусов через program_ac: {e}")
        return pandas_df


def process_program_ac_status_field(pandas_df, client):
    """
    Главная функция обработки статусов на основе таблицы program_ac
    
    Устанавливает статус 2 (Эксплуатация) для компонентов ВС, находящихся в реестре эксплуатации
    НЕ ПЕРЕЗАПИСЫВАЕТ уже установленные статусы - работает только с status_id = 0
    
    Логика приоритетов:
    1. status_id = 4 (Ремонт) - НЕ ТРОГАЕМ (установлен status_processor.py)
    2. status_id = 0 (не определен) - устанавливаем 2 (Эксплуатация)
    3. status_id != 0 и != 4 - НЕ ТРОГАЕМ
    """
    try:
        print("\n🚀 === ОБРАБОТКА СТАТУСОВ ЧЕРЕЗ PROGRAM_AC ===")
        
        original_count = len(pandas_df)
        print(f"📊 Обрабатываем {original_count:,} записей")
        
        # Проверяем наличие поля status_id и показываем начальное распределение
        initial_status_counts = {}
        if 'status_id' in pandas_df.columns:
            initial_status_counts = pandas_df['status_id'].value_counts().sort_index()
            print(f"📊 Начальное распределение статусов:")
            dict_status = load_dict_status_flat()
            for status_id, count in initial_status_counts.items():
                status_name = dict_status.get(status_id, f"Неизвестно({status_id})")
                print(f"   {status_id} - {status_name}: {count:,} записей")
        else:
            print("⚠️ Поле 'status_id' отсутствует - будет создано")
        
        # Обработка компонентов ВС через program_ac
        pandas_df = process_aircraft_operation_status(pandas_df, client)
        
        # Проверяем результаты
        if 'status_id' in pandas_df.columns:
            final_status_counts = pandas_df['status_id'].value_counts().sort_index()
            dict_status = load_dict_status_flat()
            
            print(f"\n📊 Итоговое распределение статусов:")
            for status_id, count in final_status_counts.items():
                status_name = dict_status.get(status_id, f"Неизвестно({status_id})")
                initial_count = initial_status_counts.get(status_id, 0)
                change = count - initial_count
                change_str = f" (+{change})" if change > 0 else f" ({change})" if change < 0 else ""
                print(f"   {status_id} - {status_name}: {count:,} записей{change_str}")
        
        print(f"\n✅ Обработка статусов через program_ac завершена")
        return pandas_df
        
    except Exception as e:
        print(f"❌ Критическая ошибка обработки статусов через program_ac: {e}")
        # Возвращаем исходный DataFrame при ошибке
        return pandas_df