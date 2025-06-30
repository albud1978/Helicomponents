"""
Модуль обработки статусов компонентов для dual_loader
Определяет начальные статусы на основе данных status_overhaul и архивной логики
"""

import pandas as pd
import numpy as np
from datetime import datetime, date


def load_dict_status_flat():
    """Возвращает словарь статусов из архивного проекта"""
    return {
        1: "Неактивно",
        2: "Эксплуатация", 
        3: "Исправен",
        4: "Ремонт",
        5: "Хранение"
        # 6: "Резерв" - не используется в финальной логике согласно архиву
    }


def get_status_overhaul_data(client):
    """Получает данные из таблицы status_overhaul с фильтром по статусу != 'Закрыто'"""
    try:
        print("📋 Загружаем данные из status_overhaul...")
        
        # Проверяем наличие таблицы
        check_table_query = "SELECT COUNT(*) FROM system.tables WHERE name = 'status_overhaul'"
        table_exists = client.execute(check_table_query)[0][0] > 0
        
        if not table_exists:
            print("❌ Таблица status_overhaul не найдена в ClickHouse!")
            print("💡 Сначала запустите: python3 code/status_overhaul_loader.py")
            return None
        
        # Получаем данные с фильтром
        query = """
        SELECT 
            ac_registr,
            status,
            act_start_date,
            sched_end_date
        FROM status_overhaul 
        WHERE status != 'Закрыто'
        ORDER BY ac_registr
        """
        
        result = client.execute(query)
        
        if not result:
            print("ℹ️ Нет активных записей капитального ремонта (все имеют status='Закрыто')")
            return pd.DataFrame(columns=['ac_registr', 'status', 'act_start_date', 'sched_end_date'])
        
        # Создаем DataFrame
        df = pd.DataFrame(result, columns=['ac_registr', 'status', 'act_start_date', 'sched_end_date'])
        
        print(f"✅ Загружено {len(df)} записей активного капремонта ВС")
        print(f"📊 Статусы: {df['status'].value_counts().to_dict()}")
        
        return df
        
    except Exception as e:
        print(f"❌ Ошибка загрузки данных status_overhaul: {e}")
        return None


def process_aircraft_status(pandas_df, client):
    """
    Обрабатывает статусы планеров ВС на основе status_overhaul
    
    Логика:
    1. КЛЮЧЕВОЕ УСЛОВИЕ: ac_registr (status_overhaul) = serialno (heli_pandas)
    2. Если ВС в капремонте (status != 'Закрыто') - устанавливаем статус 4 (Ремонт)
    3. Переносим act_start_date → removal_date, sched_end_date → target_date
    """
    try:
        print("🔧 Обработка статусов компонентов ВС через status_overhaul...")
        
        # Получаем данные по капремонту
        status_overhaul_df = get_status_overhaul_data(client)
        if status_overhaul_df is None:
            print("⚠️ Не удалось загрузить данные status_overhaul - пропускаем обработку статусов ВС")
            return pandas_df
        
        if len(status_overhaul_df) == 0:
            print("ℹ️ Нет активных капремонтов - все планеры получат статус по умолчанию")
            return pandas_df
        
        # Добавляем колонку status если ее нет
        if 'status' not in pandas_df.columns:
            pandas_df['status'] = pd.Series(0, index=pandas_df.index, dtype='int64')  # Явно int64
            print("➕ Добавлена колонка 'status' со значением по умолчанию 0")
        
        # Создаем словарь для быстрого поиска
        # Ключ: регистрационный номер ВС (str), значение: данные капремонта
        status_dict = {}
        for _, row in status_overhaul_df.iterrows():
            ac_registr = str(row['ac_registr'])  # Оставляем как строку
            status_dict[ac_registr] = {
                'status': row['status'],
                'act_start_date': row['act_start_date'],
                'sched_end_date': row['sched_end_date']
            }
        
        print(f"📋 Ключи в status_dict: {list(status_dict.keys())}")
        
        # КЛЮЧЕВОЕ УСЛОВИЕ: ac_registr (status_overhaul) = serialno (heli_pandas)
        # Ищем компоненты где serialno совпадает с номерами ВС из status_overhaul
        aircraft_serialnos = set(status_dict.keys())
        aircraft_mask = pandas_df['serialno'].isin(aircraft_serialnos)
        
        aircraft_rows = pandas_df[aircraft_mask].copy()
        print(f"🚁 Найдено {len(aircraft_rows)} компонентов с serialno = ac_registr для обработки")
        
        if len(aircraft_rows) == 0:
            print("ℹ️ Не найдено компонентов с serialno = ac_registr - обработка не требуется")
            return pandas_df
        
        # Счетчики для отчета
        processed_count = 0
        status_updated_count = 0
        dates_updated_count = 0
        
        # Обрабатываем каждый компонент с совпадающим serialno
        for idx, row in aircraft_rows.iterrows():
            serialno = row['serialno']  # Номер ВС из serialno
            
            try:
                # ПРЯМОЕ СРАВНЕНИЕ: ac_registr = serialno
                if serialno in status_dict:
                    overhaul_data = status_dict[serialno]
                    
                    # 1. Устанавливаем статус 4 (Ремонт)
                    pandas_df.at[idx, 'status'] = 4
                    status_updated_count += 1
                    
                    # 2. Переносим даты
                    if overhaul_data['act_start_date']:
                        pandas_df.at[idx, 'removal_date'] = overhaul_data['act_start_date']
                        dates_updated_count += 1
                    
                    if overhaul_data['sched_end_date']:
                        pandas_df.at[idx, 'target_date'] = overhaul_data['sched_end_date']
                        dates_updated_count += 1
                    
                    print(f"✅ Компонент serialno={serialno}: статус={overhaul_data['status']} → status=4 (Ремонт)")
                    
                processed_count += 1
                
            except (ValueError, TypeError) as e:
                print(f"⚠️ Ошибка обработки компонента {serialno}: {e}")
                continue
        
        print(f"\n📊 Результаты обработки компонентов:")
        print(f"   Обработано компонентов: {processed_count}")
        print(f"   Статусов обновлено: {status_updated_count}")
        print(f"   Дат обновлено: {dates_updated_count}")
        
        return pandas_df
        
    except Exception as e:
        print(f"❌ Ошибка обработки статусов планеров: {e}")
        return pandas_df


def add_status_column_to_schema():
    """Возвращает SQL для добавления колонки status в схему таблицы heli_pandas"""
    return """
    ALTER TABLE heli_pandas 
    ADD COLUMN IF NOT EXISTS status UInt8 DEFAULT 0
    """


def process_component_status(pandas_df):
    """
    Обрабатывает статусы остальных компонентов (не планеров) 
    
    Пока заглушка - в следующих итерациях будет реализована полная логика
    определения статусов на основе архивного алгоритма
    """
    try:
        print("🔧 Обработка статусов компонентов (заглушка)...")
        
        # Временная логика - оставляем статус по умолчанию для всех не-планеров
        non_aircraft_mask = ~(
            (pandas_df['serialno'] == pandas_df['location']) & 
            pandas_df['location'].str.startswith('RA-', na=False)
        )
        
        non_aircraft_count = non_aircraft_mask.sum()
        print(f"🔧 Найдено {non_aircraft_count} компонентов (не планеров) - статус по умолчанию")
        
        # В будущих итерациях здесь будет полная логика из архивного проекта:
        # - Определение статусов по sne/ppr/condition/location
        # - Эксплуатация/Исправен/Ремонт/Хранение/Неактивно
        
        return pandas_df
        
    except Exception as e:
        print(f"❌ Ошибка обработки статусов компонентов: {e}")
        return pandas_df


def process_status_field(pandas_df, client):
    """
    Главная функция обработки поля status
    
    Этап 1: Планеры ВС через status_overhaul
    Этап 2: Остальные компоненты (в следующих итерациях)
    """
    try:
        print("\n🚀 === ОБРАБОТКА ПОЛЯ STATUS ===")
        
        original_count = len(pandas_df)
        print(f"📊 Обрабатываем {original_count:,} записей")
        
        # Этап 1: Компоненты ВС через status_overhaul
        pandas_df = process_aircraft_status(pandas_df, client)
        
        # Этап 2: Остальные компоненты (заглушка)
        pandas_df = process_component_status(pandas_df)
        
        # Проверяем результаты
        if 'status' in pandas_df.columns:
            status_counts = pandas_df['status'].value_counts().sort_index()
            dict_status = load_dict_status_flat()
            
            print(f"\n📊 Итоговое распределение статусов:")
            for status_id, count in status_counts.items():
                status_name = dict_status.get(status_id, f"Неизвестно({status_id})")
                print(f"   {status_id} - {status_name}: {count:,} записей")
        
        print(f"✅ Обработка статусов завершена")
        return pandas_df
        
    except Exception as e:
        print(f"❌ Критическая ошибка обработки статусов: {e}")
        # Возвращаем исходный DataFrame при ошибке
        return pandas_df 