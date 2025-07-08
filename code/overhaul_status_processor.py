#!/usr/bin/env python3
"""
Процессор статусов капитального ремонта для dual_loader.py

Функционал:
- Устанавливает статус 4 (Ремонт) для планеров ВС в капремонте
- Работает с данными status_overhaul (ВС в капремонте != 'Закрыто')
- Сопоставляет ac_registr (status_overhaul) с serialno (heli_pandas)
- Используется на ЭТАПЕ 1 в dual_loader.py

Логика:
- Прямое сопоставление: ac_registr == serialno
- Если ВС в капремонте → статус 4 (Ремонт)
- Переносит даты: act_start_date → removal_date, sched_end_date → target_date

Автор: AI Assistant
Дата: 2025-01-11
"""

import pandas as pd
from datetime import datetime, date


def load_dict_status_flat():
    """Возвращает словарь статусов из архивного проекта (без 'Резерва')"""
    return {
        1: "Неактивно",
        2: "Эксплуатация", 
        3: "Исправен",
        4: "Ремонт",
        5: "Хранение"
        # 6: "Резерв" - не используется в финальной логике согласно архиву
    }


def get_status_overhaul_data(client):
    """
    Получает данные из таблицы status_overhaul с фильтром по статусу != 'Закрыто'
    
    Логика: ВС в капремонте (status != 'Закрыто') влияют на статусы планеров
    """
    try:
        print("📋 Загружаем данные из status_overhaul...")
        
        # Проверяем наличие таблицы
        check_table_query = "SELECT COUNT(*) FROM system.tables WHERE name = 'status_overhaul'"
        table_exists = client.execute(check_table_query)[0][0] > 0
        
        if not table_exists:
            print("❌ Таблица status_overhaul не найдена в ClickHouse!")
            print("💡 Сначала запустите: python3 code/status_overhaul_loader.py")
            return None
        
        # Получаем данные с фильтром по статусу != 'Закрыто'
        query = """
        SELECT 
            ac_registr,
            status,
            act_start_date,
            sched_end_date,
            act_end_date
        FROM status_overhaul 
        WHERE status != 'Закрыто'
        ORDER BY ac_registr
        """
        
        result = client.execute(query)
        
        if not result:
            print("ℹ️ Нет активных записей капитального ремонта (все имеют status='Закрыто')")
            return pd.DataFrame(columns=['ac_registr', 'status', 'act_start_date', 'sched_end_date', 'act_end_date'])
        
        # Создаем DataFrame
        df = pd.DataFrame(result, columns=['ac_registr', 'status', 'act_start_date', 'sched_end_date', 'act_end_date'])
        
        print(f"✅ Загружено {len(df)} записей активного капремонта ВС")
        print(f"📊 Статусы: {df['status'].value_counts().to_dict()}")
        
        # Показываем примеры
        if len(df) > 0:
            print(f"🔍 Примеры ВС в капремонте:")
            for i, (_, row) in enumerate(df.head(3).iterrows()):
                print(f"   RA-{row['ac_registr']}: {row['status']}")
        
        return df
        
    except Exception as e:
        print(f"❌ Ошибка загрузки данных status_overhaul: {e}")
        return None


def process_aircraft_status(pandas_df, client):
    """
    Обрабатывает статусы планеров ВС на основе status_overhaul
    
    УПРОЩЕННАЯ ЛОГИКА:
    - Прямое сопоставление: ac_registr (status_overhaul) = serialno (heli_pandas)
    - Если ВС в капремонте (status != 'Закрыто') - устанавливаем статус 4 (Ремонт)
    - Переносим act_start_date → removal_date, sched_end_date → target_date
    """
    try:
        print("🚁 Обработка статусов планеров ВС (упрощенная логика)...")
        
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
            pandas_df['status'] = 0  # По умолчанию 0 (не определен)
            print("➕ Добавлена колонка 'status' со значением по умолчанию 0")
        
        # Добавляем колонку repair_days если ее нет
        if 'repair_days' not in pandas_df.columns:
            pandas_df['repair_days'] = None  # По умолчанию None (не определен)
            print("➕ Добавлена колонка 'repair_days' со значением по умолчанию None")
        
        # Определяем version_date для расчета repair_days
        version_date = pandas_df['version_date'].iloc[0] if 'version_date' in pandas_df.columns else date.today()
        print(f"📅 Используем version_date для расчета repair_days: {version_date}")
        
        # Создаем словарь для быстрого поиска: ac_registr -> данные капремонта
        status_dict = {}
        for _, row in status_overhaul_df.iterrows():
            ac_registr = str(row['ac_registr'])  # Простое приведение к строке
            status_dict[ac_registr] = {
                'status': row['status'],
                'act_start_date': row['act_start_date'],
                'sched_end_date': row['sched_end_date'],
                'act_end_date': row['act_end_date']
            }
        
        print(f"📋 Создан словарь капремонтов для {len(status_dict)} ВС")
        print(f"🔍 Номера ВС в капремонте: {sorted(status_dict.keys())}")
        
        # Ищем совпадения через прямое сопоставление serialno
        matches_found = 0
        status_updated_count = 0
        dates_updated_count = 0
        
        # Проверяем каждую запись в pandas_df
        for idx, row in pandas_df.iterrows():
            serialno = str(row['serialno'])
            
            # Проверяем есть ли такой serialno в данных капремонта
            if serialno in status_dict:
                overhaul_data = status_dict[serialno]
                
                print(f"✅ СОВПАДЕНИЕ: serialno={serialno} - капремонт: {overhaul_data['status']}")
                
                # Устанавливаем статус 4 (Ремонт) только если текущий статус = 0
                if pandas_df.at[idx, 'status'] == 0:
                    pandas_df.at[idx, 'status'] = 4
                    status_updated_count += 1
                    print(f"   ✅ status = 4 (Ремонт)")
                else:
                    print(f"   ⚠️ status уже установлен ({pandas_df.at[idx, 'status']}), не перезаписываем")
                
                # Переносим даты если они есть
                if overhaul_data['act_start_date']:
                    pandas_df.at[idx, 'removal_date'] = overhaul_data['act_start_date']
                    dates_updated_count += 1
                    print(f"   ✅ removal_date = {overhaul_data['act_start_date']}")
                
                if overhaul_data['sched_end_date']:
                    pandas_df.at[idx, 'target_date'] = overhaul_data['sched_end_date']
                    dates_updated_count += 1
                    print(f"   ✅ target_date = {overhaul_data['sched_end_date']}")
                
                # Рассчитываем repair_days
                if pandas_df.at[idx, 'target_date'] and pandas_df.at[idx, 'version_date']:
                    repair_days = (pandas_df.at[idx, 'target_date'] - pandas_df.at[idx, 'version_date']).days
                    pandas_df.at[idx, 'repair_days'] = repair_days
                    print(f"   ✅ repair_days = {repair_days} дней")
                
                matches_found += 1
        
        print(f"\n📊 Результаты сопоставления:")
        print(f"   ВС в капремонте: {len(status_overhaul_df)}")
        print(f"   Совпадений найдено: {matches_found}")
        print(f"   Статусов обновлено до 'Ремонт': {status_updated_count}")
        print(f"   Дат обновлено: {dates_updated_count}")
        
        if matches_found < len(status_overhaul_df):
            missing_count = len(status_overhaul_df) - matches_found
            print(f"⚠️ НЕ НАЙДЕНО {missing_count} ВС из капремонта в heli_pandas")
            print(f"💡 Возможные причины: дефекты данных, планеры не попали в фильтр MD_Components")
        
        return pandas_df
        
    except Exception as e:
        print(f"❌ Ошибка обработки статусов планеров: {e}")
        return pandas_df


def process_component_status(pandas_df):
    """
    Обрабатывает статусы остальных компонентов (не планеров) 
    
    ЗАГЛУШКА для первой итерации
    В будущих версиях будет реализована полная логика из архивного проекта:
    - Эксплуатация: на ВС, есть ресурс, исправен
    - Исправен: не на ВС, есть ресурс, исправен  
    - Ремонт: неисправен, но sne < beyond_repair
    - Хранение: неисправен и sne >= beyond_repair
    - Неактивно: прочие случаи
    """
    try:
        print("🔧 Обработка статусов компонентов (заглушка для v1)...")
        
        # Находим компоненты (не планеры)
        non_aircraft_mask = ~(
            (pandas_df['serialno'] == pandas_df['location']) & 
            pandas_df['location'].str.startswith('RA-', na=False)
        )
        
        non_aircraft_count = non_aircraft_mask.sum()
        print(f"🔧 Найдено {non_aircraft_count} компонентов (не планеров)")
        
        # Временно оставляем статус по умолчанию (0) для всех компонентов
        # В следующих итерациях здесь будет архивная логика:
        # if location.startswith("RA-") and condition == "ИСПРАВНЫЙ" and sne < (ll - daily_flight_hours):
        #     status = 2  # Эксплуатация
        # elif condition == "ИСПРАВНЫЙ" and sne < (ll - daily_flight_hours):
        #     status = 3  # Исправен
        # elif condition == "НЕИСПРАВНЫЙ" and sne < beyond_repair:
        #     status = 4  # Ремонт  
        # elif condition == "НЕИСПРАВНЫЙ" and sne >= beyond_repair:
        #     status = 5  # Хранение
        # else:
        #     status = 1  # Неактивно
        
        print(f"ℹ️ Компоненты получат статус по умолчанию 0 (обработка в следующих версиях)")
        
        return pandas_df
        
    except Exception as e:
        print(f"❌ Ошибка обработки статусов компонентов: {e}")
        return pandas_df


def process_status_field(pandas_df, client):
    """
    Главная функция обработки поля status
    
    Этап 1: Планеры ВС через status_overhaul (реализован)
    Этап 2: Остальные компоненты через архивную логику (заглушка)
    """
    try:
        print("\n🚀 === ОБРАБОТКА ПОЛЯ STATUS ===")
        
        original_count = len(pandas_df)
        print(f"📊 Обрабатываем {original_count:,} записей")
        
        # Этап 1: Планеры ВС через status_overhaul
        print("\n🚁 Этап 1: Планеры ВС через status_overhaul")
        pandas_df = process_aircraft_status(pandas_df, client)
        
        # Этап 2: Остальные компоненты (заглушка)
        print("\n🔧 Этап 2: Остальные компоненты")
        pandas_df = process_component_status(pandas_df)
        
        # Проверяем результаты
        if 'status' in pandas_df.columns:
            status_counts = pandas_df['status'].value_counts().sort_index()
            dict_status = load_dict_status_flat()
            
            print(f"\n📊 Итоговое распределение статусов:")
            for status_id, count in status_counts.items():
                status_name = dict_status.get(status_id, f"Неизвестно({status_id})")
                print(f"   {status_id} - {status_name}: {count:,} записей")
        
        print(f"\n✅ Обработка статусов завершена")
        return pandas_df
        
    except Exception as e:
        print(f"❌ Критическая ошибка обработки статусов: {e}")
        # Возвращаем исходный DataFrame с колонкой status по умолчанию
        if 'status' not in pandas_df.columns:
            pandas_df['status'] = 0
        return pandas_df 