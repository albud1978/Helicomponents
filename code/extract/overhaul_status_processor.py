#!/usr/bin/env python3
"""
Процессор статусов капитального ремонта для dual_loader.py

Функционал:
- Устанавливает статус 4 (Ремонт) для планеров ВС в капремонте
- Работает с данными status_overhaul (ВС в капремонте != 'Закрыто')
- Сопоставляет ac_registr (status_overhaul) с serialno (heli_pandas)
- Фильтрует по PLANER_PARTNOS (единый подход с другими процессорами)
- Используется на ЭТАПЕ 1 в dual_loader.py

Логика:
- Прямое сопоставление: ac_registr == serialno
- Фильтрация: partno IN PLANER_PARTNOS (не по group_by, т.к. он ещё не заполнен)
- Если ВС в капремонте → статус 4 (Ремонт)
- Переносит даты: act_start_date → removal_date, sched_end_date → target_date

Автор: AI Assistant
Дата: 2025-01-11
"""

import pandas as pd
from datetime import datetime, date


# Единый список partno планеров (как в program_ac_status_processor и inactive_planery_processor)
PLANER_PARTNOS = {'МИ-8Т', 'МИ-8П', 'МИ-8ПС', 'МИ-8ТП', 'МИ-8АМТ', 'МИ-8МТВ'}


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


def get_status_overhaul_data(client, version_date):
    """
    Получает данные из таблицы status_overhaul для конкретной версии.
    
    Логика: 
    - Фильтруем по version_date (каждый датасет имеет свои данные)
    - Возвращаем ВСЕ записи (включая 'Закрыто') для последующей обработки по датам
    """
    try:
        print(f"📋 Загружаем данные из status_overhaul для version_date={version_date}...")
        
        # Проверяем наличие таблицы
        check_table_query = "SELECT COUNT(*) FROM system.tables WHERE name = 'status_overhaul'"
        table_exists = client.execute(check_table_query)[0][0] > 0
        
        if not table_exists:
            print("❌ Таблица status_overhaul не найдена в ClickHouse!")
            print("💡 Сначала запустите: python3 code/extract/status_overhaul_loader.py")
            return None
        
        # Получаем данные с фильтром по version_date (ВСЕ записи, включая Закрыто)
        query = """
        SELECT 
            ac_registr,
            status,
            sched_start_date,
            act_start_date,
            sched_end_date,
            act_end_date
        FROM status_overhaul 
        WHERE version_date = %(version_date)s
        ORDER BY ac_registr
        """
        
        result = client.execute(query, {"version_date": version_date})
        
        if not result:
            print(f"ℹ️ Нет записей капитального ремонта для version_date={version_date}")
            return pd.DataFrame(columns=['ac_registr', 'status', 'sched_start_date', 'act_start_date', 'sched_end_date', 'act_end_date'])
        
        # Создаем DataFrame
        df = pd.DataFrame(result, columns=['ac_registr', 'status', 'sched_start_date', 'act_start_date', 'sched_end_date', 'act_end_date'])
        
        print(f"✅ Загружено {len(df)} записей капремонта ВС")
        print(f"📊 Статусы: {df['status'].value_counts().to_dict()}")
        
        return df
        
    except Exception as e:
        print(f"❌ Ошибка загрузки данных status_overhaul: {e}")
        return None


def process_aircraft_status(pandas_df, client):
    """
    Обрабатывает статусы планеров ВС на основе status_overhaul
    
    ЛОГИКА (ИСПРАВЛЕННАЯ):
    - Фильтр по version_date: каждый датасет имеет свои данные
    - Прямое сопоставление: ac_registr (status_overhaul) = serialno (heli_pandas)
    - Фильтрация: partno IN PLANER_PARTNOS
    - Если sched_end_date >= version_date → status_id=4 (в ремонте)
    - Если sched_end_date < version_date → status_id=2, ppr=0, repair_days=0 (ремонт завершён)
    - Переносим act_start_date → removal_date, sched_end_date → target_date
    """
    try:
        print("🚁 Обработка статусов планеров ВС (фильтрация по PLANER_PARTNOS)...")
        
        # Определяем version_date ДО загрузки данных
        version_date = pandas_df['version_date'].iloc[0] if 'version_date' in pandas_df.columns else date.today()
        print(f"📅 Используем version_date: {version_date}")
        
        # Получаем данные по капремонту С ФИЛЬТРОМ ПО version_date
        status_overhaul_df = get_status_overhaul_data(client, version_date)
        if status_overhaul_df is None:
            print("⚠️ Не удалось загрузить данные status_overhaul - пропускаем обработку статусов ВС")
            return pandas_df
        
        if len(status_overhaul_df) == 0:
            print("ℹ️ Нет записей капремонтов для данной версии - все планеры получат статус по умолчанию")
            return pandas_df
        
        # Добавляем колонку status_id если ее нет
        if 'status_id' not in pandas_df.columns:
            pandas_df['status_id'] = 0  # По умолчанию 0 (не определен)
            print("➕ Добавлена колонка 'status_id' со значением по умолчанию 0")
        
        # Добавляем колонку repair_days если ее нет
        if 'repair_days' not in pandas_df.columns:
            pandas_df['repair_days'] = None  # По умолчанию None (не определен)
            print("➕ Добавлена колонка 'repair_days' со значением по умолчанию None")
        
        # Добавляем колонку ppr если ее нет
        if 'ppr' not in pandas_df.columns:
            pandas_df['ppr'] = 0
            print("➕ Добавлена колонка 'ppr' со значением по умолчанию 0")
        
        # Создаем словарь для быстрого поиска: ac_registr -> данные капремонта
        status_dict = {}
        for _, row in status_overhaul_df.iterrows():
            ac_registr = str(row['ac_registr'])
            status_dict[ac_registr] = {
                'status': row['status'],
                'sched_start_date': row['sched_start_date'],
                'act_start_date': row['act_start_date'],
                'sched_end_date': row['sched_end_date'],
                'act_end_date': row['act_end_date']
            }
        
        print(f"📋 Создан словарь капремонтов для {len(status_dict)} ВС")
        
        # Счётчики
        matches_found = 0
        status_to_repair = 0  # status_id=4 (ремонт идёт)
        status_to_ops = 0     # status_id=2 (ремонт завершён)
        dates_updated_count = 0
        
        # Проверяем каждую запись в pandas_df
        for idx, row in pandas_df.iterrows():
            # Фильтруем только планеры по PLANER_PARTNOS
            partno = str(row.get('partno', ''))
            if partno not in PLANER_PARTNOS:
                continue
            
            serialno = str(row['serialno'])
            
            # Проверяем есть ли такой serialno в данных капремонта
            if serialno in status_dict:
                overhaul_data = status_dict[serialno]
                sched_end_date = overhaul_data.get('sched_end_date')
                
                print(f"✅ СОВПАДЕНИЕ: serialno={serialno} ({partno}) - капремонт: {overhaul_data['status']}, sched_end={sched_end_date}")
                
                # Переносим даты ВСЕГДА (независимо от статуса)
                if overhaul_data['act_start_date']:
                    pandas_df.at[idx, 'removal_date'] = overhaul_data['act_start_date']
                    dates_updated_count += 1
                
                if sched_end_date:
                    pandas_df.at[idx, 'target_date'] = sched_end_date
                    dates_updated_count += 1
                
                # ГЛАВНАЯ ЛОГИКА: определяем статус по sched_end_date vs version_date
                if sched_end_date and sched_end_date < version_date:
                    # Ремонт ЗАВЕРШЁН (target_date в прошлом)
                    pandas_df.at[idx, 'status_id'] = 2
                    pandas_df.at[idx, 'ppr'] = 0           # Обнуляем ppr после ремонта
                    pandas_df.at[idx, 'repair_days'] = 0   # Ремонт завершён
                    status_to_ops += 1
                    print(f"   ✅ status_id=2 (Эксплуатация), ppr=0, repair_days=0 - ремонт завершён")
                else:
                    # Ремонт ИДЁТ (target_date в будущем или сегодня)
                    # Проверяем что дата начала в прошлом
                    sched_start_date = overhaul_data.get('sched_start_date')
                    act_start_date = overhaul_data.get('act_start_date')
                    
                    start_in_past = False
                    if sched_start_date and sched_start_date < version_date:
                        start_in_past = True
                    if act_start_date and act_start_date < version_date:
                        start_in_past = True
                    
                    if start_in_past:
                        pandas_df.at[idx, 'status_id'] = 4
                        status_to_repair += 1
                        print(f"   ✅ status_id=4 (Ремонт) - ремонт в процессе до {sched_end_date}")
                    else:
                        print(f"   ⚠️ Ремонт ещё не начался (start >= version_date) - НЕ устанавливаем статус")
                
                matches_found += 1
        
        print(f"\n📊 Результаты сопоставления:")
        print(f"   ВС в таблице капремонта: {len(status_overhaul_df)}")
        print(f"   Совпадений найдено: {matches_found}")
        print(f"   → status_id=4 (ремонт идёт): {status_to_repair}")
        print(f"   → status_id=2 (ремонт завершён): {status_to_ops}")
        print(f"   Дат обновлено: {dates_updated_count}")
        
        return pandas_df
        
    except Exception as e:
        print(f"❌ Ошибка обработки статусов планеров: {e}")
        import traceback
        traceback.print_exc()
        return pandas_df


def process_component_status(pandas_df):
    """
    Обрабатывает статусы остальных компонентов (не планеров) 
    
    ЗАГЛУШКА для первой итерации
    """
    try:
        print("🔧 Обработка статусов компонентов (заглушка для v1)...")
        print(f"ℹ️ Компоненты получат статус по умолчанию 0 (обработка в следующих этапах)")
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
        if 'status_id' in pandas_df.columns:
            status_counts = pandas_df['status_id'].value_counts().sort_index()
            dict_status = load_dict_status_flat()
            
            print(f"\n📊 Итоговое распределение статусов:")
            for status_id, count in status_counts.items():
                status_name = dict_status.get(status_id, f"Неизвестно({status_id})")
                print(f"   {status_id} - {status_name}: {count:,} записей")
        
        print(f"\n✅ Обработка статусов завершена")
        return pandas_df
        
    except Exception as e:
        print(f"❌ Критическая ошибка обработки статусов: {e}")
        # Возвращаем исходный DataFrame с колонкой status_id по умолчанию
        if 'status_id' not in pandas_df.columns:
            pandas_df['status_id'] = 0
        return pandas_df
