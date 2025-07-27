#!/usr/bin/env python3
"""
Flame GPU Helicopter Component Lifecycle Model
Модель имитационного моделирования оборота планеров

Дата создания: 24-07-2025
Автор: ETL Helicopter Project
Версия: 1.0

Архитектура: множественные слои по статусам планеров
- 6 Agent States по status_id (1-6)
- 2 глобальных RTC слоя (spawn + balance)
- Параллельное выполнение без зависимостей
"""

import sys
import os
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import json

# Импорты для подключения к ClickHouse
sys.path.append('/home/budnik_an/cube linux/cube/code/utils')
from config_loader import load_clickhouse_config

# Настройка логирования - используем общий Transform лог
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/transform_master.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class FlameGPUHelicopterModel:
    """
    Основной класс модели Flame GPU для планеров
    
    Реализует:
    - Загрузку данных из ClickHouse в MacroProperty
    - Создание агентов планеров с нормативами
    - 8 RTC функций (6 слоев + 2 глобальных)
    - Логирование результатов в LoggingLayer_Planes
    """
    
    def __init__(self, simulation_start_date: str = None, 
                 simulation_days: int = 365):
        """
        Инициализация модели
        
        Args:
            simulation_start_date: Дата начала симуляции (YYYY-MM-DD). 
                                 Если None, определяется автоматически из heli_pandas
            simulation_days: Количество дней симуляции
        """
        # Временная дата, будет перезаписана в load_macro_properties
        self.simulation_start_date = datetime.strptime(simulation_start_date, "%Y-%m-%d") if simulation_start_date else datetime(2025, 1, 1)
        self.simulation_days = simulation_days
        self.auto_detect_start_date = simulation_start_date is None
        self.current_day = 0
        self.version_id = f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # MacroProperty структуры
        self.macro_property_1 = {}  # md_components (нормативы)
        self.macro_property_2 = []  # LoggingLayer_Planes (результат)
        self.macro_property_3 = {}  # heli_pandas (агенты)
        self.macro_property_4 = {}  # flight_program_ac (триггеры)
        self.macro_property_5 = {}  # flight_program_fl (налеты)
        
        # Словарь field_id согласно dict_digital_values_flat
        self.field_mapping = {
            'partno_comp': 1, 'group_by': 10, 'll_mi8': 12, 'll_mi17': 15,
            'oh_mi8': 13, 'oh_mi17': 16, 'repair_time': 17, 'partout_time': 18,
            'assembly_time': 19, 'br': 20, 'dates': 21, 'aircraft_number': 22,
            'daily_hours': 24, 'status_id': 25, 'partout_trigger': 26,
            'assembly_trigger': 27, 'mfg_date': 30, 'active_trigger': 31,
            'partseqno_i': 34, 'psn': 35, 'address_i': 36, 'ac_type_mask': 37,
            'sne': 38, 'ppr': 39, 'repair_days': 40, 'lease_restricted': 41,
            'trigger_program_mi8': 42, 'trigger_program_mi17': 43, 'new_counter_mi17': 44
        }
        
        # Агенты планеров
        self.agents = {}
        self.next_agent_id = 100000  # Для новорожденных МИ-17 (диапазон 100000-150000)
        
        # Хардкод константы согласно transform.md
        self.HARDCODE_CONSTANTS = {
            'MI8_TYPE_MASK': 32,
            'MI17_TYPE_MASK': 64,
            'STATUS_INACTIVE': 1,
            'STATUS_OPERATIONS': 2,
            'STATUS_STOCK': 3,
            'STATUS_REPAIR': 4,
            'STATUS_RESERVE': 5,
            'STATUS_STORE': 6,
            'MI17_BASELINE_PARTNO': 'MI17_BASELINE',
            'MIN_FLIGHT_HOURS': 0.5,
            'MAX_FLIGHT_HOURS': 12.0,
            'REPAIR_THRESHOLD_DAYS': 365,
            'SPAWN_STATUS': 3,  # Новорожденные начинают со склада
            'SPAWNED_ID_MIN': 100000,  # Минимальный ID новорожденных
            'SPAWNED_ID_MAX': 150000   # Максимальный ID новорожденных
        }
        
        logger.info(f"Модель инициализирована: {self.version_id}")
        logger.info(f"Период симуляции: {simulation_start_date} + {simulation_days} дней")

    def load_macro_properties(self) -> bool:
        """
        ЭТАП 1: Загрузка данных из ClickHouse в MacroProperty структуры
        
        Returns:
            bool: True если загрузка успешна
        """
        try:
            logger.info("ЭТАП 1: Загрузка MacroProperty из ClickHouse")
            
            # Используем clickhouse_connect для pandas совместимости
            import clickhouse_connect
            sys.path.append('/home/budnik_an/cube linux/cube/code/utils')
            from config_loader import load_clickhouse_config
            
            config = load_clickhouse_config()
            client = clickhouse_connect.get_client(
                host=config['host'],
                port=8123,  # HTTP порт для clickhouse_connect
                username=config['user'],
                password=config['password'],
                database=config['database']
            )
            
            # MacroProperty1: md_components (нормативы компонентов)
            logger.info("Загрузка MacroProperty1: md_components")
            query1 = """
            SELECT 
                partno_comp, group_by, ll_mi8, ll_mi17, oh_mi8, oh_mi17,
                repair_time, partout_time, assembly_time, br
            FROM md_components 
            WHERE partno_comp IS NOT NULL
            """
            result1 = client.query(query1)
            df1 = pd.DataFrame(result1.result_rows, columns=result1.column_names)
            self.macro_property_1 = df1.set_index('partno_comp').to_dict('index')
            logger.info(f"MacroProperty1: {len(self.macro_property_1)} записей нормативов")
            
            # Получаем планерные partno_comp для фильтрации
            mi8_planery = df1[df1['group_by'] == 1]['partno_comp'].tolist()
            mi17_planery = df1[df1['group_by'] == 2]['partno_comp'].tolist()
            planery_partno = mi8_planery + mi17_planery
            
            logger.info(f"Планерные partno_comp: МИ-8={len(mi8_planery)}, МИ-17={len(mi17_planery)}")
            
            # MacroProperty3: heli_pandas (ТОЛЬКО планеры по partseqno_i)
            logger.info("Загрузка MacroProperty3: heli_pandas (только планеры)")
            
            # Формируем IN clause для планерных partseqno_i
            planery_str = ','.join(map(str, planery_partno))
            
            query3 = f"""
            SELECT 
                serialno, aircraft_number, partseqno_i, psn, address_i,
                ac_type_mask, sne, ppr, status_id, lease_restricted, mfg_date,
                version_date
            FROM heli_pandas 
            WHERE serialno IS NOT NULL
            AND partseqno_i IN ({planery_str})
            ORDER BY serialno, version_date DESC
            """
            result3 = client.query(query3)
            df3 = pd.DataFrame(result3.result_rows, columns=result3.column_names)
            
            # Берем последнюю версию для каждого serialno (версионность данных)
            df3_latest = df3.drop_duplicates(subset=['serialno'], keep='first')
            self.macro_property_3 = df3_latest.set_index('serialno').to_dict('index')
            
            total_agents = len(df3)
            unique_agents = len(df3_latest)
            logger.info(f"MacroProperty3: {unique_agents} агентов планеров (из {total_agents} версионных записей)")
            
            # Статистика по типам планеров
            mi8_count = len(df3_latest[df3_latest['partseqno_i'].isin(mi8_planery)])
            mi17_count = len(df3_latest[df3_latest['partseqno_i'].isin(mi17_planery)])
            logger.info(f"Агенты: МИ-8={mi8_count}, МИ-17={mi17_count}")
            
            # Автоматическое определение даты начала симуляции из version_date
            if self.auto_detect_start_date and not df3_latest.empty:
                latest_version_date = df3_latest['version_date'].max()
                self.simulation_start_date = datetime.combine(latest_version_date, datetime.min.time())
                logger.info(f"🗓️ Дата начала симуляции автоматически установлена: {self.simulation_start_date.strftime('%Y-%m-%d')}")
            
            # MacroProperty4: flight_program_ac (триггеры)
            logger.info("Загрузка MacroProperty4: flight_program_ac")
            query4 = """
            SELECT 
                trigger_program_mi8, trigger_program_mi17, new_counter_mi17
            FROM flight_program_ac 
            LIMIT 1
            """
            result4 = client.query(query4)
            if len(result4.result_rows) > 0:
                df4 = pd.DataFrame(result4.result_rows, columns=result4.column_names)
                self.macro_property_4 = df4.iloc[0].to_dict()
            logger.info(f"MacroProperty4: Триггеры загружены")
            
            # MacroProperty5: flight_program_fl (налеты по планерам)
            logger.info("Загрузка MacroProperty5: flight_program_fl")
            
            # Получаем список планерных aircraft_number из MacroProperty3
            planery_aircraft_numbers = list(self.macro_property_3.keys())
            aircraft_str = ','.join(f"'{str(ac)}'" for ac in planery_aircraft_numbers)
            
            # Используем автоматически определенную дату начала симуляции
            start_date_str = self.simulation_start_date.strftime('%Y-%m-%d')
            
            query5 = f"""
            SELECT dates, aircraft_number, daily_hours
            FROM flight_program_fl 
            WHERE dates >= toDate('{start_date_str}') 
            AND aircraft_number IN ({aircraft_str})
            ORDER BY dates, aircraft_number
            """
            result5 = client.query(query5)
            df5 = pd.DataFrame(result5.result_rows, columns=result5.column_names)
            
            if len(df5) > 0:
                # Создаем тензор aircraft_numbers × dates (планеры → дни)
                pivot_df = df5.pivot(
                    index='aircraft_number',
                    columns='dates', 
                    values='daily_hours'
                ).fillna(0)
                
                # Преобразуем в словарь [aircraft_number][date] = daily_hours
                self.macro_property_5 = {}
                for aircraft_num in pivot_df.index:
                    self.macro_property_5[aircraft_num] = {}
                    for date_val in pivot_df.columns:
                        self.macro_property_5[aircraft_num][date_val] = pivot_df.loc[aircraft_num, date_val]
                
                unique_dates = df5['dates'].nunique()
                unique_aircraft = df5['aircraft_number'].nunique()
                logger.info(f"MacroProperty5: Тензор {unique_aircraft} планеров × {unique_dates} дней = {len(df5)} записей")
            else:
                self.macro_property_5 = {}
                logger.warning("MacroProperty5: Нет данных flight_program_fl для планеров")
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка загрузки MacroProperty: {e}")
            return False

    def create_agents(self) -> bool:
        """
        ЭТАП 2: Создание агентов планеров с обогащением нормативами
        
        Returns:
            bool: True если создание успешно
        """
        try:
            logger.info("ЭТАП 2: Создание агентов планеров")
            
            for serialno, agent_data in self.macro_property_3.items():
                # Получаем нормативы из MacroProperty1 по partseqno_i
                partno = agent_data.get('partseqno_i', '')
                norms = self.macro_property_1.get(partno, {})
                
                # Определяем тип ВС для выбора нормативов
                ac_type = agent_data.get('ac_type_mask', 32)
                ll_field = 'll_mi17' if ac_type == 64 else 'll_mi8'
                oh_field = 'oh_mi17' if ac_type == 64 else 'oh_mi8'
                
                # Создаем агента с полным набором данных
                agent = {
                    # Идентификация
                    'agent_id': serialno,
                    'aircraft_number': agent_data.get('aircraft_number', serialno),
                    'ac_type_mask': ac_type,
                    'partseqno_i': partno,
                    'mfg_date': agent_data.get('mfg_date'),
                    
                    # Динамические переменные (изменяются в симуляции)
                    'status_id': agent_data.get('status_id', 3),
                    'sne': float(agent_data.get('sne', 0)),
                    'ppr': float(agent_data.get('ppr', 0)),
                    'repair_days': int(agent_data.get('repair_days', 0)),
                    
                    # Нормативы из heli_pandas (реальные данные, БЕЗ дефолтов)
                    'll': float(agent_data['ll']),
                    'oh': float(agent_data['oh']),
                    'br': float(agent_data['br']),
                    'repair_time': int(norms.get('repair_time', 180)),  # Для планеров 180 дней по умолчанию
                    'partout_time': int(norms.get('partout_time', 7)),  # Для планеров 7 дней
                    'assembly_time': int(norms.get('assembly_time', 30)),  # Для планеров 30 дней
                    
                    # Триггерные переменные
                    'active_trigger': 0,
                    'partout_trigger': 0,
                    'assembly_trigger': 0,
                    
                    # Вспомогательные поля
                    'lease_restricted': agent_data.get('lease_restricted', 0),
                    'psn': agent_data.get('psn', ''),
                    'address_i': agent_data.get('address_i', '')
                }
                
                self.agents[serialno] = agent
            
            logger.info(f"Создано {len(self.agents)} агентов планеров")
            
            # Статистика по статусам
            status_counts = {}
            for agent in self.agents.values():
                status = agent['status_id']
                status_counts[status] = status_counts.get(status, 0) + 1
            
            logger.info(f"Распределение по статусам: {status_counts}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка создания агентов: {e}")
            return False

    def rtc_inactive_layer(self, day: int) -> None:
        """
        RTC функция слоя 1: Неактивные планеры (status_id = 1)
        
        Логика:
        - Проверка active_trigger для выхода из неактивности
        - Переход в статус 3 (склад) при активации
        """
        inactive_agents = [a for a in self.agents.values() if a['status_id'] == 1]
        
        logger.info(f"🔄 ДЕНЬ {day} rtc_inactive_layer: Начинаем обработку {len(inactive_agents)} неактивных планеров")
        
        for agent in inactive_agents:
            # Проверяем триггер активации
            if agent.get('active_trigger', 0) > 0:
                target_date = agent['active_trigger']
                current_date = self.simulation_start_date + timedelta(days=day)
                
                if current_date.timestamp() >= target_date:
                    agent['status_id'] = self.HARDCODE_CONSTANTS['STATUS_STOCK']
                    agent['active_trigger'] = 0
                    logger.debug(f"Агент {agent['agent_id']} активирован в день {day}")

    def rtc_ops_layer(self, day: int) -> None:
        """
        RTC функция слоя 2: Операционные планеры (status_id = 2)
        
        Логика согласно transform.md:
        - Накопление налета из MacroProperty5
        - 3 ресурсных триггера в порядке приоритета:
          1. Исчерпание назначенного ресурса (ll) → Хранение (6)
          2. МРР исчерпан + НЕремонтопригодный → Хранение (6) 
          3. МРР исчерпан + ремонтопригодный → Ремонт (4)
        """
        ops_agents = [a for a in self.agents.values() if a['status_id'] == 2]
        current_date = self.simulation_start_date + timedelta(days=day)
        
        logger.info(f"🔄 ДЕНЬ {day} rtc_ops_layer: Начинаем обработку {len(ops_agents)} планеров в эксплуатации")
        
        for agent in ops_agents:
            aircraft_num = agent['aircraft_number']
            
            # Получаем суточный налет из MacroProperty5
            daily_flight = 0
            if aircraft_num in self.macro_property_5:
                # Используем .date() для совместимости типов
                current_date_only = current_date.date()
                daily_flight = self.macro_property_5[aircraft_num].get(current_date_only, 0)
            
            # Накопление наработки
            agent['sne'] += daily_flight
            agent['ppr'] += daily_flight
            
            # Получение нормативов из переменных агента
            ll = agent['ll']        # назначенный ресурс
            oh = agent['oh']        # межремонтный ресурс  
            br = agent['br']        # граница ремонтопригодности
            
            # РЕСУРСНЫЙ ТРИГГЕР 1: Исчерпание назначенного ресурса
            if (ll - agent['sne']) < daily_flight:
                old_status = agent['status_id']
                agent['status_id'] = self.HARDCODE_CONSTANTS['STATUS_STORE']  # → "Хранение"
                logger.info(f"🚨 ДЕНЬ {day} rtc_ops_layer: Планер {aircraft_num} {old_status}→6 (Хранение) - исчерпан ll: {ll-agent['sne']} < {daily_flight}")
                continue
            
            # РЕСУРСНЫЙ ТРИГГЕР 2: МРР исчерпан + НЕ ремонтопригодный
            if (oh - agent['ppr']) < daily_flight and agent['sne'] >= br:
                old_status = agent['status_id']
                agent['status_id'] = self.HARDCODE_CONSTANTS['STATUS_STORE']  # → "Хранение"
                logger.info(f"🚨 ДЕНЬ {day} rtc_ops_layer: Планер {aircraft_num} {old_status}→6 (Хранение) - МРР исчерпан + неремонтопригоден: oh={oh}, ppr={agent['ppr']}, sne={agent['sne']}, br={br}")
                continue
            
            # РЕСУРСНЫЙ ТРИГГЕР 3: МРР исчерпан + ремонтопригодный
            if (oh - agent['ppr']) < daily_flight and agent['sne'] < br:
                old_status = agent['status_id']
                agent['status_id'] = self.HARDCODE_CONSTANTS['STATUS_REPAIR']  # → "Ремонт"
                agent['repair_days'] = 1  # Установка счетчика дней ремонта
                
                # Формирование триггеров ремонта (как timestamp для совместимости)
                partout_date = current_date + timedelta(days=agent['partout_time'])
                assembly_date = current_date + timedelta(days=agent['repair_time'] - agent['assembly_time'])
                
                agent['partout_trigger'] = int(partout_date.timestamp())
                agent['assembly_trigger'] = int(assembly_date.timestamp())
                
                logger.info(f"🔧 ДЕНЬ {day} rtc_ops_layer: Планер {aircraft_num} {old_status}→4 (Ремонт) - МРР исчерпан + ремонтопригоден: oh={oh}, ppr={agent['ppr']}, sne={agent['sne']}, br={br}")
                continue

    def rtc_stock_layer(self, day: int) -> None:
        """
        RTC функция слоя 3: Складские планеры (status_id = 3)
        
        Логика:
        - Проверка готовности к вводу в эксплуатацию
        - Переход в операции при балансировке
        """
        stock_agents = [a for a in self.agents.values() if a['status_id'] == 3]
        
        logger.info(f"🔄 ДЕНЬ {day} rtc_stock_layer: Начинаем обработку {len(stock_agents)} планеров на складе")
        
        # Простая логика готовности - проверяем наработку
        for agent in stock_agents:
            if (agent['sne'] < agent['ll'] and 
                agent['ppr'] < agent['oh'] and
                not self._check_beyond_repair(agent)):
                # Агент готов к эксплуатации (будет активирован в balance)
                agent['ready_for_ops'] = True
            else:
                agent['ready_for_ops'] = False

    def rtc_repair_layer(self, day: int) -> None:
        """
        RTC функция слоя 4: Ремонтные планеры (status_id = 4)
        
        Логика согласно transform.md:
        - Увеличение счетчика repair_days
        - Agent триггер: завершение ремонта при РАВЕНСТВЕ repair_time
        - Переход в РЕЗЕРВ (5) после ремонта, НЕ склад!
        """
        repair_agents = [a for a in self.agents.values() if a['status_id'] == 4]
        
        for agent in repair_agents:
            agent['repair_days'] += 1
            
            # Agent триггер: завершение ремонта
            if agent['repair_days'] == agent['repair_time']:  # РАВЕНСТВО, не больше
                agent['status_id'] = self.HARDCODE_CONSTANTS['STATUS_RESERVE']  # → "Резерв" (НЕ исправен!)
                agent['ppr'] = 0  # Сброс наработки после ремонта
                agent['repair_days'] = 0  # Сброс счетчика
                agent['partout_trigger'] = 0
                agent['assembly_trigger'] = 0
                logger.debug(f"Агент {agent['agent_id']} завершил ремонт → резерв")

    def rtc_reserve_layer(self, day: int) -> None:
        """
        RTC функция слоя 5: Резервные планеры (status_id = 5)
        
        Логика:
        - Статический слой без изменений
        - Возможное возвращение в эксплуатацию при критической нехватке
        """
        reserve_agents = [a for a in self.agents.values() if a['status_id'] == 5]
        
        # В текущей версии резерв статичен
        # Будущее развитие: логика реактивации при критической нехватке
        for agent in reserve_agents:
            agent['reserve_days'] = agent.get('reserve_days', 0) + 1

    def rtc_store_layer(self, day: int) -> None:
        """
        RTC функция слоя 6: Хранимые планеры (status_id = 6)
        
        Логика:
        - Финальное состояние
        - Статический слой без переходов
        """
        store_agents = [a for a in self.agents.values() if a['status_id'] == 6]
        
        # Хранение - финальное состояние
        for agent in store_agents:
            agent['store_days'] = agent.get('store_days', 0) + 1

    def rtc_spawn_layer(self, day: int) -> None:
        """
        Глобальная RTC функция: Рождение новых МИ-17
        
        Логика:
        - Проверка new_counter_mi17 из MacroProperty4
        - Создание новых агентов МИ-17 в статусе 3 (склад)
        """
        if not self.macro_property_4:
            return
            
        spawn_count = self.macro_property_4.get('new_counter_mi17', 0)
        
        # Простая логика спавна - 1 раз в месяц при нехватке МИ-17
        if day % 30 == 0 and spawn_count > 0:
            mi17_ops_count = len([a for a in self.agents.values() 
                                 if a['ac_type_mask'] == 64 and a['status_id'] == 2])
            target_mi17 = self.macro_property_4.get('trigger_program_mi17', 20)
            
            if mi17_ops_count < target_mi17 and self.next_agent_id <= self.HARDCODE_CONSTANTS['SPAWNED_ID_MAX']:
                # Создаем нового МИ-17 (проверяем лимит диапазона)
                new_agent_id = self.next_agent_id
                self.next_agent_id += 1
                
                current_date = self.simulation_start_date + timedelta(days=day)
                
                new_agent = {
                    'agent_id': new_agent_id,
                    'aircraft_number': new_agent_id,
                    'ac_type_mask': self.HARDCODE_CONSTANTS['MI17_TYPE_MASK'],
                    'partseqno_i': self.HARDCODE_CONSTANTS['MI17_BASELINE_PARTNO'],
                    'mfg_date': current_date,
                    'status_id': self.HARDCODE_CONSTANTS['SPAWN_STATUS'],
                    'sne': 0.0,
                    'ppr': 0.0,
                    'repair_days': 0,
                    'll': 3000.0,
                    'oh': 1500.0,
                    'br': 0.01,
                    'repair_time': 45,
                    'partout_time': 15,
                    'assembly_time': 30,
                    'active_trigger': 0,
                    'partout_trigger': 0,
                    'assembly_trigger': 0,
                    'lease_restricted': 0,
                    'psn': f"NEW_{new_agent_id}",
                    'address_i': 'SPAWN'
                }
                
                self.agents[new_agent_id] = new_agent
                logger.info(f"Spawned новый МИ-17: {new_agent_id} в день {day}")

    def rtc_balance_layer(self, day: int) -> None:
        """
        Глобальная RTC функция: Балансировка программы полетов
        
        Логика:
        - Триггеры показывают ДЕФИЦИТ/ИЗБЫТОК относительно текущего состояния
        - Отрицательные триггеры = нужно ДОБАВИТЬ планеров
        - Положительные триггеры = нужно УБРАТЬ планеров
        - Балансировка выполняется только в первый день (day == 0)
        """
        if not self.macro_property_4 or day != 0:
            return  # Балансировка только в первый день
            
        trigger_mi8 = self.macro_property_4.get('trigger_program_mi8', 0)
        trigger_mi17 = self.macro_property_4.get('trigger_program_mi17', 0)
        
        # Текущее количество в операциях
        mi8_ops = len([a for a in self.agents.values() 
                     if a['ac_type_mask'] == 32 and a['status_id'] == 2])
        mi17_ops = len([a for a in self.agents.values() 
                      if a['ac_type_mask'] == 64 and a['status_id'] == 2])
        
        logger.info(f"Балансировка: МИ-8 в операциях={mi8_ops}, триггер={trigger_mi8}")
        logger.info(f"Балансировка: МИ-17 в операциях={mi17_ops}, триггер={trigger_mi17}")
        
        # Балансировка МИ-8: trigger_mi8 = дефицит/избыток планеров
        if trigger_mi8 < 0:
            # Дефицит МИ-8: активация по правильным приоритетам
            deficit = abs(trigger_mi8)
            activated = 0
            
            # Приоритет 1: Склад (status_id = 3)
            stock_mi8 = [a for a in self.agents.values() 
                        if a['ac_type_mask'] == 32 and a['status_id'] == 3]
            
            for agent in stock_mi8[:deficit - activated]:
                agent['status_id'] = self.HARDCODE_CONSTANTS['STATUS_OPERATIONS']
                activated += 1
                logger.info(f"МИ-8 {agent['agent_id']} со склада в операции")
            
            # Приоритет 2: Резерв (status_id = 5)
            if activated < deficit:
                reserve_mi8 = [a for a in self.agents.values() 
                              if a['ac_type_mask'] == 32 and a['status_id'] == 5]
                
                for agent in reserve_mi8[:deficit - activated]:
                    agent['status_id'] = self.HARDCODE_CONSTANTS['STATUS_OPERATIONS']
                    activated += 1
                    logger.info(f"МИ-8 {agent['agent_id']} из резерва в операции")
            
            # Приоритет 3: Неактивные (status_id = 1) с проверкой времени и сортировкой по mfg_date
            if activated < deficit:
                inactive_mi8 = [a for a in self.agents.values() 
                               if a['ac_type_mask'] == 32 and a['status_id'] == 1]
                
                # Сортируем по максимальной mfg_date (самые новые первые)
                inactive_mi8.sort(key=lambda x: x.get('mfg_date', datetime.min), reverse=True)
                
                for agent in inactive_mi8:
                    if activated >= deficit:
                        break
                        
                    # Проверка времени: (текущая_дата_симуляции - version_date) >= repair_time
                    if self._check_activation_time_feasibility(agent, day):
                        agent['status_id'] = self.HARDCODE_CONSTANTS['STATUS_OPERATIONS']
                        agent['active_trigger'] = day  # Устанавливаем триггер
                        activated += 1
                        logger.info(f"МИ-8 {agent['agent_id']} из неактивных в операции")
                    # Если время не подходит, просто пропускаем этого агента
            
            if activated < deficit:
                logger.warning(f"МИ-8: активировано {activated} из {deficit} требуемых (недостаток планеров)")
                
        elif trigger_mi8 > 0:
            # Избыток МИ-8: снимаем trigger_mi8 планеров с эксплуатации
            excess = trigger_mi8
            excess_mi8 = [a for a in self.agents.values() 
                         if a['ac_type_mask'] == 32 and a['status_id'] == 2]
            
            for agent in excess_mi8[:excess]:
                agent['status_id'] = self.HARDCODE_CONSTANTS['STATUS_STOCK']
                logger.debug(f"МИ-8 {agent['agent_id']} снят с эксплуатации (избыток)")
        
        # Аналогичная балансировка для МИ-17
        if trigger_mi17 < 0:
            # Дефицит МИ-17: активация по правильным приоритетам
            deficit = abs(trigger_mi17)
            activated = 0
            
            # Приоритет 1: Склад (status_id = 3)
            stock_mi17 = [a for a in self.agents.values() 
                         if a['ac_type_mask'] == 64 and a['status_id'] == 3]
            
            for agent in stock_mi17[:deficit - activated]:
                agent['status_id'] = self.HARDCODE_CONSTANTS['STATUS_OPERATIONS']
                activated += 1
                logger.info(f"МИ-17 {agent['agent_id']} со склада в операции")
            
            # Приоритет 2: Резерв (status_id = 5)
            if activated < deficit:
                reserve_mi17 = [a for a in self.agents.values() 
                               if a['ac_type_mask'] == 64 and a['status_id'] == 5]
                
                for agent in reserve_mi17[:deficit - activated]:
                    agent['status_id'] = self.HARDCODE_CONSTANTS['STATUS_OPERATIONS']
                    activated += 1
                    logger.info(f"МИ-17 {agent['agent_id']} из резерва в операции")
            
            # Приоритет 3: Неактивные (status_id = 1) с проверкой времени и сортировкой по mfg_date
            if activated < deficit:
                inactive_mi17 = [a for a in self.agents.values() 
                                if a['ac_type_mask'] == 64 and a['status_id'] == 1]
                
                # Сортируем по максимальной mfg_date (самые новые первые)
                inactive_mi17.sort(key=lambda x: x.get('mfg_date', datetime.min), reverse=True)
                
                for agent in inactive_mi17:
                    if activated >= deficit:
                        break
                        
                    # Проверка времени: (текущая_дата_симуляции - version_date) >= repair_time
                    if self._check_activation_time_feasibility(agent, day):
                        agent['status_id'] = self.HARDCODE_CONSTANTS['STATUS_OPERATIONS']
                        agent['active_trigger'] = day  # Устанавливаем триггер
                        activated += 1
                        logger.info(f"МИ-17 {agent['agent_id']} из неактивных в операции")
                    # Если время не подходит, просто пропускаем этого агента
            
            if activated < deficit:
                logger.warning(f"МИ-17: активировано {activated} из {deficit} требуемых (недостаток планеров)")
                
        elif trigger_mi17 > 0:
            # Избыток МИ-17: снимаем trigger_mi17 планеров с эксплуатации
            excess = trigger_mi17
            excess_mi17 = [a for a in self.agents.values() 
                          if a['ac_type_mask'] == 64 and a['status_id'] == 2]
            
            for agent in excess_mi17[:excess]:
                agent['status_id'] = self.HARDCODE_CONSTANTS['STATUS_STOCK']
                logger.debug(f"МИ-17 {agent['agent_id']} снят с эксплуатации (избыток)")

    def _check_beyond_repair(self, agent: Dict) -> bool:
        """
        Проверка ремонтопригодности агента
        
        Args:
            agent: Словарь данных агента
            
        Returns:
            bool: True если агент beyond repair
        """
        # Базовая логика beyond repair
        age_factor = (agent['sne'] / agent['ll']) if agent['ll'] > 0 else 0
        wear_factor = (agent['ppr'] / agent['oh']) if agent['oh'] > 0 else 0
        
        # Вероятностная модель с br коэффициентом
        beyond_repair_prob = agent['br'] * (age_factor + wear_factor)
        
        return np.random.random() < beyond_repair_prob

    def _check_activation_time_feasibility(self, agent: Dict, current_day: int) -> bool:
        """
        Проверка возможности активации неактивного планера согласно алгоритму:
        (текущая дата симуляции - version_date) >= repair_time для partno_comp
        
        Args:
            agent: Данные агента
            current_day: Текущий день симуляции
            
        Returns:
            bool: True если планер можно активировать
        """
        # Текущая дата симуляции
        current_simulation_date = self.simulation_start_date + timedelta(days=current_day)
        
        # version_date из данных (дата версии)
        version_date = getattr(self, 'version_date', self.simulation_start_date)
        if isinstance(version_date, str):
            version_date = datetime.strptime(version_date, '%Y-%m-%d')
        
        # Время ремонта для этого агента из MacroProperty1 (по partno_comp = partseqno_i)
        repair_time = agent.get('repair_time', 180)  # Для планеров 180 дней по умолчанию
        
        # Разность дней между текущей датой симуляции и version_date
        days_since_version = (current_simulation_date - version_date).days
        
        agent_id = agent.get('agent_id', 'unknown')
        
        # Проверка: (текущая дата симуляции - version_date) >= repair_time
        can_activate = days_since_version >= repair_time
        
        if can_activate:
            logger.debug(f"Планер {agent_id}: активируем - прошло {days_since_version} дней с version_date, нужно {repair_time}")
        else:
            logger.debug(f"Планер {agent_id}: НЕ активируем - прошло {days_since_version} дней с version_date, нужно {repair_time}")
        
        return can_activate

    def log_daily_state(self, day: int) -> None:
        """
        ЭТАП 4: Логирование ежедневного состояния в MacroProperty2
        
        Args:
            day: Номер дня симуляции
        """
        current_date = self.simulation_start_date + timedelta(days=day)
        
        for agent in self.agents.values():
            # Получаем суточный налет
            aircraft_num = agent['aircraft_number']
            daily_hours = 0
            if aircraft_num in self.macro_property_5:
                # Используем .date() для совместимости типов
                current_date_only = current_date.date()
                daily_hours = self.macro_property_5[aircraft_num].get(current_date_only, 0)
            
            # Создаем запись для LoggingLayer_Planes с правильными типами
            log_record = {
                'dates': current_date.strftime('%Y-%m-%d'),
                'aircraft_number': int(agent['aircraft_number']),  # UInt32
                'daily_hours': int(daily_hours),  # UInt32 (минуты)
                'status_id': int(agent['status_id']),  # UInt8
                'partout_trigger': self._format_trigger_date(agent.get('partout_trigger', 0)),  # Date или 0
                'assembly_trigger': self._format_trigger_date(agent.get('assembly_trigger', 0)),  # Date или 0
                'mfg_date': agent['mfg_date'].strftime('%Y-%m-%d') if agent.get('mfg_date') else None,
                'active_trigger': self._format_trigger_date(agent.get('active_trigger', 0)),  # Date или 0
                'ac_type_mask': int(agent['ac_type_mask']),  # UInt8
                'sne': int(round(agent['sne'])),  # UInt32 (минуты)
                'ppr': int(round(agent['ppr'])),  # UInt32 (минуты)
                'repair_days': int(agent['repair_days']),  # UInt16
                'version_date': agent.get('version_date', self.simulation_start_date.strftime('%Y-%m-%d')),
                'version_id': self.version_id,
                'aircraft_age_years': round(self._calculate_age_years(agent['mfg_date'], current_date), 2)  # Float64
            }
            
            self.macro_property_2.append(log_record)
            
            # Сброс триггеров после логирования
            agent['partout_trigger'] = 0
            agent['assembly_trigger'] = 0
        
        # ЕЖЕДНЕВНАЯ СВОДКА СТАТУСОВ В ЛОГИ
        self._log_daily_status_summary(day, current_date)

    def _format_trigger_date(self, timestamp: int) -> Optional[str]:
        """
        Форматирование timestamp триггера в строку даты
        """
        if timestamp > 0:
            return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
        return None

    def _calculate_age_years(self, mfg_date, current_date: datetime) -> float:
        """
        Расчет возраста планера в годах
        
        Args:
            mfg_date: Дата производства (любой тип)
            current_date: Текущая дата
            
        Returns:
            float: Возраст в годах
        """
        if not mfg_date or mfg_date is None:
            return 0.0
            
        try:
            # Конвертируем в datetime если нужно
            if isinstance(mfg_date, str):
                mfg_date = datetime.strptime(mfg_date, '%Y-%m-%d')
            elif isinstance(mfg_date, datetime):
                pass  # Уже datetime
            elif hasattr(mfg_date, 'year'):  # date объект
                mfg_date = datetime.combine(mfg_date, datetime.min.time())
            else:
                return 0.0
                
            age_days = (current_date - mfg_date).days
            return age_days / 365.25
            
        except Exception as e:
            return 0.0

    def run_simulation(self) -> bool:
        """
        ЭТАП 3: Основной цикл симуляции с RTC функциями
        
        Returns:
            bool: True если симуляция успешна
        """
        try:
            logger.info("ЭТАП 3: Запуск симуляции Flame GPU")
            logger.info(f"Симуляция {self.simulation_days} дней с {len(self.agents)} агентами")
            
            for day in range(self.simulation_days):
                self.current_day = day
                current_date = self.simulation_start_date + timedelta(days=day)
                
                # Глобальные RTC функции (в начале)
                self.rtc_balance_layer(day)  # Балансировка ПЕРВАЯ
                self.rtc_spawn_layer(day)
                
                # Выполнение RTC функций слоев (после балансировки)
                self.rtc_inactive_layer(day)
                self.rtc_ops_layer(day)
                self.rtc_stock_layer(day)
                self.rtc_repair_layer(day)
                self.rtc_reserve_layer(day)
                self.rtc_store_layer(day)
                
                # Логирование состояния
                self.log_daily_state(day)
                
                # Прогресс симуляции
                if day % 30 == 0:
                    logger.info(f"День {day}: {current_date.strftime('%Y-%m-%d')}")
                    self._log_daily_statistics()
            
            logger.info("Симуляция завершена успешно")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка симуляции: {e}")
            return False

    def _log_daily_statistics(self) -> None:
        """
        Логирование ежедневной статистики симуляции
        """
        status_counts = {}
        type_counts = {}
        
        for agent in self.agents.values():
            status = agent['status_id']
            ac_type = 'МИ-17' if agent['ac_type_mask'] == 64 else 'МИ-8'
            
            status_counts[status] = status_counts.get(status, 0) + 1
            type_counts[ac_type] = type_counts.get(ac_type, 0) + 1
        
        logger.info(f"Статусы: {status_counts}")
        logger.info(f"Типы ВС: {type_counts}")

    def validate_results(self) -> Dict:
        """
        ЭТАП 5: Проверка результатов симуляции
        
        Returns:
            Dict: Метрики качества симуляции
        """
        logger.info("ЭТАП 5: Валидация результатов симуляции")
        
        df_results = pd.DataFrame(self.macro_property_2)
        
        validation_metrics = {
            'total_records': len(df_results),
            'unique_aircraft': df_results['aircraft_number'].nunique(),
            'simulation_days': df_results['dates'].nunique(),
            'status_distribution': df_results['status_id'].value_counts().to_dict(),
            'type_distribution': df_results['ac_type_mask'].value_counts().to_dict(),
            'avg_daily_hours': df_results['daily_hours'].mean(),
            'repair_events': df_results['partout_trigger'].notna().sum(),  # Количество непустых триггеров
            'assembly_events': df_results['assembly_trigger'].notna().sum(),  # Количество непустых триггеров
            'spawned_aircraft': len([a for a in self.agents.values() if self.HARDCODE_CONSTANTS['SPAWNED_ID_MIN'] <= int(a['agent_id']) <= self.HARDCODE_CONSTANTS['SPAWNED_ID_MAX']]),
            'max_sne': df_results['sne'].max(),
            'max_ppr': df_results['ppr'].max()
        }
        
        logger.info(f"Метрики валидации: {validation_metrics}")
        return validation_metrics

    def export_results(self) -> bool:
        """
        Экспорт результатов в ClickHouse (LoggingLayer_Planes и FlameGPU_Agents)
        
        Returns:
            bool: True если экспорт успешен
        """
        try:
            logger.info("Экспорт результатов в ClickHouse")
            
            # Используем clickhouse_connect для pandas совместимости
            import clickhouse_connect
            sys.path.append('/home/budnik_an/cube linux/cube/code/utils')
            from config_loader import load_clickhouse_config
            
            config = load_clickhouse_config()
            client = clickhouse_connect.get_client(
                host=config['host'],
                port=8123,  # HTTP порт для clickhouse_connect
                username=config['user'],
                password=config['password'],
                database=config['database']
            )
            
            # 1. ОЧИСТКА ТАБЛИЦЫ FlameGPU_Agents ПЕРЕД ЗАПИСЬЮ
            logger.info("Очистка таблицы FlameGPU_Agents...")
            try:
                client.command("TRUNCATE TABLE IF EXISTS FlameGPU_Agents")
                logger.info("✅ Таблица FlameGPU_Agents очищена")
            except Exception as e:
                logger.warning(f"Не удалось очистить FlameGPU_Agents: {e}")
                
            # 2. ЭКСПОРТ АГЕНТОВ В FlameGPU_Agents
            logger.info("Экспорт агентов в FlameGPU_Agents...")
            self._export_agents_to_clickhouse(client)
            
            df_export = pd.DataFrame(self.macro_property_2)
            
            # Отфильтровываем только нужные колонки для экспорта
            required_columns = [
                'dates', 'aircraft_number', 'daily_hours', 'status_id', 
                'partout_trigger', 'assembly_trigger', 'mfg_date', 'active_trigger',
                'ac_type_mask', 'sne', 'ppr', 'repair_days', 
                'version_date', 'version_id', 'aircraft_age_years'
            ]
            df_export = df_export[required_columns]
            
            # Конвертируем строковые даты в объекты date для ClickHouse
            from datetime import datetime, date
            date_columns = ['dates', 'mfg_date', 'version_date']
            for col in date_columns:
                if col in df_export.columns:
                    df_export[col] = pd.to_datetime(df_export[col], errors='coerce').dt.date
            
            # Создание таблицы LoggingLayer_Planes с правильными типами данных
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS LoggingLayer_Planes (
                dates Date,
                aircraft_number UInt32,
                daily_hours UInt32,
                status_id UInt8,
                partout_trigger UInt32,
                assembly_trigger UInt32,
                mfg_date Date,
                active_trigger UInt32,
                ac_type_mask UInt8,
                sne UInt32,
                ppr UInt32,
                repair_days UInt16,
                version_date Date,
                version_id String,
                aircraft_age_years Float64
            ) ENGINE = MergeTree()
            ORDER BY (dates, aircraft_number)
            """
            
            client.command(create_table_sql)
            
            # Вставка данных
            client.insert_df('LoggingLayer_Planes', df_export)
            
            logger.info(f"Экспортировано {len(df_export)} записей в LoggingLayer_Planes")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка экспорта: {e}")
            return False
    
    def _export_agents_to_clickhouse(self, client):
        """
        Экспорт финальных состояний агентов в FlameGPU_Agents
        
        Args:
            client: ClickHouse клиент
        """
        try:
            # Создание таблицы FlameGPU_Agents
            create_agents_table_sql = """
            CREATE TABLE IF NOT EXISTS FlameGPU_Agents (
                aircraft_number UInt32,
                ac_type_mask UInt8,
                status_id UInt8,
                sne UInt32,
                ppr UInt32,
                ll UInt32,
                oh UInt32,
                br UInt32,
                repair_days UInt16,
                repair_time UInt16,
                partout_time UInt16,
                assembly_time UInt16,
                partout_trigger UInt32,
                assembly_trigger UInt32,
                active_trigger UInt32,
                mfg_date Date,
                version_id String,
                export_timestamp DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY aircraft_number
            """
            
            client.command(create_agents_table_sql)
            
            # Подготовка данных агентов для экспорта
            agents_data = []
            for agent in self.macro_property_3:
                # Конвертируем mfg_date в объект date
                mfg_date_obj = agent['mfg_date']
                if isinstance(mfg_date_obj, str):
                    from datetime import datetime
                    mfg_date_obj = datetime.strptime(mfg_date_obj, '%Y-%m-%d').date()
                
                agent_record = {
                    'aircraft_number': int(agent['aircraft_number']),
                    'ac_type_mask': int(agent['ac_type_mask']),
                    'status_id': int(agent['status_id']),
                    'sne': int(agent['sne']),
                    'ppr': int(agent['ppr']),
                    'll': int(agent['ll']),
                    'oh': int(agent['oh']),
                    'br': int(agent['br']),
                    'repair_days': int(agent['repair_days']),
                    'repair_time': int(agent['repair_time']),
                    'partout_time': int(agent['partout_time']),
                    'assembly_time': int(agent['assembly_time']),
                    'partout_trigger': int(agent['partout_trigger']),
                    'assembly_trigger': int(agent['assembly_trigger']),
                    'active_trigger': int(agent['active_trigger']),
                    'mfg_date': mfg_date_obj,
                    'version_id': self.version_id
                }
                agents_data.append(agent_record)
            
            # Создаем DataFrame и экспортируем
            df_agents = pd.DataFrame(agents_data)
            client.insert_df('FlameGPU_Agents', df_agents)
            
            logger.info(f"✅ Экспортировано {len(df_agents)} агентов в FlameGPU_Agents")
            
        except Exception as e:
            logger.error(f"Ошибка экспорта агентов: {e}")
            raise
    
    def _log_daily_status_summary(self, day: int, current_date: datetime) -> None:
        """
        Генерация ежедневной сводки статусов агентов в логи
        
        Args:
            day: Номер дня симуляции  
            current_date: Текущая дата симуляции
        """
        # Подсчет статусов
        status_counts = {}
        type_counts = {}
        
        for agent in self.agents.values():
            status_id = agent['status_id']
            ac_type = agent['ac_type_mask']
            
            status_counts[status_id] = status_counts.get(status_id, 0) + 1
            type_counts[ac_type] = type_counts.get(ac_type, 0) + 1
        
        # Названия статусов для читаемости
        status_names = {
            1: "Неактивный",
            2: "Эксплуатация", 
            3: "Склад",
            4: "Ремонт",
            5: "Резерв",
            6: "Хранение"
        }
        
        type_names = {
            32: "МИ-8",
            64: "МИ-17"
        }
        
        # Формируем красивую сводку
        logger.info(f"📊 ДЕНЬ {day+1} ({current_date.strftime('%Y-%m-%d')}) - СВОДКА СТАТУСОВ:")
        
        # Статусы
        for status_id in sorted(status_counts.keys()):
            count = status_counts[status_id]
            name = status_names.get(status_id, f"Статус_{status_id}")
            percentage = (count / len(self.agents)) * 100
            logger.info(f"   {name} ({status_id}): {count:3d} планеров ({percentage:5.1f}%)")
        
        # Типы планеров
        logger.info(f"📋 РАСПРЕДЕЛЕНИЕ ПО ТИПАМ:")
        for ac_type in sorted(type_counts.keys()):
            count = type_counts[ac_type]
            name = type_names.get(ac_type, f"Тип_{ac_type}")
            percentage = (count / len(self.agents)) * 100
            logger.info(f"   {name} ({ac_type}): {count:3d} планеров ({percentage:5.1f}%)")
            
        logger.info(f"📈 ВСЕГО АГЕНТОВ: {len(self.agents)}")
        logger.info("─" * 60)

    def generate_report(self) -> str:
        """
        Генерация итогового отчета симуляции
        
        Returns:
            str: Путь к файлу отчета
        """
        report_file = f"logs/simulation_report_{self.version_id}.json"
        
        validation_metrics = self.validate_results()
        
        report_data = {
            'simulation_info': {
                'version_id': self.version_id,
                'start_date': self.simulation_start_date.strftime('%Y-%m-%d'),
                'simulation_days': self.simulation_days,
                'completion_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            },
            'model_configuration': {
                'initial_agents': len(self.macro_property_3),
                'final_agents': len(self.agents),
                'spawned_agents': len([a for a in self.agents.values() if self.HARDCODE_CONSTANTS['SPAWNED_ID_MIN'] <= int(a['agent_id']) <= self.HARDCODE_CONSTANTS['SPAWNED_ID_MAX']]),
                'hardcode_constants': self.HARDCODE_CONSTANTS
            },
            'validation_metrics': validation_metrics,
            'data_sources': {
                'md_components_count': len(self.macro_property_1),
                'heli_pandas_count': len(self.macro_property_3),
                'flight_program_loaded': bool(self.macro_property_4),
                'flight_tensor_size': len(self.macro_property_5) if self.macro_property_5 else 0
            },
            'performance_summary': {
                'total_logging_records': len(self.macro_property_2),
                'average_ops_aircraft': validation_metrics['status_distribution'].get(2, 0),
                'repair_utilization': validation_metrics['repair_events'],
                'simulation_success': True
            }
        }
        
        # Обработчик numpy типов для JSON сериализации
        def convert_numpy_types(obj):
            if hasattr(obj, 'item'):
                return obj.item()
            elif hasattr(obj, 'tolist'):
                return obj.tolist()
            return obj
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False, default=convert_numpy_types)
        
        logger.info(f"Отчет сохранен: {report_file}")
        return report_file


def main():
    """
    Основная функция запуска модели Flame GPU
    """
    try:
        # Создание модели
        model = FlameGPUHelicopterModel(
            simulation_start_date="2025-01-01",
            simulation_days=365
        )
        
        # Выполнение полного цикла
        success = (
            model.load_macro_properties() and
            model.create_agents() and
            model.run_simulation() and
            model.export_results()
        )
        
        if success:
            report_file = model.generate_report()
            logger.info(f"🎯 СИМУЛЯЦИЯ ЗАВЕРШЕНА УСПЕШНО")
            logger.info(f"📊 Отчет: {report_file}")
            
            print("\n" + "="*60)
            print("🚁 FLAME GPU HELICOPTER MODEL - РЕЗУЛЬТАТЫ")
            print("="*60)
            print(f"Version ID: {model.version_id}")
            print(f"Agents: {len(model.agents)}")
            print(f"Records: {len(model.macro_property_2)}")
            print(f"Report: {report_file}")
            print("="*60)
            
        else:
            logger.error("❌ СИМУЛЯЦИЯ ЗАВЕРШЕНА С ОШИБКАМИ")
            
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")


if __name__ == "__main__":
    main() 