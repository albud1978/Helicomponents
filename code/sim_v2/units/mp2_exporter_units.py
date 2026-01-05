#!/usr/bin/env python3
"""
Экспортёр MP2 агрегатов в ClickHouse

Записывает результаты симуляции агрегатов в таблицу sim_units_v2:
- psn (PRIMARY KEY агрегата)
- group_by
- sne, ppr
- state
- aircraft_number
- repair_days
- queue_position

Дата: 05.01.2026
"""

import numpy as np
from datetime import date, timedelta
from typing import Dict, Optional
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from utils.config_loader import get_clickhouse_client


def create_sim_units_table(client, drop_first: bool = False):
    """Создаёт таблицу sim_units_v2 если не существует"""
    
    if drop_first:
        client.execute("DROP TABLE IF EXISTS sim_units_v2")
        print("   🗑️ Таблица sim_units_v2 удалена")
    
    ddl = """
    CREATE TABLE IF NOT EXISTS sim_units_v2 (
        -- Версионирование
        version_date UInt32,
        version_id UInt32,
        
        -- Индексы
        day_u16 UInt16,
        day_date Date,
        idx UInt32,
        
        -- Идентификаторы
        psn UInt32,
        group_by UInt8,
        partseqno_i UInt32,
        aircraft_number UInt32,
        
        -- Наработки
        sne UInt32,
        ppr UInt32,
        
        -- Состояние
        state UInt8,
        repair_days UInt16,
        queue_position UInt32,
        
        -- Метаданные
        export_timestamp DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (version_date, version_id, day_u16, psn)
    """
    
    client.execute(ddl)
    print("   ✅ Таблица sim_units_v2 готова")


def export_mp2_to_clickhouse(
    simulation,  # CUDASimulation
    env_data: Dict,
    version_date: date,
    version_id: int = 1,
    batch_size: int = 100000,
    drop_table: bool = False,
    agent_desc = None  # AgentDescription
):
    """
    Экспортирует MP2 агрегатов в ClickHouse
    
    Args:
        simulation: CUDASimulation с результатами
        env_data: Данные окружения
        version_date: Дата версии
        version_id: ID версии
        batch_size: Размер батча для вставки
        drop_table: Удалить таблицу перед записью
        agent_desc: AgentDescription для создания AgentVector
    """
    import pyflamegpu as fg
    
    client = get_clickhouse_client()
    create_sim_units_table(client, drop_table)
    
    # Конвертируем дату
    if isinstance(version_date, str):
        version_date = date.fromisoformat(version_date)
    version_date_int = (version_date - date(1970, 1, 1)).days
    
    # Удаляем старые данные для этой версии
    client.execute(
        "ALTER TABLE sim_units_v2 DELETE WHERE version_date = %(vd)s AND version_id = %(vi)s",
        {'vd': version_date_int, 'vi': version_id}
    )
    print(f"   🧹 Очищены старые данные для {version_date}")
    
    # Получаем размеры
    max_frames = int(env_data.get('units_frames_total', 12000))
    max_days = int(env_data.get('days_total_u16', 3650))
    
    print(f"   📊 Экспорт MP2: {max_frames} агентов × {max_days} дней")
    
    if agent_desc is None:
        print("   ⚠️ AgentDescription не передан, экспорт невозможен")
        return 0
    
    # Получаем популяции
    all_states = ['operations', 'serviceable', 'repair', 'reserve', 'storage']
    state_to_code = {'operations': 2, 'serviceable': 3, 'repair': 4, 'reserve': 5, 'storage': 6}
    total_exported = 0
    
    for state_name in all_states:
        try:
            # Создаём AgentVector для получения данных
            pop = fg.AgentVector(agent_desc)
            simulation.getPopulationData(pop, state_name)
            
            if len(pop) == 0:
                continue
            
            # Собираем данные
            batch_data = []
            for i in range(len(pop)):
                agent = pop[i]
                row = (
                    version_date_int,
                    version_id,
                    max_days,  # Последний день (финальное состояние)
                    version_date + timedelta(days=max_days),
                    agent.getVariableUInt("idx"),
                    agent.getVariableUInt("psn"),
                    agent.getVariableUInt("group_by"),
                    agent.getVariableUInt("partseqno_i"),
                    agent.getVariableUInt("aircraft_number"),
                    agent.getVariableUInt("sne"),
                    agent.getVariableUInt("ppr"),
                    state_to_code[state_name],
                    agent.getVariableUInt("repair_days"),
                    agent.getVariableUInt("queue_position"),
                )
                batch_data.append(row)
                
                if len(batch_data) >= batch_size:
                    _insert_batch(client, batch_data)
                    total_exported += len(batch_data)
                    batch_data = []
            
            if batch_data:
                _insert_batch(client, batch_data)
                total_exported += len(batch_data)
            
            print(f"      {state_name}: {len(pop)} агентов")
            
        except Exception as e:
            print(f"      ⚠️ {state_name}: {e}")
    
    print(f"   ✅ Экспортировано {total_exported} записей")
    return total_exported


def _insert_batch(client, batch_data):
    """Вставляет батч данных"""
    client.execute(
        """
        INSERT INTO sim_units_v2 (
            version_date, version_id, day_u16, day_date, idx,
            psn, group_by, partseqno_i, aircraft_number,
            sne, ppr, state, repair_days, queue_position
        ) VALUES
        """,
        batch_data
    )


if __name__ == "__main__":
    print("Экспортёр MP2 агрегатов в ClickHouse")
    print("Используется из orchestrator_units.py")

