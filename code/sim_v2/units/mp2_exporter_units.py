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


def export_mp2_with_history(
    simulation,  # CUDASimulation
    env_data: Dict,
    version_date: date,
    version_id: int = 1,
    total_days: int = 3650,
    sample_interval: int = 100,
    drop_table: bool = False,
    agent_desc = None,
    batch_size: int = 100000
):
    """
    Экспортирует MP2 агрегатов в ClickHouse с историей
    
    Читает данные из MP2 MacroProperty за все дни (sample или полностью)
    
    Args:
        simulation: CUDASimulation с результатами
        env_data: Данные окружения
        version_date: Дата версии
        version_id: ID версии
        total_days: Количество дней симуляции
        sample_interval: Интервал выборки (0 = все дни)
        drop_table: Удалить таблицу перед записью
        agent_desc: AgentDescription для создания AgentVector
        batch_size: Размер батча для вставки
    """
    import pyflamegpu as fg
    import time
    
    t0 = time.time()
    client = get_clickhouse_client()
    
    # Пересоздаём таблицу с кодеками
    if drop_table:
        client.execute("DROP TABLE IF EXISTS sim_units_v2")
        print("   🗑️ Таблица удалена")
    
    # DDL с оптимальными кодеками
    ddl = """
    CREATE TABLE IF NOT EXISTS sim_units_v2 (
        version_date UInt32,
        version_id UInt32,
        day_u16 UInt16 CODEC(Delta, ZSTD(1)),
        day_date Date MATERIALIZED addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)),
        idx UInt32,
        psn UInt32,
        group_by UInt8 CODEC(ZSTD(1)),
        partseqno_i UInt32,
        aircraft_number UInt32,
        sne UInt32 CODEC(Delta, ZSTD(1)),
        ppr UInt32 CODEC(Delta, ZSTD(1)),
        state UInt8 CODEC(ZSTD(1)),
        repair_days UInt16 CODEC(Delta, ZSTD(1)),
        queue_position UInt32,
        export_timestamp DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (version_date, version_id, day_u16, psn)
    """
    client.execute(ddl)
    print("   ✅ Таблица sim_units_v2 готова (с кодеками)")
    
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
    
    # Читаем MP2 MacroProperty
    print(f"   📊 Чтение MP2: {total_days+1} дней × {max_frames} фреймов...")
    
    try:
        # Получаем данные из MacroProperty
        mp2_psn = simulation.getEnvironmentMacroPropertyUInt("mp2_units_psn")
        mp2_group_by = simulation.getEnvironmentMacroPropertyUInt("mp2_units_group_by")
        mp2_sne = simulation.getEnvironmentMacroPropertyUInt("mp2_units_sne")
        mp2_ppr = simulation.getEnvironmentMacroPropertyUInt("mp2_units_ppr")
        mp2_state = simulation.getEnvironmentMacroPropertyUInt("mp2_units_state")
        mp2_ac = simulation.getEnvironmentMacroPropertyUInt("mp2_units_ac")
        mp2_repair_days = simulation.getEnvironmentMacroPropertyUInt("mp2_units_repair_days")
        mp2_queue_pos = simulation.getEnvironmentMacroPropertyUInt("mp2_units_queue_pos")
        mp2_partseqno = simulation.getEnvironmentMacroPropertyUInt("mp2_units_partseqno")
        mp2_active = simulation.getEnvironmentMacroPropertyUInt("mp2_units_active")
        
        print(f"   ✅ MP2 MacroProperty прочитаны (размер: {len(mp2_psn):,})")
    except Exception as e:
        print(f"   ⚠️ Не удалось прочитать MP2 MacroProperty: {e}")
        print("   ➡️ Fallback: экспорт только финального состояния")
        # Fallback к старому методу
        return export_mp2_to_clickhouse(
            simulation, env_data, version_date, version_id,
            batch_size, drop_table, agent_desc
        )
    
    # Определяем дни для экспорта
    if sample_interval > 0:
        days_to_export = list(range(0, total_days + 1, sample_interval))
        if total_days not in days_to_export:
            days_to_export.append(total_days)
    else:
        days_to_export = list(range(total_days + 1))
    
    print(f"   📅 Экспорт {len(days_to_export)} дней: {days_to_export[:5]}...{days_to_export[-3:]}")
    
    batch = []
    total_exported = 0
    skipped = 0
    
    for day in days_to_export:
        for idx in range(max_frames):
            pos = day * max_frames + idx
            
            if pos >= len(mp2_psn):
                continue
            
            psn = mp2_psn[pos]
            if psn == 0:
                skipped += 1
                continue
            
            # Проверяем active флаг
            active = mp2_active[pos] if pos < len(mp2_active) else 1
            if active == 0:
                skipped += 1
                continue
            
            row = (
                version_date_int,
                version_id,
                day,
                version_date + timedelta(days=day),
                idx,
                psn,
                mp2_group_by[pos],
                mp2_partseqno[pos] if pos < len(mp2_partseqno) else 0,
                mp2_ac[pos],
                mp2_sne[pos],
                mp2_ppr[pos],
                mp2_state[pos],
                mp2_repair_days[pos],
                mp2_queue_pos[pos],
            )
            batch.append(row)
            
            if len(batch) >= batch_size:
                _insert_batch(client, batch)
                total_exported += len(batch)
                batch = []
    
    if batch:
        _insert_batch(client, batch)
        total_exported += len(batch)
    
    elapsed = time.time() - t0
    print(f"   ✅ Экспортировано {total_exported:,} записей за {elapsed:.2f}с")
    print(f"   ⏩ Пропущено {skipped:,} пустых слотов")
    
    return total_exported


if __name__ == "__main__":
    print("Экспортёр MP2 агрегатов в ClickHouse")
    print("Используется из orchestrator_units.py")

