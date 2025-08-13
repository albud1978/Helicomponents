#!/usr/bin/env python3
"""
FLAME GPU MacroProperty2 Exporter (LoggingLayer Planes)

Экспортирует логи симуляции (планеры) в таблицу ClickHouse `flame_macroproperty2_export`.
Использование: через методы ensure_table() и insert_rows([...]).

Дата: 2025-08-10
"""

from typing import List, Dict, Any, Optional
from datetime import date, timedelta
import sys
import os

# Добавляем utils
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from config_loader import get_clickhouse_client


class FlameMacroProperty2Exporter:
    """Экспортер LoggingLayer Planes в ClickHouse"""

    def __init__(self, client=None, table_name: str = "flame_macroproperty2_export"):
        self.client = client or get_clickhouse_client()
        self.table_name = table_name

    def ensure_table(self) -> None:
        """Создает таблицу экспорта, если отсутствует"""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            dates Date,
            aircraft_number UInt32,
            ac_type_mask UInt8,
            status_id UInt8,
            daily_flight UInt32,
            trigger_pr_final_mi8 Int32,
            trigger_pr_final_mi17 Int32,
            partout_trigger Date,
            assembly_trigger Date,
            active_trigger Date,
            aircraft_age_years UInt8,
            mfg_date_final Date,
            simulation_metadata String
        ) ENGINE = MergeTree()
        ORDER BY (dates, aircraft_number)
        COMMENT 'LoggingLayer Planes (MP2) из FLAME GPU'
        """
        self.client.execute(ddl)

    def insert_rows(self, rows: List[Dict[str, Any]]) -> None:
        """Вставка пачки строк. Поля должны соответствовать схеме ensure_table()."""
        if not rows:
            return
        fields = [
            'dates','aircraft_number','ac_type_mask','status_id','daily_flight',
            'trigger_pr_final_mi8','trigger_pr_final_mi17','partout_trigger','assembly_trigger','active_trigger',
            'aircraft_age_years','mfg_date_final','simulation_metadata'
        ]
        data = []
        for r in rows:
            data.append([
                r.get('dates'),
                int(r.get('aircraft_number', 0) or 0),
                int(r.get('ac_type_mask', 0) or 0),
                int(r.get('status_id', 0) or 0),
                int(r.get('daily_flight', 0) or 0),
                int(r.get('trigger_pr_final_mi8', 0) or 0),
                int(r.get('trigger_pr_final_mi17', 0) or 0),
                r.get('partout_trigger', date(1970,1,1)),
                r.get('assembly_trigger', date(1970,1,1)),
                r.get('active_trigger', date(1970,1,1)),
                int(r.get('aircraft_age_years', 0) or 0),
                r.get('mfg_date_final', date(1970,1,1)),
                str(r.get('simulation_metadata', '')),
            ])
        insert_sql = f"INSERT INTO {self.table_name} ({', '.join(fields)}) VALUES"
        self.client.execute(insert_sql, data)


def main():
    print("📤 FlameMacroProperty2Exporter — подготовка таблицы и тестовая вставка")
    exporter = FlameMacroProperty2Exporter()
    exporter.ensure_table()
    print(f"✅ Таблица {exporter.table_name} готова")

if __name__ == '__main__':
    main()