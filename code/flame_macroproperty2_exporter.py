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
            psn UInt32,
            partseqno_i UInt32,
            aircraft_number UInt32,
            ac_type_mask UInt8,
            status_id UInt8,
            daily_flight UInt32,
            ops_counter_mi8 UInt16,
            ops_counter_mi17 UInt16,
            ops_current_mi8 UInt16,
            ops_current_mi17 UInt16,
            partout_trigger Nullable(Date),
            assembly_trigger Nullable(Date),
            active_trigger Nullable(Date),
            aircraft_age_years UInt8,
            mfg_date Nullable(Date),
            sne UInt32,
            ppr UInt32,
            repair_days UInt16,
            simulation_metadata String
        ) ENGINE = MergeTree()
        ORDER BY (dates, psn, aircraft_number)
        COMMENT 'LoggingLayer Planes (MP2) из FLAME GPU'
        """
        self.client.execute(ddl)
        self._migrate_schema_if_needed()

    def _migrate_schema_if_needed(self) -> None:
        """Проверяет типы колонок и при необходимости мигрирует их к актуальным.

        Правила миграции:
        - ops_counter_mi8/ops_counter_mi17 → UInt16 (были Int32).
        - partout_trigger/assembly_trigger/active_trigger → Nullable(Date) (были Date).
        """
        try:
            cols = self.client.execute(
                """
                SELECT name, type
                FROM system.columns
                WHERE database = currentDatabase() AND table = %(t)s
                """,
                {"t": self.table_name},
            )
        except Exception:
            # Если по какой-то причине нет доступа к system.columns — пропускаем авто-миграцию
            return

        type_by_name = {name: col_type for name, col_type in cols}

        alters = []
        if type_by_name.get("ops_counter_mi8") == "Int32":
            alters.append("MODIFY COLUMN ops_counter_mi8 UInt16")
        if type_by_name.get("ops_counter_mi17") == "Int32":
            alters.append("MODIFY COLUMN ops_counter_mi17 UInt16")
        # Триггеры и mfg_date должны быть Nullable(Date)
        if type_by_name.get("partout_trigger") == "Date":
            alters.append("MODIFY COLUMN partout_trigger Nullable(Date)")
        if type_by_name.get("assembly_trigger") == "Date":
            alters.append("MODIFY COLUMN assembly_trigger Nullable(Date)")
        if type_by_name.get("active_trigger") == "Date":
            alters.append("MODIFY COLUMN active_trigger Nullable(Date)")
        if type_by_name.get("mfg_date") == "Date":
            alters.append("MODIFY COLUMN mfg_date Nullable(Date)")
        # Новые колонки агрегатов агентов
        if "sne" not in type_by_name:
            alters.append("ADD COLUMN IF NOT EXISTS sne UInt32")
        if "ppr" not in type_by_name:
            alters.append("ADD COLUMN IF NOT EXISTS ppr UInt32")
        if "repair_days" not in type_by_name:
            alters.append("ADD COLUMN IF NOT EXISTS repair_days UInt16")
        # Идентификаторы агента
        if "psn" not in type_by_name:
            alters.append("ADD COLUMN IF NOT EXISTS psn UInt32")
        if "partseqno_i" not in type_by_name:
            alters.append("ADD COLUMN IF NOT EXISTS partseqno_i UInt32")

        if alters:
            alter_sql = f"ALTER TABLE {self.table_name} " + ", ".join(alters)
            self.client.execute(alter_sql)

    def insert_rows(self, rows: List[Dict[str, Any]]) -> None:
        """Вставка пачки строк. Поля должны соответствовать схеме ensure_table()."""
        if not rows:
            return
        fields = [
            'dates','psn','partseqno_i','aircraft_number','ac_type_mask','status_id','daily_flight',
            'ops_counter_mi8','ops_counter_mi17','ops_current_mi8','ops_current_mi17',
            'partout_trigger','assembly_trigger','active_trigger',
            'aircraft_age_years','mfg_date',
            'sne','ppr','repair_days',
            'simulation_metadata'
        ]
        data = []
        for r in rows:
            data.append([
                r.get('dates'),
                int(r.get('psn', 0) or 0),
                int(r.get('partseqno_i', 0) or 0),
                int(r.get('aircraft_number', 0) or 0),
                int(r.get('ac_type_mask', 0) or 0),
                int(r.get('status_id', 0) or 0),
                int(r.get('daily_flight', 0) or 0),
                int(r.get('ops_counter_mi8', 0) or 0),
                int(r.get('ops_counter_mi17', 0) or 0),
                int(r.get('ops_current_mi8', 0) or 0),
                int(r.get('ops_current_mi17', 0) or 0),
                r.get('partout_trigger', None),
                r.get('assembly_trigger', None),
                r.get('active_trigger', None),
                int(r.get('aircraft_age_years', 0) or 0),
                r.get('mfg_date', date(1970,1,1)),
                int(r.get('sne', 0) or 0),
                int(r.get('ppr', 0) or 0),
                int(r.get('repair_days', 0) or 0),
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