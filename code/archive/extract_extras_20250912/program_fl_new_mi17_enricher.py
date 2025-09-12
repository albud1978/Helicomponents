#!/usr/bin/env python3
"""
Program FL New MI-17 Enricher - Дополнение FL тензора новыми планёрами
======================================================================

Назначение:
- По сумме new_counter_mi17 из flight_program_ac (текущая версия) сгенерировать новые
  aircraft_number (UInt32 начиная с 100000, ac_type_mask=64), дозаполнить словарь
  dict_aircraft_number_flat и добавить строки в flight_program_fl для этих бортов.

Требования:
- Не изменять порядок существующих этапов. Скрипт — отдельный завершающий enricher.
- Идемпотентность: не создавать дубликаты; проверять существующие номера ≥100000.
- Ошибки — критичны: при невозможности корректной дозагрузки — завершать с ошибкой.

Дата: 2025-09-05
"""

from __future__ import annotations

import sys
import logging
from pathlib import Path
from typing import List, Tuple, Optional
from datetime import datetime, date, timedelta

sys.path.append(str(Path(__file__).parent / 'utils'))
from config_loader import get_clickhouse_client


def setup_logger() -> logging.Logger:
    logger = logging.getLogger(__name__)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(fmt)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


class FLNewMi17Enricher:
    def __init__(self, version_date: Optional[date] = None, version_id: int = 1):
        self.logger = setup_logger()
        self.client = get_clickhouse_client()
        self.version_date = version_date
        self.version_id = version_id

    def ensure_flight_program_fl_table(self) -> None:
        self.client.execute(
            """
            CREATE TABLE IF NOT EXISTS flight_program_fl (
                aircraft_number UInt32,
                dates Date,
                daily_hours UInt32,
                ac_type_mask UInt8,
                version_date Date DEFAULT today(),
                version_id UInt8 DEFAULT 1
            ) ENGINE = MergeTree()
            ORDER BY (aircraft_number, dates)
            SETTINGS index_granularity = 8192
            """
        )

    def get_latest_version(self) -> Tuple[date, int]:
        if self.version_date and self.version_id:
            return self.version_date, int(self.version_id)
        row = self.client.execute(
            """
            SELECT MAX(version_date) AS vd
            FROM heli_pandas
            WHERE version_date IS NOT NULL
            """
        )
        vd = row[0][0]
        return vd, 1

    def sum_new_counter_mi17(self, vd: date, vid: int) -> int:
        row = self.client.execute(
            f"""
            SELECT toInt64(SUM(new_counter_mi17))
            FROM flight_program_ac
            WHERE version_date = '{vd}' AND version_id = {vid}
            """
        )
        return int(row[0][0] or 0)

    def generate_new_numbers(self, count: int) -> List[int]:
        if count <= 0:
            return []
        row = self.client.execute(
            """
            SELECT toInt64(max(aircraft_number))
            FROM dict_aircraft_number_flat
            WHERE aircraft_number >= 100000
            """
        )
        max_existing = int(row[0][0] or 99999)
        start = max(100000, max_existing + 1)
        return [start + i for i in range(count)]

    def upsert_dict_aircraft_numbers(self, nums: List[int], vd: date, vid: int) -> int:
        if not nums:
            return 0
        # Фильтр уже существующих
        existing = {int(r[0]) for r in self.client.execute(
            "SELECT aircraft_number FROM dict_aircraft_number_flat WHERE aircraft_number >= 100000"
        )}
        new_nums = [int(n) for n in nums if int(n) not in existing]
        if not new_nums:
            return 0
        # Вставляем только новые номера, сразу с форматными полями (без апдейтов существующих строк)
        rows = []
        for n in new_nums:
            fmt = f"{n:05d}"
            reg = f"RA-{fmt}"
            lead = 1 if n < 10000 else 0
            rows.append((n, fmt, reg, lead, 64, vd, vid))
        self.client.execute(
            """
            INSERT INTO dict_aircraft_number_flat 
                (aircraft_number, formatted_number, registration_code, is_leading_zero, ac_type_mask, version_date, version_id)
            VALUES
            """,
            rows,
        )
        return len(rows)

    def upsert_base_aircraft_from_hp(self, vd: date, vid: int) -> int:
        """Гарантирует, что в dict_aircraft_number_flat присутствуют ВСЕ борта из heli_pandas.

        Идемпотентно: не перезаписывает, а только добавляет отсутствующие aircraft_number.
        """
        # Существующие номера в словаре
        existing = {int(r[0]) for r in self.client.execute(
            "SELECT aircraft_number FROM dict_aircraft_number_flat"
        )}
        # Борта из heli_pandas с масками
        hp_rows = self.client.execute(
            """
            SELECT DISTINCT toUInt32(aircraft_number) AS ac, toUInt8(ac_type_mask) AS m
            FROM heli_pandas
            WHERE aircraft_number > 0
            ORDER BY ac
            """
        )
        to_insert = [(int(ac), int(m), vd, vid) for ac, m in hp_rows if int(ac) not in existing]
        if not to_insert:
            return 0
        self.client.execute(
            "INSERT INTO dict_aircraft_number_flat (aircraft_number, ac_type_mask, version_date, version_id) VALUES",
            to_insert,
        )
        return len(to_insert)

    def dates_calendar(self, base_date: date, days: int = 4000) -> List[date]:
        return [base_date + timedelta(days=i) for i in range(days)]

    def build_type64_baseline(self, vd: date, vid: int) -> List[Tuple[date, int]]:
        # Средний по типу 64 дневной налёт на каждую дату как baseline
        rows = self.client.execute(
            f"""
            SELECT dates, toUInt32(avg(daily_hours)) AS daily
            FROM flight_program_fl
            WHERE version_date = '{vd}' AND version_id = {vid} AND ac_type_mask = 64
            GROUP BY dates
            ORDER BY dates
            """
        )
        return [(r[0], int(r[1] or 0)) for r in rows]

    def insert_fl_rows(self, nums: List[int], vd: date, vid: int) -> int:
        if not nums:
            return 0
        self.ensure_flight_program_fl_table()
        baseline = self.build_type64_baseline(vd, vid)
        if not baseline:
            raise RuntimeError("Нет baseline по типу 64 в flight_program_fl для текущей версии")
        # Формируем записи
        batch = []
        for d, hours in baseline:
            for ac in nums:
                batch.append((int(ac), d, int(hours), 64, vd, vid))
        # Вставляем батчами
        step = 100000
        total = 0
        for i in range(0, len(batch), step):
            part = batch[i:i+step]
            self.client.execute(
                "INSERT INTO flight_program_fl (aircraft_number, dates, daily_hours, ac_type_mask, version_date, version_id) VALUES",
                part,
            )
            total += len(part)
        return total

    def run(self) -> bool:
        self.logger.info("🚀 FL New MI-17 Enricher старт")
        vd, vid = self.get_latest_version()
        total_new = self.sum_new_counter_mi17(vd, vid)
        self.logger.info(f"📊 new_counter_mi17 суммарно: {total_new}")
        if total_new <= 0:
            self.logger.info("ℹ️ Нет новых Ми-17 для дозагрузки — завершаем без изменений")
            return True
        nums = self.generate_new_numbers(total_new)
        if not nums:
            raise RuntimeError("Не удалось сгенерировать новые номера Ми-17")
        ins_dict = self.upsert_dict_aircraft_numbers(nums, vd, vid)
        self.logger.info(f"📘 В словарь добавлено новых Ми‑17: {ins_dict}")
        inserted = self.insert_fl_rows(nums, vd, vid)
        self.logger.info(f"✅ В flight_program_fl добавлено строк: {inserted}")
        return True


def main(version_date: Optional[str] = None, version_id: Optional[int] = None):
    logger = setup_logger()
    try:
        vd = None
        vid = 1
        if version_date and version_id is not None:
            vd = datetime.strptime(version_date, '%Y-%m-%d').date()
            vid = int(version_id)
        enricher = FLNewMi17Enricher(version_date=vd, version_id=vid)
        ok = enricher.run()
        if ok:
            print("✅ FL New MI-17 Enricher завершен успешно!")
            return True
        print("❌ Ошибка работы FL New MI-17 Enricher")
        return False
    except Exception as e:
        logger.error(f"💥 Критическая ошибка Enricher: {e}")
        return False


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='FL New MI-17 Enricher для Helicopter Component Lifecycle')
    parser.add_argument('--version-date', type=str, help='Дата версии (YYYY-MM-DD)')
    parser.add_argument('--version-id', type=int, help='ID версии')
    args = parser.parse_args()
    success = main(version_date=args.version_date, version_id=args.version_id)
    sys.exit(0 if success else 1)


