#!/usr/bin/env python3
"""
Калькулятор repair_days/repair_time для ВС в ремонте

Логика:
- Работает с ВС в статусе 4 (Ремонт) в таблице heli_pandas
- Сначала заполняет repair_time стандартом из md_components по partseqno_i
- Для day-0 активных ремонтов планеров group_by IN (1,2) берет активную строку
  status_overhaul, совпадающую с target_date, и пишет фактическую длительность цикла
- repair_days для таких планеров = прошедшие дни с act_start_date до version_date

Зависимости:
- md_components (с заполненным repair_time)
- heli_pandas (с установленным status_id=4)
- status_overhaul (для дат ремонта)

Автор: AI Assistant  
Дата: 2025-07-26
"""

import sys
import logging
from pathlib import Path
from datetime import datetime, date
import argparse

import pandas as pd

# Добавляем пути к utils и общему коду
code_root = Path(__file__).resolve().parents[1]
sys.path.append(str(code_root / 'utils'))
sys.path.append(str(code_root))
from config_loader import get_clickhouse_client


class RepairDaysCalculator:
    """Калькулятор repair_days для ВС в ремонте"""
    
    def __init__(self, version_date=None, version_id=None, dry_run=False):
        """Инициализация калькулятора"""
        self.logger = self._setup_logging()
        self.client = None
        self.version_date = version_date
        self.version_id = version_id
        self.dry_run = dry_run
    
    def _setup_logging(self) -> logging.Logger:
        """Настройка логирования"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def connect_to_database(self) -> bool:
        """Подключение к ClickHouse"""
        self.client = get_clickhouse_client()
        self.client.execute('SELECT 1')
        self.logger.info("✅ Подключение к ClickHouse успешно!")
        return True

    def ensure_repair_time_column(self):
        """Гарантирует наличие целочисленной колонки repair_time."""
        self.client.execute(
            "ALTER TABLE heli_pandas ADD COLUMN IF NOT EXISTS repair_time UInt16 DEFAULT 0"
        )
        self.logger.info("✅ Колонка heli_pandas.repair_time готова (UInt16)")

    def validate_version_scope(self):
        """Калькулятор должен работать в явном version scope."""
        if self.version_date is None or self.version_id is None:
            raise ValueError("repair_days_calculator требует --version-date и --version-id")

    def validate_standard_repair_times(self):
        """Fail-fast если для heli_pandas нет стандартного repair_time в md_components."""
        query = """
        SELECT
            count() AS missing_count,
            groupArrayDistinct(toUInt32(ifNull(hp.partseqno_i, 0))) AS missing_partseqno
        FROM heli_pandas AS hp
        LEFT JOIN
        (
            SELECT
                toUInt32(partseqno_i) AS partseqno_i,
                any(repair_time) AS repair_time
            FROM md_components
            WHERE partseqno_i IS NOT NULL
            GROUP BY partseqno_i
        ) AS md
            ON toUInt32(ifNull(hp.partseqno_i, 0)) = md.partseqno_i
        WHERE hp.version_date = %(version_date)s
          AND hp.version_id = %(version_id)s
          AND md.repair_time IS NULL
        """
        missing_count, missing_partseqno = self.client.execute(
            query,
            {"version_date": self.version_date, "version_id": self.version_id},
        )[0]
        if missing_count:
            raise ValueError(
                "md_components.repair_time не найден для heli_pandas "
                f"({missing_count} rows, partseqno_i={missing_partseqno[:20]})"
            )

    def populate_standard_repair_times(self):
        """Заполняет repair_time стандартами из md_components для всего среза."""
        self.logger.info("📋 Заполнение стандартного repair_time из md_components...")
        self.validate_standard_repair_times()
        query = """
        SELECT
            toUInt32(hp.partseqno_i) AS partseqno_i,
            toUInt16(any(md.repair_time)) AS repair_time
        FROM heli_pandas AS hp
        INNER JOIN md_components AS md
            ON toUInt32(hp.partseqno_i) = toUInt32(md.partseqno_i)
        WHERE hp.version_date = %(version_date)s
          AND hp.version_id = %(version_id)s
        GROUP BY hp.partseqno_i
        ORDER BY hp.partseqno_i
        """
        rows = self.client.execute(
            query,
            {"version_date": self.version_date, "version_id": self.version_id},
        )
        if not rows:
            raise ValueError(
                f"Не найдены строки heli_pandas для {self.version_date} v{self.version_id}"
            )

        if self.dry_run:
            self.logger.info(
                f"📝 DRY-RUN: стандартный repair_time был бы заполнен для {len(rows)} partseqno_i"
            )
            return

        params = {"version_date": self.version_date, "version_id": self.version_id}
        cases = []
        partseqno_params = []
        for idx, (partseqno_i, repair_time) in enumerate(rows):
            part_key = f"partseqno_i_{idx}"
            repair_time_key = f"repair_time_{idx}"
            params[part_key] = int(partseqno_i)
            params[repair_time_key] = int(repair_time)
            cases.append(
                f"toUInt32(ifNull(partseqno_i, 0)) = %({part_key})s, "
                f"toUInt16(%({repair_time_key})s)"
            )
            partseqno_params.append(f"%({part_key})s")

        repair_time_expr = "multiIf(\n                    "
        repair_time_expr += ",\n                    ".join(cases)
        repair_time_expr += ",\n                    repair_time\n                )"

        self.client.execute("SET mutations_sync = 1")
        self.client.execute(
            f"""
            ALTER TABLE heli_pandas
            UPDATE repair_time = {repair_time_expr}
            WHERE version_date = %(version_date)s
              AND version_id = %(version_id)s
              AND toUInt32(ifNull(partseqno_i, 0)) IN ({", ".join(partseqno_params)})
            """,
            params,
        )
        self.logger.info(
            f"✅ Стандартный repair_time заполнен для {len(rows)} partseqno_i одной мутацией"
        )
    
    def get_repair_aircraft(self):
        """Получает day-0 планеры в ремонте (status_id=4, group_by IN (1,2))."""
        self.logger.info("🔍 Поиск планеров в ремонте (status_id=4, group_by IN (1,2))...")

        query = """
        SELECT serialno, target_date, version_date, partseqno_i, group_by
        FROM heli_pandas
        WHERE status_id = 4
            AND toUInt32(ifNull(group_by, 0)) IN (1, 2)
            AND version_date = %(version_date)s
            AND version_id = %(version_id)s
        ORDER BY serialno
        """
        rows = self.client.execute(
            query,
            {"version_date": self.version_date, "version_id": self.version_id},
        )

        if not rows:
            self.logger.info("ℹ️ Не найдено ВС в ремонте (status_id=4)")
            return []

        repair_aircraft = []
        for row in rows:
            serialno, target_date, version_date, partseqno_i, group_by = row
            repair_aircraft.append({
                'serialno': serialno,
                'target_date': target_date,
                'version_date': version_date,
                'partseqno_i': partseqno_i,
                'group_by': group_by,
            })

        self.logger.info(f"✅ Найдено {len(repair_aircraft)} ВС в ремонте")
        for aircraft in repair_aircraft[:5]:  # Показываем первые 5
            self.logger.info(f"   ВС {aircraft['serialno']}: target_date={aircraft['target_date']}")

        return repair_aircraft
    
    def get_active_overhaul_cycle(self, aircraft):
        """Возвращает act_start/sched_end из активной строки, совпадающей с target_date."""
        serialno = aircraft['serialno']
        target_date = aircraft['target_date']
        if not target_date:
            raise ValueError(f"ВС {serialno}: отсутствует target_date для active repair")
        try:
            ac_registr = int(serialno)
        except ValueError as exc:
            raise ValueError(f"ВС {serialno}: serialno не приводится к ac_registr UInt32") from exc

        query = """
        SELECT act_start_date, sched_end_date, status
        FROM status_overhaul
        WHERE version_date = %(version_date)s
          AND version_id = %(version_id)s
          AND ac_registr = %(ac_registr)s
          AND status != 'Закрыто'
          AND sched_end_date = %(target_date)s
        ORDER BY act_start_date, sched_end_date
        """
        rows = self.client.execute(
            query,
            {
                "version_date": self.version_date,
                "version_id": self.version_id,
                "ac_registr": ac_registr,
                "target_date": target_date,
            },
        )
        if len(rows) != 1:
            raise ValueError(
                f"ВС {serialno}: ожидалась 1 активная строка status_overhaul "
                f"для target_date={target_date}, найдено {len(rows)}"
            )

        act_start_date, sched_end_date, status = rows[0]
        if act_start_date is None:
            raise ValueError(f"ВС {serialno}: act_start_date=NULL в активной строке {status}")
        if sched_end_date is None:
            raise ValueError(f"ВС {serialno}: sched_end_date=NULL в активной строке {status}")
        if sched_end_date != target_date:
            raise ValueError(
                f"ВС {serialno}: sched_end_date={sched_end_date} не совпадает с target_date={target_date}"
            )
        return act_start_date, sched_end_date
    
    def calculate_repair_days(self, repair_aircraft):
        """Рассчитывает repair_days и repair_time по активному status_overhaul."""
        self.logger.info("🔢 Расчет: repair_time=act_start→target_date, repair_days=act_start→version_date")

        updates = []
        calculated_count = 0

        for aircraft in repair_aircraft:
            serialno = aircraft['serialno']
            target_date = aircraft['target_date']
            version_date = aircraft['version_date']
            partseqno_i = aircraft['partseqno_i']
            group_by = aircraft['group_by']

            if not target_date or not version_date:
                raise ValueError(
                    f"ВС {serialno}: отсутствуют даты target_date={target_date}, version_date={version_date}"
                )

            act_start_date, sched_end_date = self.get_active_overhaul_cycle(aircraft)
            repair_time = (sched_end_date - act_start_date).days
            repair_days = max(0, (version_date - act_start_date).days)

            if repair_time < 0:
                raise ValueError(
                    f"ВС {serialno}: отрицательная длительность ремонта "
                    f"act_start={act_start_date}, target_date={sched_end_date}"
                )

            self.logger.info(
                f"✅ {serialno}: repair_time={repair_time}, repair_days={repair_days} "
                f"(act_start={act_start_date}, target_date={target_date}, version_date={version_date})"
            )

            # Подготавливаем UPDATE
            updates.append({
                'serialno': serialno,
                'repair_days': repair_days,
                'repair_time': repair_time,
                'partseqno_i': partseqno_i,
                'group_by': group_by,
                'target_date': target_date,
                'version_date': self.version_date,
                'version_id': self.version_id,
            })

            calculated_count += 1

        self.logger.info(f"✅ Рассчитано repair_days для {calculated_count} ВС")
        return updates
    
    def update_repair_days(self, updates):
        """Обновляет repair_days и agent-level repair_time в таблице heli_pandas."""
        self.logger.info("💾 Обновление repair_days/repair_time в heli_pandas...")

        if not updates:
            self.logger.info("ℹ️ Нет обновлений для применения")
            return True

        params = {"version_date": self.version_date, "version_id": self.version_id}
        repair_days_cases = []
        repair_time_cases = []
        where_conditions = []

        for idx, update in enumerate(updates):
            serialno_key = f"serialno_{idx}"
            repair_days_key = f"repair_days_{idx}"
            repair_time_key = f"repair_time_{idx}"
            partseqno_key = f"partseqno_i_{idx}"
            group_by_key = f"group_by_{idx}"
            target_date_key = f"target_date_{idx}"

            params[serialno_key] = update["serialno"]
            params[repair_days_key] = int(update["repair_days"])
            params[repair_time_key] = int(update["repair_time"])
            params[partseqno_key] = int(update["partseqno_i"])
            params[group_by_key] = int(update["group_by"])
            params[target_date_key] = update["target_date"]

            condition = (
                f"serialno = %({serialno_key})s "
                f"AND toUInt32(ifNull(partseqno_i, 0)) = %({partseqno_key})s "
                f"AND toUInt32(ifNull(group_by, 0)) = %({group_by_key})s "
                f"AND target_date = %({target_date_key})s"
            )
            repair_days_cases.append(
                f"{condition}, toUInt16(%({repair_days_key})s)"
            )
            repair_time_cases.append(
                f"{condition}, toUInt16(%({repair_time_key})s)"
            )
            where_conditions.append(f"({condition})")
            self.logger.info(
                f"   ✅ ВС {update['serialno']}: "
                f"repair_days={update['repair_days']}, repair_time={update['repair_time']}"
            )

        repair_days_expr = "multiIf(\n                    "
        repair_days_expr += ",\n                    ".join(repair_days_cases)
        repair_days_expr += ",\n                    repair_days\n                )"
        repair_time_expr = "multiIf(\n                    "
        repair_time_expr += ",\n                    ".join(repair_time_cases)
        repair_time_expr += ",\n                    repair_time\n                )"

        self.client.execute("SET mutations_sync = 1")
        self.client.execute(
            f"""
            ALTER TABLE heli_pandas
            UPDATE
                repair_days = {repair_days_expr},
                repair_time = {repair_time_expr}
            WHERE version_date = %(version_date)s
              AND version_id = %(version_id)s
              AND toUInt8(ifNull(status_id, 0)) = 4
              AND ({" OR ".join(where_conditions)})
            """,
            params,
        )

        self.logger.info(
            f"✅ Обновлено {len(updates)} записей с repair_days/repair_time одной мутацией"
        )
        return True
    
    def run(self):
        """Основной метод выполнения"""
        self.logger.info("🚀 === РАСЧЕТ REPAIR_DAYS ===")

        # 1. Подключение к базе
        if not self.connect_to_database():
            return False

        self.validate_version_scope()
        self.ensure_repair_time_column()
        self.populate_standard_repair_times()

        # 2. Получение ВС в ремонте
        repair_aircraft = self.get_repair_aircraft()
        if not repair_aircraft:
            self.logger.info("ℹ️ Нет ВС в ремонте для обработки")
            return True

        # 3. Расчет repair_days/repair_time
        updates = self.calculate_repair_days(repair_aircraft)
        if not updates:
            self.logger.warning("⚠️ Нет данных для обновления repair_days")
            return True

        if self.dry_run:
            self.logger.info("📝 DRY-RUN: обновление repair_days не выполнялось")
        elif not self.update_repair_days(updates):
            return False

        self.logger.info("✅ === РАСЧЕТ ЗАВЕРШЕН ===")
        self.logger.info(f"📊 Итого записей с repair_days: {len(updates)}")

        return True


def _as_u32(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="raise").fillna(0).astype("int64")


def _load_standard_repair_time_map(client) -> dict[int, int]:
    rows = client.execute(
        """
        SELECT
            toUInt32(partseqno_i) AS partseqno_i,
            toUInt16(any(repair_time)) AS repair_time
        FROM md_components
        WHERE partseqno_i IS NOT NULL
        GROUP BY partseqno_i
        ORDER BY partseqno_i
        """
    )
    return {int(partseqno_i): int(repair_time) for partseqno_i, repair_time in rows}


def _active_overhaul_cycle(client, serialno: str, target_date: date, version_date: date, version_id: int):
    if not target_date:
        raise ValueError(f"ВС {serialno}: отсутствует target_date для active repair")
    try:
        ac_registr = int(serialno)
    except ValueError as exc:
        raise ValueError(f"ВС {serialno}: serialno не приводится к ac_registr UInt32") from exc

    rows = client.execute(
        """
        SELECT act_start_date, sched_end_date, status
        FROM status_overhaul
        WHERE version_date = %(version_date)s
          AND version_id = %(version_id)s
          AND ac_registr = %(ac_registr)s
          AND status != 'Закрыто'
          AND sched_end_date = %(target_date)s
        ORDER BY act_start_date, sched_end_date
        """,
        {
            "version_date": version_date,
            "version_id": version_id,
            "ac_registr": ac_registr,
            "target_date": target_date,
        },
    )
    if len(rows) != 1:
        raise ValueError(
            f"ВС {serialno}: ожидалась 1 активная строка status_overhaul "
            f"для target_date={target_date}, найдено {len(rows)}"
        )
    act_start_date, sched_end_date, status = rows[0]
    if act_start_date is None:
        raise ValueError(f"ВС {serialno}: act_start_date=NULL в активной строке {status}")
    if sched_end_date is None:
        raise ValueError(f"ВС {serialno}: sched_end_date=NULL в активной строке {status}")
    if sched_end_date != target_date:
        raise ValueError(
            f"ВС {serialno}: sched_end_date={sched_end_date} не совпадает с target_date={target_date}"
        )
    return act_start_date, sched_end_date


def apply_repair_days(
    df: pd.DataFrame, client, version_date: date, version_id: int
) -> pd.DataFrame:
    """Заполняет repair_time и day-0 repair_days в DataFrame без ALTER UPDATE."""
    updated = df.copy()
    required = {"partseqno_i", "status_id", "group_by", "serialno", "target_date", "repair_days", "repair_time"}
    missing = required - set(updated.columns)
    if missing:
        raise ValueError(f"heli_pandas DataFrame missing columns: {sorted(missing)}")

    repair_time_map = _load_standard_repair_time_map(client)
    partseqno = _as_u32(updated["partseqno_i"])
    missing_partseqno = sorted({int(value) for value in partseqno if int(value) not in repair_time_map})
    if missing_partseqno:
        raise ValueError(
            "md_components.repair_time не найден для heli_pandas "
            f"({len(missing_partseqno)} partseqno_i={missing_partseqno[:20]})"
        )

    updated["repair_time"] = partseqno.map(lambda value: repair_time_map[int(value)]).astype("int64")

    status_id = _as_u32(updated["status_id"])
    group_by = _as_u32(updated["group_by"])
    repair_aircraft = updated.loc[
        (status_id == 4) & (group_by.isin([1, 2]))
    ].sort_values("serialno")
    print(f"✅ repair_days in-memory: repair planers={len(repair_aircraft)}")

    for idx, row in repair_aircraft.iterrows():
        serialno = str(row["serialno"])
        target_date = row["target_date"]
        if pd.isna(target_date):
            raise ValueError(f"ВС {serialno}: отсутствует target_date для active repair")
        act_start_date, sched_end_date = _active_overhaul_cycle(
            client, serialno, target_date, version_date, version_id
        )
        repair_time = (sched_end_date - act_start_date).days
        repair_days = max(0, (version_date - act_start_date).days)
        if repair_time < 0:
            raise ValueError(
                f"ВС {serialno}: отрицательная длительность ремонта "
                f"act_start={act_start_date}, target_date={sched_end_date}"
            )
        exact_mask = (
            (updated["serialno"] == row["serialno"])
            & (_as_u32(updated["partseqno_i"]) == int(row["partseqno_i"]))
            & (_as_u32(updated["group_by"]) == int(row["group_by"]))
            & (updated["target_date"] == target_date)
            & (_as_u32(updated["status_id"]) == 4)
        )
        updated.loc[exact_mask, ["repair_days", "repair_time"]] = [repair_days, repair_time]
        print(
            f"   ✅ ВС {serialno}: repair_days={repair_days}, repair_time={repair_time}"
        )

    return updated


def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description='Расчет repair_days для ВС в ремонте')
    parser.add_argument('--version-date', type=str, help='Дата версии данных (YYYY-MM-DD)')
    parser.add_argument('--version-id', type=int, help='ID версии данных')
    parser.add_argument('--dry-run', action='store_true', help='Только вывести расчет без UPDATE')
    
    args = parser.parse_args()
    
    # Преобразуем строку даты в объект date
    version_date = None
    if args.version_date:
        version_date = datetime.strptime(args.version_date, '%Y-%m-%d').date()
    
    client = get_clickhouse_client()
    client.execute("ALTER TABLE heli_pandas ADD COLUMN IF NOT EXISTS repair_time UInt16 DEFAULT 0")
    from utils.dwh_post_enrichment import (
        _load_heli_pandas_version,
        _replace_heli_pandas_version,
    )

    if version_date is None or args.version_id is None:
        raise ValueError("repair_days_calculator требует --version-date и --version-id")

    df = _load_heli_pandas_version(client, version_date, args.version_id)
    updated_df = apply_repair_days(df, client, version_date, args.version_id)
    if args.dry_run:
        print("📝 DRY-RUN: обновление repair_days не выполнялось")
    else:
        _replace_heli_pandas_version(client, updated_df, version_date, args.version_id)
    print("🎯 Успешно!")
    return True


if __name__ == "__main__":
    if main():
        sys.exit(0)
    else:
        sys.exit(1) 