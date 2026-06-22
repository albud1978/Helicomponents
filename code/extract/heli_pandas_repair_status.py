#!/usr/bin/env python3
"""
Микросервис установки status_id для агрегатов в ремонте (с target_date).

Логика:
1. Если target_date < version_date (ремонт завершился) → status_id=2 (Эксплуатация)
2. Если target_date >= version_date (ремонт идёт) → status_id=4 (Ремонт) + repair_days
   Приоритет: condition='ИСПРАВНЫЙ' — агрегаты не переводим в status_id=4 в этом модуле.

Формула repair_days: repair_time - (target_date - version_date)
- repair_time берётся из md_components через связь partseqno_i = partseqno_i

Условия:
- group_by >= 1 (планеры и агрегаты)
- status_id = 0 (ещё не обработан)
- target_date IS NOT NULL и != 1970-01-01

Выполняется ПОСЛЕ heli_pandas_storage_status.py.
Идемпотентен, поддерживает dry-run.
"""

import argparse
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Tuple

code_root = Path(__file__).resolve().parents[1]
sys.path.append(str(code_root / 'utils'))
sys.path.append(str(code_root))
from config_loader import get_clickhouse_client  # type: ignore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Установка status_id для агрегатов в ремонте (с target_date)"
    )
    parser.add_argument(
        "--version-date",
        type=str,
        help="Дата версии данных (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--version-id",
        type=int,
        help="ID версии данных (UInt8)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Только вывести статистику без выполнения UPDATE",
    )
    return parser.parse_args()


def resolve_version(
    client,
    version_date: Optional[str],
    version_id: Optional[int],
) -> Tuple[date, int]:
    if version_date:
        parsed_date = datetime.strptime(version_date, "%Y-%m-%d").date()
        vid = version_id if version_id is not None else 1
        return parsed_date, vid

    row = client.execute(
        """
        SELECT version_date, version_id
        FROM heli_pandas
        ORDER BY version_date DESC, version_id DESC
        LIMIT 1
        """
    )
    if not row:
        raise RuntimeError("Таблица heli_pandas пуста — нечего обрабатывать")
    v_date, v_id = row[0]
    return v_date, int(v_id)


def count_past_target_date(client, version_date: date, version_id: int) -> int:
    """Подсчёт агрегатов с target_date в прошлом"""
    query = """
    SELECT count(*)
    FROM heli_pandas
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND toUInt32(ifNull(group_by, 0)) >= 1
      AND toUInt8(ifNull(status_id, 0)) = 0
      AND target_date IS NOT NULL
      AND target_date != toDate('1970-01-01')
      AND target_date < %(version_date)s
    """
    result = client.execute(query, {"version_date": version_date, "version_id": version_id})
    return int(result[0][0])


def count_future_target_date(client, version_date: date, version_id: int) -> int:
    """Подсчёт агрегатов с target_date в будущем"""
    query = """
    SELECT count(*)
    FROM heli_pandas
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND toUInt32(ifNull(group_by, 0)) >= 1
      AND toUInt8(ifNull(status_id, 0)) = 0
      AND target_date IS NOT NULL
      AND target_date != toDate('1970-01-01')
      AND target_date >= %(version_date)s
      AND (
          toUInt32(ifNull(group_by, 0)) <= 2
          OR ifNull(condition, '') != 'ИСПРАВНЫЙ'
      )
    """
    result = client.execute(query, {"version_date": version_date, "version_id": version_id})
    return int(result[0][0])


def count_condition_ok_aggregates(client, version_date: date, version_id: int) -> int:
    """Подсчёт агрегатов с condition='ИСПРАВНЫЙ' и валидным target_date"""
    query = """
    SELECT count(*)
    FROM heli_pandas
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND toUInt32(ifNull(group_by, 0)) > 2
      AND condition = 'ИСПРАВНЫЙ'
      AND target_date IS NOT NULL
      AND target_date != toDate('1970-01-01')
    """
    result = client.execute(query, {"version_date": version_date, "version_id": version_id})
    return int(result[0][0])


def update_past_to_operations(client, version_date: date, version_id: int) -> int:
    """target_date в прошлом → status_id=2 (ремонт завершился)
    
    ВАЖНО: Для планеров (group_by IN 1,2) принудительно ставим status_id=2
    даже если текущий status_id=4 (были ошибочно помечены как "в ремонте")
    """
    # Сначала обрабатываем агрегаты (status_id=0)
    query_aggregates = """
    ALTER TABLE heli_pandas
    UPDATE status_id = 2
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND toUInt32(ifNull(group_by, 0)) > 2
      AND toUInt8(ifNull(status_id, 0)) = 0
      AND target_date IS NOT NULL
      AND target_date != toDate('1970-01-01')
      AND target_date < %(version_date)s
    """
    client.execute("SET mutations_sync = 1")
    client.execute(query_aggregates, {"version_date": version_date, "version_id": version_id})
    
    # Затем ПРИНУДИТЕЛЬНО обрабатываем планеры (status_id любой, т.к. ремонт завершён)
    query_planers = """
    ALTER TABLE heli_pandas
    UPDATE status_id = 2
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND toUInt32(ifNull(group_by, 0)) IN (1, 2)
      AND target_date IS NOT NULL
      AND target_date != toDate('1970-01-01')
      AND target_date < %(version_date)s
    """
    client.execute("SET mutations_sync = 1")
    client.execute(query_planers, {"version_date": version_date, "version_id": version_id})
    
    # Проверяем сколько осталось
    remaining = count_past_target_date(client, version_date, version_id)
    return remaining


def update_future_to_repair(client, version_date: date, version_id: int) -> int:
    """
    target_date в будущем → status_id=4 + repair_days
    
    repair_days = repair_time - (target_date - version_date)
    """
    # Сначала ставим status_id=4
    query_status = """
    ALTER TABLE heli_pandas
    UPDATE status_id = 4
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND toUInt32(ifNull(group_by, 0)) >= 1
      AND toUInt8(ifNull(status_id, 0)) = 0
      AND target_date IS NOT NULL
      AND target_date != toDate('1970-01-01')
      AND target_date >= %(version_date)s
      AND (
          toUInt32(ifNull(group_by, 0)) <= 2
          OR ifNull(condition, '') != 'ИСПРАВНЫЙ'
      )
    """
    client.execute("SET mutations_sync = 1")
    client.execute(query_status, {"version_date": version_date, "version_id": version_id})
    
    # ClickHouse ALTER UPDATE не поддерживает JOIN, поэтому переносим JOIN
    # в compact mapping partseqno_i -> repair_time и применяем его одним UPDATE.
    candidate_where = """
    WHERE hp.version_date = %(version_date)s
      AND hp.version_id = %(version_id)s
      AND toUInt32(ifNull(hp.group_by, 0)) >= 1
      AND toUInt8(ifNull(hp.status_id, 0)) = 4
      AND hp.target_date IS NOT NULL
      AND hp.target_date != toDate('1970-01-01')
      AND hp.target_date >= %(version_date)s
      AND (
          toUInt32(ifNull(hp.group_by, 0)) <= 2
          OR ifNull(hp.condition, '') != 'ИСПРАВНЫЙ'
      )
    """

    params = {"version_date": version_date, "version_id": version_id}
    candidate_count = int(
        client.execute(
            f"""
            SELECT count()
            FROM heli_pandas AS hp
            {candidate_where}
            """,
            params,
        )[0][0]
    )
    if candidate_count == 0:
        return 0

    ambiguous_rows = client.execute(
        f"""
        SELECT
            toUInt32(ifNull(hp.partseqno_i, 0)) AS partseqno_i,
            groupArrayDistinct(toUInt16(ifNull(md.repair_time, 0))) AS repair_times
        FROM heli_pandas AS hp
        LEFT JOIN md_components AS md
            ON hp.partseqno_i = md.partseqno_i
        {candidate_where}
        GROUP BY partseqno_i
        HAVING length(repair_times) > 1
        ORDER BY partseqno_i
        LIMIT 10
        """,
        params,
    )
    if ambiguous_rows:
        raise ValueError(
            "md_components.repair_time неоднозначен для partseqno_i "
            f"в future repair candidates: {ambiguous_rows}"
        )

    mapping_rows = client.execute(
        f"""
        SELECT
            toUInt32(ifNull(hp.partseqno_i, 0)) AS partseqno_i,
            toUInt16(min(ifNull(md.repair_time, 0))) AS repair_time
        FROM heli_pandas AS hp
        LEFT JOIN md_components AS md
            ON hp.partseqno_i = md.partseqno_i
        {candidate_where}
        GROUP BY partseqno_i
        ORDER BY partseqno_i
        """,
        params,
    )
    if not mapping_rows:
        raise ValueError("Не найден mapping partseqno_i -> repair_time для future repair candidates")

    cases = []
    for idx, (partseqno_i, repair_time) in enumerate(mapping_rows):
        part_key = f"partseqno_i_{idx}"
        repair_time_key = f"repair_time_{idx}"
        params[part_key] = int(partseqno_i)
        params[repair_time_key] = int(repair_time)
        cases.append(
            f"toUInt32(ifNull(partseqno_i, 0)) = %({part_key})s, "
            f"toUInt16(%({repair_time_key})s)"
        )

    repair_time_expr = "multiIf(\n            "
    repair_time_expr += ",\n            ".join(cases)
    repair_time_expr += ",\n            toUInt16(0)\n        )"

    update_query = f"""
    ALTER TABLE heli_pandas
    UPDATE repair_days = toUInt16(
        greatest(
            0,
            toInt32({repair_time_expr}) - toInt32(dateDiff('day', toDate(%(version_date)s), target_date))
        )
    )
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND toUInt32(ifNull(group_by, 0)) >= 1
      AND toUInt8(ifNull(status_id, 0)) = 4
      AND target_date IS NOT NULL
      AND target_date != toDate('1970-01-01')
      AND target_date >= %(version_date)s
      AND (
          toUInt32(ifNull(group_by, 0)) <= 2
          OR ifNull(condition, '') != 'ИСПРАВНЫЙ'
      )
    """
    client.execute("SET mutations_sync = 1")
    client.execute(update_query, params)

    return candidate_count


def main() -> int:
    args = parse_args()
    client = get_clickhouse_client()

    version_date, version_id = resolve_version(
        client, args.version_date, args.version_id
    )
    print(
        f"📅 Версия {version_date} (version_id={version_id}), "
        f"dry-run={'ON' if args.dry_run else 'OFF'}"
    )

    # Подсчёт кандидатов
    past_count = count_past_target_date(client, version_date, version_id)
    future_count = count_future_target_date(client, version_date, version_id)
    condition_ok_count = count_condition_ok_aggregates(client, version_date, version_id)

    print(f"📊 Агрегатов с target_date в ПРОШЛОМ (→ status_id=2): {past_count}")
    print(f"📊 Агрегатов с target_date в БУДУЩЕМ (→ status_id=4): {future_count}")
    print(
        "⚠️ Найдено агрегатов с condition='ИСПРАВНЫЙ' и валидным target_date: "
        f"{condition_ok_count}. Они исключаются из перевода в status_id=4."
    )

    if past_count == 0 and future_count == 0:
        print("✅ Нет агрегатов для обработки")
        return 0

    if args.dry_run:
        print("\n📝 DRY-RUN завершён без изменений")
        return 0

    # Обновление
    if past_count > 0:
        remaining = update_past_to_operations(client, version_date, version_id)
        updated = past_count - remaining
        print(f"✅ Обновлено: {updated} агрегатов → status_id=2 (ремонт завершён)")
    
    if future_count > 0:
        updated = update_future_to_repair(client, version_date, version_id)
        print(f"✅ Обновлено: {updated} агрегатов → status_id=4 + repair_days")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())










