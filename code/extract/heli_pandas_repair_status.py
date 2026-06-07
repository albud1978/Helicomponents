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
    
    # Теперь рассчитываем repair_days
    # repair_days = repair_time - days_remaining
    # days_remaining = target_date - version_date
    query_repair_days = """
    ALTER TABLE heli_pandas
    UPDATE repair_days = toUInt16(
        greatest(0, 
            toInt32(ifNull(md.repair_time, 0)) - toInt32(dateDiff('day', %(version_date)s, hp.target_date))
        )
    )
    WHERE hp.version_date = %(version_date)s
      AND hp.version_id = %(version_id)s
      AND toUInt32(ifNull(hp.group_by, 0)) >= 1
      AND hp.status_id = 4
      AND hp.target_date IS NOT NULL
      AND hp.target_date != toDate('1970-01-01')
      AND hp.target_date >= %(version_date)s
      AND hp.serialno IN (
          SELECT hp2.serialno
          FROM heli_pandas hp2
          INNER JOIN md_components md ON hp2.partseqno_i = md.partseqno_i
          WHERE hp2.version_date = %(version_date)s
            AND hp2.version_id = %(version_id)s
            AND toUInt32(ifNull(hp2.group_by, 0)) >= 1
            AND hp2.status_id = 4
      )
    """
    # К сожалению, ClickHouse не поддерживает JOIN в ALTER UPDATE
    # Используем другой подход: выбираем данные и обновляем построчно
    
    # Получаем список агрегатов для обновления repair_days
    select_query = """
    SELECT hp.serialno, hp.target_date, md.repair_time
    FROM heli_pandas hp
    LEFT JOIN md_components md ON hp.partseqno_i = md.partseqno_i
    WHERE hp.version_date = %(version_date)s
      AND hp.version_id = %(version_id)s
      AND toUInt32(ifNull(hp.group_by, 0)) >= 1
      AND hp.status_id = 4
      AND hp.target_date IS NOT NULL
      AND hp.target_date != toDate('1970-01-01')
      AND hp.target_date >= %(version_date)s
      AND (
          toUInt32(ifNull(hp.group_by, 0)) <= 2
          OR ifNull(hp.condition, '') != 'ИСПРАВНЫЙ'
      )
    """
    result = client.execute(select_query, {"version_date": version_date, "version_id": version_id})
    
    updated_count = 0
    for row in result:
        serialno, target_date, repair_time = row
        if repair_time is None:
            repair_time = 0
        
        # days_remaining = target_date - version_date
        days_remaining = (target_date - version_date).days
        repair_days = max(0, repair_time - days_remaining)
        
        update_query = """
        ALTER TABLE heli_pandas
        UPDATE repair_days = %(repair_days)s
        WHERE version_date = %(version_date)s
          AND version_id = %(version_id)s
          AND serialno = %(serialno)s
          AND status_id = 4
        """
        client.execute(update_query, {
            "version_date": version_date,
            "version_id": version_id,
            "serialno": serialno,
            "repair_days": repair_days
        })
        updated_count += 1
    
    return updated_count


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










