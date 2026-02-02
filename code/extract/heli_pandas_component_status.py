#!/usr/bin/env python3
"""
Пометка компонентов (aggregates), установленных на планерах, статусом 2 (эксплуатация).

Условия:
- aircraft_number > 0 (компонент привязан к планеру)
- group_by > 2 (компоненты, не планеры)
- condition = 'ИСПРАВНЫЙ' (по данным Status_Components.xlsx)

Скрипт обновляет записи текущей версии в heli_pandas так, чтобы все подходящие
компоненты имели status_id = 2. Идемпотентен, поддерживает dry-run.
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


PLANE_FILTER = """
    SELECT DISTINCT aircraft_number
    FROM heli_pandas
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND toUInt8(ifNull(status_id, 0)) = 2
      AND toUInt32(ifNull(aircraft_number, 0)) > 0
      AND toUInt32(ifNull(group_by, 0)) IN (1, 2)
"""

BASE_CONDITION = f"""
    version_date = %(version_date)s
    AND version_id = %(version_id)s
    AND toUInt32(ifNull(group_by, 0)) > 2
    AND toUInt32(ifNull(aircraft_number, 0)) > 0
    AND aircraft_number IN (
        {PLANE_FILTER}
    )
    AND upperUTF8(
        replaceRegexpAll(ifNull(condition, ''), '^\\s+|\\s+$', '')
    ) = 'ИСПРАВНЫЙ'
"""

STATS_SQL = f"""
SELECT
    count() AS total_candidates,
    sum(toUInt8(ifNull(status_id, 0)) = 2) AS already_ops,
    sum(toUInt8(ifNull(status_id, 0)) != 2) AS need_update
FROM heli_pandas
WHERE {BASE_CONDITION}
"""

UPDATE_SQL = f"""
ALTER TABLE heli_pandas
UPDATE status_id = 2
WHERE {BASE_CONDITION}
  AND toUInt8(ifNull(status_id, 0)) != 2
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Помечает компоненты на планерах статусом 2 (эксплуатация)"
    )
    parser.add_argument(
        "--version-date",
        type=str,
        help="Дата версии данных (YYYY-MM-DD). Обязательна при запуске из оркестратора.",
    )
    parser.add_argument(
        "--version-id",
        type=int,
        help="ID версии данных (UInt8). Обязателен при запуске из оркестратора.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Только вывести статистику без выполнения ALTER UPDATE",
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


def ensure_columns(client) -> None:
    required = {"aircraft_number", "group_by", "status_id", "condition"}
    rows = client.execute(
        """
        SELECT name
        FROM system.columns
        WHERE database = currentDatabase()
          AND table = 'heli_pandas'
        """
    )
    existing = {name for (name,) in rows}
    missing = required - existing
    if missing:
        raise RuntimeError(f"В heli_pandas отсутствуют колонки: {sorted(missing)}")


def fetch_stats(client, version_date: date, version_id: int) -> Tuple[int, int, int]:
    params = {"version_date": version_date, "version_id": version_id}
    total, already, need = client.execute(STATS_SQL, params)[0]
    return int(total), int(already), int(need)


def run_update(client, version_date: date, version_id: int) -> None:
    params = {"version_date": version_date, "version_id": version_id}
    client.execute("SET mutations_sync = 1")
    client.execute(UPDATE_SQL, params)


def main() -> int:
    args = parse_args()
    client = get_clickhouse_client()
    ensure_columns(client)

    version_date, version_id = resolve_version(
        client, args.version_date, args.version_id
    )
    print(
        f"📅 Версия {version_date} (version_id={version_id}), "
        f"dry-run={'ON' if args.dry_run else 'OFF'}"
    )

    total, already, need = fetch_stats(client, version_date, version_id)
    print(
        f"📊 Кандидатов: {total}, уже status=2: {already}, требуется обновить: {need}"
    )

    if total == 0:
        print("ℹ️ Нет агрегатов, подходящих под критерии — обновление не требуется")
        return 0

    if need == 0:
        print("✅ Все агрегаты на планерах уже имеют status_id=2")
        return 0

    if args.dry_run:
        print("📝 DRY-RUN завершён без изменений")
        return 0

    run_update(client, version_date, version_id)
    _, already_after, need_after = fetch_stats(client, version_date, version_id)
    print(
        f"✅ Обновление завершено. Теперь status=2: {already_after}, осталось не обновлённых: {need_after}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

