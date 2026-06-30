#!/usr/bin/env python3
"""
Day-0 economics screen for serviceable planers.

Only planers already classified as status_id=3 are checked. Economically
unreasonable planers move to status_id=7; all other statuses stay untouched.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

code_root = Path(__file__).resolve().parents[1]
sys.path.append(str(code_root / "utils"))
sys.path.append(str(code_root))
sys.path.append(str(code_root / "sim_v2" / "messaging"))

from config_loader import get_clickhouse_client  # type: ignore
from static_data_resolver import resolve_economics_workbook  # type: ignore
from precompute_events import get_economics_costs_for_date  # type: ignore


Candidate = Tuple[str, int, int, int, int, int]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Day-0 economics screen: status_id=3 planers -> status_id=7"
    )
    parser.add_argument("--version-date", type=str, help="Дата версии данных (YYYY-MM-DD)")
    parser.add_argument("--version-id", type=int, help="ID версии данных (UInt8)")
    parser.add_argument("--dry-run", action="store_true", help="Не выполнять ALTER UPDATE")
    return parser.parse_args()


def resolve_version(client, version_date: Optional[str], version_id: Optional[int]) -> Tuple[date, int]:
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
    required = {
        "serialno",
        "partseqno_i",
        "group_by",
        "status_id",
        "oh",
        "ppr",
        "aircraft_number",
        "repair_days",
    }
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


def fetch_candidates(client, version_date: date, version_id: int) -> List[Candidate]:
    rows = client.execute(
        """
        SELECT
            serialno,
            toUInt32(ifNull(partseqno_i, 0)) AS partseqno_i,
            toUInt8(ifNull(group_by, 0)) AS group_by,
            toUInt32(ifNull(oh, 0)) AS oh,
            toUInt32(ifNull(ppr, 0)) AS ppr,
            toUInt32(ifNull(aircraft_number, 0)) AS aircraft_number
        FROM heli_pandas
        WHERE version_date = %(version_date)s
          AND version_id = %(version_id)s
          AND toUInt8(ifNull(status_id, 0)) = 3
          AND toUInt8(ifNull(group_by, 0)) IN (1, 2)
        ORDER BY group_by, aircraft_number, serialno
        """,
        {"version_date": version_date, "version_id": version_id},
    )
    return [
        (str(serialno), int(partseqno_i), int(group_by), int(oh), int(ppr), int(aircraft_number))
        for serialno, partseqno_i, group_by, oh, ppr, aircraft_number in rows
    ]


def _cost_pair_for_group(group_by: int, costs: Dict[str, int]) -> Tuple[int, int]:
    if group_by == 1:
        return costs["repair_cost_mi8"], costs["ferry_cost_mi8"]
    if group_by == 2:
        return costs["repair_cost_mi17"], costs["ferry_cost_mi17"]
    raise ValueError(f"Unsupported planer group_by={group_by}")


def classify_demotions(
    candidates: List[Candidate],
    costs: Dict[str, int],
) -> Tuple[List[Candidate], List[Candidate], List[Candidate]]:
    demote: List[Candidate] = []
    keep: List[Candidate] = []
    oh_zero: List[Candidate] = []

    for row in candidates:
        _, _, group_by, oh, ppr, _ = row
        if oh == 0:
            oh_zero.append(row)
            keep.append(row)
            continue

        repair_cost, ferry_cost = _cost_pair_for_group(group_by, costs)
        remaining = oh - ppr
        if remaining * repair_cost < ferry_cost * oh:
            demote.append(row)
        else:
            keep.append(row)

    return demote, keep, oh_zero


def apply_demotions(client, version_date: date, version_id: int, demote: List[Candidate]) -> None:
    if not demote:
        return

    client.execute("SET mutations_sync = 1")
    for serialno, partseqno_i, group_by, _, _, _ in demote:
        client.execute(
            """
            ALTER TABLE heli_pandas
            UPDATE status_id = 7, repair_days = 0
            WHERE version_date = %(version_date)s
              AND version_id = %(version_id)s
              AND serialno = %(serialno)s
              AND toUInt32(ifNull(partseqno_i, 0)) = %(partseqno_i)s
              AND toUInt8(ifNull(group_by, 0)) = %(group_by)s
              AND toUInt8(ifNull(status_id, 0)) = 3
            """,
            {
                "version_date": version_date,
                "version_id": version_id,
                "serialno": serialno,
                "partseqno_i": partseqno_i,
                "group_by": group_by,
            },
        )


def _count_by_group(rows: List[Candidate]) -> Dict[int, int]:
    counts: Dict[int, int] = {}
    for _, _, group_by, _, _, _ in rows:
        counts[group_by] = counts.get(group_by, 0) + 1
    return counts


def main() -> int:
    args = parse_args()
    client = get_clickhouse_client()
    ensure_columns(client)

    version_date, version_id = resolve_version(client, args.version_date, args.version_id)
    workbook_path = resolve_economics_workbook()
    costs = get_economics_costs_for_date(version_date, workbook_path=workbook_path)
    candidates = fetch_candidates(client, version_date, version_id)
    demote, keep, oh_zero = classify_demotions(candidates, costs)

    print(
        f"📅 Версия {version_date} (version_id={version_id}), "
        f"dry-run={'ON' if args.dry_run else 'OFF'}"
    )
    print(f"📘 Economics.xlsx: {workbook_path}")
    print(f"💰 Стоимости: {costs}")
    print(f"📊 status_id=3 планеров-кандидатов: {len(candidates)}")
    print(f"📊 Останутся status=3: {len(keep)} by group_by={_count_by_group(keep)}")
    print(f"📊 Перейдут 3→7: {len(demote)} by group_by={_count_by_group(demote)}")
    if oh_zero:
        print(f"⚠️ Кандидатов с oh=0 оставлено без изменения: {len(oh_zero)}")
    for row in demote[:10]:
        serialno, _, group_by, oh, ppr, aircraft_number = row
        print(
            f"   3→7 serialno={serialno}, aircraft_number={aircraft_number}, "
            f"group_by={group_by}, oh={oh}, ppr={ppr}"
        )

    if args.dry_run:
        print("📝 DRY-RUN завершён без изменений")
        return 0

    apply_demotions(client, version_date, version_id, demote)
    print(f"✅ Обновлено status_id=7: {len(demote)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
