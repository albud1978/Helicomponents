#!/usr/bin/env python3
"""
Финальный BR-gate для heli_pandas.

Логика:
- Обрабатывает ТОЛЬКО status_id IN (1, 7)
- Для всех group_by (без ограничений)
- br_effective вычисляется через md_components и ac_type_mask
- В terminal (status_id=6) переводим только при br_effective > 0 и sne >= br_effective
- Для переведённых ставим repair_days = 0
"""

import argparse
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Tuple, List, Dict

import pandas as pd

code_root = Path(__file__).resolve().parents[1]
sys.path.append(str(code_root / 'utils'))
sys.path.append(str(code_root))
from config_loader import get_clickhouse_client  # type: ignore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Финальный BR-gate: перевод status_id=1/7 в terminal по BR"
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
        help="Только вывести статистику и примеры без выполнения UPDATE",
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


def _br_effective_expr(hp_alias: str, md_alias: str) -> str:
    br_mi8 = f"toUInt32(ifNull({md_alias}.br_mi8, 0))"
    br_mi17 = f"toUInt32(ifNull({md_alias}.br_mi17, 0))"
    mask = f"toUInt8(ifNull({hp_alias}.ac_type_mask, 0))"
    pick_available = (
        f"multiIf({br_mi8} > 0 AND {br_mi17} > 0, least({br_mi8}, {br_mi17}), "
        f"{br_mi8} > 0, {br_mi8}, "
        f"{br_mi17} > 0, {br_mi17}, "
        f"toUInt32(0))"
    )
    return (
        f"CASE "
        f"WHEN bitAnd({mask}, 32) > 0 AND bitAnd({mask}, 64) > 0 THEN {pick_available} "
        f"WHEN bitAnd({mask}, 32) > 0 THEN {br_mi8} "
        f"WHEN bitAnd({mask}, 64) > 0 THEN {br_mi17} "
        f"ELSE {pick_available} "
        f"END"
    )


BR_EFFECTIVE = _br_effective_expr("hp", "md")
SNE_VALUE = "toUInt32(ifNull(hp.sne, 0))"

BASE_CONDITION = """
    hp.version_date = %(version_date)s
    AND hp.version_id = %(version_id)s
    AND toUInt8(ifNull(hp.status_id, 0)) IN (1, 7)
"""

COUNT_SQL = f"""
SELECT count()
FROM heli_pandas hp
LEFT JOIN md_components md ON hp.partseqno_i = md.partseqno_i
WHERE {BASE_CONDITION}
  AND {BR_EFFECTIVE} > 0
  AND {SNE_VALUE} >= {BR_EFFECTIVE}
"""

DETAILS_SQL = f"""
SELECT
    hp.serialno,
    hp.partno,
    hp.group_by,
    hp.status_id,
    hp.sne,
    hp.ac_type_mask,
    md.br_mi8,
    md.br_mi17,
    {BR_EFFECTIVE} AS br_effective
FROM heli_pandas hp
LEFT JOIN md_components md ON hp.partseqno_i = md.partseqno_i
WHERE {BASE_CONDITION}
  AND {BR_EFFECTIVE} > 0
  AND {SNE_VALUE} >= {BR_EFFECTIVE}
ORDER BY hp.serialno
LIMIT %(limit)s
"""

UPDATE_SQL = f"""
ALTER TABLE heli_pandas
UPDATE status_id = 6, repair_days = 0
WHERE version_date = %(version_date)s
  AND version_id = %(version_id)s
  AND toUInt8(ifNull(status_id, 0)) IN (1, 7)
  AND (serialno, partseqno_i, group_by) IN (
      SELECT hp.serialno, hp.partseqno_i, hp.group_by
      FROM heli_pandas hp
      LEFT JOIN md_components md ON hp.partseqno_i = md.partseqno_i
      WHERE {BASE_CONDITION}
        AND {BR_EFFECTIVE} > 0
        AND {SNE_VALUE} >= {BR_EFFECTIVE}
  )
"""


def fetch_candidates_count(client, version_date: date, version_id: int) -> int:
    params = {"version_date": version_date, "version_id": version_id}
    result = client.execute(COUNT_SQL, params)
    return int(result[0][0])


def fetch_candidate_examples(
    client, version_date: date, version_id: int, limit: int = 10
) -> List[Dict[str, object]]:
    params = {"version_date": version_date, "version_id": version_id, "limit": limit}
    result = client.execute(DETAILS_SQL, params)
    examples = []
    for row in result:
        (
            serialno,
            partno,
            group_by,
            status_id,
            sne,
            ac_type_mask,
            br_mi8,
            br_mi17,
            br_effective,
        ) = row
        examples.append(
            {
                "serialno": serialno,
                "partno": partno,
                "group_by": group_by,
                "status_id": status_id,
                "sne": sne,
                "ac_type_mask": ac_type_mask,
                "br_mi8": br_mi8,
                "br_mi17": br_mi17,
                "br_effective": br_effective,
            }
        )
    return examples


def run_update(client, version_date: date, version_id: int) -> None:
    params = {"version_date": version_date, "version_id": version_id}
    client.execute("SET mutations_sync = 1")
    client.execute(UPDATE_SQL, params)


def _as_u32(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="raise").fillna(0).astype("int64")


def _load_br_rows(client) -> dict[int, list[tuple[int, int]]]:
    rows = client.execute("SELECT partseqno_i, br_mi8, br_mi17 FROM md_components")
    result: dict[int, list[tuple[int, int]]] = {}
    for partseqno_i, br_mi8, br_mi17 in rows:
        if partseqno_i is None:
            continue
        result.setdefault(int(partseqno_i), []).append((int(br_mi8 or 0), int(br_mi17 or 0)))
    return result


def _terminal_br_effective(ac_type_mask: int, br_mi8: int, br_mi17: int) -> int:
    if br_mi8 > 0 and br_mi17 > 0:
        pick_available = min(br_mi8, br_mi17)
    elif br_mi8 > 0:
        pick_available = br_mi8
    elif br_mi17 > 0:
        pick_available = br_mi17
    else:
        pick_available = 0
    if ac_type_mask & 32 and ac_type_mask & 64:
        return pick_available
    if ac_type_mask & 32:
        return br_mi8
    if ac_type_mask & 64:
        return br_mi17
    return pick_available


def _matches_terminal_br(
    partseqno_i: int,
    sne,
    ac_type_mask: int,
    br_rows: dict[int, list[tuple[int, int]]],
) -> bool:
    if pd.isna(sne):
        return False
    sne_value = int(sne)
    return any(
        (br := _terminal_br_effective(ac_type_mask, br_mi8, br_mi17)) > 0
        and sne_value >= br
        for br_mi8, br_mi17 in br_rows.get(partseqno_i, [])
    )


def apply_terminal_br_gate(
    df: pd.DataFrame, client, version_date: date, version_id: int
) -> pd.DataFrame:
    """Применяет финальный BR-gate к DataFrame без ALTER UPDATE."""
    del version_date, version_id
    updated = df.copy()
    required = {"status_id", "partseqno_i", "group_by", "sne", "ac_type_mask", "repair_days"}
    missing = required - set(updated.columns)
    if missing:
        raise ValueError(f"heli_pandas DataFrame missing columns: {sorted(missing)}")

    br_rows = _load_br_rows(client)
    status_id = _as_u32(updated["status_id"])
    partseqno = _as_u32(updated["partseqno_i"])
    ac_type_mask = _as_u32(updated["ac_type_mask"])
    br_matches = pd.Series(
        [
            _matches_terminal_br(int(part), sne, int(mask_value), br_rows)
            for part, sne, mask_value in zip(partseqno, updated["sne"], ac_type_mask)
        ],
        index=updated.index,
    )
    mask = status_id.isin([1, 7]) & br_matches
    updated.loc[mask, ["status_id", "repair_days"]] = [6, 0]
    print(f"✅ terminal_br_gate in-memory: updated={int(pd.Series(mask).sum())}")
    return updated


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

    candidates = fetch_candidates_count(client, version_date, version_id)
    print(f"📊 Кандидатов (status_id IN 1,7) для terminal BR-gate: {candidates}")

    if candidates == 0:
        print("✅ Нет записей для перевода в terminal")
        return 0

    if args.dry_run:
        print("\n📝 DRY-RUN — примеры кандидатов:")
        for example in fetch_candidate_examples(client, version_date, version_id, limit=10):
            print(
                "   {serialno} ({partno}): group_by={group_by}, status_id={status_id}, "
                "sne={sne}, br_effective={br_effective}".format(**example)
            )
        print("\n📝 DRY-RUN завершён без изменений")
        return 0

    from utils.dwh_post_enrichment import (
        _load_heli_pandas_version,
        _replace_heli_pandas_version,
    )

    df = _load_heli_pandas_version(client, version_date, version_id)
    updated_df = apply_terminal_br_gate(df, client, version_date, version_id)
    _replace_heli_pandas_version(client, updated_df, version_date, version_id)

    remaining = fetch_candidates_count(client, version_date, version_id)
    updated = candidates - remaining
    print(
        "✅ Обновлено: "
        f"{updated} записей → status_id=6 (terminal), repair_days=0"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
