#!/usr/bin/env python3
"""
Микросервис установки status_id для агрегатов (финальный этап).

Обрабатывает пять категорий:
1. Beyond Repair: sne >= br → status_id=6 (Хранение)
2. ДОНОР: condition='ДОНОР' → status_id=6 (Хранение)
3. ВОЗМОЖНОЕ ПРОДЛЕНИЕ НР: condition='ВОЗМОЖНОЕ ПРОДЛЕНИЕ НР' → status_id=6 (Хранение)
4. Оставшиеся НЕИСПРАВНЫЕ: → status_id=7 (unserviceable/ремонтопригодный), repair_days=0
5. Fallback: любые агрегаты со status_id=0 (не попавшие в ветки выше, без учёта condition) → status_id=7, repair_days=0

Логика блока 4:
- Неисправные агрегаты без target_date → unserviceable (status_id=7, нет реального плана ремонта)
- repair_days=0 (нет обратного отсчёта — нет ремонта)
- P2/P3 квотирование решит дальнейшую судьбу агента

Условия:
- group_by > 2 (только агрегаты, не планеры)
- status_id = 0 (ещё не обработан)

Связь таблиц:
- heli_pandas.partseqno_i = md_components.partseqno_i

BR (Beyond Repair) выбирается по типу ВС:
- ac_type_mask & 32 → br_mi8
- ac_type_mask & 64 → br_mi17
- Если оба → берём минимум

Выполняется ПОСЛЕ heli_pandas_serviceable_status.py (этап 14).
Идемпотентен, поддерживает dry-run.
"""

import argparse
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

code_root = Path(__file__).resolve().parents[1]
sys.path.append(str(code_root / 'utils'))
sys.path.append(str(code_root))
from config_loader import get_clickhouse_client  # type: ignore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Установка status_id=6 для агрегатов beyond repair"
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


# SQL для поиска кандидатов: неисправные агрегаты с sne > br
CANDIDATES_SQL = """
SELECT 
    hp.serialno,
    hp.partno,
    hp.group_by,
    hp.sne,
    hp.ac_type_mask,
    md.br_mi8,
    md.br_mi17,
    -- Выбираем br по типу ВС
    CASE 
        WHEN bitAnd(toUInt8(ifNull(hp.ac_type_mask, 0)), 32) > 0 AND bitAnd(toUInt8(ifNull(hp.ac_type_mask, 0)), 64) > 0 
            THEN least(ifNull(md.br_mi8, 999999999), ifNull(md.br_mi17, 999999999))
        WHEN bitAnd(toUInt8(ifNull(hp.ac_type_mask, 0)), 32) > 0 
            THEN ifNull(md.br_mi8, 999999999)
        WHEN bitAnd(toUInt8(ifNull(hp.ac_type_mask, 0)), 64) > 0 
            THEN ifNull(md.br_mi17, 999999999)
        ELSE least(ifNull(md.br_mi8, 999999999), ifNull(md.br_mi17, 999999999))
    END as br_effective
FROM heli_pandas hp
LEFT JOIN md_components md ON hp.partseqno_i = md.partseqno_i
WHERE hp.version_date = %(version_date)s
  AND hp.version_id = %(version_id)s
  AND toUInt32(ifNull(hp.group_by, 0)) > 2
  AND upperUTF8(replaceRegexpAll(ifNull(hp.condition, ''), '^\\s+|\\s+$', '')) != 'ИСПРАВНЫЙ'
  AND toUInt8(ifNull(hp.status_id, 0)) = 0
  AND md.partseqno_i IS NOT NULL
"""

# SQL для подсчёта кандидатов с фильтром sne >= br
COUNT_SQL = """
SELECT count(*)
FROM (
    SELECT 
        hp.serialno,
        hp.sne,
        CASE 
            WHEN bitAnd(toUInt8(ifNull(hp.ac_type_mask, 0)), 32) > 0 AND bitAnd(toUInt8(ifNull(hp.ac_type_mask, 0)), 64) > 0 
                THEN least(ifNull(md.br_mi8, 999999999), ifNull(md.br_mi17, 999999999))
            WHEN bitAnd(toUInt8(ifNull(hp.ac_type_mask, 0)), 32) > 0 
                THEN ifNull(md.br_mi8, 999999999)
            WHEN bitAnd(toUInt8(ifNull(hp.ac_type_mask, 0)), 64) > 0 
                THEN ifNull(md.br_mi17, 999999999)
            ELSE least(ifNull(md.br_mi8, 999999999), ifNull(md.br_mi17, 999999999))
        END as br_effective
    FROM heli_pandas hp
    LEFT JOIN md_components md ON hp.partseqno_i = md.partseqno_i
    WHERE hp.version_date = %(version_date)s
      AND hp.version_id = %(version_id)s
      AND toUInt32(ifNull(hp.group_by, 0)) > 2
      AND upperUTF8(replaceRegexpAll(ifNull(hp.condition, ''), '^\\s+|\\s+$', '')) != 'ИСПРАВНЫЙ'
      AND toUInt8(ifNull(hp.status_id, 0)) = 0
      AND md.partseqno_i IS NOT NULL
) sub
WHERE sub.sne >= sub.br_effective
"""

# SQL для обновления
UPDATE_SQL = """
ALTER TABLE heli_pandas
UPDATE status_id = 6
WHERE version_date = %(version_date)s
  AND version_id = %(version_id)s
  AND toUInt32(ifNull(group_by, 0)) > 2
  AND upperUTF8(replaceRegexpAll(ifNull(condition, ''), '^\\s+|\\s+$', '')) != 'ИСПРАВНЫЙ'
  AND toUInt8(ifNull(status_id, 0)) = 0
  AND serialno IN (
      SELECT hp.serialno
      FROM heli_pandas hp
      LEFT JOIN md_components md ON hp.partseqno_i = md.partseqno_i
      WHERE hp.version_date = %(version_date)s
        AND hp.version_id = %(version_id)s
        AND toUInt32(ifNull(hp.group_by, 0)) > 2
        AND upperUTF8(replaceRegexpAll(ifNull(hp.condition, ''), '^\\s+|\\s+$', '')) != 'ИСПРАВНЫЙ'
        AND toUInt8(ifNull(hp.status_id, 0)) = 0
        AND md.partseqno_i IS NOT NULL
        AND hp.sne >= (
            CASE 
                WHEN bitAnd(toUInt8(ifNull(hp.ac_type_mask, 0)), 32) > 0 AND bitAnd(toUInt8(ifNull(hp.ac_type_mask, 0)), 64) > 0 
                    THEN least(ifNull(md.br_mi8, 999999999), ifNull(md.br_mi17, 999999999))
                WHEN bitAnd(toUInt8(ifNull(hp.ac_type_mask, 0)), 32) > 0 
                    THEN ifNull(md.br_mi8, 999999999)
                WHEN bitAnd(toUInt8(ifNull(hp.ac_type_mask, 0)), 64) > 0 
                    THEN ifNull(md.br_mi17, 999999999)
                ELSE least(ifNull(md.br_mi8, 999999999), ifNull(md.br_mi17, 999999999))
            END
        )
  )
"""


def fetch_stats(client, version_date: date, version_id: int) -> int:
    """Подсчитывает количество кандидатов для status_id=6"""
    params = {"version_date": version_date, "version_id": version_id}
    result = client.execute(COUNT_SQL, params)
    return int(result[0][0])


def fetch_candidates_details(client, version_date: date, version_id: int, limit: int = 10):
    """Получает детали кандидатов для отображения"""
    params = {"version_date": version_date, "version_id": version_id}
    
    detail_sql = CANDIDATES_SQL + " LIMIT %(limit)s"
    params["limit"] = limit
    
    result = client.execute(detail_sql, params)
    
    candidates = []
    for row in result:
        serialno, partno, group_by, sne, ac_type_mask, br_mi8, br_mi17, br_effective = row
        if br_effective is not None and sne >= br_effective:
            candidates.append({
                'serialno': serialno,
                'partno': partno,
                'group_by': group_by,
                'sne': sne,
                'br': br_effective,
                'ac_type_mask': ac_type_mask
            })
    
    return candidates


def run_update(client, version_date: date, version_id: int) -> None:
    """Выполняет UPDATE"""
    params = {"version_date": version_date, "version_id": version_id}
    client.execute("SET mutations_sync = 1")
    client.execute(UPDATE_SQL, params)


def _as_u32(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="raise").fillna(0).astype("int64")


def _normalized_text(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.upper()


def _load_br_rows(client) -> dict[int, list[tuple[Optional[int], Optional[int]]]]:
    rows = client.execute("SELECT partseqno_i, br_mi8, br_mi17 FROM md_components")
    result: dict[int, list[tuple[Optional[int], Optional[int]]]] = {}
    for partseqno_i, br_mi8, br_mi17 in rows:
        if partseqno_i is None:
            continue
        result.setdefault(int(partseqno_i), []).append(
            (
                None if br_mi8 is None else int(br_mi8),
                None if br_mi17 is None else int(br_mi17),
            )
        )
    return result


def _storage_br_effective(ac_type_mask: int, br_mi8: Optional[int], br_mi17: Optional[int]) -> int:
    sentinel = 999_999_999
    b8 = sentinel if br_mi8 is None else int(br_mi8)
    b17 = sentinel if br_mi17 is None else int(br_mi17)
    if ac_type_mask & 32 and ac_type_mask & 64:
        return min(b8, b17)
    if ac_type_mask & 32:
        return b8
    if ac_type_mask & 64:
        return b17
    return min(b8, b17)


def _matches_storage_br(
    partseqno_i: int,
    sne,
    ac_type_mask: int,
    br_rows: dict[int, list[tuple[Optional[int], Optional[int]]]],
) -> bool:
    if pd.isna(sne) or partseqno_i not in br_rows:
        return False
    sne_value = int(sne)
    return any(
        sne_value >= _storage_br_effective(ac_type_mask, br_mi8, br_mi17)
        for br_mi8, br_mi17 in br_rows[partseqno_i]
    )


def apply_storage_status(
    df: pd.DataFrame, client, version_date: date, version_id: int
) -> pd.DataFrame:
    """Применяет storage/unserviceable этап к DataFrame без ALTER UPDATE."""
    del version_date, version_id
    updated = df.copy()
    required = {"group_by", "status_id", "condition", "partseqno_i", "sne", "ac_type_mask", "repair_days"}
    missing = required - set(updated.columns)
    if missing:
        raise ValueError(f"heli_pandas DataFrame missing columns: {sorted(missing)}")

    br_rows = _load_br_rows(client)
    group_by = _as_u32(updated["group_by"])
    status_id = _as_u32(updated["status_id"])
    condition_norm = _normalized_text(updated["condition"])
    partseqno = _as_u32(updated["partseqno_i"])
    ac_type_mask = _as_u32(updated["ac_type_mask"])

    br_matches = pd.Series(
        [
            _matches_storage_br(int(part), sne, int(mask), br_rows)
            for part, sne, mask in zip(partseqno, updated["sne"], ac_type_mask)
        ],
        index=updated.index,
    )
    br_mask = (
        (group_by > 2)
        & (condition_norm != "ИСПРАВНЫЙ")
        & (status_id == 0)
        & br_matches
    )
    updated.loc[br_mask, "status_id"] = 6

    group_by = _as_u32(updated["group_by"])
    status_id = _as_u32(updated["status_id"])
    condition_norm = _normalized_text(updated["condition"])
    donor_mask = (group_by > 2) & (status_id == 0) & (condition_norm == "ДОНОР")
    updated.loc[donor_mask, "status_id"] = 6

    status_id = _as_u32(updated["status_id"])
    extension_mask = (
        (group_by > 2)
        & (status_id == 0)
        & (condition_norm == "ВОЗМОЖНОЕ ПРОДЛЕНИЕ НР")
    )
    updated.loc[extension_mask, "status_id"] = 6

    status_id = _as_u32(updated["status_id"])
    unserviceable_mask = (
        (group_by > 2) & (status_id == 0) & (condition_norm == "НЕИСПРАВНЫЙ")
    )
    updated.loc[unserviceable_mask, ["status_id", "repair_days"]] = [7, 0]

    status_id = _as_u32(updated["status_id"])
    fallback_mask = (group_by > 2) & (status_id == 0)
    updated.loc[fallback_mask, ["status_id", "repair_days"]] = [7, 0]

    print(
        "✅ storage_status in-memory: "
        f"br={int(pd.Series(br_mask).sum())}, donor={int(donor_mask.sum())}, "
        f"extension={int(extension_mask.sum())}, "
        f"unserviceable={int(unserviceable_mask.sum())}, "
        f"fallback={int(fallback_mask.sum())}"
    )
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

    params = {"version_date": version_date, "version_id": version_id}

    # --- Блок 1: sne >= br → хранение (6) ---
    candidates_count = fetch_stats(client, version_date, version_id)
    print(f"📊 Неисправных агрегатов с sne >= br (beyond repair): {candidates_count}")

    if candidates_count == 0:
        print("ℹ️ Нет агрегатов (sne >= br) для перевода в хранение")
    elif args.dry_run:
        print("\n📝 DRY-RUN — примеры кандидатов (BR):")
        details = fetch_candidates_details(client, version_date, version_id, limit=10)
        for d in details:
            print(f"   {d['serialno']} ({d['partno']}): sne={d['sne']:,} > br={d['br']:,}")
        print("📝 DRY-RUN: блок BR без записи в БД")

    # --- Блок 2: ДОНОР → Хранение ---
    donor_count_sql = """
    SELECT count(*) 
    FROM heli_pandas 
    WHERE group_by > 2 
      AND status_id = 0 
      AND upperUTF8(replaceRegexpAll(ifNull(condition, ''), '^\\s+|\\s+$', '')) = 'ДОНОР'
      AND version_date = %(version_date)s
      AND version_id = %(version_id)s
    """
    donor_count = int(client.execute(donor_count_sql, params)[0][0])
    
    if donor_count > 0:
        print(f"📊 ДОНОР агрегатов со status_id=0: {donor_count}")
        
    else:
        print("ℹ️ Нет ДОНОР агрегатов со status_id=0")
    
    # --- Блок 3: ВОЗМОЖНОЕ ПРОДЛЕНИЕ НР → Хранение ---
    prodlenie_count_sql = """
    SELECT count(*) 
    FROM heli_pandas 
    WHERE group_by > 2 
      AND status_id = 0 
      AND upperUTF8(replaceRegexpAll(ifNull(condition, ''), '^\\s+|\\s+$', '')) = 'ВОЗМОЖНОЕ ПРОДЛЕНИЕ НР'
      AND version_date = %(version_date)s
      AND version_id = %(version_id)s
    """
    prodlenie_count = int(client.execute(prodlenie_count_sql, params)[0][0])
    
    if prodlenie_count > 0:
        print(f"📊 ВОЗМОЖНОЕ ПРОДЛЕНИЕ НР агрегатов со status_id=0: {prodlenie_count}")
        
    else:
        print("ℹ️ Нет ВОЗМОЖНОЕ ПРОДЛЕНИЕ НР агрегатов со status_id=0")
    
    # --- Блок 4: Оставшиеся НЕИСПРАВНЫЕ → unserviceable (status_id=7, repair_days=0) ---
    # FIX: без target_date нет реального ремонта → unserviceable, не repair
    # P2/P3 квотирование решит дальнейшую судьбу агента
    remaining_count_sql = """
    SELECT count(*) 
    FROM heli_pandas 
    WHERE group_by > 2 
      AND status_id = 0 
      AND upperUTF8(replaceRegexpAll(ifNull(condition, ''), '^\\s+|\\s+$', '')) = 'НЕИСПРАВНЫЙ'
      AND version_date = %(version_date)s
      AND version_id = %(version_id)s
    """
    remaining_count = int(client.execute(remaining_count_sql, params)[0][0])
    
    if remaining_count > 0:
        print(f"📊 Оставшихся НЕИСПРАВНЫХ агрегатов со status_id=0: {remaining_count}")
        
    else:
        print("ℹ️ Нет оставшихся НЕИСПРАВНЫХ агрегатов со status_id=0")

    # --- Блок 5: Fallback — агрегаты со status_id=0 без учёта condition → как «неисправный» (7) ---
    # Закрывает редкие значения condition (рекламация и т.п.), не попавшие в явные ветки выше.
    # Только group_by > 2; планеры (1,2) не трогаем.
    fallback_sql = """
    SELECT count(*)
    FROM heli_pandas
    WHERE toUInt32(ifNull(group_by, 0)) > 2
      AND toUInt8(ifNull(status_id, 0)) = 0
      AND version_date = %(version_date)s
      AND version_id = %(version_id)s
    """
    fallback_count = int(client.execute(fallback_sql, params)[0][0])
    if fallback_count > 0:
        print(
            f"📊 Агрегатов со status_id=0 (fallback, любой condition): {fallback_count}"
        )
    else:
        print("ℹ️ Нет агрегатов для fallback (status_id=0)")

    if args.dry_run:
        print("📝 DRY-RUN завершён без изменений")
        return 0

    from utils.dwh_post_enrichment import (
        _load_heli_pandas_version,
        _replace_heli_pandas_version,
    )

    df = _load_heli_pandas_version(client, version_date, version_id)
    updated_df = apply_storage_status(df, client, version_date, version_id)
    _replace_heli_pandas_version(client, updated_df, version_date, version_id)
    print("✅ storage_status применён через in-memory replace-slice")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

