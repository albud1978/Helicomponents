#!/usr/bin/env python3
"""
Микросервис установки status_id для агрегатов (финальный этап).

Обрабатывает четыре категории:
1. Beyond Repair: sne >= br → status_id=6 (Хранение)
2. ДОНОР: condition='ДОНОР' → status_id=6 (Хранение)
3. ВОЗМОЖНОЕ ПРОДЛЕНИЕ НР: condition='ВОЗМОЖНОЕ ПРОДЛЕНИЕ НР' → status_id=6 (Хранение)
4. Оставшиеся НЕИСПРАВНЫЕ: → status_id=4 (Ремонт), repair_days=0

Логика блока 4:
- Неисправные агрегаты без target_date отправляются в ремонт
- repair_days=0 означает "ремонт только начался"
- После симуляции они станут исправными по мере завершения ремонта

Условия:
- group_by > 2 (только агрегаты, не планеры)
- status_id = 0 (ещё не обработан)

Связь таблиц:
- heli_pandas.partseqno_i = md_components.partno_comp

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

sys.path.append(str(Path(__file__).parent / 'utils'))
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
LEFT JOIN md_components md ON hp.partseqno_i = md.partno_comp
WHERE hp.version_date = %(version_date)s
  AND hp.version_id = %(version_id)s
  AND toUInt32(ifNull(hp.group_by, 0)) > 2
  AND upperUTF8(replaceRegexpAll(ifNull(hp.condition, ''), '^\\s+|\\s+$', '')) != 'ИСПРАВНЫЙ'
  AND toUInt8(ifNull(hp.status_id, 0)) = 0
  AND md.partno_comp IS NOT NULL
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
    LEFT JOIN md_components md ON hp.partseqno_i = md.partno_comp
    WHERE hp.version_date = %(version_date)s
      AND hp.version_id = %(version_id)s
      AND toUInt32(ifNull(hp.group_by, 0)) > 2
      AND upperUTF8(replaceRegexpAll(ifNull(hp.condition, ''), '^\\s+|\\s+$', '')) != 'ИСПРАВНЫЙ'
      AND toUInt8(ifNull(hp.status_id, 0)) = 0
      AND md.partno_comp IS NOT NULL
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
      LEFT JOIN md_components md ON hp.partseqno_i = md.partno_comp
      WHERE hp.version_date = %(version_date)s
        AND hp.version_id = %(version_id)s
        AND toUInt32(ifNull(hp.group_by, 0)) > 2
        AND upperUTF8(replaceRegexpAll(ifNull(hp.condition, ''), '^\\s+|\\s+$', '')) != 'ИСПРАВНЫЙ'
        AND toUInt8(ifNull(hp.status_id, 0)) = 0
        AND md.partno_comp IS NOT NULL
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
    candidates_count = fetch_stats(client, version_date, version_id)
    print(f"📊 Неисправных агрегатов с sne >= br (beyond repair): {candidates_count}")

    if candidates_count == 0:
        print("✅ Нет агрегатов для перевода в хранение")
        return 0

    # Показываем примеры
    if args.dry_run:
        print("\n📝 DRY-RUN — примеры кандидатов:")
        details = fetch_candidates_details(client, version_date, version_id, limit=10)
        for d in details:
            print(f"   {d['serialno']} ({d['partno']}): sne={d['sne']:,} > br={d['br']:,}")
        print(f"\n📝 DRY-RUN завершён без изменений")
        return 0

    # Выполняем обновление
    run_update(client, version_date, version_id)
    
    # Проверяем результат
    remaining = fetch_stats(client, version_date, version_id)
    updated = candidates_count - remaining
    
    print(f"✅ Обновлено: {updated} агрегатов (sne >= br) → status_id=6 (Хранение)")

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
    params = {"version_date": version_date, "version_id": version_id}
    donor_count = int(client.execute(donor_count_sql, params)[0][0])
    
    if donor_count > 0:
        print(f"📊 ДОНОР агрегатов со status_id=0: {donor_count}")
        
        if not args.dry_run:
            donor_update_sql = """
            ALTER TABLE heli_pandas
            UPDATE status_id = 6
            WHERE group_by > 2 
              AND status_id = 0 
              AND upperUTF8(replaceRegexpAll(ifNull(condition, ''), '^\\s+|\\s+$', '')) = 'ДОНОР'
              AND version_date = %(version_date)s
              AND version_id = %(version_id)s
            """
            client.execute(donor_update_sql, params)
            print(f"✅ Обновлено: {donor_count} ДОНОР → status_id=6 (Хранение)")
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
        
        if not args.dry_run:
            prodlenie_update_sql = """
            ALTER TABLE heli_pandas
            UPDATE status_id = 6
            WHERE group_by > 2 
              AND status_id = 0 
              AND upperUTF8(replaceRegexpAll(ifNull(condition, ''), '^\\s+|\\s+$', '')) = 'ВОЗМОЖНОЕ ПРОДЛЕНИЕ НР'
              AND version_date = %(version_date)s
              AND version_id = %(version_id)s
            """
            client.execute(prodlenie_update_sql, params)
            print(f"✅ Обновлено: {prodlenie_count} ВОЗМОЖНОЕ ПРОДЛЕНИЕ НР → status_id=6 (Хранение)")
    else:
        print("ℹ️ Нет ВОЗМОЖНОЕ ПРОДЛЕНИЕ НР агрегатов со status_id=0")
    
    # --- Блок 4: Оставшиеся НЕИСПРАВНЫЕ → Ремонт (status_id=4, repair_days=0) ---
    # Логика: неисправные агрегаты без target_date отправляем в ремонт
    # После симуляции они станут исправными и попадут в резерв/на борт по потребности
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
        
        if not args.dry_run:
            # Устанавливаем status_id=4 (Ремонт) и repair_days=0 (ремонт только начался)
            remaining_update_sql = """
            ALTER TABLE heli_pandas
            UPDATE status_id = 4, repair_days = 0
            WHERE group_by > 2 
              AND status_id = 0 
              AND upperUTF8(replaceRegexpAll(ifNull(condition, ''), '^\\s+|\\s+$', '')) = 'НЕИСПРАВНЫЙ'
              AND version_date = %(version_date)s
              AND version_id = %(version_id)s
            """
            client.execute(remaining_update_sql, params)
            print(f"✅ Обновлено: {remaining_count} НЕИСПРАВНЫЙ → status_id=4 (Ремонт), repair_days=0")
    else:
        print("ℹ️ Нет оставшихся НЕИСПРАВНЫХ агрегатов со status_id=0")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

