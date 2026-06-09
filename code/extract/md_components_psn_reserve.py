#!/usr/bin/env python3
"""Резервирование per-номенклатурных диапазонов psn для симуляционных рождений.

Каждая номенклатура (partseqno_i) в md_components получает индивидуальный блок
значений psn, не пересекающийся с блоками других номенклатур и лежащий выше
реального максимума psn из AMOS. Это позволяет по значению psn сразу понять:
(а) что экземпляр симуляционный, (б) какой он номенклатуры, (в) каким по счёту
родился — аналог планерного резервирования acn >= 100000, но с разбивкой.

Карта нарезки (детерминированная, читаемая):
    k = ранг partseqno_i по возрастанию (0-based, только non-null)
    psn_spawn_start(k) = PSN_SPAWN_BASE + k * PSN_BLOCK_STRIDE
    обратно:  block   = (psn - PSN_SPAWN_BASE) // PSN_BLOCK_STRIDE
              counter = (psn - PSN_SPAWN_BASE) %  PSN_BLOCK_STRIDE
    block -> partseqno_i: по отсортированному списку (см. output/psn_reservation/step1_report.md)

Номенклатура без AMOS-ID (partseqno_i IS NULL) не спавнится и получает
psn_spawn_start = 0 (sentinel). Контракт выдачи (реализует GPU-счётчик L2/L3):
    psn = psn_spawn_start(partseqno_i) + counter(partseqno_i),  counter per-номенклатура с 0.

Место в ETL Pipeline:
- ПОСЛЕ: md_components_enricher.py (partseqno_i уже заполнен)
- heli_pandas должен быть загружен (нужны фактический max(psn) и масштаб парка для fail-fast).
"""

import sys
from pathlib import Path

code_root = Path(__file__).resolve().parents[1]
sys.path.append(str(code_root))
from utils.config_loader import get_clickhouse_client

UINT32_MAX = 0xFFFFFFFF
# Старт первого блока. Реальный max(psn) в AMOS ~1.81M; 10M даёт зазор ×5.5.
PSN_SPAWN_BASE = 10_000_000
# Ширина блока на номенклатуру. Максимум реального парка ~1100 экз/номенклатуру,
# 1M даёт запас ~900x — переполнение блока исключено; страйд круглый и читаемый.
PSN_BLOCK_STRIDE = 1_000_000
# Минимальный множитель запаса блока над фактическим максимумом парка номенклатуры.
MIN_STRIDE_SAFETY = 50


def _column_exists(client, table: str, column: str) -> bool:
    return bool(
        client.execute(
            """
            SELECT count()
            FROM system.columns
            WHERE database = currentDatabase() AND table = %(t)s AND name = %(c)s
            """,
            {"t": table, "c": column},
        )[0][0]
    )


def reserve_psn_ranges(
    client, base: int = PSN_SPAWN_BASE, stride: int = PSN_BLOCK_STRIDE
) -> dict[int, int]:
    """Назначает per-номенклатурные psn_spawn_start. Возвращает map partseqno_i -> start."""
    real_max = client.execute(
        "SELECT max(psn) FROM heli_pandas WHERE psn IS NOT NULL"
    )[0][0] or 0
    if base <= real_max:
        raise ValueError(
            f"PSN_SPAWN_BASE={base} не выше реального max(psn)={real_max}: зазор отсутствует"
        )

    max_fleet = client.execute(
        """
        SELECT max(cnt) FROM (
            SELECT countDistinct(psn) AS cnt
            FROM heli_pandas
            WHERE psn IS NOT NULL AND partseqno_i IS NOT NULL
            GROUP BY partseqno_i
        )
        """
    )[0][0] or 0
    if stride < max_fleet * MIN_STRIDE_SAFETY:
        raise ValueError(
            f"PSN_BLOCK_STRIDE={stride} мал: max парк номенклатуры={max_fleet}, "
            f"нужен запас x{MIN_STRIDE_SAFETY}"
        )

    partseqnos = [
        int(r[0])
        for r in client.execute(
            """
            SELECT DISTINCT partseqno_i
            FROM md_components
            WHERE partseqno_i IS NOT NULL
            ORDER BY partseqno_i
            """
        )
    ]
    mapping = {ps: base + k * stride for k, ps in enumerate(partseqnos)}

    last_block_end = base + len(partseqnos) * stride
    if last_block_end > UINT32_MAX:
        raise ValueError(
            f"последний блок {last_block_end} выходит за UInt32 ({len(partseqnos)} номенклатур)"
        )

    if not _column_exists(client, "md_components", "psn_spawn_start"):
        client.execute(
            "ALTER TABLE md_components ADD COLUMN psn_spawn_start UInt32 DEFAULT 0"
        )
        print("✅ Колонка psn_spawn_start (UInt32) добавлена в md_components")
    else:
        print("ℹ️ Колонка psn_spawn_start уже существует")

    cases = " ".join(f"WHEN partseqno_i = {ps} THEN {start}" for ps, start in mapping.items())
    client.execute(
        f"ALTER TABLE md_components UPDATE psn_spawn_start = CASE {cases} ELSE 0 END WHERE 1 = 1",
        settings={"mutations_sync": 1},
    )

    total_rows, spawnable, zero_rows, distinct_start, max_start = client.execute(
        """
        SELECT count(), countIf(psn_spawn_start > 0), countIf(psn_spawn_start = 0),
               countDistinct(psn_spawn_start), max(psn_spawn_start)
        FROM md_components
        """
    )[0]
    if spawnable != len(partseqnos):
        raise ValueError(f"заполнено блоков {spawnable}, ожидалось {len(partseqnos)}")
    # distinct = len(блоков) + 1 sentinel(0), если есть хотя бы одна null-номенклатура
    expected_distinct = len(partseqnos) + (1 if zero_rows > 0 else 0)
    if distinct_start != expected_distinct:
        raise ValueError(
            f"psn_spawn_start не уникальны: distinct={distinct_start}, ожидалось {expected_distinct}"
        )
    if zero_rows:
        print(f"ℹ️ {zero_rows} номенклатур без AMOS-ID (partseqno_i NULL): psn_spawn_start=0 (не спавнится)")

    print(
        f"✅ {len(partseqnos)} номенклатур: psn_spawn_start от {base:,} до {max_start:,} "
        f"(страйд {stride:,}, реальный max(psn)={real_max:,}, зазор={base - real_max:,})"
    )
    return mapping


def main() -> int:
    print("🚀 === PER-НОМЕНКЛАТУРНОЕ РЕЗЕРВИРОВАНИЕ PSN ===")
    client = get_clickhouse_client()
    reserve_psn_ranges(client)
    print("🎯 Готово: md_components.psn_spawn_start (per-номенклатурные блоки) зафиксирован")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
