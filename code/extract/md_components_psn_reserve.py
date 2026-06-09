#!/usr/bin/env python3
"""Резервирование диапазона psn для симуляционных рождений агрегатов.

Заполняет в справочнике md_components колонку psn_spawn_start — стартовое
значение psn, с которого симуляция выдаёт идентификаторы новорождённым
экземплярам агрегатов (L2/L3). Аналог планерного резервирования acn >= 100000.

Стратегия (глобальный диапазон):
- Реальные psn из AMOS — общий пул значений (psn почти уникален по экземпляру,
  диапазоны номенклатур взаимно перекрываются), поэтому единый глобальный старт
  естественнее по-номенклатурных бэндов. Номенклатура экземпляра всегда известна
  из его partseqno_i, кодировать её в psn не требуется.
- psn >= PSN_SPAWN_BASE => симуляционно-рождённый экземпляр (sim/real дискриминатор).

Место в ETL Pipeline:
- ПОСЛЕ: md_components_enricher.py (справочник уже с partseqno_i)
- heli_pandas должен быть загружен (нужен фактический max(psn) для fail-fast проверки зазора)
"""

import sys
from pathlib import Path

code_root = Path(__file__).resolve().parents[1]
sys.path.append(str(code_root))
from utils.config_loader import get_clickhouse_client

# Глобальный старт резервирования. Реальный max(psn) в AMOS ~1.81M;
# 10M даёт запас ×5.5 и чистую границу, UInt32 (max 4.29 млрд) с избытком.
PSN_SPAWN_BASE = 10_000_000


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


def reserve_psn_ranges(client, base: int = PSN_SPAWN_BASE) -> int:
    """Добавляет и заполняет md_components.psn_spawn_start. Возвращает base."""
    real_max = client.execute(
        "SELECT max(psn) FROM heli_pandas WHERE psn IS NOT NULL"
    )[0][0] or 0
    if base <= real_max:
        raise ValueError(
            f"PSN_SPAWN_BASE={base} не выше реального max(psn)={real_max}: "
            "зазор отсутствует, диапазоны пересекутся"
        )

    if not _column_exists(client, "md_components", "psn_spawn_start"):
        client.execute(
            "ALTER TABLE md_components ADD COLUMN psn_spawn_start UInt32 DEFAULT 0"
        )
        print("✅ Колонка psn_spawn_start (UInt32) добавлена в md_components")
    else:
        print("ℹ️ Колонка psn_spawn_start уже существует")

    client.execute(
        "ALTER TABLE md_components UPDATE psn_spawn_start = %(b)s WHERE 1 = 1",
        {"b": base},
        settings={"mutations_sync": 1},
    )

    rows, distinct_start, min_start = client.execute(
        "SELECT count(), countDistinct(psn_spawn_start), min(psn_spawn_start) FROM md_components"
    )[0]
    if distinct_start != 1 or min_start != base:
        raise ValueError(
            f"psn_spawn_start заполнен некорректно: distinct={distinct_start}, min={min_start}, ожидалось {base}"
        )

    print(
        f"✅ psn_spawn_start = {base:,} для всех {rows} номенклатур "
        f"(реальный max(psn) = {real_max:,}, зазор = {base - real_max:,})"
    )
    return base


def main() -> int:
    print("🚀 === РЕЗЕРВИРОВАНИЕ PSN ДЛЯ СИМУЛЯЦИОННЫХ РОЖДЕНИЙ ===")
    client = get_clickhouse_client()
    reserve_psn_ranges(client)
    print("🎯 Готово: md_components.psn_spawn_start зафиксирован")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
