#!/usr/bin/env python3
"""Валидатор per-номенклатурных диапазонов psn для симуляционных рождений.

`psn_spawn_start` больше не вычисляется этим шагом. Значение приходит из
`data_input/master_data/MD_Сomponents.xlsx`, а валидатор проверяет контракт:

    k = ранг partseqno_i по возрастанию (0-based, только non-null)
    psn_spawn_start(k) = PSN_SPAWN_BASE + k * PSN_BLOCK_STRIDE

Номенклатура без AMOS-ID (partseqno_i IS NULL) не спавнится и должна иметь
psn_spawn_start = 0. Реальные `psn` из `heli_pandas` проверяются с явным
учётом `group_by`, чтобы не смешивать группы неявно.
"""

import argparse
import csv
import sys
from pathlib import Path
from typing import Any

code_root = Path(__file__).resolve().parents[1]
sys.path.append(str(code_root))
from utils.config_loader import get_clickhouse_client

UINT32_MAX = 0xFFFFFFFF
PSN_SPAWN_BASE = 10_000_000
PSN_BLOCK_STRIDE = 1_000_000
MIN_STRIDE_SAFETY = 50
REPORT_PATH = Path("output/psn_reservation/md_components_psn_reserve_validation_report.csv")


def _require_table(client, table: str) -> None:
    exists = client.execute(
        """
        SELECT count()
        FROM system.tables
        WHERE database = currentDatabase() AND name = %(table)s
        """,
        {"table": table},
    )[0][0]
    if not exists:
        raise RuntimeError(f"Отсутствует обязательная таблица {table}")


def _require_columns(client, table: str, required: set[str]) -> None:
    rows = client.execute(
        """
        SELECT name
        FROM system.columns
        WHERE database = currentDatabase() AND table = %(table)s
        """,
        {"table": table},
    )
    columns = {row[0] for row in rows}
    missing = sorted(required - columns)
    if missing:
        raise RuntimeError(f"В таблице {table} отсутствуют обязательные колонки: {missing}")


def _clean_partno(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("\n", "")


def _load_md_rows(client) -> list[dict[str, Any]]:
    rows = client.execute(
        """
        SELECT partno, partseqno_i, psn_spawn_start
        FROM md_components
        WHERE partno IS NOT NULL AND partno != ''
        ORDER BY partseqno_i, partno
        """
    )
    result = [
        {
            "partno": _clean_partno(partno),
            "partseqno_i": None if partseqno_i is None else int(partseqno_i),
            "psn_spawn_start": int(psn_spawn_start),
        }
        for partno, partseqno_i, psn_spawn_start in rows
    ]
    if not result:
        raise RuntimeError("md_components пустая или не содержит partno")
    duplicate_partnos = sorted({row["partno"] for row in result if [r["partno"] for r in result].count(row["partno"]) > 1})
    if duplicate_partnos:
        raise RuntimeError(f"Дубли очищенного partno в md_components: {duplicate_partnos}")
    spawn_partseqnos = [row["partseqno_i"] for row in result if row["partseqno_i"] is not None]
    duplicate_partseqnos = sorted({ps for ps in spawn_partseqnos if spawn_partseqnos.count(ps) > 1})
    if duplicate_partseqnos:
        raise RuntimeError(f"Дубли partseqno_i в md_components: {duplicate_partseqnos}")
    return result


def _load_real_psn_stats(client) -> dict[int, dict[str, Any]]:
    rows = client.execute(
        """
        SELECT
            partseqno_i,
            group_by,
            max(psn) AS max_psn,
            countDistinct(psn) AS distinct_psn
        FROM heli_pandas
        WHERE partseqno_i IS NOT NULL
          AND psn IS NOT NULL
          AND group_by IS NOT NULL
        GROUP BY partseqno_i, group_by
        ORDER BY partseqno_i, group_by
        """
    )
    stats: dict[int, dict[str, Any]] = {}
    for partseqno_i, group_by, max_psn, distinct_psn in rows:
        ps = int(partseqno_i)
        entry = stats.setdefault(ps, {"max_psn": 0, "groups": [], "distinct_psn": 0})
        entry["max_psn"] = max(entry["max_psn"], int(max_psn or 0))
        entry["groups"].append(int(group_by))
        entry["distinct_psn"] += int(distinct_psn or 0)
    return stats


def reserve_psn_ranges(
    client, base: int = PSN_SPAWN_BASE, stride: int = PSN_BLOCK_STRIDE
) -> dict[int, int]:
    """Совместимый интерфейс: валидирует и возвращает map partseqno_i -> psn_spawn_start."""
    _require_table(client, "md_components")
    _require_table(client, "heli_pandas")
    _require_columns(client, "md_components", {"partno", "partseqno_i", "psn_spawn_start"})
    _require_columns(client, "heli_pandas", {"partseqno_i", "psn", "group_by"})

    md_rows = _load_md_rows(client)
    real_stats = _load_real_psn_stats(client)

    spawn_rows = [row for row in md_rows if row["partseqno_i"] is not None]
    non_spawn_rows = [row for row in md_rows if row["partseqno_i"] is None]
    ranked_partseqnos = sorted(row["partseqno_i"] for row in spawn_rows)
    expected_mapping = {ps: base + k * stride for k, ps in enumerate(ranked_partseqnos)}

    last_block_end = base + len(ranked_partseqnos) * stride
    if last_block_end > UINT32_MAX:
        raise ValueError(f"последний блок {last_block_end} выходит за UInt32 ({len(ranked_partseqnos)} номенклатур)")

    max_fleet = max((entry["distinct_psn"] for entry in real_stats.values()), default=0)
    if stride < max_fleet * MIN_STRIDE_SAFETY:
        raise ValueError(
            f"PSN_BLOCK_STRIDE={stride} мал: max парк номенклатуры={max_fleet}, нужен запас x{MIN_STRIDE_SAFETY}"
        )

    starts = [row["psn_spawn_start"] for row in spawn_rows]
    zero_rows = [row for row in md_rows if row["psn_spawn_start"] == 0]
    violations: list[str] = []
    report_rows: list[dict[str, Any]] = []

    if len(set(starts)) != len(starts):
        violations.append("psn_spawn_start не уникальны среди spawnable номенклатур")

    for row in md_rows:
        partno = row["partno"]
        partseqno_i = row["partseqno_i"]
        actual_start = row["psn_spawn_start"]
        groups = []
        real_max = 0
        expected_start = 0

        if partseqno_i is None:
            status = "non_spawnable_zero" if actual_start == 0 else "non_spawnable_nonzero"
            if actual_start != 0:
                violations.append(f"{partno}: partseqno_i=NULL, psn_spawn_start должен быть 0, факт={actual_start}")
        else:
            expected_start = expected_mapping[partseqno_i]
            stats = real_stats.get(partseqno_i, {"max_psn": 0, "groups": [], "distinct_psn": 0})
            real_max = int(stats["max_psn"])
            groups = stats["groups"]
            status = "matched"
            if actual_start <= 0:
                status = "non_positive"
                violations.append(f"{partno}: spawnable partseqno_i={partseqno_i}, psn_spawn_start должен быть >0")
            if actual_start != expected_start:
                status = "formula_mismatch"
                violations.append(
                    f"{partno}: psn_spawn_start={actual_start}, ожидалось {expected_start} "
                    f"для ранга partseqno_i={partseqno_i}"
                )
            if actual_start > UINT32_MAX or actual_start + stride > UINT32_MAX:
                status = "uint32_overflow"
                violations.append(f"{partno}: блок psn_spawn_start={actual_start} выходит за UInt32")
            if actual_start <= real_max:
                status = "real_psn_overlap"
                violations.append(
                    f"{partno}: psn_spawn_start={actual_start} не выше real max(psn)={real_max}; group_by={groups}"
                )

        report_rows.append(
            {
                "partno": partno,
                "partseqno_i": partseqno_i,
                "psn_spawn_start": actual_start,
                "expected_psn_spawn_start": expected_start,
                "real_max_psn": real_max,
                "heli_pandas_group_by": ",".join(str(g) for g in groups),
                "status": status,
            }
        )

    if non_spawn_rows and len(zero_rows) < len(non_spawn_rows):
        violations.append("Не все non-spawnable номенклатуры имеют psn_spawn_start=0")

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with REPORT_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "partno",
                "partseqno_i",
                "psn_spawn_start",
                "expected_psn_spawn_start",
                "real_max_psn",
                "heli_pandas_group_by",
                "status",
            ],
        )
        writer.writeheader()
        writer.writerows(report_rows)

    if violations:
        preview = "\n".join(violations[:20])
        raise RuntimeError(f"psn_spawn_start validation failed ({len(violations)} violations):\n{preview}")

    max_start = max(starts) if starts else 0
    print(
        f"✅ {len(spawn_rows)} spawnable номенклатур: psn_spawn_start от {base:,} до {max_start:,}; "
        f"non-spawnable={len(non_spawn_rows)}; report={REPORT_PATH}"
    )
    return expected_mapping


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate md_components.psn_spawn_start from Excel SSoT")
    parser.add_argument("--version-date", type=str, help="Совместимость с extract_master; не используется")
    parser.add_argument("--version-id", type=int, help="Совместимость с extract_master; не используется")
    parser.parse_args()

    print("🚀 === ВАЛИДАТОР PER-НОМЕНКЛАТУРНОГО РЕЗЕРВА PSN ===")
    client = get_clickhouse_client()
    reserve_psn_ranges(client)
    print("🎯 psn_spawn_start: Excel SSoT соответствует контракту резервирования")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
