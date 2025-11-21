#!/usr/bin/env python3
"""
Инвентаризация агрегатов на бортах в состоянии operations (status_id=2).

- Использует версионность `heli_pandas`/`md_components`
- Сводит количество установленных агрегатов по каждому борту
- Дополнительно выводит расхождения с требуемым `comp_number`
- По умолчанию сохраняет отчёт в docs/heli_pandas_ops_inventory_<version>.md
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import sys

sys.path.append(str(Path(__file__).resolve().parent / "utils"))
from config_loader import get_clickhouse_client  # type: ignore


@dataclass(frozen=True)
class VersionInfo:
    version_date: str
    version_id: int


@dataclass
class InventoryRow:
    aircraft_number: int
    ac_type_mask: Optional[int]
    partseqno_i: Optional[int]
    partno: Optional[str]
    installed_count: int
    required_count: Optional[int]
    group_by: Optional[int]


def decode_ac_type(ac_type_mask: Optional[int]) -> str:
    if ac_type_mask is None:
        return "unknown"
    names: List[str] = []
    if ac_type_mask & 32:
        names.append("Mi-8T")
    if ac_type_mask & 64:
        names.append("Mi-17")
    return "/".join(names) if names else "unknown"


def resolve_version(
    client,
    version_date: Optional[str],
    version_id: Optional[int],
) -> VersionInfo:
    if version_date and version_id is not None:
        return VersionInfo(version_date=version_date, version_id=version_id)

    rows: Sequence[Tuple[str, int]] = client.execute(
        """
        SELECT toString(version_date) AS version_date,
               toUInt8(version_id) AS version_id
        FROM heli_pandas
        ORDER BY version_date DESC, version_id DESC
        LIMIT 1
        """
    )
    if not rows:
        raise RuntimeError("Таблица heli_pandas пуста, нет данных для анализа")
    latest_date, latest_id = rows[0]
    return VersionInfo(version_date=latest_date, version_id=int(latest_id))


def fetch_inventory_rows(client, version: VersionInfo) -> List[InventoryRow]:
    query = """
        WITH requirements AS (
            SELECT
                partno_comp AS partseqno_i,
                max(comp_number) AS required_count,
                any(group_by) AS group_by
            FROM md_components
            WHERE version_date = %(version_date)s
              AND version_id = %(version_id)s
            GROUP BY partno_comp
        )
        SELECT
            hp.aircraft_number,
            any(hp.ac_type_mask) AS ac_type_mask,
            hp.partseqno_i,
            any(hp.partno) AS partno,
            count() AS installed_count,
            req.required_count,
            req.group_by
        FROM heli_pandas hp
        LEFT JOIN requirements req
            ON req.partseqno_i = hp.partseqno_i
        WHERE hp.status_id = 2
          AND hp.aircraft_number != 0
          AND hp.version_date = %(version_date)s
          AND hp.version_id = %(version_id)s
        GROUP BY
            hp.aircraft_number,
            hp.partseqno_i,
            req.required_count,
            req.group_by
        ORDER BY hp.aircraft_number ASC, installed_count DESC
    """
    rows = client.execute(
        query,
        {"version_date": version.version_date, "version_id": version.version_id},
    )
    result: List[InventoryRow] = []
    for (
        aircraft_number,
        ac_type_mask,
        partseqno_i,
        partno,
        installed_count,
        required_count,
        group_by,
    ) in rows:
        result.append(
            InventoryRow(
                aircraft_number=int(aircraft_number),
                ac_type_mask=int(ac_type_mask) if ac_type_mask is not None else None,
                partseqno_i=int(partseqno_i) if partseqno_i is not None else None,
                partno=partno,
                installed_count=int(installed_count),
                required_count=int(required_count)
                if required_count is not None
                else None,
                group_by=int(group_by) if group_by is not None else None,
            )
        )
    return result


def build_summary(rows: Sequence[InventoryRow]) -> List[Dict[str, object]]:
    summary: Dict[int, Dict[str, object]] = {}
    for row in rows:
        data = summary.setdefault(
            row.aircraft_number,
            {
                "aircraft_number": row.aircraft_number,
                "ac_type_mask": row.ac_type_mask,
                "components_total": 0,
                "unique_parts": 0,
                "required_total": 0,
            },
        )
        if data["ac_type_mask"] is None and row.ac_type_mask is not None:
            data["ac_type_mask"] = row.ac_type_mask
        data["components_total"] = int(data["components_total"]) + row.installed_count
        if row.partseqno_i is not None:
            data["unique_parts"] = int(data["unique_parts"]) + 1
        if row.required_count is not None:
            data["required_total"] = int(data["required_total"]) + row.required_count
    ordered = sorted(summary.values(), key=lambda x: x["aircraft_number"])
    for item in ordered:
        item["ac_type"] = decode_ac_type(item["ac_type_mask"])
    return ordered


def format_summary_table(summary: Sequence[Dict[str, object]]) -> str:
    header = f"\n📊 Планеры в статусе operations (status_id=2) — {len(summary)} шт."
    lines = [header, "-" * len(header)]
    lines.append(f"{'aircraft':>8}  {'type':<10}  {'components':>10}  {'unique_parts':>12}")
    lines.append("-" * 52)
    for item in summary:
        lines.append(
            f"{item['aircraft_number']:>8}  {item['ac_type']:<10}  "
            f"{item['components_total']:>10}  {item['unique_parts']:>12}"
        )
    lines.append("-" * 52)
    return "\n".join(lines)


def format_details_table(
    rows: Sequence[InventoryRow],
    limit: Optional[int] = None,
) -> str:
    show_rows = rows if limit is None else rows[:limit]
    lines = ["\n🔍 Детализация по агрегатам (первые {} строк)".format(len(show_rows))]
    lines.append(
        f"{'aircraft':>8}  {'type':<10}  {'partseqno':>9}  {'partno':<20}  "
        f"{'installed':>9}  {'required':>9}  {'delta':>6}"
    )
    lines.append("-" * 80)
    for row in show_rows:
        ac_type = decode_ac_type(row.ac_type_mask)
        required = row.required_count if row.required_count is not None else "-"
        delta = (
            row.installed_count - row.required_count
            if row.required_count is not None
            else "-"
        )
        lines.append(
            f"{row.aircraft_number:>8}  {ac_type:<10}  "
            f"{(row.partseqno_i or 0):>9}  { (row.partno or '-')[:20]:<20}  "
            f"{row.installed_count:>9}  {required:>9}  {delta:>6}"
        )
    lines.append("-" * 80)
    if limit is not None and len(rows) > limit:
        lines.append(f"... ещё {len(rows) - limit} строк скрыто ...")
    return "\n".join(lines)


def build_markdown(
    version: VersionInfo,
    summary: Sequence[Dict[str, object]],
    rows: Sequence[InventoryRow],
    detail_limit: Optional[int],
) -> str:
    total_components = sum(int(item["components_total"]) for item in summary)
    lines = [
        "# Инвентаризация агрегатов (status_id=2)",
        "",
        f"- Версия данных: `{version.version_date} v{version.version_id}`",
        f"- Сгенерировано: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Планеров в эксплуатации: **{len(summary)}**",
        f"- Всего агрегатов на бортах: **{total_components}**",
        "",
        "## Сводка по планерам",
        "| aircraft_number | type | components | unique_parts | required_total | delta |",
        "| ---: | --- | ---: | ---: | ---: | ---: |",
    ]
    for item in summary:
        required_total = item["required_total"]
        delta = (
            int(item["components_total"]) - int(required_total)
            if required_total
            else "-"
        )
        lines.append(
            f"| {item['aircraft_number']} | {item['ac_type']} | "
            f"{item['components_total']} | {item['unique_parts']} | "
            f"{required_total} | {delta} |"
        )
    lines.append("")
    lines.append("## Детализация по агрегатам")
    lines.append(
        "| aircraft_number | type | partseqno_i | partno | installed | required | delta |"
    )
    lines.append("| ---: | --- | ---: | --- | ---: | ---: | ---: |")
    detail_rows = rows if detail_limit is None else rows[:detail_limit]
    for row in detail_rows:
        ac_type = decode_ac_type(row.ac_type_mask)
        required = row.required_count if row.required_count is not None else ""
        delta = (
            row.installed_count - row.required_count
            if row.required_count is not None
            else ""
        )
        lines.append(
            f"| {row.aircraft_number} | {ac_type} | {row.partseqno_i or ''} | "
            f"{row.partno or ''} | {row.installed_count} | {required} | {delta} |"
        )
    if detail_limit is not None and len(rows) > detail_limit:
        lines.append(
            f"| … | … | … | … | … | … | ещё {len(rows) - detail_limit} строк … |"
        )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Инвентаризация агрегатов на планерах (status_id=2)"
    )
    parser.add_argument("--version-date", type=str, help="Дата версии (YYYY-MM-DD)")
    parser.add_argument("--version-id", type=int, help="ID версии данных")
    parser.add_argument(
        "--detail-limit",
        type=int,
        default=200,
        help="Количество строк детализации для вывода/Markdown (по умолчанию 200)",
    )
    parser.add_argument(
        "--md-path",
        type=str,
        help="Путь к Markdown-отчёту. По умолчанию docs/heli_pandas_ops_inventory_<version>.md",
    )
    parser.add_argument(
        "--skip-md",
        action="store_true",
        help="Не сохранять Markdown-отчёт",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = get_clickhouse_client()
    version = resolve_version(client, args.version_date, args.version_id)
    rows = fetch_inventory_rows(client, version)
    summary = build_summary(rows)

    print(
        f"\n✅ Инвентаризация для версии {version.version_date} v{version.version_id}: "
        f"{len(summary)} планеров, {len(rows)} строк детализации."
    )
    print(format_summary_table(summary))
    print(format_details_table(rows, args.detail_limit))

    if not args.skip_md:
        md_content = build_markdown(version, summary, rows, args.detail_limit)
        default_path = Path(
            f"docs/heli_pandas_ops_inventory_{version.version_date}.md"
        )
        target_path = Path(args.md_path) if args.md_path else default_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(md_content, encoding="utf-8")
        print(f"\n📝 Markdown-отчёт сохранён в {target_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

