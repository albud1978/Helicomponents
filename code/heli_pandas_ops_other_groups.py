#!/usr/bin/env python3
"""
Аналитика по агрегатам других групп (group_by > 2) на бортах со статусом operations.

- Ищет планеры (aircraft_number) с `status_id=2`
- Подсчитывает количество привязанных агрегатов с `group_by > 2` в любом статусе
- Сводит статистику по типам ВС и сохраняет Markdown-отчёт
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

sys.path.append(str(Path(__file__).resolve().parent / "utils"))
from config_loader import get_clickhouse_client  # type: ignore


@dataclass(frozen=True)
class VersionInfo:
    version_date: str
    version_id: int


@dataclass
class PlaneAggregation:
    aircraft_number: int
    ac_type_mask: Optional[int]
    mfg_date: Optional[str]
    total_components: int
    required_components: int
    delta: int
    shortage_groups: List[str]


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
        raise RuntimeError("Таблица heli_pandas пуста")
    latest_date, latest_id = rows[0]
    return VersionInfo(version_date=latest_date, version_id=int(latest_id))


def fetch_plane_meta(client, version: VersionInfo) -> Dict[int, Tuple[Optional[int], Optional[str]]]:
    rows = client.execute(
        """
        SELECT
            aircraft_number,
            any(ac_type_mask) AS ac_type_mask,
            formatDateTime(
                maxIf(mfg_date, mfg_date IS NOT NULL),
                '%%Y-%%m-%%d'
            ) AS mfg_date
        FROM heli_pandas
        WHERE status_id = 2
          AND aircraft_number != 0
          AND version_date = %(version_date)s
          AND version_id = %(version_id)s
        GROUP BY aircraft_number
        """,
        {"version_date": version.version_date, "version_id": version.version_id},
    )
    return {
        int(aircraft_number): (
            int(ac_type_mask) if ac_type_mask is not None else None,
            mfg_date,
        )
        for aircraft_number, ac_type_mask, mfg_date in rows
    }


def fetch_group_counts(client, version: VersionInfo) -> Dict[int, Dict[int, int]]:
    rows = client.execute(
        """
        SELECT
            aircraft_number,
            group_by,
            count() AS installed_count
        FROM heli_pandas
        WHERE aircraft_number != 0
          AND group_by > 2
          AND version_date = %(version_date)s
          AND version_id = %(version_id)s
        GROUP BY aircraft_number, group_by
        """,
        {"version_date": version.version_date, "version_id": version.version_id},
    )
    result: Dict[int, Dict[int, int]] = {}
    for aircraft_number, group_by, installed_count in rows:
        plane = int(aircraft_number)
        group = int(group_by)
        result.setdefault(plane, {})[group] = int(installed_count)
    return result


def fetch_group_requirements(client, version: VersionInfo) -> Dict[int, int]:
    rows = client.execute(
        """
        SELECT
            group_by,
            max(comp_number) AS required_count
        FROM md_components
        WHERE version_date = %(version_date)s
          AND version_id = %(version_id)s
          AND group_by > 2
        GROUP BY group_by
        """,
        {"version_date": version.version_date, "version_id": version.version_id},
    )
    return {int(group_by): int(required_count) for group_by, required_count in rows}


def fetch_requirement_details(client, version: VersionInfo) -> Dict[int, List[str]]:
    rows = client.execute(
        """
        SELECT
            group_by,
            partno,
            comp_number
        FROM md_components
        WHERE version_date = %(version_date)s
          AND version_id = %(version_id)s
          AND group_by > 2
        """,
        {"version_date": version.version_date, "version_id": version.version_id},
    )
    details: Dict[int, List[str]] = {}
    for group_by, partno, comp_number in rows:
        group = int(group_by)
        part_desc = f"{partno}:{int(comp_number) if comp_number is not None else ''}"
        details.setdefault(group, []).append(part_desc)
    return details


def fetch_aggregations(client, version: VersionInfo) -> List[PlaneAggregation]:
    plane_meta = fetch_plane_meta(client, version)
    counts = fetch_group_counts(client, version)
    requirements = fetch_group_requirements(client, version)
    requirement_details = fetch_requirement_details(client, version)
    exempt_groups = {33, 34, 35}

    aggregations: List[PlaneAggregation] = []
    for aircraft_number, (ac_type_mask, mfg_date) in plane_meta.items():
        group_counts = counts.get(aircraft_number, {})
        total_components = sum(group_counts.values())
        required_components = 0
        shortages: List[str] = []
        allowed_extra = 0
        for group, installed in group_counts.items():
            required = requirements.get(group, installed)
            required_components += required
            if installed < required:
                comp_desc = ";".join(requirement_details.get(group, []))
                entry = f"{group}:{installed}/{required}"
                if comp_desc:
                    entry += f"({comp_desc})"
                if group not in {33, 34, 35}:
                    entry = f"**{entry}**"
                shortages.append(entry)
            elif installed > required:
                comp_desc = ";".join(requirement_details.get(group, []))
                entry = f"{group}:{installed}/{required}"
                if comp_desc:
                    entry += f"({comp_desc})"
                if group not in {33, 34, 35}:
                    entry = f"**{entry}**"
                shortages.append(entry)
                if group in exempt_groups:
                    allowed_extra += installed - required
        delta = total_components - required_components - allowed_extra
        aggregations.append(
            PlaneAggregation(
                aircraft_number=aircraft_number,
                ac_type_mask=ac_type_mask,
                mfg_date=mfg_date,
                total_components=total_components,
                required_components=required_components,
                delta=delta,
                shortage_groups=shortages,
            )
        )
    return aggregations


def split_by_type(rows: Sequence[PlaneAggregation]) -> Tuple[List[PlaneAggregation], List[PlaneAggregation], List[PlaneAggregation]]:
    mi8: List[PlaneAggregation] = []
    mi17: List[PlaneAggregation] = []
    mixed: List[PlaneAggregation] = []
    for row in rows:
        mask = row.ac_type_mask or 0
        if (mask & 32) and not (mask & 64):
            mi8.append(row)
        elif (mask & 64) and not (mask & 32):
            mi17.append(row)
        else:
            mixed.append(row)
    sort_key = lambda r: r.mfg_date or ""
    mi8.sort(key=sort_key)
    mi17.sort(key=sort_key)
    mixed.sort(key=sort_key)
    return mi8, mi17, mixed


def build_markdown(version: VersionInfo, rows: Sequence[PlaneAggregation]) -> str:
    mi8, mi17, mixed = split_by_type(rows)
    sections = [
        "# Агрегаты других групп на планерах operations",
        "",
        f"- Версия данных: `{version.version_date} v{version.version_id}`",
        f"- Сгенерировано: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Планеров в статусе 2: **{len(rows)}**",
        f"- Суммарно агрегатов (group_by>2): **{sum(r.total_components for r in rows)}**",
        "",
    ]

    def append_section(title: str, items: Sequence[PlaneAggregation]) -> None:
        sections.append(f"## {title}")
        sections.append(f"Всего планеров: **{len(items)}**")
        sections.append("| aircraft_number | mfg_date | агрегаты | норма | Δ | отклонения по группам |")
        sections.append("| ---: | --- | ---: | ---: | ---: | --- |")
        for row in items:
            sections.append(
                f"| {row.aircraft_number} | {row.mfg_date or ''} | "
                f"{row.total_components} | {row.required_components} | {row.delta} | "
                f"{', '.join(row.shortage_groups) if row.shortage_groups else ''} |"
            )
        sections.append("")

    append_section("Mi-8T", mi8)
    append_section("Mi-17", mi17)
    if mixed:
        append_section("Mixed/Unknown", mixed)

    lines = sections
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Подсчёт агрегатов других групп на планерах со status_id=2"
    )
    parser.add_argument("--version-date", type=str, help="Дата версии (YYYY-MM-DD)")
    parser.add_argument("--version-id", type=int, help="ID версии")
    parser.add_argument(
        "--md-path",
        type=str,
        help="Путь к Markdown-отчёту (по умолчанию docs/heli_pandas_ops_other_groups_<version>.md)",
    )
    parser.add_argument("--skip-md", action="store_true", help="Не сохранять Markdown")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = get_clickhouse_client()
    version = resolve_version(client, args.version_date, args.version_id)
    rows = fetch_aggregations(client, version)

    print(
        f"\n✅ Планеров в status_id=2: {len(rows)}; агрегатов других групп: "
        f"{sum(r.total_components for r in rows)}"
    )
    mi8, mi17, mixed = split_by_type(rows)

    def print_group(title: str, group: Sequence[PlaneAggregation]) -> None:
        print(f"\n{title} ({len(group)} планеров)")
        print(
            f"{'aircraft':>8}  {'mfg_date':<10}  {'inst':>5}  {'norm':>5}  "
            f"{'Δ':>3}  {'shortages':<20}"
        )
        print("-" * 64)
        for row in group:
            print(
                f"{row.aircraft_number:>8}  {(row.mfg_date or ''):<10}  "
                f"{row.total_components:>5}  {row.required_components:>5}  "
                f"{row.delta:>3}  {', '.join(row.shortage_groups) if row.shortage_groups else ''}"
            )

    print_group("Mi-8T", mi8)
    print_group("Mi-17", mi17)
    if mixed:
        print_group("Mixed/Unknown", mixed)

    if not args.skip_md:
        md_path = (
            Path(args.md_path)
            if args.md_path
            else Path(f"docs/heli_pandas_ops_other_groups_{version.version_date}.md")
        )
        md_path.write_text(build_markdown(version, rows), encoding="utf-8")
        print(f"\n📝 Markdown-отчёт сохранён в {md_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

