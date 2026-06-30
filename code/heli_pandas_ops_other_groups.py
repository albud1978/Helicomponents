#!/usr/bin/env python3
"""
Аналитика по агрегатам других групп (group_by > 2) на бортах со статусом operations.

- Ищет планеры (aircraft_number) с `status_id=2`
- Подсчитывает количество привязанных агрегатов с `group_by > 2` в любом статусе
- Сводит статистику по типам ВС и сохраняет Markdown-отчёт
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

sys.path.append(str(Path(__file__).resolve().parent / "utils"))
from config_loader import get_clickhouse_client  # type: ignore
from static_data_resolver import (  # type: ignore
    latest_source_dataset_label,
    resolve_latest_md_slice,
)

OPTIONAL_GROUPS = {32, 33, 34}
ALLOW_EXTRA_GROUPS = {32, 33, 34, 35}
MI17_OR_GROUPS = {14, 15, 16}
MI17_ALT_GROUPS = {22, 23, 24}
MI17_AGB_GROUPS = {35, 36, 37}
MI17_ENGINE_GROUPS = {28, 29, 30}
MI17_OR_REQUIRED = 1
MI8_CORE_TARGET = 33


def _mask_applies(group_mask: int, plane_mask: Optional[int]) -> bool:
    if group_mask == 0 or plane_mask is None:
        return True
    return bool(group_mask & plane_mask)


def _extract_variant(partno: Optional[str]) -> Optional[str]:
    if not partno:
        return None
    normalized = partno.strip().upper()
    prefix = "МИ-8"
    if normalized.startswith(prefix):
        suffix = normalized[len(prefix) :]
        suffix = suffix.strip("- ")
        return suffix or None
    return None


@dataclass(frozen=True)
class VersionInfo:
    version_date: str
    version_id: int


@dataclass
class PlaneAggregation:
    aircraft_number: int
    ac_type_mask: Optional[int]
    mfg_date: Optional[str]
    partno: Optional[str]
    variant: Optional[str]
    total_components: int
    required_components: int
    optional_components: int
    delta: int
    shortage_groups: List[str]
    core_components: int
    core_missing_groups: List[str]


def _escape_md_cell(value: Optional[str]) -> str:
    """Экранирует спецсимволы для корректного отображения в markdown-таблицах."""
    if not value:
        return ""
    return value.replace("|", "\\|")


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


def fetch_plane_meta(client, version: VersionInfo) -> Dict[int, Tuple[Optional[int], Optional[str], Optional[str]]]:
    rows = client.execute(
        """
        SELECT
            aircraft_number,
            any(ac_type_mask) AS ac_type_mask,
            any(partno) AS partno,
            formatDateTime(
                minIf(mfg_date, mfg_date IS NOT NULL),
                '%%Y-%%m-%%d'
            ) AS mfg_date
        FROM heli_pandas
        WHERE status_id = 2
          AND aircraft_number != 0
          AND group_by IN (1, 2)
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
            partno,
        )
        for aircraft_number, ac_type_mask, partno, mfg_date in rows
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


def fetch_group_requirements(client, md_version: VersionInfo) -> Dict[int, Dict[str, int]]:
    rows = client.execute(
        """
        SELECT
            group_by,
            max(comp_number) AS required_count,
            groupBitOr(toUInt16(coalesce(ac_type_mask, 0))) AS ac_type_mask
        FROM md_components
        WHERE version_date = %(version_date)s
          AND version_id = %(version_id)s
          AND group_by > 2
        GROUP BY group_by
        """,
        {"version_date": md_version.version_date, "version_id": md_version.version_id},
    )
    return {
        int(group_by): {
            "required": int(required_count) if required_count is not None else 0,
            "mask": int(ac_type_mask) if ac_type_mask is not None else 0,
        }
        for group_by, required_count, ac_type_mask in rows
    }


def fetch_requirement_details(client, md_version: VersionInfo) -> Dict[int, List[str]]:
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
        {"version_date": md_version.version_date, "version_id": md_version.version_id},
    )
    details: Dict[int, List[str]] = {}
    for group_by, partno, comp_number in rows:
        group = int(group_by)
        part_desc = f"{partno}:{int(comp_number) if comp_number is not None else ''}"
        details.setdefault(group, []).append(part_desc)
    return details


def fetch_aggregations(
    client,
    version: VersionInfo,
    md_version: VersionInfo,
) -> List[PlaneAggregation]:
    plane_meta = fetch_plane_meta(client, version)
    counts = fetch_group_counts(client, version)
    requirements = fetch_group_requirements(client, md_version)
    requirement_details = fetch_requirement_details(client, md_version)

    aggregations: List[PlaneAggregation] = []
    for aircraft_number, (ac_type_mask, mfg_date, partno) in plane_meta.items():
        group_counts = counts.get(aircraft_number, {})
        total_components = sum(group_counts.values())
        required_components = 0
        shortages: List[str] = []
        allowed_extra = 0
        core_components = 0
        core_missing: List[str] = []
        relevant_groups = set(group_counts.keys())
        variant = _extract_variant(partno)
        is_mi17 = bool(ac_type_mask and (ac_type_mask & 64))
        mi17_or_present = False
        mi17_or_installed = 0
        mi17_or_core = 0
        mi17_or_details: List[Tuple[int, int, int, str]] = []
        mi17_alt_present = False
        mi17_alt_counts: Dict[int, int] = {}
        mi17_alt_details: Dict[int, Tuple[int, int, str]] = {}
        mi17_agb_counts: Dict[int, int] = {}
        mi17_agb_details: Dict[int, Tuple[int, int, str]] = {}
        mi17_engine_counts: Dict[int, int] = {}
        mi17_engine_details: Dict[int, Tuple[int, int, str]] = {}
        for group, info in requirements.items():
            if _mask_applies(info["mask"], ac_type_mask):
                relevant_groups.add(group)

        for group in sorted(relevant_groups):
            installed = group_counts.get(group, 0)
            req_info = requirements.get(group)
            required = req_info["required"] if req_info else installed
            comp_desc = ";".join(requirement_details.get(group, []))
            entry_plain = f"{group}:{installed}/{required}"
            if comp_desc:
                entry_plain += f"({comp_desc})"

            if is_mi17 and group in MI17_OR_GROUPS:
                mi17_or_present = True
                mi17_or_installed += installed
                mi17_or_core += installed
                mi17_or_details.append((group, installed, required, comp_desc))
                continue

            if is_mi17 and group in MI17_ALT_GROUPS:
                mi17_alt_present = True
                mi17_alt_counts[group] = installed
                mi17_alt_details[group] = (installed, required, comp_desc)
                continue

            if is_mi17 and group in MI17_AGB_GROUPS:
                mi17_agb_counts[group] = installed
                mi17_agb_details[group] = (installed, required, comp_desc)
                continue

            if is_mi17 and group in MI17_ENGINE_GROUPS:
                mi17_engine_counts[group] = installed
                mi17_engine_details[group] = (installed, required, comp_desc)
                continue

            if group in OPTIONAL_GROUPS:
                # Полностью опциональные группы: увеличиваем счётчик опционала и
                # компенсируем дельту через allowed_extra.
                allowed_extra += installed
                continue

            required_components += required
            core_components += installed

            if installed < required:
                entry = f"**{entry_plain}**"
                shortages.append(entry)
                core_missing.append(entry_plain)
            elif installed > required:
                if group in ALLOW_EXTRA_GROUPS:
                    allowed_extra += installed - required
                else:
                    entry = f"**{entry_plain}**"
                    shortages.append(entry)

        def _req_for(group: int) -> int:
            req_info = requirements.get(group)
            return req_info["required"] if req_info else 0

        if mi17_or_present:
            required_components += MI17_OR_REQUIRED
            core_components += mi17_or_core
            if mi17_or_installed == 0:
                detail_parts = []
                for group, installed, required, comp_desc in mi17_or_details:
                    detail_entry = f"{group}:{installed}/{required}"
                    if comp_desc:
                        detail_entry += f"({comp_desc})"
                    detail_parts.append(detail_entry)
                details = "; ".join(detail_parts)
                entry_plain = f"12|13|14:0/{MI17_OR_REQUIRED}"
                if details:
                    entry_plain += f"({details})"
                entry = f"**{entry_plain}**"
                shortages.append(entry)
                core_missing.append(entry_plain)
            elif mi17_or_installed > MI17_OR_REQUIRED:
                allowed_extra += mi17_or_installed - MI17_OR_REQUIRED

        if is_mi17 and mi17_alt_present:
            req22 = _req_for(22)
            req23 = _req_for(23)
            req24 = _req_for(24)
            count22 = mi17_alt_counts.get(22, 0)
            count23 = mi17_alt_counts.get(23, 0)
            count24 = mi17_alt_counts.get(24, 0)

            combo1_deficit = max(0, req22 - count22) + max(0, req23 - count23)
            combo2_deficit = max(0, req24 - count24)
            use_combo1 = combo1_deficit <= combo2_deficit

            def _format_entry(group: int, installed: int, required: int, comp_desc: str) -> str:
                entry_plain = f"{group}:{installed}/{required}"
                if comp_desc:
                    entry_plain += f"({comp_desc})"
                return entry_plain

            if use_combo1:
                required_components += req22 + req23
                core_components += count22 + count23
                if combo1_deficit == 0:
                    allowed_extra += max(0, count22 - req22)
                    allowed_extra += max(0, count23 - req23)
                    allowed_extra += count24
                else:
                    if count22 < req22:
                        installed, required_val, desc = mi17_alt_details.get(22, (count22, req22, ""))
                        entry_plain = _format_entry(22, installed, required_val, desc)
                        shortages.append(f"**{entry_plain}**")
                        core_missing.append(entry_plain)
                    if count23 < req23:
                        installed, required_val, desc = mi17_alt_details.get(23, (count23, req23, ""))
                        entry_plain = _format_entry(23, installed, required_val, desc)
                        shortages.append(f"**{entry_plain}**")
                        core_missing.append(entry_plain)
                    allowed_extra += count24
            else:
                required_components += req24
                core_components += count24
                if combo2_deficit == 0:
                    allowed_extra += max(0, count24 - req24)
                    allowed_extra += count22 + count23
                else:
                    installed, required_val, desc = mi17_alt_details.get(24, (count24, req24, ""))
                    entry_plain = _format_entry(24, installed, required_val, desc)
                    shortages.append(f"**{entry_plain}**")
                    core_missing.append(entry_plain)
                    allowed_extra += count22 + count23

        if is_mi17:
            req_values = {group: _req_for(group) for group in MI17_ENGINE_GROUPS}
            engine_required = max(req_values.values()) if req_values else 0
            engine_total = sum(mi17_engine_counts.get(group, 0) for group in MI17_ENGINE_GROUPS)

            if engine_required == 0:
                allowed_extra += engine_total
            else:
                required_components += engine_required
                core_components += engine_total
                if engine_total < engine_required:
                    detail_parts: List[str] = []
                    for group in sorted(MI17_ENGINE_GROUPS):
                        installed, required_val, comp_desc = mi17_engine_details.get(
                            group, (mi17_engine_counts.get(group, 0), req_values.get(group, 0), "")
                        )
                        entry_plain = f"{group}:{installed}/{required_val}"
                        if comp_desc:
                            entry_plain += f"({comp_desc})"
                        detail_parts.append(entry_plain)
                    entry_plain = f"28|29|30:{engine_total}/{engine_required}"
                    if detail_parts:
                        entry_plain += f"({'; '.join(detail_parts)})"
                    shortages.append(f"**{entry_plain}**")
                    core_missing.append(entry_plain)
                elif engine_total > engine_required:
                    allowed_extra += engine_total - engine_required

        if is_mi17:
            agb_required = 2
            total_agb = sum(mi17_agb_counts.get(group, 0) for group in MI17_AGB_GROUPS)
            required_components += agb_required
            core_components += total_agb

            if total_agb < agb_required:
                detail_parts: List[str] = []
                for group in sorted(MI17_AGB_GROUPS):
                    installed = mi17_agb_counts.get(group, 0)
                    req_detail = requirements.get(group)
                    req_val = req_detail["required"] if req_detail else 0
                    comp_desc = ";".join(requirement_details.get(group, []))
                    entry_plain = f"{group}:{installed}/{req_val}"
                    if comp_desc:
                        entry_plain += f"({comp_desc})"
                    detail_parts.append(entry_plain)
                entry_plain = f"35|36|37:{total_agb}/{agb_required}"
                if detail_parts:
                    entry_plain += f"({'; '.join(detail_parts)})"
                shortages.append(f"**{entry_plain}**")
                core_missing.append(entry_plain)
            elif total_agb > agb_required:
                allowed_extra += total_agb - agb_required
        delta = total_components - required_components - allowed_extra
        aggregations.append(
            PlaneAggregation(
                aircraft_number=aircraft_number,
                ac_type_mask=ac_type_mask,
                mfg_date=mfg_date,
                partno=partno,
                variant=variant,
                total_components=total_components,
                required_components=required_components,
                optional_components=allowed_extra,
                delta=delta,
                shortage_groups=shortages,
                core_components=core_components,
                core_missing_groups=core_missing,
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


def build_markdown(
    version: VersionInfo,
    md_version: VersionInfo,
    rows: Sequence[PlaneAggregation],
) -> str:
    mi8, mi17, mixed = split_by_type(rows)
    sections = [
        "# Агрегаты других групп на планерах operations",
        "",
        "**Легенда:** `группа:установлено/норма(partno:qty)` — формат записи в колонке отклонений. "
        "Жирным выделены обязательные группы с дефицитом.",
        "",
        f"- Версия heli_pandas: `{version.version_date} v{version.version_id}`",
        f"- Нормы md_components: `{md_version.version_date} v{md_version.version_id}` "
        f"(статика из `{latest_source_dataset_label()}`)",
        f"- Сгенерировано: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Планеров в эксплуатации: **{len(rows)}**",
        f"- Агрегатов (кроме планеров): **{sum(r.total_components for r in rows)}**",
        "",
    ]

    def append_section(title: str, items: Sequence[PlaneAggregation]) -> None:
        sections.append(f"## {title}")
        sections.append(f"Всего планеров: **{len(items)}**")
        sections.append("")
        sections.append(
            "| aircraft_number | variant | mfg_date | агрегаты | норма | опциональное | Δ | отклонения по группам |"
        )
        sections.append("| ---: | --- | --- | ---: | ---: | ---: | ---: | --- |")
        for row in items:
            variant = _escape_md_cell(row.variant)
            mfg_date = _escape_md_cell(row.mfg_date)
            shortages_md = (
                ", ".join(_escape_md_cell(item) for item in row.shortage_groups)
                if row.shortage_groups
                else ""
            )
            sections.append(
                f"| {row.aircraft_number} | {variant} | {mfg_date} | "
                f"{row.total_components} | {row.required_components} | {row.optional_components} | "
                f"{row.delta} | "
                f"{shortages_md} |"
            )
        sections.append("")

    append_section("Ми-8", mi8)
    append_section("Ми-17", mi17)
    if mixed:
        append_section("Смешанные/неизвестные", mixed)

    lines = sections
    return "\n".join(lines)


def render_pdf(markdown: str, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".md", delete=False, encoding="utf-8"
        ) as tmp:
            tmp.write(markdown)
            tmp_path = Path(tmp.name)
        subprocess.run(
            [
                "pandoc",
                str(tmp_path),
                "-o",
                str(target_path),
                "--pdf-engine=xelatex",
                "-V",
                "mainfont=DejaVu Serif",
                "-V",
                "sansfont=DejaVu Sans",
                "-V",
                "monofont=DejaVu Sans Mono",
                "-V",
                "fontsize=10pt",
            ],
            check=True,
        )
    finally:
        if tmp_path and tmp_path.exists():
            tmp_path.unlink()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Подсчёт агрегатов других групп на планерах со status_id=2"
    )
    parser.add_argument("--version-date", type=str, help="Дата версии (YYYY-MM-DD)")
    parser.add_argument("--version-id", type=int, help="ID версии")
    parser.add_argument(
        "--md-path",
        type=str,
        help="Путь к Markdown-отчёту (по умолчанию output/heli_pandas_ops_other_groups_<version>.md)",
    )
    parser.add_argument("--skip-md", action="store_true", help="Не сохранять Markdown")
    parser.add_argument(
        "--pdf-path",
        type=str,
        help="Путь к PDF-отчёту (по умолчанию output/heli_pandas_ops_other_groups_<version>.pdf)",
    )
    parser.add_argument("--skip-pdf", action="store_true", help="Не сохранять PDF")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = get_clickhouse_client()
    version = resolve_version(client, args.version_date, args.version_id)
    md_slice = resolve_latest_md_slice(client)
    md_version = VersionInfo(
        version_date=md_slice.version_date,
        version_id=md_slice.version_id,
    )
    rows = fetch_aggregations(client, version, md_version)

    print(
        f"\n✅ heli_pandas: {version.version_date} v{version.version_id}; "
        f"md_components: {md_version.version_date} v{md_version.version_id} "
        f"(статика: {latest_source_dataset_label()})"
    )
    print(
        f"✅ Планеров в status_id=2: {len(rows)}; агрегатов других групп: "
        f"{sum(r.total_components for r in rows)}"
    )
    mi8, mi17, mixed = split_by_type(rows)

    def print_group(title: str, group: Sequence[PlaneAggregation]) -> None:
        print(f"\n{title} ({len(group)} планеров)")
        print(
            f"{'aircraft':>8}  {'var':<6}  {'mfg_date':<10}  {'inst':>5}  {'norm':>5}  "
            f"{'opt':>5}  {'Δ':>3}  {'shortages':<20}"
        )
        print("-" * 78)
        for row in group:
            variant = row.variant or ""
            print(
                f"{row.aircraft_number:>8}  {variant:<6}  {(row.mfg_date or ''):<10}  "
                f"{row.total_components:>5}  {row.required_components:>5}  "
                f"{row.optional_components:>5}  "
                f"{row.delta:>3}  {', '.join(row.shortage_groups) if row.shortage_groups else ''}"
            )

    print_group("Ми-8", mi8)
    mi8_core_alerts = [row for row in mi8 if row.core_components < MI8_CORE_TARGET]
    if mi8_core_alerts:
        print(
            f"\nMi-8T < {MI8_CORE_TARGET} агрегатов (без групп 33/34/35): "
            f"{len(mi8_core_alerts)} шт."
        )
        print(
            f"{'aircraft':>8}  {'var':<6}  {'inst_core':>9}  {'target':>6}  {'дефицит':>7}  "
            f"{'нужны группы':<30}"
        )
        print("-" * 80)
        for row in mi8_core_alerts:
            deficit = MI8_CORE_TARGET - row.core_components
            variant = row.variant or ""
            print(
                f"{row.aircraft_number:>8}  {variant:<6}  {row.core_components:>9}  "
                f"{MI8_CORE_TARGET:>6}  {deficit:>7}  "
                f"{', '.join(row.core_missing_groups) if row.core_missing_groups else ''}"
            )
    print_group("Ми-17", mi17)
    if mixed:
        print_group("Смешанные/неизвестные", mixed)

    markdown = build_markdown(version, md_version, rows)

    if not args.skip_md:
        md_path = (
            Path(args.md_path)
            if args.md_path
            else Path(f"output/heli_pandas_ops_other_groups_{version.version_date}.md")
        )
        md_path.write_text(markdown, encoding="utf-8")
        print(f"\n📝 Markdown-отчёт сохранён в {md_path}")

    if not args.skip_pdf:
        pdf_path = (
            Path(args.pdf_path)
            if args.pdf_path
            else Path(f"output/heli_pandas_ops_other_groups_{version.version_date}.pdf")
        )
        try:
            render_pdf(markdown, pdf_path)
            print(f"📄 PDF-отчёт сохранён в {pdf_path}")
        except (subprocess.CalledProcessError, FileNotFoundError) as exc:
            print(f"⚠️ Не удалось создать PDF ({exc}). Проверьте установку pandoc/xelatex.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

