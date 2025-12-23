#!/usr/bin/env python3
"""
Анализ агрегатов с ограничениями по лизингу (lease_restricted=1).

Отчёты:
1. Планеры с lease_restricted=1 в эксплуатации
2. Агрегаты на restricted бортах с другим собственником
3. Агрегаты с lease_restricted=1, не установленные на борту
4. Статистика по собственникам

Выход:
- Консольный отчёт
- Markdown: docs/lease_restricted_analysis_<version>.md
- Excel: docs/lease_restricted_analysis_<version>.xlsx
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

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


@dataclass(frozen=True)
class VersionInfo:
    version_date: str
    version_id: int


@dataclass
class RestrictedPlane:
    aircraft_number: int
    owner: str
    group_by: int
    partno: str


@dataclass
class MismatchedAggregate:
    aircraft_number: int
    plane_owner: str
    agg_owner: str
    group_by: int
    partno: str
    serialno: str
    lease_restricted: int


@dataclass
class PlaneAggregateStats:
    aircraft_number: int
    owner: str
    ac_type: str
    total_aggregates: int
    with_owner: int
    without_owner: int
    same_owner: int
    different_owner: int


@dataclass
class RestrictedAggregateOnOtherPlane:
    """Агрегат с ограничениями, установленный на борту другого собственника или не установленный."""
    agg_owner: str
    partno: str
    serialno: str
    group_by: int
    condition: str
    aircraft_number: int  # 0 = не установлен
    plane_owner: str  # пусто если не установлен
    status: str  # "не установлен" или "на чужом борту"


@dataclass
class UninstalledAggregate:
    owner: str
    partno: str
    serialno: str
    group_by: int
    condition: str
    location: str


@dataclass
class OwnerStats:
    owner: str
    planes: int
    installed_agg: int
    uninstalled_agg: int


def resolve_version(client, version_date: Optional[str], version_id: Optional[int]) -> VersionInfo:
    if version_date and version_id is not None:
        return VersionInfo(version_date=version_date, version_id=version_id)
    
    rows = client.execute("""
        SELECT toString(version_date) AS version_date,
               toUInt8(version_id) AS version_id
        FROM heli_pandas
        ORDER BY version_date DESC, version_id DESC
        LIMIT 1
    """)
    if not rows:
        raise RuntimeError("Таблица heli_pandas пуста")
    return VersionInfo(version_date=rows[0][0], version_id=int(rows[0][1]))


def fetch_restricted_planes(client, version: VersionInfo) -> List[RestrictedPlane]:
    """Планеры с lease_restricted=1 в эксплуатации."""
    rows = client.execute("""
        SELECT 
            aircraft_number,
            owner,
            group_by,
            partno
        FROM heli_pandas
        WHERE version_date = %(version_date)s
          AND version_id = %(version_id)s
          AND group_by IN (1, 2)
          AND lease_restricted = 1
          AND status_id = 2
        ORDER BY aircraft_number
    """, {"version_date": version.version_date, "version_id": version.version_id})
    
    return [
        RestrictedPlane(
            aircraft_number=int(row[0]),
            owner=row[1] or "",
            group_by=int(row[2]),
            partno=row[3] or ""
        )
        for row in rows
    ]


def fetch_mismatched_aggregates(client, version: VersionInfo) -> List[MismatchedAggregate]:
    """Агрегаты на restricted бортах с другим или пустым собственником."""
    rows = client.execute("""
        SELECT 
            hp.aircraft_number,
            hp.owner as agg_owner,
            hp.partno,
            hp.serialno,
            hp.group_by,
            hp.lease_restricted,
            plane.owner as plane_owner
        FROM heli_pandas hp
        JOIN (
            SELECT aircraft_number, any(owner) as owner
            FROM heli_pandas
            WHERE version_date = %(version_date)s
              AND version_id = %(version_id)s
              AND group_by IN (1, 2)
              AND lease_restricted = 1
              AND status_id = 2
            GROUP BY aircraft_number
        ) plane ON hp.aircraft_number = plane.aircraft_number
        WHERE hp.version_date = %(version_date)s
          AND hp.version_id = %(version_id)s
          AND hp.group_by > 2
          AND (
              hp.owner IS NULL 
              OR hp.owner = ''
              OR hp.owner = '0'
              OR hp.owner != plane.owner
          )
        ORDER BY hp.aircraft_number, hp.group_by
    """, {"version_date": version.version_date, "version_id": version.version_id})
    
    return [
        MismatchedAggregate(
            aircraft_number=int(row[0]),
            agg_owner=row[1] or "(пусто)",
            partno=row[2] or "",
            serialno=row[3] or "",
            group_by=int(row[4]),
            lease_restricted=int(row[5]),
            plane_owner=row[6] or ""
        )
        for row in rows
    ]


def fetch_plane_aggregate_stats(client, version: VersionInfo) -> List[PlaneAggregateStats]:
    """Статистика по количеству агрегатов на каждом restricted вертолёте."""
    rows = client.execute("""
        WITH restricted_planes AS (
            SELECT 
                aircraft_number,
                any(owner) as plane_owner,
                any(group_by) as plane_group_by
            FROM heli_pandas
            WHERE version_date = %(version_date)s
              AND version_id = %(version_id)s
              AND group_by IN (1, 2)
              AND lease_restricted = 1
              AND status_id = 2
            GROUP BY aircraft_number
        )
        SELECT 
            rp.aircraft_number,
            rp.plane_owner,
            rp.plane_group_by,
            countIf(hp.partno IS NOT NULL) as total_aggregates,
            countIf(hp.owner IS NOT NULL AND hp.owner != '' AND hp.owner != '0') as with_owner,
            countIf(hp.owner IS NULL OR hp.owner = '' OR hp.owner = '0') as without_owner,
            countIf(hp.owner = rp.plane_owner) as same_owner,
            countIf(hp.owner IS NOT NULL AND hp.owner != '' AND hp.owner != '0' AND hp.owner != rp.plane_owner) as different_owner
        FROM restricted_planes rp
        LEFT JOIN heli_pandas hp 
            ON hp.aircraft_number = rp.aircraft_number
            AND hp.version_date = %(version_date)s
            AND hp.version_id = %(version_id)s
            AND hp.group_by > 2
        GROUP BY rp.aircraft_number, rp.plane_owner, rp.plane_group_by
        ORDER BY rp.aircraft_number
    """, {"version_date": version.version_date, "version_id": version.version_id})
    
    return [
        PlaneAggregateStats(
            aircraft_number=int(row[0]),
            owner=row[1] or "",
            ac_type="Mi-8" if row[2] == 1 else "Mi-17",
            total_aggregates=int(row[3]),
            with_owner=int(row[4]),
            without_owner=int(row[5]),
            same_owner=int(row[6]),
            different_owner=int(row[7])
        )
        for row in rows
    ]


def fetch_uninstalled_aggregates(client, version: VersionInfo) -> List[UninstalledAggregate]:
    """Агрегаты с lease_restricted=1, не установленные на борту."""
    rows = client.execute("""
        SELECT 
            owner,
            partno,
            serialno,
            group_by,
            condition,
            location
        FROM heli_pandas
        WHERE version_date = %(version_date)s
          AND version_id = %(version_id)s
          AND lease_restricted = 1
          AND group_by > 2
          AND (aircraft_number = 0 OR aircraft_number IS NULL)
        ORDER BY owner, group_by
    """, {"version_date": version.version_date, "version_id": version.version_id})
    
    return [
        UninstalledAggregate(
            owner=row[0] or "",
            partno=row[1] or "",
            serialno=row[2] or "",
            group_by=int(row[3]),
            condition=row[4] or "",
            location=row[5] or ""
        )
        for row in rows
    ]


def fetch_owner_stats(client, version: VersionInfo) -> List[OwnerStats]:
    """Статистика по собственникам."""
    rows = client.execute("""
        SELECT 
            owner,
            countIf(group_by IN (1,2)) as planes,
            countIf(group_by > 2 AND aircraft_number > 0) as installed_agg,
            countIf(group_by > 2 AND (aircraft_number = 0 OR aircraft_number IS NULL)) as uninstalled_agg
        FROM heli_pandas
        WHERE version_date = %(version_date)s
          AND version_id = %(version_id)s
          AND lease_restricted = 1
        GROUP BY owner
        ORDER BY planes DESC, installed_agg DESC
    """, {"version_date": version.version_date, "version_id": version.version_id})
    
    return [
        OwnerStats(
            owner=row[0] or "NULL",
            planes=int(row[1]),
            installed_agg=int(row[2]),
            uninstalled_agg=int(row[3])
        )
        for row in rows
    ]


def fetch_restricted_agg_misplaced(client, version: VersionInfo) -> List[RestrictedAggregateOnOtherPlane]:
    """Агрегаты с ограничениями (lease_restricted=1), которые не установлены или установлены на борту другого собственника."""
    rows = client.execute("""
        SELECT 
            hp.owner as agg_owner,
            hp.partno,
            hp.serialno,
            hp.group_by,
            hp.condition,
            hp.aircraft_number,
            plane.owner as plane_owner
        FROM heli_pandas hp
        LEFT JOIN (
            SELECT aircraft_number, any(owner) as owner
            FROM heli_pandas
            WHERE version_date = %(version_date)s
              AND version_id = %(version_id)s
              AND group_by IN (1, 2)
            GROUP BY aircraft_number
        ) plane ON hp.aircraft_number = plane.aircraft_number
        WHERE hp.version_date = %(version_date)s
          AND hp.version_id = %(version_id)s
          AND hp.group_by > 2
          AND hp.lease_restricted = 1
          AND (
              hp.aircraft_number = 0 
              OR hp.aircraft_number IS NULL
              OR hp.owner != plane.owner
          )
        ORDER BY hp.owner, hp.group_by, hp.aircraft_number
    """, {"version_date": version.version_date, "version_id": version.version_id})
    
    result = []
    for row in rows:
        agg_owner, partno, serialno, group_by, condition, ac_num, plane_owner = row
        ac_num = int(ac_num) if ac_num else 0
        if ac_num == 0:
            status = "не установлен"
        else:
            status = "на чужом борту"
        result.append(RestrictedAggregateOnOtherPlane(
            agg_owner=agg_owner or "",
            partno=partno or "",
            serialno=serialno or "",
            group_by=int(group_by),
            condition=condition or "",
            aircraft_number=ac_num,
            plane_owner=plane_owner or "",
            status=status
        ))
    return result


def get_ac_type(group_by: int) -> str:
    if group_by == 1:
        return "Mi-8"
    elif group_by == 2:
        return "Mi-17"
    return f"G{group_by}"


def build_markdown(
    version: VersionInfo,
    planes: List[RestrictedPlane],
    mismatched: List[MismatchedAggregate],
    uninstalled: List[UninstalledAggregate],
    stats: List[OwnerStats],
    plane_stats: List[PlaneAggregateStats]
) -> str:
    lines = [
        "# Анализ ограничений по лизингу (lease_restricted=1)",
        "",
        f"- **Версия данных**: `{version.version_date} v{version.version_id}`",
        f"- **Дата отчёта**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "---",
        "",
        "## Сводка",
        "",
        "| Показатель | Значение |",
        "|------------|----------|",
        f"| Планеров с ограничениями | **{len(planes)}** |",
        f"| Агрегатов с другим owner | **{len(mismatched)}** |",
        f"| Агрегатов не установленных | **{len(uninstalled)}** |",
        "",
        "---",
        "",
        "## 1. Планеры с lease_restricted=1 (в эксплуатации)",
        "",
        "| Борт | Собственник | Тип | PartNo |",
        "|---:|---|---|---|",
    ]
    
    for p in planes:
        ac_type = get_ac_type(p.group_by)
        lines.append(f"| {p.aircraft_number} | {p.owner} | {ac_type} | {p.partno[:40]} |")
    
    lines.extend([
        "",
        "---",
        "",
        "## 2. Агрегаты на restricted бортах с ДРУГИМ собственником",
        "",
        f"Всего: **{len(mismatched)}** агрегатов",
        "",
        "| Борт | Owner планера | Owner агрегата | Группа | PartNo | SerialNo |",
        "|---:|---|---|---:|---|---|",
    ])
    
    for m in mismatched[:100]:  # Лимит для читаемости
        lines.append(
            f"| {m.aircraft_number} | {m.plane_owner} | {m.agg_owner} | "
            f"{m.group_by} | {m.partno[:30]} | {m.serialno[:20]} |"
        )
    
    if len(mismatched) > 100:
        lines.append(f"| ... | ... | ... | ... | ... | ещё {len(mismatched) - 100} записей |")
    
    lines.extend([
        "",
        "---",
        "",
        "## 3. Агрегаты с lease_restricted=1, НЕ установленные на борту",
        "",
        f"Всего: **{len(uninstalled)}** агрегатов",
        "",
        "| Собственник | Группа | PartNo | SerialNo | Состояние | Локация |",
        "|---|---:|---|---|---|---|",
    ])
    
    for u in uninstalled:
        lines.append(
            f"| {u.owner} | {u.group_by} | {u.partno[:30]} | "
            f"{u.serialno[:20]} | {u.condition[:15]} | {u.location[:20]} |"
        )
    
    lines.extend([
        "",
        "---",
        "",
        "## 4. Статистика по собственникам",
        "",
        "| Собственник | Планеры | Установлено | Не установлено |",
        "|---|---:|---:|---:|",
    ])
    
    for s in stats:
        lines.append(f"| {s.owner} | {s.planes} | {s.installed_agg} | {s.uninstalled_agg} |")
    
    lines.extend([
        "",
        "---",
        "",
        "## 5. Количество агрегатов на каждом restricted вертолёте",
        "",
        "| Борт | Собственник | Тип | Всего агр. | С owner | Без owner | Совпадает | Другой owner |",
        "|---:|---|---|---:|---:|---:|---:|---:|",
    ])
    
    for ps in plane_stats:
        lines.append(
            f"| {ps.aircraft_number} | {ps.owner} | {ps.ac_type} | "
            f"{ps.total_aggregates} | {ps.with_owner} | {ps.without_owner} | "
            f"{ps.same_owner} | {ps.different_owner} |"
        )
    
    lines.extend([
        "",
        "---",
        "",
        f"*Отчёт сгенерирован автоматически*",
    ])
    
    return "\n".join(lines)


def save_excel(
    version: VersionInfo,
    planes: List[RestrictedPlane],
    mismatched: List[MismatchedAggregate],
    uninstalled: List[UninstalledAggregate],
    stats: List[OwnerStats],
    plane_stats: List[PlaneAggregateStats],
    restricted_misplaced: List[RestrictedAggregateOnOtherPlane],
    path: Path
) -> None:
    if not HAS_PANDAS:
        print("⚠️ pandas не установлен, Excel не будет сохранён")
        return
    
    with pd.ExcelWriter(path, engine='openpyxl') as writer:
        # Лист 1: Планеры
        df_planes = pd.DataFrame([
            {
                "Борт": p.aircraft_number,
                "Собственник": p.owner,
                "Тип": get_ac_type(p.group_by),
                "PartNo": p.partno
            }
            for p in planes
        ])
        df_planes.to_excel(writer, sheet_name="Планеры", index=False)
        
        # Лист 2: Агрегаты с другим owner
        df_mismatched = pd.DataFrame([
            {
                "Борт": m.aircraft_number,
                "Owner планера": m.plane_owner,
                "Owner агрегата": m.agg_owner,
                "Группа": m.group_by,
                "PartNo": m.partno,
                "SerialNo": m.serialno,
                "lease_restricted": m.lease_restricted
            }
            for m in mismatched
        ])
        df_mismatched.to_excel(writer, sheet_name="Другой owner", index=False)
        
        # Лист 3: Не установленные
        df_uninstalled = pd.DataFrame([
            {
                "Собственник": u.owner,
                "Группа": u.group_by,
                "PartNo": u.partno,
                "SerialNo": u.serialno,
                "Состояние": u.condition,
                "Локация": u.location
            }
            for u in uninstalled
        ])
        df_uninstalled.to_excel(writer, sheet_name="Не установлены", index=False)
        
        # Лист 4: Статистика по собственникам
        df_stats = pd.DataFrame([
            {
                "Собственник": s.owner,
                "Планеры": s.planes,
                "Установлено": s.installed_agg,
                "Не установлено": s.uninstalled_agg
            }
            for s in stats
        ])
        df_stats.to_excel(writer, sheet_name="Статистика", index=False)
        
        # Лист 5: Агрегаты по бортам
        df_plane_stats = pd.DataFrame([
            {
                "Борт": ps.aircraft_number,
                "Собственник": ps.owner,
                "Тип ВС": ps.ac_type,
                "Всего агрегатов": ps.total_aggregates,
                "С owner": ps.with_owner,
                "Без owner": ps.without_owner,
                "Owner совпадает": ps.same_owner,
                "Другой owner": ps.different_owner
            }
            for ps in plane_stats
        ])
        df_plane_stats.to_excel(writer, sheet_name="Агрегаты по бортам", index=False)
        
        # Лист 6: Restricted агрегаты на чужих бортах или не установлены
        df_misplaced = pd.DataFrame([
            {
                "Owner агрегата": rm.agg_owner,
                "Группа": rm.group_by,
                "PartNo": rm.partno,
                "SerialNo": rm.serialno,
                "Состояние": rm.condition,
                "Статус": rm.status,
                "Борт": rm.aircraft_number if rm.aircraft_number > 0 else "",
                "Owner планера": rm.plane_owner
            }
            for rm in restricted_misplaced
        ])
        df_misplaced.to_excel(writer, sheet_name="Restricted на чужих", index=False)
        
        # Лист 7: Метаданные
        df_meta = pd.DataFrame([
            {"Параметр": "Версия данных", "Значение": f"{version.version_date} v{version.version_id}"},
            {"Параметр": "Дата отчёта", "Значение": datetime.now().strftime('%Y-%m-%d %H:%M:%S')},
            {"Параметр": "Планеров с ограничениями", "Значение": len(planes)},
            {"Параметр": "Агрегатов с другим owner", "Значение": len(mismatched)},
            {"Параметр": "Агрегатов не установленных", "Значение": len(uninstalled)},
        ])
        df_meta.to_excel(writer, sheet_name="Метаданные", index=False)


def print_console_report(
    version: VersionInfo,
    planes: List[RestrictedPlane],
    mismatched: List[MismatchedAggregate],
    uninstalled: List[UninstalledAggregate],
    stats: List[OwnerStats],
    plane_stats: List[PlaneAggregateStats]
) -> None:
    print("=" * 100)
    print("🔍 АНАЛИЗ LEASE_RESTRICTED")
    print(f"   Версия данных: {version.version_date} v{version.version_id}")
    print("=" * 100)
    
    # 1. Планеры
    print(f"\n📋 1. ПЛАНЕРЫ С LEASE_RESTRICTED=1 (в эксплуатации): {len(planes)}")
    print("-" * 80)
    print(f"{'Борт':>8}  {'Собственник':<20}  {'Тип':<8}  {'PartNo':<30}")
    print("-" * 70)
    for p in planes[:20]:
        print(f"{p.aircraft_number:>8}  {p.owner[:20]:<20}  {get_ac_type(p.group_by):<8}  {p.partno[:30]:<30}")
    if len(planes) > 20:
        print(f"... ещё {len(planes) - 20} планеров")
    
    # 2. Агрегаты с другим owner
    print(f"\n📋 2. АГРЕГАТЫ С ДРУГИМ СОБСТВЕННИКОМ: {len(mismatched)}")
    print("-" * 100)
    print(f"{'Борт':>8}  {'Owner планера':<15}  {'Owner агрегата':<15}  {'Группа':>6}  {'PartNo':<25}  {'SerialNo':<15}")
    print("-" * 100)
    for m in mismatched[:30]:
        print(f"{m.aircraft_number:>8}  {m.plane_owner[:15]:<15}  {m.agg_owner[:15]:<15}  {m.group_by:>6}  {m.partno[:25]:<25}  {m.serialno[:15]:<15}")
    if len(mismatched) > 30:
        print(f"... ещё {len(mismatched) - 30} записей")
    
    # 3. Не установленные
    print(f"\n📋 3. АГРЕГАТЫ НЕ УСТАНОВЛЕННЫЕ: {len(uninstalled)}")
    print("-" * 115)
    print(f"{'Собственник':<20}  {'Группа':>6}  {'PartNo':<30}  {'SerialNo':<15}  {'Состояние':<15}  {'Локация':<20}")
    print("-" * 115)
    for u in uninstalled[:30]:
        print(f"{u.owner[:20]:<20}  {u.group_by:>6}  {u.partno[:30]:<30}  {u.serialno[:15]:<15}  {u.condition[:15]:<15}  {u.location[:20]:<20}")
    if len(uninstalled) > 30:
        print(f"... ещё {len(uninstalled) - 30} записей")
    
    # 4. Статистика
    print(f"\n📋 4. СТАТИСТИКА ПО СОБСТВЕННИКАМ")
    print("-" * 60)
    print(f"{'Собственник':<25}  {'Планеры':>8}  {'Установл.':>10}  {'Не установл.':>12}")
    print("-" * 60)
    for s in stats:
        print(f"{s.owner[:25]:<25}  {s.planes:>8}  {s.installed_agg:>10}  {s.uninstalled_agg:>12}")
    
    # 5. Агрегаты по бортам
    print(f"\n📋 5. КОЛИЧЕСТВО АГРЕГАТОВ НА КАЖДОМ RESTRICTED ВЕРТОЛЁТЕ")
    print("-" * 110)
    print(f"{'Борт':>8}  {'Собственник':<15}  {'Тип':<8}  {'Всего':>6}  {'С owner':>8}  {'Без owner':>10}  {'Совпад.':>8}  {'Другой':>8}")
    print("-" * 110)
    for ps in plane_stats:
        print(f"{ps.aircraft_number:>8}  {ps.owner[:15]:<15}  {ps.ac_type:<8}  {ps.total_aggregates:>6}  {ps.with_owner:>8}  {ps.without_owner:>10}  {ps.same_owner:>8}  {ps.different_owner:>8}")
    
    # Итого
    total_agg = sum(ps.total_aggregates for ps in plane_stats)
    total_with = sum(ps.with_owner for ps in plane_stats)
    total_without = sum(ps.without_owner for ps in plane_stats)
    total_same = sum(ps.same_owner for ps in plane_stats)
    total_diff = sum(ps.different_owner for ps in plane_stats)
    print("-" * 110)
    print(f"{'ИТОГО':>8}  {'':<15}  {'':<8}  {total_agg:>6}  {total_with:>8}  {total_without:>10}  {total_same:>8}  {total_diff:>8}")
    
    print("\n" + "=" * 100)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Анализ ограничений по лизингу (lease_restricted=1)"
    )
    parser.add_argument("--version-date", type=str, help="Дата версии (YYYY-MM-DD)")
    parser.add_argument("--version-id", type=int, help="ID версии")
    parser.add_argument("--md-path", type=str, help="Путь к Markdown-отчёту")
    parser.add_argument("--xlsx-path", type=str, help="Путь к Excel-отчёту")
    parser.add_argument("--skip-md", action="store_true", help="Не сохранять Markdown")
    parser.add_argument("--skip-xlsx", action="store_true", help="Не сохранять Excel")
    parser.add_argument("--quiet", action="store_true", help="Минимальный вывод в консоль")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = get_clickhouse_client()
    version = resolve_version(client, args.version_date, args.version_id)
    
    # Загрузка данных
    planes = fetch_restricted_planes(client, version)
    mismatched = fetch_mismatched_aggregates(client, version)
    uninstalled = fetch_uninstalled_aggregates(client, version)
    stats = fetch_owner_stats(client, version)
    plane_stats = fetch_plane_aggregate_stats(client, version)
    restricted_misplaced = fetch_restricted_agg_misplaced(client, version)
    
    # Консольный отчёт
    if not args.quiet:
        print_console_report(version, planes, mismatched, uninstalled, stats, plane_stats)
        # Дополнительная статистика по restricted_misplaced
        not_installed = [r for r in restricted_misplaced if r.status == "не установлен"]
        on_other = [r for r in restricted_misplaced if r.status == "на чужом борту"]
        print(f"\n📋 6. RESTRICTED АГРЕГАТЫ НЕ НА СВОЁМ МЕСТЕ: {len(restricted_misplaced)}")
        print(f"   - Не установлены: {len(not_installed)}")
        print(f"   - На чужих бортах: {len(on_other)}")
    else:
        print(f"✅ Планеров: {len(planes)}, агрегатов с другим/пустым owner: {len(mismatched)}, не установленных: {len(uninstalled)}")
    
    # Markdown
    if not args.skip_md:
        md_content = build_markdown(version, planes, mismatched, uninstalled, stats, plane_stats)
        md_path = Path(args.md_path) if args.md_path else Path(f"docs/lease_restricted_analysis_{version.version_date}.md")
        md_path.parent.mkdir(parents=True, exist_ok=True)
        md_path.write_text(md_content, encoding="utf-8")
        print(f"\n📝 Markdown-отчёт сохранён: {md_path}")
    
    # Excel
    if not args.skip_xlsx:
        xlsx_path = Path(args.xlsx_path) if args.xlsx_path else Path(f"docs/lease_restricted_analysis_{version.version_date}.xlsx")
        xlsx_path.parent.mkdir(parents=True, exist_ok=True)
        save_excel(version, planes, mismatched, uninstalled, stats, plane_stats, restricted_misplaced, xlsx_path)
        print(f"📊 Excel-отчёт сохранён: {xlsx_path}")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

