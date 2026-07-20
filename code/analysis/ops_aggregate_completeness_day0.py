#!/usr/bin/env python3
"""
Day0-аналитика недокомплекта агрегатов на бортах в эксплуатации (OPS, status_id=2).

Вопрос: какие борты, числящиеся на day0 в OPS, имеют недокомплект агрегатов
(в исправном/OPS-статусе) и в каком количестве — по ВСЕЙ агрегатике (group_by>2),
а не только по двигателям.

Важно: комплектность нетривиальна — есть альтернативные группы (АПУ 14/15/16,
гидро 22/23/24, АГБ 35/36/37, топливные 28/29/30) и опциональные (32/33/34).
Поэтому НЕ считаем «каждая группа = своя жёсткая норма», а переиспользуем уже
доменно-валидированную логику `heli_pandas_ops_other_groups.fetch_aggregations`
(она схлопывает альтернативы в одну позицию вида `12|13|14:0/1`).

Недокомплект борта = сумма по shortage-позициям, где installed < required
(«лишние» вида `41:3/2` в дефицит НЕ попадают).

На OPS-бортах агрегаты практически все в статусе 2 (эксплуатация) + единично 3
(исправный), неисправных смонтированных нет -> «в статусе опс» == общий подсчёт.
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

code_root = Path(__file__).resolve().parents[1]
sys.path.append(str(code_root / "utils"))
sys.path.append(str(code_root))
from config_loader import get_clickhouse_client  # type: ignore
from heli_pandas_ops_other_groups import (  # type: ignore
    VersionInfo,
    fetch_aggregations,
    resolve_version,
)
from static_data_resolver import resolve_latest_md_slice  # type: ignore

SHORTAGE_RE = re.compile(r":(\d+)/(\d+)")


def parse_shortage(entry: str) -> tuple[int, int]:
    """Возвращает (installed, required) из shortage-строки. Работает и для
    альтернатив вида `12|13|14:0/1` — берём первое совпадение `a/b`."""
    m = SHORTAGE_RE.search(entry)
    if not m:
        return (0, 0)
    return int(m.group(1)), int(m.group(2))


def parse_prefix_groups(entry: str) -> list[int]:
    """group_by-префикс shortage-строки: `6:...` -> [6]; `35|36|37:...` -> [35,36,37]."""
    prefix = entry.strip("*").split(":", 1)[0]
    return [int(x) for x in prefix.split("|") if x.strip().isdigit()]


def load_nomenclatures(client, md_version: VersionInfo) -> dict[int, str]:
    """group_by -> название номенклатуры (partno) агрегата из md_components."""
    rows = client.execute(
        """
        SELECT group_by, any(partno) AS partno
        FROM md_components
        WHERE version_date = %(vd)s AND version_id = %(vi)s AND group_by > 2
        GROUP BY group_by
        """,
        {"vd": md_version.version_date, "vi": md_version.version_id},
    )
    return {int(g): str(p) for g, p in rows}


def render_nomenclature(entry: str, nomen: dict[int, str]) -> str:
    """Название недостающей номенклатуры вместо номера группы.
    Для альтернатив (несколько групп) -> «одна из: A / B / C»."""
    groups = parse_prefix_groups(entry)
    names = [nomen.get(g, f"gb{g}") for g in groups]
    if len(names) == 1:
        return names[0]
    return "одна из: " + " / ".join(names)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--version-date", type=str)
    p.add_argument("--version-id", type=int)
    p.add_argument("--output", type=str)
    args = p.parse_args()

    client = get_clickhouse_client()
    version = resolve_version(client, args.version_date, args.version_id)
    md_slice = resolve_latest_md_slice(client)
    md_version = VersionInfo(md_slice.version_date, md_slice.version_id)
    print(f"📅 heli_pandas {version.version_date} v{version.version_id}; "
          f"md_components {md_version.version_date} v{md_version.version_id}")

    nomen = load_nomenclatures(client, md_version)
    rows = fetch_aggregations(client, version, md_version)
    print(f"OPS-бортов (status_id=2): {len(rows)}")

    all_rows = []
    detail_rows = []
    for r in rows:
        board_type = "Ми-8" if (r.ac_type_mask or 0) & 32 else "Ми-17"
        missing = []
        deficit_units = 0
        for entry in r.shortage_groups:
            installed, required = parse_shortage(entry)
            gap = required - installed
            if gap > 0:  # только реальный недобор, не «лишние»
                nomenclature = render_nomenclature(entry, nomen)
                deficit_units += gap
                missing.append(f"{nomenclature}×{gap}")
                detail_rows.append({
                    "acn": r.aircraft_number,
                    "type": board_type,
                    "variant": r.variant or "",
                    "nomenclature": nomenclature,
                    "installed": installed,
                    "required": required,
                    "deficit": gap,
                })
        all_rows.append({
            "acn": r.aircraft_number,
            "type": board_type,
            "variant": r.variant or "",
            "mfg_date": r.mfg_date or "",
            "installed_total": r.total_components,
            "required": r.required_components,
            "deficit_positions": len(missing),
            "deficit_units": deficit_units,
            "has_deficit": "да" if deficit_units > 0 else "нет",
            "missing_nomenclatures": "; ".join(missing),
        })

    all_boards = (pd.DataFrame(all_rows)
                  .sort_values(["deficit_units", "acn"], ascending=[False, True]))
    boards = all_boards[all_boards["deficit_units"] > 0].copy()
    details = pd.DataFrame(detail_rows) if detail_rows else pd.DataFrame()

    if not boards.empty:
        by_nomen = (details.groupby("nomenclature", as_index=False)
                    .agg(boards=("acn", "nunique"), deficit_units=("deficit", "sum"))
                    .sort_values("deficit_units", ascending=False))
        print(f"\n=== OPS-борты с недокомплектом агрегатов: {len(boards)} из {len(rows)} ===")
        print(boards[["acn", "type", "variant", "installed_total", "required",
                      "deficit_positions", "deficit_units", "missing_nomenclatures"]]
              .to_string(index=False, max_colwidth=90))
        print(f"\nИтого недокомплект (агрегато-позиций): {int(boards['deficit_units'].sum())}")
        print("\n=== Недостающие номенклатуры (сводка) ===")
        print(by_nomen.to_string(index=False, max_colwidth=60))
    else:
        by_nomen = pd.DataFrame()
        print("\nБортов с недокомплектом не найдено.")

    out = Path(args.output) if args.output else (
        code_root.parent / "output" / f"engine_completeness_{version.version_date}"
        / f"OPS_Aggregate_Deficit_day0_{version.version_date}_v{version.version_id}.xlsx")
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        all_boards.to_excel(w, sheet_name="all_ops_boards", index=False)
        (boards if not boards.empty else pd.DataFrame(columns=["acn"])).to_excel(
            w, sheet_name="deficit_by_board", index=False)
        (details if not details.empty else pd.DataFrame(columns=["acn"])).to_excel(
            w, sheet_name="deficit_detail", index=False)
        (by_nomen if not by_nomen.empty else pd.DataFrame(columns=["nomenclature"])).to_excel(
            w, sheet_name="deficit_by_nomenclature", index=False)
    print(f"\n✅ Сохранено: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
