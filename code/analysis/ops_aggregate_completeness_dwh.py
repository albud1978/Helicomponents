#!/usr/bin/env python3
"""
DWH-native аналитика комплектности агрегатов по бортам программы (read-only из DWH,
опционально материализация результата в ClickHouse для BI).

Вопрос: сколько бортов должно быть по программе, сколько по факту укомплектованы
ИСПРАВНЫМИ агрегатами, какие имеют дефицит и каких именно номенклатур.

Правила (воспроизводят ETL heli_pandas + доменную логику комплектности):
- aircraft_number = 5 цифр из location `RA-XXXXX` (aircraft_number_processor).
- group_by = md_components.group_by по ключу partseqno_i (group_by_enricher).
- ac_type_mask планера из ac_typ program_ac / partno витрины (Ми-8Т=32, Ми-17=64).
- «По программе» = все борта program_ac(date) (плановый реестр ЮТ-ВУ Ми-8/Ми-17).
- Ремонт = борт в активном капремонте status_overhaul(date) — помечается флагом,
  но остаётся внутри программы.
- Комплектность: переиспользуем доменно-валидированную логику
  `heli_pandas_ops_other_groups.fetch_aggregations` (альтернативы/опционал) через
  монкейпатч источников данных.

Семантика «исправных» (по требованию): позиция закрыта только если смонтированный
агрегат ИСПРАВЕН (condition='ИСПРАВНЫЙ'); неисправный/донор на борту = дефицит.

Режим --validate-against сверяет ЛОГИКУ с heli_pandas (в семантике heli_pandas:
любой condition, роестр status_id=2) — доказательство эквивалентности схлопывания.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, Optional, Set, Tuple

import pandas as pd

code_root = Path(__file__).resolve().parents[1]
sys.path.append(str(code_root / "utils"))
sys.path.append(str(code_root))

import heli_pandas_ops_other_groups as og  # type: ignore
from heli_pandas_ops_other_groups import VersionInfo, _extract_variant  # type: ignore
from config_loader import auto_load_env_file, get_clickhouse_client  # type: ignore
from static_data_resolver import resolve_latest_md_slice  # type: ignore
from dwh_golden_replay_export import (  # type: ignore
    dwh_client,
    program_ac_dataframe,
    status_components_dataframe,
    status_overhaul_dataframe,
)
from ops_aggregate_completeness_day0 import (  # type: ignore
    load_nomenclatures,
    parse_shortage,
    render_nomenclature,
)

SERVICEABLE = "ИСПРАВНЫЙ"

# partno планера -> битовая маска типа ВС (enrich_heli_pandas + факт heli_pandas)
PLANER_MASK = {
    "МИ-8Т": 32, "МИ-8П": 32, "МИ-8ПС": 32, "МИ-8ТП": 32,
    "МИ-8АМТ": 64, "МИ-8МТВ": 64,
}
PLANER_PARTNOS = set(PLANER_MASK)
# ac_typ program_ac (нормализованный) -> маска
ACTYP_MASK = {"Ми-8Т": 32, "Ми-17": 64}


def load_partseqno_to_group(client, md_version: VersionInfo) -> Dict[int, int]:
    """partseqno_i -> group_by из md_components (как group_by_enricher)."""
    rows = client.execute(
        """
        SELECT toUInt32(partseqno_i) AS partseqno_i, toUInt8(any(group_by)) AS gb
        FROM md_components
        WHERE version_date = %(vd)s AND version_id = %(vi)s
          AND partseqno_i IS NOT NULL AND group_by IS NOT NULL
        GROUP BY partseqno_i
        """,
        {"vd": md_version.version_date, "vi": md_version.version_id},
    )
    return {int(p): int(g) for p, g in rows if p and g}


def _fmt_mfg(value) -> Optional[str]:
    if value is None:
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.strftime("%Y-%m-%d")


def build_rosters(dwh, report_date: str) -> Tuple[Set[int], Set[int], Dict[int, str]]:
    """Возвращает (program_set, repair_set, type_map board->ac_typ)."""
    prog = program_ac_dataframe(dwh, report_date=report_date)
    program_set: Set[int] = set()
    type_map: Dict[int, str] = {}
    for _, r in prog.iterrows():
        reg = int(r["ac_registr"]) if pd.notna(r["ac_registr"]) else 0
        if reg:
            program_set.add(reg)
            type_map[reg] = str(r.get("ac_typ") or "").strip()

    ovh = status_overhaul_dataframe(dwh, report_date=report_date)
    rd = pd.Timestamp(report_date)
    repair: Set[int] = set()
    for _, r in ovh.iterrows():
        if str(r.get("status", "")).strip() == "Закрыто":
            continue
        s_start, a_start = r.get("sched_start_date"), r.get("act_start_date")
        if not (pd.notna(s_start) or pd.notna(a_start)):
            continue
        started = (pd.notna(s_start) and pd.Timestamp(s_start) < rd) or \
                  (pd.notna(a_start) and pd.Timestamp(a_start) < rd)
        if started:
            reg = int(r["ac_registr"]) if pd.notna(r["ac_registr"]) else 0
            if reg:
                repair.add(reg)
    return program_set, repair, type_map


def build_from_dwh(
    dwh,
    report_date: str,
    p2g: Dict[int, int],
    roster: Set[int],
    *,
    serviceable_only: bool,
    type_map: Optional[Dict[int, str]] = None,
) -> Tuple[Dict[int, Tuple[Optional[int], Optional[str], Optional[str]]], Dict[int, Dict[int, int]]]:
    """Строит plane_meta и counts из витрины DWH на report_date, ограничивая роестром.

    serviceable_only=True -> в counts попадают только condition='ИСПРАВНЫЙ'.
    type_map (board->ac_typ) используется как fallback маски для бортов без planer-строки.
    """
    df = status_components_dataframe(dwh, report_date=report_date)
    # NB: не дедуплицируем — heli_pandas сохраняет дубли строк витрины (SCD-артефакты).
    df["partno"] = df["partno"].astype(str).str.strip()

    loc = df["location"].fillna("").astype(str).str.strip()
    ra = loc.str.match(r"^RA-\d{5}$")
    df["aircraft_number"] = 0
    df.loc[ra, "aircraft_number"] = loc[ra].str[3:].astype(int)
    df["group_by"] = df["partseqno_i"].map(lambda v: p2g.get(int(v), 0) if pd.notna(v) else 0)

    # plane_meta: приоритет — planer-строка витрины (partno->маска, mfg, вариант)
    planers = df[df["partno"].isin(PLANER_PARTNOS) & df["aircraft_number"].isin(roster)]
    plane_meta: Dict[int, Tuple[Optional[int], Optional[str], Optional[str]]] = {}
    for _, r in planers.iterrows():
        board = int(r["aircraft_number"])
        if board in plane_meta:
            continue
        plane_meta[board] = (PLANER_MASK.get(r["partno"], 0), _fmt_mfg(r.get("mfg_date")), r["partno"])
    # борта роестра без planer-строки — добираем из program_ac type_map
    for board in roster:
        if board not in plane_meta:
            mask = ACTYP_MASK.get((type_map or {}).get(board, ""), 0)
            plane_meta[board] = (mask, None, None)

    present = set(plane_meta)
    aggs = df[(df["group_by"] > 2) & (df["aircraft_number"].isin(present))]
    if serviceable_only:
        aggs = aggs[aggs["condition"].astype(str).str.strip() == SERVICEABLE]
    counts: Dict[int, Dict[int, int]] = {}
    for board, group in zip(aggs["aircraft_number"].astype(int), aggs["group_by"].astype(int)):
        d = counts.setdefault(board, {})
        d[group] = d.get(group, 0) + 1
    return plane_meta, counts


def run_aggregations(project_client, md_version: VersionInfo, plane_meta, counts):
    """Переиспользует доменную логику fetch_aggregations через монкейпатч источников."""
    og.fetch_plane_meta = lambda client, v: plane_meta  # noqa: E731
    og.fetch_group_counts = lambda client, v: counts  # noqa: E731
    return og.fetch_aggregations(project_client, VersionInfo("dwh", 0), md_version)


def rows_to_frames(rows, nomen, report_date: str, repair_set: Set[int]):
    all_rows, detail_rows = [], []
    for r in rows:
        board_type = "Ми-8" if (r.ac_type_mask or 0) & 32 else "Ми-17"
        status = "Ремонт" if r.aircraft_number in repair_set else "Эксплуатация"
        variant = r.variant or _extract_variant(r.partno) or ""
        missing, deficit_units = [], 0
        for entry in r.shortage_groups:
            installed, required = parse_shortage(entry)
            gap = required - installed
            if gap > 0:
                nomenclature = render_nomenclature(entry, nomen)
                deficit_units += gap
                missing.append(f"{nomenclature}×{gap}")
                detail_rows.append({
                    "report_date": report_date, "acn": r.aircraft_number, "ac_type": board_type,
                    "variant": variant, "status": status, "nomenclature": nomenclature,
                    "installed": installed, "required": required, "deficit": gap,
                })
        all_rows.append({
            "report_date": report_date, "acn": r.aircraft_number, "ac_type": board_type,
            "variant": variant, "status": status, "mfg_date": r.mfg_date or "",
            "required": r.required_components, "installed_serviceable": r.total_components,
            "deficit_positions": len(missing), "deficit_units": deficit_units,
            "has_deficit": 1 if deficit_units > 0 else 0, "missing_nomenclatures": "; ".join(missing),
        })
    board_df = pd.DataFrame(all_rows).sort_values(["deficit_units", "acn"], ascending=[False, True])
    detail_df = pd.DataFrame(detail_rows) if detail_rows else pd.DataFrame(
        columns=["report_date", "acn", "ac_type", "variant", "status", "nomenclature",
                 "installed", "required", "deficit"])
    return board_df, detail_df


BOARD_TABLE = "bi_ops_completeness_board"
DETAIL_TABLE = "bi_ops_completeness_detail"


def materialize(client, board_df: pd.DataFrame, detail_df: pd.DataFrame, report_date: str) -> None:
    """Идемпотентная запись среза report_date в ClickHouse (default) для Superset."""
    client.execute(f"""
        CREATE TABLE IF NOT EXISTS {BOARD_TABLE} (
            report_date Date, acn UInt32, ac_type String, variant String, status String,
            mfg_date String, required UInt16, installed_serviceable UInt16,
            deficit_positions UInt16, deficit_units UInt16, has_deficit UInt8,
            missing_nomenclatures String
        ) ENGINE = MergeTree ORDER BY (report_date, acn)
    """)
    client.execute(f"""
        CREATE TABLE IF NOT EXISTS {DETAIL_TABLE} (
            report_date Date, acn UInt32, ac_type String, variant String, status String,
            nomenclature String, installed UInt16, required UInt16, deficit UInt16
        ) ENGINE = MergeTree ORDER BY (report_date, acn, nomenclature)
    """)
    # идемпотентность: удаляем предыдущий срез этой даты (ALTER DELETE — не DROP/TRUNCATE)
    client.execute(f"ALTER TABLE {BOARD_TABLE} DELETE WHERE report_date = %(d)s", {"d": report_date})
    client.execute(f"ALTER TABLE {DETAIL_TABLE} DELETE WHERE report_date = %(d)s", {"d": report_date})

    b = board_df.copy()
    b["report_date"] = pd.to_datetime(b["report_date"]).dt.date
    client.execute(
        f"INSERT INTO {BOARD_TABLE} (report_date, acn, ac_type, variant, status, mfg_date, "
        f"required, installed_serviceable, deficit_positions, deficit_units, has_deficit, "
        f"missing_nomenclatures) VALUES",
        list(b[["report_date", "acn", "ac_type", "variant", "status", "mfg_date", "required",
                "installed_serviceable", "deficit_positions", "deficit_units", "has_deficit",
                "missing_nomenclatures"]].itertuples(index=False, name=None)),
    )
    if not detail_df.empty:
        d = detail_df.copy()
        d["report_date"] = pd.to_datetime(d["report_date"]).dt.date
        client.execute(
            f"INSERT INTO {DETAIL_TABLE} (report_date, acn, ac_type, variant, status, "
            f"nomenclature, installed, required, deficit) VALUES",
            list(d[["report_date", "acn", "ac_type", "variant", "status", "nomenclature",
                    "installed", "required", "deficit"]].itertuples(index=False, name=None)),
        )
    print(f"✅ Материализовано в ClickHouse default.{BOARD_TABLE} / default.{DETAIL_TABLE} "
          f"(report_date={report_date}): бортов {len(b)}, строк дефицита {len(detail_df)}")


def validate(project_client, dwh, md_version, hp_version: VersionInfo) -> int:
    """Сверка ЛОГИКИ с heli_pandas на общем роестре, семантика heli_pandas (любой condition)."""
    p2g = load_partseqno_to_group(project_client, md_version)
    hp_rows = og.fetch_aggregations(project_client, hp_version, md_version)
    hp = {r.aircraft_number: r for r in hp_rows}
    hp_boards = set(hp)
    print(f"[validate] heli_pandas {hp_version.version_date}: OPS-бортов {len(hp_boards)}")

    plane_meta, counts = build_from_dwh(
        dwh, str(hp_version.version_date), p2g, hp_boards, serviceable_only=False)
    dwh_rows = run_aggregations(project_client, md_version, plane_meta, counts)
    dw = {r.aircraft_number: r for r in dwh_rows}

    miss = hp_boards - set(dw)
    if miss:
        print(f"[validate] ⚠️ борта heli_pandas без planer-строки в витрине: {sorted(miss)}")
    mask_m = tot_m = def_m = 0
    for b in sorted(hp_boards & set(dw)):
        h, d = hp[b], dw[b]
        if (h.ac_type_mask or 0) != (d.ac_type_mask or 0):
            mask_m += 1
        if h.total_components != d.total_components:
            tot_m += 1
        hd = sum(max(0, parse_shortage(e)[1] - parse_shortage(e)[0]) for e in h.shortage_groups)
        dd = sum(max(0, parse_shortage(e)[1] - parse_shortage(e)[0]) for e in d.shortage_groups)
        if hd != dd:
            def_m += 1
    print(f"[validate] расхождения — mask:{mask_m}, total_components:{tot_m}, deficit_units:{def_m}")
    ok = not (mask_m or tot_m or def_m or miss)
    print(f"[validate] {'✅ ПОЛНОЕ СОВПАДЕНИЕ логики' if ok else '⚠️ ЕСТЬ РАСХОЖДЕНИЯ'}")
    return 0 if ok else 2


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--report-date", default="2026-07-14")
    p.add_argument("--output", type=str)
    p.add_argument("--materialize", action="store_true", help="Записать срез в ClickHouse для BI")
    p.add_argument("--validate-against", type=str)
    p.add_argument("--validate-version-id", type=int, default=1)
    args = p.parse_args()

    auto_load_env_file()
    project_client = get_clickhouse_client()
    md_slice = resolve_latest_md_slice(project_client)
    md_version = VersionInfo(md_slice.version_date, md_slice.version_id)
    dwh = dwh_client()

    if args.validate_against:
        return validate(project_client, dwh, md_version, VersionInfo(args.validate_against, args.validate_version_id))

    report_date = args.report_date.strip()
    print(f"📅 DWH витрина report_date={report_date}; md_components {md_version.version_date} v{md_version.version_id}")

    p2g = load_partseqno_to_group(project_client, md_version)
    program_set, repair_set, type_map = build_rosters(dwh, report_date)
    print(f"По программе (program_ac): {len(program_set)} | в ремонте (status_overhaul): "
          f"{len(program_set & repair_set)}")

    plane_meta, counts = build_from_dwh(
        dwh, report_date, p2g, program_set, serviceable_only=True, type_map=type_map)
    nomen = load_nomenclatures(project_client, md_version)
    rows = run_aggregations(project_client, md_version, plane_meta, counts)
    board_df, detail_df = rows_to_frames(rows, nomen, report_date, repair_set)

    n_prog = len(board_df)
    n_def = int(board_df["has_deficit"].sum())
    n_repair = int((board_df["status"] == "Ремонт").sum())
    print(f"\n=== Программа {report_date} ===")
    print(f"Всего по программе: {n_prog} (эксплуатация {n_prog - n_repair}, ремонт {n_repair})")
    print(f"Укомплектованы (исправный полный набор): {n_prog - n_def}")
    print(f"С дефицитом исправных агрегатов: {n_def}")
    if n_def:
        dd = board_df[board_df["has_deficit"] == 1]
        print(f"\n=== Борты с дефицитом ({n_def}) ===")
        print(dd[["acn", "ac_type", "variant", "status", "installed_serviceable", "required",
                  "deficit_positions", "deficit_units", "missing_nomenclatures"]]
              .to_string(index=False, max_colwidth=80))
        by_nomen = (detail_df.groupby("nomenclature", as_index=False)
                    .agg(boards=("acn", "nunique"), deficit_units=("deficit", "sum"))
                    .sort_values("deficit_units", ascending=False))
        print("\n=== Недостающие номенклатуры (сводка) ===")
        print(by_nomen.to_string(index=False, max_colwidth=60))

    if args.materialize:
        materialize(project_client, board_df, detail_df, report_date)

    out = Path(args.output) if args.output else (
        code_root.parent / "output" / f"ops_aggregate_completeness_dwh_{report_date}"
        / f"OPS_Aggregate_Completeness_DWH_{report_date}.xlsx")
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        board_df.to_excel(w, sheet_name="all_program_boards", index=False)
        board_df[board_df["has_deficit"] == 1].to_excel(w, sheet_name="deficit_by_board", index=False)
        detail_df.to_excel(w, sheet_name="deficit_detail", index=False)
    print(f"\n✅ Excel: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
