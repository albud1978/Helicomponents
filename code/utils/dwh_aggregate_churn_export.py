#!/usr/bin/env python3
"""
Сравнение состава агрегатов (md_components scope) между двумя срезами heli_pandas
+ история location/RA- из DWH reports.

Выход: Excel с листами entered, exited, planner_agg_entered, planner_agg_exited.
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

import pandas as pd

code_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(code_root))
sys.path.insert(0, str(code_root / "utils"))

from config_loader import get_clickhouse_client  # noqa: E402
from dwh_golden_replay_export import dwh_client  # noqa: E402

RA_RE = re.compile(r"^RA-(\d{5})$")


def parse_ra(location: str | None) -> int | None:
    if not location:
        return None
    s = str(location).strip()
    m = RA_RE.match(s)
    if m:
        return int(m.group(1))
    if s.startswith("RA-") and len(s) >= 8:
        tail = s[3:8]
        if tail.isdigit():
            return int(tail)
    return None


def load_md_group_by(client, md_version_date: str, md_version_id: int) -> dict[str, int]:
    rows = client.execute(
        f"""
        SELECT partno, group_by FROM md_components
        WHERE version_date = '{md_version_date}' AND version_id = {md_version_id}
          AND group_by > 2
        """
    )
    out: dict[str, int] = {}
    for p, gb in rows:
        out.setdefault(p, int(gb))
    return out


def load_aggregates_dwh(
    dwh, report_date: str, partnos: set[str], gb_map: dict[str, int]
) -> pd.DataFrame:
    if not partnos:
        raise RuntimeError("md_components: пустой список partno для агрегатов")
    in_list = ",".join("'" + p.replace("'", "''") + "'" for p in sorted(partnos))
    df = dwh.query_df(
        f"""
        SELECT psn, partno, serialno, mfg_date, location, owner, condition
        FROM reports.amos_heli_rotables_components_status
        WHERE report_date = toDate('{report_date}') AND partno IN ({in_list})
        """
    )
    if df.empty:
        return pd.DataFrame(
            columns=[
                "psn", "partno", "serialno", "group_by", "mfg_date",
                "aircraft_number_hp", "status_id", "location_hp",
            ]
        )
    df["group_by"] = df["partno"].map(gb_map)
    df = df[df["group_by"].notna()].copy()
    df["aircraft_number_hp"] = df["location"].map(parse_ra).fillna(0).astype(int)
    df["status_id"] = 0
    df["location_hp"] = df["location"]
    return df.drop_duplicates(subset=["psn", "partno"], keep="first")


def load_md_partnos(client, md_version_date: str, md_version_id: int) -> set[str]:
    return set(load_md_group_by(client, md_version_date, md_version_id).keys())


def load_aggregates(client, version_date: str, version_id: int, partnos: set[str]) -> pd.DataFrame:
    if not partnos:
        raise RuntimeError("md_components: пустой список partno для агрегатов")
    # partno filter via temp IN — список небольшой (~77)
    in_list = ",".join("'" + p.replace("'", "''") + "'" for p in sorted(partnos))
    rows = client.execute(
        f"""
        SELECT psn, partno, serialno, group_by, mfg_date, aircraft_number, status_id, location
        FROM heli_pandas
        WHERE version_date = '{version_date}' AND version_id = {version_id}
          AND group_by > 2 AND partno IN ({in_list})
        """
    )
    df = pd.DataFrame(
        rows,
        columns=[
            "psn",
            "partno",
            "serialno",
            "group_by",
            "mfg_date",
            "aircraft_number_hp",
            "status_id",
            "location_hp",
        ],
    )
    return df.drop_duplicates(subset=["psn", "partno"], keep="first")


def load_planner_mfg(client, version_date: str, version_id: int) -> dict[int, date | None]:
    rows = client.execute(
        f"""
        SELECT aircraft_number, any(mfg_date) AS mfg_date
        FROM heli_pandas
        WHERE version_date = '{version_date}' AND version_id = {version_id}
          AND group_by IN (1, 2) AND aircraft_number > 0
        GROUP BY aircraft_number
        """
    )
    return {int(ac): mfg for ac, mfg in rows}


def fetch_dwh_history(
    dwh,
    keys: list[tuple[int, str]],
    *,
    period_from: str | None = None,
    period_to: str | None = None,
) -> pd.DataFrame:
    if not keys:
        return pd.DataFrame(columns=["psn", "partno", "report_date", "location", "owner", "condition"])
    date_filter = ""
    if period_from and period_to:
        date_filter = f" AND report_date >= toDate('{period_from}') AND report_date <= toDate('{period_to}')"
    # batch IN via tuple list — chunk if huge
    chunks = []
    chunk_size = 500
    for i in range(0, len(keys), chunk_size):
        batch = keys[i : i + chunk_size]
        tuples = ",".join(f"({psn}, '{partno.replace(chr(39), chr(39)+chr(39))}')" for psn, partno in batch)
        q = f"""
        SELECT psn, partno, report_date, location, owner, condition
        FROM reports.amos_heli_rotables_components_status
        WHERE (psn, partno) IN ({tuples}){date_filter}
        ORDER BY psn, partno, report_date
        """
        chunks.append(dwh.query_df(q))
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def summarize_board_history(hist: pd.DataFrame, planner_mfg: dict[int, date | None]) -> dict:
    if hist.empty:
        return {
            "dwh_first": None,
            "dwh_last": None,
            "ever_on_planner": False,
            "planners": "",
            "planner_mfg_dates": "",
            "board_dates": "",
        }
    dwh_first = hist["report_date"].min()
    dwh_last = hist["report_date"].max()
    board_rows = []
    for _, row in hist.iterrows():
        ac = parse_ra(row["location"])
        if ac:
            board_rows.append((row["report_date"], ac))
    ever = bool(board_rows)
    if not ever:
        return {
            "dwh_first": dwh_first,
            "dwh_last": dwh_last,
            "ever_on_planner": False,
            "planners": "",
            "planner_mfg_dates": "",
            "board_dates": "",
        }
    by_ac: dict[int, list] = defaultdict(list)
    for rd, ac in board_rows:
        by_ac[ac].append(rd)
    planners = sorted(by_ac)
    mfg_parts = []
    date_parts = []
    for ac in planners:
        mfg = planner_mfg.get(ac)
        mfg_parts.append(f"{ac}:{mfg if mfg else '?'}")
        ds = by_ac[ac]
        date_parts.append(f"{ac}:[{min(ds)}..{max(ds)}]")
    return {
        "dwh_first": dwh_first,
        "dwh_last": dwh_last,
        "ever_on_planner": True,
        "planners": "; ".join(str(p) for p in planners),
        "planner_mfg_dates": "; ".join(mfg_parts),
        "board_dates": "; ".join(date_parts),
    }


def build_detail_rows(
    side: str,
    df_slice: pd.DataFrame,
    hist_all: pd.DataFrame,
    planner_mfg: dict[int, date | None],
    *,
    snapshot_entry: str | None = None,
    snapshot_exit: str | None = None,
) -> pd.DataFrame:
    rows = []
    for _, r in df_slice.iterrows():
        psn, partno = int(r["psn"]), r["partno"]
        h = hist_all[(hist_all["psn"] == psn) & (hist_all["partno"] == partno)]
        s = summarize_board_history(h, planner_mfg)
        entry_date = snapshot_entry if side == "entered" and snapshot_entry else (
            s["dwh_first"] if side == "entered" else None
        )
        exit_date = snapshot_exit if side == "exited" and snapshot_exit else (
            s["dwh_last"] if side == "exited" else None
        )
        rows.append(
            {
                "side": side,
                "psn": psn,
                "partno": partno,
                "serialno": r["serialno"],
                "group_by": int(r["group_by"]),
                "mfg_date": r["mfg_date"],
                "dwh_entry_date": entry_date,
                "dwh_exit_date": exit_date,
                "ever_on_planner": s["ever_on_planner"],
                "planners": s["planners"],
                "planner_mfg_dates": s["planner_mfg_dates"],
                "board_periods": s["board_dates"],
                "location_hp": r.get("location_hp"),
                "aircraft_number_hp": int(r["aircraft_number_hp"] or 0),
            }
        )
    return pd.DataFrame(rows)


def planner_aggregation(detail: pd.DataFrame, side: str) -> pd.DataFrame:
    """Агрегация по планеру, если >=2 агрегата ever_on_planner с одним бортом."""
    if detail.empty or "ever_on_planner" not in detail.columns:
        return pd.DataFrame(
            columns=[
                "side",
                "aircraft_number",
                "aggregate_count",
                "psn_list",
                "partno_list",
                "group_by_list",
            ]
        )
    agg_rows = []
    for planner_str in detail.loc[detail["ever_on_planner"], "planners"]:
        for token in str(planner_str).split(";"):
            token = token.strip()
            if token.isdigit():
                yield_planner = int(token)
                # count in detail rows that include this planner
                pass

    planner_to_items: dict[int, list[dict]] = defaultdict(list)
    for _, row in detail.iterrows():
        if not row["ever_on_planner"]:
            continue
        for token in str(row["planners"]).split(";"):
            token = token.strip()
            if not token.isdigit():
                continue
            ac = int(token)
            planner_to_items[ac].append(row.to_dict())

    for ac, items in sorted(planner_to_items.items()):
        if len(items) < 2:
            continue
        agg_rows.append(
            {
                "side": side,
                "aircraft_number": ac,
                "aggregate_count": len(items),
                "psn_list": "; ".join(str(x["psn"]) for x in items),
                "partno_list": "; ".join(x["partno"][:30] for x in items),
                "group_by_list": "; ".join(str(x["group_by"]) for x in items),
            }
        )
    return pd.DataFrame(agg_rows)


def build_location_changed_rows(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    hist_all: pd.DataFrame,
    planner_mfg: dict[int, date | None],
    *,
    baseline_date: str,
    new_date: str,
) -> pd.DataFrame:
    """Ключи в обоих срезах, но location/condition изменились (суточный сигнал без churn)."""
    if old_df.empty or new_df.empty:
        return pd.DataFrame()
    merged = old_df.merge(
        new_df,
        on=["psn", "partno"],
        suffixes=("_baseline", "_new"),
        how="inner",
    )
    loc_base = merged["location_hp_baseline"].fillna("").astype(str)
    loc_new = merged["location_hp_new"].fillna("").astype(str)
    changed = merged[loc_base != loc_new].copy()
    if changed.empty:
        return pd.DataFrame()

    rows = []
    for _, r in changed.iterrows():
        psn, partno = int(r["psn"]), r["partno"]
        h = hist_all[(hist_all["psn"] == psn) & (hist_all["partno"] == partno)]
        s = summarize_board_history(h, planner_mfg)
        rows.append(
            {
                "psn": psn,
                "partno": partno,
                "serialno": r.get("serialno_new") or r.get("serialno_baseline"),
                "group_by": int(r["group_by_new"]),
                "mfg_date": r.get("mfg_date_new") or r.get("mfg_date_baseline"),
                f"location@{baseline_date}": r["location_hp_baseline"],
                f"location@{new_date}": r["location_hp_new"],
                f"aircraft_number@{baseline_date}": int(r["aircraft_number_hp_baseline"] or 0),
                f"aircraft_number@{new_date}": int(r["aircraft_number_hp_new"] or 0),
                "ever_on_planner": s["ever_on_planner"],
                "planners": s["planners"],
                "planner_mfg_dates": s["planner_mfg_dates"],
                "board_periods": s["board_dates"],
            }
        )
    return pd.DataFrame(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="DWH aggregate churn Excel export")
    parser.add_argument(
        "--source",
        choices=("heli_pandas", "dwh"),
        default="heli_pandas",
        help="heli_pandas: срезы Project CH; dwh: снимки reports.report_date",
    )
    parser.add_argument("--baseline-date", default="2026-04-08")
    parser.add_argument("--new-date", default="2026-06-12")
    parser.add_argument("--version-id", type=int, default=1)
    parser.add_argument("--md-date", default="2026-06-11")
    parser.add_argument(
        "--out",
        default="output/aggregate_churn_2026-04-08_vs_2026-06-12.xlsx",
    )
    args = parser.parse_args()

    pch = get_clickhouse_client()
    dwh = dwh_client()

    partnos = load_md_partnos(pch, args.md_date, args.version_id)
    gb_map = load_md_group_by(pch, args.md_date, args.version_id)
    print(f"md_components aggregate partnos: {len(partnos)}")

    period_from = min(args.baseline_date, args.new_date)
    period_to = max(args.baseline_date, args.new_date)

    if args.source == "dwh":
        old_df = load_aggregates_dwh(dwh, args.baseline_date, partnos, gb_map)
        new_df = load_aggregates_dwh(dwh, args.new_date, partnos, gb_map)
        planner_mfg = load_planner_mfg(pch, "2026-06-12", args.version_id)
    else:
        old_df = load_aggregates(pch, args.baseline_date, args.version_id, partnos)
        new_df = load_aggregates(pch, args.new_date, args.version_id, partnos)
        planner_mfg = load_planner_mfg(pch, args.new_date, args.version_id)
        for ac, mfg in load_planner_mfg(pch, args.baseline_date, args.version_id).items():
            planner_mfg.setdefault(ac, mfg)

    print(f"source {args.source}")
    print(f"baseline {args.baseline_date}: {len(old_df)} aggregates")
    print(f"new       {args.new_date}: {len(new_df)} aggregates")

    old_keys = set(zip(old_df["psn"].astype(int), old_df["partno"]))
    new_keys = set(zip(new_df["psn"].astype(int), new_df["partno"]))
    exited_keys = old_keys - new_keys
    entered_keys = new_keys - old_keys
    print(f"exited: {len(exited_keys)}, entered: {len(entered_keys)}")

    snap_entry = args.new_date if args.source == "dwh" else None
    snap_exit = args.baseline_date if args.source == "dwh" else None

    stable_keys = old_keys & new_keys
    location_changed_keys: list[tuple[int, str]] = []
    if args.source == "dwh" and stable_keys:
        old_loc = {
            (int(r["psn"]), r["partno"]): str(r.get("location_hp") or "")
            for _, r in old_df.iterrows()
        }
        new_loc = {
            (int(r["psn"]), r["partno"]): str(r.get("location_hp") or "")
            for _, r in new_df.iterrows()
        }
        location_changed_keys = [k for k in stable_keys if old_loc.get(k, "") != new_loc.get(k, "")]

    history_keys = list(exited_keys | entered_keys | set(location_changed_keys))
    print(
        f"Fetching DWH history for {len(history_keys)} keys "
        f"(churn {len(exited_keys | entered_keys)}, moved {len(location_changed_keys)}, "
        f"period {period_from}..{period_to})..."
    )
    hist_all = fetch_dwh_history(
        dwh, history_keys, period_from=period_from, period_to=period_to
    )
    print(f"DWH history rows: {len(hist_all)}")

    exited_df = old_df[old_df.apply(lambda r: (int(r["psn"]), r["partno"]) in exited_keys, axis=1)]
    entered_df = new_df[new_df.apply(lambda r: (int(r["psn"]), r["partno"]) in entered_keys, axis=1)]

    detail_exited = build_detail_rows(
        "exited", exited_df, hist_all, planner_mfg, snapshot_exit=snap_exit
    )
    detail_entered = build_detail_rows(
        "entered", entered_df, hist_all, planner_mfg, snapshot_entry=snap_entry
    )

    agg_exited = planner_aggregation(detail_exited, "exited")
    agg_entered = planner_aggregation(detail_entered, "entered")
    detail_moved = build_location_changed_rows(
        old_df,
        new_df,
        hist_all,
        planner_mfg,
        baseline_date=args.baseline_date,
        new_date=args.new_date,
    )
    agg_moved = planner_aggregation(detail_moved, "moved") if not detail_moved.empty else pd.DataFrame()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        meta = pd.DataFrame(
            [
                {"param": "source", "value": args.source},
                {"param": "baseline", "value": args.baseline_date},
                {"param": "new", "value": args.new_date},
                {"param": "md_components", "value": f"{args.md_date} v{args.version_id}"},
                {"param": "exited_count", "value": len(detail_exited)},
                {"param": "entered_count", "value": len(detail_entered)},
                {"param": "location_changed_count", "value": len(detail_moved)},
            ]
        )
        meta.to_excel(writer, sheet_name="meta", index=False)
        detail_entered.to_excel(writer, sheet_name="entered", index=False)
        detail_exited.to_excel(writer, sheet_name="exited", index=False)
        detail_moved.to_excel(writer, sheet_name="location_changed", index=False)
        agg_entered.to_excel(writer, sheet_name="planner_agg_entered", index=False)
        agg_exited.to_excel(writer, sheet_name="planner_agg_exited", index=False)
        agg_moved.to_excel(writer, sheet_name="planner_agg_moved", index=False)

    print(f"Written: {out_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
