#!/usr/bin/env python3
"""
Day0-анализ комплектности планеров по двигателям + доступный пул исправных двигателей.

Только аналитический слой (read-only). heli_pandas/status_id в ETL НЕ меняем.

Правила (согласованы с Алексеем):
- Двигатель планера по типу: Ми-8 (group_by=1) -> ТВ2-117 (group_by=3);
  Ми-17 (group_by=2) -> ТВ3-117 (group_by=4). Норма из md_components (=2).
- Доступный пул = незакреплённые исправные двигатели (status_id=3, aircraft_number=0),
  МИНУС локация HELISUR и все НЕ-RA (иностранные) регистрации — по day0-снапшоту DWH
  (reports.amos_heli_rotables_components_status @ report_date=version_date).
- Агрегаты на планерах в статусе Неактивно (status_id=1) трактуем как НЕИСПРАВНЫЕ
  (даже при condition=ИСПРАВНЫЙ), поэтому в пул/комплектность они не идут.
- Комплектность учитывает состояние ремонта: планеры status_id=4 показываем отдельно.
- По недоукомплектованным планерам выгружаем наработки двигателей (sne/ppr + остатки),
  чтобы видеть случаи «пригнали в ремонт с малым остатком ресурса, не в ноль».
"""

import argparse
import re
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

code_root = Path(__file__).resolve().parents[1]
sys.path.append(str(code_root / "utils"))
sys.path.append(str(code_root))
from config_loader import get_clickhouse_client  # type: ignore
from extract.overhaul_status_processor import load_dict_status_flat  # type: ignore
from utils.export_engine_repair_dwh import _dwh_connect  # type: ignore

STATUS_LABELS: dict[int, str] = {
    **load_dict_status_flat(),
    0: "Не определён",
    7: "Неисправен",
}
# планер -> группа его двигателей
PLANER_ENGINE_GROUP = {1: 3, 2: 4}
INACTIVE = 1
OPS = 2
REPAIR = 4
SERVICEABLE_ENG = {2, 3}  # эксплуатация/исправен


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--version-date", type=str, help="Дата версии day0 (YYYY-MM-DD)")
    p.add_argument("--version-id", type=int, help="ID версии (UInt8)")
    p.add_argument("--output", type=str, help="Путь к .xlsx")
    return p.parse_args()


def resolve_version(client, version_date, version_id):
    if version_date and version_id is not None:
        return datetime.strptime(version_date, "%Y-%m-%d").date(), int(version_id)
    row = client.execute(
        "SELECT version_date, version_id FROM heli_pandas "
        "ORDER BY version_date DESC, version_id DESC LIMIT 1"
    )
    if not row:
        raise RuntimeError("heli_pandas пуста")
    return row[0][0], int(row[0][1])


def label(sid: int) -> str:
    return STATUS_LABELS.get(int(sid), f"Статус {int(sid)}")


def is_foreign_location(loc: str) -> bool:
    """HELISUR (Перу) или иностранный бортовой номер (не RA-)."""
    x = str(loc).strip().upper()
    if "HELISUR" in x:
        return True
    m = re.match(r"^([A-Z]{2})-", x)  # бортовой номер вида XX-YYYY
    if m and not x.startswith("RA-"):
        return True
    return False


def load_engine_norms(client) -> dict[int, int]:
    rows = client.execute(
        "SELECT group_by, max(comp_number) FROM md_components "
        "WHERE group_by IN (3,4) GROUP BY group_by"
    )
    return {int(g): int(n) for g, n in rows}


def load_planers(client, vd: date, vid: int) -> pd.DataFrame:
    rows = client.execute(
        """
        SELECT toUInt32(aircraft_number) AS acn, toUInt8(group_by) AS pgb,
               toUInt8(ifNull(status_id,0)) AS status_id, partno, mfg_date
        FROM heli_pandas
        WHERE version_date=%(d)s AND version_id=%(i)s
          AND group_by IN (1,2) AND aircraft_number != 0
        """,
        {"d": vd, "i": vid},
    )
    return pd.DataFrame(rows, columns=["acn", "pgb", "status_id", "partno", "mfg_date"])


def load_engines(client, vd: date, vid: int) -> pd.DataFrame:
    rows = client.execute(
        """
        SELECT toUInt32(ifNull(aircraft_number,0)) AS acn, toUInt8(group_by) AS egb,
               toUInt8(ifNull(status_id,0)) AS status_id, serialno,
               toUInt32(ifNull(psn,0)) AS psn, condition,
               toUInt32(ifNull(sne,0)) AS sne, toUInt32(ifNull(ppr,0)) AS ppr,
               toUInt32(ifNull(oh,0)) AS oh, toUInt32(ifNull(ll,0)) AS ll
        FROM heli_pandas
        WHERE version_date=%(d)s AND version_id=%(i)s AND group_by IN (3,4)
        """,
        {"d": vd, "i": vid},
    )
    cols = ["acn", "egb", "status_id", "serialno", "psn", "condition",
            "sne", "ppr", "oh", "ll"]
    return pd.DataFrame(rows, columns=cols)


def load_dwh_locations(vd: date, psns: list[int]) -> pd.DataFrame:
    if not psns:
        return pd.DataFrame(columns=["psn", "location", "condition"])
    d = _dwh_connect()
    df = d.query_df(
        """
        SELECT psn, location, condition
        FROM reports.amos_heli_rotables_components_status
        WHERE report_date=%(rd)s AND psn IN %(p)s
        """,
        parameters={"rd": vd.isoformat(), "p": tuple(int(x) for x in psns)},
    )
    df["psn"] = pd.to_numeric(df["psn"], errors="coerce").fillna(0).astype("int64")
    return df


def build_pool(engines: pd.DataFrame, vd: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Пул незакреплённых исправных двигателей (status_id=3, acn=0) + day0 DWH-локация."""
    pool = engines[(engines["status_id"] == 3) & (engines["acn"] == 0)].copy()
    dwh = load_dwh_locations(vd, pool["psn"].tolist())
    loc_map = dwh.set_index("psn")["location"].to_dict()
    pool["dwh_location"] = pool["psn"].map(loc_map).fillna("")
    pool["is_foreign"] = pool["dwh_location"].apply(is_foreign_location)
    pool["remaining_to_ll"] = (pool["ll"].astype("int64") - pool["sne"].astype("int64"))
    pool["remaining_to_oh"] = (pool["oh"].astype("int64") - pool["ppr"].astype("int64"))
    pool["engine_type"] = pool["egb"].map({3: "ТВ2-117 (Ми-8)", 4: "ТВ3-117 (Ми-17)"})
    keep = pool[~pool["is_foreign"]].copy()
    removed = pool[pool["is_foreign"]].copy()
    cols = ["egb", "engine_type", "serialno", "psn", "dwh_location", "condition",
            "sne", "ppr", "oh", "ll", "remaining_to_ll", "remaining_to_oh"]
    return keep[cols].sort_values(["egb", "serialno"]), removed[cols + ["is_foreign"]]


def build_completeness(planers: pd.DataFrame, engines: pd.DataFrame,
                       norms: dict[int, int]) -> pd.DataFrame:
    onboard = engines[engines["acn"] != 0].copy()
    pl_status = planers.set_index("acn")["status_id"].to_dict()
    onboard["planer_status"] = onboard["acn"].map(pl_status).fillna(0).astype(int)
    # Правило 4: на Неактивно-планере все двигатели неисправны
    onboard["eng_serviceable"] = (
        onboard["status_id"].isin(SERVICEABLE_ENG)
        & (onboard["planer_status"] != INACTIVE)
    )

    rows = []
    for _, p in planers.iterrows():
        acn = int(p["acn"])
        pgb = int(p["pgb"])
        eng_group = PLANER_ENGINE_GROUP[pgb]
        req = int(norms.get(eng_group, 2))
        mine = onboard[(onboard["acn"] == acn) & (onboard["egb"] == eng_group)]
        svc = int(mine["eng_serviceable"].sum())
        installed = int(len(mine))
        delta = svc - req
        st = int(p["status_id"])
        rows.append({
            "acn": acn,
            "type": "Ми-8" if pgb == 1 else "Ми-17",
            "planer_status": st,
            "planer_status_label": label(st),
            "engine_group": eng_group,
            "installed_engines": installed,
            "serviceable_engines": svc,
            "required": req,
            "delta": delta,
            "ops_deficit": bool(st == OPS and delta < 0),
            "in_repair": bool(st == REPAIR),
            "mfg_date": p["mfg_date"],
        })
    return pd.DataFrame(rows).sort_values(
        ["ops_deficit", "in_repair", "planer_status", "type", "acn"],
        ascending=[False, False, True, True, True],
    )


def build_repair_engines(engines: pd.DataFrame) -> pd.DataFrame:
    """Двигатели в ремонте (status_id=4): наработки + остаток ресурса.

    Отвечает на пункт 7 — «ресурс в ноль не выбивают, с малым остатком в ремонт».
    Сортировка по остатку до капремонта (ppr->oh), чтобы наверху были снятые «рано».
    """
    r = engines[engines["status_id"] == REPAIR].copy()
    r["engine_type"] = r["egb"].map({3: "ТВ2-117 (Ми-8)", 4: "ТВ3-117 (Ми-17)"})
    r["remaining_to_ll"] = r["ll"].astype("int64") - r["sne"].astype("int64")
    r["remaining_to_oh"] = r["oh"].astype("int64") - r["ppr"].astype("int64")
    cols = ["egb", "engine_type", "serialno", "psn", "condition",
            "sne", "ppr", "oh", "ll", "remaining_to_ll", "remaining_to_oh"]
    return r[cols].sort_values(["remaining_to_oh", "remaining_to_ll"])


def build_summary(comp: pd.DataFrame) -> pd.DataFrame:
    g = comp.groupby(
        ["planer_status", "planer_status_label", "type", "serviceable_engines"],
        dropna=False,
    ).size().reset_index(name="boards")
    return g.sort_values(["planer_status", "type", "serviceable_engines"])


def main() -> int:
    args = parse_args()
    client = get_clickhouse_client()
    vd, vid = resolve_version(client, args.version_date, args.version_id)
    print(f"📅 day0 = {vd} (version_id={vid})")

    norms = load_engine_norms(client)
    planers = load_planers(client, vd, vid)
    engines = load_engines(client, vd, vid)
    print(f"планеров: {len(planers)}, двигателей (gb3/4): {len(engines)}, нормы: {norms}")

    pool_keep, pool_removed = build_pool(engines, vd)
    comp = build_completeness(planers, engines, norms)
    repair_eng = build_repair_engines(engines)
    summary = build_summary(comp)

    print("\n=== Доступный пул исправных двигателей (после фильтра) ===")
    print(pool_keep.groupby("engine_type").size().to_string())
    print(f"Убрано (HELISUR/иностранные): {len(pool_removed)}")
    print(pool_removed[["engine_type", "serialno", "dwh_location"]].to_string(index=False))

    print("\n=== OPS-планеры с дефицитом двигателей ===")
    dfc = comp[comp["ops_deficit"]]
    print(dfc[["acn", "type", "serviceable_engines", "required", "delta"]].to_string(index=False)
          if len(dfc) else "нет")

    print(f"\n=== Двигатели в ремонте (status_id=4): {len(repair_eng)} шт ===")
    print("Топ-8 по наименьшему остатку до капремонта (сняты «рано»):")
    print(repair_eng.head(8)[["engine_type", "serialno", "sne", "ppr",
                              "remaining_to_oh", "remaining_to_ll"]].to_string(index=False))

    if args.output:
        out = Path(args.output)
    else:
        out_dir = code_root.parent / "output" / f"engine_completeness_{vd}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"Engine_Completeness_day0_{vd}_v{vid}.xlsx"
    out.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(out, engine="openpyxl") as w:
        pool_keep.to_excel(w, sheet_name="pool_available", index=False)
        pool_removed.to_excel(w, sheet_name="pool_removed", index=False)
        comp.to_excel(w, sheet_name="planer_completeness", index=False)
        repair_eng.to_excel(w, sheet_name="engines_in_repair_usage", index=False)
        summary.to_excel(w, sheet_name="summary", index=False)

    print(f"\n✅ Сохранено: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
