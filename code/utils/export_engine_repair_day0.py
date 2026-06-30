#!/usr/bin/env python3
"""
Компактная выгрузка day0-состояния по группе 4 (двигатели) из heli_pandas.

Берёт срез simulation day0 (version_date/version_id) и выгружает по каждому
двигателю: status_id (+метка), target_date (ожидаемое окончание ремонта),
repair_days/repair_time, condition и идентификаторы/ресурсы для проверки.

Это ТРЕК A (sim day0): источник target_date/status_id — heli_pandas после
каскада классификации (heli_pandas_repair_status.py и т.д.), НЕ od_detail.

Read-only по ClickHouse. Версия по умолчанию — последняя в heli_pandas.
"""

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

code_root = Path(__file__).resolve().parents[1]
sys.path.append(str(code_root / "utils"))
sys.path.append(str(code_root))
from config_loader import get_clickhouse_client  # type: ignore
from extract.overhaul_status_processor import load_dict_status_flat  # type: ignore

GROUP_BY_ENGINE = 4
EMPTY_DATE = date(1970, 1, 1)

# SSoT названий 1..6: load_dict_status_flat() → dict_status_flat.
# 0 и 7 в словаре нет; 7 используется в модели (transitions_rules.json: unserviceable).
PROJECT_STATUS_LABELS: dict[int, str] = {
    **load_dict_status_flat(),
    0: "Не определён (до каскада status_id)",
    7: "Неисправен",
}

EXPORT_COLUMNS = [
    "serialno",
    "partno",
    "partseqno_i",
    "ac_typ",
    "condition",
    "status_id",
    "status_label",
    "target_date",
    "days_to_target",
    "repair_days",
    "repair_time",
    "removal_date",
    "sne",
    "oh",
    "ll",
    "owner",
    "location",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Выгрузка day0-состояния группы 4 (двигатели) из heli_pandas"
    )
    parser.add_argument("--version-date", type=str, help="Дата версии (YYYY-MM-DD)")
    parser.add_argument("--version-id", type=int, help="ID версии (UInt8)")
    parser.add_argument("--output", type=str, help="Путь к выходному .xlsx")
    return parser.parse_args()


def resolve_version(client, version_date, version_id):
    if version_date and version_id is not None:
        return datetime.strptime(version_date, "%Y-%m-%d").date(), int(version_id)
    row = client.execute(
        """
        SELECT version_date, version_id
        FROM heli_pandas
        ORDER BY version_date DESC, version_id DESC
        LIMIT 1
        """
    )
    if not row:
        raise RuntimeError("Таблица heli_pandas пуста — нечего выгружать")
    return row[0][0], int(row[0][1])


def load_status_labels() -> dict[int, str]:
    return dict(PROJECT_STATUS_LABELS)


def build_status_catalog() -> pd.DataFrame:
    rows = [
        {
            "status_id": sid,
            "status_name": name,
            "in_dict_status_flat": sid in load_dict_status_flat(),
        }
        for sid, name in sorted(PROJECT_STATUS_LABELS.items())
    ]
    return pd.DataFrame(rows)


def load_group4(client, version_date: date, version_id: int) -> pd.DataFrame:
    rows = client.execute(
        """
        SELECT
            serialno, partno, toUInt32(ifNull(partseqno_i, 0)) AS partseqno_i,
            ac_typ, condition,
            toUInt8(ifNull(status_id, 0)) AS status_id,
            target_date,
            toUInt16(ifNull(repair_days, 0)) AS repair_days,
            toUInt16(ifNull(repair_time, 0)) AS repair_time,
            removal_date,
            toUInt32(ifNull(sne, 0)) AS sne,
            toUInt32(ifNull(oh, 0)) AS oh,
            toUInt32(ifNull(ll, 0)) AS ll,
            owner, location
        FROM heli_pandas
        WHERE version_date = %(d)s
          AND version_id = %(i)s
          AND toUInt32(ifNull(group_by, 0)) = %(g)s
        ORDER BY status_id, serialno
        """,
        {"d": version_date, "i": version_id, "g": GROUP_BY_ENGINE},
    )
    cols = [
        "serialno", "partno", "partseqno_i", "ac_typ", "condition",
        "status_id", "target_date", "repair_days", "repair_time",
        "removal_date", "sne", "oh", "ll", "owner", "location",
    ]
    return pd.DataFrame(rows, columns=cols)


def enrich(df: pd.DataFrame, labels: dict, version_date: date) -> pd.DataFrame:
    out = df.copy()
    out["status_label"] = out["status_id"].map(
        lambda sid: labels.get(int(sid), f"Статус {int(sid)}")
    )
    target = pd.to_datetime(out["target_date"], errors="coerce").dt.date
    valid = target.notna() & (target != EMPTY_DATE)
    out["days_to_target"] = [
        (t - version_date).days if v else None
        for t, v in zip(target, valid)
    ]
    return out[EXPORT_COLUMNS]


def build_summary(df: pd.DataFrame, labels: dict[int, str]) -> pd.DataFrame:
    grp = df.groupby(["status_id", "status_label"], dropna=False)
    summary = grp.agg(
        rows=("serialno", "size"),
        with_target_date=("target_date", lambda s: int(
            (pd.to_datetime(s, errors="coerce").dt.date.notna()
             & (pd.to_datetime(s, errors="coerce").dt.date != EMPTY_DATE)).sum()
        )),
        with_repair_days=("repair_days", lambda s: int((s > 0).sum())),
    ).reset_index()
    present = set(summary["status_id"].astype(int))
    for sid, name in sorted(labels.items()):
        if sid not in present:
            summary = pd.concat(
                [
                    summary,
                    pd.DataFrame(
                        [{
                            "status_id": sid,
                            "status_label": name,
                            "rows": 0,
                            "with_target_date": 0,
                            "with_repair_days": 0,
                        }]
                    ),
                ],
                ignore_index=True,
            )
    return summary.sort_values("status_id")


def main() -> int:
    args = parse_args()
    client = get_clickhouse_client()
    version_date, version_id = resolve_version(client, args.version_date, args.version_id)
    print(f"📅 day0 = {version_date} (version_id={version_id}), group_by={GROUP_BY_ENGINE}")

    labels = load_status_labels()
    raw = load_group4(client, version_date, version_id)
    if raw.empty:
        raise RuntimeError(
            f"Нет строк group_by={GROUP_BY_ENGINE} для {version_date} v{version_id}"
        )
    data = enrich(raw, labels, version_date)
    summary = build_summary(data, labels)
    status_catalog = build_status_catalog()

    print("\n=== Распределение по status_id ===")
    print(summary.to_string(index=False))
    print(f"\nВсего двигателей: {len(data)}")

    if args.output:
        out_path = Path(args.output)
    else:
        out_dir = code_root.parent / "output" / f"engine_repair_day0_{version_date}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"Engine_Repair_Group4_day0_{version_date}_v{version_id}.xlsx"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        data.to_excel(writer, sheet_name="group4_day0", index=False)
        summary.to_excel(writer, sheet_name="summary_status", index=False)
        status_catalog.to_excel(writer, sheet_name="status_catalog", index=False)

    print(f"\n✅ Сохранено: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
