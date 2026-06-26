#!/usr/bin/env python3
"""Выгрузка открытых ремонтных заказов на двигатели (ДВИГ) из DWH → Excel.

Адаптация админского SQL:
  od_header + od_detail + part_special (special='ДВИГ')
  order_type='R', open states, target_date

В DWH таблица od_header может отсутствовать — тогда фильтры header
(state/ext_state) опускаются, используются поля od_detail (order_type, state).

Пример:
  .venv/bin/python code/utils/export_engine_repair_dwh.py --report-date 2026-04-08
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import numpy as np

code_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(code_root / "utils"))
from dwh_golden_replay_export import (  # noqa: E402
    REQUIRED_ENV,
    _as_of_end,
    _as_of_start,
    _env_bool,
    default_out_subdir,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CA = REPO_ROOT / "config" / "certs" / "yandex_cloud_RootCA.pem"
MD_XLSX = REPO_ROOT / "data_input" / "master_data" / "MD_Сomponents.xlsx"
DEFAULT_DATASET_DATE = "2026-04-08"


def _sql_in_list(values: list[str]) -> str:
    return ",".join("'" + v.replace("'", "''") + "'" for v in sorted(values))


def load_md_aggregates(*, group_by: int | None = None) -> pd.DataFrame:
    """partno агрегатов из MD_Components (group_by > 2, без планеров 1/2)."""
    if not MD_XLSX.is_file():
        raise FileNotFoundError(f"MD_Components не найден: {MD_XLSX}")
    try:
        raw = pd.read_excel(MD_XLSX, sheet_name="Агрегаты", header=1, engine="openpyxl")
    except ValueError:
        raw = pd.read_excel(MD_XLSX, sheet_name=0, header=1, engine="openpyxl")
    if "partno" not in raw.columns or "group_by" not in raw.columns:
        raise RuntimeError(f"MD_Components: ожидаются колонки partno, group_by; есть {list(raw.columns)}")
    df = raw[["partno", "group_by"] + (["comp_number"] if "comp_number" in raw.columns else [])].copy()
    df["partno"] = df["partno"].astype(str).str.strip()
    df = df[(df["partno"] != "") & df["partno"].notna()]
    df["group_by"] = pd.to_numeric(df["group_by"], errors="coerce").astype("Int64")
    df = df[df["group_by"] > 2].drop_duplicates(subset=["partno"], keep="first")
    if group_by is not None:
        df = df[df["group_by"] == group_by]
    if df.empty:
        raise RuntimeError("MD_Components: пустой список агрегатов после фильтра group_by > 2")
    return df.reset_index(drop=True)


def resolve_latest_dataset_date() -> str:
    """Последний version_date из heli_pandas; fallback — DEFAULT_DATASET_DATE."""
    try:
        from config_loader import get_clickhouse_client

        client = get_clickhouse_client()
        rows = client.execute(
            """
            SELECT toString(version_date)
            FROM heli_pandas
            ORDER BY version_date DESC, version_id DESC
            LIMIT 1
            """
        )
        if rows:
            return str(rows[0][0])
    except Exception:
        pass
    return DEFAULT_DATASET_DATE


def resolve_latest_dwh_date(client=None) -> str:
    """Последняя доступная дата среза в DWH (max report_date в reports, иначе max valid_from в od_detail)."""
    own_client = client is None
    if own_client:
        client = _dwh_connect()
    candidates: list[str] = []
    for sql in (
        "SELECT toString(max(report_date)) FROM reports.amos_heli_rotables_components_status",
        "SELECT toString(max(toDate(valid_from))) FROM source.amos_heli_od_detail",
    ):
        try:
            row = client.query(sql).result_rows[0][0]
            if row:
                candidates.append(str(row)[:10])
        except Exception:
            pass
    if not candidates:
        raise RuntimeError("Не удалось определить последнюю дату в DWH")
    return max(candidates)


def _resolve_ca_cert() -> str | None:
    ca = os.getenv("DWH_CLICKHOUSE_CA_CERT", "").strip()
    if ca and Path(ca).is_file():
        return ca
    if DEFAULT_CA.is_file():
        return str(DEFAULT_CA)
    return ca or None


def _dwh_connect():
    import clickhouse_connect

    for k in REQUIRED_ENV:
        if not os.getenv(k):
            raise RuntimeError(f"Отсутствует переменная окружения {k}")
    kwargs = dict(
        host=os.environ["DWH_CLICKHOUSE_HOST"],
        port=int(os.environ["DWH_CLICKHOUSE_PORT"]),
        username=os.environ["DWH_CLICKHOUSE_USER"],
        password=os.environ["DWH_CLICKHOUSE_PASSWORD"],
        database=os.environ["DWH_CLICKHOUSE_DATABASE"],
        secure=_env_bool("DWH_CLICKHOUSE_SECURE", True),
        verify=_env_bool("DWH_CLICKHOUSE_VERIFY", True),
        connect_timeout=60,
        send_receive_timeout=300,
    )
    ca = _resolve_ca_cert()
    if ca:
        kwargs["ca_cert"] = ca
    return clickhouse_connect.get_client(**kwargs)


def _table_exists(client, database: str, name: str) -> bool:
    row = client.query(
        """
        SELECT count()
        FROM system.tables
        WHERE database = {db:String} AND name = {name:String}
        """,
        parameters={"db": database, "name": name},
    ).result_rows[0][0]
    return int(row) > 0


def _engine_repair_sql_admin() -> str:
    """Админский SQL: part_special.special='ДВИГ', od order_type=R, state=O.

    Без od_header (в DWH его нет). Дедуп od_detail по detailno_i — последняя версия SCD.
    part_special — DISTINCT partno с special='ДВИГ' (как в Oracle, без as-of).
    """
    return """
WITH
part_special_dvig AS (
    SELECT DISTINCT partno
    FROM source.amos_heli_part_special
    WHERE special = 'ДВИГ'
),
od_detail_latest AS (
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER (PARTITION BY detailno_i ORDER BY valid_from DESC) AS rn
        FROM source.amos_heli_od_detail
    )
    WHERE rn = 1
)
SELECT
    toString(od.orderno_i) AS orderno,
    od.partno AS partno,
    od.serialno AS serialno,
    coalesce(od.vendor, '') AS vendor,
    od.target_date AS target_date,
    od.orig_target_date AS orig_target_date,
    od.confirmed_date AS confirmed_date,
    od.del_date AS del_date,
    od.ext_state AS detail_ext_state,
    od.order_type AS order_type,
    '' AS header_state,
    '' AS header_ext_state,
    od.state AS detail_state,
    od.condition AS condition,
    od.ac_registr AS ac_registr,
    od.orderno_i AS orderno_i,
    od.detailno_i AS detailno_i
FROM od_detail_latest od
INNER JOIN part_special_dvig ps ON ps.partno = od.partno
WHERE od.order_type = 'R'
  AND od.state = 'O'
ORDER BY od.partno, od.serialno, orderno
"""


def _engine_repair_sql(report_date: str, *, with_header: bool) -> str:
    ae = _as_of_end(report_date)
    ast = _as_of_start(report_date)
    if with_header:
        return f"""
WITH
part_special_snap AS (
    SELECT partno
    FROM (
        SELECT
            partno,
            special,
            row_number() OVER (PARTITION BY partno ORDER BY valid_from DESC) AS rn
        FROM source.amos_heli_part_special
        WHERE valid_from <= {ae}
          AND (valid_to IS NULL OR valid_to > {ast})
    )
    WHERE rn = 1
      AND upperUTF8(trim(special)) = 'ДВИГ'
),
od_header_snap AS (
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER (PARTITION BY orderno_i ORDER BY valid_from DESC) AS rn
        FROM source.amos_heli_od_header
        WHERE valid_from <= {ae}
          AND (valid_to IS NULL OR valid_to > {ast})
    )
    WHERE rn = 1
),
od_detail_snap AS (
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER (PARTITION BY detailno_i ORDER BY valid_from DESC) AS rn
        FROM source.amos_heli_od_detail
        WHERE valid_from <= {ae}
          AND (valid_to IS NULL OR valid_to > {ast})
    )
    WHERE rn = 1
)
SELECT
    toString(oh.orderno) AS orderno,
    od.partno AS partno,
    od.serialno AS serialno,
    coalesce(oh.vendor, od.vendor, '') AS vendor,
    od.target_date AS target_date,
    od.orig_target_date AS orig_target_date,
    od.confirmed_date AS confirmed_date,
    od.del_date AS del_date,
    od.ext_state AS detail_ext_state,
    oh.order_type AS order_type,
    oh.state AS header_state,
    oh.ext_state AS header_ext_state,
    od.state AS detail_state,
    od.condition AS condition,
    od.ac_registr AS ac_registr,
    od.orderno_i AS orderno_i,
    od.detailno_i AS detailno_i
FROM od_header_snap oh
INNER JOIN od_detail_snap od ON od.orderno_i = oh.orderno_i
INNER JOIN part_special_snap ps ON ps.partno = od.partno
WHERE oh.order_type = 'R'
  AND oh.state = 'O'
  AND oh.ext_state IN ('O', 'BO', 'PB', 'PR')
  AND ps.partno != ''
  AND od.state = 'O'
ORDER BY od.partno, od.serialno, orderno
"""
    return f"""
WITH
part_special_snap AS (
    SELECT partno
    FROM (
        SELECT
            partno,
            special,
            row_number() OVER (PARTITION BY partno ORDER BY valid_from DESC) AS rn
        FROM source.amos_heli_part_special
        WHERE valid_from <= {ae}
          AND (valid_to IS NULL OR valid_to > {ast})
    )
    WHERE rn = 1
      AND upperUTF8(trim(special)) = 'ДВИГ'
),
od_detail_snap AS (
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER (PARTITION BY detailno_i ORDER BY valid_from DESC) AS rn
        FROM source.amos_heli_od_detail
        WHERE valid_from <= {ae}
          AND (valid_to IS NULL OR valid_to > {ast})
    )
    WHERE rn = 1
)
SELECT
    toString(od.orderno_i) AS orderno,
    od.partno AS partno,
    od.serialno AS serialno,
    coalesce(od.vendor, '') AS vendor,
    od.target_date AS target_date,
    od.orig_target_date AS orig_target_date,
    od.confirmed_date AS confirmed_date,
    od.del_date AS del_date,
    od.ext_state AS detail_ext_state,
    od.order_type AS order_type,
    '' AS header_state,
    '' AS header_ext_state,
    od.state AS detail_state,
    od.condition AS condition,
    od.ac_registr AS ac_registr,
    od.orderno_i AS orderno_i,
    od.detailno_i AS detailno_i
FROM od_detail_snap od
INNER JOIN part_special_snap ps ON ps.partno = od.partno
WHERE od.order_type = 'R'
  AND od.state = 'O'
ORDER BY od.partno, od.serialno, orderno
"""


def _engine_repair_sql_md_admin(partnos: list[str]) -> str:
    in_list = _sql_in_list(partnos)
    return f"""
WITH
od_detail_latest AS (
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER (PARTITION BY detailno_i ORDER BY valid_from DESC) AS rn
        FROM source.amos_heli_od_detail
    )
    WHERE rn = 1
)
SELECT
    toString(od.orderno_i) AS orderno,
    od.partno AS partno,
    od.serialno AS serialno,
    coalesce(od.vendor, '') AS vendor,
    od.target_date AS target_date,
    od.orig_target_date AS orig_target_date,
    od.confirmed_date AS confirmed_date,
    od.del_date AS del_date,
    od.ext_state AS detail_ext_state,
    od.order_type AS order_type,
    '' AS header_state,
    '' AS header_ext_state,
    od.state AS detail_state,
    od.condition AS condition,
    od.ac_registr AS ac_registr,
    od.orderno_i AS orderno_i,
    od.detailno_i AS detailno_i
FROM od_detail_latest od
WHERE od.order_type = 'R'
  AND od.state = 'O'
  AND od.partno IN ({in_list})
ORDER BY od.partno, od.serialno, orderno
"""


def _engine_repair_sql_md(report_date: str, partnos: list[str], *, with_header: bool) -> str:
    in_list = _sql_in_list(partnos)
    ae = _as_of_end(report_date)
    ast = _as_of_start(report_date)
    if with_header:
        return f"""
WITH
od_header_snap AS (
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER (PARTITION BY orderno_i ORDER BY valid_from DESC) AS rn
        FROM source.amos_heli_od_header
        WHERE valid_from <= {ae}
          AND (valid_to IS NULL OR valid_to > {ast})
    )
    WHERE rn = 1
),
od_detail_snap AS (
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER (PARTITION BY detailno_i ORDER BY valid_from DESC) AS rn
        FROM source.amos_heli_od_detail
        WHERE valid_from <= {ae}
          AND (valid_to IS NULL OR valid_to > {ast})
    )
    WHERE rn = 1
)
SELECT
    toString(oh.orderno) AS orderno,
    od.partno AS partno,
    od.serialno AS serialno,
    coalesce(oh.vendor, od.vendor, '') AS vendor,
    od.target_date AS target_date,
    od.orig_target_date AS orig_target_date,
    od.confirmed_date AS confirmed_date,
    od.del_date AS del_date,
    od.ext_state AS detail_ext_state,
    oh.order_type AS order_type,
    oh.state AS header_state,
    oh.ext_state AS header_ext_state,
    od.state AS detail_state,
    od.condition AS condition,
    od.ac_registr AS ac_registr,
    od.orderno_i AS orderno_i,
    od.detailno_i AS detailno_i
FROM od_header_snap oh
INNER JOIN od_detail_snap od ON od.orderno_i = oh.orderno_i
WHERE oh.order_type = 'R'
  AND oh.state = 'O'
  AND oh.ext_state IN ('O', 'BO', 'PB', 'PR')
  AND od.state = 'O'
  AND od.partno IN ({in_list})
ORDER BY od.partno, od.serialno, orderno
"""
    return f"""
WITH
od_detail_snap AS (
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER (PARTITION BY detailno_i ORDER BY valid_from DESC) AS rn
        FROM source.amos_heli_od_detail
        WHERE valid_from <= {ae}
          AND (valid_to IS NULL OR valid_to > {ast})
    )
    WHERE rn = 1
)
SELECT
    toString(od.orderno_i) AS orderno,
    od.partno AS partno,
    od.serialno AS serialno,
    coalesce(od.vendor, '') AS vendor,
    od.target_date AS target_date,
    od.orig_target_date AS orig_target_date,
    od.confirmed_date AS confirmed_date,
    od.del_date AS del_date,
    od.ext_state AS detail_ext_state,
    od.order_type AS order_type,
    '' AS header_state,
    '' AS header_ext_state,
    od.state AS detail_state,
    od.condition AS condition,
    od.ac_registr AS ac_registr,
    od.orderno_i AS orderno_i,
    od.detailno_i AS detailno_i
FROM od_detail_snap od
WHERE od.order_type = 'R'
  AND od.state = 'O'
  AND od.partno IN ({in_list})
ORDER BY od.partno, od.serialno, orderno
"""


def _partno_summary(df: pd.DataFrame, md: pd.DataFrame) -> pd.DataFrame:
    counts = (
        df.groupby("partno", dropna=False)
        .agg(repair_rows=("serialno", "count"), serials=("serialno", "nunique"))
        .reset_index()
    )
    summary = md.merge(counts, on="partno", how="left")
    summary["repair_rows"] = summary["repair_rows"].fillna(0).astype(int)
    summary["serials"] = summary["serials"].fillna(0).astype(int)
    return summary.sort_values(["group_by", "partno"]).reset_index(drop=True)


AMOS_EMPTY_DATE = pd.Timestamp("1971-12-31")


def _vendor_filled_mask(series: pd.Series) -> pd.Series:
    v = series.fillna("").astype(str).str.strip()
    return (v != "") & (v.str.lower() != "nan")


def _empty_amos_date_mask(series: pd.Series) -> pd.Series:
    td = pd.to_datetime(series, errors="coerce")
    return td.dt.normalize() == AMOS_EMPTY_DATE


def _filled_target_date_mask(series: pd.Series) -> pd.Series:
    return ~_empty_amos_date_mask(series)


def _target_date_past_mask(series: pd.Series, report_date: str) -> pd.Series:
    td = pd.to_datetime(series, errors="coerce").dt.normalize()
    rd = pd.Timestamp(report_date).normalize()
    return _filled_target_date_mask(series) & (td < rd)


def _target_date_vs_report_label(series: pd.Series, report_date: str) -> pd.Series:
    td = pd.to_datetime(series, errors="coerce").dt.normalize()
    rd = pd.Timestamp(report_date).normalize()
    empty = _empty_amos_date_mask(series)
    labels = np.where(
        empty,
        "Пустая (1971-12-31)",
        np.where(
            td < rd,
            "В прошлом (относительно report_date)",
            np.where(td == rd, "На дату среза", "В будущем"),
        ),
    )
    return pd.Series(labels, index=series.index)


def _nullable_date_present_mask(series: pd.Series) -> pd.Series:
    """Новые поля Nullable(Date32): заполнено = не NULL и не sentinel 1971-12-31."""
    d = pd.to_datetime(series, errors="coerce").dt.normalize()
    return d.notna() & (d != AMOS_EMPTY_DATE)


def _target_moved_label(orig: pd.Series, target: pd.Series) -> pd.Series:
    """Перенос плановой даты: orig_target_date задана и отличается от target_date."""
    o = pd.to_datetime(orig, errors="coerce").dt.normalize()
    t = pd.to_datetime(target, errors="coerce").dt.normalize()
    orig_set = o.notna() & (o != AMOS_EMPTY_DATE)
    target_set = t.notna() & (t != AMOS_EMPTY_DATE)
    moved = orig_set & target_set & (o != t)
    labels = np.where(~orig_set, "Нет данных (orig пуст)", np.where(moved, "Да", "Нет"))
    return pd.Series(labels, index=orig.index)


def _enrich_gb4_repair(df: pd.DataFrame, report_date: str) -> pd.DataFrame:
    out = df.copy()
    vf = _vendor_filled_mask(out["vendor"])
    ed = _empty_amos_date_mask(out["target_date"])
    past = _target_date_past_mask(out["target_date"], report_date)
    out["vendor_filled"] = vf.map({True: "Да", False: "Нет"})
    out["target_date_note"] = ed.map(
        {
            True: "Пустая дата AMOS (0 дней от 1971-12-31)",
            False: "Дата задана",
        }
    )
    out["target_date_vs_report"] = _target_date_vs_report_label(out["target_date"], report_date)
    out["vendor_and_past_target_date"] = (vf & past).map({True: "Да", False: "Нет"})
    if "orig_target_date" in out.columns:
        out["target_moved"] = _target_moved_label(out["orig_target_date"], out["target_date"])
    if "confirmed_date" in out.columns:
        out["confirmed"] = _nullable_date_present_mask(out["confirmed_date"]).map({True: "Да", False: "Нет"})
    if "del_date" in out.columns:
        out["delivered"] = _nullable_date_present_mask(out["del_date"]).map({True: "Да", False: "Нет"})
    orders_per_serial = out.groupby(["partno", "serialno"], dropna=False)["orderno"].transform("count")
    out["orders_for_serial"] = orders_per_serial.astype(int)
    out["serial_duplicate_orders"] = orders_per_serial.gt(1).map({True: "Да", False: "Нет"})
    return out


def _group_summary_partno(df: pd.DataFrame, md_group: pd.DataFrame, report_date: str) -> pd.DataFrame:
    vf = _vendor_filled_mask(df["vendor"]) if len(df) else pd.Series(dtype=bool)
    ed = _empty_amos_date_mask(df["target_date"]) if len(df) else pd.Series(dtype=bool)
    past = _target_date_past_mask(df["target_date"], report_date) if len(df) else pd.Series(dtype=bool)
    vendor_past = vf & past if len(df) else pd.Series(dtype=bool)
    dup_serials = (
        df.groupby(["partno", "serialno"], dropna=False)
        .size()
        .reset_index(name="cnt")
        .query("cnt > 1")
        if len(df)
        else pd.DataFrame(columns=["partno", "serialno", "cnt"])
    )
    rows = []
    for _, md_row in md_group.iterrows():
        partno = md_row["partno"]
        sub = df[df["partno"] == partno] if len(df) else df.iloc[0:0]
        idx = sub.index
        dup_n = (
            dup_serials[dup_serials["partno"] == partno]["serialno"].nunique()
            if len(sub) and len(dup_serials)
            else 0
        )
        rows.append(
            {
                "partno": partno,
                "group_by": int(md_row["group_by"]),
                "comp_number": md_row.get("comp_number"),
                "in_md_components": "Да",
                "repair_rows": len(sub),
                "unique_serialno": sub["serialno"].nunique() if len(sub) else 0,
                "extra_rows_vs_serials": len(sub) - sub["serialno"].nunique() if len(sub) else 0,
                "vendor_filled_rows": int(vf.loc[idx].sum()) if len(sub) else 0,
                "vendor_empty_rows": int((~vf.loc[idx]).sum()) if len(sub) else 0,
                "empty_target_date_rows": int(ed.loc[idx].sum()) if len(sub) else 0,
                "target_date_in_past_rows": int(past.loc[idx].sum()) if len(sub) else 0,
                "vendor_filled_and_target_date_in_past_rows": int(vendor_past.loc[idx].sum()) if len(sub) else 0,
                "serialno_with_2plus_orders": int(dup_n),
            }
        )
    return pd.DataFrame(rows)


def _gb4_summary_extended(df: pd.DataFrame, md_gb4: pd.DataFrame, report_date: str) -> pd.DataFrame:
    return _group_summary_partno(df, md_gb4, report_date)


def _readme_sheet(report_date: str, mode: str, has_header: bool) -> pd.DataFrame:
    return pd.DataFrame(
        [
            ("Отчёт", "Открытые ремонтные заказы DWH только для group_by=4 (двигатели) из MD_Components"),
            ("report_date", report_date),
            ("mode", f"{mode} — SCD-срез DWH на дату датасета"),
            ("Фильтр partno", "Только partno из MD_Components с group_by=4 (исключены планеры 1 и 2)"),
            ("Фильтр заказа", "amos_heli_od_detail: order_type=R (Repair), state=O (Open)"),
            (
                "od_header",
                "Есть в DWH (amos_heli_od_header). В as-of режиме применяется эталонный фильтр "
                "AMOS: oh.state=O AND oh.ext_state IN (O,BO,PB,PR); header_state/header_ext_state заполнены",
            ),
            (
                "SCD / detailno_i",
                "Одна позиция заказа может иметь несколько версий в DWH; берётся одна актуальная "
                "на report_date (as-of) или последняя (admin)",
            ),
            (
                "target_date = 1971-12-31",
                "В AMOS даты хранятся как число дней от 1971-12-31; 0 дней = эпоха = «дата не задана». "
                "В DWH это Date 1971-12-31, не реальная плановая дата ремонта",
            ),
            (
                "vendor пустой",
                "Пустой vendor в od_detail — поле не заполнено в AMOS; часто совпадает с пустой target_date",
            ),
            (
                "serial_duplicate_orders = Да",
                "У одного serialno несколько открытых заказов (разные orderno/detailno_i) на одну дату среза",
            ),
            (
                "target_date в прошлом",
                "target_date задана (не 1971-12-31) и строго меньше report_date — плановая дата уже прошла на дату среза",
            ),
            (
                "vendor_and_past_target_date = Да",
                "Одновременно: vendor заполнен И target_date в прошлом относительно report_date",
            ),
            (
                "orig_target_date",
                "Исходная плановая дата (до переносов). Пусто = 1971-12-31 (sentinel AMOS) или NULL. "
                "Заполняется с момента доработки DWH (бэкфилла нет) — в текущем срезе в основном пусто",
            ),
            (
                "confirmed_date / del_date",
                "Подтверждённая поставщиком и фактическая дата поставки. Пусто = 1971-12-31 (sentinel AMOS) "
                "или NULL; заполняются с момента доработки DWH",
            ),
            (
                "target_moved",
                "Да — orig_target_date задана и отличается от target_date (срок переносили); "
                "Нет данных (orig пуст) — для строк без исходной даты",
            ),
            ("confirmed / delivered", "Да/Нет — есть ли confirmed_date / del_date"),
            (
                "detail_ext_state / header_ext_state",
                "Расширенный статус строки заказа (od) и заголовка (oh)",
            ),
            ("Лист repair_gb4", "Все строки с расшифровочными колонками"),
            ("Лист duplicate_serials_detail", "Только serialno с 2+ открытыми заказами — разрез по orderno"),
            ("Лист summary_by_partno", "Сводка по каждому partno из MD gb=4"),
            ("Лист vendor_past_target_date", "Заказы с vendor заполнен и target_date в прошлом относительно report_date"),
            ("Лист vendor_stats", "Статистика по vendor"),
            ("Лист date_stats", "Статистика по target_date и связи с vendor"),
        ],
        columns=["Поле", "Пояснение"],
    )


def _fetch_md_repair(report_date: str, mode: str, md: pd.DataFrame) -> tuple[pd.DataFrame, bool, str]:
    client = _dwh_connect()
    partnos = md["partno"].tolist()
    if mode == "admin":
        has_header = False
        sql = _engine_repair_sql_md_admin(partnos)
        mode_note = "admin: текущий срез DWH"
    elif mode == "as-of":
        has_header = _table_exists(client, "source", "amos_heli_od_header")
        sql = _engine_repair_sql_md(report_date, partnos, with_header=has_header)
        mode_note = f"as-of: snapshot на {report_date}"
    else:
        raise ValueError(f"Неизвестный mode={mode!r}")
    df = client.query_df(sql)
    merge_cols = [c for c in ["partno", "group_by", "comp_number"] if c in md.columns]
    df = df.merge(md[merge_cols].drop_duplicates(subset=["partno"]), on="partno", how="left")
    return df, has_header, mode_note


def _fetch_gb4_repair(report_date: str, mode: str) -> tuple[pd.DataFrame, bool, str]:
    return _fetch_md_repair(report_date, mode, load_md_aggregates(group_by=4))


def _group_sheet_name(group_by: int, kind: str) -> str:
    """kind: svod | list (Excel ≤31 символ)."""
    return f"g{int(group_by):02d}_{kind}"[:31]


def _repair_list_columns(df: pd.DataFrame) -> list[str]:
    cols = [
        "group_by",
        "comp_number",
        "partno",
        "serialno",
        "orderno",
        "detailno_i",
        "orderno_i",
        "vendor",
        "vendor_filled",
        "orig_target_date",
        "target_date",
        "target_date_note",
        "target_date_vs_report",
        "target_moved",
        "confirmed_date",
        "confirmed",
        "del_date",
        "delivered",
        "vendor_and_past_target_date",
        "orders_for_serial",
        "serial_duplicate_orders",
        "order_type",
        "detail_state",
        "detail_ext_state",
        "header_state",
        "header_ext_state",
        "condition",
        "ac_registr",
    ]
    return [c for c in cols if c in df.columns]


def _orderno_list(series: pd.Series) -> str:
    vals: list[str] = []
    for x in series:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            continue
        s = str(x).strip()
        if s and s.lower() != "nan":
            vals.append(s)
    if not vals:
        return ""
    try:
        return ", ".join(str(int(v)) if str(v).isdigit() else v for v in sorted(vals, key=lambda v: int(v) if str(v).isdigit() else v))
    except ValueError:
        return ", ".join(sorted(set(vals)))


def _group_duplicate_serials(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=["partno", "serialno", "open_orders", "extra_rows", "orderno_list", "detailno_i_list"]
        )
    dup = (
        df.groupby(["partno", "serialno"], dropna=False)
        .agg(
            open_orders=("orderno", "count"),
            orderno_list=("orderno", _orderno_list),
            detailno_i_list=("detailno_i", lambda s: ", ".join(str(int(x)) for x in sorted(s.dropna().unique()))),
        )
        .reset_index()
        .query("open_orders > 1")
        .sort_values(["partno", "open_orders"], ascending=[True, False])
    )
    dup["extra_rows"] = dup["open_orders"] - 1
    return dup[["partno", "serialno", "open_orders", "extra_rows", "orderno_list", "detailno_i_list"]].reset_index(
        drop=True
    )


def _group_metrics_table(group_by: int, md_g: pd.DataFrame, df_g: pd.DataFrame, report_date: str) -> pd.DataFrame:
    partno_tbl = _group_summary_partno(df_g, md_g, report_date)
    dup = _group_duplicate_serials(df_g)
    return pd.DataFrame(
        [
            ("group_by", group_by),
            ("report_date", report_date),
            ("partno_in_md", len(md_g)),
            ("partno_with_open_repair", int((partno_tbl["repair_rows"] > 0).sum())),
            ("partno_without_open_repair", int((partno_tbl["repair_rows"] == 0).sum())),
            ("repair_rows_total", len(df_g)),
            ("unique_serialno", df_g["serialno"].nunique() if len(df_g) else 0),
            ("extra_rows_vs_serials", len(df_g) - df_g["serialno"].nunique() if len(df_g) else 0),
            ("serialno_with_2plus_orders", len(dup)),
            ("extra_rows_from_duplicate_serials", int(dup["extra_rows"].sum()) if len(dup) else 0),
            (
                "vendor_filled_rows",
                int(_vendor_filled_mask(df_g["vendor"]).sum()) if len(df_g) else 0,
            ),
            (
                "empty_target_date_rows",
                int(_empty_amos_date_mask(df_g["target_date"]).sum()) if len(df_g) else 0,
            ),
            (
                "vendor_filled_and_target_date_in_past",
                int((_vendor_filled_mask(df_g["vendor"]) & _target_date_past_mask(df_g["target_date"], report_date)).sum())
                if len(df_g)
                else 0,
            ),
        ],
        columns=["metric", "value"],
    )


def _write_group_svod_sheet(
    writer: pd.ExcelWriter,
    group_by: int,
    md_g: pd.DataFrame,
    df_g: pd.DataFrame,
    report_date: str,
) -> None:
    sheet = _group_sheet_name(group_by, "svod")
    metrics = _group_metrics_table(group_by, md_g, df_g, report_date)
    partno_tbl = _group_summary_partno(df_g, md_g, report_date)
    dups = _group_duplicate_serials(df_g)

    metrics.to_excel(writer, sheet_name=sheet, index=False, startrow=0)
    row = len(metrics) + 2
    pd.DataFrame([{"section": "partno — свод по MD"}]).to_excel(
        writer, sheet_name=sheet, index=False, header=False, startrow=row
    )
    row += 1
    partno_tbl.to_excel(writer, sheet_name=sheet, index=False, startrow=row)
    row += len(partno_tbl) + 2
    pd.DataFrame([{"section": "дубли serialno (2+ открытых заказа)"}]).to_excel(
        writer, sheet_name=sheet, index=False, header=False, startrow=row
    )
    row += 1
    dups.to_excel(writer, sheet_name=sheet, index=False, startrow=row)


def _all_groups_readme(report_date: str, mode: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            ("Отчёт", "Открытые ремонты DWH по всем group_by > 2 из MD_Components"),
            ("report_date", report_date),
            ("mode", mode),
            ("Листы", "На каждый group_by два листа: gNN_svod (свод) и gNN_list (список строк)"),
            ("Фильтр partno", "MD_Components, group_by > 2 (без планеров 1 и 2)"),
            (
                "Фильтр заказа",
                "order_type=R, state=O. В as-of режиме при наличии od_header добавляется "
                "эталонный фильтр AMOS: oh.state=O AND oh.ext_state IN (O,BO,PB,PR)",
            ),
            (
                "Новые поля дат",
                "orig_target_date (исходная плановая, до переносов), confirmed_date, del_date. "
                "Пусто = 1971-12-31 (sentinel AMOS) или NULL; заполняются с момента доработки DWH (бэкфилла нет)",
            ),
            ("target_moved", "Да — срок переносили (orig_target_date ≠ target_date)"),
            ("gNN_svod", "KPI группы + таблица partno + дубли serialno"),
            ("gNN_list", "Все строки ремонта группы с расшифровками (вкл. новые даты и ext_state)"),
        ],
        columns=["Поле", "Пояснение"],
    )


def export_all_groups_excel(
    report_date: str,
    out_path: Path | None = None,
    *,
    mode: str = "as-of",
) -> Path:
    md_all = load_md_aggregates()
    df_raw, has_header, mode_note = _fetch_md_repair(report_date, mode, md_all)
    df = _enrich_gb4_repair(df_raw, report_date)

    groups = sorted(int(g) for g in md_all["group_by"].dropna().unique())

    if out_path is None:
        out_dir = Path("output") / default_out_subdir(report_date, prefix="dwh_engine_repair")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"Engine_Repair_AllGroups_{report_date}.xlsx"

    meta = pd.DataFrame(
        [
            {"key": "report_date", "value": report_date},
            {"key": "mode", "value": mode},
            {"key": "groups_count", "value": len(groups)},
            {"key": "md_partno_count", "value": len(md_all)},
            {"key": "repair_rows_total", "value": len(df)},
            {"key": "target_moved_rows", "value": int((df["target_moved"] == "Да").sum()) if "target_moved" in df.columns else 0},
            {"key": "confirmed_rows", "value": int((df["confirmed"] == "Да").sum()) if "confirmed" in df.columns else 0},
            {"key": "delivered_rows", "value": int((df["delivered"] == "Да").sum()) if "delivered" in df.columns else 0},
            {"key": "od_header_used", "value": has_header},
            {"key": "note", "value": mode_note},
        ]
    )

    list_cols = _repair_list_columns(df)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        _all_groups_readme(report_date, mode).to_excel(writer, sheet_name="README", index=False)
        meta.to_excel(writer, sheet_name="meta", index=False)
        for gb in groups:
            md_g = md_all[md_all["group_by"] == gb].reset_index(drop=True)
            df_g = df[df["group_by"] == gb].copy() if "group_by" in df.columns else df.iloc[0:0]
            _write_group_svod_sheet(writer, gb, md_g, df_g, report_date)
            list_sheet = _group_sheet_name(gb, "list")
            if len(df_g):
                df_g[list_cols].sort_values(["partno", "serialno", "orderno"]).to_excel(
                    writer, sheet_name=list_sheet, index=False
                )
            else:
                pd.DataFrame(columns=list_cols).to_excel(writer, sheet_name=list_sheet, index=False)

    print(f"✅ All-groups export → {out_path}")
    print(f"   groups: {len(groups)}, MD partno: {len(md_all)}, repair rows: {len(df)}")
    print(f"   sheets: README + meta + {len(groups)*2} (svod+list per group)")
    return out_path


def export_gb4_full_excel(
    report_date: str,
    out_path: Path | None = None,
    *,
    mode: str = "as-of",
) -> Path:
    md_gb4 = load_md_aggregates(group_by=4)
    df_raw, has_header, mode_note = _fetch_gb4_repair(report_date, mode)
    df = _enrich_gb4_repair(df_raw, report_date)

    vf = _vendor_filled_mask(df["vendor"])
    ed = _empty_amos_date_mask(df["target_date"])
    past = _target_date_past_mask(df["target_date"], report_date)
    vendor_past = vf & past

    summary = _gb4_summary_extended(df_raw, md_gb4, report_date)

    dup_keys = (
        df.groupby(["partno", "serialno"], dropna=False)
        .size()
        .reset_index(name="orders_for_serial")
        .query("orders_for_serial > 1")[["partno", "serialno"]]
    )
    if len(dup_keys):
        dup_detail = df.merge(dup_keys, on=["partno", "serialno"]).sort_values(
            ["partno", "serialno", "orderno"]
        )
    else:
        dup_detail = pd.DataFrame(columns=list(df.columns))

    vendor_stats = (
        df.assign(vendor_display=df["vendor"].fillna("").astype(str).str.strip().replace("", "(пусто)"))
        .groupby(["partno", "vendor_display"], dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values(["partno", "rows"], ascending=[True, False])
    )

    date_stats = pd.DataFrame(
        [
            {
                "metric": "Всего строк",
                "value": len(df),
            },
            {
                "metric": "target_date = 1971-12-31 (пустая AMOS)",
                "value": int(ed.sum()),
            },
            {
                "metric": "target_date задана",
                "value": int((~ed).sum()),
            },
            {
                "metric": "vendor заполнен",
                "value": int(vf.sum()),
            },
            {
                "metric": "vendor пустой",
                "value": int((~vf).sum()),
            },
            {
                "metric": "vendor пустой И date=1971-12-31",
                "value": int((~vf & ed).sum()),
            },
            {
                "metric": "vendor заполнен И date=1971-12-31",
                "value": int((vf & ed).sum()),
            },
            {
                "metric": f"target_date в прошлом (< {report_date})",
                "value": int(past.sum()),
            },
            {
                "metric": f"vendor заполнен И target_date в прошлом (< {report_date})",
                "value": int(vendor_past.sum()),
            },
            {
                "metric": "serialno с 2+ заказами",
                "value": len(dup_keys),
            },
            {
                "metric": "уникальных serialno",
                "value": df["serialno"].nunique(),
            },
        ]
    )

    date_by_partno = []
    for partno, sub in df.groupby("partno"):
        sub_ed = _empty_amos_date_mask(sub["target_date"])
        sub_vf = _vendor_filled_mask(sub["vendor"])
        sub_past = _target_date_past_mask(sub["target_date"], report_date)
        sub_vendor_past = sub_vf & sub_past
        date_by_partno.append(
            {
                "partno": partno,
                "rows": len(sub),
                "empty_target_date": int(sub_ed.sum()),
                "filled_target_date": int((~sub_ed).sum()),
                "target_date_in_past": int(sub_past.sum()),
                "vendor_filled": int(sub_vf.sum()),
                "vendor_empty": int((~sub_vf).sum()),
                "vendor_filled_and_target_date_in_past": int(sub_vendor_past.sum()),
            }
        )
    date_by_partno = pd.DataFrame(date_by_partno)

    vendor_past_rows = df[vendor_past].copy()

    if out_path is None:
        out_dir = Path("output") / default_out_subdir(report_date, prefix="dwh_engine_repair")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"Engine_Repair_gb4_{report_date}.xlsx"

    dwh_latest = resolve_latest_dwh_date()

    meta = pd.DataFrame(
        [
            {"key": "report_date", "value": report_date},
            {"key": "dwh_latest_date", "value": dwh_latest},
            {"key": "group_by", "value": 4},
            {"key": "mode", "value": mode},
            {"key": "md_gb4_partnos", "value": len(md_gb4)},
            {"key": "repair_rows", "value": len(df)},
            {"key": "unique_serialno", "value": df["serialno"].nunique()},
            {"key": "vendor_filled_rows", "value": int(vf.sum())},
            {"key": "target_date_in_past_rows", "value": int(past.sum())},
            {"key": "vendor_filled_and_target_date_in_past_rows", "value": int(vendor_past.sum())},
            {"key": "empty_target_date_rows", "value": int(ed.sum())},
            {"key": "duplicate_serialno_count", "value": len(dup_keys)},
            {"key": "od_header_used", "value": has_header},
            {"key": "note", "value": mode_note},
        ]
    )

    readme = _readme_sheet(report_date, mode, has_header)

    export_cols = [
        "partno",
        "group_by",
        "comp_number",
        "serialno",
        "orderno",
        "detailno_i",
        "orderno_i",
        "vendor",
        "vendor_filled",
        "orig_target_date",
        "target_date",
        "target_date_note",
        "target_date_vs_report",
        "target_moved",
        "confirmed_date",
        "confirmed",
        "del_date",
        "delivered",
        "vendor_and_past_target_date",
        "orders_for_serial",
        "serial_duplicate_orders",
        "order_type",
        "detail_state",
        "detail_ext_state",
        "condition",
        "ac_registr",
        "header_state",
        "header_ext_state",
    ]
    export_cols = [c for c in export_cols if c in df.columns]

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        readme.to_excel(writer, sheet_name="README", index=False)
        df[export_cols].to_excel(writer, sheet_name="repair_gb4", index=False)
        md_gb4.to_excel(writer, sheet_name="md_partnos_gb4", index=False)
        summary.to_excel(writer, sheet_name="summary_by_partno", index=False)
        dup_detail[export_cols].to_excel(writer, sheet_name="duplicate_serials_detail", index=False)
        vendor_past_rows[export_cols].to_excel(
            writer, sheet_name="vendor_past_target_date", index=False
        )
        vendor_stats.to_excel(writer, sheet_name="vendor_stats", index=False)
        date_stats.to_excel(writer, sheet_name="date_stats", index=False)
        date_by_partno.to_excel(writer, sheet_name="date_stats_by_partno", index=False)
        meta.to_excel(writer, sheet_name="meta", index=False)

    print(f"✅ GB4 full export → {out_path}")
    print(f"   строк: {len(df)}, serialno: {df['serialno'].nunique()}, vendor filled: {int(vf.sum())}")
    print(f"   empty target_date: {int(ed.sum())}, duplicate serials: {len(dup_keys)}")
    print(f"   vendor+past target_date: {int(vendor_past.sum())}")
    return out_path


def export_engine_repair(
    report_date: str,
    out_path: Path | None = None,
    *,
    mode: str = "as-of",
    partno_filter: str = "md-aggregates",
) -> Path:
    client = _dwh_connect()
    md_all = load_md_aggregates()
    md_gb4 = load_md_aggregates(group_by=4)
    partnos = md_all["partno"].tolist()

    if partno_filter == "md-aggregates":
        if mode == "admin":
            has_header = False
            sql = _engine_repair_sql_md_admin(partnos)
            mode_note = (
                "md-aggregates (group_by>2): partno из MD_Components; "
                "od order_type=R state=O; latest SCD per detailno_i."
            )
        elif mode == "as-of":
            has_header = _table_exists(client, "source", "amos_heli_od_header")
            sql = _engine_repair_sql_md(report_date, partnos, with_header=has_header)
            mode_note = (
                f"md-aggregates (group_by>2) as-of {report_date}; "
                f"{len(partnos)} partno из MD_Components."
            )
        else:
            raise ValueError(f"Неизвестный mode={mode!r}")
        file_stem = f"Engine_Repair_MD_Agg_{report_date}"
    elif partno_filter == "dvig":
        if mode == "admin":
            has_header = False
            sql = _engine_repair_sql_admin()
            mode_note = "part_special.special=ДВИГ (admin)"
        elif mode == "as-of":
            has_header = _table_exists(client, "source", "amos_heli_od_header")
            sql = _engine_repair_sql(report_date, with_header=has_header)
            mode_note = f"part_special.special=ДВИГ as-of {report_date}"
        else:
            raise ValueError(f"Неизвестный mode={mode!r}")
        file_stem = f"Engine_Repair_DVIG_{report_date}"
    else:
        raise ValueError(f"Неизвестный partno_filter={partno_filter!r}")

    df = client.query_df(sql)
    df = df.merge(md_all[["partno", "group_by"]], on="partno", how="left")
    gb4_partnos = set(md_gb4["partno"])
    df_gb4 = df[df["partno"].isin(gb4_partnos)].copy()
    summary = _partno_summary(df, md_all)
    summary_gb4 = _partno_summary(df_gb4, md_gb4)

    if out_path is None:
        out_dir = Path("output") / default_out_subdir(report_date, prefix="dwh_engine_repair")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{file_stem}.xlsx"

    meta = pd.DataFrame(
        [
            {"key": "report_date", "value": report_date},
            {"key": "mode", "value": mode},
            {"key": "partno_filter", "value": partno_filter},
            {"key": "md_aggregate_partnos", "value": len(partnos)},
            {"key": "md_gb4_partnos", "value": len(gb4_partnos)},
            {"key": "repair_rows_all", "value": len(df)},
            {"key": "repair_rows_gb4", "value": len(df_gb4)},
            {"key": "od_header_used", "value": has_header},
            {"key": "note", "value": mode_note},
        ]
    )

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        md_all.to_excel(writer, sheet_name="md_partnos_all", index=False)
        md_gb4.to_excel(writer, sheet_name="md_partnos_gb4", index=False)
        summary.to_excel(writer, sheet_name="summary_by_partno", index=False)
        summary_gb4.to_excel(writer, sheet_name="summary_gb4_partno", index=False)
        df.drop(columns=["group_by"], errors="ignore").to_excel(
            writer, sheet_name="repair_all_aggregates", index=False
        )
        df_gb4.drop(columns=["group_by"], errors="ignore").to_excel(
            writer, sheet_name="repair_gb4", index=False
        )
        meta.to_excel(writer, sheet_name="meta", index=False)

    print(f"✅ Выгружено {len(df):,} строк (все агрегаты MD) → {out_path}")
    print(f"   group_by=4: {len(df_gb4):,} строк, partno в MD: {len(gb4_partnos)}")
    print(f"   od_header: {'да' if has_header else 'нет'}")
    if len(df) > 0:
        print(f"   partno с ремонтом: {df['partno'].nunique()} из {len(partnos)}")
    return out_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="DWH → Excel: открытые ремонты (фильтр MD агрегаты или ДВИГ)"
    )
    parser.add_argument(
        "--report-date",
        default=None,
        help="As-of дата среза DWH; по умолчанию — последний version_date heli_pandas",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Путь к xlsx",
    )
    parser.add_argument(
        "--mode",
        choices=("admin", "as-of"),
        default="as-of",
        help="as-of — SCD snapshot на report-date (default); admin — текущий срез DWH",
    )
    parser.add_argument(
        "--filter",
        dest="partno_filter",
        choices=("md-aggregates", "dvig"),
        default="md-aggregates",
        help="md-aggregates — partno из MD_Components group_by>2 (default); dvig — part_special",
    )
    parser.add_argument(
        "--latest-dwh",
        action="store_true",
        help="report-date = последняя доступная дата в DWH (reports / od_detail)",
    )
    parser.add_argument(
        "--gb4-full",
        action="store_true",
        help="Только group_by=4: полный Excel с расшифровками (README, дубли, vendor/date stats)",
    )
    parser.add_argument(
        "--all-groups",
        action="store_true",
        help="Все group_by>2 из MD: один Excel, 2 листа на группу (gNN_svod + gNN_list)",
    )
    args = parser.parse_args()
    if args.latest_dwh:
        report_date = resolve_latest_dwh_date()
    else:
        report_date = args.report_date or resolve_latest_dataset_date()
    out = Path(args.out) if args.out else None
    if args.all_groups:
        export_all_groups_excel(report_date, out, mode=args.mode)
    elif args.gb4_full:
        export_gb4_full_excel(report_date, out, mode=args.mode)
    else:
        export_engine_repair(report_date, out, mode=args.mode, partno_filter=args.partno_filter)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
