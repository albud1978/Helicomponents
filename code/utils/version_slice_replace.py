"""Exact-tuple ClickHouse replacement for versioned pandas DataFrames."""

from __future__ import annotations

import re
from datetime import date

import pandas as pd

_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def replace_version_slice(
    client,
    table: str,
    df: pd.DataFrame,
    version_date: date,
    version_id: int,
) -> int:
    """DELETE exact (version_date, version_id), INSERT, then verify row count."""
    if not _TABLE_RE.fullmatch(table):
        raise ValueError(f"invalid ClickHouse table name: {table!r}")
    required = {"version_date", "version_id"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{table}: missing version columns {sorted(missing)}")
    if df.empty:
        raise ValueError(f"{table}: refusing to replace slice with empty DataFrame")

    frame_dates = pd.to_datetime(df["version_date"], errors="raise").dt.date.unique()
    frame_ids = pd.to_numeric(df["version_id"], errors="raise").astype("int64").unique()
    if len(frame_dates) != 1 or frame_dates[0] != version_date:
        raise ValueError(
            f"{table}: expected one version_date={version_date}, found {frame_dates.tolist()}"
        )
    if len(frame_ids) != 1 or int(frame_ids[0]) != int(version_id):
        raise ValueError(
            f"{table}: expected one version_id={version_id}, found {frame_ids.tolist()}"
        )

    params = {"vd": version_date, "vi": int(version_id)}
    client.execute(
        f"DELETE FROM {table} "
        "WHERE version_date = %(vd)s AND version_id = %(vi)s",
        params,
    )
    columns = ", ".join(f"`{column}`" for column in df.columns)
    values = [tuple(row) for row in df.values]
    client.execute(f"INSERT INTO {table} ({columns}) VALUES", values)
    count = int(
        client.execute(
            f"SELECT count() FROM {table} "
            "WHERE version_date = %(vd)s AND version_id = %(vi)s",
            params,
        )[0][0]
    )
    if count != len(values):
        raise RuntimeError(f"{table}: inserted count {count}, expected {len(values)}")
    return count
