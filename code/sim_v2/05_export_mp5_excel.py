#!/usr/bin/env python3
"""
V2 Utility: экспорт MP5 в Excel для отладки
- Лист 1 (long): day, aircraft_number, idx, daily_hours
- Лист 2 (matrix sample): первые N_ac по строкам и N_days по столбцам (для визуальной проверки)
"""
from __future__ import annotations

import os
import sys
from typing import Dict, List

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from sim_env_setup import get_client, prepare_env_arrays


def main() -> int:
    import pandas as pd

    client = get_client()
    env = prepare_env_arrays(client)
    DAYS = int(env['days_total_u16'])
    FRAMES = int(env['frames_total_u16'])
    frames_index = env['frames_index']  # ac -> idx
    inv_index = {i: ac for ac, i in frames_index.items()}

    # Линеаризованный MP5
    arr = list(env['mp5_daily_hours_linear'])
    assert len(arr) == (DAYS + 1) * FRAMES, "mp5 length != (DAYS+1)*FRAMES"

    # Long view по дням: будем писать чанками по дням, чтобы не превысить лимит Excel
    def build_long_chunk(d_from: int, d_to: int) -> 'pd.DataFrame':
        rows: List[Dict[str, int]] = []
        for d in range(d_from, d_to):
            base = d * FRAMES
            for i in range(FRAMES):
                v = int(arr[base + i] or 0)
                if v != 0:
                    rows.append({
                        "day": d,
                        "aircraft_number": inv_index.get(i, 0),
                        "idx": i,
                        "daily_hours": v
                    })
        import pandas as _pd
        df = _pd.DataFrame(rows)
        if not df.empty:
            df.sort_values(["day", "aircraft_number"], inplace=True)
        return df

    # Matrix sample (первые 30 дней × первые 30 бортов)
    Dn = min(30, DAYS)
    Fn = min(30, FRAMES)
    mat = []
    header = ["idx/ac"] + [str(d) for d in range(Dn)]
    for i in range(Fn):
        row = [inv_index.get(i, 0)]
        for d in range(Dn):
            row.append(int(arr[d * FRAMES + i] or 0))
        mat.append(row)
    df_mat = pd.DataFrame(mat, columns=header)

    out_xlsx = os.environ.get("HL_V2_MP5_XLSX", "tmp/mp5_export.xlsx")
    os.makedirs(os.path.dirname(out_xlsx), exist_ok=True)
    with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as xw:
        # Long чанками по 1000 дней (настраивается через env)
        days_per_sheet = int(os.environ.get("HL_V2_MP5_DAYS_PER_SHEET", "1000"))
        written = 0
        start = 0
        while start < DAYS:
            end = min(start + days_per_sheet, DAYS)
            df_long_chunk = build_long_chunk(start, end)
            sheet_name = f"mp5_long_{start}_{end-1}"
            df_long_chunk.to_excel(xw, index=False, sheet_name=sheet_name)
            written += len(df_long_chunk)
            start = end
        # Матричный сэмпл отдельным листом
        df_mat.to_excel(xw, index=False, sheet_name="mp5_matrix_sample")
    print(f"saved: {out_xlsx}")
    print(f"rows_long_total={written} sample_matrix=({len(df_mat)}x{len(df_mat.columns)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())


