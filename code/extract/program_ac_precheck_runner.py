#!/usr/bin/env python3
"""
Program AC Precheck Runner

Минимально-инвазивный микросервис: запускает D1 precheck после формирования тензора FL,
используя уже существующую функцию process_program_ac_precheck_d1(). Работает только в явно
переданном скоупе version_date/version_id.
"""

import argparse
import sys
from datetime import date
from pathlib import Path


def _parse_version_date(raw: str) -> date:
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--version-date должен быть в формате YYYY-MM-DD") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Program AC D1 precheck для одной версии heli_pandas."
    )
    parser.add_argument("--version-date", required=True, type=_parse_version_date)
    parser.add_argument("--version-id", required=True, type=int)
    return parser.parse_args()


def main() -> int:
    print("🚀 === PROGRAM AC PRECHECK RUNNER ===")
    args = parse_args()
    try:
        # Подключение к ClickHouse
        code_root = Path(__file__).resolve().parents[1]
        sys.path.append(str(code_root / 'utils'))
        sys.path.append(str(code_root))
        from config_loader import get_clickhouse_client
        client = get_clickhouse_client()

        # Проверяем необходимые таблицы
        checks = {
            'heli_pandas': "EXISTS TABLE heli_pandas",
            'md_components': "EXISTS TABLE md_components",
            'flight_program_fl': "EXISTS TABLE flight_program_fl",
        }
        for name, sql in checks.items():
            if client.execute(sql)[0][0] == 0:
                print(f"❌ Таблица {name} отсутствует — precheck невозможен")
                return 1

        # Импортируем функцию precheck
        from extract.program_ac_precheck_next_day import process_program_ac_precheck_d1

        # Загружаем данные heli_pandas в память
        import pandas as pd
        rows = client.execute(
            """
            SELECT 
                partno, serialno, ac_typ, location,
                mfg_date, removal_date, target_date,
                condition, owner, lease_restricted,
                oh, oh_threshold, ll, sne, ppr,
                version_date, version_id, partseqno_i, psn, address_i, ac_type_i,
                status_id, repair_days, aircraft_number, ac_type_mask, group_by
            FROM heli_pandas
            WHERE version_date = %(vd)s AND version_id = %(vid)s
            """,
            {"vd": args.version_date, "vid": args.version_id},
        )
        cols = [
            'partno','serialno','ac_typ','location',
            'mfg_date','removal_date','target_date',
            'condition','owner','lease_restricted',
            'oh','oh_threshold','ll','sne','ppr',
            'version_date','version_id','partseqno_i','psn','address_i','ac_type_i',
            'status_id','repair_days','aircraft_number','ac_type_mask','group_by'
        ]
        df = pd.DataFrame(rows, columns=cols)
        print(f"📦 heli_pandas в памяти: {len(df):,} записей")

        if len(df) == 0:
            print("ℹ️ heli_pandas пуст — precheck не требуется")
            return 0

        old = df['status_id'].to_numpy(copy=True)

        # Выполняем precheck
        updated_df = process_program_ac_precheck_d1(df, client, args.version_date)

        # Применяем изменения статуса
        import numpy as np
        new = updated_df['status_id'].to_numpy()
        changed_idx = np.where(old != new)[0]
        changed = int(changed_idx.size)
        print(f"🔄 Изменений статуса: {changed}")

        for idx in changed_idx.tolist():
            serialno = updated_df.at[idx, 'serialno']
            status_id = int(updated_df.at[idx, 'status_id'] or 0)
            client.execute(
                """
                ALTER TABLE heli_pandas
                UPDATE status_id = %(s)s
                WHERE serialno = %(serialno)s
                  AND version_date = %(vd)s
                  AND version_id = %(vid)s
                """,
                {
                    "s": status_id,
                    "serialno": serialno,
                    "vd": args.version_date,
                    "vid": args.version_id,
                },
            )

        print("✅ Precheck применён")
        return 0

    except Exception as e:
        print(f"❌ Ошибка precheck runner: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
