#!/usr/bin/env python3
"""
Program AC Precheck Runner

Минимально-инвазивный микросервис: запускает D1 precheck после формирования тензора FL,
используя уже существующую функцию process_program_ac_precheck_d1(). Если flight_program_fl
отсутствует, шаг тихо пропускается.
"""

import sys
from pathlib import Path


def main() -> int:
    print("🚀 === PROGRAM AC PRECHECK RUNNER ===")
    try:
        # Подключение к ClickHouse
        sys.path.append(str(Path(__file__).parent / 'utils'))
        from config_loader import get_clickhouse_client
        client = get_clickhouse_client()

        # Проверяем необходимые таблицы
        checks = {
            'heli_pandas': "EXISTS TABLE heli_pandas",
            'md_components': "EXISTS TABLE md_components",
            'flight_program_fl': "EXISTS TABLE flight_program_fl",
        }
        for name, sql in checks.items():
            try:
                if client.execute(sql)[0][0] == 0:
                    print(f"ℹ️ Таблица {name} отсутствует — precheck пропускаем")
                    return 0
            except Exception:
                print(f"ℹ️ Проверка таблицы {name} завершилась ошибкой — precheck пропускаем")
                return 0

        # Импортируем функцию precheck
        sys.path.append(str(Path(__file__).parent))
        from program_ac_precheck_next_day import process_program_ac_precheck_d1

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
            """
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

        # Выполняем precheck
        updated_df = process_program_ac_precheck_d1(df, client)

        # Применяем изменения статуса
        import numpy as np
        old = df['status_id'].to_numpy()
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
                """,
                {"s": status_id, "serialno": serialno},
            )

        print("✅ Precheck применён")
        return 0

    except Exception as e:
        print(f"❌ Ошибка precheck runner: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())


