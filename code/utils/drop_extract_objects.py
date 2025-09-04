#!/usr/bin/env python3
"""
Утилита: безопасное удаление объектов ClickHouse, относящихся к Extract Master

Удаляются ТОЛЬКО таблицы и Dictionary, которые создаёт текущий Extract пайплайн.
Ничего лишнего вне проекта не трогаем.

Запуск:
  python3 code/utils/drop_extract_objects.py
"""

from pathlib import Path
import sys


def main() -> int:
    # Подключение к ClickHouse
    sys.path.append(str(Path(__file__).parent))
    from config_loader import get_clickhouse_client  # type: ignore

    client = get_clickhouse_client()
    print("🔧 Drop phase start (Extract objects only)")

    # Список Dictionary объектов (system.dictionaries.name)
    dict_objects = [
        'aircraft_number_dictionary',      # legacy Dictionary объект
        'status_dict_flat',                # словарь статусов
        'partno_dict_flat',                # партномера
        'serialno_dict_flat',              # серийники
        'owner_dict_flat',                 # владельцы
        'ac_type_dict_flat',               # типы ВС
        'aircraft_number_dict_flat',       # номера ВС
        'digital_values_dict_flat',        # цифровые значения полей
    ]

    # Список таблиц (EXTRACT пайплайн)
    tables = [
        'heli_pandas', 'heli_raw',
        'md_components', 'status_overhaul', 'program_ac',
        'flight_program_fl', 'flight_program_ac',
        'dict_aircraft_number_flat', 'dict_status_flat',
    ]

    # 1) Удаляем Dictionary объекты, если существуют
    for dname in dict_objects:
        try:
            exists = client.execute(
                "SELECT COUNT(*) FROM system.dictionaries WHERE database = currentDatabase() AND name = %(n)s",
                {"n": dname},
            )[0][0] > 0
            if exists:
                client.execute(f"DROP DICTIONARY {dname}")
                print(f"✅ Dropped dictionary: {dname}")
        except Exception as e:
            print(f"⚠️ Skip dict {dname}: {e}")

    # 2) Удаляем таблицы, если существуют
    for tname in tables:
        try:
            exists = client.execute(f"EXISTS TABLE {tname}")[0][0]
            if exists:
                client.execute(f"DROP TABLE {tname}")
                print(f"✅ Dropped table: {tname}")
        except Exception as e:
            print(f"⚠️ Skip table {tname}: {e}")

    print("🔧 Drop phase done")
    return 0


if __name__ == "__main__":
    sys.exit(main())


