#!/usr/bin/env python3
"""Валидация резервирования psn для симуляционных рождений агрегатов.

Проверяет инварианты диапазона, зафиксированного шагом ETL
md_components_psn_reserve.py, и контракт выдачи идентификаторов, который
реализует будущий GPU-счётчик L2/L3 spawn:

    psn(i) = psn_spawn_base + i,   i = atomicInc() начиная с 0

Инварианты:
- глобальная база едина для всех номенклатур;
- база строго выше реального max(psn) из AMOS (heli_pandas) — зазор есть;
- выданные за горизонт psn уникальны, не пересекаются с реальными и
  укладываются в UInt32.

Запуск: python3 code/sim_v2/test_psn_spawn_reservation.py
        либо через pytest.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from utils.config_loader import get_clickhouse_client

UINT32_MAX = 0xFFFFFFFF
# Запас рождений за горизонт с многократным запасом: ~46 бортов × 108 агрегатов
# ≈ 5k за прогон; берём 100k, чтобы покрыть замены при ремонтах и рост сценариев.
HORIZON_BIRTHS = 100_000


def _global_psn_base(client) -> int:
    distinct, base = client.execute(
        "SELECT countDistinct(psn_spawn_start), min(psn_spawn_start) FROM md_components"
    )[0]
    if distinct != 1:
        raise AssertionError(f"psn_spawn_start не глобально-единый: distinct={distinct}")
    return int(base)


def _real_psn(client):
    real_max = client.execute(
        "SELECT max(psn) FROM heli_pandas WHERE psn IS NOT NULL"
    )[0][0] or 0
    real_set = {
        r[0]
        for r in client.execute(
            "SELECT DISTINCT psn FROM heli_pandas WHERE psn IS NOT NULL"
        )
    }
    return int(real_max), real_set


def test_psn_reservation_contract():
    client = get_clickhouse_client()
    base = _global_psn_base(client)
    real_max, real_set = _real_psn(client)

    assert base > real_max, f"base={base} не выше real max(psn)={real_max}"

    allocated = [base + i for i in range(HORIZON_BIRTHS)]
    assert len(set(allocated)) == HORIZON_BIRTHS, "выданные psn не уникальны"
    assert min(allocated) >= base, "выдан psn ниже базы"
    assert max(allocated) <= UINT32_MAX, "psn вышел за UInt32"
    assert max(allocated) < base * 10, "горизонт подозрительно близок к следующему порядку"
    collisions = set(allocated) & real_set
    assert not collisions, f"пересечение с реальными psn: {sorted(collisions)[:5]}"

    print(
        f"✅ base={base:,}; real max(psn)={real_max:,}; зазор={base - real_max:,}; "
        f"выдано {HORIZON_BIRTHS:,} уникальных psn без пересечений с реальными "
        f"(max={max(allocated):,} < UInt32)"
    )


if __name__ == "__main__":
    test_psn_reservation_contract()
    print("🎯 Резервирование psn: все инварианты выполнены")
