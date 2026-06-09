#!/usr/bin/env python3
"""Валидация per-номенклатурного резервирования psn для симуляционных рождений.

Проверяет карту блоков, зафиксированную шагом ETL md_components_psn_reserve.py,
и контракт выдачи, который реализует будущий GPU-счётчик L2/L3 spawn:

    psn = psn_spawn_start(partseqno_i) + counter(partseqno_i),   counter per-номенклатура с 0
    block   = (psn - PSN_SPAWN_BASE) // PSN_BLOCK_STRIDE
    counter = (psn - PSN_SPAWN_BASE) %  PSN_BLOCK_STRIDE

Инварианты:
- блоки начинаются выше реального max(psn) из AMOS, единый страйд, все различны;
- рождения внутри каждого блока уникальны, не выходят за границы блока;
- блоки разных номенклатур не пересекаются между собой;
- ни один выданный psn не совпадает с реальным.

Запуск: python3 code/sim_v2/test_psn_spawn_reservation.py  (или pytest).
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from utils.config_loader import get_clickhouse_client

UINT32_MAX = 0xFFFFFFFF
PSN_SPAWN_BASE = 10_000_000
PSN_BLOCK_STRIDE = 1_000_000
# Число рождений на номенклатуру в тесте: сильно выше реалистичного (макс парк ~1100),
# но внутри блока (страйд 1M).
BIRTHS_PER_NOM = 5_000


def _block_map(client):
    return {
        int(ps): int(start)
        for ps, start in client.execute(
            "SELECT partseqno_i, psn_spawn_start FROM md_components "
            "WHERE partseqno_i IS NOT NULL AND psn_spawn_start > 0"
        )
    }


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


def test_psn_block_map():
    """Карта блоков: выше реального max, единый страйд, все различны, в UInt32."""
    client = get_clickhouse_client()
    blocks = _block_map(client)
    real_max, _ = _real_psn(client)
    starts = sorted(blocks.values())

    assert min(starts) > real_max, f"блок ниже real max(psn)={real_max}"
    assert len(set(starts)) == len(starts), "psn_spawn_start не уникальны"
    assert min(starts) == PSN_SPAWN_BASE, f"первый блок != {PSN_SPAWN_BASE}"
    steps = {b - a for a, b in zip(starts, starts[1:])}
    assert steps == {PSN_BLOCK_STRIDE}, f"страйд неоднороден: {steps}"
    assert max(starts) + PSN_BLOCK_STRIDE <= UINT32_MAX, "блоки выходят за UInt32"
    print(f"✅ карта: {len(blocks)} блоков {min(starts):,}…{max(starts):,}, страйд {PSN_BLOCK_STRIDE:,}")


def test_psn_births_contract():
    """Рождения по блокам: уникальны, в границах своего блока, без пересечений и real-коллизий."""
    client = get_clickhouse_client()
    blocks = _block_map(client)
    real_max, real_set = _real_psn(client)

    assert BIRTHS_PER_NOM < PSN_BLOCK_STRIDE, "рождений в тесте больше ширины блока"

    all_psn = set()
    for ps, start in blocks.items():
        block_psn = [start + i for i in range(BIRTHS_PER_NOM)]
        assert all(start <= p < start + PSN_BLOCK_STRIDE for p in block_psn), \
            f"psn вышел за блок номенклатуры {ps}"
        assert all((p - PSN_SPAWN_BASE) // PSN_BLOCK_STRIDE == (start - PSN_SPAWN_BASE) // PSN_BLOCK_STRIDE
                   for p in block_psn), f"psn попал в чужой блок (номенклатура {ps})"
        before = len(all_psn)
        all_psn.update(block_psn)
        assert len(all_psn) - before == BIRTHS_PER_NOM, f"пересечение блоков на номенклатуре {ps}"

    assert min(all_psn) > real_max, "выдан psn не выше real max"
    assert max(all_psn) <= UINT32_MAX, "psn вышел за UInt32"
    collisions = all_psn & real_set
    assert not collisions, f"пересечение с реальными psn: {sorted(collisions)[:5]}"
    print(
        f"✅ {len(blocks)} номенклатур × {BIRTHS_PER_NOM:,} рождений = {len(all_psn):,} psn: "
        f"уникальны, каждый в своём блоке, без пересечений с реальными"
    )


if __name__ == "__main__":
    test_psn_block_map()
    test_psn_births_contract()
    print("🎯 Per-номенклатурное резервирование psn: все инварианты выполнены")
