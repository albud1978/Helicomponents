#!/usr/bin/env python3
"""
V2 Step 01: setup_env
- Загружает исходные массивы из ClickHouse
- Строит плотный frames_index по (MP3_planes ∪ MP5_planes)
- Вычисляет служебные индексы (frames_union_no_future, first_reserved_idx, base_acn_spawn, first_future_idx)
- Формирует минимальный набор Env‑массивов под последующие RTC‑шаги
- Проводит строгие валидации размерностей

Выводит сводку в stdout; по флагу сохраняет JSON‑снимок для последующих шагов.
"""
from __future__ import annotations

import os
import sys
import json
import argparse
from typing import Dict, List, Tuple
from datetime import date as _date

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from sim_env_setup import (
    get_client,
    fetch_versions,
    fetch_mp3,
    preload_mp4_by_day,
    preload_mp5_maps,
    build_frames_index,
    build_mp4_arrays,
    build_mp5_linear,
    days_to_epoch_u16,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="V2 setup_env")
    p.add_argument("--dump-json", default="", help="Путь для сохранения JSON‑снимка Env (опционально)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    client = get_client()

    vdate, vid = fetch_versions(client)
    mp3_rows, mp3_fields = fetch_mp3(client, vdate, vid)
    mp4_by_day = preload_mp4_by_day(client)
    mp5_by_day = preload_mp5_maps(client)

    days_sorted: List[_date] = sorted(mp4_by_day.keys())
    DAYS = len(days_sorted)

    # Плотный индекс по планёрам: MP3 → доп. из MP5
    frames_index_mp3, _ = build_frames_index(mp3_rows, mp3_fields)
    ac_mp3_ordered = [ac for ac, _ in sorted(frames_index_mp3.items(), key=lambda kv: kv[1])]
    ac_mp5_set = set()
    for _, by_ac in mp5_by_day.items():
        for ac in by_ac.keys():
            try:
                ai = int(ac)
                if ai > 0:
                    ac_mp5_set.add(ai)
            except Exception:
                continue
    extra_from_mp5 = sorted([ac for ac in ac_mp5_set if ac not in frames_index_mp3])
    ac_union = list(ac_mp3_ordered) + extra_from_mp5
    frames_index: Dict[int, int] = {ac: i for i, ac in enumerate(ac_union)}
    frames_union_no_future = len(ac_union)
    reserved_slots_count = len(extra_from_mp5)
    first_reserved_idx = max(0, frames_union_no_future - reserved_slots_count)

    # Базовый ACN для будущего спавна: > max(existing), но не ниже 100000
    max_existing_acn = max(ac_union) if ac_union else 0
    base_acn_spawn = max(100000, max_existing_acn + 1)
    first_future_idx = int(frames_index.get(base_acn_spawn, frames_union_no_future))

    # FRAMES = |MP3 ∪ MP5| на этапе 01 (без будущих)
    FRAMES = frames_union_no_future
    mp4_ops8, mp4_ops17 = build_mp4_arrays(mp4_by_day, days_sorted)
    mp5_linear = build_mp5_linear(mp5_by_day, days_sorted, frames_index, FRAMES)

    # Валидации
    assert DAYS > 0 and FRAMES > 0, "Пустой горизонт или кадры"
    assert len(mp4_ops8) == DAYS, "mp4_ops_counter_mi8 length != DAYS"
    assert len(mp4_ops17) == DAYS, "mp4_ops_counter_mi17 length != DAYS"
    assert len(mp5_linear) == (DAYS + 1) * FRAMES, "mp5_daily_hours length != (DAYS+1)*FRAMES"
    assert len(frames_index) == FRAMES, "frames_index density mismatch"

    # Сводка
    print(f"version_date_u16={days_to_epoch_u16(vdate)}")
    print(f"DAYS={DAYS}, FRAMES={FRAMES}")
    print(f"frames_union_no_future={frames_union_no_future}")
    print(f"reserved_slots_count={reserved_slots_count}, first_reserved_idx={first_reserved_idx}")
    print(f"base_acn_spawn={base_acn_spawn}, first_future_idx={first_future_idx}")
    inv = {i: ac for ac, i in frames_index.items()}
    head = [(i, inv.get(i, -1)) for i in range(min(5, FRAMES))]
    tail = [(i, inv.get(i, -1)) for i in range(max(0, FRAMES - 5), FRAMES)]
    print(f"frames_index head={head}")
    print(f"frames_index tail={tail}")
    print(f"len(mp4_ops_counter_mi8)={len(mp4_ops8)} len(mp4_ops_counter_mi17)={len(mp4_ops17)}")
    print(f"len(mp5_daily_hours_linear)={(len(mp5_linear))} expected={(DAYS+1)*FRAMES}")

    if args.dump_json:
        out = {
            "version_date_u16": days_to_epoch_u16(vdate),
            "days_total_u16": DAYS,
            "frames_total_u16": FRAMES,
            "frames_index": frames_index,
            "frames_union_no_future": frames_union_no_future,
            "reserved_slots_count": reserved_slots_count,
            "first_reserved_idx": first_reserved_idx,
            "base_acn_spawn": base_acn_spawn,
            "first_future_idx": first_future_idx,
        }
        os.makedirs(os.path.dirname(args.dump_json), exist_ok=True)
        with open(args.dump_json, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print(f"saved: {args.dump_json}")

    return 0


if __name__ == "__main__":
    sys.exit(main())


