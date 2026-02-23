#!/usr/bin/env python3
"""
RepairLine Export: чтение GPU-буферов и ежедневная интерполяция.

Аналог rtc_mp2_export.py, но для RepairLine агентов.
GPU записывает состояние (free_days, acn, rt, bank telemetry) каждый адаптивный шаг
в MacroProperty буферы rl_buf_*.

HF_RepairLineDrain читает буферы на финальном шаге.
interpolate_repairline_daily() разворачивает адаптивные шаги в ежедневную матрицу.
export_repairline_to_ch() отправляет результат в ClickHouse (sim_repairline_v9).

Lookback-only: occupancy (aircraft_number/group_by) берётся из sim_masterv2_v9.
Runtime telemetry (acn/gb) не используется как источник occupancy, только для определения line_id
в claimless эпизодах. Метрики ремонта строятся по free_days и repair_time, bank telemetry сохраняются.
"""

import sys
import os
from collections import defaultdict
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from model_build import MAX_EXPORT_STEPS, REPAIR_LINES_MAX, RL_BUF_SIZE

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu not installed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# HF_RepairLineDrain: читает RL буферы на финальном шаге
# ═══════════════════════════════════════════════════════════════════════════════

class HF_RepairLineDrain(fg.HostFunction):
    """
    HostFunction: на каждом шаге — fast return.
    На финальном шаге (current_day >= end_day) — читает rl_buf_* буферы
    в numpy arrays shape=(num_steps, repair_quota).
    """

    def __init__(self, end_day: int, repair_quota: int):
        super().__init__()
        self.end_day = end_day
        self.repair_quota = repair_quota
        self.data = None  # Заполняется на финальном шаге

    def run(self, FLAMEGPU):
        env = FLAMEGPU.environment
        mp_day = env.getMacroPropertyUInt("current_day_mp")
        current_day = int(mp_day[0])

        if current_day < self.end_day:
            return  # Не финальный шаг

        # Финальный шаг — чтение буферов
        import numpy as np

        # Количество шагов (из mp2_num_steps)
        try:
            mp_num = env.getMacroPropertyUInt("mp2_num_steps")
            num_steps = int(mp_num[0])
        except Exception as e:
            print(f"  ⚠️ [RL Drain] Ошибка чтения mp2_num_steps: {e}")
            num_steps = 0
        if num_steps == 0:
            num_steps = FLAMEGPU.getStepCounter() + 1
        if num_steps > MAX_EXPORT_STEPS:
            num_steps = MAX_EXPORT_STEPS

        print(f"  [RL Drain] Чтение {num_steps} шагов × {self.repair_quota} линий...")

        # Маппинг шагов в дни (переиспользуем mp2_day_for_step)
        mp_days = env.getMacroPropertyUInt("mp2_day_for_step")
        days = [int(mp_days[s]) for s in range(num_steps)]

        # Чтение 7 буферов
        mp_fd = env.getMacroPropertyUInt("rl_buf_free_days")
        mp_acn = env.getMacroPropertyUInt("rl_buf_acn")
        mp_rt = env.getMacroPropertyUInt("rl_buf_rt")
        mp_gb = env.getMacroPropertyUInt("rl_buf_gb")
        mp_bank_count = env.getMacroPropertyUInt("rl_buf_bank_count")
        mp_bank_head_start = env.getMacroPropertyUInt("rl_buf_bank_head_start")
        mp_bank_head_end = env.getMacroPropertyUInt("rl_buf_bank_head_end")

        free_days = np.zeros((num_steps, self.repair_quota), dtype=np.uint32)
        acn = np.zeros((num_steps, self.repair_quota), dtype=np.uint32)
        rt = np.zeros((num_steps, self.repair_quota), dtype=np.uint32)
        gb = np.zeros((num_steps, self.repair_quota), dtype=np.uint32)
        bank_count = np.zeros((num_steps, self.repair_quota), dtype=np.uint32)
        bank_head_start = np.zeros((num_steps, self.repair_quota), dtype=np.uint32)
        bank_head_end = np.zeros((num_steps, self.repair_quota), dtype=np.uint32)

        for s in range(num_steps):
            base = s * REPAIR_LINES_MAX
            for l in range(self.repair_quota):
                free_days[s, l] = int(mp_fd[base + l]) & 0xFFFFFFFF
                acn[s, l] = int(mp_acn[base + l]) & 0xFFFFFFFF
                rt[s, l] = int(mp_rt[base + l]) & 0xFFFFFFFF
                gb[s, l] = int(mp_gb[base + l]) & 0xFFFFFFFF
                bank_count[s, l] = int(mp_bank_count[base + l]) & 0xFFFFFFFF
                bank_head_start[s, l] = int(mp_bank_head_start[base + l]) & 0xFFFFFFFF
                bank_head_end[s, l] = int(mp_bank_head_end[base + l]) & 0xFFFFFFFF

        self.data = {
            'num_steps': num_steps,
            'days': days,
            'free_days': free_days,
            'acn': acn,
            'rt': rt,
            'gb': gb,
            'bank_count': bank_count,
            'bank_head_start': bank_head_start,
            'bank_head_end': bank_head_end,
        }

        total = num_steps * self.repair_quota * 7
        print(f"  [RL Drain] ✅ Прочитано {total:,} значений")


def register_repairline_drain(model, end_day: int, repair_quota: int):
    """Регистрирует HF_RepairLineDrain как layer host function."""
    drain = HF_RepairLineDrain(end_day, repair_quota)
    layer = model.newLayer("layer_repairline_drain")
    layer.addHostFunction(drain)
    print(f"  ✅ RepairLine Drain зарегистрирован (repair_quota={repair_quota})")
    return drain


# ═══════════════════════════════════════════════════════════════════════════════
# Интерполяция: адаптивные шаги → ежедневная матрица
# ═══════════════════════════════════════════════════════════════════════════════

def interpolate_repairline_daily(data, repair_quota):
    """
    Разворачивает адаптивные снимки RepairLine в ежедневные строки.

    Для каждой линии line_id (0..repair_quota-1):
    - Между шагами S (day D1) и S+1 (day D2):
      - free_days растёт на +1/день от значения на шаге S
    - lookback-only: aircraft_number — runtime telemetry (не используется как forward occupancy)
    - group_by и bank telemetry сохраняются из GPU-снимка линии на шаге S

    Returns: list[tuple] — (day_u16, line_id, free_days, repair_time, aircraft_number, group_by,
                            bank_count, bank_head_start, bank_head_end)
    """
    num_steps = data['num_steps']
    days = data['days']
    fd_arr = data['free_days']
    rt_arr = data['rt']
    gb_arr = data['gb']
    acn_arr = data['acn']
    bank_count_arr = data['bank_count']
    bank_head_start_arr = data['bank_head_start']
    bank_head_end_arr = data['bank_head_end']

    rows = []

    for line_id in range(repair_quota):
        for s in range(num_steps):
            d1 = days[s]
            d2 = days[s + 1] if s + 1 < num_steps else d1 + 1

            fd_s = int(fd_arr[s, line_id])
            rt_s = int(rt_arr[s, line_id])
            acn_s = int(acn_arr[s, line_id])

            daily_gb = int(gb_arr[s, line_id])
            daily_bank_count = int(bank_count_arr[s, line_id])
            daily_bank_head_start = int(bank_head_start_arr[s, line_id])
            daily_bank_head_end = int(bank_head_end_arr[s, line_id])
            for d in range(d1, d2):
                daily_fd = fd_s + (d - d1)
                daily_rt = rt_s
                rows.append(
                    (
                        d,
                        line_id,
                        daily_fd,
                        daily_rt,
                        acn_s,
                        daily_gb,
                        daily_bank_count,
                        daily_bank_head_start,
                        daily_bank_head_end,
                    )
                )

    return rows


def _build_repair_episodes(master_trace):
    """
    master_trace: list of (day_u16, status_id, pre_status_id) sorted by day_u16.
    Returns list of (start_day, end_day) with end exclusive.
    """
    if not master_trace:
        return []

    episodes = []
    in_episode = False
    start_day = None
    last_day = None

    for day_val, status_id, pre_status_id in master_trace:
        day_val = int(day_val)
        status_val = int(status_id)
        pre_val = int(pre_status_id)
        last_day = day_val

        if not in_episode and status_val == 4:
            in_episode = True
            start_day = day_val
            continue

        if in_episode and pre_val == 4 and status_val in (2, 3):
            episodes.append((start_day, day_val))
            in_episode = False
            start_day = None

    if in_episode and start_day is not None:
        episodes.append((start_day, last_day + 1))

    return episodes


# ═══════════════════════════════════════════════════════════════════════════════
# Экспорт в ClickHouse
# ═══════════════════════════════════════════════════════════════════════════════

DDL_REPAIRLINE = """
CREATE TABLE IF NOT EXISTS sim_repairline_v9 (
    version_date UInt32,
    version_id   UInt32,
    day_u16      UInt16,
    day_date     Date MATERIALIZED addDays(toDate(toString(version_date)), toUInt16(day_u16)),
    line_id      UInt8,
    free_days    UInt32,
    repair_time  UInt32,
    aircraft_number UInt32,
    group_by     UInt8,
    bank_count   UInt32,
    bank_head_start UInt32,
    bank_head_end UInt32
) ENGINE = MergeTree()
PARTITION BY (version_date, toYYYYMM(day_date))
ORDER BY (version_id, version_date, day_u16, line_id)
SETTINGS index_granularity = 8192
"""


def export_repairline_to_ch(ch_client, rows, version_date_int, version_id, drop_table=False):
    """
    Экспорт ежедневной матрицы RepairLine в ClickHouse.

    Args:
        ch_client: clickhouse_driver.Client
        rows: list[tuple] — (day_u16, line_id, free_days, repair_time, aircraft_number, group_by,
                            bank_count, bank_head_start, bank_head_end)
        version_date_int: int — YYYYMMDD
        version_id: int
        drop_table: bool — дропнуть таблицу перед созданием

    Lookback-only: occupancy (aircraft_number/group_by) детерминированно строится из sim_masterv2_v9.
    Runtime telemetry (acn/gb) не используется как источник occupancy, только для выбора line_id
    для claimless эпизодов. bank telemetry сохраняется из GPU-снимка линии.
    """
    if drop_table:
        ch_client.execute("DROP TABLE IF EXISTS sim_repairline_v9")
        print("  🗑️ sim_repairline_v9 удалена")

    ch_client.execute(DDL_REPAIRLINE)
    ch_client.execute(
        "ALTER TABLE sim_repairline_v9 "
        "ADD COLUMN IF NOT EXISTS day_date Date MATERIALIZED "
        "addDays(toDate(toString(version_date)), toUInt16(day_u16))"
    )
    ch_client.execute(
        "ALTER TABLE sim_repairline_v9 "
        "ADD COLUMN IF NOT EXISTS group_by UInt8"
    )
    ch_client.execute(
        "ALTER TABLE sim_repairline_v9 "
        "ADD COLUMN IF NOT EXISTS repair_time UInt32"
    )
    ch_client.execute(
        "ALTER TABLE sim_repairline_v9 "
        "ADD COLUMN IF NOT EXISTS bank_count UInt32"
    )
    ch_client.execute(
        "ALTER TABLE sim_repairline_v9 "
        "ADD COLUMN IF NOT EXISTS bank_head_start UInt32"
    )
    ch_client.execute(
        "ALTER TABLE sim_repairline_v9 "
        "ADD COLUMN IF NOT EXISTS bank_head_end UInt32"
    )

    master_rows = ch_client.execute(
        "SELECT aircraft_number, group_by, day_u16, status_id, pre_status_id, "
        "commit_p2, commit_p3, repair_claim_line_id, repair_claim_start_day, "
        "repair_claim_end_day, repair_claim_source "
        "FROM sim_masterv2_v9 "
        "WHERE version_date=%(vd)s AND version_id=%(vid)s",
        {'vd': version_date_int, 'vid': version_id},
    )

    acn_map = {}
    master_trace_by_acn = defaultdict(list)
    claim_events = []
    for (
        acn, gb, day, status_id, pre_status_id, commit_p2, commit_p3,
        claim_line_id, claim_start_day, claim_end_day, claim_source
    ) in master_rows:
        acn_val = int(acn)
        gb_val = int(gb)
        if acn_val in acn_map and acn_map[acn_val] != gb_val:
            raise RuntimeError(
                "sim_masterv2_v9 group_by conflict for aircraft_number="
                f"{acn_val}: {acn_map[acn_val]} vs {gb_val} "
                f"(version_date={version_date_int}, version_id={version_id})"
            )
        acn_map[acn_val] = gb_val

        day_val = int(day)
        status_val = int(status_id)
        pre_val = int(pre_status_id)
        master_trace_by_acn[acn_val].append((day_val, status_val, pre_val))

        if int(commit_p2) == 1 or int(commit_p3) == 1:
            claim_source_val = int(claim_source)
            line_val = int(claim_line_id)
            start_val = int(claim_start_day)
            end_val = int(claim_end_day)
            if (
                claim_source_val in (1, 2)
                and line_val != 65535
                and start_val != 65535
                and end_val != 65535
                and end_val > start_val
            ):
                claim_events.append((acn_val, gb_val, line_val, start_val, end_val))

    runtime_lines_by_acn = defaultdict(lambda: defaultdict(set))
    for day, lid, _fd, _rt, runtime_acn, _gb, _bank_count, _bank_head_start, _bank_head_end in rows:
        acn_val = int(runtime_acn)
        if acn_val != 0:
            runtime_lines_by_acn[acn_val][int(day)].add(int(lid))

    occupancy_map = {}
    claim_rows_painted = 0
    claimless_rows_painted = 0
    claim_days_by_acn = defaultdict(set)

    def _paint(day_val, line_val, acn_val, gb_val):
        key = (day_val, line_val)
        if key in occupancy_map:
            if occupancy_map[key][0] != acn_val:
                raise RuntimeError(
                    "sim_repairline_v9 occupancy conflict: "
                    f"day={day_val}, line_id={line_val}, "
                    f"acn={occupancy_map[key][0]} vs {acn_val} "
                    f"(version_date={version_date_int}, version_id={version_id})"
                )
            return False
        occupancy_map[key] = (acn_val, gb_val)
        return True

    # Claim-based occupancy
    for acn_val, gb_val, line_val, start_val, end_val in claim_events:
        for day_val in range(start_val, end_val):
            if _paint(day_val, line_val, acn_val, gb_val):
                claim_rows_painted += 1
            claim_days_by_acn[acn_val].add(day_val)

    # Claimless episodes (repair status=4 not covered by claim-based)
    for acn_val, trace in master_trace_by_acn.items():
        if not trace:
            continue
        trace_sorted = sorted(trace, key=lambda item: item[0])
        for ep_start, ep_end in _build_repair_episodes(trace_sorted):
            covered = False
            for day_val in range(ep_start, ep_end):
                if day_val in claim_days_by_acn.get(acn_val, set()):
                    covered = True
                    break
            if covered:
                continue

            line_ids = set()
            runtime_days = runtime_lines_by_acn.get(acn_val, {})
            for day_val in range(ep_start, ep_end):
                line_ids.update(runtime_days.get(day_val, set()))

            if not line_ids:
                raise RuntimeError(
                    "sim_repairline_v9 missing runtime line_id for claimless episode: "
                    f"acn={acn_val}, episode=[{ep_start},{ep_end}) "
                    f"(version_date={version_date_int}, version_id={version_id})"
                )
            if len(line_ids) > 1:
                raise RuntimeError(
                    "sim_repairline_v9 ambiguous runtime line_id for claimless episode: "
                    f"acn={acn_val}, episode=[{ep_start},{ep_end}), line_ids={sorted(line_ids)} "
                    f"(version_date={version_date_int}, version_id={version_id})"
                )

            line_val = next(iter(line_ids))
            gb_val = acn_map[acn_val]
            for day_val in range(ep_start, ep_end):
                if _paint(day_val, line_val, acn_val, gb_val):
                    claimless_rows_painted += 1

    total_rows = len(rows)
    painted_rows = len(occupancy_map)

    # Добавляем version_date и version_id к каждой строке
    full_rows = []
    for day, lid, fd, rt, _acn, _gb, bank_count, bank_head_start, bank_head_end in rows:
        key = (int(day), int(lid))
        if key in occupancy_map:
            acn_val, gb_val = occupancy_map[key]
        else:
            acn_val, gb_val = 0, 0
        full_rows.append(
            (
                version_date_int,
                version_id,
                day,
                lid,
                fd,
                rt,
                acn_val,
                gb_val,
                bank_count,
                bank_head_start,
                bank_head_end,
            )
        )

    print(
        f"  [RL Export] reconcile: total_rows={total_rows}, painted_rows={painted_rows}, "
        f"claim_rows_painted={claim_rows_painted}, claimless_rows_painted={claimless_rows_painted}"
    )

    if full_rows:
        ch_client.execute(
            "INSERT INTO sim_repairline_v9 "
            "(version_date, version_id, day_u16, line_id, free_days, repair_time, aircraft_number, group_by, "
            "bank_count, bank_head_start, bank_head_end) "
            "VALUES",
            full_rows,
            settings={'max_partitions_per_insert_block': 300}
        )
        print(f"  ✅ sim_repairline_v9: {len(full_rows)} строк вставлено")
    else:
        print("  ⚠️ sim_repairline_v9: нет данных для вставки")
