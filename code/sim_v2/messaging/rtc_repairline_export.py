#!/usr/bin/env python3
"""
RepairLine Export: чтение GPU-буферов и ежедневная интерполяция.

Аналог rtc_mp2_export.py, но для RepairLine агентов.
GPU записывает состояние (free_days, acn, rt) каждый адаптивный шаг
в MacroProperty буферы rl_buf_*.

HF_RepairLineDrain читает буферы на финальном шаге.
interpolate_repairline_daily() разворачивает адаптивные шаги в ежедневную матрицу.
export_repairline_to_ch() отправляет результат в ClickHouse (sim_repairline_v9).
"""

import sys
import os
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

        # Чтение 3 буферов
        mp_fd = env.getMacroPropertyUInt("rl_buf_free_days")
        mp_acn = env.getMacroPropertyUInt("rl_buf_acn")
        mp_rt = env.getMacroPropertyUInt("rl_buf_rt")

        free_days = np.zeros((num_steps, self.repair_quota), dtype=np.uint32)
        acn = np.zeros((num_steps, self.repair_quota), dtype=np.uint32)
        rt = np.zeros((num_steps, self.repair_quota), dtype=np.uint32)

        for s in range(num_steps):
            base = s * REPAIR_LINES_MAX
            for l in range(self.repair_quota):
                free_days[s, l] = int(mp_fd[base + l])
                acn[s, l] = int(mp_acn[base + l])
                rt[s, l] = int(mp_rt[base + l])

        self.data = {
            'num_steps': num_steps,
            'days': days,
            'free_days': free_days,
            'acn': acn,
            'rt': rt,
        }

        total = num_steps * self.repair_quota * 3
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
      - acn фиксирован (из шага S)
      - Если acn != 0 и free_days достигает rt — линия освобождается (acn=0)

    Returns: list[tuple] — (day_u16, line_id, free_days, aircraft_number)
    """
    num_steps = data['num_steps']
    days = data['days']
    fd_arr = data['free_days']
    acn_arr = data['acn']
    rt_arr = data['rt']

    rows = []

    for line_id in range(repair_quota):
        for s in range(num_steps):
            d1 = days[s]
            d2 = days[s + 1] if s + 1 < num_steps else d1 + 1

            fd_s = int(fd_arr[s, line_id])
            acn_s = int(acn_arr[s, line_id])
            rt_s = int(rt_arr[s, line_id])

            # Определяем день освобождения линии внутри интервала
            release_day = None
            if acn_s != 0 and rt_s > 0 and fd_s < rt_s:
                # Линия освободится через (rt_s - fd_s) дней от d1
                release_day = d1 + (rt_s - fd_s)

            for d in range(d1, d2):
                daily_fd = fd_s + (d - d1)
                daily_acn = acn_s

                # Проверяем освобождение линии
                if release_day is not None and d >= release_day:
                    daily_acn = 0

                rows.append((d, line_id, daily_fd, daily_acn))

    return rows


# ═══════════════════════════════════════════════════════════════════════════════
# Экспорт в ClickHouse
# ═══════════════════════════════════════════════════════════════════════════════

DDL_REPAIRLINE = """
CREATE TABLE IF NOT EXISTS sim_repairline_v9 (
    version_date UInt32,
    version_id   UInt32,
    day_u16      UInt16,
    line_id      UInt8,
    free_days    UInt32,
    aircraft_number UInt32
) ENGINE = MergeTree()
ORDER BY (version_id, version_date, day_u16, line_id)
SETTINGS index_granularity = 8192
"""


def export_repairline_to_ch(ch_client, rows, version_date_int, version_id, drop_table=False):
    """
    Экспорт ежедневной матрицы RepairLine в ClickHouse.

    Args:
        ch_client: clickhouse_driver.Client
        rows: list[tuple] — (day_u16, line_id, free_days, aircraft_number)
        version_date_int: int — YYYYMMDD
        version_id: int
        drop_table: bool — дропнуть таблицу перед созданием
    """
    if drop_table:
        ch_client.execute("DROP TABLE IF EXISTS sim_repairline_v9")
        print("  🗑️ sim_repairline_v9 удалена")

    ch_client.execute(DDL_REPAIRLINE)

    # Добавляем version_date и version_id к каждой строке
    full_rows = [
        (version_date_int, version_id, day, lid, fd, acn)
        for day, lid, fd, acn in rows
    ]

    if full_rows:
        ch_client.execute(
            "INSERT INTO sim_repairline_v9 "
            "(version_date, version_id, day_u16, line_id, free_days, aircraft_number) "
            "VALUES",
            full_rows,
            settings={'max_partitions_per_insert_block': 300}
        )
        print(f"  ✅ sim_repairline_v9: {len(full_rows)} строк вставлено")
    else:
        print("  ⚠️ sim_repairline_v9: нет данных для вставки")
