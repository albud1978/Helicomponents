#!/usr/bin/env python3
# EXPERIMENTAL / REFERENCE (2026-06-06): пробный L2-контур (group_by=3/4) в messaging. Не production. Боевой L2 — code/sim_v2/units/orchestrator_units.py. Оставлен как справочный черновик.
"""
Host функция для дренажа MP2 агрегатов в ClickHouse (messaging v1).

Адаптация mp2_drain_units.py с поддержкой pre_state_id.

Дата: 27.02.2026
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pyflamegpu as fg
from datetime import date
import time

# Константы размеров (синхронизированы с base_model_units_v1.py)
UNITS_MAX_FRAMES = 40000
UNITS_MAX_DAYS = 3650


class MP2DrainUnitsHostFunction(fg.HostFunction):
    """Host функция для батчевой выгрузки MP2 агрегатов с GPU в СУБД"""

    def __init__(self, client, table_name: str = 'sim_units_v2',
                 batch_size: int = 500000, simulation_steps: int = 3650,
                 version_date: date = None, version_id: int = 1):
        super().__init__()
        self.client = client
        self.table_name = table_name
        self.batch_size = batch_size
        self.simulation_steps = simulation_steps
        self.version_date = version_date or date.today()
        self.version_id = version_id
        self.version_date_int = (self.version_date - date(1970, 1, 1)).days

        # Статистика
        self.total_rows_written = 0
        self.total_drain_time = 0.0
        self.flush_count = 0

        # Инкрементальный дренаж
        self.interval_days = 10
        self._last_drained_day = -1

        # Создаём таблицу
        self._ensure_table()

    def _ensure_table(self):
        """Создаёт таблицу с оптимальными кодеками для компрессии"""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            -- Версионирование
            version_date UInt32,
            version_id UInt32,

            -- Индексы
            day_u16 UInt16 CODEC(Delta, ZSTD(1)),
            day_date Date MATERIALIZED addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)),
            idx UInt32,

            -- Идентификаторы
            psn UInt32,
            group_by UInt8 CODEC(ZSTD(1)),
            partseqno_i UInt32,
            aircraft_number UInt32,

            -- Наработки (Delta для временных рядов)
            sne UInt32 CODEC(Delta, ZSTD(1)),
            ppr UInt32 CODEC(Delta, ZSTD(1)),

            -- Состояние
            state UInt8 CODEC(ZSTD(1)),
            pre_state_id UInt8 CODEC(ZSTD(1)),
            repair_days UInt16 CODEC(Delta, ZSTD(1)),
            queue_position UInt32,

            -- Флаг активности (для фильтрации spawn-резерва)
            active UInt8 CODEC(ZSTD(1)),

            -- Метаданные
            export_timestamp DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (version_date, version_id, day_u16, psn)
        SETTINGS index_granularity = 8192
        """

        self.client.execute(ddl)
        print(f"   ✅ Таблица {self.table_name} готова (с Delta+ZSTD кодеками)")

        # Добавляем колонку, если таблица уже существовала без неё
        alter_sql = f"""
        ALTER TABLE {self.table_name}
        ADD COLUMN IF NOT EXISTS pre_state_id UInt8 CODEC(ZSTD(1))
        """
        self.client.execute(alter_sql)

        # Очищаем данные для этого датасета (идемпотентность)
        delete_sql = f"""
        ALTER TABLE {self.table_name} DELETE
        WHERE version_date = {self.version_date_int}
          AND version_id = {self.version_id}
        """
        self.client.execute(delete_sql)
        time.sleep(2)
        print(f"   🗑️ Старые данные удалены (version_date={self.version_date_int}, version_id={self.version_id})")

    def run(self, FLAMEGPU):
        """Вызывается каждый step - проверяет нужен ли drain"""
        step_day = FLAMEGPU.getStepCounter()

        if step_day > 0 and step_day % self.interval_days == 0:
            self._drain_range(FLAMEGPU, self._last_drained_day + 1, step_day)
            self._last_drained_day = step_day

    def final_drain(self, FLAMEGPU):
        """Финальный дренаж после симуляции"""
        final_day = FLAMEGPU.getStepCounter()
        if final_day > self._last_drained_day:
            self._drain_range(FLAMEGPU, self._last_drained_day + 1, final_day + 1)
            self._last_drained_day = final_day

        print(f"   📊 MP2 Units: всего {self.total_rows_written:,} записей за {self.total_drain_time:.2f}с")

    def _drain_range(self, FLAMEGPU, start_day: int, end_day: int):
        """Дренирует диапазон дней из MacroProperty (циклический буфер) в СУБД"""
        t0 = time.time()

        env = FLAMEGPU.environment
        try:
            max_frames = env.getPropertyUInt("units_frames_total")
        except Exception:
            max_frames = UNITS_MAX_FRAMES

        try:
            drain_interval = env.getPropertyUInt("mp2_drain_interval")
        except Exception:
            drain_interval = self.interval_days

        print(f"   🔍 Читаем MP2 MacroProperty...", flush=True)
        try:
            mp2_psn = env.getMacroPropertyUInt32("mp2_units_psn")
            print(f"      mp2_units_psn: OK", flush=True)
        except Exception as e:
            print(f"      mp2_units_psn: FAIL - {e}", flush=True)
            raise

        mp2_group_by = env.getMacroPropertyUInt32("mp2_units_group_by")
        mp2_sne = env.getMacroPropertyUInt32("mp2_units_sne")
        mp2_ppr = env.getMacroPropertyUInt32("mp2_units_ppr")
        mp2_state = env.getMacroPropertyUInt32("mp2_units_state")
        mp2_pre_state = env.getMacroPropertyUInt32("mp2_units_pre_state")
        mp2_ac = env.getMacroPropertyUInt32("mp2_units_ac")
        mp2_repair_days = env.getMacroPropertyUInt32("mp2_units_repair_days")
        mp2_queue_pos = env.getMacroPropertyUInt32("mp2_units_queue_pos")
        mp2_partseqno = env.getMacroPropertyUInt32("mp2_units_partseqno")
        mp2_active = env.getMacroPropertyUInt32("mp2_units_active")
        print(f"   ✅ Все MP2 MacroProperty прочитаны", flush=True)

        batch = []
        rows_this_drain = 0

        print(f"   📝 Drain дней {start_day}-{end_day}, max_frames={max_frames}, drain_interval={drain_interval}", flush=True)

        for day in range(start_day, end_day):
            buffer_day = day % (drain_interval + 1)

            for idx in range(max_frames):
                pos = buffer_day * max_frames + idx

                try:
                    psn = int(mp2_psn[pos])
                    if psn == 0:
                        continue

                    active = int(mp2_active[pos])
                    if active == 0:
                        continue

                    queue_pos_raw = int(mp2_queue_pos[pos])
                    if queue_pos_raw < 0:
                        queue_pos_raw = 0
                    elif queue_pos_raw > 4294967295:
                        queue_pos_raw = 4294967295

                    row = (
                        self.version_date_int,
                        self.version_id,
                        day,
                        idx,
                        psn,
                        int(mp2_group_by[pos]),
                        int(mp2_partseqno[pos]),
                        int(mp2_ac[pos]),
                        int(mp2_sne[pos]),
                        int(mp2_ppr[pos]),
                        int(mp2_state[pos]),
                        int(mp2_pre_state[pos]),
                        int(mp2_repair_days[pos]),
                        queue_pos_raw,
                        active,
                    )
                    batch.append(row)
                except Exception as e:
                    print(f"   ❌ Error at day={day}, idx={idx}, pos={pos}: {e}", flush=True)
                    raise

                if len(batch) >= self.batch_size:
                    self._flush_batch(batch)
                    rows_this_drain += len(batch)
                    batch = []

        if batch:
            self._flush_batch(batch)
            rows_this_drain += len(batch)

        elapsed = time.time() - t0
        self.total_rows_written += rows_this_drain
        self.total_drain_time += elapsed
        self.flush_count += 1

        print(f"   🔄 Drain дней {start_day}-{end_day}: {rows_this_drain:,} записей за {elapsed:.2f}с")

    def _flush_batch(self, batch):
        """Вставляет батч в ClickHouse"""
        self.client.execute(
            f"""
            INSERT INTO {self.table_name} (
                version_date, version_id, day_u16, idx,
                psn, group_by, partseqno_i, aircraft_number,
                sne, ppr, state, pre_state_id, repair_days, queue_position, active
            ) VALUES
            """,
            batch
        )


def register_mp2_drain_units(model, env_data, client, version_date, version_id=1):
    simulation_steps = int(env_data.get('days_total_u16', 3650))

    drain_fn = MP2DrainUnitsHostFunction(
        client=client,
        table_name='sim_units_v2',
        batch_size=500000,
        simulation_steps=simulation_steps,
        version_date=version_date,
        version_id=version_id
    )

    model.addStepFunction(drain_fn)

    return drain_fn
