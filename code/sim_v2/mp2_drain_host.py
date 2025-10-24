"""
Host функция для дренажа MP2 в ClickHouse
"""

import pyflamegpu as fg
from datetime import datetime, timedelta
import time

class MP2DrainHostFunction(fg.HostFunction):
    """Host функция для батчевой выгрузки MP2 с GPU в СУБД"""
    
    def __init__(self, client, table_name: str = 'sim_masterv2', 
                 batch_size: int = 250000, simulation_steps: int = 365):
        super().__init__()
        self.client = client
        self.table_name = table_name
        self.batch_size = batch_size
        self.simulation_steps = simulation_steps
        self.batch = []
        self.total_rows_written = 0
        self.total_drain_time = 0.0
        self.flush_count = 0
        self.total_flush_time = 0.0
        self.max_batch_rows = 0
        # Инкрементальный дренаж
        self.interval_days = 0  # 0 = только финальный дренаж
        self._last_drained_day = 0
        # Поштучный (budgeted) дренаж без длинных пауз
        self.drain_rows_per_step = 100000  # лимит строк на один вызов run()
        self._pending = False
        self._pend_start_day = 0
        self._pend_end_day = 0
        self._pend_day_cursor = 0
        self._pend_idx_cursor = 0
        
        # Создаем таблицу если не существует
        self._ensure_table()
        
    def _ensure_table(self):
        """Создает таблицу для MP2 если не существует"""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            -- Метаданные
            version_date      UInt32,
            version_id        UInt32,
            day_u16          UInt16,
            day_date         Date MATERIALIZED addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)),
            
            -- Идентификаторы
            idx              UInt16,
            aircraft_number  UInt32,
            partseqno        UInt32,
            group_by         UInt8,
            
            -- V2 State информация
            state            String,
            intent_state     UInt8,
            bi_counter       UInt8,  -- Служебное поле для BI счётчиков (всегда 1)
            
            -- Наработки
            sne              UInt32,
            ppr              UInt32,
            cso              UInt32,
            
            -- Нормативы
            ll               UInt32,
            oh               UInt32,
            br               UInt32,
            
            -- Временные характеристики (нормативы времени)
            repair_time      UInt16,
            assembly_time    UInt16,
            partout_time     UInt16,
            
            -- Временные характеристики (текущие значения и триггеры)
            repair_days      UInt16,
            s4_days          UInt16,  -- Счётчик дней в repair+reserve (накопительный)
            assembly_trigger UInt8,
            active_trigger   UInt8,
            partout_trigger  UInt8,
            mfg_date_days    UInt32,
            
            -- MP5 данные
            dt               UInt32,
            dn               UInt32,
            
            -- Квоты (опционально, может быть NULL)
            -- ops_ticket удален (никогда не устанавливается)
            
            -- MP4 целевые значения (для анализа квотирования)
            quota_target_mi8    UInt16,
            quota_target_mi17   UInt16,
            
            -- Баланс квот (gap = curr - target по типам)
            quota_gap_mi8       Int16,
            quota_gap_mi17      Int16,
            
            -- Флаги квотирования (per-agent per-day)
            quota_demount       UInt8,
            quota_promote_p1    UInt8,
            quota_promote_p2    UInt8,
            quota_promote_p3    UInt8,
            
            -- Флаги переходов между состояниями (вычисляются на GPU слоем compute_transitions)
            transition_2_to_4   UInt8,   -- operations → repair
            transition_2_to_6   UInt8,   -- operations → storage
            transition_2_to_3   UInt8,   -- operations → serviceable
            transition_3_to_2   UInt8,   -- serviceable → operations
            transition_5_to_2   UInt8,   -- reserve → operations
            transition_1_to_2   UInt8,   -- inactive → operations
            transition_4_to_5   UInt8,   -- repair → reserve
            transition_1_to_4   UInt8,   -- inactive → repair
            transition_4_to_2   UInt8,   -- repair → operations
            
            -- Временная метка записи
            export_timestamp DateTime DEFAULT now(),
            
            INDEX idx_version (version_date, version_id) TYPE minmax GRANULARITY 1,
            INDEX idx_day (day_u16) TYPE minmax GRANULARITY 1,
            INDEX idx_state (state) TYPE bloom_filter GRANULARITY 1,
            INDEX idx_group_by (group_by) TYPE minmax GRANULARITY 1
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(day_date)
        ORDER BY (version_date, day_u16, idx)
        """
        self.client.execute(ddl)
        
    def run(self, FLAMEGPU):
        """HostFunction слой - вызывается каждый шаг, но дренаж только на финальном"""
        step = FLAMEGPU.getStepCounter()
        env = FLAMEGPU.environment
        export_phase = env.getPropertyUInt("export_phase")
        
        # Дренажим ТОЛЬКО на финальном шаге И ТОЛЬКО при export_phase=0
        # Логика: если включён постпроцессинг, он выполнится на шаге N с export_phase=2,
        # затем на шаге N+1 сработает дренаж с export_phase=0
        
        # Пропускаем если export_phase != 0 (постпроцессинг или другие фазы)
        if export_phase != 0:
            return  # Не дренажим во время постпроцессинга
        
        # Дренажим когда step >= simulation_steps - 1
        # (может быть steps-1 если без постпроцессинга, или steps если с постпроцессингом)
        end_day = self.simulation_steps - 1
        if step < end_day:
            return  # Ещё не время дренажа
        
        # Дренажим данные за все дни (0..end_day-1) - основная симуляция
        # Не включаем день постпроцессинга (если он был)
        actual_end_day = end_day - 1 if step > end_day else end_day
        
        print(f"  Начинаем финальный дренаж MP2: дни 0..{actual_end_day} ({actual_end_day + 1} дней)")
        
        t_start = time.perf_counter()
        rows = self._drain_mp2_range(FLAMEGPU, 0, actual_end_day)
        self.total_rows_written += rows
        self.total_drain_time += (time.perf_counter() - t_start)
        
        print(f"  ✅ Выгружено {rows} строк MP2 за {self.total_drain_time:.2f}с")
        
        # Transition флаги уже вычислены на GPU (слой compute_transitions)
        # и записаны в MP2 напрямую - постобработка НЕ НУЖНА
        self._last_drained_day = actual_end_day + 1
        self._pending = False
        return
        
    def _drain_mp2_range(self, FLAMEGPU, start_day_inclusive: int, end_day_inclusive: int) -> int:
        """Выгружает MP2 данные с GPU за диапазон дней [start..end]"""
        # HostFunction контекст - FLAMEGPU.environment доступен напрямую
        env = FLAMEGPU.environment
        
        frames = env.getPropertyUInt("frames_total")
        version_date = env.getPropertyUInt("version_date")
        version_id = env.getPropertyUInt("version_id")
        
        # Диапазон
        start_day = max(0, int(start_day_inclusive))
        end_day = int(end_day_inclusive) + 1
            
        # Читаем MacroProperty массивы
        
        # Получаем данные с GPU через environment
        mp2_day = env.getMacroPropertyUInt32("mp2_day_u16")
        mp2_idx = env.getMacroPropertyUInt32("mp2_idx")
        mp2_aircraft = env.getMacroPropertyUInt32("mp2_aircraft_number")
        mp2_partseqno = env.getMacroPropertyUInt32("mp2_partseqno")
        mp2_group_by = env.getMacroPropertyUInt32("mp2_group_by")
        
        mp2_state = env.getMacroPropertyUInt32("mp2_state")
        mp2_intent = env.getMacroPropertyUInt32("mp2_intent_state")
        mp2_bi_counter = env.getMacroPropertyUInt32("mp2_bi_counter")
        
        mp2_sne = env.getMacroPropertyUInt32("mp2_sne")
        mp2_ppr = env.getMacroPropertyUInt32("mp2_ppr")
        mp2_cso = env.getMacroPropertyUInt32("mp2_cso")
        
        mp2_ll = env.getMacroPropertyUInt32("mp2_ll")
        mp2_oh = env.getMacroPropertyUInt32("mp2_oh")
        mp2_br = env.getMacroPropertyUInt32("mp2_br")
        
        mp2_repair_time = env.getMacroPropertyUInt32("mp2_repair_time")
        mp2_assembly_time = env.getMacroPropertyUInt32("mp2_assembly_time")
        mp2_partout_time = env.getMacroPropertyUInt32("mp2_partout_time")
        
        mp2_repair_days = env.getMacroPropertyUInt32("mp2_repair_days")
        mp2_s4_days = env.getMacroPropertyUInt32("mp2_s4_days")
        mp2_assembly_trigger = env.getMacroPropertyUInt32("mp2_assembly_trigger")
        mp2_active_trigger = env.getMacroPropertyUInt32("mp2_active_trigger")
        mp2_partout_trigger = env.getMacroPropertyUInt32("mp2_partout_trigger")
        mp2_mfg_date = env.getMacroPropertyUInt32("mp2_mfg_date_days")
        
        mp2_dt = env.getMacroPropertyUInt32("mp2_dt")
        mp2_dn = env.getMacroPropertyUInt32("mp2_dn")
        
        
        # MP4 целевые значения (читаем НАПРЯМУЮ из mp4_ops_counter, т.к. это глобальные значения)
        # ✅ КРИТИЧНО: НЕ используем mp2_mp4_target_* MacroProperty, т.к. они заполняются через RTC,
        #    который вызывается только для существующих агентов! Это приводит к пропускам дней.
        try:
            mp4_ops_counter_mi8 = env.getPropertyArrayUInt32("mp4_ops_counter_mi8")
        except:
            mp4_ops_counter_mi8 = None
        try:
            mp4_ops_counter_mi17 = env.getPropertyArrayUInt32("mp4_ops_counter_mi17")
        except:
            mp4_ops_counter_mi17 = None
        
        # Баланс квот (gap = curr - target по типам)
        try:
            mp2_quota_gap_mi8 = env.getMacroPropertyInt32("mp2_quota_gap_mi8")
        except:
            mp2_quota_gap_mi8 = None
        try:
            mp2_quota_gap_mi17 = env.getMacroPropertyInt32("mp2_quota_gap_mi17")
        except:
            mp2_quota_gap_mi17 = None
        
        # Флаги квотирования (per-agent per-day)
        try:
            mp2_quota_demount = env.getMacroPropertyUInt32("mp2_quota_demount")
        except:
            mp2_quota_demount = None
        try:
            mp2_quota_promote_p1 = env.getMacroPropertyUInt32("mp2_quota_promote_p1")
        except:
            mp2_quota_promote_p1 = None
        try:
            mp2_quota_promote_p2 = env.getMacroPropertyUInt32("mp2_quota_promote_p2")
        except:
            mp2_quota_promote_p2 = None
        try:
            mp2_quota_promote_p3 = env.getMacroPropertyUInt32("mp2_quota_promote_p3")
        except:
            mp2_quota_promote_p3 = None
        
        # Флаги переходов (вычисляются GPU post-processing слоем compute_transitions)
        try:
            mp2_transition_2_to_4 = env.getMacroPropertyUInt32("mp2_transition_2_to_4")
            print(f"  ✅ Получена mp2_transition_2_to_4")
        except Exception as e:
            print(f"  ⚠️ Не удалось получить mp2_transition_2_to_4: {e}")
            mp2_transition_2_to_4 = None
        try:
            mp2_transition_2_to_6 = env.getMacroPropertyUInt32("mp2_transition_2_to_6")
        except:
            mp2_transition_2_to_6 = None
        try:
            mp2_transition_2_to_3 = env.getMacroPropertyUInt32("mp2_transition_2_to_3")
        except:
            mp2_transition_2_to_3 = None
        try:
            mp2_transition_3_to_2 = env.getMacroPropertyUInt32("mp2_transition_3_to_2")
        except:
            mp2_transition_3_to_2 = None
        try:
            mp2_transition_5_to_2 = env.getMacroPropertyUInt32("mp2_transition_5_to_2")
        except:
            mp2_transition_5_to_2 = None
        try:
            mp2_transition_1_to_2 = env.getMacroPropertyUInt32("mp2_transition_1_to_2")
        except:
            mp2_transition_1_to_2 = None
        try:
            mp2_transition_4_to_5 = env.getMacroPropertyUInt32("mp2_transition_4_to_5")
        except:
            mp2_transition_4_to_5 = None
        try:
            mp2_transition_1_to_4 = env.getMacroPropertyUInt32("mp2_transition_1_to_4")
        except:
            mp2_transition_1_to_4 = None
        try:
            mp2_transition_4_to_2 = env.getMacroPropertyUInt32("mp2_transition_4_to_2")
        except:
            mp2_transition_4_to_2 = None
        
        # Получаем days_total для safe_day логики
        days_total = env.getPropertyUInt32("days_total")
        
        rows_count = 0
        # day_date вычисляется в ClickHouse (MATERIALIZED), в Python не считаем
        
        # Собираем батч
        for day in range(start_day, end_day):
            # Прямая адресация в плотной матрице
            day_offset = day * frames
            
            for idx in range(frames):
                pos = day_offset + idx
                
                # Проверяем aircraft_number > 0 для исключения пустых слотов
                aircraft_number = int(mp2_aircraft[pos])
                if aircraft_number > 0:
                    row = (
                        version_date,
                        version_id,
                        day,
                        int(mp2_idx[pos]),
                        int(mp2_aircraft[pos]),
                        int(mp2_partseqno[pos]),
                        int(mp2_group_by[pos]),
                        self._map_state_to_string(int(mp2_state[pos])),
                        int(mp2_intent[pos]),
                        int(mp2_bi_counter[pos]),
                        int(mp2_sne[pos]),
                        int(mp2_ppr[pos]),
                        int(mp2_cso[pos]),
                        int(mp2_ll[pos]),
                        int(mp2_oh[pos]),
                        int(mp2_br[pos]),
                        int(mp2_repair_time[pos]),
                        int(mp2_assembly_time[pos]),
                        int(mp2_partout_time[pos]),
                        int(mp2_repair_days[pos]),
                        int(mp2_s4_days[pos]),
                        int(mp2_assembly_trigger[pos]),
                        int(mp2_active_trigger[pos]),
                        int(mp2_partout_trigger[pos]),
                        int(mp2_mfg_date[pos]),
                        int(mp2_dt[pos]),
                        int(mp2_dn[pos]),
                        # MP4 целевые значения по дню (читаем из mp4_ops_counter с safe_day логикой)
                        self._get_mp4_target(mp4_ops_counter_mi8, day, days_total),
                        self._get_mp4_target(mp4_ops_counter_mi17, day, days_total),
                        int(mp2_quota_gap_mi8[day]) if mp2_quota_gap_mi8 is not None else 0,
                        int(mp2_quota_gap_mi17[day]) if mp2_quota_gap_mi17 is not None else 0,
                        int(mp2_quota_demount[pos]) if mp2_quota_demount is not None else 0,
                        int(mp2_quota_promote_p1[pos]) if mp2_quota_promote_p1 is not None else 0,
                        int(mp2_quota_promote_p2[pos]) if mp2_quota_promote_p2 is not None else 0,
                        int(mp2_quota_promote_p3[pos]) if mp2_quota_promote_p3 is not None else 0,
                        # Флаги переходов (записываются на GPU слоем compute_transitions)
                        int(mp2_transition_2_to_4[pos]) if mp2_transition_2_to_4 is not None else 0,
                        int(mp2_transition_2_to_6[pos]) if mp2_transition_2_to_6 is not None else 0,
                        int(mp2_transition_2_to_3[pos]) if mp2_transition_2_to_3 is not None else 0,
                        int(mp2_transition_3_to_2[pos]) if mp2_transition_3_to_2 is not None else 0,
                        int(mp2_transition_5_to_2[pos]) if mp2_transition_5_to_2 is not None else 0,
                        int(mp2_transition_1_to_2[pos]) if mp2_transition_1_to_2 is not None else 0,
                        int(mp2_transition_4_to_5[pos]) if mp2_transition_4_to_5 is not None else 0,
                        int(mp2_transition_1_to_4[pos]) if mp2_transition_1_to_4 is not None else 0,
                        int(mp2_transition_4_to_2[pos]) if mp2_transition_4_to_2 is not None else 0
                    )
                    self.batch.append(row)
                    rows_count += 1
                    
                    # Flush при достижении размера батча
                    if len(self.batch) >= self.batch_size:
                        self._flush_batch()
                        
        # Финальный flush
        if self.batch:
            self._flush_batch()
            
        return rows_count

    def _drain_mp2_budgeted(self, FLAMEGPU, start_day_inclusive: int, end_day_inclusive: int, 
                             start_idx: int, max_rows: int):
        """Дренаж с ограничением по количеству строк за вызов. Возвращает (rows, finished),
        где finished либо True (если диапазон завершён), либо (day_cursor, idx_cursor) для продолжения."""
        frames = FLAMEGPU.environment.getPropertyUInt("frames_total")
        version_date = FLAMEGPU.environment.getPropertyUInt("version_date")
        version_id = FLAMEGPU.environment.getPropertyUInt("version_id")
        # day_date вычисляется в ClickHouse (MATERIALIZED), в Python не считаем
        
        # Читаем ссылки на MP2 MacroProperty (host view)
        mp2_day = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_day_u16")
        mp2_idx = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_idx")
        mp2_aircraft = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_aircraft_number")
        mp2_partseqno = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_partseqno")
        mp2_group_by = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_group_by")
        
        mp2_state = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_state")
        mp2_intent = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_intent_state")
        mp2_s6_started = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_s6_started")
        
        mp2_sne = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_sne")
        mp2_ppr = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_ppr")
        mp2_cso = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_cso")
        
        mp2_ll = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_ll")
        mp2_oh = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_oh")
        mp2_br = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_br")
        
        mp2_repair_time = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_repair_time")
        mp2_assembly_time = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_assembly_time")
        mp2_partout_time = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_partout_time")
        
        mp2_repair_days = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_repair_days")
        mp2_s6_days = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_s6_days")
        mp2_assembly_trigger = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_assembly_trigger")
        mp2_active_trigger = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_active_trigger")
        mp2_partout_trigger = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_partout_trigger")
        mp2_mfg_date = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_mfg_date_days")
        
        mp2_dt = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_dt")
        mp2_dn = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_dn")
        
        
        # MP4 целевые значения (читаем из буферов, заполненных rtc_log_mp4_targets)
        try:
            mp2_mp4_target_mi8 = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_mp4_target_mi8")
        except:
            mp2_mp4_target_mi8 = None
        try:
            mp2_mp4_target_mi17 = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_mp4_target_mi17")
        except:
            mp2_mp4_target_mi17 = None
        
        # Баланс квот (gap = curr - target по типам)
        try:
            mp2_quota_gap_mi8 = FLAMEGPU.environment.getMacroPropertyInt32("mp2_quota_gap_mi8")
        except:
            mp2_quota_gap_mi8 = None
        try:
            mp2_quota_gap_mi17 = FLAMEGPU.environment.getMacroPropertyInt32("mp2_quota_gap_mi17")
        except:
            mp2_quota_gap_mi17 = None
        
        # Флаги квотирования (per-agent per-day)
        try:
            mp2_quota_demount = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_quota_demount")
        except:
            mp2_quota_demount = None
        try:
            mp2_quota_promote_p1 = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_quota_promote_p1")
        except:
            mp2_quota_promote_p1 = None
        try:
            mp2_quota_promote_p2 = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_quota_promote_p2")
        except:
            mp2_quota_promote_p2 = None
        try:
            mp2_quota_promote_p3 = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_quota_promote_p3")
        except:
            mp2_quota_promote_p3 = None
        
        # Флаги переходов (вычисляются GPU post-processing слоем compute_transitions)
        try:
            mp2_transition_2_to_4 = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_transition_2_to_4")
        except:
            mp2_transition_2_to_4 = None
        try:
            mp2_transition_2_to_6 = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_transition_2_to_6")
        except:
            mp2_transition_2_to_6 = None
        try:
            mp2_transition_2_to_3 = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_transition_2_to_3")
        except:
            mp2_transition_2_to_3 = None
        try:
            mp2_transition_3_to_2 = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_transition_3_to_2")
        except:
            mp2_transition_3_to_2 = None
        try:
            mp2_transition_5_to_2 = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_transition_5_to_2")
        except:
            mp2_transition_5_to_2 = None
        try:
            mp2_transition_1_to_2 = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_transition_1_to_2")
        except:
            mp2_transition_1_to_2 = None
        try:
            mp2_transition_4_to_5 = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_transition_4_to_5")
        except:
            mp2_transition_4_to_5 = None
        try:
            mp2_transition_1_to_4 = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_transition_1_to_4")
        except:
            mp2_transition_1_to_4 = None
        try:
            mp2_transition_4_to_2 = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_transition_4_to_2")
        except:
            mp2_transition_4_to_2 = None
        
        # MP4 целевые значения (читаем НАПРЯМУЮ из mp4_ops_counter, т.к. это глобальные значения)
        try:
            mp4_ops_counter_mi8 = FLAMEGPU.environment.getPropertyArrayUInt32("mp4_ops_counter_mi8")
        except:
            mp4_ops_counter_mi8 = None
        try:
            mp4_ops_counter_mi17 = FLAMEGPU.environment.getPropertyArrayUInt32("mp4_ops_counter_mi17")
        except:
            mp4_ops_counter_mi17 = None
        
        # Получаем days_total для safe_day логики
        days_total = FLAMEGPU.environment.getPropertyUInt32("days_total")
        
        rows_count = 0
        day = max(0, int(start_day_inclusive))
        end_day = int(end_day_inclusive)
        idx_cursor = int(start_idx)
        
        while day <= end_day:
            day_offset = day * frames
            # Итерируем по индексам начиная с текущего курсора
            for idx in range(idx_cursor, frames):
                pos = day_offset + idx
                aircraft_number = int(mp2_aircraft[pos])
                if aircraft_number > 0:
                    row = (
                        version_date,
                        version_id,
                        day,
                        int(mp2_idx[pos]),
                        aircraft_number,
                        int(mp2_partseqno[pos]),
                        int(mp2_group_by[pos]),
                        self._map_state_to_string(int(mp2_state[pos])),
                        int(mp2_intent[pos]),
                        int(mp2_bi_counter[pos]),
                        int(mp2_sne[pos]),
                        int(mp2_ppr[pos]),
                        int(mp2_cso[pos]),
                        int(mp2_ll[pos]),
                        int(mp2_oh[pos]),
                        int(mp2_br[pos]),
                        int(mp2_repair_time[pos]),
                        int(mp2_assembly_time[pos]),
                        int(mp2_partout_time[pos]),
                        int(mp2_repair_days[pos]),
                        int(mp2_s4_days[pos]),
                        int(mp2_assembly_trigger[pos]),
                        int(mp2_active_trigger[pos]),
                        int(mp2_partout_trigger[pos]),
                        int(mp2_mfg_date[pos]),
                        int(mp2_dt[pos]),
                        int(mp2_dn[pos]),
                        # MP4 целевые значения по дню (читаем из mp4_ops_counter с safe_day логикой)
                        self._get_mp4_target(mp4_ops_counter_mi8, day, days_total),
                        self._get_mp4_target(mp4_ops_counter_mi17, day, days_total),
                        int(mp2_quota_gap_mi8[day]) if mp2_quota_gap_mi8 is not None else 0,
                        int(mp2_quota_gap_mi17[day]) if mp2_quota_gap_mi17 is not None else 0,
                        int(mp2_quota_demount[pos]) if mp2_quota_demount is not None else 0,
                        int(mp2_quota_promote_p1[pos]) if mp2_quota_promote_p1 is not None else 0,
                        int(mp2_quota_promote_p2[pos]) if mp2_quota_promote_p2 is not None else 0,
                        int(mp2_quota_promote_p3[pos]) if mp2_quota_promote_p3 is not None else 0,
                        # Флаги переходов (вычисляются GPU post-processing слоем)
                        int(mp2_transition_2_to_4[pos]) if mp2_transition_2_to_4 is not None else 0,
                        int(mp2_transition_2_to_6[pos]) if mp2_transition_2_to_6 is not None else 0,
                        int(mp2_transition_2_to_3[pos]) if mp2_transition_2_to_3 is not None else 0,
                        int(mp2_transition_3_to_2[pos]) if mp2_transition_3_to_2 is not None else 0,
                        int(mp2_transition_5_to_2[pos]) if mp2_transition_5_to_2 is not None else 0,
                        int(mp2_transition_1_to_2[pos]) if mp2_transition_1_to_2 is not None else 0,
                        int(mp2_transition_4_to_5[pos]) if mp2_transition_4_to_5 is not None else 0,
                        int(mp2_transition_1_to_4[pos]) if mp2_transition_1_to_4 is not None else 0,
                        int(mp2_transition_4_to_2[pos]) if mp2_transition_4_to_2 is not None else 0
                    )
                    self.batch.append(row)
                    rows_count += 1
                    if len(self.batch) >= self.batch_size:
                        self._flush_batch()
                    if rows_count >= max_rows:
                        # Сохраняем курсор и возвращаемся в следующем шаге
                        if self.batch:
                            self._flush_batch()
                        return rows_count, (day, idx + 1)
            # Перешли на следующий день
            idx_cursor = 0
            day += 1
        
        # Диапазон завершён
        if self.batch:
            self._flush_batch()
        return rows_count, True
        
    def _flush_batch(self):
        """Записывает батч в ClickHouse"""
        if not self.batch:
            return
        # Фиксируем размер батча и время вставки
        batch_rows = len(self.batch)
        if batch_rows > self.max_batch_rows:
            self.max_batch_rows = batch_rows
        t_start = time.perf_counter()
        # MATERIALIZED day_date вычисляется на стороне ClickHouse, не вставляем её явно
        columns = "version_date,version_id,day_u16,idx,aircraft_number,partseqno,group_by,state,intent_state,bi_counter,sne,ppr,cso,ll,oh,br,repair_time,assembly_time,partout_time,repair_days,s4_days,assembly_trigger,active_trigger,partout_trigger,mfg_date_days,dt,dn,quota_target_mi8,quota_target_mi17,quota_gap_mi8,quota_gap_mi17,quota_demount,quota_promote_p1,quota_promote_p2,quota_promote_p3,transition_2_to_4,transition_2_to_6,transition_2_to_3,transition_3_to_2,transition_5_to_2,transition_1_to_2,transition_4_to_5,transition_1_to_4,transition_4_to_2"
        query = f"INSERT INTO {self.table_name} ({columns}) VALUES"
        # Подаём данные в колоннарном формате для уменьшения накладных расходов драйвера
        num_cols = 44  # 27 базовых + 2 MP4 целей + 4 флага квот + 2 gap + 9 transition флагов
        cols = [[] for _ in range(num_cols)]
        for r in self.batch:
            for i, v in enumerate(r):
                cols[i].append(v)
        self.client.execute(query, cols, columnar=True)
        self.flush_count += 1
        self.total_flush_time += (time.perf_counter() - t_start)
        self.batch.clear()
        
    def _map_state_to_string(self, state_id: int) -> str:
        """Маппинг числового state в строку"""
        mapping = {
            1: 'inactive',
            2: 'operations', 
            3: 'serviceable',
            4: 'repair',
            5: 'reserve',
            6: 'storage'
        }
        return mapping.get(state_id, f'unknown_{state_id}')
    
    def _get_mp4_target(self, mp4_array, day: int, days_total: int) -> int:
        """Получает целевое значение из mp4_ops_counter с safe_day логикой"""
        if mp4_array is None:
            return 0
        safe_day = (day + 1) if (day + 1) < days_total else (days_total - 1 if days_total > 0 else 0)
        return int(mp4_array[safe_day])
        
    def get_summary(self) -> str:
        """Возвращает сводку по дренажу"""
        return (f"MP2 Drain Summary: {self.total_rows_written} rows written "
                f"in {self.total_drain_time:.2f}s total")

    def _compute_transitions_sql(self):
        """Вычисляет transition флаги через SQL window функции после дренажа"""
        try:
            version_date = self.client.execute("SELECT max(version_date) FROM sim_masterv2")[0][0]
            
            # Получаем уникальные самолеты и дни для вычисления переходов
            # Используем JOIN с предыдущим днем
            sql_updates = [
                f"ALTER TABLE {self.table_name} UPDATE transition_2_to_4 = 1 WHERE version_date = {version_date} AND aircraft_number IN (SELECT DISTINCT prev.aircraft_number FROM {self.table_name} AS curr ASOF LEFT JOIN {self.table_name} AS prev ON curr.aircraft_number = prev.aircraft_number AND prev.day_u16 + 1 = curr.day_u16 AND prev.version_date = {version_date} WHERE curr.state = 'repair' AND prev.state = 'operations' AND curr.version_date = {version_date})",
            ]
            
            # На самом деле это не сработает. Давайте используем более простой подход:
            # Для каждого уникального aircraft - SELECT все дни, проверить переходы в Python
            
            print(f"  ℹ️  Вычисление transition флагов: используем Python для window logic")
            
            # SELECT все записи отсортированные по aircraft и дню
            query = f"SELECT aircraft_number, day_u16, state FROM {self.table_name} WHERE version_date = {version_date} ORDER BY aircraft_number, day_u16"
            rows = self.client.execute(query)
            
            updates_list = []
            prev_row = None
            
            for row in rows:
                curr_aircraft = row[0]
                curr_day = row[1]
                curr_state = row[2]
                
                if prev_row and prev_row[0] == curr_aircraft:  # Same aircraft, consecutive check
                    prev_state = prev_row[2]
                    
                    if prev_state != curr_state:
                        # Вычисляем какой переход произошел
                        transition_field = None
                        if prev_state == 'operations' and curr_state == 'repair':
                            transition_field = 'transition_2_to_4'
                        elif prev_state == 'operations' and curr_state == 'storage':
                            transition_field = 'transition_2_to_6'
                        elif prev_state == 'operations' and curr_state == 'serviceable':
                            transition_field = 'transition_2_to_3'
                        elif prev_state == 'serviceable' and curr_state == 'operations':
                            transition_field = 'transition_3_to_2'
                        elif prev_state == 'reserve' and curr_state == 'operations':
                            transition_field = 'transition_5_to_2'
                        elif prev_state == 'inactive' and curr_state == 'operations':
                            transition_field = 'transition_1_to_2'
                        elif prev_state == 'repair' and curr_state == 'reserve':
                            transition_field = 'transition_4_to_5'
                        elif prev_state == 'inactive' and curr_state == 'repair':
                            transition_field = 'transition_1_to_4'
                        elif prev_state == 'repair' and curr_state == 'operations':
                            transition_field = 'transition_4_to_2'
                        
                        if transition_field:
                            updates_list.append((transition_field, curr_aircraft, curr_day, version_date))
                
                prev_row = row
            
            # Выполняем UPDATE для каждого найденного перехода
            for transition_field, aircraft, day, vdate in updates_list:
                sql = f"ALTER TABLE {self.table_name} UPDATE {transition_field} = 1 WHERE version_date = {vdate} AND aircraft_number = {aircraft} AND day_u16 = {day}"
                self.client.execute(sql)
            
            print(f"  ✅ Transition флаги вычислены ({len(updates_list)} переходов)")
            
        except Exception as e:
            print(f"  ⚠️ Ошибка при вычислении transition флагов: {e}")


class MP2EventDrainHostFunction(fg.HostFunction):
    """Host функция для выгрузки событий MP2"""
    
    def __init__(self, client, table_name: str = 'sim_events_v2',
                 event_buffer_size: int = 10000):
        super().__init__()
        self.client = client
        self.table_name = table_name
        self.event_buffer_size = event_buffer_size
        self.last_event_pos = 0
        
        self._ensure_table()
        
    def _ensure_table(self):
        """Создает таблицу для событий"""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            version_date     UInt32,
            day_u16         UInt16,
            idx             UInt16,
            event_type      UInt8,   -- 1=transition, 2=spawn, 3=trigger
            from_state      UInt8,
            to_state        UInt8,
            value1          UInt32,  -- sne для transitions
            value2          UInt32,  -- ppr для transitions
            
            INDEX idx_day (day_u16) TYPE minmax GRANULARITY 1,
            INDEX idx_type (event_type) TYPE minmax GRANULARITY 1
        ) ENGINE = MergeTree()
        ORDER BY (version_date, day_u16, idx)
        """
        self.client.execute(ddl)
        
    def run(self, FLAMEGPU):
        """Выгружает новые события"""
        # Читаем текущую позицию счетчика событий (скаляр)
        event_counter = int(FLAMEGPU.environment.getMacroPropertyUInt32("mp2_event_counter")[0])
        
        if event_counter > self.last_event_pos:
            new_events = min(event_counter - self.last_event_pos, self.event_buffer_size)
            
            # Читаем события
            event_day = FLAMEGPU.environment.getMacroPropertyUInt32("event_day")
            event_idx = FLAMEGPU.environment.getMacroPropertyUInt32("event_idx")
            event_type = FLAMEGPU.environment.getMacroPropertyUInt32("event_type")
            event_from = FLAMEGPU.environment.getMacroPropertyUInt32("event_from_state")
            event_to = FLAMEGPU.environment.getMacroPropertyUInt32("event_to_state")
            event_value1 = FLAMEGPU.environment.getMacroPropertyUInt32("event_value1")
            event_value2 = FLAMEGPU.environment.getMacroPropertyUInt32("event_value2")
            
            version_date = FLAMEGPU.environment.getPropertyUInt("version_date")
            
            batch = []
            for i in range(new_events):
                pos = (self.last_event_pos + i) % self.event_buffer_size
                
                if int(event_type[pos]) > 0:  # Валидное событие
                    batch.append((
                        version_date,
                        int(event_day[pos]),
                        int(event_idx[pos]),
                        int(event_type[pos]),
                        int(event_from[pos]),
                        int(event_to[pos]),
                        int(event_value1[pos]),
                        int(event_value2[pos])
                    ))
                    
            if batch:
                columns = "version_date,day_u16,idx,event_type,from_state,to_state,value1,value2"
                query = f"INSERT INTO {self.table_name} ({columns}) VALUES"
                self.client.execute(query, batch)
                
            self.last_event_pos = event_counter
