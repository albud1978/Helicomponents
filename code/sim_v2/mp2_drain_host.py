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
            day_date         Date,
            
            -- Идентификаторы
            idx              UInt16,
            aircraft_number  UInt32,
            
            -- V2 State информация
            state            String,
            intent_state     UInt8,
            
            -- Наработки
            sne              UInt32,
            ppr              UInt32,
            repair_days      UInt16,
            
            -- MP5 данные
            dt               UInt32,
            dn               UInt32,
            
            -- Временная метка записи
            export_timestamp DateTime DEFAULT now(),
            
            INDEX idx_version (version_date, version_id) TYPE minmax GRANULARITY 1,
            INDEX idx_day (day_u16) TYPE minmax GRANULARITY 1,
            INDEX idx_state (state) TYPE bloom_filter GRANULARITY 1
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(day_date)
        ORDER BY (version_date, day_u16, idx)
        """
        self.client.execute(ddl)
        
    def run(self, FLAMEGPU):
        """Выполняет финальный дренаж MP2 в конце симуляции"""
        step = FLAMEGPU.getStepCounter()
        
        # Дренаж только на последнем шаге
        if step == self.simulation_steps - 1:
            t_start = time.perf_counter()
            rows_drained = self._drain_mp2(FLAMEGPU, step)
            t_elapsed = time.perf_counter() - t_start
            
            self.total_drain_time += t_elapsed
            self.total_rows_written += rows_drained
            
            # Логирование MP2 drain отключено
            
    def _drain_mp2(self, FLAMEGPU, current_step: int) -> int:
        """Выгружает MP2 данные с GPU"""
        # В Host функции доступ к environment через FLAMEGPU.environment
        frames = FLAMEGPU.environment.getPropertyUInt("frames_total")
        version_date = FLAMEGPU.environment.getPropertyUInt("version_date")
        version_id = FLAMEGPU.environment.getPropertyUInt("version_id")
        
        # Финальный дренаж - выгружаем все дни
        start_day = 0
        end_day = current_step + 1
            
        # Читаем MacroProperty массивы
        
        # Получаем данные с GPU через environment
        mp2_day = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_day_u16")
        mp2_idx = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_idx")
        mp2_aircraft = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_aircraft_number")
        mp2_state = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_state")
        mp2_intent = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_intent_state")
        mp2_sne = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_sne")
        mp2_ppr = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_ppr")
        mp2_repair_days = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_repair_days")
        mp2_dt = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_dt")
        mp2_dn = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_dn")
        
        rows_count = 0
        base_date = datetime(1970, 1, 1)
        
        # Собираем батч
        for day in range(start_day, end_day):
            # Прямая адресация в плотной матрице
            day_offset = day * frames
            day_date = base_date + timedelta(days=version_date + day)
            
            for idx in range(frames):
                pos = day_offset + idx
                
                # Проверяем aircraft_number > 0 для исключения пустых слотов
                aircraft_number = int(mp2_aircraft[pos])
                if aircraft_number > 0:
                    row = (
                        version_date,
                        version_id,
                        day,
                        day_date,
                        int(mp2_idx[pos]),
                        int(mp2_aircraft[pos]),
                        self._map_state_to_string(int(mp2_state[pos])),
                        int(mp2_intent[pos]),
                        int(mp2_sne[pos]),
                        int(mp2_ppr[pos]),
                        int(mp2_repair_days[pos]),
                        int(mp2_dt[pos]),
                        int(mp2_dn[pos])
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
        
    def _flush_batch(self):
        """Записывает батч в ClickHouse"""
        if not self.batch:
            return
            
        columns = "version_date,version_id,day_u16,day_date,idx,aircraft_number,state,intent_state,sne,ppr,repair_days,dt,dn"
        query = f"INSERT INTO {self.table_name} ({columns}) VALUES"
        
        self.client.execute(query, self.batch)
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
        
    def get_summary(self) -> str:
        """Возвращает сводку по дренажу"""
        return (f"MP2 Drain Summary: {self.total_rows_written} rows written "
                f"in {self.total_drain_time:.2f}s total")


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
