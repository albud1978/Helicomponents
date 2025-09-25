"""
MP2 Architecture for V2 State-based simulation
Device-side export через MacroProperty
"""

# Константы для MP2
MP2_RING_DAYS = 30      # Кольцевой буфер на 30 дней
MP2_EVENT_SIZE = 10000  # Размер буфера событий

# Схема MP2 SoA (Structure of Arrays)
MP2_SCHEMA = {
    # Основные колонки MP2 (полный снимок)
    'mp2_day_u16': 'UInt16',           # День записи
    'mp2_idx': 'UInt16',               # Индекс агента
    'mp2_aircraft_number': 'UInt32',   # Бортовой номер
    'mp2_state': 'UInt8',              # 1-6 mapping состояний
    'mp2_intent_state': 'UInt8',       # Намерение перехода
    
    # Наработки и счетчики
    'mp2_sne': 'UInt32',               # Наработка с начала эксплуатации
    'mp2_ppr': 'UInt32',               # После последнего ремонта
    'mp2_repair_days': 'UInt16',       # Дни в ремонте
    's6_days': 'UInt16',               # Дни в хранении
    
    # MP5 данные
    'mp2_dt': 'UInt32',                # daily_today
    'mp2_dn': 'UInt32',                # daily_next
    
    # Нормативы (для валидации)
    'mp2_ll': 'UInt32',
    'mp2_oh': 'UInt32',
    'mp2_br': 'UInt32',
}

# Схема MP2_Events (только переходы)
MP2_EVENTS_SCHEMA = {
    'event_day': 'UInt16',
    'event_idx': 'UInt16', 
    'event_aircraft': 'UInt32',
    'event_type': 'UInt8',      # 1=transition, 2=spawn, 3=trigger
    'event_from_state': 'UInt8',
    'event_to_state': 'UInt8',
    'event_value1': 'UInt32',   # sne/ppr для transitions
    'event_value2': 'UInt32',   # oh/br для transitions
}

# RTC код для записи в MP2
RTC_WRITE_MP2 = """
FLAMEGPU_AGENT_FUNCTION(rtc_write_mp2, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Кольцевая адресация: позиция в буфере
    const unsigned int ring_pos = (step_day % ${MP2_RING_DAYS}) * ${MAX_FRAMES} + idx;
    
    // Получаем MacroProperty массивы
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MP2_SIZE}>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MP2_SIZE}>("mp2_idx");
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MP2_SIZE}>("mp2_state");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MP2_SIZE}>("mp2_intent_state");
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MP2_SIZE}>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MP2_SIZE}>("mp2_ppr");
    
    // Записываем текущее состояние
    mp2_day[ring_pos] = step_day;
    mp2_idx[ring_pos] = idx;
    mp2_state[ring_pos] = ${STATE_MAPPING};  // Маппинг string state -> uint
    mp2_intent[ring_pos] = FLAMEGPU->getVariable<unsigned int>("intent_state");
    mp2_sne[ring_pos] = FLAMEGPU->getVariable<unsigned int>("sne");
    mp2_ppr[ring_pos] = FLAMEGPU->getVariable<unsigned int>("ppr");
    
    return flamegpu::ALIVE;
}
"""

# RTC код для записи событий
RTC_WRITE_EVENT = """
FLAMEGPU_AGENT_FUNCTION(rtc_write_event, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Проверяем, есть ли переход
    const unsigned int old_intent = FLAMEGPU->getVariable<unsigned int>("old_intent");
    const unsigned int new_intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    if (old_intent != new_intent && new_intent != ${INTENT_SAME_STATE}) {
        // Атомарно получаем позицию в буфере событий
        auto event_counter = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("mp2_event_counter");
        const unsigned int event_pos = atomicAdd(&event_counter[0], 1u) % ${MP2_EVENT_SIZE};
        
        // Записываем событие
        auto event_day = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MP2_EVENT_SIZE}>("event_day");
        auto event_type = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MP2_EVENT_SIZE}>("event_type");
        auto event_from = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MP2_EVENT_SIZE}>("event_from_state");
        auto event_to = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MP2_EVENT_SIZE}>("event_to_state");
        
        event_day[event_pos] = FLAMEGPU->getStepCounter();
        event_type[event_pos] = 1u; // transition
        event_from[event_pos] = ${OLD_STATE_MAPPING};
        event_to[event_pos] = ${NEW_STATE_MAPPING};
    }
    
    return flamegpu::ALIVE;
}
"""

# Host функция для дренажа MP2
def create_mp2_drain_function(batch_size: int = 250000):
    """Создает host функцию для выгрузки MP2 в СУБД"""
    
    class MP2DrainFunction(pyflamegpu.HostFunction):
        def __init__(self, client, table_name: str, ring_days: int):
            super().__init__()
            self.client = client
            self.table_name = table_name
            self.ring_days = ring_days
            self.batch = []
            self.batch_size = batch_size
            
        def run(self, FLAMEGPU):
            step = FLAMEGPU.getStepCounter()
            
            # Дренаж каждые N шагов или на последнем шаге
            if step % 30 == 0 or step == FLAMEGPU.getSimulationConfig().steps - 1:
                self._drain_mp2(FLAMEGPU)
                
        def _drain_mp2(self, FLAMEGPU):
            """Выгружает данные MP2 с GPU в СУБД"""
            frames = FLAMEGPU.environment.getPropertyUInt("frames_total")
            
            # Определяем диапазон дней для выгрузки
            current_step = FLAMEGPU.getStepCounter()
            start_day = max(0, current_step - self.ring_days + 1)
            end_day = current_step + 1
            
            # Читаем MacroProperty массивы
            mp2_day = FLAMEGPU.environment.getMacroPropertyUInt("mp2_day_u16")
            mp2_idx = FLAMEGPU.environment.getMacroPropertyUInt("mp2_idx")
            mp2_state = FLAMEGPU.environment.getMacroPropertyUInt("mp2_state")
            mp2_intent = FLAMEGPU.environment.getMacroPropertyUInt("mp2_intent_state")
            mp2_sne = FLAMEGPU.environment.getMacroPropertyUInt("mp2_sne")
            mp2_ppr = FLAMEGPU.environment.getMacroPropertyUInt("mp2_ppr")
            
            # Собираем батч
            for day in range(start_day, end_day):
                ring_offset = (day % self.ring_days) * frames
                
                for idx in range(frames):
                    pos = ring_offset + idx
                    
                    # Проверяем, что это актуальные данные
                    if mp2_day[pos] == day:
                        row = (
                            mp2_day[pos],
                            mp2_idx[pos],
                            self._map_state_to_string(mp2_state[pos]),
                            mp2_intent[pos],
                            mp2_sne[pos],
                            mp2_ppr[pos],
                            # ... остальные поля
                        )
                        self.batch.append(row)
                        
                        # Flush при достижении размера батча
                        if len(self.batch) >= self.batch_size:
                            self._flush_batch()
                            
            # Финальный flush
            if self.batch:
                self._flush_batch()
                
        def _flush_batch(self):
            """Записывает батч в ClickHouse"""
            if not self.batch:
                return
                
            query = f"INSERT INTO {self.table_name} VALUES"
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
            return mapping.get(state_id, 'unknown')
    
    return MP2DrainFunction
