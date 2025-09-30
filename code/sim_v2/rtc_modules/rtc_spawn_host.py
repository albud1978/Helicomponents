#!/usr/bin/env python3
"""
Host-based spawn (без RTC agent_out)
Обходит проблему NVRTC 425
"""

import pyflamegpu as fg


class HostSpawnFunction(fg.HostFunction):
    """Host функция для создания новых агентов"""
    
    def __init__(self, env_data: dict):
        super().__init__()
        self.env_data = env_data
        self.first_future_idx = env_data.get('first_future_idx', 286)
        self.base_acn = env_data.get('base_acn_spawn', 100000)
        self.next_idx = self.first_future_idx
        
    def run(self, FLAMEGPU):
        """Создаёт новых агентов на текущий день"""
        day = FLAMEGPU.getStepCounter()
        days_total = FLAMEGPU.environment.getPropertyUInt("days_total")
        frames_total = FLAMEGPU.environment.getPropertyUInt("frames_total")
        
        # Читаем сколько нужно создать
        need = 0
        if day < days_total:
            mp4_new = list(self.env_data.get('mp4_new_counter_mi17_seed', []))
            if day < len(mp4_new):
                need = int(mp4_new[day] or 0)
        
        # Клиппинг
        if self.next_idx >= frames_total:
            need = 0
        capacity = frames_total - self.next_idx
        if need > capacity:
            need = capacity
        
        if need == 0:
            return
        
        # Создаём агентов через host API
        agent_desc = FLAMEGPU.agent("heli", "serviceable")
        pop = fg.AgentVector(agent_desc)
        
        for i in range(need):
            idx = self.next_idx + i
            acn = self.base_acn + idx - self.first_future_idx
            
            agent = pop.push_back()
            agent.setVariableUInt("idx", idx)
            agent.setVariableUInt("aircraft_number", acn)
            agent.setVariableUInt("partseqno_i", 70482)
            agent.setVariableUInt("group_by", 2)
            
            # Нормативы Mi-17
            agent.setVariableUInt("ll", 1800000)
            agent.setVariableUInt("oh", 270000)
            agent.setVariableUInt("br", 1551121)
            
            # Обнуляем наработки
            agent.setVariableUInt("sne", 0)
            agent.setVariableUInt("ppr", 0)
            agent.setVariableUInt("cso", 0)
            agent.setVariableUInt("repair_days", 0)
            agent.setVariableUInt("s6_started", 0)
            agent.setVariableUInt("s6_days", 0)
            
            # Intent: хотим в operations
            agent.setVariableUInt("intent_state", 2)
            agent.setVariableUInt("prev_intent_state", 0)
            
            # MP5
            agent.setVariableUInt("daily_today_u32", 0)
            agent.setVariableUInt("daily_next_u32", 0)
            
            # Времена
            agent.setVariableUInt("repair_time", 180)
            agent.setVariableUInt("assembly_time", 180)
            agent.setVariableUInt("partout_time", 180)
            agent.setVariableUInt("mfg_date", 0)
            
            # Триггеры
            agent.setVariableUInt("assembly_trigger", 0)
            agent.setVariableUInt("active_trigger", 0)
            agent.setVariableUInt("partout_trigger", 0)
        
        # Добавляем в симуляцию
        FLAMEGPU.setPopulationData(pop, "serviceable")
        
        self.next_idx += need
        print(f"  [Spawn Day {day}] Создано {need} новых агентов (idx {self.next_idx-need}..{self.next_idx-1})")


def register_host_spawn(model: fg.ModelDescription, env_data: dict):
    """
    Регистрирует host-based spawn
    """
    print("  Регистрация HOST spawn (обход NVRTC 425)...")
    
    # Добавляем host function в конец каждого шага
    spawn_func = HostSpawnFunction(env_data)
    layer = model.newLayer("host_spawn_layer")
    layer.addHostFunction(spawn_func)
    
    print("  HOST spawn зарегистрирован")
    return spawn_func
