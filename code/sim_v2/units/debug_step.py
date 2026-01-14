#!/usr/bin/env python3
"""
Отладочная StepFunction для вывода состояния очередей и request_count
Дата: 14.01.2026
"""

import pyflamegpu as fg

MAX_GROUPS = 50


class DebugQueueStepFunction(fg.HostFunction):
    """Выводит состояние очередей каждые N шагов"""
    
    def __init__(self, interval: int = 100, target_groups: list = None):
        super().__init__()
        self.interval = interval
        self.target_groups = target_groups or [3, 4]
    
    def run(self, FLAMEGPU):
        step_day = FLAMEGPU.getStepCounter()
        
        if step_day % self.interval != 0 and step_day != 0:
            return
        
        mp_request_count = FLAMEGPU.environment.getMacroPropertyUInt32("mp_request_count")
        mp_svc_head = FLAMEGPU.environment.getMacroPropertyUInt32("mp_svc_head")
        mp_svc_tail = FLAMEGPU.environment.getMacroPropertyUInt32("mp_svc_tail")
        mp_rsv_head = FLAMEGPU.environment.getMacroPropertyUInt32("mp_rsv_head")
        mp_rsv_tail = FLAMEGPU.environment.getMacroPropertyUInt32("mp_rsv_tail")
        mp_rsv_count = FLAMEGPU.environment.getMacroPropertyUInt32("mp_rsv_count")  # FIX 14.01.2026
        
        for gb in self.target_groups:
            req = int(mp_request_count[gb])
            svc_h, svc_t = int(mp_svc_head[gb]), int(mp_svc_tail[gb])
            rsv_h, rsv_t = int(mp_rsv_head[gb]), int(mp_rsv_tail[gb])
            rsv_cnt = int(mp_rsv_count[gb])  # Точный счётчик
            
            svc_len = svc_t - svc_h if svc_t > svc_h else 0
            rsv_len = rsv_t - rsv_h if rsv_t > rsv_h else 0
            
            if step_day == 0 or req > 0 or svc_len == 0 or rsv_cnt == 0:
                print(f"  🔍 День {step_day}, group={gb}: req={req}, svc={svc_len}, rsv_count={rsv_cnt} (queue={rsv_len})")

