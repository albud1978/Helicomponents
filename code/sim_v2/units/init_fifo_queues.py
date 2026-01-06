#!/usr/bin/env python3
"""
InitFunction для инициализации FIFO-очередей агрегатов

Инициализирует MacroProperties:
- mp_svc_tail[group] — хвост serviceable очереди
- mp_rsv_tail[group] — хвост reserve очереди

Дата: 05.01.2026
"""

import pyflamegpu as fg

MAX_GROUPS = 50


class InitFifoQueuesHostFunction(fg.HostFunction):
    """
    Инициализирует mp_svc_tail и mp_rsv_tail из данных популяции
    """
    
    def __init__(self, svc_tails: dict, rsv_tails: dict):
        super().__init__()
        self.svc_tails = svc_tails
        self.rsv_tails = rsv_tails
        self.initialized = False
    
    def run(self, FLAMEGPU):
        if self.initialized:
            return
        
        print("  📊 InitFifoQueues: Инициализация очередей...")
        
        env = FLAMEGPU.environment
        
        # Инициализируем mp_svc_tail
        try:
            mp_svc_tail = env.getMacroPropertyUInt32("mp_svc_tail")
            for gb, tail in self.svc_tails.items():
                if gb < MAX_GROUPS:
                    mp_svc_tail[gb] = tail
            print(f"     mp_svc_tail: {sum(self.svc_tails.values())} агентов в {len([v for v in self.svc_tails.values() if v > 0])} группах")
        except Exception as e:
            print(f"     ⚠️ mp_svc_tail: {e}")
        
        # Инициализируем mp_rsv_tail
        try:
            mp_rsv_tail = env.getMacroPropertyUInt32("mp_rsv_tail")
            for gb, tail in self.rsv_tails.items():
                if gb < MAX_GROUPS:
                    mp_rsv_tail[gb] = tail
            print(f"     mp_rsv_tail: {sum(self.rsv_tails.values())} агентов в {len([v for v in self.rsv_tails.values() if v > 0])} группах")
        except Exception as e:
            print(f"     ⚠️ mp_rsv_tail: {e}")
        
        self.initialized = True
        print("  ✅ InitFifoQueues: Очереди инициализированы")

