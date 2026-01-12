#!/usr/bin/env python3
"""
InitFunction для инициализации FIFO-очередей агрегатов (ДО первого шага!)

Инициализирует MacroProperties:
- mp_svc_head[group] — голова serviceable очереди (= 1)
- mp_svc_tail[group] — хвост serviceable очереди (= N+1, где N = кол-во агентов)
- mp_rsv_head[group] — голова reserve очереди (= 1)
- mp_rsv_tail[group] — хвост reserve очереди (= M+1)
- mp_planer_slots[group * MAX_PLANERS + planer_idx] — начальное количество агрегатов

ВАЖНО: head = 1, т.к. queue_position агентов начинается с 1!

Используется как InitFunction (addInitFunction), выполняется ОДИН РАЗ ДО step=0.
Это критично, т.к. mp_planer_slots должен быть инициализирован ДО assembly!

Дата: 06.01.2026, обновлено 09.01.2026
"""

import pyflamegpu as fg

MAX_GROUPS = 50


MAX_PLANERS = 400


class InitFifoQueuesFunction(fg.HostFunction):
    """
    InitFunction для инициализации mp_svc_tail, mp_rsv_tail и mp_planer_slots.
    Выполняется ОДИН РАЗ ДО первого шага (addInitFunction).
    
    ВАЖНО: Это НЕ StepFunction! Выполняется ДО step=0, чтобы mp_planer_slots
    был инициализирован до первого вызова assembly.
    """
    
    def __init__(self):
        super().__init__()
        self.svc_tails = {}  # Будет установлено позже
        self.rsv_tails = {}  # Будет установлено позже
        self.initial_slots = {}  # Будет установлено позже
    
    def set_tails(self, svc_tails: dict, rsv_tails: dict, initial_slots: dict = None):
        """Устанавливает данные очередей (вызывается после populate_agents)"""
        self.svc_tails = svc_tails
        self.rsv_tails = rsv_tails
        self.initial_slots = initial_slots or {}
    
    def run(self, FLAMEGPU):
        print("  📊 InitFifoQueues: Инициализация очередей ДО step=0...")
        
        # Инициализируем mp_svc_head и mp_svc_tail
        try:
            mp_svc_head = FLAMEGPU.environment.getMacroPropertyUInt32("mp_svc_head")
            mp_svc_tail = FLAMEGPU.environment.getMacroPropertyUInt32("mp_svc_tail")
            # Инициализируем ВСЕ группы, даже если svc_tails[gb] = 0
            all_groups = set(self.svc_tails.keys()) | set(self.rsv_tails.keys())
            for gb in all_groups:
                if gb < MAX_GROUPS:
                    tail = self.svc_tails.get(gb, 0)
                    mp_svc_head[gb] = 1       # head = 1 (ВСЕГДА)
                    mp_svc_tail[gb] = tail + 1 if tail > 0 else 1  # tail = N+1 или 1 если пусто
            total_svc = sum(self.svc_tails.values())
            groups_svc = len(all_groups)
            print(f"     mp_svc: head=1, tail инициализирован для {groups_svc} групп ({total_svc} агентов)")
        except Exception as e:
            print(f"     ⚠️ mp_svc: {e}")
        
        # Инициализируем mp_rsv_head и mp_rsv_tail
        try:
            mp_rsv_head = FLAMEGPU.environment.getMacroPropertyUInt32("mp_rsv_head")
            mp_rsv_tail = FLAMEGPU.environment.getMacroPropertyUInt32("mp_rsv_tail")
            # Инициализируем ВСЕ группы, даже если rsv_tails[gb] = 0
            # Это нужно чтобы spawn и 4→5 работали корректно
            all_groups = set(self.svc_tails.keys()) | set(self.rsv_tails.keys())
            for gb in all_groups:
                if gb < MAX_GROUPS:
                    tail = self.rsv_tails.get(gb, 0)
                    mp_rsv_head[gb] = 1       # head = 1 (ВСЕГДА)
                    mp_rsv_tail[gb] = tail + 1 if tail > 0 else 1  # tail = M+1 или 1 если пусто
            total_rsv = sum(self.rsv_tails.values())
            groups_rsv = len(all_groups)
            print(f"     mp_rsv: head=1, tail инициализирован для {groups_rsv} групп ({total_rsv} агентов)")
        except Exception as e:
            print(f"     ⚠️ mp_rsv: {e}")
        
        # Инициализируем mp_planer_slots (сколько агрегатов уже на планерах)
        try:
            if self.initial_slots:
                mp_slots = FLAMEGPU.environment.getMacroPropertyUInt32("mp_planer_slots")
                for (gb, planer_idx), count in self.initial_slots.items():
                    if gb < MAX_GROUPS and planer_idx < MAX_PLANERS:
                        slot_pos = gb * MAX_PLANERS + planer_idx
                        mp_slots[slot_pos] = count
                total_slots = sum(self.initial_slots.values())
                print(f"     mp_planer_slots: {total_slots} агрегатов на {len(self.initial_slots)} планерах")
        except Exception as e:
            print(f"     ⚠️ mp_planer_slots: {e}")
        
        print("  ✅ InitFifoQueues: Очереди и слоты инициализированы ДО step=0")


# Для обратной совместимости
InitFifoQueuesStepFunction = InitFifoQueuesFunction  # Алиас для старого кода
InitFifoQueuesHostFunction = InitFifoQueuesFunction

