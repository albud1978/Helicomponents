#!/usr/bin/env python3
"""
InitFunction для загрузки dt планеров в MacroProperty

Загружает данные из sim_masterv2 в:
- mp_planer_dt[day * MAX_PLANERS + planer_idx] = dt в минутах
- mp_ac_to_idx[aircraft_number] = planer_idx

Дата: 05.01.2026
"""

import numpy as np
import pyflamegpu as fg
from typing import Dict, Tuple, Optional

MAX_PLANERS = 400
MAX_AC_NUMBER = 2000000


MAX_GROUPS = 50


class InitPlanerDtHostFunction(fg.HostFunction):
    """
    HostFunction для инициализации:
    - mp_planer_dt, mp_planer_assembly, mp_ac_to_idx, mp_idx_to_ac, mp_planer_in_ops
    - mp_planer_slots (КРИТИЧНО! Должно быть инициализировано ДО assembly)
    - mp_planer_type (для проверки соответствия типов двигателей)
    
    Выполняется один раз в начале симуляции (step 0, первый слой).
    """
    
    def __init__(self, dt_array: np.ndarray, ac_to_idx: Dict[int, int], 
                 max_days: int = 3651, assembly_array: Optional[np.ndarray] = None,
                 planer_in_ops: Optional[Dict[int, int]] = None,
                 planer_type: Optional[Dict[int, int]] = None,
                 planer_in_ops_history: Optional[np.ndarray] = None):
        """
        Args:
            dt_array: numpy массив dt значений [day * MAX_PLANERS + planer_idx]
            ac_to_idx: словарь {aircraft_number: planer_idx}
            max_days: максимальное количество дней
            assembly_array: numpy массив assembly_trigger значений (0 или 1)
            planer_in_ops: словарь {planer_idx: 1} для планеров в operations (день 0)
            planer_type: словарь {planer_idx: type (1=Mi-8, 2=Mi-17)}
            planer_in_ops_history: numpy массив [day * MAX_PLANERS + planer_idx] = 0/1 (вся история)
        """
        super().__init__()
        self.dt_array = dt_array
        self.ac_to_idx = ac_to_idx
        # Создаём обратный маппинг: planer_idx → aircraft_number
        self.idx_to_ac = {v: k for k, v in ac_to_idx.items()} if ac_to_idx else {}
        self.max_days = max_days
        self.assembly_array = assembly_array
        self.planer_in_ops = planer_in_ops or {}
        self.planer_type = planer_type or {}  # planer_idx → type (1=Mi-8, 2=Mi-17)
        self.planer_in_ops_history = planer_in_ops_history  # NEW: полная история
        self.initial_slots = {}  # (group_by, planer_idx) -> count, устанавливается позже
        self.initialized = False
    
    def set_initial_slots(self, initial_slots: Dict[Tuple[int, int], int]):
        """Устанавливает начальные слоты (вызывается после populate_agents)"""
        self.initial_slots = initial_slots or {}
    
    def run(self, FLAMEGPU):
        """Загружает dt и ac_to_idx в MacroProperty (один раз)"""
        if self.initialized:
            return
        
        step = FLAMEGPU.getStepCounter()
        
        # Инициализируем только на step 0
        if step != 0:
            return
        
        print(f"  📥 InitPlanerDt: Загрузка dt планеров в MacroProperty...")
        
        # === 1. Загрузка mp_planer_dt ===
        if self.dt_array is not None and len(self.dt_array) > 0:
            try:
                mp_dt = FLAMEGPU.environment.getMacroPropertyUInt("mp_planer_dt")
                
                # Оптимизация: загружаем только ненулевые значения
                nonzero_count = 0
                for i, val in enumerate(self.dt_array):
                    if val > 0:
                        mp_dt[i] = int(val)
                        nonzero_count += 1
                
                total_dt = np.sum(self.dt_array)
                print(f"     mp_planer_dt: {nonzero_count:,} ненулевых из {len(self.dt_array):,}, сумма={total_dt:,}")
            except Exception as e:
                print(f"     ⚠️ Ошибка mp_planer_dt: {e}")
        else:
            print(f"     ⚠️ dt_array пуст, используется fallback 90 мин/день")
        
        # === 2. Загрузка mp_ac_to_idx ===
        if self.ac_to_idx and len(self.ac_to_idx) > 0:
            try:
                mp_ac = FLAMEGPU.environment.getMacroPropertyUInt("mp_ac_to_idx")
                
                for ac_num, planer_idx in self.ac_to_idx.items():
                    if ac_num < MAX_AC_NUMBER:
                        mp_ac[ac_num] = planer_idx
                
                print(f"     mp_ac_to_idx: {len(self.ac_to_idx)} маппингов загружено")
            except Exception as e:
                print(f"     ⚠️ Ошибка mp_ac_to_idx: {e}")
        else:
            print(f"     ⚠️ ac_to_idx пуст")
        
        # === 2b. Загрузка mp_idx_to_ac (обратный маппинг) ===
        if self.idx_to_ac and len(self.idx_to_ac) > 0:
            try:
                mp_idx = FLAMEGPU.environment.getMacroPropertyUInt("mp_idx_to_ac")
                
                for planer_idx, ac_num in self.idx_to_ac.items():
                    if planer_idx < MAX_PLANERS:
                        mp_idx[planer_idx] = ac_num
                
                print(f"     mp_idx_to_ac: {len(self.idx_to_ac)} маппингов загружено")
            except Exception as e:
                print(f"     ⚠️ Ошибка mp_idx_to_ac: {e}")
        else:
            print(f"     ⚠️ idx_to_ac пуст")
        
        # === 3. Загрузка mp_planer_assembly (assembly_trigger) ===
        if self.assembly_array is not None and len(self.assembly_array) > 0:
            try:
                mp_assembly = FLAMEGPU.environment.getMacroPropertyUInt8("mp_planer_assembly")
                
                # Загружаем только единицы
                trigger_count = 0
                for i, val in enumerate(self.assembly_array):
                    if val > 0:
                        mp_assembly[i] = int(val)
                        trigger_count += 1
                
                print(f"     mp_planer_assembly: {trigger_count:,} записей с assembly_trigger=1")
            except Exception as e:
                print(f"     ⚠️ Ошибка mp_planer_assembly: {e}")
        else:
            print(f"     ⚠️ assembly_array пуст, комплектация отключена")
        
        # === 4. Загрузка mp_planer_in_ops (планеры в operations) ===
        if self.planer_in_ops and len(self.planer_in_ops) > 0:
            try:
                mp_in_ops = FLAMEGPU.environment.getMacroPropertyUInt8("mp_planer_in_ops")
                
                for planer_idx, is_ops in self.planer_in_ops.items():
                    if planer_idx < MAX_PLANERS and is_ops > 0:
                        mp_in_ops[planer_idx] = 1
                
                ops_count = sum(1 for v in self.planer_in_ops.values() if v > 0)
                print(f"     mp_planer_in_ops: {ops_count} планеров в operations")
            except Exception as e:
                print(f"     ⚠️ Ошибка mp_planer_in_ops: {e}")
        else:
            print(f"     ⚠️ planer_in_ops пуст, assembly только по trigger")
        
        # === 5. Инициализация mp_planer_slots (КРИТИЧНО!) ===
        # Это ДОЛЖНО быть сделано ДО assembly, иначе агрегаты назначатся на планеры
        # которые уже имеют свои агрегаты из исходных данных
        if self.initial_slots:
            try:
                mp_slots = FLAMEGPU.environment.getMacroPropertyUInt32("mp_planer_slots")
                
                for (gb, planer_idx), count in self.initial_slots.items():
                    if gb < MAX_GROUPS and planer_idx < MAX_PLANERS:
                        slot_pos = gb * MAX_PLANERS + planer_idx
                        mp_slots[slot_pos] = count
                
                total_slots = sum(self.initial_slots.values())
                print(f"     mp_planer_slots: {total_slots} агрегатов на {len(self.initial_slots)} планерах (ДО assembly!)")
            except Exception as e:
                print(f"     ⚠️ Ошибка mp_planer_slots: {e}")
        else:
            print(f"     ⚠️ initial_slots пуст, assembly может назначить лишние агрегаты!")
        
        # === 6. Инициализация mp_planer_type (для проверки соответствия типов) ===
        # Тип планера: 1=Mi-8, 2=Mi-17
        # Используется для проверки соответствия типа двигателя:
        #   - group_by=3 (ТВ2-117) → только для Mi-8 (planer_type=1)
        #   - group_by=4 (ТВ3-117) → только для Mi-17 (planer_type=2)
        if self.planer_type:
            try:
                mp_type = FLAMEGPU.environment.getMacroPropertyUInt8("mp_planer_type")
                
                mi8_count = 0
                mi17_count = 0
                for planer_idx, ptype in self.planer_type.items():
                    if planer_idx < MAX_PLANERS:
                        mp_type[planer_idx] = ptype
                        if ptype == 1:
                            mi8_count += 1
                        else:
                            mi17_count += 1
                
                print(f"     mp_planer_type: Mi-8={mi8_count}, Mi-17={mi17_count}")
            except Exception as e:
                print(f"     ⚠️ Ошибка mp_planer_type: {e}")
        else:
            print(f"     ⚠️ planer_type пуст, проверка соответствия типов отключена")
        
        # === 7. Загрузка mp_planer_in_ops_history (полная история состояний планеров) ===
        # Используется для детекции ВЫХОДА планера из operations
        if self.planer_in_ops_history is not None and len(self.planer_in_ops_history) > 0:
            try:
                mp_history = FLAMEGPU.environment.getMacroPropertyUInt8("mp_planer_in_ops_history")
                
                # Загружаем только единицы
                ops_count = 0
                for i, val in enumerate(self.planer_in_ops_history):
                    if val > 0:
                        mp_history[i] = 1
                        ops_count += 1
                
                print(f"     mp_planer_in_ops_history: {ops_count:,} записей operations")
            except Exception as e:
                print(f"     ⚠️ Ошибка mp_planer_in_ops_history: {e}")
        else:
            print(f"     ⚠️ planer_in_ops_history пуст, детекция выхода из operations отключена")
        
        self.initialized = True
        print(f"  ✅ InitPlanerDt: Инициализация завершена")


def register_init_planer_dt(model: fg.ModelDescription, 
                            dt_array: np.ndarray, 
                            ac_to_idx: Dict[int, int],
                            max_days: int = 3651,
                            assembly_array: Optional[np.ndarray] = None,
                            planer_in_ops: Optional[Dict[int, int]] = None,
                            planer_type: Optional[Dict[int, int]] = None,
                            planer_in_ops_history: Optional[np.ndarray] = None) -> InitPlanerDtHostFunction:
    """
    Регистрирует InitFunction для загрузки dt планеров, assembly_trigger, planer_in_ops, planer_type и истории operations
    
    Args:
        model: описание модели FLAME GPU
        dt_array: массив dt значений
        ac_to_idx: маппинг aircraft_number → planer_idx
        max_days: максимальное количество дней
        assembly_array: массив assembly_trigger значений (0 или 1)
        planer_in_ops: Dict[planer_idx → 1] для планеров в operations (день 0)
        planer_type: Dict[planer_idx → type (1=Mi-8, 2=Mi-17)]
        planer_in_ops_history: массив [day * MAX_PLANERS + planer_idx] = 0/1 (полная история)
    
    Returns:
        InitPlanerDtHostFunction для возможного повторного использования
    """
    hf = InitPlanerDtHostFunction(dt_array, ac_to_idx, max_days, assembly_array, planer_in_ops, planer_type, planer_in_ops_history)
    
    # Создаём слой инициализации (должен быть первым!)
    init_layer = model.newLayer("layer_init_planer_dt")
    init_layer.addHostFunction(hf)
    
    print(f"  ✅ InitPlanerDt зарегистрирован (слой layer_init_planer_dt)")
    
    return hf

