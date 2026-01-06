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


class InitPlanerDtHostFunction(fg.HostFunction):
    """
    HostFunction для инициализации mp_planer_dt и mp_ac_to_idx
    
    Выполняется один раз в начале симуляции (step 0).
    """
    
    def __init__(self, dt_array: np.ndarray, ac_to_idx: Dict[int, int], max_days: int = 3651):
        """
        Args:
            dt_array: numpy массив dt значений [day * MAX_PLANERS + planer_idx]
            ac_to_idx: словарь {aircraft_number: planer_idx}
            max_days: максимальное количество дней
        """
        super().__init__()
        self.dt_array = dt_array
        self.ac_to_idx = ac_to_idx
        self.max_days = max_days
        self.initialized = False
    
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
        
        self.initialized = True
        print(f"  ✅ InitPlanerDt: Инициализация завершена")


def register_init_planer_dt(model: fg.ModelDescription, 
                            dt_array: np.ndarray, 
                            ac_to_idx: Dict[int, int],
                            max_days: int = 3651) -> InitPlanerDtHostFunction:
    """
    Регистрирует InitFunction для загрузки dt планеров
    
    Args:
        model: описание модели FLAME GPU
        dt_array: массив dt значений
        ac_to_idx: маппинг aircraft_number → planer_idx
        max_days: максимальное количество дней
    
    Returns:
        InitPlanerDtHostFunction для возможного повторного использования
    """
    hf = InitPlanerDtHostFunction(dt_array, ac_to_idx, max_days)
    
    # Создаём слой инициализации (должен быть первым!)
    init_layer = model.newLayer("layer_init_planer_dt")
    init_layer.addHostFunction(hf)
    
    print(f"  ✅ InitPlanerDt зарегистрирован (слой layer_init_planer_dt)")
    
    return hf

