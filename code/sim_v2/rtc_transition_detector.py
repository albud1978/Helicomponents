#!/usr/bin/env python3
"""
Host функция для постпроцессинга MP2: детектирование переходов между состояниями
Работает ПОСЛЕ дренажа MP2 - читает выгруженную таблицу sim_masterv2 и добавляет transition_ флаги
"""

import pyflamegpu as fg
import time


class TransitionDetectorHostFunction(fg.HostFunction):
    """Host функция для детектирования переходов между состояниями"""
    
    def __init__(self, client, table_name: str = 'sim_masterv2'):
        super().__init__()
        self.client = client
        self.table_name = table_name
        self.is_done = False
        
    def run(self, FLAMEGPU):
        """Детектирует переходы в таблице MP2 после дренажа"""
        if self.is_done:
            return  # Запускается один раз в конце
        
        # Просто выполняем один раз — это достаточно
        # (вызывается после всех шагов симуляции и дренажа MP2)
        
        print("\n🔍 Детектирование переходов между состояниями...")
        t_start = time.perf_counter()
        
        try:
            # Сначала обнуляем все transition поля
            reset_transitions = f"""
ALTER TABLE {self.table_name}
UPDATE 
    transition_2_to_4 = 0,
    transition_2_to_6 = 0,
    transition_2_to_3 = 0,
    transition_3_to_2 = 0,
    transition_5_to_2 = 0,
    transition_1_to_2 = 0,
    transition_4_to_5 = 0,
    transition_1_to_4 = 0,
    transition_4_to_2 = 0
WHERE version_date = (SELECT max(version_date) FROM {self.table_name})
"""
            
            print("  🔄 Обнуляем transition поля...")
            self.client.execute(reset_transitions)
            
            # Теперь устанавливаем флаги переходов используя window функцию LAG
            set_transitions = f"""
ALTER TABLE {self.table_name}
UPDATE 
    transition_2_to_4 = if(state = 'repair' AND lag_state = 'operations', 1, 0),
    transition_2_to_6 = if(state = 'storage' AND lag_state = 'operations', 1, 0),
    transition_2_to_3 = if(state = 'serviceable' AND lag_state = 'operations', 1, 0),
    transition_3_to_2 = if(state = 'operations' AND lag_state = 'serviceable', 1, 0),
    transition_5_to_2 = if(state = 'operations' AND lag_state = 'reserve', 1, 0),
    transition_1_to_2 = if(state = 'operations' AND lag_state = 'inactive', 1, 0),
    transition_4_to_5 = if(state = 'reserve' AND lag_state = 'repair', 1, 0),
    transition_1_to_4 = if(state = 'repair' AND lag_state = 'inactive', 1, 0),
    transition_4_to_2 = if(state = 'operations' AND lag_state = 'repair', 1, 0)
FROM (
    SELECT 
        *,
        lagIf(state, 1) OVER (PARTITION BY aircraft_number ORDER BY day_u16) as lag_state
    FROM {self.table_name}
    WHERE version_date = (SELECT max(version_date) FROM {self.table_name})
)
WHERE version_date = (SELECT max(version_date) FROM {self.table_name})
"""
            print("  ✅ Устанавливаем transition флаги...")
            self.client.execute(set_transitions)
            
        except Exception as e:
            print(f"  ❌ Ошибка при детектировании переходов: {e}")
            # Fallback - используем более простой SQL без UPDATE
            self._fallback_transitions()
        
        t_elapsed = time.perf_counter() - t_start
        print(f"  ✅ Детектирование переходов завершено за {t_elapsed:.2f}с")
        
        self.is_done = True
    
    def _fallback_transitions(self):
        """Fallback: вычисляем переходы через SELECT и заполняем вручную"""
        print("  🔄 Используем fallback метод (вычисление переходов через Python)...")
        
        # Считываем данные из таблицы
        query = f"""
SELECT 
    aircraft_number, day_u16, state
FROM {self.table_name}
WHERE version_date = (SELECT max(version_date) FROM {self.table_name})
ORDER BY aircraft_number, day_u16
"""
        
        rows = self.client.execute(query)
        
        # Группируем по aircraft_number и вычисляем переходы
        prev_row = None
        
        for row in rows:
            curr_aircraft = row[0]  # aircraft_number
            curr_state = row[2]     # state
            curr_day = row[1]       # day_u16
            
            if prev_row and prev_row[0] == curr_aircraft:  # Same aircraft
                prev_state = prev_row[2]
                
                # Определяем переход
                if prev_state != curr_state:
                    transition_field = f"transition_{self._state_to_id(prev_state)}_to_{self._state_to_id(curr_state)}"
                    update = f"""
ALTER TABLE {self.table_name}
UPDATE {transition_field} = 1
WHERE aircraft_number = {curr_aircraft} AND day_u16 = {curr_day}
"""
                    try:
                        self.client.execute(update)
                    except:
                        pass  # Ignore errors for unknown transitions
            
            prev_row = row
    
    def _state_to_id(self, state_str: str) -> int:
        """Конвертирует string state в числовой ID"""
        mapping = {
            'inactive': 1,
            'operations': 2,
            'serviceable': 3,
            'repair': 4,
            'reserve': 5,
            'storage': 6
        }
        return mapping.get(state_str, 0)


def register_mp2_transition_detector(model, clickhouse_client):
    """Регистрирует host функцию для детектирования переходов"""
    detector = TransitionDetectorHostFunction(clickhouse_client, 'sim_masterv2')
    
    # Создаем слой с host функцией в конце симуляции
    layer = model.newLayer("detect_transitions")
    layer.addHostFunction(detector)
    
    return detector
