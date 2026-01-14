#!/usr/bin/env python3
"""
V2 Base Model для Агрегатов (Units): базовая модель и окружение для RTC симуляции агрегатов

Архитектурные отличия от планеров:
- Нет state "inactive" (новые агрегаты создаются в reserve)
- Добавлена переменная queue_position для FIFO-очереди
- Добавлен psn (PRIMARY KEY) вместо aircraft_number
- Связь с планером через aircraft_number (0 = в пуле)
- MacroProperty для FIFO-очереди замен

Дата: 05.01.2026
"""
import os
import sys
from typing import Dict, Optional, List

# Добавляем пути
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


# Константы для агрегатов (отдельно от планеров!)
MAX_GROUPS = 50  # Реально до 42 групп
UNITS_MAX_FRAMES = 40000  # ~10634 агрегатов + ~20% резерв по формуле оборота
UNITS_MAX_DAYS = 4000     # 10+ лет  # group_by от 0 до 24


class V2BaseModelUnits:
    """Базовая модель для симуляции агрегатов (group_by >= 3)"""
    
    def __init__(self):
        self.model: Optional[fg.ModelDescription] = None
        self.agent: Optional[fg.AgentDescription] = None
        self.env: Optional[fg.EnvironmentDescription] = None
        self.env_data: Optional[Dict[str, object]] = None
    
    def create_model(self, env_data: Dict[str, object]) -> fg.ModelDescription:
        """Создает базовую модель для агрегатов"""
        
        self.env_data = env_data
        
        # Для агрегатов используем собственные константы (не model_build)
        frames_from_data = int(env_data.get('units_frames_total', UNITS_MAX_FRAMES))
        if frames_from_data > UNITS_MAX_FRAMES:
            print(f"⚠️ units_frames_total={frames_from_data} > UNITS_MAX_FRAMES={UNITS_MAX_FRAMES}")
            frames_from_data = UNITS_MAX_FRAMES
        
        self.model = fg.ModelDescription("HeliUnitsSimV2")
        self.env = self.model.Environment()
        
        # Настройка окружения
        self._setup_scalar_properties(env_data)
        self._setup_macro_properties(env_data)
        self._setup_property_arrays(env_data)
        
        # Агент UNIT (агрегат)
        self.agent = self._setup_agent()
        
        return self.model
    
    def _setup_scalar_properties(self, env_data: Dict[str, object]):
        """Настройка скалярных свойств окружения"""
        self.env.newPropertyUInt("version_date", int(env_data['version_date_u16']))
        self.env.newPropertyUInt("version_id", int(env_data.get('version_id_u32', 0)))
        self.env.newPropertyUInt("units_frames_total", int(env_data.get('units_frames_total', 400)))
        self.env.newPropertyUInt("days_total", int(env_data['days_total_u16']))
        
        # Фазовый флаг для постпроцессинга
        self.env.newPropertyUInt("export_phase", 0)
        
        # Количество планеров для расчёта нормы пула
        self.env.newPropertyUInt("planers_mi8_count", int(env_data.get('n_mi8', 163)))
        self.env.newPropertyUInt("planers_mi17_count", int(env_data.get('n_mi17', 116)))
    
    def _setup_macro_properties(self, env_data: Dict[str, object]):
        """Настройка MacroProperty для FIFO-очереди и данных"""
        max_frames = int(env_data.get('units_frames_total', UNITS_MAX_FRAMES))
        max_days = int(env_data.get('days_total_u16', UNITS_MAX_DAYS))
        
        # Сохраняем для использования в других модулях
        self._mp2_max_frames = max_frames
        self._mp2_max_days = max_days
        
        # === FIFO-очереди для замены агрегатов (трёхуровневая приоритетная система) ===
        # Приоритет 1: Serviceable (готовые на складе)
        self.env.newMacroPropertyUInt32("mp_svc_head", MAX_GROUPS)  # Голова очереди serviceable
        self.env.newMacroPropertyUInt32("mp_svc_tail", MAX_GROUPS)  # Хвост очереди serviceable
        # Приоритет 2: Reserve (после ремонта, active=1)
        self.env.newMacroPropertyUInt32("mp_rsv_head", MAX_GROUPS)  # Голова очереди reserve
        self.env.newMacroPropertyUInt32("mp_rsv_tail", MAX_GROUPS)  # Хвост очереди reserve
        # FIX 14.01.2026: Точный счётчик свободных агентов в reserve (не rsv_tail - rsv_head!)
        # rsv_tail - rsv_head показывает "транзитных" агентов, не реальных свободных
        self.env.newMacroPropertyUInt32("mp_rsv_count", MAX_GROUPS)  # Реальное количество свободных в reserve
        # Приоритет 3: Spawn (active=0) — не в очереди, активируется при пустых очередях
        
        # Устаревшие (для совместимости, можно удалить позже)
        self.env.newMacroPropertyUInt32("mp_queue_head", MAX_GROUPS)
        self.env.newMacroPropertyUInt32("mp_queue_tail", MAX_GROUPS)
        # Запросы на замену: aircraft_number для которого нужна замена (0 = нет запроса)
        self.env.newMacroPropertyUInt32("mp_replacement_request", max_frames)
        # group_by для запроса замены
        self.env.newMacroPropertyUInt32("mp_replacement_group", max_frames)
        
        # === Счётчики по группам ===
        # Количество агентов в каждом состоянии по группам
        self.env.newMacroPropertyUInt32("units_pool_count", MAX_GROUPS)  # reserve + serviceable
        self.env.newMacroPropertyUInt32("units_ops_count", MAX_GROUPS)   # operations
        self.env.newMacroPropertyUInt32("units_repair_count", MAX_GROUPS)  # repair
        # Счётчик запросов на замену по группам (для FIFO Phase 1→2)
        self.env.newMacroPropertyUInt32("mp_request_count", MAX_GROUPS)
        
        # === MP dt планеров (для чтения агрегатами) ===
        # Формат: mp_planer_dt[day * MAX_PLANERS + planer_idx] = dt в минутах
        MAX_PLANERS = 400
        planer_dt_size = MAX_PLANERS * (max_days + 1)
        self.env.newMacroPropertyUInt("mp_planer_dt", planer_dt_size)
        # Маппинг aircraft_number → planer_idx (для агрегатов)
        self.env.newMacroPropertyUInt("mp_ac_to_idx", 2000000)  # MAX aircraft_number
        
        # === assembly_trigger планеров (для комплектации агрегатами) ===
        # Формат: mp_planer_assembly[day * MAX_PLANERS + planer_idx] = 0 или 1
        # assembly_trigger=1 означает, что планер в последней стадии ремонта
        self.env.newMacroPropertyUInt8("mp_planer_assembly", planer_dt_size)
        
        # === Слоты комплектации: сколько агрегатов группы назначено на планер ===
        # Формат: mp_planer_slots[group_by * MAX_PLANERS + planer_idx] = count
        self.env.newMacroPropertyUInt32("mp_planer_slots", MAX_GROUPS * MAX_PLANERS)
        
        # === Обратный маппинг: planer_idx → aircraft_number ===
        self.env.newMacroPropertyUInt32("mp_idx_to_ac", MAX_PLANERS)
        
        # === Флаг: планер в operations (для assembly) ===
        # mp_planer_in_ops[planer_idx] = 1 если планер в operations, 0 иначе
        self.env.newMacroPropertyUInt8("mp_planer_in_ops", MAX_PLANERS)
        
        # === Тип планера (для проверки соответствия с типом двигателя) ===
        # mp_planer_type[planer_idx] = 1 (Mi-8) или 2 (Mi-17)
        # Используется в assembly для проверки:
        #   - group_by=3 (ТВ2-117) → только для Mi-8 (planer_type=1)
        #   - group_by=4 (ТВ3-117) → только для Mi-17 (planer_type=2)
        self.env.newMacroPropertyUInt8("mp_planer_type", MAX_PLANERS)
        
        # === Дефицит планеров (не хватает агрегатов) ===
        # mp_planer_deficit[group_by * MAX_PLANERS + planer_idx] = количество недостающих агрегатов
        # Если > 0, планер не летает (dt не применяется) пока не получит агрегат
        # ВАЖНО: используем UInt32 для совместимости с CUDA atomicAdd
        self.env.newMacroPropertyUInt32("mp_planer_deficit", MAX_GROUPS * MAX_PLANERS)
        
        # === Лог дефицитов для аналитики ===
        # mp_deficit_days[group_by * MAX_PLANERS + planer_idx] = количество дней в дефиците
        # ВАЖНО: используем UInt32 т.к. CUDA atomicAdd не поддерживает UInt16
        self.env.newMacroPropertyUInt32("mp_deficit_days", MAX_GROUPS * MAX_PLANERS)
        
        # === MP2 для агрегатов (циклический буфер на DRAIN_INTERVAL дней) ===
        DRAIN_INTERVAL = 10
        mp2_buffer_size = max_frames * (DRAIN_INTERVAL + 1)  # ~400K элементов
        self._mp2_drain_interval = DRAIN_INTERVAL
        print(f"  MP2 Units: max_frames={max_frames}, buffer_days={DRAIN_INTERVAL}, buffer_size={mp2_buffer_size:,}")
        
        # Основные поля (используем newMacroPropertyUInt как у планеров)
        self.env.newMacroPropertyUInt("mp2_units_psn", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_group_by", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_sne", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_ppr", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_state", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_ac", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_repair_days", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_queue_pos", mp2_buffer_size)
        # Дополнительные поля для полной истории
        self.env.newMacroPropertyUInt("mp2_units_partseqno", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_active", mp2_buffer_size)
        
        # Сохраняем для RTC и drain
        self.env.newPropertyUInt("mp2_drain_interval", DRAIN_INTERVAL)
    
    def _setup_property_arrays(self, env_data: Dict[str, object]):
        """Настройка PropertyArray для небольших массивов"""
        # comp_number по group_by (количество агрегатов на 1 вертолёт)
        comp_numbers = list(env_data.get('comp_numbers', [0] * MAX_GROUPS))
        comp_numbers = (comp_numbers + [0] * MAX_GROUPS)[:MAX_GROUPS]
        self.env.newPropertyArrayUInt32("comp_numbers", comp_numbers)
        
        # repair_time по group_by (время ремонта агрегатов)
        repair_times = list(env_data.get('units_repair_times', [30] * MAX_GROUPS))
        repair_times = (repair_times + [30] * MAX_GROUPS)[:MAX_GROUPS]
        self.env.newPropertyArrayUInt32("units_repair_times", repair_times)
    
    def _setup_agent(self) -> fg.AgentDescription:
        """Создание и настройка агента UNIT (агрегат)"""
        agent = self.model.newAgent("unit")
        
        # === Состояния агента (без inactive!) ===
        agent.newState("operations")    # state_2: на планере, в полёте
        agent.newState("serviceable")   # state_3: исправен, на складе
        agent.newState("repair")        # state_4: в ремонте
        agent.newState("reserve")       # state_5: готов к установке
        agent.newState("storage")       # state_6: хранение/списание
        
        # === Идентификаторы ===
        agent.newVariableUInt("idx", 0)                 # Плотный индекс
        agent.newVariableUInt("psn", 0)                 # PRIMARY KEY (уникальный ID агрегата)
        agent.newVariableUInt("aircraft_number", 0)     # Номер планера (0 = в пуле)
        agent.newVariableUInt("partseqno_i", 0)         # Код типа агрегата
        agent.newVariableUInt("group_by", 0)            # Тип агрегата (3-24)
        
        # === FIFO-очередь ===
        agent.newVariableUInt("queue_position", 0)      # Позиция в очереди замен
        
        # === Наработки ===
        agent.newVariableUInt("sne", 0)                 # Наработка с начала эксплуатации
        agent.newVariableUInt("ppr", 0)                 # Наработка после последнего ремонта
        
        # === Нормативы ===
        agent.newVariableUInt("ll", 0)                  # Назначенный ресурс
        agent.newVariableUInt("oh", 0)                  # Межремонтный ресурс
        agent.newVariableUInt("br", 0)                  # Breakeven repair порог
        
        # === Ремонт ===
        agent.newVariableUInt("repair_time", 30)        # Нормативное время ремонта
        agent.newVariableUInt("repair_days", 0)         # Дней в ремонте
        
        # === Управление ===
        agent.newVariableUInt("intent_state", 0)        # Намерение перехода
        agent.newVariableUInt("mfg_date", 0)            # Дата производства
        agent.newVariableUInt("active", 1)              # Активен (0 = резервный слот для spawn)
        agent.newVariableUInt("want_spawn", 0)          # Флаг для двухфазного spawn
        agent.newVariableUInt("want_assign", 0)         # Флаг для двухфазного assign
        agent.newVariableUInt("target_planer_idx", 0)   # Временное хранение planer_idx для assembly
        
        # === Служебное ===
        agent.newVariableUInt("bi_counter", 1)          # Счётчик для BI
        
        # === Флаги переходов ===
        agent.newVariableUInt("transition_2_to_3", 0)   # operations → serviceable
        agent.newVariableUInt("transition_2_to_4", 0)   # operations → repair
        agent.newVariableUInt("transition_2_to_6", 0)   # operations → storage
        agent.newVariableUInt("transition_3_to_2", 0)   # serviceable → operations
        agent.newVariableUInt("transition_4_to_5", 0)   # repair → reserve
        agent.newVariableUInt("transition_5_to_2", 0)   # reserve → operations
        
        return agent
    
    def add_rtc_module(self, module_name: str):
        """Динамически подключает RTC модуль по имени"""
        try:
            if module_name == "units_count":
                import rtc_units_count
                rtc_units_count.register_rtc(self.model, self.agent)
                
            elif module_name == "units_request_replacement":
                import rtc_units_request_replacement
                rtc_units_request_replacement.register_rtc(self.model, self.agent)
                
            elif module_name == "units_fifo_assignment":
                import rtc_units_fifo_assignment
                rtc_units_fifo_assignment.register_rtc(self.model, self.agent)
                
            elif module_name == "units_state_manager":
                import rtc_units_state_manager
                rtc_units_state_manager.register_rtc(self.model, self.agent)
                
            elif module_name == "units_return_to_pool":
                import rtc_units_return_to_pool
                rtc_units_return_to_pool.register_rtc(self.model, self.agent)
                
            else:
                # Стандартная обработка
                module = __import__(f'rtc_units_{module_name}', fromlist=['register_rtc'])
                if hasattr(module, 'register_rtc'):
                    module.register_rtc(self.model, self.agent)
                else:
                    raise ValueError(f"Модуль rtc_units_{module_name} не содержит функцию register_rtc")
                    
        except ImportError as e:
            raise RuntimeError(f"Не удалось загрузить RTC модуль '{module_name}': {e}")

