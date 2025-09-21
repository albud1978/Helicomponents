#!/usr/bin/env python3
"""
V2 Base Model: базовая модель и окружение для всех RTC модулей
Создает пустую модель с правильными Environment properties и Agent variables
"""
import os
import sys
from typing import Dict, Optional

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from model_build import MAX_FRAMES, MAX_DAYS, MAX_SIZE, set_max_frames_from_data

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


class V2BaseModel:
    """Базовая модель для V2 архитектуры с поддержкой модульных RTC"""
    
    def __init__(self):
        self.model: Optional[fg.ModelDescription] = None
        self.agent: Optional[fg.AgentDescription] = None
        self.env: Optional[fg.EnvironmentDescription] = None
    
    def create_model(self, env_data: Dict[str, object]) -> fg.ModelDescription:
        """Создает базовую модель с окружением из env_data"""
        
        # Устанавливаем MAX_FRAMES из данных
        frames_from_data = int(env_data['frames_total_u16'])
        if MAX_FRAMES is None:
            set_max_frames_from_data(frames_from_data)
        
        self.model = fg.ModelDescription("HeliSimV2")
        self.env = self.model.Environment()
        
        # Скалярные свойства окружения
        self._setup_scalar_properties(env_data)
        
        # MacroProperty для данных
        self._setup_macro_properties()
        
        # PropertyArray для квот и планов
        self._setup_property_arrays(env_data)
        
        # Агент HELI
        self.agent = self._setup_agent()
        
        return self.model
    
    def _setup_scalar_properties(self, env_data: Dict[str, object]):
        """Настройка скалярных свойств окружения"""
        self.env.newPropertyUInt("version_date", int(env_data['version_date_u16']))
        self.env.newPropertyUInt("frames_total", int(env_data['frames_total_u16']))
        self.env.newPropertyUInt("days_total", int(env_data['days_total_u16']))
        
        # Константы нормативов из MP1
        self.env.newPropertyUInt("mi8_repair_time_const", int(env_data.get('mi8_repair_time_const', 180)))
        self.env.newPropertyUInt("mi8_assembly_time_const", int(env_data.get('mi8_assembly_time_const', 180)))
        self.env.newPropertyUInt("mi17_repair_time_const", int(env_data.get('mi17_repair_time_const', 180)))
        self.env.newPropertyUInt("mi17_assembly_time_const", int(env_data.get('mi17_assembly_time_const', 180)))
        
        # Инициализация констант для группы 17
        if 'mi17_ll_const' in env_data:
            self.env.newPropertyUInt("mi17_ll_const", int(env_data['mi17_ll_const']))
            self.env.newPropertyUInt("mi17_oh_const", int(env_data['mi17_oh_const']))
            self.env.newPropertyUInt("mi17_br_const", int(env_data['mi17_br_const']))
    
    def _setup_macro_properties(self):
        """Настройка MacroProperty для больших данных"""
        # MP5 линейный массив
        self.env.newMacroPropertyUInt32("mp5_lin", MAX_SIZE)
        
        # MP4 квоты (если включены)
        if os.environ.get('HL_ENABLE_QUOTAS', '0') == '1':
            self.env.newMacroPropertyUInt("mp4_quota_mi8", MAX_DAYS)
            self.env.newMacroPropertyUInt("mp4_quota_mi17", MAX_DAYS)
            
            # Буферы менеджера квот
            self.env.newMacroPropertyUInt32("mi8_intent", MAX_FRAMES)
            self.env.newMacroPropertyUInt32("mi17_intent", MAX_FRAMES)
            self.env.newMacroPropertyUInt32("mi8_approve", MAX_FRAMES)
            self.env.newMacroPropertyUInt32("mi17_approve", MAX_FRAMES)
    
    def _setup_property_arrays(self, env_data: Dict[str, object]):
        """Настройка PropertyArray для небольших массивов"""
        # MP4 массивы квот - всегда создаем полного размера
        mp4_ops8 = list(env_data.get('mp4_ops_counter_mi8', []))
        mp4_ops17 = list(env_data.get('mp4_ops_counter_mi17', []))
        
        # Дополняем нулями до MAX_DAYS
        mp4_ops8 = (mp4_ops8 + [0] * MAX_DAYS)[:MAX_DAYS]
        mp4_ops17 = (mp4_ops17 + [0] * MAX_DAYS)[:MAX_DAYS]
        
        self.env.newPropertyArrayUInt32("mp4_ops_counter_mi8", mp4_ops8)
        self.env.newPropertyArrayUInt32("mp4_ops_counter_mi17", mp4_ops17)
    
    def _setup_agent(self) -> fg.AgentDescription:
        """Создание и настройка агента HELI"""
        agent = self.model.newAgent("heli")
        
        # Идентификаторы
        agent.newVariableUInt("idx", 0)
        agent.newVariableUInt("aircraft_number", 0)
        agent.newVariableUInt("partseqno_i", 0)
        agent.newVariableUInt("group_by", 0)
        
        # Состояние
        agent.newVariableUInt("status_id", 0)
        agent.newVariableUInt("s6_started", 0)
        
        # Наработки
        agent.newVariableUInt("sne", 0)
        agent.newVariableUInt("ppr", 0)
        agent.newVariableUInt("cso", 0)
        
        # Нормативы
        agent.newVariableUInt("ll", 0)
        agent.newVariableUInt("oh", 0)
        agent.newVariableUInt("br", 0)
        
        # Временные характеристики
        agent.newVariableUInt("repair_time", 180)
        agent.newVariableUInt("assembly_time", 180)
        agent.newVariableUInt("repair_days", 0)
        
        # MP5 данные (текущий/следующий день)
        agent.newVariableUInt("daily_today_u32", 0)
        agent.newVariableUInt("daily_next_u32", 0)
        
        # Квоты (если включены)
        if os.environ.get('HL_ENABLE_QUOTAS', '0') == '1':
            agent.newVariableUInt("ops_ticket", 0)
        
        return agent
    
    def add_rtc_module(self, module_name: str):
        """Динамически подключает RTC модуль по имени"""
        try:
            # Импортируем модуль
            module = __import__(f'rtc_{module_name}', fromlist=['register_rtc'])
            
            # Регистрируем RTC функции и слои
            if hasattr(module, 'register_rtc'):
                module.register_rtc(self.model, self.agent)
            else:
                raise ValueError(f"Модуль rtc_{module_name} не содержит функцию register_rtc")
                
        except ImportError as e:
            raise RuntimeError(f"Не удалось загрузить RTC модуль '{module_name}': {e}")
