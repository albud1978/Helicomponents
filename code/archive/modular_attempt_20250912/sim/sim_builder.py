#!/usr/bin/env python3
"""
Единый сборщик FLAME GPU модели
Загружает RTC функции из отдельных файлов и собирает пайплайн
Дата: 2025-09-12
"""

from typing import Dict, List, Optional, Any
import os
import importlib

try:
    import pyflamegpu
except ImportError:
    pyflamegpu = None

from .pipeline_config import RTCPipeline, RTC_PROFILES


class SimBuilder:
    """Сборщик FLAME GPU модели с модульными RTC функциями"""
    
    def __init__(self):
        self.rtc_pipeline = RTCPipeline()
        self.rtc_registry: Dict[str, Any] = {}
        self._load_rtc_modules()
    
    def _load_rtc_modules(self):
        """Загружает все RTC модули из папки rtc/"""
        rtc_modules = [
            'begin_day', 'status_2', 'status_4', 'status_6',
            'quota_intent', 'quota_approve', 'quota_apply', 'quota_clear',
            'status_post', 'log_mp2', 'spawn_mgr', 'spawn_ticket'
        ]
        
        for module_name in rtc_modules:
            try:
                module = importlib.import_module(f'rtc.{module_name}')
                # Ищем класс RTC в модуле
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if (hasattr(attr, 'NAME') and hasattr(attr, 'get_source') and 
                        attr_name.endswith('RTC')):
                        self.rtc_registry[attr.NAME] = attr
            except ImportError as e:
                print(f"⚠️ Не удалось загрузить RTC модуль {module_name}: {e}")
    
    def build_model(self, frames_total: int, days_total: int, 
                   profile: str = "minimal", 
                   custom_flags: Dict[str, bool] = None) -> Optional["pyflamegpu.ModelDescription"]:
        """Собирает FLAME GPU модель согласно профилю"""
        
        if pyflamegpu is None:
            raise RuntimeError("pyflamegpu не установлен")
        
        FRAMES = max(1, int(frames_total))
        DAYS = max(1, int(days_total))
        
        model = pyflamegpu.ModelDescription("HeliSimV2")
        
        # 1. Настройка Environment
        self._setup_environment(model, FRAMES, DAYS)
        
        # 2. Создание агента
        agent = self._create_agent(model)
        
        # 3. Регистрация RTC функций
        enabled_functions = self.rtc_pipeline.get_enabled_functions(profile, custom_flags)
        self._register_rtc_functions(agent, enabled_functions, FRAMES, DAYS)
        
        # 4. Сборка слоев
        self._build_layers(model, agent, enabled_functions)
        
        return model
    
    def _setup_environment(self, model: "pyflamegpu.ModelDescription", frames: int, days: int):
        """Настройка Environment Properties"""
        env = model.Environment()
        
        # Базовые скаляры
        env.newPropertyUInt("version_date", 0)
        env.newPropertyUInt("frames_total", 0)
        env.newPropertyUInt("days_total", 0)
        env.newPropertyUInt("frames_initial", 0)
        
        # Управление фазами экспорта
        env.newPropertyUInt("export_phase", 0)  # 0=sim, 1=export, 2=log_only
        env.newPropertyUInt("export_day", 0)
        
        # MP4 квоты по дням
        env.newPropertyArrayUInt32("mp4_ops_counter_mi8", [0] * days)
        env.newPropertyArrayUInt32("mp4_ops_counter_mi17", [0] * days)
        env.newPropertyArrayUInt32("mp4_new_counter_mi17_seed", [0] * days)
        env.newPropertyArrayUInt32("month_first_u32", [0] * days)
        
        # MP5 налеты (с паддингом D+1)
        env.newPropertyArrayUInt16("mp5_daily_hours", [0] * ((days + 1) * frames))
        
        # MP1 справочники (SoA)
        env.newPropertyArrayUInt32("mp1_br_mi8", [0])
        env.newPropertyArrayUInt32("mp1_br_mi17", [0])
        env.newPropertyArrayUInt32("mp1_repair_time", [0])
        env.newPropertyArrayUInt32("mp1_partout_time", [0])
        env.newPropertyArrayUInt32("mp1_assembly_time", [0])
        env.newPropertyArrayUInt32("mp1_oh_mi8", [0])
        env.newPropertyArrayUInt32("mp1_oh_mi17", [0])
        
        # MP3 агентные данные (SoA)
        env.newPropertyArrayUInt32("mp3_psn", [0])
        env.newPropertyArrayUInt32("mp3_aircraft_number", [0])
        env.newPropertyArrayUInt32("mp3_ac_type_mask", [0])
        env.newPropertyArrayUInt32("mp3_status_id", [0])
        env.newPropertyArrayUInt32("mp3_sne", [0])
        env.newPropertyArrayUInt32("mp3_ppr", [0])
        env.newPropertyArrayUInt32("mp3_repair_days", [0])
        env.newPropertyArrayUInt32("mp3_ll", [0])
        env.newPropertyArrayUInt32("mp3_oh", [0])
        env.newPropertyArrayUInt32("mp3_mfg_date_days", [0])
        
        # Quota буферы (MacroProperty по кадрам)
        env.newMacroPropertyUInt32("mi8_intent", frames)
        env.newMacroPropertyUInt32("mi17_intent", frames)
        env.newMacroPropertyUInt32("mi8_approve", frames)
        env.newMacroPropertyUInt32("mi17_approve", frames)
        env.newMacroPropertyUInt32("mi8_approve_s3", frames)
        env.newMacroPropertyUInt32("mi17_approve_s3", frames)
        env.newMacroPropertyUInt32("mi8_approve_s5", frames)
        env.newMacroPropertyUInt32("mi17_approve_s5", frames)
        env.newMacroPropertyUInt32("mi8_approve_s1", frames)
        env.newMacroPropertyUInt32("mi17_approve_s1", frames)
        
        # MP2 логирование (SoA с паддингом)
        mp2_size = frames * (days + 1)
        env.newMacroPropertyUInt32("mp2_status", mp2_size)
        env.newMacroPropertyUInt32("mp2_repair_days", mp2_size)
        env.newMacroPropertyUInt32("mp2_sne", mp2_size)
        env.newMacroPropertyUInt32("mp2_ppr", mp2_size)
        env.newMacroPropertyUInt32("mp2_active_trigger", mp2_size)
        env.newMacroPropertyUInt32("mp2_assembly_trigger", mp2_size)
        env.newMacroPropertyUInt32("mp2_partout_trigger", mp2_size)
        env.newMacroPropertyUInt32("mp2_ops_ticket", mp2_size)
        env.newMacroPropertyUInt32("mp2_intent_flag", mp2_size)
        env.newMacroPropertyUInt32("mp2_daily_today", mp2_size)
        env.newMacroPropertyUInt32("mp2_daily_next", mp2_size)
        
        # Spawn счетчики (MacroProperty скаляры)
        env.newMacroPropertyUInt32("spawn_need_u32", 1)
        env.newMacroPropertyUInt32("spawn_base_idx_u32", 1)
        env.newMacroPropertyUInt32("spawn_base_acn_u32", 1)
        env.newMacroPropertyUInt32("spawn_base_psn_u32", 1)
        env.newMacroPropertyUInt32("next_idx_spawn", 1)
        env.newMacroPropertyUInt32("next_aircraft_no_mi17", 1)
        env.newMacroPropertyUInt32("next_psn_mi17", 1)
    
    def _create_agent(self, model: "pyflamegpu.ModelDescription") -> "pyflamegpu.AgentDescription":
        """Создает агента component с полным набором переменных"""
        agent = model.newAgent("component")
        
        # Базовые переменные агента
        agent_variables = [
            "idx", "psn", "partseqno_i", "group_by", "aircraft_number", "ac_type_mask",
            "mfg_date", "status_id", "repair_days", "repair_time", "assembly_time", 
            "partout_time", "sne", "ppr", "ll", "oh", "br",
            "daily_today_u32", "daily_next_u32", "ops_ticket", "intent_flag",
            "active_trigger", "assembly_trigger", "partout_trigger",
            "active_trigger_mark", "assembly_trigger_mark", "partout_trigger_mark"
        ]
        
        for var_name in agent_variables:
            agent.newVariableUInt(var_name, 0)
        
        return agent
    
    def _register_rtc_functions(self, agent: "pyflamegpu.AgentDescription", 
                               enabled_functions: List[Dict], frames: int, days: int):
        """Регистрирует RTC функции в агенте"""
        
        for func_config in enabled_functions:
            func_name = func_config["name"]
            
            # Ищем RTC класс в реестре
            rtc_class = self._find_rtc_class(func_name)
            if rtc_class is None:
                print(f"⚠️ RTC функция {func_name} не найдена в реестре")
                continue
            
            try:
                # Получаем исходный код функции
                kwargs = self._get_function_kwargs(func_name)
                source = rtc_class.get_source(frames, days, **kwargs)
                
                # Регистрируем в агенте
                agent.newRTCFunction(func_name, source)
                
            except Exception as e:
                print(f"❌ Ошибка регистрации RTC {func_name}: {e}")
                # Выводим исходный код для отладки
                try:
                    kwargs = self._get_function_kwargs(func_name)
                    source = rtc_class.get_source(frames, days, **kwargs)
                    print(f"\n===== NVRTC ERROR in {func_name} =====")
                    print(f"{e}")
                    print(f"----- SOURCE BEGIN -----")
                    print(source)
                    print(f"----- SOURCE END -----\n")
                except:
                    pass
                raise
    
    def _find_rtc_class(self, func_name: str) -> Optional[Any]:
        """Находит RTC класс по имени функции"""
        
        # Прямое соответствие
        if func_name in self.rtc_registry:
            return self.rtc_registry[func_name]
        
        # Алиасы для обратной совместимости
        if func_name == "rtc_quota_begin_day" or func_name == "rtc_probe_mp5":
            return self.rtc_registry.get("rtc_prepare_day")
        
        # Поиск по паттернам для параметризованных функций
        if func_name.startswith("rtc_quota_intent_s"):
            return self.rtc_registry.get("rtc_quota_intent")
        elif func_name.startswith("rtc_quota_approve_s"):
            return self.rtc_registry.get("rtc_quota_approve")
        elif func_name.startswith("rtc_quota_apply_s"):
            return self.rtc_registry.get("rtc_quota_apply")
        elif func_name.startswith("rtc_quota_clear_s"):
            return self.rtc_registry.get("rtc_quota_clear")
        elif func_name.startswith("rtc_status_") and func_name.endswith("_post"):
            return self.rtc_registry.get("rtc_status_post")
        
        return None
    
    def _get_function_kwargs(self, func_name: str) -> Dict[str, any]:
        """Возвращает kwargs для параметризованных функций"""
        kwargs = {}
        
        # Извлекаем status_id для quota функций
        if "_s" in func_name and func_name.startswith("rtc_quota_"):
            try:
                status_id = int(func_name.split("_s")[-1])
                kwargs["status_id"] = status_id
            except:
                pass
        
        # Извлекаем status_id для post функций
        if func_name.startswith("rtc_status_") and func_name.endswith("_post"):
            try:
                parts = func_name.split("_")
                if len(parts) >= 3:
                    status_id = int(parts[2])
                    kwargs["status_id"] = status_id
            except:
                pass
        
        # Специальные типы для log функций
        if func_name == "rtc_log_d0":
            kwargs["func_type"] = "log_d0"
        elif func_name == "rtc_mp2_copyout":
            kwargs["func_type"] = "mp2_copyout"
        
        return kwargs
    
    def _build_layers(self, model: "pyflamegpu.ModelDescription", 
                     agent: "pyflamegpu.AgentDescription", 
                     enabled_functions: List[Dict]):
        """Создает слои модели в правильном порядке"""
        
        # Сортируем по номеру слоя
        sorted_functions = sorted(enabled_functions, key=lambda x: x["layer"])
        
        for func_config in sorted_functions:
            func_name = func_config["name"]
            layer_num = func_config["layer"]
            
            try:
                # Создаем слой и добавляем функцию
                layer = model.newLayer()
                rtc_function = agent.getFunction(func_name)
                layer.addAgentFunction(rtc_function)
                
                print(f"✅ Слой {layer_num:2d}: {func_name}")
                
            except Exception as e:
                print(f"❌ Ошибка создания слоя {layer_num} для {func_name}: {e}")
                raise
    
    def create_simulation(self, model: "pyflamegpu.ModelDescription") -> "pyflamegpu.CUDASimulation":
        """Создает симуляцию из модели"""
        if pyflamegpu is None:
            raise RuntimeError("pyflamegpu не установлен")
        
        return pyflamegpu.CUDASimulation(model)
    
    def get_pipeline_info(self, profile: str = "minimal") -> str:
        """Возвращает информацию о пайплайне"""
        return self.rtc_pipeline.get_pipeline_summary(profile)
