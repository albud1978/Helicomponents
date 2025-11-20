#!/usr/bin/env python3
"""
V2 Base Model: базовая модель и окружение для всех RTC модулей
Создает пустую модель с правильными Environment properties и Agent variables
"""
import os
import sys
from typing import Dict, Optional

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import model_build
from model_build import set_max_frames_from_data

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
        self.env_data: Optional[Dict[str, object]] = None  # Для модулей типа spawn
    
    def create_model(self, env_data: Dict[str, object]) -> fg.ModelDescription:
        """Создает базовую модель с окружением из env_data"""
        
        # Сохраняем env_data для модулей
        self.env_data = env_data
        
        # Устанавливаем model_build.MAX_FRAMES из данных
        frames_from_data = int(env_data['frames_total_u16'])
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
        # Идентификатор версии симуляции
        self.env.newPropertyUInt("version_id", int(env_data.get('version_id_u32', 0)))
        self.env.newPropertyUInt("frames_total", int(env_data['frames_total_u16']))
        self.env.newPropertyUInt("days_total", int(env_data['days_total_u16']))
        
        # Фазовый флаг для постпроцессинга MP2
        # 0 = обычная симуляция, 2 = постпроцессинг active_trigger
        self.env.newPropertyUInt("export_phase", 0)
        
        # Константы нормативов из MP1
        # БЕЗ FALLBACK! Если ключа нет → ошибка
        if 'mi8_repair_time_const' not in env_data:
            raise KeyError("❌ 'mi8_repair_time_const' отсутствует в env_data! Проверьте sim_env_setup.py")
        if 'mi8_assembly_time_const' not in env_data:
            raise KeyError("❌ 'mi8_assembly_time_const' отсутствует в env_data! Проверьте sim_env_setup.py")
        if 'mi8_partout_time_const' not in env_data:
            raise KeyError("❌ 'mi8_partout_time_const' отсутствует в env_data! Проверьте sim_env_setup.py")
        
        self.env.newPropertyUInt("mi8_repair_time_const", int(env_data['mi8_repair_time_const']))
        self.env.newPropertyUInt("mi8_assembly_time_const", int(env_data['mi8_assembly_time_const']))
        self.env.newPropertyUInt("mi8_partout_time_const", int(env_data['mi8_partout_time_const']))
        
        # Mi-17 константы БЕЗ FALLBACK
        if 'mi17_repair_time_const' not in env_data:
            raise KeyError("❌ 'mi17_repair_time_const' отсутствует в env_data! Проверьте sim_env_setup.py")
        if 'mi17_assembly_time_const' not in env_data:
            raise KeyError("❌ 'mi17_assembly_time_const' отсутствует в env_data! Проверьте sim_env_setup.py")
        if 'mi17_partout_time_const' not in env_data:
            raise KeyError("❌ 'mi17_partout_time_const' отсутствует в env_data! Проверьте sim_env_setup.py")
        
        self.env.newPropertyUInt("mi17_repair_time_const", int(env_data['mi17_repair_time_const']))
        self.env.newPropertyUInt("mi17_assembly_time_const", int(env_data['mi17_assembly_time_const']))
        self.env.newPropertyUInt("mi17_partout_time_const", int(env_data['mi17_partout_time_const']))
        
        # Инициализация констант для группы 17 из MP1 (ВСЕГДА, для spawn!)
        # Читаем partseqno из env_data (единая точка определения)
        spawn_partseqno_mi17 = env_data.get('spawn_partseqno_mi17')
        if spawn_partseqno_mi17 is None:
            raise KeyError("❌ 'spawn_partseqno_mi17' отсутствует в env_data!")
        
        mp1_index = env_data.get('mp1_index', {})
        pidx_mi17 = mp1_index.get(spawn_partseqno_mi17, -1)
        
        if pidx_mi17 < 0:
            raise RuntimeError(f"partseqno_i={spawn_partseqno_mi17} (Mi-17) НЕ найден в mp1_index! Проверьте Extract/MP1.")
        
        arr_ll17 = env_data.get('mp1_ll_mi17', [])
        arr_oh17 = env_data.get('mp1_oh_mi17', [])
        arr_br17 = env_data.get('mp1_br_mi17', [])
        
        if pidx_mi17 >= len(arr_ll17) or pidx_mi17 >= len(arr_oh17) or pidx_mi17 >= len(arr_br17):
            raise RuntimeError(f"MP1 массивы недостаточной длины для pidx={pidx_mi17}")
        
        mi17_ll = int(arr_ll17[pidx_mi17])
        mi17_oh = int(arr_oh17[pidx_mi17])
        mi17_br = int(arr_br17[pidx_mi17])
        
        if mi17_ll == 0 or mi17_oh == 0 or mi17_br == 0:
            raise RuntimeError(f"Mi-17 нормативы = 0! ll={mi17_ll}, oh={mi17_oh}, br={mi17_br}. Проверьте MP1 данные.")
        
        self.env.newPropertyUInt("mi17_ll_const", mi17_ll)
        self.env.newPropertyUInt("mi17_oh_const", mi17_oh)
        self.env.newPropertyUInt("mi17_br_const", mi17_br)
        
        # sne_new и ppr_new для spawn (начальная наработка новых агентов)
        # SENTINEL = 0xFFFFFFFF (4294967295) означает NULL (агрегат не выпускается)
        arr_sne_new = env_data.get('mp1_sne_new', [])
        arr_ppr_new = env_data.get('mp1_ppr_new', [])
        
        mi17_sne_new = 0  # По умолчанию 0 (новый агент)
        mi17_ppr_new = 0
        
        if pidx_mi17 < len(arr_sne_new) and pidx_mi17 < len(arr_ppr_new):
            mi17_sne_new = int(arr_sne_new[pidx_mi17])
            mi17_ppr_new = int(arr_ppr_new[pidx_mi17])
            
            # Если sentinel (NULL), используем 0
            SENTINEL = 4294967295
            if mi17_sne_new == SENTINEL:
                mi17_sne_new = 0
            if mi17_ppr_new == SENTINEL:
                mi17_ppr_new = 0
        
        self.env.newPropertyUInt("mi17_sne_new_const", mi17_sne_new)
        self.env.newPropertyUInt("mi17_ppr_new_const", mi17_ppr_new)
        
        # Partseqno и group_by для spawn (читаем из env_data)
        self.env.newPropertyUInt("spawn_partseqno_mi17", spawn_partseqno_mi17)
        self.env.newPropertyUInt("spawn_group_by_mi17", int(env_data.get('spawn_group_by_mi17', 2)))
    
    def _setup_macro_properties(self):
        """Настройка MacroProperty для больших данных"""
        # MP5 линейный массив (используем динамическое значение)
        max_size = model_build.MAX_FRAMES * (model_build.MAX_DAYS + 1)
        self.env.newMacroPropertyUInt32("mp5_lin", max_size)
        
        # Маска для квотного демоута (0: не кандидат, 1: кандидат, 2: выбран на демоут)
        self.env.newMacroPropertyUInt32("quota_ops_mask", model_build.MAX_FRAMES)
        # Скаляр для передачи решения менеджера (количество демоутов)
        self.env.newMacroPropertyUInt32("quota_decision", 1)
        
        # Временные маски для квотирования (используются модулями quota_*)
        # Подсчёт агентов в operations и serviceable (для расчёта balance и rank)
        self.env.newMacroPropertyUInt32("mi8_ops_count", model_build.MAX_FRAMES)
        self.env.newMacroPropertyUInt32("mi17_ops_count", model_build.MAX_FRAMES)
        self.env.newMacroPropertyUInt32("mi8_svc_count", model_build.MAX_FRAMES)
        self.env.newMacroPropertyUInt32("mi17_svc_count", model_build.MAX_FRAMES)
        self.env.newMacroPropertyUInt32("mi8_reserve_count", model_build.MAX_FRAMES)
        self.env.newMacroPropertyUInt32("mi17_reserve_count", model_build.MAX_FRAMES)
        self.env.newMacroPropertyUInt32("mi8_inactive_count", model_build.MAX_FRAMES)
        self.env.newMacroPropertyUInt32("mi17_inactive_count", model_build.MAX_FRAMES)
        
        # Демоут (operations → serviceable)
        self.env.newMacroPropertyUInt32("mi8_approve", model_build.MAX_FRAMES)
        self.env.newMacroPropertyUInt32("mi17_approve", model_build.MAX_FRAMES)
        
        # Промоут (раздельные буферы для каждого приоритета)
        self.env.newMacroPropertyUInt32("mi8_approve_s3", model_build.MAX_FRAMES)   # serviceable → operations
        self.env.newMacroPropertyUInt32("mi17_approve_s3", model_build.MAX_FRAMES)
        self.env.newMacroPropertyUInt32("mi8_approve_s5", model_build.MAX_FRAMES)   # reserve → operations
        self.env.newMacroPropertyUInt32("mi17_approve_s5", model_build.MAX_FRAMES)
        self.env.newMacroPropertyUInt32("mi8_approve_s1", model_build.MAX_FRAMES)   # inactive → operations
        self.env.newMacroPropertyUInt32("mi17_approve_s1", model_build.MAX_FRAMES)
        
        # Динамический spawn (pending агенты, которые ещё не появились в operations)
        self.env.newMacroPropertyUInt32("mi8_spawn_pending", model_build.MAX_FRAMES)
        self.env.newMacroPropertyUInt32("mi17_spawn_pending", model_build.MAX_FRAMES)
        
        # Квотирование ремонтов (repair quota)
        self.env.newMacroPropertyUInt32("repair_state_buffer", model_build.MAX_FRAMES)  # Агенты в repair
        self.env.newMacroPropertyUInt32("reserve_queue_buffer", model_build.MAX_FRAMES)  # reserve & intent=0
        self.env.newMacroPropertyUInt32("ops_repair_buffer", model_build.MAX_FRAMES)  # operations & intent=4
        self.env.newMacroPropertyUInt8("repair_number_by_idx", model_build.MAX_FRAMES)  # repair_number для каждого idx

        # MP4 квоты (если включены)
        if os.environ.get('HL_ENABLE_QUOTAS', '0') == '1':
            self.env.newMacroPropertyUInt("mp4_quota_mi8", model_build.MAX_DAYS)
            self.env.newMacroPropertyUInt("mp4_quota_mi17", model_build.MAX_DAYS)
            
            # Буферы менеджера квот
            self.env.newMacroPropertyUInt32("mi8_intent", model_build.MAX_FRAMES)
            self.env.newMacroPropertyUInt32("mi17_intent", model_build.MAX_FRAMES)
    
    def _setup_property_arrays(self, env_data: Dict[str, object]):
        """Настройка PropertyArray для небольших массивов"""
        # MP4 массивы квот - всегда создаем полного размера
        mp4_ops8 = list(env_data.get('mp4_ops_counter_mi8', []))
        mp4_ops17 = list(env_data.get('mp4_ops_counter_mi17', []))
        
        # Дополняем нулями до model_build.MAX_DAYS
        mp4_ops8 = (mp4_ops8 + [0] * model_build.MAX_DAYS)[:model_build.MAX_DAYS]
        mp4_ops17 = (mp4_ops17 + [0] * model_build.MAX_DAYS)[:model_build.MAX_DAYS]
        
        self.env.newPropertyArrayUInt32("mp4_ops_counter_mi8", mp4_ops8)
        self.env.newPropertyArrayUInt32("mp4_ops_counter_mi17", mp4_ops17)
        
        # MP3 даты производства для приоритизации квот
        mp3_mfg = list(env_data.get('mp3_arrays', {}).get('mp3_mfg_date_days', []))
        if not mp3_mfg:
            mp3_mfg = [0] * model_build.MAX_FRAMES
        mp3_mfg = (mp3_mfg + [0] * model_build.MAX_FRAMES)[:model_build.MAX_FRAMES]
        self.env.newPropertyArrayUInt32("mp3_mfg_date_days", mp3_mfg)
        
        # MP4 new_counter для spawn (дата берём из version_date+day, НЕ из month_first)
        mp4_new = list(env_data.get('mp4_new_counter_mi17_seed', []))
        if not mp4_new:
            mp4_new = [0] * model_build.MAX_DAYS
        mp4_new = (mp4_new + [0] * model_build.MAX_DAYS)[:model_build.MAX_DAYS]
        self.env.newPropertyArrayUInt32("mp4_new_counter_mi17_seed", mp4_new)
        
        # MP1 sne_new и ppr_new для spawn агрегатов
        # SENTINEL = 0xFFFFFFFF (4294967295) означает NULL (агрегат не выпускается)
        mp1_sne_new = list(env_data.get('mp1_sne_new', []))
        mp1_ppr_new = list(env_data.get('mp1_ppr_new', []))
        mp1_repair_number = list(env_data.get('mp1_repair_number', []))
        mp1_index = list(env_data.get('mp1_index', {}).values()) if isinstance(env_data.get('mp1_index', {}), dict) else list(env_data.get('mp1_index', []))
        
        # Записываем размер MP1 для использования в RTC
        mp1_size = len(mp1_index) if mp1_index else len(mp1_repair_number) if mp1_repair_number else 0
        if mp1_size > 0:
            self.env.newPropertyUInt("mp1_size", mp1_size)
        
        if mp1_sne_new:
            self.env.newPropertyArrayUInt32("mp1_sne_new", mp1_sne_new)
        if mp1_ppr_new:
            self.env.newPropertyArrayUInt32("mp1_ppr_new", mp1_ppr_new)
        if mp1_repair_number:
            self.env.newPropertyArrayUInt8("mp1_repair_number", mp1_repair_number)
    
    def _setup_agent(self) -> fg.AgentDescription:
        """Создание и настройка агента HELI"""
        agent = self.model.newAgent("heli")
        
        # Создаем состояния агентов
        agent.newState("inactive")      # state_1
        agent.newState("operations")    # state_2
        agent.newState("serviceable")   # state_3
        agent.newState("repair")        # state_4
        agent.newState("reserve")       # state_5
        agent.newState("storage")       # state_6
        
        # Идентификаторы
        agent.newVariableUInt("idx", 0)
        agent.newVariableUInt("aircraft_number", 0)
        agent.newVariableUInt("partseqno_i", 0)
        agent.newVariableUInt("group_by", 0)
        
        # Состояние (только для state_6)
        
        # Служебное поле для BI (всегда = 1)
        agent.newVariableUInt("bi_counter", 1)
        
        # Новая переменная для intent-based архитектуры
        agent.newVariableUInt("intent_state", 0)
        
        # Флаги переходов между состояниями (устанавливаются compute_transitions перед state_managers)
        agent.newVariableUInt("transition_0_to_2", 0)   # spawn → operations (динамический)
        agent.newVariableUInt("transition_0_to_3", 0)   # spawn → serviceable (детерминированный)
        agent.newVariableUInt("transition_2_to_4", 0)   # operations → repair
        agent.newVariableUInt("transition_2_to_6", 0)   # operations → storage
        agent.newVariableUInt("transition_2_to_3", 0)   # operations → serviceable
        agent.newVariableUInt("transition_3_to_2", 0)   # serviceable → operations
        agent.newVariableUInt("transition_5_to_2", 0)   # reserve → operations
        agent.newVariableUInt("transition_1_to_2", 0)   # inactive → operations
        agent.newVariableUInt("transition_4_to_5", 0)   # repair → reserve
        agent.newVariableUInt("transition_1_to_4", 0)   # inactive → repair
        agent.newVariableUInt("transition_4_to_2", 0)   # repair → operations
        
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
        agent.newVariableUInt("partout_time", 180)
        agent.newVariableUInt("repair_days", 0)
        agent.newVariableUInt("assembly_trigger", 0)
        agent.newVariableUInt("active_trigger", 0)
        agent.newVariableUInt("partout_trigger", 0)
        agent.newVariableUInt("mfg_date", 0)  # Дата производства для приоритизации
        
        # MP5 данные (текущий/следующий день)
        agent.newVariableUInt("daily_today_u32", 0)
        agent.newVariableUInt("daily_next_u32", 0)
        
        # Счётчик дней в repair + reserve (накопительный)
        # s4_days: начинается с 1 при переходе 2→4, инкрементируется в состояниях 4 и 5, сбрасывается при 2→2
        agent.newVariableUInt("s4_days", 0)
        
        # ops_ticket был зарезервирован, но никогда не использовался
        # agent.newVariableUInt("ops_ticket", 0)  # УДАЛЕНО
        
        return agent
    
    def add_rtc_module(self, module_name: str):
        """Динамически подключает RTC модуль по имени"""
        try:
            # Специальная обработка для некоторых модулей
            if module_name == "mp5_probe":
                # Регистрируем RTC функцию чтения MP5
                import rtc_mp5_probe
                rtc_mp5_probe.register_rtc(self.model, self.agent)
                
            elif module_name == "count_ops":
                import rtc_quota_count_ops
                rtc_quota_count_ops.register_rtc(self.model, self.agent)
                
            elif module_name == "state_2_operations":
                import rtc_state_2_operations
                rtc_state_2_operations.register_rtc(self.model, self.agent)
                
            elif module_name == "states_stub":
                import rtc_states_stub
                rtc_states_stub.register_rtc(self.model, self.agent)
                
            elif module_name == "state_manager":
                import rtc_state_manager
                layer = self.model.newLayer()
                rtc_state_manager.register_state_manager_simple(self.model, self.agent, layer)
                
            elif module_name == "state_manager_v2":
                import rtc_state_manager_v2
                rtc_state_manager_v2.register_state_manager_v2(self.model, self.agent)
                
            elif module_name == "state_manager_intermediate":
                import rtc_state_manager_intermediate
                rtc_state_manager_intermediate.register_state_manager_intermediate(self.model, self.agent)
                
            elif module_name == "state_manager_conditional":
                import rtc_state_manager_conditional
                rtc_state_manager_conditional.register_state_manager_conditional(self.model, self.agent)
                
            elif module_name == "state_manager_test":
                import rtc_state_manager_test
                rtc_state_manager_test.register_state_manager_test(self.model, self.agent)
                
            elif module_name == "state_manager_full":
                import rtc_state_manager_full
                rtc_state_manager_full.register_state_manager_full(self.model, self.agent)
                
            elif module_name == "state_manager_serviceable":
                import rtc_state_manager_serviceable
                rtc_state_manager_serviceable.register_rtc(self.model, self.agent)
                
            elif module_name == "state_manager_operations":
                import rtc_state_manager_operations
                rtc_state_manager_operations.register_state_manager_operations(self.model, self.agent)
                
            elif module_name == "state_manager_repair":
                import rtc_state_manager_repair
                rtc_state_manager_repair.register_state_manager_repair(self.model, self.agent)
            
            elif module_name == "state_manager_inactive":
                import rtc_state_manager_inactive
                rtc_state_manager_inactive.register_state_manager_inactive(self.model, self.agent)
            
            elif module_name == "state_manager_reserve":
                import rtc_state_manager_reserve
                rtc_state_manager_reserve.register_state_manager_reserve(self.model, self.agent)
            
            elif module_name == "state_manager_storage":
                import rtc_state_manager_storage
                rtc_state_manager_storage.register_state_manager_storage(self.model, self.agent)
                
            elif module_name == "quota_ops_excess":
                import rtc_quota_ops_excess
                rtc_quota_ops_excess.register_rtc(self.model, self.agent)
            
            elif module_name == "quota_promote_serviceable":
                import rtc_quota_promote_serviceable
                rtc_quota_promote_serviceable.register_rtc(self.model, self.agent)
            
            elif module_name == "quota_promote_reserve":
                import rtc_quota_promote_reserve
                rtc_quota_promote_reserve.register_rtc(self.model, self.agent)
            
            elif module_name == "quota_promote_inactive":
                import rtc_quota_promote_inactive
                rtc_quota_promote_inactive.register_rtc(self.model, self.agent)
                print("  RTC модуль quota_promote_inactive зарегистрирован (1 слой, приоритет 3)")
            
            elif module_name == "quota_repair":
                from rtc_modules import rtc_quota_repair
                rtc_quota_repair.register_rtc(self.model, self.agent)
            
            elif module_name == "compute_transitions":
                import rtc_compute_transitions
                rtc_compute_transitions.register_compute_transitions(self.model, self.agent)
            
            elif module_name == "spawn":
                # Спавн новых агентов (Mi-17)
                from rtc_modules import rtc_spawn_integration
                rtc_spawn_integration.register_spawn_rtc(self.model, self.agent, self.env_data)
                print("  RTC модуль spawn зарегистрирован")
            
            elif module_name == "spawn_simple":
                # Упрощённый спавн для отладки NVRTC
                from rtc_modules import rtc_spawn_simple
                rtc_spawn_simple.register_simple_spawn(self.model, self.agent)
                print("  RTC модуль spawn_simple зарегистрирован")
            
            elif module_name == "spawn_v2":
                # Адаптированный spawn для orchestrator_v2
                from rtc_modules import rtc_spawn_v2
                rtc_spawn_v2.register_rtc(self.model, self.agent, self.env_data)
                print("  RTC модуль spawn_v2 зарегистрирован")
            
            elif module_name == "spawn_dynamic":
                # Динамический spawn для покрытия дефицита после P3
                from rtc_modules import rtc_spawn_dynamic
                self.spawn_dynamic_result = rtc_spawn_dynamic.register_rtc(self.model, self.agent, self.env_data)
                print("  RTC модуль spawn_dynamic зарегистрирован (слой 7.5)")
                
            else:
                # Стандартная обработка для других модулей
                module = __import__(f'rtc_{module_name}', fromlist=['register_rtc'])
                
                # Регистрируем RTC функции и слои
                if hasattr(module, 'register_rtc'):
                    module.register_rtc(self.model, self.agent)
                else:
                    raise ValueError(f"Модуль rtc_{module_name} не содержит функцию register_rtc")
                
        except ImportError as e:
            raise RuntimeError(f"Не удалось загрузить RTC модуль '{module_name}': {e}")
