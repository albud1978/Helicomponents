#!/usr/bin/env python3
"""
V2 Base Model с Messaging: расширенная модель с поддержкой сообщений и QuotaManager

Отличия от base_model.py:
1. Добавлен тип агента QuotaManager (2 агента: Mi-8, Mi-17)
2. Добавлены типы сообщений: PlanerReport, QuotaDecision
3. MacroProperty буферы для хранения данных в QuotaManager (масштабирование >1000)
"""
import os
import sys
from typing import Dict, Optional

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
import model_build
from model_build import set_max_frames_from_data

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


# Максимум ремонтных линий для адресных сообщений RepairLine
REPAIR_LINES_MAX = 64


class V2BaseModelMessaging:
    """Базовая модель V2 с Messaging архитектурой"""
    
    def __init__(self):
        self.model: Optional[fg.ModelDescription] = None
        self.agent: Optional[fg.AgentDescription] = None  # HELI (планеры)
        self.quota_agent: Optional[fg.AgentDescription] = None  # QuotaManager
        self.repair_line_agent: Optional[fg.AgentDescription] = None  # RepairLine (ремонтные линии)
        self.env: Optional[fg.EnvironmentDescription] = None
        self.env_data: Optional[Dict[str, object]] = None
        
        # Messages
        self.msg_planer_report: Optional[fg.MessageDescription] = None
        self.msg_quota_decision: Optional[fg.MessageDescription] = None
        self.msg_repair_line_status: Optional[fg.MessageDescription] = None
    
    def create_model(self, env_data: Dict[str, object]) -> fg.ModelDescription:
        """Создает модель с messaging архитектурой"""
        
        self.env_data = env_data
        
        # Устанавливаем MAX_FRAMES из данных
        frames_from_data = int(env_data['frames_total_u16'])
        set_max_frames_from_data(frames_from_data)
        
        self.model = fg.ModelDescription("HeliSimV2_Messaging")
        self.env = self.model.Environment()
        
        # Скалярные свойства окружения (как в base_model.py)
        self._setup_scalar_properties(env_data)
        
        # MacroProperty для данных
        self._setup_macro_properties()
        
        # PropertyArray для квот и планов
        self._setup_property_arrays(env_data)
        
        # ═══════════════════════════════════════════════════════════════
        # MESSAGES: Типы сообщений для messaging архитектуры
        # ═══════════════════════════════════════════════════════════════
        self._setup_messages()
        
        # ═══════════════════════════════════════════════════════════════
        # AGENTS: HELI (планеры) + QuotaManager
        # ═══════════════════════════════════════════════════════════════
        self.agent = self._setup_agent()
        self.quota_agent = self._setup_quota_agent()
        self.repair_line_agent = self._setup_repair_line_agent()
        
        return self.model
    
    def _setup_messages(self):
        """Настройка типов сообщений"""
        
        # ═══════════════════════════════════════════════════════════════
        # Message "PlanerReport": Планер → QuotaManager (POLLING - deprecated)
        # ═══════════════════════════════════════════════════════════════
        self.msg_planer_report = self.model.newMessageBruteForce("PlanerReport")
        self.msg_planer_report.newVariableUInt16("idx")
        self.msg_planer_report.newVariableUInt8("group_by")      # 1=Mi-8, 2=Mi-17
        self.msg_planer_report.newVariableUInt8("state")         # 1-6 (текущее состояние)
        self.msg_planer_report.newVariableUInt8("intent")        # Текущий intent после state_functions
        self.msg_planer_report.newVariableUInt16("mfg_date")     # Для ранжирования
        self.msg_planer_report.newVariableUInt8("repair_ready")  # 1 если step_day >= repair_time
        self.msg_planer_report.newVariableUInt8("skip_repair")   # 1 если Mi-17 && ppr < br2
        
        # ═══════════════════════════════════════════════════════════════
        # Message "PlanerEvent": EVENT-DRIVEN (только при изменениях!)
        # ═══════════════════════════════════════════════════════════════
        # event_type: 0=none, 1=DEMOUNT (выход из ops), 2=READY (готов к промоуту)
        self.msg_planer_event = self.model.newMessageBruteForce("PlanerEvent")
        self.msg_planer_event.newVariableUInt16("idx")
        self.msg_planer_event.newVariableUInt8("group_by")       # 1=Mi-8, 2=Mi-17
        self.msg_planer_event.newVariableUInt8("state")          # текущее состояние
        self.msg_planer_event.newVariableUInt8("intent")         # целевое состояние
        self.msg_planer_event.newVariableUInt16("mfg_date")      # для ранжирования
        self.msg_planer_event.newVariableUInt8("event_type")     # 1=DEMOUNT, 2=READY
        self.msg_planer_event.newVariableUInt8("repair_ready")
        self.msg_planer_event.newVariableUInt8("skip_repair")
        
        # ═══════════════════════════════════════════════════════════════
        # Message "QuotaDecision": QuotaManager → Планер
        # ═══════════════════════════════════════════════════════════════
        self.msg_quota_decision = self.model.newMessageBruteForce("QuotaDecision")
        self.msg_quota_decision.newVariableUInt16("idx")         # Кому адресовано
        self.msg_quota_decision.newVariableUInt8("action")       # 0=none, 1=demote→3, 2=promote→2
        self.msg_quota_decision.newVariableUInt8("group_by")     # Для фильтрации

        # ═══════════════════════════════════════════════════════════════
        # Message "RepairLineStatus": RepairLine → QuotaManager (addressed)
        # ═══════════════════════════════════════════════════════════════
        self.msg_repair_line_status = self.model.newMessageArray("RepairLineStatus")
        self.msg_repair_line_status.setLength(REPAIR_LINES_MAX)
        self.msg_repair_line_status.newVariableUInt("free_days")
        self.msg_repair_line_status.newVariableUInt("aircraft_number")
        
        print("  ✅ Messages: PlanerReport, PlanerEvent, QuotaDecision, RepairLineStatus")
    
    def _setup_quota_agent(self) -> fg.AgentDescription:
        """Настройка агента QuotaManager"""
        
        max_frames = model_build.RTC_MAX_FRAMES
        
        quota = self.model.newAgent("QuotaManager")
        
        # Переменные QuotaManager
        quota.newVariableUInt8("group_by")           # 1=Mi-8, 2=Mi-17
        quota.newVariableUInt("target")              # Целевое количество в operations (UInt32 для RTC)
        quota.newVariableUInt("current")             # Текущее количество в operations (UInt32 для RTC)
        quota.newVariableInt("balance")              # current - target (+ = excess, - = deficit)
        quota.newVariableUInt("remaining_deficit")   # Остаток после P1+P2+P3
        
        # MacroProperty буферы для хранения idx агентов (для масштабирования >1000)
        # Каждый QuotaManager хранит idx своей группы
        # ВАЖНО: Используем UInt32 т.к. atomic exchange поддерживает только 32/64-bit типы
        self.env.newMacroPropertyUInt32("qm_ops_idx", max_frames)       # idx агентов в operations
        self.env.newMacroPropertyUInt32("qm_svc_idx", max_frames)       # idx агентов в serviceable
        self.env.newMacroPropertyUInt32("qm_rsv_idx", max_frames)       # idx агентов в reserve
        self.env.newMacroPropertyUInt32("qm_ina_idx", max_frames)       # idx агентов в inactive (готовых)
        
        # Счётчики заполнения буферов (по группам)
        self.env.newMacroPropertyUInt32("qm_ops_count", 2)   # [0]=Mi-8, [1]=Mi-17
        self.env.newMacroPropertyUInt32("qm_svc_count", 2)
        self.env.newMacroPropertyUInt32("qm_rsv_count", 2)
        self.env.newMacroPropertyUInt32("qm_ina_count", 2)
        
        # MacroProperty для решений (idx → action)
        # Используем для хранения решений перед публикацией
        # ВАЖНО: Используем UInt32 т.к. atomic exchange поддерживает только 32/64-bit типы
        self.env.newMacroPropertyUInt32("qm_decisions", max_frames)  # 0=none, 1=demote, 2=promote
        
        print(f"  ✅ Agent QuotaManager: 2 агента (Mi-8, Mi-17), MacroProperty буферы {max_frames}")
        
        return quota

    def _setup_repair_line_agent(self) -> fg.AgentDescription:
        """Агент ремонтной линии (одна линия ремонта)"""
        repair_line = self.model.newAgent("RepairLine")
        repair_line.newVariableUInt("line_id")
        repair_line.newVariableUInt("free_days")
        repair_line.newVariableUInt("aircraft_number")
        print("  ✅ Agent RepairLine: переменные line_id, free_days, aircraft_number")
        return repair_line
    
    def _setup_scalar_properties(self, env_data: Dict[str, object]):
        """Настройка скалярных свойств окружения (идентично base_model.py)"""
        self.env.newPropertyUInt("version_date", int(env_data['version_date_u16']))
        self.env.newPropertyUInt("version_id", int(env_data.get('version_id_u32', 0)))
        self.env.newPropertyUInt("frames_total", int(env_data['frames_total_u16']))
        self.env.newPropertyUInt("days_total", int(env_data['days_total_u16']))
        self.env.newPropertyUInt("export_phase", 0)
        
        # Свойства для адаптивного шага
        self.env.newPropertyUInt("current_day", 0)
        self.env.newPropertyUInt("step_days", 1)
        self.env.newPropertyUInt("quota_enabled", 1)  # По умолчанию квотирование включено
        self.env.newPropertyUInt("repair_line_mode", 0)  # 1 = RepairLine логика ремонта (V8)
        
        # Константы нормативов из MP1
        self._setup_norm_constants(env_data)
    
    def _setup_norm_constants(self, env_data: Dict[str, object]):
        """Настройка констант нормативов (идентично base_model.py)"""
        # Mi-8 константы
        required_keys = [
            'mi8_repair_time_const', 'mi8_assembly_time_const', 'mi8_partout_time_const',
            'mi17_repair_time_const', 'mi17_assembly_time_const', 'mi17_partout_time_const'
        ]
        for key in required_keys:
            if key not in env_data:
                raise KeyError(f"❌ '{key}' отсутствует в env_data!")
        
        self.env.newPropertyUInt("mi8_repair_time_const", int(env_data['mi8_repair_time_const']))
        self.env.newPropertyUInt("mi8_assembly_time_const", int(env_data['mi8_assembly_time_const']))
        self.env.newPropertyUInt("mi8_partout_time_const", int(env_data['mi8_partout_time_const']))
        self.env.newPropertyUInt("mi17_repair_time_const", int(env_data['mi17_repair_time_const']))
        self.env.newPropertyUInt("mi17_assembly_time_const", int(env_data['mi17_assembly_time_const']))
        self.env.newPropertyUInt("mi17_partout_time_const", int(env_data['mi17_partout_time_const']))
        
        # Mi-17 нормативы из MP1
        spawn_partseqno_mi17 = env_data.get('spawn_partseqno_mi17')
        if spawn_partseqno_mi17 is None:
            raise KeyError("❌ 'spawn_partseqno_mi17' отсутствует в env_data!")
        
        mp1_index = env_data.get('mp1_index', {})
        pidx_mi17 = mp1_index.get(spawn_partseqno_mi17, -1)
        
        if pidx_mi17 < 0:
            raise RuntimeError(f"partseqno_i={spawn_partseqno_mi17} НЕ найден в mp1_index!")
        
        arr_ll17 = env_data.get('mp1_ll_mi17', [])
        arr_oh17 = env_data.get('mp1_oh_mi17', [])
        arr_br17 = env_data.get('mp1_br_mi17', [])
        arr_br2_17 = env_data.get('mp1_br2_mi17', [])
        
        mi17_ll = int(arr_ll17[pidx_mi17]) if pidx_mi17 < len(arr_ll17) else 0
        mi17_oh = int(arr_oh17[pidx_mi17]) if pidx_mi17 < len(arr_oh17) else 0
        mi17_br = int(arr_br17[pidx_mi17]) if pidx_mi17 < len(arr_br17) else 0
        mi17_br2 = int(arr_br2_17[pidx_mi17]) if pidx_mi17 < len(arr_br2_17) else 0
        
        self.env.newPropertyUInt("mi17_ll_const", mi17_ll)
        self.env.newPropertyUInt("mi17_oh_const", mi17_oh)
        self.env.newPropertyUInt("mi17_br_const", mi17_br)
        self.env.newPropertyUInt("mi17_br2_const", mi17_br2)
        
        # sne_new и ppr_new для spawn (начальная наработка новых агентов)
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
        
        # Partseqno и group_by для spawn — создаются здесь (а не в rtc_spawn_v2)
        # чтобы RTC ядра spawn могли их использовать
        self.env.newPropertyUInt("spawn_partseqno_mi17", int(spawn_partseqno_mi17))
        self.env.newPropertyUInt("spawn_group_by_mi17", int(env_data.get('spawn_group_by_mi17', 2)))
    
    def _setup_macro_properties(self):
        """Настройка MacroProperty (MP5 + quota буферы для совместимости)
        
        ВАЖНО: MP2 буферы НЕ создаются здесь — они создаются в rtc_mp2_writer.py
        при вызове register_mp2_writer(), чтобы типы были консистентны (UInt32).
        """
        max_frames = model_build.RTC_MAX_FRAMES
        
        # MP5: Суточные часы работы
        self.env.newMacroPropertyUInt32("mp5_lin", model_build.MAX_SIZE)
        
        # ═══════════════════════════════════════════════════════════════════════
        # Quota буферы (для совместимости с rtc_mp2_writer.py)
        # В messaging архитектуре эти буферы НЕ используются — квотирование
        # через сообщения. Но rtc_mp2_writer читает их для логирования.
        # ═══════════════════════════════════════════════════════════════════════
        self.env.newMacroPropertyUInt32("mi8_ops_count", max_frames)
        self.env.newMacroPropertyUInt32("mi17_ops_count", max_frames)
        self.env.newMacroPropertyUInt32("mi8_svc_count", max_frames)
        self.env.newMacroPropertyUInt32("mi17_svc_count", max_frames)
        self.env.newMacroPropertyUInt32("mi8_rsv_count", max_frames)
        self.env.newMacroPropertyUInt32("mi17_rsv_count", max_frames)
        self.env.newMacroPropertyUInt32("mi8_inactive_count", max_frames)
        self.env.newMacroPropertyUInt32("mi17_inactive_count", max_frames)
        self.env.newMacroPropertyUInt32("mi8_reserve_count", max_frames)
        self.env.newMacroPropertyUInt32("mi17_reserve_count", max_frames)
        
        self.env.newMacroPropertyUInt32("mi8_approve", max_frames)
        self.env.newMacroPropertyUInt32("mi17_approve", max_frames)
        self.env.newMacroPropertyUInt32("mi8_approve_s3", max_frames)
        self.env.newMacroPropertyUInt32("mi17_approve_s3", max_frames)
        self.env.newMacroPropertyUInt32("mi8_approve_s5", max_frames)
        self.env.newMacroPropertyUInt32("mi17_approve_s5", max_frames)
        self.env.newMacroPropertyUInt32("mi8_approve_s1", max_frames)
        self.env.newMacroPropertyUInt32("mi17_approve_s1", max_frames)
        
        # V6: MacroProperty для state 7 (unserviceable)
        self.env.newMacroPropertyUInt32("mi8_unsvc_count", max_frames)
        self.env.newMacroPropertyUInt32("mi17_unsvc_count", max_frames)
        # UNSVC, готовые к промоуту (exit_date <= current_day)
        self.env.newMacroPropertyUInt32("mi8_unsvc_ready_count", max_frames)
        self.env.newMacroPropertyUInt32("mi17_unsvc_ready_count", max_frames)
        # UNSVC, ожидающие назначения RepairLine
        self.env.newMacroPropertyUInt32("mi8_unsvc_wait_count", max_frames)
        self.env.newMacroPropertyUInt32("mi17_unsvc_wait_count", max_frames)
        self.env.newMacroPropertyUInt32("mi8_approve_s7", max_frames)
        self.env.newMacroPropertyUInt32("mi17_approve_s7", max_frames)
        
        # Spawn pending для квотирования
        self.env.newMacroPropertyUInt32("mi8_spawn_pending", max_frames)
        self.env.newMacroPropertyUInt32("mi17_spawn_pending", max_frames)
        
        # Буферы для repair и reserve queue
        self.env.newMacroPropertyUInt32("repair_state_buffer", max_frames)
        self.env.newMacroPropertyUInt32("reserve_queue_buffer", max_frames)
        self.env.newMacroPropertyUInt32("ops_repair_buffer", max_frames)
        self.env.newMacroPropertyUInt32("repair_number_by_idx", max_frames)
        
        # Quota decision
        self.env.newMacroPropertyUInt32("quota_decision", 1)
        self.env.newMacroPropertyUInt32("quota_ops_mask", max_frames)
        
        # Quota gap для логирования (per-day, не per-agent)
        self.env.newMacroPropertyInt32("mp2_quota_gap_mi8", model_build.MAX_DAYS + 1)
        self.env.newMacroPropertyInt32("mp2_quota_gap_mi17", model_build.MAX_DAYS + 1)
        
        # ═══════════════════════════════════════════════════════════════════════
        # MP2 Transition flags (для spawn_v2 и rtc_mp2_writer)
        # Размер: MAX_FRAMES * (MAX_DAYS + 1) = 400 * 4001 = 1,600,400
        # ═══════════════════════════════════════════════════════════════════════
        mp2_size = model_build.MAX_SIZE
        self.env.newMacroPropertyUInt32("mp2_transition_0_to_2", mp2_size)   # spawn → operations
        self.env.newMacroPropertyUInt32("mp2_transition_0_to_3", mp2_size)   # spawn → serviceable
        self.env.newMacroPropertyUInt32("mp2_transition_2_to_4", mp2_size)   # operations → repair
        self.env.newMacroPropertyUInt32("mp2_transition_2_to_6", mp2_size)   # operations → storage
        self.env.newMacroPropertyUInt32("mp2_transition_2_to_3", mp2_size)   # operations → serviceable
        self.env.newMacroPropertyUInt32("mp2_transition_3_to_2", mp2_size)   # serviceable → operations
        self.env.newMacroPropertyUInt32("mp2_transition_5_to_2", mp2_size)   # reserve → operations
        self.env.newMacroPropertyUInt32("mp2_transition_1_to_2", mp2_size)   # inactive → operations
        self.env.newMacroPropertyUInt32("mp2_transition_4_to_5", mp2_size)   # repair → reserve
        self.env.newMacroPropertyUInt32("mp2_transition_1_to_4", mp2_size)   # inactive → repair
        self.env.newMacroPropertyUInt32("mp2_transition_4_to_2", mp2_size)   # repair → operations
        
        print(f"  ✅ MacroProperty: MP5={model_build.MAX_SIZE}, Quota буферы для совместимости")
    
    def _setup_property_arrays(self, env_data: Dict[str, object]):
        """Настройка PropertyArray (квоты MP4)"""
        days = int(env_data['days_total_u16'])
        
        # MP4: Целевые количества в operations по дням
        mp4_mi8 = env_data.get('mp4_ops_counter_mi8', [0] * days)
        mp4_mi17 = env_data.get('mp4_ops_counter_mi17', [0] * days)
        
        # Приводим к int и обрезаем до days
        mp4_mi8_int = [int(x) for x in mp4_mi8[:days]]
        mp4_mi17_int = [int(x) for x in mp4_mi17[:days]]
        
        self.env.newPropertyArrayUInt("mp4_ops_counter_mi8", mp4_mi8_int)
        self.env.newPropertyArrayUInt("mp4_ops_counter_mi17", mp4_mi17_int)
        
        # MP4 new_counter для spawn (дата берём из version_date+day, НЕ из month_first)
        mp4_new = list(env_data.get('mp4_new_counter_mi17_seed', []))
        if not mp4_new:
            mp4_new = [0] * model_build.MAX_DAYS
        mp4_new = (mp4_new + [0] * model_build.MAX_DAYS)[:model_build.MAX_DAYS]
        self.env.newPropertyArrayUInt32("mp4_new_counter_mi17_seed", mp4_new)
        
        # MP3 mfg_date_days для spawn
        mp3_mfg = list(env_data.get('mp3_mfg_date_days', []))
        if not mp3_mfg:
            mp3_mfg = [0] * model_build.RTC_MAX_FRAMES
        mp3_mfg = (mp3_mfg + [0] * model_build.RTC_MAX_FRAMES)[:model_build.RTC_MAX_FRAMES]
        self.env.newPropertyArrayUInt32("mp3_mfg_date_days", mp3_mfg)
        
        print(f"  ✅ PropertyArray: mp4_ops_counter[{days}], mp4_new_counter_mi17_seed[{model_build.MAX_DAYS}]")
    
    def _setup_agent(self) -> fg.AgentDescription:
        """Настройка агента HELI (планеры) — идентично base_model.py"""
        agent = self.model.newAgent("HELI")
        
        # Состояния (7 штук) — V6 архитектура
        agent.newState("inactive")      # 1 — давно запаркованные
        agent.newState("operations")    # 2 — в эксплуатации
        agent.newState("serviceable")   # 3 — исправные в холдинге
        agent.newState("repair")        # 4 — только день 0, детерминированный выход
        agent.newState("reserve")       # 5 — только spawn, детерминированный
        agent.newState("storage")       # 6 — списанные (BR)
        agent.newState("unserviceable") # 7 — НОВЫЙ: после OH, ждёт промоут P2
        
        # Переменные агента
        agent.newVariableUInt("idx")
        agent.newVariableUInt("aircraft_number")
        agent.newVariableUInt("partseqno_i")
        agent.newVariableUInt("group_by")
        agent.newVariableUInt("status_id")
        agent.newVariableUInt("intent_state")
        agent.newVariableUInt("prev_intent", 0)  # Для event-driven: отслеживание изменений
        agent.newVariableUInt("bi_counter", 1)  # Служебное поле для BI (всегда 1)
        
        # Transition флаги — V6 архитектура
        agent.newVariableUInt("transition_0_to_2", 0)  # spawn → operations
        agent.newVariableUInt("transition_2_to_3", 0)  # демоут
        agent.newVariableUInt("transition_2_to_6", 0)  # списание (BR)
        agent.newVariableUInt("transition_2_to_7", 0)  # ops → unserviceable (OH)
        agent.newVariableUInt("transition_3_to_2", 0)  # промоут P1
        agent.newVariableUInt("transition_7_to_2", 0)  # промоут P2 (unserviceable)
        agent.newVariableUInt("transition_1_to_2", 0)  # промоут P3
        agent.newVariableUInt("transition_4_to_3", 0)  # repair → serviceable (детерм.)
        agent.newVariableUInt("transition_5_to_2", 0)  # spawn → operations (детерм.)
        
        # V6: Детерминированная дата выхода (для repair и spawn)
        agent.newVariableUInt("exit_date", 0xFFFFFFFF)  # День перехода (MAX = нет)
        
        # Наработка
        agent.newVariableUInt("sne", 0)
        agent.newVariableUInt("ppr", 0)
        agent.newVariableUInt("cso", 0)
        agent.newVariableUInt("daily_today_u32", 0)
        agent.newVariableUInt("daily_next_u32", 0)
        
        # Нормативы
        agent.newVariableUInt("ll", 0)
        agent.newVariableUInt("second_ll", 0)
        agent.newVariableUInt("oh", 0)
        agent.newVariableUInt("br", 0)
        
        # Ремонт
        agent.newVariableUInt("repair_time", 180)
        agent.newVariableUInt("assembly_time", 180)
        agent.newVariableUInt("partout_time", 180)
        agent.newVariableUInt("repair_days", 0)
        agent.newVariableUInt("assembly_trigger", 0)
        agent.newVariableUInt("active_trigger", 0)
        agent.newVariableUInt("partout_trigger", 0)
        
        # Дата производства (для ранжирования)
        agent.newVariableUInt("mfg_date", 0)
        
        # Счётчик дней в repair + reserve
        agent.newVariableUInt("s4_days", 0)
        
        # Adaptive step: дата лимита ресурса (день, когда агент выйдет из ops)
        # Вычисляется при входе в operations, MAX = нет лимита
        agent.newVariableUInt("limiter_date", 0xFFFFFFFF)
        
        # V2 Optimized: limiter = дней до выхода из ops
        # Вычисляется ОДИН РАЗ при входе в operations
        # На каждом шаге: limiter -= adaptive_days
        # При выходе: limiter = 0
        agent.newVariableUInt16("limiter", 0)
        
        # V4: Временное хранение для GPU-only вычисления adaptive_days
        agent.newVariableUInt("computed_adaptive_days", 1)
        
        # Дата последней смены статуса (для аналитики и ремонтов)
        agent.newVariableUInt("status_change_day", 0)
        
        # V8: данные для выбора ремонтной линии (двухфазно)
        agent.newVariableUInt("repair_candidate", 0)
        agent.newVariableUInt("repair_line_id", 0xFFFFFFFF)
        agent.newVariableUInt("repair_line_day", 0xFFFFFFFF)
        agent.newVariableUInt("repair_done", 0)  # 1 = ремонт завершён, ждёт промоут в ops
        
        # V7: Флаги для однофазной архитектуры (без intent)
        agent.newVariableUInt("promoted", 0)     # 1 = получил промоут в этом шаге
        agent.newVariableUInt("needs_demote", 0) # 1 = должен выйти из operations
        
        print("  ✅ Agent HELI: 7 состояний, переменные + limiter + V7 флаги")
        
        return agent

