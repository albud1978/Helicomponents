#!/usr/bin/env python3
"""
Конфигурация RTC пайплайна и профилей
Основано на rtc_pipeline_architecture.md
Дата: 2025-09-12
"""

from typing import Dict, List

# Канонический порядок выполнения RTC функций (из rtc_pipeline_architecture.md)
RTC_PIPELINE = [
    # Подготовка данных на начало суток (объединенная функция)
    {"name": "rtc_prepare_day", "flag": "HL_ALWAYS_ON", "layer": 1},
    
    # Status обработка (статусы 6,4,2)
    {"name": "rtc_status_6", "flag": "HL_ENABLE_STATUS_6", "layer": 3},
    {"name": "rtc_status_4", "flag": "HL_ENABLE_STATUS_4", "layer": 4},
    {"name": "rtc_status_2", "flag": "HL_ENABLE_STATUS_2", "layer": 5},
    
    # Quota цикл для статуса 2
    {"name": "rtc_quota_intent_s2", "flag": "HL_ENABLE_QUOTA_S2", "layer": 6},
    {"name": "rtc_quota_approve_s2", "flag": "HL_ENABLE_QUOTA_S2", "layer": 7},
    {"name": "rtc_quota_apply_s2", "flag": "HL_ENABLE_QUOTA_S2", "layer": 8},
    {"name": "rtc_quota_clear_s2", "flag": "HL_ENABLE_QUOTA_S2", "layer": 9},
    
    # Quota цикл для статуса 3
    {"name": "rtc_quota_intent_s3", "flag": "HL_ENABLE_QUOTA_S3", "layer": 10},
    {"name": "rtc_quota_approve_s3", "flag": "HL_ENABLE_QUOTA_S3", "layer": 11},
    {"name": "rtc_quota_apply_s3", "flag": "HL_ENABLE_QUOTA_S3", "layer": 12},
    {"name": "rtc_quota_clear_s3", "flag": "HL_ENABLE_QUOTA_S3", "layer": 13},
    
    # Post-обработка статуса 3
    {"name": "rtc_status_3_post", "flag": "HL_ENABLE_STATUS_3_POST", "layer": 14},
    
    # Quota цикл для статуса 5
    {"name": "rtc_quota_intent_s5", "flag": "HL_ENABLE_QUOTA_S5", "layer": 15},
    {"name": "rtc_quota_approve_s5", "flag": "HL_ENABLE_QUOTA_S5", "layer": 16},
    {"name": "rtc_quota_apply_s5", "flag": "HL_ENABLE_QUOTA_S5", "layer": 17},
    {"name": "rtc_quota_clear_s5", "flag": "HL_ENABLE_QUOTA_S5", "layer": 18},
    
    # Quota цикл для статуса 1
    {"name": "rtc_quota_intent_s1", "flag": "HL_ENABLE_QUOTA_S1", "layer": 19},
    {"name": "rtc_quota_approve_s1", "flag": "HL_ENABLE_QUOTA_S1", "layer": 20},
    {"name": "rtc_quota_apply_s1", "flag": "HL_ENABLE_QUOTA_S1", "layer": 21},
    {"name": "rtc_quota_clear_s1", "flag": "HL_ENABLE_QUOTA_S1", "layer": 22},
    
    # Post-обработка остальных статусов
    {"name": "rtc_status_1_post", "flag": "HL_ENABLE_STATUS_1_POST", "layer": 23},
    {"name": "rtc_status_5_post", "flag": "HL_ENABLE_STATUS_5_POST", "layer": 24},
    {"name": "rtc_status_2_post", "flag": "HL_ENABLE_STATUS_2_POST", "layer": 25},
    
    # Логирование
    {"name": "rtc_log_day", "flag": "HL_ENABLE_MP2_LOG", "layer": 26},
    
    # Спавн (последние слои)
    {"name": "rtc_spawn_mgr", "flag": "HL_ENABLE_SPAWN", "layer": 27},
    {"name": "rtc_spawn_ticket", "flag": "HL_ENABLE_SPAWN", "layer": 28},
]

# Профили конфигурации для разных сценариев
RTC_PROFILES = {
    "minimal": {
        "description": "Минимальный набор для базовых тестов",
        "flags": {
            "HL_ALWAYS_ON": True,
            "HL_ENABLE_STATUS_6": True,
            "HL_ENABLE_STATUS_4": True,
            "HL_ENABLE_STATUS_2": True,
            "HL_ENABLE_QUOTA_S2": False,
            "HL_ENABLE_QUOTA_S3": False,
            "HL_ENABLE_QUOTA_S5": False,
            "HL_ENABLE_QUOTA_S1": False,
            "HL_ENABLE_STATUS_3_POST": False,
            "HL_ENABLE_STATUS_1_POST": False,
            "HL_ENABLE_STATUS_5_POST": False,
            "HL_ENABLE_STATUS_2_POST": False,
            "HL_ENABLE_MP2_LOG": False,
            "HL_ENABLE_SPAWN": False,
        }
    },
    
    "quota_smoke": {
        "description": "Тестирование системы квот",
        "flags": {
            "HL_ALWAYS_ON": True,
            "HL_ENABLE_STATUS_6": True,
            "HL_ENABLE_STATUS_4": True,
            "HL_ENABLE_STATUS_2": True,
            "HL_ENABLE_QUOTA_S2": True,
            "HL_ENABLE_QUOTA_S3": True,
            "HL_ENABLE_QUOTA_S5": True,
            "HL_ENABLE_QUOTA_S1": True,
            "HL_ENABLE_STATUS_3_POST": True,
            "HL_ENABLE_STATUS_1_POST": True,
            "HL_ENABLE_STATUS_5_POST": True,
            "HL_ENABLE_STATUS_2_POST": True,
            "HL_ENABLE_MP2_LOG": True,
            "HL_ENABLE_SPAWN": False,
        }
    },
    
    "production": {
        "description": "Полная производственная конфигурация",
        "flags": {
            "HL_ALWAYS_ON": True,
            "HL_ENABLE_STATUS_6": True,
            "HL_ENABLE_STATUS_4": True,
            "HL_ENABLE_STATUS_2": True,
            "HL_ENABLE_QUOTA_S2": True,
            "HL_ENABLE_QUOTA_S3": True,
            "HL_ENABLE_QUOTA_S5": True,
            "HL_ENABLE_QUOTA_S1": True,
            "HL_ENABLE_STATUS_3_POST": True,
            "HL_ENABLE_STATUS_1_POST": True,
            "HL_ENABLE_STATUS_5_POST": True,
            "HL_ENABLE_STATUS_2_POST": True,
            "HL_ENABLE_MP2_LOG": True,
            "HL_ENABLE_SPAWN": True,
        }
    },
    
    "status_246": {
        "description": "Статусы 2,4,6 без квотирования",
        "flags": {
            "HL_ALWAYS_ON": True,
            "HL_ENABLE_STATUS_6": True,
            "HL_ENABLE_STATUS_4": True,
            "HL_ENABLE_STATUS_2": True,
            "HL_ENABLE_QUOTA_S2": False,
            "HL_ENABLE_QUOTA_S3": False,
            "HL_ENABLE_QUOTA_S5": False,
            "HL_ENABLE_QUOTA_S1": False,
            "HL_ENABLE_STATUS_3_POST": False,
            "HL_ENABLE_STATUS_1_POST": False,
            "HL_ENABLE_STATUS_5_POST": False,
            "HL_ENABLE_STATUS_2_POST": False,
            "HL_ENABLE_MP2_LOG": True,
            "HL_ENABLE_SPAWN": False,
        }
    }
}


class RTCPipeline:
    """Управление пайплайном RTC функций"""
    
    def __init__(self):
        self.pipeline = RTC_PIPELINE.copy()
    
    def get_enabled_functions(self, profile_name: str = None, custom_flags: Dict[str, bool] = None) -> List[Dict]:
        """Возвращает список включенных RTC функций согласно профилю"""
        
        # Определяем активные флаги
        active_flags = {}
        if profile_name and profile_name in RTC_PROFILES:
            active_flags = RTC_PROFILES[profile_name]["flags"].copy()
        
        # Переопределяем кастомными флагами
        if custom_flags:
            active_flags.update(custom_flags)
        
        # Фильтруем включенные функции
        enabled = []
        for func in self.pipeline:
            flag = func["flag"]
            if flag == "HL_ALWAYS_ON" or active_flags.get(flag, False):
                enabled.append(func.copy())
        
        return enabled
    
    def get_pipeline_summary(self, profile_name: str = None) -> str:
        """Возвращает краткое описание активного пайплайна"""
        enabled = self.get_enabled_functions(profile_name)
        summary = f"RTC Pipeline ({len(enabled)} функций):\n"
        
        for func in enabled:
            layer = func["layer"]
            name = func["name"]
            summary += f"  Слой {layer:2d}: {name}\n"
        
        return summary


def apply_profile(profile_name: str) -> Dict[str, bool]:
    """Применяет профиль к environment переменным"""
    if profile_name not in RTC_PROFILES:
        raise ValueError(f"Неизвестный профиль: {profile_name}")
    
    import os
    profile = RTC_PROFILES[profile_name]
    
    for flag, value in profile["flags"].items():
        os.environ[flag] = "1" if value else "0"
    
    return profile["flags"].copy()


def get_profile_flags(profile_name: str) -> Dict[str, bool]:
    """Возвращает флаги профиля без применения к environment"""
    if profile_name not in RTC_PROFILES:
        raise ValueError(f"Неизвестный профиль: {profile_name}")
    
    return RTC_PROFILES[profile_name]["flags"].copy()
