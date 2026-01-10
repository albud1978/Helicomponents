#!/usr/bin/env python3
"""
Adaptive 2.0: Определения агентов

Два типа агентов:
1. Planer — основной агент планера
2. ProgramEvent — агент-событие изменения программы

Дата: 10.01.2026
"""
import pyflamegpu as fg


def create_planer_agent(model: fg.ModelDescription) -> fg.AgentDescription:
    """
    Создаёт агента Planer с минималистичным набором переменных.
    
    Ключевое: limiter_date — вычисляется при входе в состояние
    """
    agent = model.newAgent("Planer")
    
    # ═══════════════════════════════════════════════════════════════════════
    # Идентификация
    # ═══════════════════════════════════════════════════════════════════════
    agent.newVariableUInt16("idx")
    agent.newVariableUInt32("aircraft_number")
    agent.newVariableUInt8("group_by")  # 1=Mi-8, 2=Mi-17
    
    # ═══════════════════════════════════════════════════════════════════════
    # Ресурсы
    # ═══════════════════════════════════════════════════════════════════════
    agent.newVariableUInt32("sne", 0)       # Наработка СНЭ
    agent.newVariableUInt32("ppr", 0)       # Межремонтный ресурс
    agent.newVariableUInt32("ll", 0)        # Назначенный ресурс
    agent.newVariableUInt32("oh", 0)        # Межремонтный лимит
    agent.newVariableUInt32("br", 0)        # Межремонтный контроль
    
    # ═══════════════════════════════════════════════════════════════════════
    # Ремонт
    # ═══════════════════════════════════════════════════════════════════════
    agent.newVariableUInt16("repair_days", 0)    # Дней в ремонте
    agent.newVariableUInt16("repair_time", 180)  # Длительность ремонта
    
    # ═══════════════════════════════════════════════════════════════════════
    # КЛЮЧЕВОЕ: Лимитер
    # ═══════════════════════════════════════════════════════════════════════
    # Вычисляется при входе в состояние
    # operations: день исчерпания ресурса
    # repair: день завершения ремонта
    # другие: MAX_UINT (не участвует в min)
    agent.newVariableUInt16("limiter_date", 0xFFFF)
    
    # ═══════════════════════════════════════════════════════════════════════
    # Вспомогательные
    # ═══════════════════════════════════════════════════════════════════════
    agent.newVariableUInt32("mfg_date", 0)  # Дата производства (для ранжирования)
    
    # ═══════════════════════════════════════════════════════════════════════
    # Состояния
    # ═══════════════════════════════════════════════════════════════════════
    agent.newState("inactive")
    agent.newState("operations")
    agent.newState("repair")
    agent.newState("reserve")
    agent.newState("storage")
    
    # Начальное состояние по умолчанию
    agent.setInitialState("inactive")
    
    print("  ✅ Агент Planer создан (5 состояний, limiter_date)")
    return agent


def create_program_event_agent(model: fg.ModelDescription) -> fg.AgentDescription:
    """
    Создаёт агента ProgramEvent — события изменения программы.
    
    Каждое изменение target в mp4_ops_counter = один агент.
    """
    agent = model.newAgent("ProgramEvent")
    
    # День события
    agent.newVariableUInt16("event_day")
    
    # Новые targets
    agent.newVariableUInt16("target_mi8")
    agent.newVariableUInt16("target_mi17")
    
    # Флаг обработки (чтобы не срабатывать повторно)
    agent.newVariableUInt8("processed", 0)
    
    # Единственное состояние
    agent.newState("active")
    agent.setInitialState("active")
    
    print("  ✅ Агент ProgramEvent создан")
    return agent


def create_quota_manager_agent(model: fg.ModelDescription) -> fg.AgentDescription:
    """
    Создаёт агента QuotaManager для вычисления adaptive_days.
    
    Один агент на всю симуляцию — читает limiter_date всех Planer.
    """
    agent = model.newAgent("QuotaManager")
    
    # Для идентификации (опционально)
    agent.newVariableUInt8("id", 0)
    
    # Единственное состояние
    agent.newState("active")
    agent.setInitialState("active")
    
    print("  ✅ Агент QuotaManager создан")
    return agent


def setup_environment_2_0(env: fg.EnvironmentDescription, max_frames: int = 400, max_days: int = 4000, max_events: int = 500):
    """
    Настраивает Environment для Adaptive 2.0.
    """
    # ═══════════════════════════════════════════════════════════════════════
    # Скалярные свойства
    # ═══════════════════════════════════════════════════════════════════════
    env.newPropertyUInt("current_day", 0)
    env.newPropertyUInt("end_day", 3650)
    env.newPropertyUInt("adaptive_days", 1)
    env.newPropertyUInt("frames_total", 0)
    env.newPropertyUInt("events_total", 0)
    
    # Текущие targets (обновляются при обработке ProgramEvent)
    env.newPropertyUInt("target_mi8", 0)
    env.newPropertyUInt("target_mi17", 0)
    
    # ═══════════════════════════════════════════════════════════════════════
    # MacroProperty
    # ═══════════════════════════════════════════════════════════════════════
    
    # Кумулятивная сумма налёта: mp5_cumsum[idx * (MAX_DAYS+1) + day]
    cumsum_size = max_frames * (max_days + 1)
    env.newMacroPropertyUInt32("mp5_cumsum", cumsum_size)
    
    # ProgramEvent данные (альтернатива агентам для простоты)
    env.newMacroPropertyUInt16("program_event_days", max_events)
    env.newMacroPropertyUInt16("program_target_mi8", max_events)
    env.newMacroPropertyUInt16("program_target_mi17", max_events)
    
    # Буфер limiter_date для global min (одно значение на агента)
    env.newMacroPropertyUInt16("limiter_buffer", max_frames)
    
    # Результат global min
    env.newMacroPropertyUInt16("global_min_result", 4)  # [min_value, min_idx, ...]
    
    # MP2 буфер для batch drain
    mp2_size = max_frames * 500  # ~500 шагов за симуляцию
    env.newMacroPropertyUInt32("mp2_buffer_sne", mp2_size)
    env.newMacroPropertyUInt32("mp2_buffer_ppr", mp2_size)
    env.newMacroPropertyUInt16("mp2_buffer_day", mp2_size)
    env.newMacroPropertyUInt8("mp2_buffer_state", mp2_size)
    env.newPropertyUInt("mp2_write_idx", 0)
    
    print(f"  ✅ Environment 2.0: cumsum[{cumsum_size}], events[{max_events}], mp2[{mp2_size}]")


# ═══════════════════════════════════════════════════════════════════════════
# Тестирование
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    print("=== Тест создания агентов 2.0 ===")
    
    model = fg.ModelDescription("TestAdaptive2_0")
    
    planer = create_planer_agent(model)
    program_event = create_program_event_agent(model)
    quota_manager = create_quota_manager_agent(model)
    
    env = model.Environment()
    setup_environment_2_0(env)
    
    print("\n✅ Все агенты и Environment созданы успешно")

