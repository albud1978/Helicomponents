#!/usr/bin/env python3
"""
Исполнители симуляции (smoke тесты и продуктивные прогоны)
Упрощенная архитектура без множественного ветвления
Дата: 2025-09-12
"""

from typing import Dict, List, Any
import time

try:
    import pyflamegpu
except ImportError:
    pyflamegpu = None


class SmokeRunner:
    """Исполнитель smoke тестов"""
    
    def __init__(self, builder, env_setup):
        self.builder = builder
        self.env_setup = env_setup
    
    def run_quota_smoke(self, frames: int = None, days: int = 7) -> Dict[str, Any]:
        """Запуск теста системы квот"""
        
        print("🧪 GPU quota smoke test")
        
        # Подготовка данных
        env_data = self.env_setup.prepare_full_environment()
        frames_total = frames or env_data['frames_total']
        
        # Сборка модели
        model = self.builder.build_model(frames_total, days, "quota_smoke")
        sim = self.builder.create_simulation(model)
        
        # Применение данных
        self.env_setup.apply_to_simulation(sim, env_data)
        
        # Создание тестовой популяции
        agent_desc = model.getAgent("component")
        population = self._create_quota_test_population(agent_desc, frames_total)
        sim.setPopulationData(population)
        
        # Выполнение одного шага
        start_time = time.perf_counter()
        sim.step()
        end_time = time.perf_counter()
        
        # Анализ результатов
        result_population = pyflamegpu.AgentVector(agent_desc)
        sim.getPopulationData(result_population)
        
        return self._analyze_quota_results(result_population, start_time, end_time, env_data)
    
    def run_status_smoke(self, statuses: List[int], frames: int = None, days: int = 7) -> Dict[str, Any]:
        """Запуск теста статусов"""
        
        status_str = "".join(map(str, sorted(statuses)))
        print(f"🧪 Status {status_str} smoke test")
        
        # Подготовка данных
        env_data = self.env_setup.prepare_full_environment()
        frames_total = frames or env_data['frames_total']
        
        # Выбор профиля
        if set(statuses) == {2, 4, 6}:
            profile = "status_246"
        else:
            profile = "production"
        
        # Сборка модели
        model = self.builder.build_model(frames_total, days, profile)
        sim = self.builder.create_simulation(model)
        
        # Применение данных
        self.env_setup.apply_to_simulation(sim, env_data)
        
        # Создание популяции из реальных данных
        agent_desc = model.getAgent("component")
        population = self._create_real_population(agent_desc, env_data, statuses)
        sim.setPopulationData(population)
        
        # Выполнение симуляции
        start_time = time.perf_counter()
        
        for day in range(days):
            sim.step()
            if day % 100 == 0 or day < 10:
                print(f"  День {day}: OK")
        
        end_time = time.perf_counter()
        
        # Анализ результатов
        result_population = pyflamegpu.AgentVector(agent_desc)
        sim.getPopulationData(result_population)
        
        return self._analyze_status_results(result_population, start_time, end_time, days, statuses)
    
    def _create_quota_test_population(self, agent_desc, frames_total: int) -> "pyflamegpu.AgentVector":
        """Создает тестовую популяцию для quota smoke"""
        
        population = pyflamegpu.AgentVector(agent_desc, frames_total)
        
        for i in range(frames_total):
            agent = population[i]
            agent.setVariableUInt("idx", i)
            agent.setVariableUInt("group_by", 1 if i % 2 == 0 else 2)  # Чередуем MI-8/MI-17
            agent.setVariableUInt("status_id", 2)  # Все в эксплуатации
            agent.setVariableUInt("ops_ticket", 0)
            agent.setVariableUInt("intent_flag", 0)
        
        return population
    
    def _create_real_population(self, agent_desc, env_data: Dict, filter_statuses: List[int]) -> "pyflamegpu.AgentVector":
        """Создает популяцию из реальных данных MP3"""
        
        mp3_rows = env_data['mp3_rows']
        mp3_fields = env_data['mp3_fields']
        frames_index = env_data['frames_index']
        
        idx_map = {name: i for i, name in enumerate(mp3_fields)}
        
        # Фильтруем по статусам
        filtered_rows = [
            row for row in mp3_rows 
            if int(row[idx_map['status_id']] or 0) in filter_statuses
        ]
        
        population = pyflamegpu.AgentVector(agent_desc, len(filtered_rows))
        
        for i, row in enumerate(filtered_rows):
            agent = population[i]
            
            # Заполняем основные поля
            ac_num = int(row[idx_map['aircraft_number']] or 0)
            frame_idx = frames_index.get(ac_num, i)
            
            agent.setVariableUInt("idx", frame_idx)
            agent.setVariableUInt("psn", int(row[idx_map['psn']] or 0))
            agent.setVariableUInt("partseqno_i", int(row[idx_map['partseqno_i']] or 0))
            agent.setVariableUInt("aircraft_number", ac_num)
            agent.setVariableUInt("group_by", int(row[idx_map.get('group_by', -1)] or 0))
            agent.setVariableUInt("ac_type_mask", int(row[idx_map['ac_type_mask']] or 0))
            agent.setVariableUInt("status_id", int(row[idx_map['status_id']] or 0))
            agent.setVariableUInt("sne", int(row[idx_map['sne']] or 0))
            agent.setVariableUInt("ppr", int(row[idx_map['ppr']] or 0))
            agent.setVariableUInt("ll", int(row[idx_map['ll']] or 0))
            agent.setVariableUInt("oh", int(row[idx_map['oh']] or 0))
            agent.setVariableUInt("repair_days", int(row[idx_map['repair_days']] or 0))
            
            # Остальные поля по умолчанию
            agent.setVariableUInt("daily_today_u32", 0)
            agent.setVariableUInt("daily_next_u32", 0)
            agent.setVariableUInt("ops_ticket", 0)
            agent.setVariableUInt("intent_flag", 0)
        
        return population
    
    def _analyze_quota_results(self, population, start_time: float, end_time: float, env_data: Dict) -> Dict[str, Any]:
        """Анализирует результаты quota smoke"""
        
        # Подсчет билетов по группам
        tickets_mi8 = 0
        tickets_mi17 = 0
        
        for agent in population:
            if int(agent.getVariableUInt("ops_ticket")) == 1:
                gb = int(agent.getVariableUInt("group_by"))
                if gb == 1:
                    tickets_mi8 += 1
                elif gb == 2:
                    tickets_mi17 += 1
        
        # Семена квоты на D+1
        seed8 = env_data['mp4_ops_counter_mi8'][1] if len(env_data['mp4_ops_counter_mi8']) > 1 else 0
        seed17 = env_data['mp4_ops_counter_mi17'][1] if len(env_data['mp4_ops_counter_mi17']) > 1 else 0
        
        duration = end_time - start_time
        
        result = {
            "test_type": "quota_smoke",
            "duration_ms": duration * 1000,
            "seed8": seed8,
            "seed17": seed17,
            "claimed8": tickets_mi8,
            "claimed17": tickets_mi17,
            "success": (tickets_mi8 <= seed8) and (tickets_mi17 <= seed17)
        }
        
        print(f"GPU quota (internal): seed8[D1]={seed8}, claimed8={tickets_mi8}; seed17[D1]={seed17}, claimed17={tickets_mi17}")
        print(f"Время выполнения: {duration*1000:.2f} мс")
        
        return result
    
    def _analyze_status_results(self, population, start_time: float, end_time: float, 
                              days: int, initial_statuses: List[int]) -> Dict[str, Any]:
        """Анализирует результаты status smoke"""
        
        # Подсчет финальных статусов
        final_status_counts = {}
        for agent in population:
            status = int(agent.getVariableUInt('status_id'))
            final_status_counts[status] = final_status_counts.get(status, 0) + 1
        
        duration = end_time - start_time
        
        result = {
            "test_type": "status_smoke",
            "duration_ms": duration * 1000,
            "days": days,
            "initial_statuses": initial_statuses,
            "final_status_counts": final_status_counts,
            "total_agents": len(population)
        }
        
        print(f"Status smoke результаты:")
        print(f"  Дней: {days}, Агентов: {len(population)}")
        print(f"  Время: {duration*1000:.2f} мс")
        print(f"  Финальные статусы: {final_status_counts}")
        
        return result


class ProductionRunner:
    """Исполнитель продуктивных прогонов"""
    
    def __init__(self, builder, env_setup):
        self.builder = builder
        self.env_setup = env_setup
    
    def run_full_simulation(self, days: int, export_enabled: bool = False, 
                           export_table: str = "sim_results") -> Dict[str, Any]:
        """Запуск полной симуляции"""
        
        print(f"🚀 Полная симуляция на {days} дней")
        
        # Подготовка данных
        env_data = self.env_setup.prepare_full_environment()
        frames_total = env_data['frames_total']
        
        # Сборка модели
        model = self.builder.build_model(frames_total, days, "production")
        sim = self.builder.create_simulation(model)
        
        # Применение данных
        self.env_setup.apply_to_simulation(sim, env_data)
        
        # Создание популяции из всех агентов MP3
        agent_desc = model.getAgent("component")
        population = self._create_full_population(agent_desc, env_data)
        sim.setPopulationData(population)
        
        # Выполнение симуляции
        start_time = time.perf_counter()
        
        for day in range(days):
            sim.step()
            
            if day % 365 == 0 or day < 10:
                print(f"  День {day}: OK")
        
        end_time = time.perf_counter()
        
        # Результаты
        final_population = pyflamegpu.AgentVector(agent_desc)
        sim.getPopulationData(final_population)
        
        result = {
            "test_type": "production",
            "duration_ms": (end_time - start_time) * 1000,
            "days": days,
            "total_agents": len(final_population),
            "export_enabled": export_enabled
        }
        
        print(f"✅ Симуляция завершена: {days} дней, {(end_time - start_time)*1000:.2f} мс")
        
        return result
    
    def _create_full_population(self, agent_desc, env_data: Dict) -> "pyflamegpu.AgentVector":
        """Создает полную популяцию из MP3 данных"""
        
        mp3_rows = env_data['mp3_rows']
        mp3_fields = env_data['mp3_fields']
        frames_index = env_data['frames_index']
        mp1_map = env_data.get('mp1_map', {})
        
        idx_map = {name: i for i, name in enumerate(mp3_fields)}
        population = pyflamegpu.AgentVector(agent_desc, len(mp3_rows))
        
        for i, row in enumerate(mp3_rows):
            agent = population[i]
            
            # Базовые поля из MP3
            ac_num = int(row[idx_map['aircraft_number']] or 0)
            frame_idx = frames_index.get(ac_num, i)
            partseq = int(row[idx_map['partseqno_i']] or 0)
            
            agent.setVariableUInt("idx", frame_idx)
            agent.setVariableUInt("psn", int(row[idx_map['psn']] or 0))
            agent.setVariableUInt("partseqno_i", partseq)
            agent.setVariableUInt("aircraft_number", ac_num)
            agent.setVariableUInt("group_by", int(row[idx_map.get('group_by', -1)] or 0))
            agent.setVariableUInt("ac_type_mask", int(row[idx_map['ac_type_mask']] or 0))
            agent.setVariableUInt("status_id", int(row[idx_map['status_id']] or 0))
            agent.setVariableUInt("sne", int(row[idx_map['sne']] or 0))
            agent.setVariableUInt("ppr", int(row[idx_map['ppr']] or 0))
            agent.setVariableUInt("ll", int(row[idx_map['ll']] or 0))
            agent.setVariableUInt("oh", int(row[idx_map['oh']] or 0))
            agent.setVariableUInt("repair_days", int(row[idx_map['repair_days']] or 0))
            
            # Данные из MP1 по partseqno_i
            if mp1_map and partseq in mp1_map:
                b8, b17, rt, pt, at = mp1_map[partseq]
                agent.setVariableUInt("repair_time", int(rt or 0))
                agent.setVariableUInt("partout_time", int(pt or 0))
                agent.setVariableUInt("assembly_time", int(at or 0))
                
                # BR по типу планера
                mask = int(row[idx_map['ac_type_mask']] or 0)
                br_val = 0
                if mask & 32:  # MI-8
                    br_val = int(b8 or 0)
                elif mask & 64:  # MI-17
                    br_val = int(b17 or 0)
                agent.setVariableUInt("br", br_val)
            
            # mfg_date в ordinal days
            md = row[idx_map.get('mfg_date', -1)] if 'mfg_date' in idx_map else None
            ord_val = 0
            if md:
                try:
                    from datetime import date
                    epoch = date(1970, 1, 1)
                    ord_val = max(0, (md - epoch).days)
                except:
                    ord_val = 0
            agent.setVariableUInt("mfg_date", ord_val)
            
            # Остальные поля
            agent.setVariableUInt("daily_today_u32", 0)
            agent.setVariableUInt("daily_next_u32", 0)
            agent.setVariableUInt("ops_ticket", 0)
            agent.setVariableUInt("intent_flag", 0)
            agent.setVariableUInt("active_trigger", 0)
            agent.setVariableUInt("assembly_trigger", 0)
            agent.setVariableUInt("partout_trigger", 0)
        
        return population
    
    def _analyze_quota_results(self, population, start_time: float, end_time: float, env_data: Dict) -> Dict[str, Any]:
        """Анализ результатов quota теста"""
        
        tickets_mi8 = 0
        tickets_mi17 = 0
        intent_mi8 = 0
        intent_mi17 = 0
        
        for agent in population:
            gb = int(agent.getVariableUInt("group_by"))
            ticket = int(agent.getVariableUInt("ops_ticket"))
            intent = int(agent.getVariableUInt("intent_flag"))
            
            if gb == 1:
                if ticket == 1:
                    tickets_mi8 += 1
                if intent == 1:
                    intent_mi8 += 1
            elif gb == 2:
                if ticket == 1:
                    tickets_mi17 += 1
                if intent == 1:
                    intent_mi17 += 1
        
        return {
            "tickets_mi8": tickets_mi8,
            "tickets_mi17": tickets_mi17,
            "intent_mi8": intent_mi8,
            "intent_mi17": intent_mi17,
            "duration_ms": (end_time - start_time) * 1000
        }
    
    def _analyze_status_results(self, population, start_time: float, end_time: float, 
                              days: int, initial_statuses: List[int]) -> Dict[str, Any]:
        """Анализ результатов status теста"""
        
        final_counts = {}
        transitions = {}
        
        for agent in population:
            status = int(agent.getVariableUInt('status_id'))
            final_counts[status] = final_counts.get(status, 0) + 1
        
        return {
            "initial_statuses": initial_statuses,
            "final_counts": final_counts,
            "days": days,
            "total_agents": len(population),
            "duration_ms": (end_time - start_time) * 1000
        }


