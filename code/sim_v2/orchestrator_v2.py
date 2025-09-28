#!/usr/bin/env python3
"""
V2 Orchestrator: модульная архитектура с динамической загрузкой RTC модулей
"""
import os
import sys
import json
import argparse
import datetime
import time
from typing import Dict, List

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from sim_env_setup import get_client, prepare_env_arrays
from base_model import V2BaseModel
from rtc_mp5_probe import create_host_function_mp5_init

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


class V2Orchestrator:
    """Оркестратор для управления модульной симуляцией"""
    
    def __init__(self, env_data: Dict[str, object], enable_mp2: bool = False, clickhouse_client = None):
        self.env_data = env_data
        self.base_model = V2BaseModel()
        self.model = None
        self.simulation = None
        
        # Параметры из окружения
        self.frames = int(env_data['frames_total_u16'])
        # Берем реальное количество дней из данных MP5
        self.days = int(env_data.get('days_total_u16', 90))
        # Базовая дата (ord) для вычисления календарного дня
        self.version_date_ord = int(env_data.get('version_date_u16', 0))
        
        # MP2 параметры
        self.enable_mp2 = enable_mp2
        self.clickhouse_client = clickhouse_client
        self.mp2_drain_func = None
        
    def build_model(self, rtc_modules: List[str]):
        """Строит модель с указанными RTC модулями"""
        print(f"Построение модели с модулями: {', '.join(rtc_modules)}")
        
        # Создаем базовую модель
        self.model = self.base_model.create_model(self.env_data)
        
        # MP5 всегда инициализируется, так как используется в функциях состояний
        self._add_mp5_init_layer()
        
        # Создаем слой для обработки состояний
        state_layer = self.model.newLayer('state_processing')
        
        # Подключаем RTC модули
        for module_name in rtc_modules:
            print(f"  Подключение модуля: {module_name}")
            self.base_model.add_rtc_module(module_name)
            
        # Добавляем MP2 writer если включен
        if self.enable_mp2:
            print("  Подключение MP2 device-side export")
            import rtc_mp2_writer
            self.mp2_drain_func = rtc_mp2_writer.register_mp2_writer(self.model, self.base_model.agent, self.clickhouse_client)
        
        return self.model
    
    def create_simulation(self):
        """Создает и настраивает симуляцию"""
        if not self.model:
            raise RuntimeError("Модель не построена")
            
        self.simulation = fg.CUDASimulation(self.model)
        
        # Заполняем окружение
        sim = self.simulation
        sim.setEnvironmentPropertyUInt("version_date", int(self.env_data['version_date_u16']))
        if 'version_id_u32' in self.env_data:
            sim.setEnvironmentPropertyUInt("version_id", int(self.env_data['version_id_u32']))
        sim.setEnvironmentPropertyUInt("frames_total", self.frames)
        sim.setEnvironmentPropertyUInt("days_total", self.days)
        
        # MP4 квоты уже установлены в базовой модели с правильным размером
        
        # Создаем популяцию агентов из MP3
        self._populate_agents()
        
        return self.simulation
    
    def _populate_agents(self):
        """Загружает агентов из MP3 данных с поддержкой States"""
        # Извлекаем массивы MP3
        mp3 = self.env_data.get('mp3_arrays', {})
        ac_list = mp3.get('mp3_aircraft_number', [])
        status_list = mp3.get('mp3_status_id', [])
        sne_list = mp3.get('mp3_sne', [])
        ppr_list = mp3.get('mp3_ppr', [])
        repair_days_list = mp3.get('mp3_repair_days', [])
        gb_list = mp3.get('mp3_group_by', [])
        pseq_list = mp3.get('mp3_partseqno_i', [])
        
        # Получаем MP1 данные для OH
        mp1_arrays = self.env_data.get('mp1_arrays', {})
        mp1_partseqno = mp1_arrays.get('partseqno_i', [])
        # OH берём из верхнего уровня env_data как в sim_master
        mp1_oh_mi8 = self.env_data.get('mp1_oh_mi8', [])
        mp1_oh_mi17 = self.env_data.get('mp1_oh_mi17', [])
        
        # Индекс кадров
        frames_index = self.env_data.get('frames_index', {})
        
        # Предварительно вычисляем LL/OH/BR по кадрам
        ll_by_frame, oh_by_frame, br_by_frame = self._build_norms_by_frame()
        
        # Отладка OH отключена (минимальный лог)
        
        # Создаем популяции для каждого состояния
        populations = {
            'inactive': fg.AgentVector(self.base_model.agent),      # state_1
            'operations': fg.AgentVector(self.base_model.agent),    # state_2
            'serviceable': fg.AgentVector(self.base_model.agent),   # state_3
            'repair': fg.AgentVector(self.base_model.agent),        # state_4
            'reserve': fg.AgentVector(self.base_model.agent),       # state_5
            'storage': fg.AgentVector(self.base_model.agent)        # state_6
        }
        
        # Маппинг status_id -> state name
        status_to_state = {
            0: 'inactive',      # Статус 0 тоже считаем неактивным
            1: 'inactive',
            2: 'operations',
            3: 'serviceable',
            4: 'repair',
            5: 'reserve',
            6: 'storage'
        }
        
        # Сначала фильтруем записи с group_by in [1,2]
        plane_records = []
        for j in range(len(ac_list)):
            gb = int(gb_list[j] or 0) if j < len(gb_list) else 0
            if gb in [1, 2]:
                ac = int(ac_list[j] or 0)
                if ac > 0 and ac in frames_index:
                    plane_records.append({
                        'idx': j,
                        'aircraft_number': ac,
                        'frame_idx': frames_index[ac],
                        'status_id': int(status_list[j] or 1) if j < len(status_list) else 1,
                        'sne': int(sne_list[j] or 0) if j < len(sne_list) else 0,
                        'ppr': int(ppr_list[j] or 0) if j < len(ppr_list) else 0,
                        'repair_days': int(repair_days_list[j] or 0) if j < len(repair_days_list) else 0,
                        'group_by': gb,
                        'partseqno_i': int(pseq_list[j] or 0) if j < len(pseq_list) else 0
                    })
        
        # Группируем по frame_idx и берем первую запись для каждого
        records_by_frame = {}
        for rec in plane_records:
            frame_idx = rec['frame_idx']
            if frame_idx not in records_by_frame:
                records_by_frame[frame_idx] = rec
        
        # Получаем информацию о зарезервированных слотах
        first_reserved_idx = self.env_data.get('first_reserved_idx', self.frames)
        
        # Заполняем агентов и распределяем по состояниям
        # Создаем только реальных агентов, пропускаем зарезервированные слоты
        for i in range(self.frames):
            # Пропускаем зарезервированные слоты для будущего спавна
            if i >= first_reserved_idx:
                continue
                
            # Пропускаем индексы без реальных агентов
            if i not in records_by_frame:
                continue
                
            # Берем данные для этого frame_idx
            agent_data = records_by_frame[i]
            
            # Определяем состояние и добавляем агента
            status_id = agent_data['status_id']
            state_name = status_to_state.get(status_id, 'inactive')
            pop = populations[state_name]
            pop.push_back()
            agent = pop[len(pop) - 1]
            
            # Базовые переменные
            agent.setVariableUInt("idx", i)
            agent.setVariableUInt("aircraft_number", agent_data['aircraft_number'])
            agent.setVariableUInt("status_id", status_id)
            agent.setVariableUInt("sne", agent_data['sne'])
            agent.setVariableUInt("ppr", agent_data['ppr'])
            agent.setVariableUInt("repair_days", agent_data['repair_days'])
            gb = agent_data.get('group_by', 0)
            partseqno = agent_data.get('partseqno_i', 0)
            agent.setVariableUInt("group_by", gb)
            agent.setVariableUInt("partseqno_i", partseqno)
            
            # OH берём из MP1 по типу вертолёта
            oh_value = oh_by_frame[i]  # Значение по умолчанию
            
            # Используем mp1_index как в sim_master
            mp1_index = self.env_data.get('mp1_index', {})
            pidx = mp1_index.get(partseqno, -1)
            
            if pidx >= 0:
                # В sim_master используется ac_type_mask, но у нас есть group_by
                # group_by = 1 это Mi-8 (mask & 32)
                # group_by = 2 это Mi-17 (mask & 64)
                if gb == 1:  # Mi-8
                    if pidx < len(mp1_oh_mi8):
                        oh_value = int(mp1_oh_mi8[pidx] or 0)
                elif gb == 2:  # Mi-17
                    if pidx < len(mp1_oh_mi17):
                        oh_value = int(mp1_oh_mi17[pidx] or 0)
            
            # Нормативы
            # LL берём из MP3 (heli_pandas)
            ll_list = mp3.get('mp3_ll', [])
            # Используем индекс из agent_data, который указывает на позицию в MP3
            mp3_idx = agent_data.get('idx', -1)
            if mp3_idx >= 0 and mp3_idx < len(ll_list):
                ll_value = int(ll_list[mp3_idx] or 0)
                if ll_value == 0:  # Если 0, используем значение по умолчанию
                    ll_value = ll_by_frame[i]
            else:
                ll_value = ll_by_frame[i]  # значение по умолчанию
            
            agent.setVariableUInt("ll", ll_value)
            agent.setVariableUInt("oh", oh_value)
            agent.setVariableUInt("br", br_by_frame[i])

            # mfg_date для приоритизации квот (ord days от 1970-01-01)
            mfg_list = mp3.get('mp3_mfg_date_days', [])
            if mp3_idx >= 0 and mp3_idx < len(mfg_list):
                mfg_val = int(mfg_list[mp3_idx] or 0)
            else:
                mfg_val = 0
            agent.setVariableUInt("mfg_date", mfg_val)
            
            # Отладка для первых нескольких агентов operations
            real_ac = agent_data.get('aircraft_number', 0)
            if state_name == "operations":
                mp1_idx = self.env_data.get('mp1_index', {}).get(partseqno, -1)
                ppr_val = agent_data.get('ppr', 0)
                sne_val = agent_data.get('sne', 0)
                # Отладочный вывод отключён
            
            # Времена ремонта из констант
            if gb == 1:
                agent.setVariableUInt("repair_time", self.simulation.getEnvironmentPropertyUInt("mi8_repair_time_const"))
                agent.setVariableUInt("assembly_time", self.simulation.getEnvironmentPropertyUInt("mi8_assembly_time_const"))
            elif gb == 2:
                agent.setVariableUInt("repair_time", self.simulation.getEnvironmentPropertyUInt("mi17_repair_time_const"))
                agent.setVariableUInt("assembly_time", self.simulation.getEnvironmentPropertyUInt("mi17_assembly_time_const"))
            
            # Для агентов в статусе 6 устанавливаем s6_started
            if status_id == 6:
                agent.setVariableUInt("s6_started", 0)  # Изначально в статусе 6
            
            # Для агентов в статусе 4 проверяем assembly_trigger
            if status_id == 4:
                repair_time = agent.getVariableUInt("repair_time")
                repair_days = agent.getVariableUInt("repair_days")
                assembly_time = agent.getVariableUInt("assembly_time")
                
                if repair_time - repair_days > assembly_time:
                    agent.setVariableUInt("assembly_trigger", 1)
            
            # intent_state = 0 для всех (нет начальных намерений)
            agent.setVariableUInt("intent_state", 0)
        
        # Удаляем отладочный код для AC 22417
        
        # Загружаем популяции в симуляцию по состояниям
        for state_name, pop in populations.items():
            if len(pop) > 0:
                self.simulation.setPopulationData(pop, state_name)
                print(f"  Загружено {len(pop)} агентов в состояние '{state_name}'")
    
    def _build_norms_by_frame(self):
        """Вычисляет нормативы LL/OH/BR по кадрам"""
        ll_by_frame = [0] * self.frames
        oh_by_frame = [0] * self.frames
        br_by_frame = [0] * self.frames
        
        # Получаем данные из env_data
        mp3_arrays = self.env_data.get('mp3_arrays', {})
        mp1_arrays = self.env_data.get('mp1_arrays', {})
        
        # MP3 данные
        ac_list = mp3_arrays.get('mp3_aircraft_number', [])
        gb_list = mp3_arrays.get('mp3_group_by', [])
        pseq_list = mp3_arrays.get('mp3_partseqno_i', [])
        
        # MP1 данные (нормативы из md_components)
        mp1_partseqno = mp1_arrays.get('partseqno_i', [])
        mp1_ll_mi8 = mp1_arrays.get('ll_mi8', [])
        mp1_ll_mi17 = mp1_arrays.get('ll_mi17', [])
        # OH берём из верхнего уровня env_data как в sim_master
        mp1_oh_mi8 = self.env_data.get('mp1_oh_mi8', [])
        mp1_oh_mi17 = self.env_data.get('mp1_oh_mi17', [])
        mp1_br_mi8 = mp1_arrays.get('br_mi8', [])
        mp1_br_mi17 = mp1_arrays.get('br_mi17', [])
        
        # Создаем карту partseqno -> нормативы
        norms_map = {}
        for i, partseq in enumerate(mp1_partseqno):
            if i < len(mp1_ll_mi8):
                norms_map[partseq] = {
                    'll_mi8': mp1_ll_mi8[i] if i < len(mp1_ll_mi8) else 0,
                    'll_mi17': mp1_ll_mi17[i] if i < len(mp1_ll_mi17) else 0,
                    'oh_mi8': mp1_oh_mi8[i] if i < len(mp1_oh_mi8) else 0,
                    'oh_mi17': mp1_oh_mi17[i] if i < len(mp1_oh_mi17) else 0,
                    'br_mi8': mp1_br_mi8[i] if i < len(mp1_br_mi8) else 0,
                    'br_mi17': mp1_br_mi17[i] if i < len(mp1_br_mi17) else 0,
                }
        
        # Индекс кадров
        frames_index = self.env_data.get('frames_index', {})
        # Начало зарезервированной области под будущий спавн — эти кадры пропускаем
        first_reserved_idx = int(self.env_data.get('first_reserved_idx', self.frames))
        
        # Заполняем нормативы только для реальных кадров (без зарезервированных)
        for i in range(first_reserved_idx):
            # Находим aircraft_number для этого frame
            ac = 0
            gb = 0
            partseq = 0
            
            # Ищем в frames_index
            for aircraft_number, frame_idx in frames_index.items():
                if frame_idx == i:
                    ac = aircraft_number
                    # Находим group_by и partseqno для этого AC в MP3
                    for j, mp3_ac in enumerate(ac_list):
                        if int(mp3_ac or 0) == ac and j < len(gb_list):
                            gb_val = int(gb_list[j] or 0)
                            if gb_val in [1, 2]:  # Только планеры
                                gb = gb_val
                                partseq = int(pseq_list[j] or 0) if j < len(pseq_list) else 0
                                break
                    break
            
            # Проверяем принадлежность к планёрам и наличие ключевых полей
            if gb not in (1, 2) or partseq == 0:
                raise RuntimeError(
                    f"Отсутствуют MP1-данные для кадра i={i} (ac={ac}, group_by={gb}, partseqno_i={partseq}). "
                    "Пайплайн остановлен: запрещены дефолтные нормативы."
                )

            # Получаем нормативы из карты (строго без дефолтов)
            if partseq not in norms_map:
                raise RuntimeError(
                    f"Не найден partseqno_i={partseq} в MP1 для кадра i={i} (ac={ac}, group_by={gb}). "
                    "Проверьте md_components.* источники."
                )

            norms = norms_map[partseq]
            if gb == 1:  # Mi-8
                ll_by_frame[i] = int(norms.get('ll_mi8', 0) or 0)
                oh_by_frame[i] = int(norms.get('oh_mi8', 0) or 0)
                br_by_frame[i] = int(norms.get('br_mi8', 0) or 0)
                if oh_by_frame[i] == 0 or br_by_frame[i] == 0:
                    raise RuntimeError(
                        f"Нормативы OH/BR отсутствуют (0) для Mi-8: i={i}, ac={ac}, partseq={partseq}. "
                        "Запрещены дефолтные значения."
                    )
            else:  # gb == 2, Mi-17
                ll_by_frame[i] = int(norms.get('ll_mi17', 0) or 0)
                oh_by_frame[i] = int(norms.get('oh_mi17', 0) or 0)
                br_by_frame[i] = int(norms.get('br_mi17', 0) or 0)
                if oh_by_frame[i] == 0 or br_by_frame[i] == 0:
                    raise RuntimeError(
                        f"Нормативы OH/BR отсутствуют (0) для Mi-17: i={i}, ac={ac}, partseq={partseq}. "
                        "Запрещены дефолтные значения."
                    )
        
        return ll_by_frame, oh_by_frame, br_by_frame
    
    def _add_mp5_init_layer(self):
        """Добавляет слой инициализации MP5 данных через HostFunction"""
            
        # Подготовка данных MP5
        mp5_data = list(self.env_data['mp5_daily_hours_linear'])
        need = (self.days + 1) * self.frames
        mp5_data = mp5_data[:need]
        
        # Создаем HostFunction
        hf_init = create_host_function_mp5_init(mp5_data, self.frames, self.days)
        
        # Создаем отдельный слой для инициализации MP5 в самом начале
        # Важно: это должно быть до всех RTC функций
        init_layer = self.model.newLayer()
        init_layer.addHostFunction(hf_init)
        
        print("MP5 будет инициализирован при первом шаге симуляции")
    
    def run(self, steps: int):
        """Запускает симуляцию на указанное количество шагов"""
        print(f"Запуск симуляции на {steps} шагов")
        
        # Обновляем количество шагов в MP2 drain функции
        if self.mp2_drain_func:
            self.mp2_drain_func.simulation_steps = steps
        

        # Подготовка: состояния и предыдущее значение intent для operations
        state_names = [
            'inactive', 'operations', 'serviceable', 'repair', 'reserve', 'storage'
        ]

        def get_operations_intents():
            pop = fg.AgentVector(self.base_model.agent)
            self.simulation.getPopulationData(pop, 'operations')
            result = {}
            for i in range(len(pop)):
                ag = pop[i]
                idx = ag.getVariableUInt('idx')
                intent = ag.getVariableUInt('intent_state')
                ac = ag.getVariableUInt('aircraft_number')
                sne = ag.getVariableUInt('sne')
                ppr = ag.getVariableUInt('ppr')
                dt = ag.getVariableUInt('daily_today_u32')
                dn = ag.getVariableUInt('daily_next_u32')
                ll = ag.getVariableUInt('ll')
                oh = ag.getVariableUInt('oh')
                br = ag.getVariableUInt('br')
                result[idx] = (intent, ac, sne, ppr, dt, dn, ll, oh, br)
            return result

        def print_step_state_counts(step_index: int):
            counts = {}
            for s in state_names:
                pop = fg.AgentVector(self.base_model.agent)
                self.simulation.getPopulationData(pop, s)
                counts[s] = len(pop)
            print(f"  Step {step_index}: counts "
                  f"inactive={counts['inactive']}, operations={counts['operations']}, "
                  f"serviceable={counts['serviceable']}, repair={counts['repair']}, "
                  f"reserve={counts['reserve']}, storage={counts['storage']}")

        prev_ops_intent = get_operations_intents()

        # Расширенная телеметрия по шагам
        step_times = []
        for step in range(steps):
            t0 = time.perf_counter()
            self.simulation.step()
            step_times.append(time.perf_counter() - t0)

            # Сводка по состояниям отключена
            
            # MP2 дренаж выполняется автоматически через зарегистрированную host функцию

            # Логи изменений intent в operations на значения != 2
            curr_ops_intent = get_operations_intents()
            for idx, vals in curr_ops_intent.items():
                new_intent, ac, sne, ppr, dt, dn, ll, oh, br = vals
                old_vals = prev_ops_intent.get(idx, (None, ac, None, None, None, None, None, None, None))
                old_intent, _ac_old, sne_old, ppr_old, dt_old, dn_old, _ll_old, _oh_old, _br_old = old_vals
                if new_intent != 2 and new_intent != old_intent:
                    # Прогноз на завтра должен основываться на значениях ДО шага: p_next=ppr_old+dn_old; s_next=sne_old+dn_old
                    s_next = (sne_old + dn_old) if sne_old is not None and dn_old is not None else None
                    p_next = (ppr_old + dn_old) if ppr_old is not None and dn_old is not None else None
                    # Дата перехода: day_abs = version_date_u16 + day_u16, где day_u16 = step+1
                    base_date = datetime.date(1970, 1, 1)
                    day_abs = int(self.version_date_ord) + (step + 1)
                    date_str = (base_date + datetime.timedelta(days=day_abs)).isoformat()
                    print(
                        f"  [Day {step+1} | date={date_str}] AC {ac} idx={idx}: "
                        f"intent {old_intent}->{new_intent} (operations) "
                        f"sne={sne}, ppr={ppr}, dt={dt}, dn={dn}, s_next={s_next}, p_next={p_next}, "
                        f"ll={ll}, oh={oh}, br={br}"
                    )
            prev_ops_intent = curr_ops_intent
    
    def get_results(self):
        """Извлекает результаты симуляции из всех состояний"""
        results = []
        
        # Маппинг state -> status_id для обратной совместимости
        state_to_status = {
            'inactive': 1,
            'operations': 2,
            'serviceable': 3,
            'repair': 4,
            'reserve': 5,
            'storage': 6
        }
        
        # Извлекаем агентов из каждого состояния
        for state_name in state_to_status.keys():
            pop = fg.AgentVector(self.base_model.agent)
            self.simulation.getPopulationData(pop, state_name)
            
            # Подробный вывод по извлечению отключен
            
            for i in range(len(pop)):
                agent = pop[i]
                results.append({
                    'idx': agent.getVariableUInt("idx"),
                    'aircraft_number': agent.getVariableUInt("aircraft_number"),
                    'state': state_name,
                    'sne': agent.getVariableUInt("sne"),
                    'ppr': agent.getVariableUInt("ppr"),
                    'daily_today': agent.getVariableUInt("daily_today_u32"),
                    'daily_next': agent.getVariableUInt("daily_next_u32"),
                    'intent_state': agent.getVariableUInt("intent_state"),
                    'repair_days': agent.getVariableUInt("repair_days"),
                    'll': agent.getVariableUInt("ll"),
                    'oh': agent.getVariableUInt("oh"),
                    'br': agent.getVariableUInt("br")
                })
        
        # Сортируем по idx для удобства
        results.sort(key=lambda x: x['idx'])
        
        # Отладочная информация о пропущенных слотах
        actual_count = len(results)
        expected_count = self.env_data.get('first_reserved_idx', self.frames)
        if actual_count != expected_count:
            print(f"  Внимание: создано {actual_count} агентов из {expected_count} ожидаемых (без учета {self.frames - expected_count} зарезервированных слотов)")
        
        return results


def main():
    """Главная функция оркестратора"""
    parser = argparse.ArgumentParser(description='V2 Orchestrator с модульной архитектурой')
    parser.add_argument('--modules', nargs='+', default=['mp5_probe', 'status_246'],
                      help='Список RTC модулей для подключения')
    parser.add_argument('--steps', type=int, default=None,
                      help='Количество шагов симуляции (по умолчанию из HL_V2_STEPS)')
    parser.add_argument('--enable-mp2', action='store_true',
                      help='Включить MP2 device-side export')
    parser.add_argument('--mp2-drain-interval', type=int, default=0,
                      help='Интервал дренажа MP2 (шаги). 0 = только финальный дренаж')
    parser.add_argument('--drop-table', action='store_true',
                      help='Перед запуском дропнуть таблицу sim_masterv2 (DROP TABLE IF EXISTS)')
    args = parser.parse_args()
    
    # Начало общего времени
    t_total_start = time.perf_counter()
    
    # Загружаем данные
    print("Загрузка данных из ClickHouse...")
    t_data_start = time.perf_counter()
    client = get_client()
    # Опционально дропаем таблицу проекта перед запуском
    if args.drop_table:
        try:
            print("Удаление таблицы sim_masterv2 (DROP TABLE IF EXISTS)...")
            client.execute("DROP TABLE IF EXISTS sim_masterv2")
            print("  Таблица sim_masterv2 удалена (если существовала)")
        except Exception as e:
            print(f"  Ошибка удаления таблицы sim_masterv2: {e}")
            raise

    env_data = prepare_env_arrays(client)
    t_data_load = time.perf_counter() - t_data_start
    print(f"  Данные загружены за {t_data_load:.2f}с")
    
    # Создаем оркестратор с поддержкой MP2
    orchestrator = V2Orchestrator(env_data, enable_mp2=args.enable_mp2,
                                  clickhouse_client=client if args.enable_mp2 else None)
    
    # Строим модель с указанными модулями
    orchestrator.build_model(args.modules)
    
    # Создаем симуляцию
    orchestrator.create_simulation()
    # Настраиваем интервал инкрементального дренажа MP2 (если подключен)
    if args.enable_mp2 and orchestrator.mp2_drain_func:
        try:
            orchestrator.mp2_drain_func.interval_days = max(0, int(args.mp2_drain_interval))
        except Exception:
            pass
    
    # Замеряем время GPU обработки
    t_gpu_start = time.perf_counter()
    
    # Запускаем симуляцию
    steps = args.steps or orchestrator.days
    orchestrator.run(steps)
    
    t_gpu_total = time.perf_counter() - t_gpu_start
    
    # Получаем время дренажа если MP2 включен
    t_db_total = 0.0
    if args.enable_mp2 and orchestrator.mp2_drain_func:
        t_db_total = orchestrator.mp2_drain_func.total_drain_time
    
    # Общее время
    t_total = time.perf_counter() - t_total_start
    
    # Выводим статистику по таймингам
    print(f"\n=== Тайминги выполнения ===")
    print(f"Загрузка модели и данных: {t_data_load:.2f}с")
    print(f"Обработка на GPU: {t_gpu_total:.2f}с")
    if args.enable_mp2:
        print(f"  - в т.ч. выгрузка в СУБД: {t_db_total:.2f}с (параллельно)")
    print(f"Общее время выполнения: {t_total:.2f}с")
    print(f"Среднее время на шаг: {t_gpu_total/args.steps*1000:.1f}мс")
    # Детализация шагов (p50/p95/max)
    try:
        import statistics
        p50 = statistics.median(step_times) if 'step_times' in locals() and step_times else 0.0
        p95 = sorted(step_times)[int(0.95*len(step_times))-1] if step_times else 0.0
        pmax = max(step_times) if step_times else 0.0
        print(f"Шаги: p50={p50*1000:.1f}мс, p95={p95*1000:.1f}мс, max={pmax*1000:.1f}мс")
    except Exception:
        pass
    if args.enable_mp2 and orchestrator.mp2_drain_func:
        d = orchestrator.mp2_drain_func
        # Скорость и статистика дренажа
        rows = getattr(d, 'total_rows_written', 0)
        flushes = getattr(d, 'flush_count', 0)
        t_flush = getattr(d, 'total_flush_time', 0.0)
        max_batch = getattr(d, 'max_batch_rows', 0)
        rps = (rows / t_db_total) if t_db_total > 0 else 0.0
        print(f"Дренаж MP2: rows={rows}, flushes={flushes}, max_batch={max_batch}, flush_time={t_flush:.2f}с, rows/s≈{rps:,.0f}")
    
    # Получаем результаты (без подробного печатного вывода)
    _ = orchestrator.get_results()
    # Итоговую сводку по состояниям не печатаем — по требованию выводим только
    # поминутные/помесячные сводки по шагам и переходы intent в operations
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
